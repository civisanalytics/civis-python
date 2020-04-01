#!/usr/bin/env python3

"""
This script tests end-to-end functionality using the Civis Python client.
It uses the live Civis API and Redshift, so a valid CIVIS_API_KEY is needed.

This is based on a similar script for the R client:
https://github.com/civisanalytics/civis-r/blob/master/tools/integration_tests/smoke_test.R
"""

import io
import logging
import time

import civis
import pandas as pd
from sklearn.datasets import load_iris


def main():
    logging.basicConfig(format="", level=logging.INFO)

    logger = logging.getLogger("civis")

    t0 = time.time()

    database = "redshift-general"
    client = civis.APIClient()

    # Test read_civis and read_civis_sql produce the same results.
    # The table used here has an explicit index column to sort by in case the
    # rows come back in a different order.
    logger.info('Testing reading from redshift...')
    sql = "SELECT * FROM datascience.iris"
    df1 = civis.io.read_civis_sql(
        sql=sql, database=database, use_pandas=True, client=client
    ).set_index('index')
    df2 = civis.io.read_civis(
        table="datascience.iris", database=database, use_pandas=True,
        client=client
    ).set_index('index')
    assert df1.shape == (150, 5)
    # check_like=True since the order in which rows are retrieved may vary.
    pd.testing.assert_frame_equal(df1, df2, check_like=True)

    # Test uploading data.
    logger.info('Testing uploading to redshift...')
    table = "scratch.smoke_test_{}".format(int(time.time()))
    iris = load_iris()
    df_iris1 = (
        pd.DataFrame(iris.data)
        .rename(columns={0: 'c0', 1: 'c1', 2: 'c2', 3: 'c3'})
        .join(pd.DataFrame(iris.target).rename(columns={0: 'label'}))
        .reset_index()
    )
    try:
        civis.io.dataframe_to_civis(
            df_iris1, database, table, client=client).result()
        df_iris2 = civis.io.read_civis(
            table=table, database=database, use_pandas=True, client=client)
        pd.testing.assert_frame_equal(
            df_iris1.sort_values(by='index').set_index('index'),
            df_iris2.sort_values(by='index').set_index('index')
        )
    finally:
        civis.io.query_civis("DROP TABLE IF EXISTS %s" % table,
                             database=database, client=client)

    # Test uploading and downloading file.
    logger.info('Testing File uploading and downloading...')
    buf = io.BytesIO()
    csv_bytes1 = df_iris1.to_csv(index=False).encode('utf-8')
    buf.write(csv_bytes1)
    buf.seek(0)
    file_id = civis.io.file_to_civis(
        buf, name="civis-python test file", client=client)
    buf.seek(0)
    civis.io.civis_to_file(file_id, buf, client=client)
    buf.seek(0)
    csv_bytes2 = buf.read()
    assert csv_bytes1 == csv_bytes2, "File upload/download did not match."

    # Test modeling.
    logger.info('Testing Civis-ML...')
    for civisml_version in (None, 'v2.2'):  # None = latest production version
        logger.info('CivisML version: %r', civisml_version)
        mp = civis.ml.ModelPipeline(
            model="sparse_logistic",
            dependent_variable="type",
            primary_key="index",
            client=client,
            civisml_version=civisml_version,
        )
        result = mp.train(
            table_name="datascience.iris", database_name=database).result()
        assert result['state'] == 'succeeded'

    logger.info("%.1f seconds elapsed in total.", time.time() - t0)


if __name__ == '__main__':
    main()
