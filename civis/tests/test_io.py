from collections import OrderedDict
from io import StringIO, BytesIO
import json
import os
import tempfile
from unittest.mock import patch

import pytest
import vcr

try:
    import pandas as pd
    has_pandas = True
except ImportError:
    has_pandas = False

import civis
from civis.resources._resources import get_swagger_spec, generate_classes
from civis.tests.testcase import (CivisVCRTestCase,
                                  cassette_dir,
                                  conditionally_patch)

swagger_import_str = 'civis.resources._resources.get_swagger_spec'
THIS_DIR = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(THIS_DIR, "civis_api_spec.json")) as f:
    civis_api_spec = json.load(f, object_pairs_hook=OrderedDict)


@conditionally_patch('civis.polling.time.sleep', return_value=None)
@conditionally_patch('civis.polling.PollableResult._poll_wait_elapsed',
                     return_value=True)
@patch(swagger_import_str, return_value=civis_api_spec)
class ImportTests(CivisVCRTestCase):

    @classmethod
    def setUpClass(cls):
        get_swagger_spec.cache_clear()
        generate_classes.cache_clear()

    @classmethod
    def tearDownClass(cls):
        get_swagger_spec.cache_clear()
        generate_classes.cache_clear()

    @classmethod
    @conditionally_patch('civis.polling.time.sleep', return_value=None)
    @conditionally_patch('civis.polling.PollableResult._poll_wait_elapsed',
                         return_value=True)
    @patch(swagger_import_str, return_value=civis_api_spec)
    def setup_class(cls, *mocks):
        setup_vcr = vcr.VCR(filter_headers=['Authorization'])
        setup_cassette = os.path.join(cassette_dir(), 'io_setup.yml')
        with setup_vcr.use_cassette(setup_cassette):
            # create a file
            buf = StringIO()
            buf.write('a,b,c\n1,2,3')
            buf.seek(0)
            file_id = civis.io.file_to_civis(buf, 'somename')
            cls.file_id = file_id

            # create the table. assumes this function works.
            sql = """
                DROP TABLE IF EXISTS scratch.api_client_test_fixture;

                CREATE TABLE scratch.api_client_test_fixture (
                    a int,
                    b int,
                    c int
                );

                INSERT INTO scratch.api_client_test_fixture
                VALUES (1,2,3);
            """
            res = civis.io.query_civis(sql, 'redshift-general')
            res.result()  # block

            # create an export to check get_url. also tests export_csv
            with tempfile.NamedTemporaryFile() as tmp:
                sql = "SELECT * FROM scratch.api_client_test_fixture"
                database = 'redshift-general'
                result = civis.io.civis_to_csv(tmp.name, sql, database)
                result = result.result()
                assert result.state == 'succeeded'

            cls.export_job_id = result.sql_id

    @patch(swagger_import_str, return_value=civis_api_spec)
    def test_get_url_from_file_id(self, *mocks):
        client = civis.APIClient()
        url = civis.io._files._get_url_from_file_id(self.file_id, client)
        assert url.startswith('https://civis-console.s3.amazonaws.com/files/')

    @patch(swagger_import_str, return_value=civis_api_spec)
    def test_civis_to_file(self, *mocks):
        buf = BytesIO()
        civis.io.civis_to_file(self.file_id, buf)
        buf.seek(0)
        assert buf.read() == b'a,b,c\n1,2,3'

    @patch(swagger_import_str, return_value=civis_api_spec)
    def test_csv_to_civis(self, *mocks):
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b'a,b,c\n1,2,3')
            tmp.flush()

            table = "scratch.api_client_test_fixture"
            database = 'redshift-general'
            result = civis.io.csv_to_civis(tmp.name, database, table,
                                           existing_table_rows='truncate')
            result = result.result()  # block until done

        assert isinstance(result.id, int)
        assert result.state == 'succeeded'

    @pytest.mark.skipif(not has_pandas, reason="pandas not installed")
    @patch(swagger_import_str, return_value=civis_api_spec)
    def test_read_civis_pandas(self, *mocks):
        expected = pd.DataFrame([[1, 2, 3]], columns=['a', 'b', 'c'])
        df = civis.io.read_civis('scratch.api_client_test_fixture',
                                 'redshift-general', use_pandas=True)
        assert df.equals(expected)

    @patch(swagger_import_str, return_value=civis_api_spec)
    def test_read_civis_no_pandas(self, *mocks):
        expected = [['a', 'b', 'c'], ['1', '2', '3']]
        data = civis.io.read_civis('scratch.api_client_test_fixture',
                                   'redshift-general', use_pandas=False)
        assert data == expected

    @patch(swagger_import_str, return_value=civis_api_spec)
    def test_read_civis_sql(self, *mocks):
        sql = "SELECT * FROM scratch.api_client_test_fixture"
        expected = [['a', 'b', 'c'], ['1', '2', '3']]
        data = civis.io.read_civis_sql(sql, 'redshift-general',
                                       use_pandas=False)
        assert data == expected

    @pytest.mark.skipif(not has_pandas, reason="pandas not installed")
    @patch(swagger_import_str, return_value=civis_api_spec)
    def test_dataframe_to_civis(self, *mocks):
        df = pd.DataFrame([['1', '2', '3']], columns=['a', 'b', 'c'])
        result = civis.io.dataframe_to_civis(df, 'redshift-general',
                                             'scratch.api_client_test_fixture',
                                             existing_table_rows='truncate')
        result = result.result()
        assert result.state == 'succeeded'

    @patch(swagger_import_str, return_value=civis_api_spec)
    def test_civis_to_multifile_csv(self, *mocks):
        sql = "SELECT * FROM scratch.api_client_test_fixture"
        result = civis.io.civis_to_multifile_csv(sql,
                                                 database='redshift-general')
        assert set(result.keys()) == {'entries', 'query', 'header'}
        assert result['query'] == sql
        assert result['header'] == ['a', 'b', 'c']
        assert isinstance(result['entries'], list)
        assert set(result['entries'][0].keys()) == {'id', 'name', 'size',
                                                    'url', 'url_signed'}
        assert result['entries'][0]['url_signed'].startswith('https://civis-'
                                                             'console.s3.'
                                                             'amazonaws.com/')

    @patch(swagger_import_str, return_value=civis_api_spec)
    def test_transfer_table(self, *mocks):
        result = civis.io.transfer_table('redshift-general', 'redshift-test',
                                         'scratch.api_client_test_fixture',
                                         'scratch.api_client_test_fixture')
        result = result.result()
        assert result.state == 'succeeded'

        # check for side effect
        sql = 'select * from scratch.api_client_test_fixture'
        result = civis.io.query_civis(sql, 'redshift-test').result()
        assert result.state == 'succeeded'

    def test_get_sql_select(self, *mocks):
        x = "select * from schema.table"
        y = "select a, b, c from schema.table"
        table = "schema.table"
        assert civis.io._tables._get_sql_select(table) == x
        assert civis.io._tables._get_sql_select(table, ['a', 'b', 'c']) == y
        with pytest.raises(TypeError):
            civis.io._tables._get_sql_select(table, "column_a")

    def test_download_file(self, *mocks):
        url = "https://httpbin.org/stream/3"
        x = '{"url": "https://httpbin.org/stream/3", "headers": {"Host": "httpbin.org", "Accept-Encoding": "gzip, deflate", "Accept": "*/*", "User-Agent": "python-requests/2.7.0 CPython/3.4.3 Linux/3.19.0-25-generic"}, "args": {}, "id": 0, "origin": "108.211.184.39"}\n'  # noqa: E501
        y = '{"url": "https://httpbin.org/stream/3", "headers": {"Host": "httpbin.org", "Accept-Encoding": "gzip, deflate", "Accept": "*/*", "User-Agent": "python-requests/2.7.0 CPython/3.4.3 Linux/3.19.0-25-generic"}, "args": {}, "id": 1, "origin": "108.211.184.39"}\n'  # noqa: E501
        z = '{"url": "https://httpbin.org/stream/3", "headers": {"Host": "httpbin.org", "Accept-Encoding": "gzip, deflate", "Accept": "*/*", "User-Agent": "python-requests/2.7.0 CPython/3.4.3 Linux/3.19.0-25-generic"}, "args": {}, "id": 2, "origin": "108.211.184.39"}\n'  # noqa: E501
        expected = x + y + z
        with tempfile.NamedTemporaryFile() as tmp:
            civis.io._tables._download_file(url, tmp.name)
            with open(tmp.name, "r") as f:
                data = f.read()
        assert data == expected
