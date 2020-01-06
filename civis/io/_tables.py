import json
import concurrent.futures
import csv
from os import path
import io
import logging
import os
import six
import shutil
import warnings
import zlib

import gzip
import zipfile

from civis import APIClient
from civis._utils import maybe_get_random_name
from civis.base import EmptyResultError, CivisImportError
from civis.compat import TemporaryDirectory
from civis.futures import CivisFuture
from civis.io import civis_to_file, file_to_civis, query_civis
from civis.utils import run_job
from civis._deprecation import deprecate_param

import requests

try:
    from io import StringIO
except ImportError:
    from cStringIO import StringIO
try:
    import pandas as pd
    NO_PANDAS = False
except ImportError:
    NO_PANDAS = True

CHUNK_SIZE = 32 * 1024
log = logging.getLogger(__name__)
__all__ = ['read_civis', 'read_civis_sql', 'civis_to_csv',
           'civis_to_multifile_csv', 'dataframe_to_civis', 'csv_to_civis',
           'civis_file_to_table', 'split_schema_tablename',
           'export_to_civis_file']

DELIMITERS = {
    ',': 'comma',
    '\t': 'tab',
    '|': 'pipe',
}


@deprecate_param('v2.0.0', 'api_key')
def read_civis(table, database, columns=None, use_pandas=False,
               job_name=None, api_key=None, client=None, credential_id=None,
               polling_interval=None, archive=False, hidden=True, **kwargs):
    """Read data from a Civis table.

    Parameters
    ----------
    table : str
        Name of table, including schema, in the database. E.g.
        ``'my_schema.my_table'``. Schemas or tablenames with periods must
        be double quoted, e.g. ``'my_schema."my.table"'``.
    database : str or int
        Read data from this database. Can be the database name or ID.
    columns : list, optional
        A list of column names. Column SQL transformations are possible.
        If omitted, all columns are exported.
    use_pandas : bool, optional
        If ``True``, return a :class:`pandas:pandas.DataFrame`. Otherwise,
        return a list of results from :func:`python:csv.reader`.
    job_name : str, optional
        A name to give the job. If omitted, a random job name will be
        used.
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    credential_id : str or int, optional
        The database credential ID.  If ``None``, the default credential
        will be used.
    polling_interval : int or float, optional
        Number of seconds to wait between checks for query completion.
    archive : bool, optional (deprecated)
        If ``True``, archive the import job as soon as it completes.
    hidden : bool, optional
        If ``True`` (the default), this job will not appear in the Civis UI.
    **kwargs : kwargs
        Extra keyword arguments are passed into
        :func:`pandas:pandas.read_csv` if `use_pandas` is ``True`` or
        passed into :func:`python:csv.reader` if `use_pandas` is
        ``False``.

    Returns
    -------
    data : :class:`pandas:pandas.DataFrame` or list
        A list of rows (with header as first row) if `use_pandas` is
        ``False``, otherwise a `pandas` `DataFrame`. Note that if
        `use_pandas` is ``False``, no parsing of types is performed and
        each row will be a list of strings.

    Raises
    ------
    ImportError
        If `use_pandas` is ``True`` and `pandas` is not installed.

    Examples
    --------
    >>> table = "schema.table"
    >>> database = "my_data"
    >>> columns = ["column_a", "ROW_NUMBER() OVER(ORDER BY date) AS order"]
    >>> data = read_civis(table, database, columns=columns)
    >>> columns = data.pop(0)
    >>> col_a_index = columns.index("column_a")
    >>> col_a = [row[col_a_index] for row in data]

    >>> df = read_civis("schema.table", "my_data", use_pandas=True)
    >>> col_a = df["column_a"]

    See Also
    --------
    civis.io.read_civis_sql : Read directly into memory using SQL.
    civis.io.civis_to_csv : Write directly to csv.
    civis.io.export_to_civis_file : Store a SQL query's results in a Civis file
    """
    if use_pandas and NO_PANDAS:
        raise ImportError("use_pandas is True but pandas is not installed.")
    if archive:
        warnings.warn("`archive` is deprecated and will be removed in v2.0.0. "
                      "Use `hidden` instead.", FutureWarning)
    if client is None:
        # Instantiate client here in case users provide a (deprecated) api_key
        client = APIClient(api_key=api_key)
    sql = _get_sql_select(table, columns)
    data = read_civis_sql(sql=sql, database=database, use_pandas=use_pandas,
                          job_name=job_name, client=client,
                          credential_id=credential_id,
                          polling_interval=polling_interval,
                          archive=archive, hidden=hidden, **kwargs)
    return data


def export_to_civis_file(sql, database, job_name=None, client=None,
                         credential_id=None, polling_interval=None,
                         hidden=True, csv_settings=None):
    """Store results of a query to a Civis file

    Parameters
    ----------
    sql : str
        The SQL select string to be executed.
    database : str or int
        Execute the query against this database. Can be the database name
        or ID.
    job_name : str, optional
        A name to give the job. If omitted, a random job name will be
        used.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    credential_id : str or int, optional
        The database credential ID.  If ``None``, the default credential
        will be used.
    polling_interval : int or float, optional
        Number of seconds to wait between checks for query completion.
    hidden : bool, optional
        If ``True`` (the default), this job will not appear in the Civis UI.
    csv_settings : dict, optional
        A dictionary of csv_settings to pass to
        :func:`civis.APIClient.scripts.post_sql`.

    Returns
    -------
    fut : :class:`~civis.futures.CivisFuture`
        A future which returns the response from
        :func:`civis.APIClient.scripts.get_sql_runs` after the sql query
        has completed and the result has been stored as a Civis file.

    Examples
    --------
    >>> sql = "SELECT * FROM schema.table"
    >>> fut = export_to_civis_file(sql, "my_database")
    >>> file_id = fut.result()['output'][0]["file_id"]


    See Also
    --------
    civis.io.read_civis : Read directly into memory without SQL.
    civis.io.read_civis_sql : Read results of a SQL query into memory.
    civis.io.civis_to_csv : Write directly to a CSV file.
    civis.io.civis_file_to_table : Upload a Civis file to a Civis table
    """
    client = client or APIClient()
    script_id, run_id = _sql_script(client=client,
                                    sql=sql,
                                    database=database,
                                    job_name=job_name,
                                    credential_id=credential_id,
                                    csv_settings=csv_settings,
                                    hidden=hidden)
    fut = CivisFuture(client.scripts.get_sql_runs, (script_id, run_id),
                      polling_interval=polling_interval, client=client,
                      poll_on_creation=False)
    return fut


@deprecate_param('v2.0.0', 'api_key')
def read_civis_sql(sql, database, use_pandas=False, job_name=None,
                   api_key=None, client=None, credential_id=None,
                   polling_interval=None, archive=False,
                   hidden=True, **kwargs):
    """Read data from Civis using a custom SQL string.

    The custom SQL string will be executed twice; once to attempt to
    retrieve headers and once to retrieve the data. This is done to
    use a more performant method for retrieving the data. The first
    execution of the custom SQL is controlled such that changes in
    state cannot occur (e.g., INSERT, UPDATE, DELETE, etc.).

    Parameters
    ----------
    sql : str
        The SQL select string to be executed.
    database : str or int
        Execute the query against this database. Can be the database name
        or ID.
    use_pandas : bool, optional
        If ``True``, return a :class:`pandas:pandas.DataFrame`. Otherwise,
        return a list of results from :func:`python:csv.reader`.
    job_name : str, optional
        A name to give the job. If omitted, a random job name will be
        used.
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    credential_id : str or int, optional
        The database credential ID.  If ``None``, the default credential
        will be used.
    polling_interval : int or float, optional
        Number of seconds to wait between checks for query completion.
    archive : bool, optional (deprecated)
        If ``True``, archive the import job as soon as it completes.
    hidden : bool, optional
        If ``True`` (the default), this job will not appear in the Civis UI.
    **kwargs : kwargs
        Extra keyword arguments are passed into
        :func:`pandas:pandas.read_csv` if `use_pandas` is ``True`` or
        passed into :func:`python:csv.reader` if `use_pandas` is
        ``False``.

    Returns
    -------
    data : :class:`pandas:pandas.DataFrame` or list
        A list of rows (with header as first row) if `use_pandas` is
        ``False``, otherwise a `pandas` `DataFrame`. Note that if
        `use_pandas` is ``False``, no parsing of types is performed and
        each row will be a list of strings.

    Raises
    ------
    ImportError
        If `use_pandas` is ``True`` and `pandas` is not installed.

    Examples
    --------
    >>> sql = "SELECT * FROM schema.table"
    >>> df = read_civis_sql(sql, "my_database", use_pandas=True)
    >>> col_a = df["column_a"]

    >>> data = read_civis_sql(sql, "my_database")
    >>> columns = data.pop(0)
    >>> col_a_index = columns.index("column_a")
    >>> col_a = [row[col_a_index] for row in data]

    Notes
    -----
    This reads the data into memory.

    See Also
    --------
    civis.io.read_civis : Read directly into memory without SQL.
    civis.io.civis_to_csv : Write directly to a CSV file.
    """
    if client is None:
        client = APIClient(api_key=api_key)
    if use_pandas and NO_PANDAS:
        raise ImportError("use_pandas is True but pandas is not installed.")
    if archive:
        warnings.warn("`archive` is deprecated and will be removed in v2.0.0. "
                      "Use `hidden` instead.", FutureWarning)

    db_id = client.get_database_id(database)
    credential_id = credential_id or client.default_credential

    # Try to get headers separately. In most scenarios this will greatly
    # reduce the work that Platform does to provide a single output file
    # with headers prepended to it due to how distributed databases export
    # data at scale.
    headers = _get_headers(client, sql, db_id, credential_id, polling_interval)
    # include_header defaults to True in the API.
    include_header = True if headers is None else False

    csv_settings = dict(include_header=include_header,
                        compression='gzip')

    script_id, run_id = _sql_script(client, sql, db_id,
                                    job_name, credential_id,
                                    csv_settings=csv_settings,
                                    hidden=hidden)
    fut = CivisFuture(client.scripts.get_sql_runs, (script_id, run_id),
                      polling_interval=polling_interval, client=client,
                      poll_on_creation=False)
    if archive:

        def f(x):
            return client.scripts.put_sql_archive(script_id, True)

        fut.add_done_callback(f)
    fut.result()
    outputs = client.scripts.get_sql_runs(script_id, run_id)["output"]
    if not outputs:
        raise EmptyResultError("Query {} returned no output."
                               .format(script_id))

    url = outputs[0]["path"]
    file_id = outputs[0]["file_id"]
    log.debug('Exported results to Civis file %s (%s)',
              outputs[0]["output_name"], file_id)

    if use_pandas:
        # allows users to enter their own names parameter
        _kwargs = {'names': headers}
        _kwargs.update(kwargs)
        _kwargs['compression'] = 'gzip'

        data = pd.read_csv(url, **_kwargs)
    else:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with StringIO() as buf:
            if headers:
                buf.write(','.join(headers) + '\n')
            _decompress_stream(response, buf, write_bytes=False)
            buf.seek(0)
            data = list(csv.reader(buf, **kwargs))

    return data


@deprecate_param('v2.0.0', 'api_key')
def civis_to_csv(filename, sql, database, job_name=None, api_key=None,
                 client=None, credential_id=None, include_header=True,
                 compression='none', delimiter=',', unquoted=False,
                 archive=False, hidden=True, polling_interval=None):
    """Export data from Civis to a local CSV file.

    The custom SQL string will be executed twice; once to attempt to
    retrieve headers and once to retrieve the data. This is done to
    use a more performant method for retrieving the data. The first
    execution of the custom SQL is controlled such that changes in
    state cannot occur (e.g., INSERT, UPDATE, DELETE, etc.).

    Parameters
    ----------
    filename : str
        Download exported data into this file.
    sql : str
        The SQL select string to be executed.
    database : str or int
        Export data from this database. Can be the database name or ID.
    job_name : str, optional
        A name to give the job. If omitted, a random job name will be
        used.
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    credential_id : str or int, optional
        The ID of the database credential.  If ``None``, the default
        credential will be used.
    include_header: bool, optional
        If ``True``, the first line of the CSV will be headers.
        Default: ``True``.
    compression: str, optional
        Type of compression to use, if any. One of ``'none'``, ``'zip'``, or
        ``'gzip'``. Default ``'none'``. ``'gzip'`` currently returns a file
        with no compression unless include_header is set to False. In a
        future release, a ``'gzip'`` compressed file will be returned for
        all cases.
    delimiter: str, optional
        Which delimiter to use, if any. One of ``','``, ``'\t'``, or
        ``'|'``. Default: ``','``.
    unquoted: bool, optional
        Whether or not to quote fields. Default: ``False``.
    polling_interval : int or float, optional
        Number of seconds to wait between checks for query completion.
    archive : bool, optional (deprecated)
        If ``True``, archive the import job as soon as it completes.
    hidden : bool, optional
        If ``True`` (the default), this job will not appear in the Civis UI.

    Returns
    -------
    results : :class:`~civis.futures.CivisFuture`
        A `CivisFuture` object.

    Examples
    --------
    >>> sql = "SELECT * FROM schema.table"
    >>> fut = civis_to_csv("file.csv", sql, "my_database")
    >>> fut.result()  # Wait for job to complete

    See Also
    --------
    civis.io.read_civis : Read table contents into memory.
    civis.io.read_civis_sql : Read results of a SQL query into memory.
    civis.io.export_to_civis_file : Store a SQL query's results in a Civis file
    """
    if archive:
        warnings.warn("`archive` is deprecated and will be removed in v2.0.0. "
                      "Use `hidden` instead.", FutureWarning)
    if client is None:
        client = APIClient(api_key=api_key)

    db_id = client.get_database_id(database)
    credential_id = credential_id or client.default_credential

    # don't fix bug that would cause breaking change for now
    # when gzip compression is requested, a gzip file is not actually returned
    # instead the gzip file is decompressed during download
    if compression == 'gzip' and include_header:
        compression = 'none'

    # don't support parallel unload; the output format
    # is different which would introduce a breaking change
    headers = b''

    delimiter = DELIMITERS.get(delimiter)
    if not delimiter:
        raise ValueError("delimiter must be one of {}"
                         .format(DELIMITERS.keys()))

    # always set compression to gzip to reduce I/O
    csv_settings = dict(include_header=include_header,
                        compression='gzip',
                        column_delimiter=delimiter,
                        unquoted=unquoted,
                        filename_prefix=None,
                        force_multifile=False)

    script_id, run_id = _sql_script(client, sql, db_id, job_name,
                                    credential_id, hidden=hidden,
                                    csv_settings=csv_settings)
    fut = CivisFuture(client.scripts.get_sql_runs, (script_id, run_id),
                      polling_interval=polling_interval, client=client,
                      poll_on_creation=False)
    download = _download_callback(script_id, run_id, filename,
                                  headers, compression)
    fut.add_done_callback(download)
    if archive:

        def f(x):
            return client.scripts.put_sql_archive(script_id, True)

        fut.add_done_callback(f)

    return fut


@deprecate_param('v2.0.0', 'api_key')
def civis_to_multifile_csv(sql, database, job_name=None, api_key=None,
                           client=None, credential_id=None,
                           include_header=True,
                           compression='none', delimiter='|',
                           max_file_size=None,
                           unquoted=False, prefix=None,
                           polling_interval=None, hidden=True):
    """Unload the result of SQL query and return presigned urls.

    This function is intended for unloading large queries/tables from redshift
    as it uses a 'PARALLEL ON' S3 unload. It returns a similar manifest file
    to conventional S3 UNLOAD statements except the CSV parts are accessible
    via both files endpoint IDs and presigned S3 urls.

    Parameters
    ----------
    sql : str
        The SQL select string to be executed.
    database : str or int
        Execute the query against this database. Can be the database name
        or ID.
    job_name : str, optional
        A name to give the job. If omitted, a random job name will be
        used.
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    credential_id : str or int, optional
        The database credential ID.  If ``None``, the default credential
        will be used.
    include_header: bool, optional
        If ``True`` include a key in the returned dictionary containing a list
        of column names. Default: ``True``.
    compression: str, optional
        Type of compression to use, if any. One of ``'none'``, ``'zip'``, or
        ``'gzip'``. Default ``'none'``.
    delimiter: str, optional
        Which delimiter to use, if any. One of ``','``, ``'\t'``, or
        ``'|'``. Default: ``'|'``.
    max_file_size: int, optional
        Maximum number of Megabytes each created file will be.
    unquoted: bool, optional
        Whether or not to quote fields. Default: ``False``.
    prefix: str, optional
        A user specified filename prefix for the output file to have. Default:
        ``None``.
    polling_interval : int or float, optional
        Number of seconds to wait between checks for query completion.
    hidden : bool, optional
        If ``True`` (the default), this job will not appear in the Civis UI.

    Returns
    -------
    unload_manifest: dict
        A dictionary resembling an AWS manifest file. Has the following keys:

        'query': str
            The query.

        'header': list of str
            The columns from the query.

        'entries': list of dict
            Each dict has the following keys:

            'id': int
                File ID
            'name': str
                Filename
            'size': int
                File size in bytes
            'url': str
                Unsigned S3 URL ('s3://...')
            'url_signed': str
                Signed S3 URL ('https://...')

        'unquoted': bool
            Whether the cells are quoted.

        'compression': str
            Type of compression used.

        'delimiter': str
            Delimiter that separates the cells.

    Examples
    --------
    >>> sql = "SELECT * FROM schema.my_big_table"
    >>> database = "my_database"
    >>> delimiter = "|"
    >>> manifest = civis_to_multifile_csv(sql, database, delimiter=delimiter)
    >>> ids = [entry['id'] for entry in manifest['entries']]
    >>> buf = BytesIO()
    >>> civis_to_file(ids[0], buf)
    >>> buf.seek(0)
    >>> df = pd.read_csv(buf, delimiter=delimiter)

    See Also
    --------
    civis.APIClient.scripts.post_sql
    """
    if client is None:
        client = APIClient(api_key=api_key)
    delimiter = DELIMITERS.get(delimiter)
    assert delimiter, "delimiter must be one of {}".format(DELIMITERS.keys())

    csv_settings = dict(include_header=include_header,
                        compression=compression,
                        column_delimiter=delimiter,
                        unquoted=unquoted,
                        filename_prefix=prefix,
                        force_multifile=True,
                        max_file_size=max_file_size)
    script_id, run_id = _sql_script(client, sql, database, job_name,
                                    credential_id, hidden,
                                    csv_settings=csv_settings)

    fut = CivisFuture(client.scripts.get_sql_runs, (script_id, run_id),
                      polling_interval=polling_interval, client=client,
                      poll_on_creation=False)

    outputs = fut.result()["output"]
    if not outputs:
        raise EmptyResultError("Unload query {} returned no manifest."
                               .format(script_id))

    buf = io.BytesIO()
    civis_to_file(outputs[0]['file_id'], buf, client=client)
    txt = io.TextIOWrapper(buf, encoding='utf-8')
    txt.seek(0)
    unload_manifest = json.load(txt)

    return unload_manifest


@deprecate_param('v2.0.0', 'api_key', 'headers')
def dataframe_to_civis(df, database, table, api_key=None, client=None,
                       max_errors=None, existing_table_rows="fail",
                       diststyle=None, distkey=None,
                       sortkey1=None, sortkey2=None,
                       headers=None, credential_id=None,
                       primary_keys=None, last_modified_keys=None,
                       execution="immediate",
                       delimiter=None, polling_interval=None,
                       archive=False, hidden=True, **kwargs):
    """Upload a `pandas` `DataFrame` into a Civis table.

    The `DataFrame`'s index will not be included. To store the index
    along with the other values, use `df.reset_index()` instead
    of `df` as the first argument to this function.

    Parameters
    ----------
    df : :class:`pandas:pandas.DataFrame`
        The `DataFrame` to upload to Civis.
    database : str or int
        Upload data into this database. Can be the database name or ID.
    table : str
        The schema and table you want to upload to. E.g.,
        ``'scratch.table'``. Schemas or tablenames with periods must
        be double quoted, e.g. ``'scratch."my.table"'``.
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    max_errors : int, optional
        The maximum number of rows with errors to remove from the import
        before failing.
    existing_table_rows : str, optional
        The behaviour if a table with the requested name already exists.
        One of ``'fail'``, ``'truncate'``, ``'append'``, ``'drop'``, or
        ``'upsert'``. Defaults to ``'fail'``.
    diststyle : str, optional
        The distribution style for the table.
        One of ``'even'``, ``'all'`` or ``'key'``.
    distkey : str, optional
        The column to use as the distkey for the table.
    sortkey1 : str, optional
        The column to use as the sortkey for the table.
    sortkey2 : str, optional
        The second column in a compound sortkey for the table.
    headers : bool, optional [DEPRECATED]
        Whether or not the first row of the file should be treated as
        headers. The default, ``None``, attempts to autodetect whether
        or not the first row contains headers.

        This parameter has no effect in versions >= 1.11 and will be
        removed in v2.0. Tables will always be written with column
        names read from the DataFrame. Use the `header` parameter
        (which will be passed directly to :func:`~pandas.DataFrame.to_csv`)
        to modify the column names in the Civis Table.
    credential_id : str or int, optional
        The ID of the database credential.  If ``None``, the default
        credential will be used.
    primary_keys: list[str], optional
        A list of the primary key column(s) of the destination table that
        uniquely identify a record. If existing_table_rows is "upsert", this
        field is required. Note that this is true regardless of whether the
        destination database itself requires a primary key.
    last_modified_keys: list[str], optional
        A list of the columns indicating a record has been updated. If
        existing_table_rows is "upsert", this field is required.
    escaped: bool, optional
        A boolean value indicating whether or not the source file has quotes
        escaped with a backslash. Defaults to false.
    execution: string, optional, default "immediate"
        One of "delayed" or "immediate". If "immediate", refresh column
        statistics as part of the run. If "delayed", flag the table for a
        deferred statistics update; column statistics may not be available
        for up to 24 hours. In addition, if existing_table_rows is "upsert",
        delayed executions move data from staging table to final table after a
        brief delay, in order to accommodate multiple concurrent imports to the
        same destination table.
    polling_interval : int or float, optional
        Number of seconds to wait between checks for job completion.
    archive : bool, optional (deprecated)
        If ``True``, archive the import job as soon as it completes.
    hidden : bool, optional
        If ``True`` (the default), this job will not appear in the Civis UI.
    **kwargs : kwargs
        Extra keyword arguments will be passed to
        :meth:`pandas:pandas.DataFrame.to_csv`.

    Returns
    -------
    fut : :class:`~civis.futures.CivisFuture`
        A `CivisFuture` object.

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    >>> fut = civis.io.dataframe_to_civis(df, 'my-database',
    ...                                   'scratch.df_table')
    >>> fut.result()

    See Also
    --------
    :func:`~pandas.DataFrame.to_csv`
    """
    if client is None:
        client = APIClient(api_key=api_key)
    if archive:
        warnings.warn("`archive` is deprecated and will be removed in v2.0.0. "
                      "Use `hidden` instead.", FutureWarning)

    headers = False if kwargs.get('header') is False else True
    with TemporaryDirectory() as tmp_dir:
        tmp_path = os.path.join(tmp_dir, 'dataframe_to_civis.csv')
        to_csv_kwargs = {'encoding': 'utf-8', 'index': False}
        to_csv_kwargs.update(kwargs)
        df.to_csv(tmp_path, **to_csv_kwargs)
        _, name = split_schema_tablename(table)
        file_id = file_to_civis(tmp_path, name, client=client)

    delimiter = ','
    fut = civis_file_to_table(file_id, database, table,
                              client=client, max_errors=max_errors,
                              existing_table_rows=existing_table_rows,
                              diststyle=diststyle, distkey=distkey,
                              sortkey1=sortkey1, sortkey2=sortkey2,
                              delimiter=delimiter, headers=headers,
                              credential_id=credential_id,
                              primary_keys=primary_keys,
                              last_modified_keys=last_modified_keys,
                              escaped=False, execution=execution,
                              polling_interval=polling_interval,
                              hidden=hidden)

    return fut


@deprecate_param('v2.0.0', 'api_key')
def csv_to_civis(filename, database, table, api_key=None, client=None,
                 max_errors=None, existing_table_rows="fail",
                 diststyle=None, distkey=None,
                 sortkey1=None, sortkey2=None,
                 delimiter=",", headers=None,
                 primary_keys=None, last_modified_keys=None,
                 escaped=False, execution="immediate",
                 credential_id=None, polling_interval=None, archive=False,
                 hidden=True):

    """Upload the contents of a local CSV file to Civis.

    Parameters
    ----------
    filename : str
        Upload the contents of this file.
    database : str or int
        Upload data into this database. Can be the database name or ID.
    table : str
        The schema and table you want to upload to. E.g.,
        ``'scratch.table'``.
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    max_errors : int, optional
        The maximum number of rows with errors to remove from the import
        before failing.
    existing_table_rows : str, optional
        The behaviour if a table with the requested name already exists.
        One of ``'fail'``, ``'truncate'``, ``'append'``, ``'drop'``, or
        ``'upsert'``. Defaults to ``'fail'``.
    diststyle : str, optional
        The distribution style for the table.
        One of ``'even'``, ``'all'`` or ``'key'``.
    distkey : str, optional
        The column to use as the distkey for the table.
    sortkey1 : str, optional
        The column to use as the sortkey for the table.
    sortkey2 : str, optional
        The second column in a compound sortkey for the table.
    delimiter : string, optional
        The column delimiter. One of ``','``, ``'\\t'`` or ``'|'``.
    headers : bool, optional
        Whether or not the first row of the file should be treated as
        headers. The default, ``None``, attempts to autodetect whether
        or not the first row contains headers.
    primary_keys: list[str], optional
        A list of the primary key column(s) of the destination table that
        uniquely identify a record. If existing_table_rows is "upsert", this
        field is required. Note that this is true regardless of whether the
        destination database itself requires a primary key.
    last_modified_keys: list[str], optional
        A list of the columns indicating a record has been updated. If
        existing_table_rows is "upsert", this field is required.
    escaped: bool, optional
        A boolean value indicating whether or not the source file has quotes
        escaped with a backslash. Defaults to false.
    execution: string, optional, default "immediate"
        One of "delayed" or "immediate". If "immediate", refresh column
        statistics as part of the run. If "delayed", flag the table for a
        deferred statistics update; column statistics may not be available
        for up to 24 hours. In addition, if existing_table_rows is "upsert",
        delayed executions move data from staging table to final table after a
        brief delay, in order to accommodate multiple concurrent imports to the
        same destination table.
    credential_id : str or int, optional
        The ID of the database credential.  If ``None``, the default
        credential will be used.
    polling_interval : int or float, optional
        Number of seconds to wait between checks for job completion.
    archive : bool, optional (deprecated)
        If ``True``, archive the import job as soon as it completes.
    hidden : bool, optional
        If ``True`` (the default), this job will not appear in the Civis UI.

    Returns
    -------
    results : :class:`~civis.futures.CivisFuture`
        A `CivisFuture` object.

    Notes
    -----
    This reads the contents of `filename` into memory.

    Examples
    --------
    >>> with open('input_file.csv', 'w') as _input:
    ...     _input.write('a,b,c\\n1,2,3')
    >>> fut = civis.io.csv_to_civis('input_file.csv',
    ...                             'my-database',
    ...                             'scratch.my_data')
    >>> fut.result()
    """
    if client is None:
        client = APIClient(api_key=api_key)
    if archive:
        warnings.warn("`archive` is deprecated and will be removed in v2.0.0. "
                      "Use `hidden` instead.", FutureWarning)

    name = path.basename(filename)
    with open(filename, "rb") as data:
        file_id = file_to_civis(data, name, client=client)
        log.debug('Uploaded file %s to Civis file %s', filename, file_id)
        fut = civis_file_to_table(file_id, database, table,
                                  client=client, max_errors=max_errors,
                                  existing_table_rows=existing_table_rows,
                                  diststyle=diststyle, distkey=distkey,
                                  sortkey1=sortkey1, sortkey2=sortkey2,
                                  delimiter=delimiter, headers=headers,
                                  credential_id=credential_id,
                                  primary_keys=primary_keys,
                                  last_modified_keys=last_modified_keys,
                                  escaped=escaped, execution=execution,
                                  polling_interval=polling_interval,
                                  hidden=hidden)
    return fut


@deprecate_param('v2.0.0', 'file_id')
def civis_file_to_table(file_id, database, table, client=None,
                        max_errors=None, existing_table_rows="fail",
                        diststyle=None, distkey=None,
                        sortkey1=None, sortkey2=None,
                        primary_keys=None, last_modified_keys=None,
                        escaped=False, execution="immediate",
                        delimiter=None, headers=None,
                        credential_id=None, polling_interval=None,
                        hidden=True):
    """Upload the contents of one or more Civis files to a Civis table.
       All provided files will be loaded as an atomic unit in parallel, and
       should share the same columns in the same order, and be in the same
       format.

    Parameters
    ----------
    file_id : int or list[int]
        Civis file ID or a list of Civis file IDs. Reference by name to this
        argument is deprecated, as the name will change in v2.0.0.
    database : str or int
        Upload data into this database. Can be the database name or ID.
    table : str
        The schema and table you want to upload to. E.g.,
        ``'scratch.table'``.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    max_errors : int, optional
        The maximum number of rows with errors to remove from the import
        before failing. If multiple files are provided, this limit applies
        across all files combined.
    existing_table_rows : str, optional
        The behaviour if a table with the requested name already exists.
        One of ``'fail'``, ``'truncate'``, ``'append'``, ``'drop'``, or
        ``'upsert'``. Defaults to ``'fail'``.
    diststyle : str, optional
        The distribution style for the table.
        One of ``'even'``, ``'all'`` or ``'key'``.
    distkey : str, optional
        The column to use as the distkey for the table.
    sortkey1 : str, optional
        The column to use as the sortkey for the table.
    sortkey2 : str, optional
        The second column in a compound sortkey for the table.
    primary_keys: list[str], optional
        A list of the primary key column(s) of the destination table that
        uniquely identify a record. If existing_table_rows is "upsert", this
        field is required. Note that this is true regardless of whether the
        destination database itself requires a primary key.
    last_modified_keys: list[str], optional
        A list of the columns indicating a record has been updated. If
        existing_table_rows is "upsert", this field is required.
    escaped: bool, optional
        A boolean value indicating whether or not the source file(s) escape
        quotes with a backslash. Defaults to false.
    execution: string, optional, default "immediate"
        One of "delayed" or "immediate". If "immediate", refresh column
        statistics as part of the run. If "delayed", flag the table for a
        deferred statistics update; column statistics may not be available
        for up to 24 hours. In addition, if existing_table_rows is "upsert",
        delayed executions move data from staging table to final table after a
        brief delay, in order to accommodate multiple concurrent imports to the
        same destination table.
    delimiter : string, optional
        The column delimiter. One of ``','``, ``'\\t'`` or ``'|'``. If not
        provided, will attempt to auto-detect.
    headers : bool, optional
        Whether or not the first row of the file should be treated as
        headers. The default, ``None``, attempts to autodetect whether
        or not the first row contains headers.
    credential_id : str or int, optional
        The ID of the database credential.  If ``None``, the default
        credential will be used.
    polling_interval : int or float, optional
        Number of seconds to wait between checks for job completion.
    hidden : bool, optional
        If ``True`` (the default), this job will not appear in the Civis UI.

    Returns
    -------
    results : :class:`~civis.futures.CivisFuture`
        A `CivisFuture` object.

    Raises
    ------
    CivisImportError
        If multiple files are given and determined to be incompatible for
        import. This may be the case if their columns have different types,
        their delimiters are different, headers are present in some but not
        others, or compressions do not match.

    Examples
    --------
    >>> file_id = 100
    >>> fut = civis.io.civis_file_to_table(file_id,
    ...                                    'my-database',
    ...                                    'scratch.my_data')
    >>> fut.result()
    """
    if client is None:
        client = APIClient()

    schema, table = split_schema_tablename(table)
    if isinstance(file_id, int):
        file_id = [file_id]
    if schema is None:
        raise ValueError("Provide a schema as part of the `table` input.")
    db_id = client.get_database_id(database)
    cred_id = credential_id or client.default_credential
    if delimiter is not None:  # i.e. it was provided as an argument
        delimiter = DELIMITERS.get(delimiter)
        assert delimiter, "delimiter must be one of {}".format(
            DELIMITERS.keys()
        )

    try:
        client.get_table_id(table, database)
        log.debug('Table {table} already exists - skipping column '
                  'detection'.format(table=table))
        table_exists = True
    except ValueError:
        table_exists = False

    # Use Preprocess endpoint to get the table columns as needed
    # and perform necessary file cleaning
    need_table_columns = not table_exists or existing_table_rows == 'drop'

    cleaning_futures = _run_cleaning(file_id, client, need_table_columns,
                                     headers, delimiter, hidden)

    (cleaned_file_ids, headers, compression, delimiter,
     table_columns) = _process_cleaning_results(
        cleaning_futures, client, headers, need_table_columns, delimiter
    )

    source = dict(file_ids=cleaned_file_ids)
    destination = dict(schema=schema, table=table, remote_host_id=db_id,
                       credential_id=cred_id, primary_keys=primary_keys,
                       last_modified_keys=last_modified_keys)

    redshift_options = dict(distkey=distkey, sortkeys=[sortkey1, sortkey2],
                            diststyle=diststyle)

    # If multiple files are being imported, there might be differences in
    # their precisions/lengths - setting this option will allow the Civis API
    # to increase these values for the data types provided, and decreases the
    # risk of a length-related import failure
    loosen_types = len(file_id) > 1

    import_name = 'CSV import to {}.{}'.format(schema, table)
    import_job = client.imports.post_files_csv(
        source,
        destination,
        headers,
        name=import_name,
        max_errors=max_errors,
        existing_table_rows=existing_table_rows,
        column_delimiter=delimiter,
        compression=compression,
        escaped=escaped,
        execution=execution,
        loosen_types=loosen_types,
        table_columns=table_columns,
        redshift_destination_options=redshift_options
    )
    fut = run_job(import_job.id, client=client,
                  polling_interval=polling_interval)
    log.debug('Started run %d for import %d', fut.run_id, import_job.id)
    return fut


def _sql_script(client, sql, database, job_name, credential_id, hidden=False,
                csv_settings=None):
    job_name = maybe_get_random_name(job_name)
    db_id = client.get_database_id(database)
    credential_id = credential_id or client.default_credential
    csv_settings = csv_settings or {}

    export_job = client.scripts.post_sql(job_name,
                                         remote_host_id=db_id,
                                         credential_id=credential_id,
                                         sql=sql,
                                         hidden=hidden,
                                         csv_settings=csv_settings)

    run_job = client.scripts.post_sql_runs(export_job.id)
    log.debug('Started run %d of SQL script %d', run_job.id, export_job.id)
    return export_job.id, run_job.id


def _get_sql_select(table, columns=None):
    if columns and not isinstance(columns, (list, tuple)):
        raise TypeError("columns must be a list, tuple or None")
    select = ", ".join(columns) if columns is not None else "*"
    sql = "select {} from {}".format(select, table)
    return sql


def _get_headers(client, sql, database, credential_id, polling_interval=None):
    headers = None

    try:
        # use 'begin read only;' to ensure we can't change state
        sql = 'begin read only; select * from ({}) limit 1'.format(sql)
        fut = query_civis(sql, database, client=client,
                          credential_id=credential_id,
                          polling_interval=polling_interval)
        headers = fut.result()['result_columns']
    except Exception as exc:  # NOQA
        log.debug("Failed to retrieve headers due to %s", str(exc))

    return headers


def _decompress_stream(response, buf, write_bytes=True):

    # use response.raw for a more consistent approach
    # if content-encoding is specified in the headers
    # then response.iter_content will decompress the stream
    # however, our use of content-encoding is inconsistent
    chunk = response.raw.read(CHUNK_SIZE)
    d = zlib.decompressobj(zlib.MAX_WBITS | 32)

    while chunk or d.unused_data:
        if d.unused_data:
            to_decompress = d.unused_data + chunk
            d = zlib.decompressobj(zlib.MAX_WBITS | 32)
        else:
            to_decompress = d.unconsumed_tail + chunk
        if write_bytes:
            buf.write(d.decompress(to_decompress))
        else:
            buf.write(d.decompress(to_decompress).decode('utf-8'))
        chunk = response.raw.read(CHUNK_SIZE)


def _download_file(url, local_path, headers, compression):
    response = requests.get(url, stream=True)
    response.raise_for_status()

    # gzipped buffers can be concatenated so write headers as gzip
    if compression == 'gzip':
        with gzip.open(local_path, 'wb') as fout:
            fout.write(headers)
        with open(local_path, 'ab') as fout:
            shutil.copyfileobj(response.raw, fout, CHUNK_SIZE)

    # write headers and decompress the stream
    elif compression == 'none':
        with open(local_path, 'wb') as fout:
            fout.write(headers)
            _decompress_stream(response, fout)

    # decompress the stream, write headers, and zip the file
    elif compression == 'zip':
        with TemporaryDirectory() as tmp_dir:
            tmp_path = path.join(tmp_dir, 'civis_to_csv.csv')
            with open(tmp_path, 'wb') as tmp_file:
                tmp_file.write(headers)
                _decompress_stream(response, tmp_file)

            with zipfile.ZipFile(local_path, 'w') as fout:
                arcname = path.basename(local_path)
                if arcname.split('.')[-1] == 'zip':
                    arcname = arcname.split('.')[0] + '.csv'
                fout.write(tmp_path, arcname, zipfile.ZIP_DEFLATED)


def _download_callback(job_id, run_id, filename, headers, compression):

    def callback(future):
        if not future.succeeded():
            return
        outputs = future.result().get("output")
        if not outputs:
            warnings.warn("Job %s, run %s does not have any output to "
                          "download. Not creating file %s."
                          % (job_id, run_id, filename),
                          RuntimeWarning)
            return
        else:
            url = outputs[0]["path"]
            file_id = outputs[0]["file_id"]
            log.debug('Exported results to Civis file %s', file_id)
            return _download_file(url, filename, headers, compression)

    return callback


def split_schema_tablename(table):
    """Split a Redshift 'schema.tablename' string

    Remember that special characters (such as '.') can only
    be included in a schema or table name if delimited by double-quotes.

    Parameters
    ----------
    table: str
        Either a Redshift schema and table name combined
        with a ".", or else a single table name.

    Returns
    -------
    schema, tablename
        A 2-tuple of strings. The ``schema`` may be None if the input
        is only a table name, but the ``tablename`` will always be filled.

    Raises
    ------
    ValueError
        If the input ``table`` is not separable into a schema and
        table name.
    """
    reader = csv.reader(StringIO(six.text_type(table)),
                        delimiter=".",
                        doublequote=True,
                        quotechar='"')
    schema_name_tup = next(reader)
    if len(schema_name_tup) == 1:
        schema_name_tup = (None, schema_name_tup[0])
    if len(schema_name_tup) != 2:
        raise ValueError("Cannot parse schema and table. "
                         "Does '{}' follow the pattern 'schema.table'?"
                         .format(table))
    return tuple(schema_name_tup)


def _replace_null_column_names(column_list):
    """Replace null names in columns from file cleaning with an appropriately
    blank column name.

    Parameters
    ----------
    column_list: list[dict]
      the list of columns from file cleaning.

    Returns
    --------
    column_list: list[dict]
    """

    new_cols = []
    for i, col in enumerate(column_list):
        # Avoid mutating input arguments
        new_col = dict(col)
        if new_col.get('name') is None:
            new_col['name'] = 'column_{}'.format(i)
        new_cols.append(new_col)
    return new_cols


def _run_cleaning(file_ids, client, need_table_columns, headers, delimiter,
                  hidden, polling_interval=None):
    cleaning_futures = []
    for fid in file_ids:
        cleaner_job = client.files.post_preprocess_csv(
            file_id=fid,
            in_place=False,
            detect_table_columns=need_table_columns,
            force_character_set_conversion=True,
            include_header=headers,
            column_delimiter=delimiter,
            hidden=hidden
        )
        cleaning_futures.append(run_job(cleaner_job.id, client=client,
                                        polling_interval=polling_interval))
    return cleaning_futures


def _check_all_detected_info(detected_info, headers, delimiter,
                             compression, output_file_id):
    """Check a single round of cleaning results as compared to provided values.

    Parameters
    ----------
    detected_info: Dict[str, Any]
      The detected info of the file as returned by the Civis API.
    headers: bool
      The provided value for whether or not the file contains errors.
    delimiter: str
      The provided value for the file delimiter.
    compression: str
      The provided value for the file compression.
    output_file_id: int
      The cleaned file's Civis ID. Used for debugging.

    Raises
    ------
    CivisImportError
      If the values detected on the file do not match their expected
      attributes.
    """
    if headers != detected_info['includeHeader']:
        raise CivisImportError('Mismatch between detected headers - '
                               'please ensure all imported files either '
                               'have a header or do not.')
    if delimiter != detected_info['columnDelimiter']:
        raise CivisImportError('Provided delimiter "{}" does not match '
                               'detected delimiter for {}: "{}"'.format(
                                    delimiter,
                                    output_file_id,
                                    detected_info["columnDelimiter"])
                               )
    if compression != detected_info['compression']:
        raise CivisImportError('Mismatch between detected and provided '
                               'compressions - provided compression was {}'
                               ' but detected compression {}. Please '
                               'ensure all imported files have the same '
                               'compression.'.format(
                                   compression,
                                   detected_info['compression'])
                               )


def _process_cleaning_results(cleaning_futures, client, headers,
                              need_table_columns, delimiter):
    cleaned_file_ids = []

    done, still_going = concurrent.futures.wait(
        cleaning_futures, return_when=concurrent.futures.FIRST_COMPLETED
    )

    # Set values from first completed file cleaning - other files will be
    # compared to this one. If inconsistencies are detected, raise an error.
    first_completed = done.pop()
    output_file = client.jobs.list_runs_outputs(
            first_completed.job_id,
            first_completed.run_id
        )[0]
    detected_info = client.files.get(output_file.object_id).detected_info
    table_columns = (detected_info['tableColumns'] if need_table_columns
                     else None)
    if headers is None:
        headers = detected_info['includeHeader']
    if delimiter is None:
        delimiter = detected_info['columnDelimiter']

    compression = detected_info['compression']

    _check_all_detected_info(detected_info, headers, delimiter, compression,
                             output_file.object_id)

    cleaned_file_ids.append(output_file.object_id)

    # Ensure that all results from files are correctly accounted for -
    # Since concurrent.futures.wait returns two sets, it is possible
    # That done contains more than one Future. Thus it is necessary to account
    # for these possible completed cleaning runs while waiting on those which
    # are still running.
    for result in concurrent.futures.as_completed(done | still_going):
        output_file = client.jobs.list_runs_outputs(
            result.job_id,
            result.run_id
        )[0]
        detected_info = client.files.get(output_file.object_id).detected_info
        if need_table_columns:
            file_columns = detected_info['tableColumns']
            _check_column_types(table_columns, file_columns,
                                output_file.object_id)

        _check_all_detected_info(detected_info, headers, delimiter,
                                 compression, output_file.object_id)
        cleaned_file_ids.append(output_file.object_id)

    if need_table_columns:
        table_columns = _replace_null_column_names(table_columns)
    return cleaned_file_ids, headers, compression, delimiter, table_columns


def _check_column_types(table_columns, file_columns, output_obj_id):
    """Check that base column types match those current defined for the table.

    Parameters
    ----------

    table_columns: List[Dict[str, str]]
      The columns for the table to be created.
    file_columns: List[Dict[str, str]]
      The columns detected by the Civis API for the file.
    output_obj_id: int
      The file ID under consideration; used for error messaging.

    Raises
    ------

    CivisImportError
      If the table columns and the file columns have a type mismatch, or
      differ in count.
    """
    if len(table_columns) != len(file_columns):
        raise CivisImportError('All files should have the same number of '
                               'columns. Expected {} columns but file {} '
                               'has {} columns'.format(
                                   len(table_columns),
                                   output_obj_id,
                                   len(file_columns))
                               )

    error_msgs = []
    for idx, (tcol, fcol) in enumerate(zip(table_columns, file_columns)):
        # for the purposes of type checking, we care only that the types
        # share a base type (e.g. INT, VARCHAR, DECIMAl) rather than that
        # they have the same precision and length
        # (e.g VARCHAR(42), DECIMAL(8, 10))
        tcol_base_type = tcol['sql_type'].split('(', 1)[0]
        fcol_base_type = fcol['sql_type'].split('(', 1)[0]

        if tcol_base_type != fcol_base_type:
            error_msgs.append(
                'Column {}: File base type was {}, but expected {}'.format(
                    idx,
                    fcol_base_type,
                    tcol_base_type
                )
            )
    if error_msgs:
        raise CivisImportError(
            'Encountered the following errors for file {}:\n\t{}'.format(
                output_obj_id,
                '\n\t'.join(error_msgs)
            )
        )
