import json
import csv
from os import path
import io
import logging
import shutil
import six
import tempfile
import warnings
import zlib

import gzip
import zipfile

from civis import APIClient
from civis.io import civis_to_file, file_to_civis, query_civis
from civis._utils import maybe_get_random_name
from civis.base import EmptyResultError
from civis.futures import CivisFuture
from civis.utils._deprecation import deprecate_param

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

log = logging.getLogger(__name__)
__all__ = ['read_civis', 'read_civis_sql', 'civis_to_csv',
           'civis_to_multifile_csv', 'dataframe_to_civis', 'csv_to_civis',
           'civis_file_to_table']

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
        Name of table, including schema, in the database. I.e.
        ``'my_schema.my_table'``.
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
    """
    if use_pandas and NO_PANDAS:
        raise ImportError("use_pandas is True but pandas is not installed.")
    if archive:
        warnings.warn("`archive` is deprecated and will be removed in v2.0.0. "
                      "Use `hidden` instead.", FutureWarning)
    if client is None:
        # Instantiate client here in case users provide a (deprecated) api_key
        client = APIClient(api_key=api_key, resources='all')
    sql = _get_sql_select(table, columns)
    data = read_civis_sql(sql=sql, database=database, use_pandas=use_pandas,
                          job_name=job_name, client=client,
                          credential_id=credential_id,
                          polling_interval=polling_interval,
                          archive=archive, hidden=hidden, **kwargs)
    return data


@deprecate_param('v2.0.0', 'api_key')
def read_civis_sql(sql, database, use_pandas=False, job_name=None,
                   api_key=None, client=None, credential_id=None,
                   polling_interval=None, archive=False,
                   hidden=True, **kwargs):
    """Read data from Civis using a custom SQL string.

    Parameters
    ----------
    sql : str, optional
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
        client = APIClient(api_key=api_key, resources='all')
    if use_pandas and NO_PANDAS:
        raise ImportError("use_pandas is True but pandas is not installed.")
    if archive:
        warnings.warn("`archive` is deprecated and will be removed in v2.0.0. "
                      "Use `hidden` instead.", FutureWarning)

    if isinstance(database, str):
        database = client.get_database_id(database)
    credential_id = credential_id or client.default_credential

    delimiter = ','
    headers = _get_headers(client, sql, database, credential_id)

    csv_settings = dict(include_header=False,
                        compression='gzip',
                        column_delimiter=delimiter,
                        filename_prefix=None,
                        force_multifile=False)

    script_id, run_id = _sql_script(client, sql, database,
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
    log.info('Exported results to Civis file %s (%s)',
             outputs[0]["output_name"], file_id)

    if use_pandas:
        data = pd.read_csv(url, names=headers, compression='gzip', **kwargs)
    else:
        r = requests.get(url)
        r.raise_for_status()
        headers = delimiter.join(headers) + '\n'
        raw_data = zlib.decompress(r.content, zlib.MAX_WBITS | 32)
        raw_data = StringIO(headers + raw_data.decode('utf-8'))
        data = list(csv.reader(raw_data, **kwargs))
    return data


@deprecate_param('v2.0.0', 'api_key')
def civis_to_csv(filename, sql, database, job_name=None, api_key=None,
                 client=None, credential_id=None, include_header=True,
                 compression='none', delimiter=',', unquoted=False,
                 archive=False, hidden=True, polling_interval=None):
    """Export data from Civis to a local CSV file.

    Parameters
    ----------
    filename : str
        Download exported data into this file.
    sql : str, optional
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
        ``'gzip'``. Default ``'none'``.
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
    """
    if archive:
        warnings.warn("`archive` is deprecated and will be removed in v2.0.0. "
                      "Use `hidden` instead.", FutureWarning)
    if client is None:
        client = APIClient(api_key=api_key, resources='all')

    if isinstance(database, str):
        database = client.get_database_id(database)
    credential_id = credential_id or client.default_credential

    if include_header:
        headers = _get_headers(client, sql, database, credential_id)
        headers = delimiter.join(headers) + '\n'
        headers = headers.encode('utf-8')
    else:
        headers = b''

    delimiter = DELIMITERS.get(delimiter)
    if not delimiter:
        raise ValueError("delimiter must be one of {}"
                         .format(DELIMITERS.keys()))

    csv_settings = dict(include_header=False,
                        compression='gzip',
                        column_delimiter=delimiter,
                        unquoted=unquoted,
                        filename_prefix=None,
                        force_multifile=False)

    script_id, run_id = _sql_script(client, sql, database, job_name,
                                    credential_id, hidden=hidden,
                                    csv_settings=csv_settings)
    fut = CivisFuture(client.scripts.get_sql_runs, (script_id, run_id),
                      polling_interval=polling_interval, client=client,
                      poll_on_creation=False)
    download = _download_callback(script_id, run_id, client, filename,
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
                           unquoted=False, prefix=None,
                           polling_interval=None, hidden=True):
    """Unload the result of SQL query and return presigned urls.

    This function is intended for unloading large queries/tables from redshift
    as it uses a 'PARALLEL ON' S3 unload. It returns a similar manifest file
    to conventional S3 UNLOAD statements except the CSV parts are accessible
    via both files endpoint IDs and presigned S3 urls.

    Parameters
    ----------
    sql : str, optional
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
        client = APIClient(api_key=api_key, resources='all')
    delimiter = DELIMITERS.get(delimiter)
    assert delimiter, "delimiter must be one of {}".format(DELIMITERS.keys())

    csv_settings = dict(include_header=include_header,
                        compression=compression,
                        column_delimiter=delimiter,
                        unquoted=unquoted,
                        filename_prefix=prefix,
                        force_multifile=True)
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
    civis_to_file(outputs[0]['file_id'], buf)
    txt = io.TextIOWrapper(buf, encoding='utf-8')
    txt.seek(0)
    unload_manifest = json.load(txt)

    return unload_manifest


@deprecate_param('v2.0.0', 'api_key')
def dataframe_to_civis(df, database, table, api_key=None, client=None,
                       max_errors=None, existing_table_rows="fail",
                       distkey=None, sortkey1=None, sortkey2=None,
                       headers=None, credential_id=None,
                       polling_interval=None,
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
        One of ``'fail'``, ``'truncate'``, ``'append'`` or ``'drop'``.
        Defaults to ``'fail'``.
    distkey : str, optional
        The column to use as the distkey for the table.
    sortkey1 : str, optional
        The column to use as the sortkey for the table.
    sortkey2 : str, optional
        The second column in a compound sortkey for the table.
    headers : bool, optional
        Whether or not the first row of the file should be treated as
        headers. The default, ``None``, attempts to autodetect whether
        or not the first row contains headers.
    credential_id : str or int, optional
        The ID of the database credential.  If ``None``, the default
        credential will be used.
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
    """
    if client is None:
        client = APIClient(api_key=api_key, resources='all')
    if archive:
        warnings.warn("`archive` is deprecated and will be removed in v2.0.0. "
                      "Use `hidden` instead.", FutureWarning)
    buf = six.BytesIO()
    if six.PY3:
        txt = io.TextIOWrapper(buf, encoding='utf-8')
    else:
        txt = buf
    df.to_csv(txt, encoding='utf-8', index=False, **kwargs)
    txt.flush()
    buf.seek(0)
    delimiter = ','
    name = table.split('.')[-1]
    file_id = file_to_civis(buf, name, api_key=api_key, client=client)
    fut = civis_file_to_table(file_id, database, table,
                              client=client, max_errors=max_errors,
                              existing_table_rows=existing_table_rows,
                              distkey=distkey,
                              sortkey1=sortkey1, sortkey2=sortkey2,
                              delimiter=delimiter, headers=headers,
                              credential_id=credential_id,
                              polling_interval=polling_interval,
                              hidden=hidden)

    return fut


@deprecate_param('v2.0.0', 'api_key')
def csv_to_civis(filename, database, table, api_key=None, client=None,
                 max_errors=None, existing_table_rows="fail",
                 distkey=None, sortkey1=None, sortkey2=None,
                 delimiter=",", headers=None,
                 credential_id=None, polling_interval=None,
                 archive=False, hidden=True):
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
        One of ``'fail'``, ``'truncate'``, ``'append'`` or ``'drop'``.
        Defaults to ``'fail'``.
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
        client = APIClient(api_key=api_key, resources='all')
    if archive:
        warnings.warn("`archive` is deprecated and will be removed in v2.0.0. "
                      "Use `hidden` instead.", FutureWarning)

    name = path.basename(filename)
    with open(filename, "rb") as data:
        file_id = file_to_civis(data, name, api_key=api_key, client=client)
        log.info('Uploaded file %s to Civis file %s', filename, file_id)
        fut = civis_file_to_table(file_id, database, table,
                                  client=client, max_errors=max_errors,
                                  existing_table_rows=existing_table_rows,
                                  distkey=distkey,
                                  sortkey1=sortkey1, sortkey2=sortkey2,
                                  delimiter=delimiter, headers=headers,
                                  credential_id=credential_id,
                                  polling_interval=polling_interval,
                                  hidden=hidden)
    return fut


def civis_file_to_table(file_id, database, table, client=None,
                        max_errors=None, existing_table_rows="fail",
                        distkey=None, sortkey1=None, sortkey2=None,
                        delimiter=",", headers=None,
                        credential_id=None, polling_interval=None,
                        hidden=True):
    """Upload the contents of a Civis file to a Civis table.

    Parameters
    ----------
    file_id : int
        Civis file ID.
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
        before failing.
    existing_table_rows : str, optional
        The behaviour if a table with the requested name already exists.
        One of ``'fail'``, ``'truncate'``, ``'append'`` or ``'drop'``.
        Defaults to ``'fail'``.
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
        client = APIClient(resources='all')

    schema, table = table.split(".", 1)
    db_id = client.get_database_id(database)
    cred_id = credential_id or client.default_credential
    delimiter = DELIMITERS.get(delimiter)
    assert delimiter, "delimiter must be one of {}".format(DELIMITERS.keys())

    destination = dict(remote_host_id=db_id, credential_id=cred_id)
    import_name = 'CSV import to {}.{}'.format(schema, table)
    import_job = client.imports.post(import_name, 'AutoImport',
                                     is_outbound=False,
                                     destination=destination,
                                     hidden=hidden)

    options = dict(max_errors=max_errors,
                   existing_table_rows=existing_table_rows,
                   distkey=distkey, sortkey1=sortkey1, sortkey2=sortkey2,
                   column_delimiter=delimiter, first_row_is_header=headers)

    client.imports.post_syncs(
        import_job.id,
        source=dict(file=dict(id=file_id)),
        destination=dict(database_table=dict(schema=schema, table=table)),
        advanced_options=options)

    run = client.jobs.post_runs(import_job.id)
    fut = CivisFuture(client.jobs.get_runs,
                      (import_job.id, run['id']),
                      polling_interval=polling_interval,
                      client=client,
                      poll_on_creation=False)

    return fut


def _sql_script(client, sql, database, job_name, credential_id, hidden=False,
                csv_settings=None):
    job_name = maybe_get_random_name(job_name)
    if isinstance(database, str):
        database = client.get_database_id(database)
    credential_id = credential_id or client.default_credential
    csv_settings = csv_settings or {}
    export_job = client.scripts.post_sql(job_name,
                                         remote_host_id=database,
                                         credential_id=credential_id,
                                         sql=sql,
                                         hidden=hidden,
                                         csv_settings=csv_settings)
    run_job = client.scripts.post_sql_runs(export_job.id)
    return export_job.id, run_job.id


def _get_sql_select(table, columns=None):
    if columns and not isinstance(columns, (list, tuple)):
        raise TypeError("columns must be a list, tuple or None")
    select = ", ".join(columns) if columns is not None else "*"
    sql = "select {} from {}".format(select, table)
    return sql


def _get_headers(client, sql, database, credential_id):
    sql = 'select * from ({}) limit 1'.format(sql)
    fut = query_civis(sql, database, client=client,
                      credential_id=credential_id)
    return fut.result()['result_columns']


def _download_file(url, local_path, headers, compression):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    decompress = zlib.decompressobj(zlib.MAX_WBITS | 32)
    chunk_size = 32 * 1024

    def _write_file(buf):
        buf.write(headers)
        chunks = response.iter_content(chunk_size)
        for chunk in chunks:
            buf.write(decompress.decompress(chunk))

    def _read_file(buf):
        while True:
            data = buf.read(chunk_size)
            if not data:
                break
            yield data

    if compression == 'none':
        with open(local_path, 'wb') as fout:
            _write_file(fout)
            return

    with tempfile.NamedTemporaryFile() as temp:
        _write_file(temp)
        temp.flush()
        if compression == 'gzip':
            with open(temp.name, 'rb') as fin:
                with gzip.open(local_path, 'wb') as fout:
                    #shutil.copyfileobj(fin, fout, chunk_size)
                    for chunk in _read_file(fin):
                        fout.write(chunk)
        elif compression == 'zip':
            with zipfile.ZipFile(local_path, 'w') as fout:
                arcname = path.basename(local_path)
                if arcname.split('.')[-1] == 'zip':
                    arcname = arcname.split('.')[0] + '.csv'
                fout.write(temp.name, arcname, zipfile.ZIP_DEFLATED)


def _download_callback(job_id, run_id, client, filename, headers, compression):

    def callback(future):
        outputs = client.scripts.get_sql_runs(job_id, run_id)["output"]
        if not outputs:
            if future.succeeded():
                # Only warn if the job succeeded. Otherwise the user
                # will see an error surfaced through the Future.
                warnings.warn("Job %s, run %s does not have any output to "
                              "download. Not creating file %s."
                              % (job_id, run_id, filename),
                              RuntimeWarning)
            return
        else:
            url = outputs[0]["path"]
            file_id = outputs[0]["file_id"]
            output_name = outputs[0]["output_name"]
            log.info('Exported results to Civis file %s (%s)',
                     output_name, file_id)
            return _download_file(url, filename, headers, compression)

    return callback
