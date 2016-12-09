import csv
import io

from civis import APIClient
from civis._utils import maybe_get_random_name
from civis.base import EmptyResultError
from civis.polling import PollableResult, _DEFAULT_POLLING_INTERVAL
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


DELIMITERS = {
    ',': 'comma',
    '\t': 'tab',
    '|': 'pipe',
}


def read_civis(table, database, columns=None, use_pandas=False,
               job_name=None, api_key=None, credential_id=None,
               polling_interval=_DEFAULT_POLLING_INTERVAL,
               archive=True, **kwargs):
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
    api_key : str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    credential_id : str or int, optional
        The database credential ID.  If ``None``, the default credential
        will be used.
    polling_interval : int or float, optional
        Number of seconds to wait between checks for query completion.
    archive : bool, optional
        If ``True`` (the default), archive the export job as soon as it
        completes.
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
    sql = _get_sql_select(table, columns)
    data = read_civis_sql(sql=sql, database=database, use_pandas=use_pandas,
                          job_name=job_name, api_key=api_key,
                          credential_id=credential_id,
                          polling_interval=polling_interval,
                          archive=archive, **kwargs)
    return data


def read_civis_sql(sql, database, use_pandas=False, job_name=None,
                   api_key=None, credential_id=None,
                   polling_interval=_DEFAULT_POLLING_INTERVAL,
                   archive=True, **kwargs):
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
    api_key : str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    credential_id : str or int, optional
        The database credential ID.  If ``None``, the default credential
        will be used.
    polling_interval : int or float, optional
        Number of seconds to wait between checks for query completion.
    archive : bool, optional
        If ``True`` (the default), archive the export job as soon as it
        completes.
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
    if use_pandas and NO_PANDAS:
        raise ImportError("use_pandas is True but pandas is not installed.")
    client = APIClient(api_key=api_key)
    script_id, run_id = _sql_script(client, sql, database,
                                    job_name, credential_id)
    poll = PollableResult(client.scripts.get_sql_runs,
                          (script_id, run_id),
                          polling_interval)
    if archive:

        def f(x):
            return client.scripts.put_sql_archive(script_id, True)

        poll.add_done_callback(f)
    poll.result()
    outputs = client.scripts.get_sql_runs(script_id, run_id)["output"]
    if not outputs:
        raise EmptyResultError("Query {} returned no output."
                               .format(script_id))
    url = outputs[0]["path"]
    if use_pandas:
        data = pd.read_csv(url, **kwargs)
    else:
        r = requests.get(url)
        r.raise_for_status()
        data = list(csv.reader(StringIO(r.text), **kwargs))
    return data


def civis_to_csv(filename, sql, database, job_name=None, api_key=None,
                 credential_id=None,
                 polling_interval=_DEFAULT_POLLING_INTERVAL, archive=True):
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
    api_key : str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    credential_id : str or int, optional
        The ID of the database credential.  If ``None``, the default
        credential will be used.
    polling_interval : int or float, optional
        Number of seconds to wait between checks for query completion.
    archive : bool, optional
        If ``True`` (the default), archive the export job as soon as it
        completes.

    Returns
    -------
    results : :class:`~civis.polling.PollableResult`
        A `PollableResult` object.

    Examples
    --------
    >>> sql = "SELECT * FROM schema.table"
    >>> poll = civis_to_csv("file.csv", sql, "my_database")
    >>> poll.result()  # Wait for job to complete

    See Also
    --------
    civis.io.read_civis : Read table contents into memory.
    civis.io.read_civis_sql : Read results of a SQL query into memory.
    """
    client = APIClient(api_key=api_key)
    script_id, run_id = _sql_script(client, sql, database,
                                    job_name, credential_id)
    poll = PollableResult(client.scripts.get_sql_runs,
                          (script_id, run_id),
                          polling_interval)
    download = _download_callback(script_id, run_id, client, filename)
    poll.add_done_callback(download)
    if archive:

        def f(x):
            return client.scripts.put_sql_archive(script_id, True)

        poll.add_done_callback(f)

    return poll


def dataframe_to_civis(df, database, table, api_key=None,
                       max_errors=None, existing_table_rows="fail",
                       distkey=None, sortkey1=None, sortkey2=None,
                       headers=None, credential_id=None,
                       polling_interval=_DEFAULT_POLLING_INTERVAL,
                       archive=True, **kwargs):
    """Upload a `pandas` `DataFrame` into a Civis table.

    Parameters
    ----------
    df : :class:`pandas:pandas.DataFrame`
        The `DataFrame` to upload to Civis.
    database : str or int
        Upload data into this database. Can be the database name or ID.
    table : str
        The schema and table you want to upload to. E.g.,
        ``'scratch.table'``.
    api_key : str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
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
    archive : bool, optional
        If ``True`` (the default), archive the import job as soon as it
        completes.
    **kwargs : kwargs
        Extra keyword arguments will be passed to
        :meth:`pandas:pandas.DataFrame.to_csv`.

    Returns
    -------
    poll : :class:`~civis.polling.PollableResult`
        A `PollableResult` object.

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    >>> poller = civis.io.dataframe_to_civis(df, 'my-database',
    ...                                      'scratch.df_table')
    >>> poller.result()
    """
    buf = io.BytesIO()
    txt = io.TextIOWrapper(buf, encoding='utf-8')
    df.to_csv(txt, encoding='utf-8', index=False, **kwargs)
    txt.flush()
    buf.seek(0)
    delimiter = ','
    return _import_bytes(buf, database, table, api_key, max_errors,
                         existing_table_rows, distkey, sortkey1, sortkey2,
                         delimiter, headers, credential_id, polling_interval,
                         archive)


def csv_to_civis(filename, database, table, api_key=None,
                 max_errors=None, existing_table_rows="fail",
                 distkey=None, sortkey1=None, sortkey2=None,
                 delimiter=",", headers=None,
                 credential_id=None,
                 polling_interval=_DEFAULT_POLLING_INTERVAL,
                 archive=True):
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
    api_key : str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
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
    archive : bool, optional
        If ``True`` (the default), archive the import job as soon as it
        completes.

    Returns
    -------
    results : :class:`~civis.polling.PollableResult`
        A `PollableResult` object.

    Notes
    -----
    This reads the contents of `filename` into memory.

    Examples
    --------
    >>> with open('input_file.csv', 'w') as _input:
    ...     _input.write('a,b,c\\n1,2,3')
    >>> poller = civis.io.csv_to_civis('input_file.csv',
    ...                                'my-database',
    ...                                'scratch.my_data')
    >>> poller.result()
    """
    with open(filename, "rb") as data:
        poll = _import_bytes(data, database, table, api_key, max_errors,
                             existing_table_rows, distkey, sortkey1, sortkey2,
                             delimiter, headers, credential_id,
                             polling_interval, archive)
    return poll


def _sql_script(client, sql, database, job_name, credential_id):
    job_name = maybe_get_random_name(job_name)
    db_id = client.get_database_id(database)
    cred_id = credential_id or client.default_credential
    export_job = client.scripts.post_sql(job_name,
                                         remote_host_id=db_id,
                                         credential_id=cred_id,
                                         sql=sql)
    run_job = client.scripts.post_sql_runs(export_job.id)
    return export_job.id, run_job.id


def _get_sql_select(table, columns=None):
    if columns and not isinstance(columns, (list, tuple)):
        raise TypeError("columns must be a list, tuple or None")
    select = ", ".join(columns) if columns is not None else "*"
    sql = "select {} from {}".format(select, table)
    return sql


def _download_file(url, local_path):
    response = requests.get(url, stream=True)
    response.raise_for_status()

    chunk_size = 32 * 1024
    with open(local_path, 'wb') as fout:
        chunk = response.iter_content(chunk_size)
        for lines in chunk:
            fout.write(lines)


def _download_callback(job_id, run_id, client, filename):

    def callback(future):
        url = client.scripts.get_sql_runs(job_id, run_id)["output"][0]["path"]
        return _download_file(url, filename)

    return callback


def _import_bytes(buf, database, table, api_key, max_errors,
                  existing_table_rows, distkey, sortkey1, sortkey2, delimiter,
                  headers, credential_id, polling_interval, archive):
    client = APIClient(api_key=api_key)
    schema, table = table.split(".", 1)
    db_id = client.get_database_id(database)
    cred_id = credential_id or client.default_credential
    delimiter = DELIMITERS.get(delimiter)
    assert delimiter, "delimiter must be one of {}".format(DELIMITERS.keys())

    kwargs = dict(schema=schema, name=table, remote_host_id=db_id,
                  credential_id=cred_id, max_errors=max_errors,
                  existing_table_rows=existing_table_rows, distkey=distkey,
                  sortkey1=sortkey1, sortkey2=sortkey2,
                  column_delimiter=delimiter, first_row_is_header=headers)

    import_job = client.imports.post_files(**kwargs)
    put_response = requests.put(import_job.upload_uri, buf)

    put_response.raise_for_status()
    run_job_result = client._session.post(import_job.run_uri)
    run_job_result.raise_for_status()
    run_info = run_job_result.json()
    poll = PollableResult(client.imports.get_files_runs,
                          (run_info['importId'], run_info['id']),
                          polling_interval=polling_interval)
    if archive:

        def f(x):
            return client.imports.put_archive(import_job.id, True)

        poll.add_done_callback(f)
    return poll
