import io
import json
import warnings

from civis import APIClient
from civis.base import EmptyResultError
from civis.io import civis_to_file
from civis.io._tables import _sql_script, DELIMITERS
from civis.polling import PollableResult, _DEFAULT_POLLING_INTERVAL


def civis_multifile_unload(sql, database, job_name=None, api_key=None,
                           credential_id=None, include_header=True,
                           compression='none', delimiter='|',
                           unquoted=False, prefix=None,
                           polling_interval=_DEFAULT_POLLING_INTERVAL,
                           archive=False, hidden=True):
    """Unload the result of SQL query and return presigned urls.

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
    api_key : str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    credential_id : str or int, optional
        The database credential ID.  If ``None``, the default credential
        will be used.
    include_header: bool, optional
        If ``True`` include a key in the returned dictionary containing a list
        of column names. Default: ``True``.
    compression: str, optional
        Type of compression to use, if any. One of ``'none'``, ``'zip'``, or
        ``'gzip'``. Default ``'gzip'``.
    delimiter, str: optional
        Which delimiter to use, if any. One of ``','``, ``'\t'``, or
        ``'|'``. Default: ``','``.
    unquoted: bool, optional
        Where or not to quote fields. Default: ``False``.
    prefix: str, optional
        A user specified filename prefix for the output file to have. Default:
        ``None``.
    polling_interval : int or float, optional
        Number of seconds to wait between checks for query completion.
    archive : bool, optional (deprecated)
        If ``True``, archive the import job as soon as it completes.
    hidden : bool, optional
        If ``True`` (the default), this job will not appear in the Civis UI.

    Returns
    -------
    unload_manifest: dict
        A dictionary resembling an AWS manifest file. Has the following keys:
        ``'header'``, ``'query'``, ``'files'``, respresenting the columns from
        the query, the query itself, and a list of dictionaries for each
        unloaded CSV part, each containing its file ``'id'``, ``'name'``,
        ``'size'``, and a presigned S3 ``'url'``.

    Examples
    --------
    >>> sql = "SELECT * FROM schema.my_big_table"
    >>> database = "my_database"
    >>> delimiter = "|"
    >>> manifest = civis_multipart_unload(sql, database, delimiter=delimiter)
    >>> ids = [file['id'] for file in manifest['files']]
    >>> buf = BytesIO()
    >>> civis_to_file(ids[0], buf)
    >>> buf.seek(0)
    >>> df = pd.read_csv(buf, delimiter=delimiter)

    See Also
    --------
    civis.APIClient.scripts.post_sql
    """
    if archive:
        warnings.warn("`archive` is deprecated and will be removed in v2.0.0. "
                      "Use `hidden` instead.", FutureWarning)
    delimiter = DELIMITERS.get(delimiter)
    assert delimiter, "delimiter must be one of {}".format(DELIMITERS.keys())

    client = APIClient(api_key=api_key)

    csv_settings = dict(include_header=include_header,
                        compression=compression,
                        column_delimiter=delimiter,
                        unquoted=unquoted,
                        filename_prefix=prefix,
                        force_multifile=True)
    script_id, run_id = _sql_script(client, sql, database, job_name,
                                    credential_id, hidden,
                                    csv_settings=csv_settings)

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
        raise EmptyResultError("Unload query {} returned no manifest."
                               .format(script_id))

    buf = io.BytesIO()
    civis_to_file(outputs[0]['file_id'], buf)
    txt = io.TextIOWrapper(buf, encoding='utf-8')
    txt.seek(0)
    unload_manifest = json.loads(txt.read())

    return unload_manifest
