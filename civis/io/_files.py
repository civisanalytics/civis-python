from collections import OrderedDict
import io
from functools import partial
import json
import logging
import math
from multiprocessing.dummy import Pool
import os
import re
import shutil
from tempfile import TemporaryDirectory

import requests
from requests import HTTPError

from civis import APIClient, find_one
from civis.base import CivisAPIError, EmptyResultError
from civis._deprecation import deprecate_param
from civis._utils import BufferedPartialReader, retry

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

MIN_MULTIPART_SIZE = 50 * 2 ** 20  # 50MB
MIN_PART_SIZE = 5 * 2 ** 20  # 5MB
MAX_PART_SIZE = 5 * 2 ** 30  # 5GB
MAX_FILE_SIZE = 5 * 2 ** 40  # 5TB
MAX_THREADS = 4

RETRY_EXCEPTIONS = (requests.HTTPError,
                    requests.ConnectionError,
                    requests.ConnectTimeout)

log = logging.getLogger(__name__)
# standard chunk size; provides good performance across various buffer sizes
CHUNK_SIZE = 32 * 1024
__all__ = ['file_to_civis', 'civis_to_file', 'file_id_from_run_output',
           'file_to_dataframe', 'dataframe_to_file',
           'file_to_json', 'json_to_file']


def _get_aws_error_message(response):
    # Amazon gives back informative error messages
    # http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
    # NOTE: This is cribbed from response.raise_for_status with AWS
    # message appended
    msg = ''

    if 400 <= response.status_code < 500:
        msg = '%s Client Error: %s for url: %s' % (response.status_code,
                                                   response.reason,
                                                   response.url)

    elif 500 <= response.status_code < 600:
        msg = '%s Server Error: %s for url: %s' % (response.status_code,
                                                   response.reason,
                                                   response.url)

    msg += '\nAWS Content: %s' % response.content

    return msg


def _buf_len(buf):
    if hasattr(buf, '__len__'):
        return len(buf)

    if hasattr(buf, 'len'):
        return buf.len

    if hasattr(buf, 'fileno'):
        try:
            fileno = buf.fileno()
        except io.UnsupportedOperation:
            pass
        else:
            return os.fstat(fileno).st_size

    if hasattr(buf, 'getvalue'):
        # e.g. BytesIO, cStringIO.StringIO
        return len(buf.getvalue())

    return None


def _single_upload(buf, name, client, **kwargs):
    file_response = client.files.post(name, **kwargs)

    # Platform has given us a URL to which we can upload a file.
    # The file must be uploaded with a POST formatted as per
    # http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-post-example.html
    # Note that the payload must have "key" first and "file" last.
    url = file_response.upload_url
    form = file_response.upload_fields
    form_key = OrderedDict(key=form.pop('key'))
    form_key.update(form)

    # Store the current buffer position in case we need to retry below.
    buf_orig_position = buf.tell()

    @retry(RETRY_EXCEPTIONS)
    def _post():
        # Reset the buffer in case we had to retry.
        buf.seek(buf_orig_position)

        form_key['file'] = buf
        # requests will not stream multipart/form-data, but _single_upload
        # is only used for small file objects or non-seekable file objects
        # which can't be streamed with using requests-toolbelt anyway
        response = requests.post(url, files=form_key, timeout=60)

        if not response.ok:
            msg = _get_aws_error_message(response)
            raise HTTPError(msg, response=response)

    _post()

    log.debug('Uploaded File %d', file_response.id)
    return file_response.id


def _multipart_upload(buf, name, file_size, client, **kwargs):
    # scale the part size based on file size
    part_size = max(int(math.sqrt(MIN_PART_SIZE) * math.sqrt(file_size)),
                    MIN_PART_SIZE)
    num_parts = int(math.ceil((file_size) / float(part_size)))

    log.debug('Uploading file with %s bytes using %s file parts with a part '
              'size of %s bytes', file_size, num_parts, part_size)
    file_response = client.files.post_multipart(name=name, num_parts=num_parts,
                                                **kwargs)

    # Platform will give us a URL for each file part
    urls = file_response.upload_urls
    if num_parts != len(urls):
        raise ValueError(
            f"There are {num_parts} file parts but only {len(urls)} urls"
        )

    # upload function wrapped with a retry decorator
    @retry(RETRY_EXCEPTIONS)
    def _upload_part_base(item, file_path, part_size, file_size):
        part_num, part_url = item[0], item[1]
        offset = part_size * part_num
        num_bytes = min(part_size, file_size - offset)

        log.debug('Uploading file part %s', part_num)
        with open(file_path, 'rb') as fin:
            fin.seek(offset)
            partial_buf = BufferedPartialReader(fin, num_bytes)
            part_response = requests.put(part_url, data=partial_buf,
                                         timeout=60)

        if not part_response.ok:
            msg = _get_aws_error_message(part_response)
            raise HTTPError(msg, response=part_response)

        log.debug('Completed upload of file part %s', part_num)

    # upload each part
    try:
        pool = Pool(MAX_THREADS)
        _upload_part = partial(_upload_part_base, file_path=buf.name,
                               part_size=part_size, file_size=file_size)
        pool.map(_upload_part, enumerate(urls))

    # complete the multipart upload; an abort will be triggered
    # if any part except the last failed to upload at least 5MB
    finally:
        pool.terminate()
        client.files.post_multipart_complete(file_response.id)

    log.debug('Uploaded File %d', file_response.id)
    return file_response.id


@deprecate_param('v2.0.0', 'api_key')
def file_to_civis(buf, name=None, api_key=None, client=None, **kwargs):
    """Upload a file to Civis.

    Parameters
    ----------
    buf : file-like object or str
        Either a file-like object for a buffer or a string for a local file
        path.
        Note that if a file-like object is provided and it's not
        an :class:`io.BufferedReader <io.BufferedReader>`
        or :class:`io.TextIoWrapper <io.TextIoWrapper>` object,
        the current implementation requires extra disk space
        (which could be an issue if your file is large).
    name : str, optional
        The name you wish to give the file. If not given, it will be inferred
        from the basename of ``buf`` (if ``buf`` is a string for a file path)
        or ``buf.name`` (if ``buf`` is a file-like object).
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    **kwargs : kwargs
        Extra keyword arguments will be passed to the file creation
        endpoint. See :func:`~civis.resources._resources.Files.post`.
        In particular, ``expires_at`` can be specified for
        the date and time the file will expire. If not specified, the file
        will expire in 30 days. To keep a file indefinitely, specify ``None``.
        To specify a date and time, format it by the ISO 8601 standard,
        e.g., ``"2020-12-31"``, ``"2020-12-31T23:03:40Z"``,
        and what the ``isoformat()`` method returns from
        a :class:`datetime.date <datetime.date>`
        or :class:`datetime.datetime <datetime.datetime>` object.

    Returns
    -------
    file_id : int
        The new Civis file ID.

    Raises
    ------
    TypeError
        If ``name`` is not provided and cannot be inferred from ``buf``

    Examples
    --------
    >>> # Upload file at a given path on the local filesystem.
    >>> file_id = file_to_civis("my_data.csv", 'my_data')
    >>> # If not given, ``name`` will be the basename of the given file path.
    >>> file_id = file_to_civis("foo/bar/data.csv")  # ``name`` is 'data.csv'
    >>> # Upload file which expires in 30 days
    >>> with open("my_data.csv", "r") as f:
    ...     file_id = file_to_civis(f, 'my_data')
    >>> # Upload file which never expires
    >>> with open("my_data.csv", "r") as f:
    ...     file_id = file_to_civis(f, 'my_data', expires_at=None)

    Notes
    -----
    If you are opening a binary file (e.g., a compressed archive) to
    pass to this function, do so using the ``'rb'`` (read binary)
    mode (e.g., ``open('myfile.zip', 'rb')``).

    Warning: If the file-like object is seekable, the current
    position will be reset to 0.

    This facilitates retries and is used to chunk files for multipart
    uploads for improved performance.

    Small or non-seekable file-like objects will be uploaded with a
    single post.
    """
    if name is None:
        if isinstance(buf, str):
            name = os.path.basename(buf)
        elif hasattr(buf, 'name'):
            name = buf.name
        else:
            msg = (
                "`buf` is a file-like object, but its name cannot be inferred."
                " Please provide `name` explicitly."
            )
            raise TypeError(msg)

    if isinstance(buf, str):
        with open(buf, 'rb') as f:
            return _file_to_civis(
                f, name, api_key=api_key, client=client, **kwargs)

    # we should only pass _file_to_civis a file-like object that is
    # on disk, seekable and at position 0
    if not isinstance(buf, (io.BufferedReader, io.TextIOWrapper)) or \
            buf.tell() != 0:
        # determine mode for writing
        mode = 'w'
        if isinstance(buf.read(0), bytes):
            mode += 'b'
        with TemporaryDirectory() as tmp_dir:
            tmp_path = os.path.join(tmp_dir, 'file_to_civis.csv')
            with open(tmp_path, mode) as fout:
                shutil.copyfileobj(buf, fout, CHUNK_SIZE)
            with open(tmp_path, 'rb') as fin:
                return _file_to_civis(
                    fin, name, api_key=api_key, client=client, **kwargs)
    else:
        return _file_to_civis(
            buf, name, api_key=api_key, client=client, **kwargs)


def _file_to_civis(buf, name, api_key=None, client=None, **kwargs):
    if client is None:
        client = APIClient(api_key=api_key)

    file_size = _buf_len(buf)
    if not file_size:
        log.warning('Could not determine file size; defaulting to '
                    'single post. Files over 5GB will fail.')

    if not file_size or file_size <= MIN_MULTIPART_SIZE:
        return _single_upload(buf, name, client, **kwargs)
    elif file_size > MAX_FILE_SIZE:
        msg = "File is greater than the maximum allowable file size (5TB)"
        raise ValueError(msg)
    else:
        return _multipart_upload(buf, name, file_size, client, **kwargs)


@deprecate_param('v2.0.0', 'api_key')
def civis_to_file(file_id, buf, api_key=None, client=None):
    """Download a file from Civis.

    Parameters
    ----------
    file_id : int
        The Civis file ID.
    buf : file-like object or str
        A buffer or path specifying where to write the contents of the Civis
        file. Strings will be treated as paths to local files to open.
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.

    Returns
    -------
    None

    Examples
    --------
    >>> file_id = 100
    >>> # Download a file to a path on the local filesystem.
    >>> civis_to_file(file_id, "my_file.txt")
    >>> # Download a file to a file object.
    >>> with open("my_file.txt", "wb") as f:
    ...    civis_to_file(file_id, f)
    >>> # Download a file as a bytes object.
    >>> import io
    >>> buf = io.BytesIO()
    >>> civis_to_file(file_id, buf)
    >>> # Note that s could be converted to a string with s.decode('utf-8').
    >>> s = buf.read()
    """
    if isinstance(buf, str):
        with open(buf, 'wb') as f:
            _civis_to_file(file_id, f, api_key=api_key, client=client)
    else:
        _civis_to_file(file_id, buf, api_key=api_key, client=client)
        buf.flush()


def _civis_to_file(file_id, buf, api_key=None, client=None):
    if client is None:
        client = APIClient(api_key=api_key)
    files_response = client.files.get(file_id)
    url = files_response.file_url
    if not url:
        raise EmptyResultError('Unable to locate file {}. If it previously '
                               'existed, it may have '
                               'expired.'.format(file_id))

    # Store the current buffer position in case we need to retry below.
    buf_orig_position = buf.tell()

    @retry(RETRY_EXCEPTIONS)
    def _download_url_to_buf():
        # Reset the buffer in case we had to retry.
        buf.seek(buf_orig_position)

        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        chunked = response.iter_content(CHUNK_SIZE)
        for lines in chunked:
            buf.write(lines)

    _download_url_to_buf()


def file_id_from_run_output(name, job_id, run_id, regex=False, client=None):
    """Find the file ID of a File run output with the name "name"

    The run output is required to have type "File".
    If using an approximate match and multiple names match the
    provided string, return only the first file ID.

    Parameters
    ----------
    name : str
        The "name" field of the run output you wish to retrieve
    job_id : int
    run_id : int
    regex : bool, optional
        If False (the default), require an exact string match between
        ``name`` and the name of the run output. If True, search for a
        name which matches the regular expression ``name`` and
        retrieve the first found.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.

    Returns
    -------
    file_id : int
        The ID of a Civis File with name matching ``name``

    Raises
    ------
    IOError
        If the provided job ID and run ID combination can't be found
    FileNotFoundError
        If the run exists, but ``name`` isn't in its run outputs

    See Also
    --------
    APIClient.scripts.list_containers.runs_outputs
    """
    client = APIClient() if client is None else client
    # Retrieve run outputs
    try:
        outputs = client.scripts.list_containers_runs_outputs(job_id, run_id)
    except CivisAPIError as err:
        if err.status_code == 404:
            raise IOError('Could not find job/run ID {}/{}'
                          .format(job_id, run_id)) from err
        else:
            raise

    # Find file in the run outputs.
    if not regex:
        # Require an exact match on the "name" string.
        obj = find_one(outputs, name=name, object_type='File')
    else:
        # Search for a filename which contains the "name" string
        obj_matches = [o for o in outputs
                       if re.search(name, o.name) and o.object_type == 'File']
        if len(obj_matches) > 1:
            log.warning('Found %s matches to "%s". Returning the first.',
                        len(obj_matches), name)
        obj = None if not obj_matches else obj_matches[0]
    if obj is None:
        prefix = "A file containing the pattern" if regex else "File"
        raise FileNotFoundError('{} "{}" is not an output of job/run ID '
                                '{}/{}.'.format(prefix, name, job_id, run_id))
    return obj['object_id']


def file_to_dataframe(file_id, compression='infer', client=None,
                      **read_kwargs):
    """Load a :class:`~pandas.DataFrame` from a CSV stored in a Civis File

    The :class:`~pandas.DataFrame` will be read directly from Civis
    without copying the CSV to a local file on disk.

    Parameters
    ----------
    file_id : int
        ID of a Civis File which contains a CSV
    compression : str, optional
        If "infer", set the ``compression`` argument of ``pandas.read_csv``
        based on the file extension of the name of the Civis File.
        Otherwise pass this argument to ``pandas.read_csv``.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    **read_kwargs
        Additional arguments will be passed directly to
        :func:`~pandas.read_csv`.

    Returns
    -------
    :class:`~pandas.DataFrame` containing the contents of the CSV

    Raises
    ------
    ImportError
        If ``pandas`` is not available

    See Also
    --------
    pandas.read_csv
    """
    if not HAS_PANDAS:
        raise ImportError('file_to_dataframe requires pandas to be installed.')
    client = APIClient() if client is None else client
    file_info = client.files.get(file_id)
    file_url = file_info.file_url
    if not file_url:
        raise EmptyResultError('Unable to locate file {}. If it previously '
                               'existed, it may have '
                               'expired.'.format(file_id))
    file_name = file_info.name
    if compression == 'infer':
        comp_exts = {'.gz': 'gzip', '.xz': 'xz', '.bz2': 'bz2', '.zip': 'zip'}
        ext = os.path.splitext(file_name)[-1]
        if ext in comp_exts:
            compression = comp_exts[ext]

    return pd.read_csv(file_url, compression=compression, **read_kwargs)


def dataframe_to_file(df, name='data.csv', expires_at='DEFAULT',
                      client=None, **to_csv_kws):
    """Store a :class:`~pandas.DataFrame` as a CSV in Civis Platform

    Parameters
    ----------
    df : :class:`~pandas.DataFrame`
        The table to upload.
    name : str, optional
        The name of the Civis File
    expires_at : str, optional
        The date and time the file will expire. If not specified, the file
        will expire in 30 days. To keep a file indefinitely, specify ``None``.
        To specify a date and time, format it by the ISO 8601 standard,
        e.g., ``"2020-12-31"``, ``"2020-12-31T23:03:40Z"``,
        and what the ``isoformat()`` method returns from
        a :class:`datetime.date <datetime.date>`
        or :class:`datetime.datetime <datetime.datetime>` object.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    **to_csv_kws
        Additional keyword parameters will be passed directly to
        :func:`~pandas.DataFrame.to_csv`.

    Returns
    -------
    file_id : int
        The integer ID of the new Civis File object

    See Also
    --------
    :func:`file_to_civis`
    :func:`~pandas.DataFrame.to_csv`
    """
    with TemporaryDirectory() as tdir:
        path = os.path.join(tdir, name)
        df.to_csv(path, **to_csv_kws)
        file_kwargs = {'name': name}
        if expires_at != 'DEFAULT':
            # A missing parameter signifies the default value here.
            file_kwargs['expires_at'] = expires_at
        fid = file_to_civis(path, client=client, **file_kwargs)
    return fid


def file_to_json(file_id, client=None, **json_kwargs):
    """Restore JSON stored in a Civis File

    Parameters
    ----------
    file_id : int
        ID of a JSON-formatted Civis File
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    **json_kwargs
        Additional keyword arguments will be passed directly to
        :func:`json.load`.

    Returns
    -------
    The object extracted from the JSON-formatted file

    See Also
    --------
    :func:`civis_to_file`
    :func:`json.load`
    """
    buf = io.BytesIO()
    civis_to_file(file_id, buf, client=client)
    txt = io.TextIOWrapper(buf, encoding='utf-8')
    txt.seek(0)
    return json.load(txt, **json_kwargs)


def json_to_file(obj, name='file.json', expires_at='DEFAULT',
                 client=None, **json_kwargs):
    """Store a JSON-serializable object in a Civis File

    Parameters
    ----------
    obj
        The object to be JSON-serialized and stored in a Civis File
    name : str, optional
        The name of the Civis File
    expires_at : str, optional
        The date and time the file will expire. If not specified, the file
        will expire in 30 days. To keep a file indefinitely, specify ``None``.
        To specify a date and time, format it by the ISO 8601 standard,
        e.g., ``"2020-12-31"``, ``"2020-12-31T23:03:40Z"``,
        and what the ``isoformat()`` method returns from
        a :class:`datetime.date <datetime.date>`
        or :class:`datetime.datetime <datetime.datetime>` object.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    **json_kwargs
        Additional keyword arguments will be passed directly to
        :func:`json.dump`.

    Returns
    -------
    file_id : int
        The integer ID of the new Civis File object

    See Also
    --------
    :func:`file_to_civis`
    :func:`json.dump`
    """
    buf = io.BytesIO()
    txt = io.TextIOWrapper(buf, encoding='utf-8')
    json.dump(obj, txt, **json_kwargs)
    txt.seek(0)

    file_kwargs = {'name': name}
    if expires_at != 'DEFAULT':
        # A missing parameter signifies the default value here.
        file_kwargs['expires_at'] = expires_at
    fid = file_to_civis(txt.buffer, client=client, **file_kwargs)
    return fid
