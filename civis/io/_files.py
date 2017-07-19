from collections import OrderedDict
import io
import json
import logging
import os
import re
import six

import requests
from requests import HTTPError

from civis import APIClient, find_one
from civis.base import CivisAPIError, EmptyResultError
from civis.compat import FileNotFoundError
from civis.utils._deprecation import deprecate_param
try:
    from requests_toolbelt.multipart.encoder import MultipartEncoder
    HAS_TOOLBELT = True
except ImportError:
    HAS_TOOLBELT = False
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

log = logging.getLogger(__name__)
__all__ = ['file_to_civis', 'civis_to_file', 'file_id_from_run_output',
           'file_to_dataframe', 'file_to_json']


def _get_aws_error_message(response):
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


@deprecate_param('v2.0.0', 'api_key')
def file_to_civis(buf, name, api_key=None, client=None, **kwargs):
    """Upload a file to Civis.

    Parameters
    ----------
    buf : file-like object
        The file or other buffer that you wish to upload.
    name : str
        The name you wish to give the file.
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    **kwargs : kwargs
        Extra keyword arguments will be passed to the file creation
        endpoint. See :func:`~civis.resources._resources.Files.post`.

    Returns
    -------
    file_id : int
        The new Civis file ID.

    Examples
    --------
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

    If you have the `requests-toolbelt` package installed
    (`pip install requests-toolbelt`) and the file-like object is seekable,
    then this function will stream from the open file pointer into Platform.
    If `requests-toolbelt` is not installed or the file-like object is not
    seekable, then it will need to read the entire buffer into memory before
    writing.
    """
    if client is None:
        client = APIClient(api_key=api_key)
    file_response = client.files.post(name, **kwargs)

    # Platform has given us a URL to which we can upload a file.
    # The file must be uploaded with a POST formatted as per
    # http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-post-example.html
    # Note that the payload must have "key" first and "file" last.
    form = file_response.upload_fields
    form_key = OrderedDict(key=form.pop('key'))
    form_key.update(form)
    form_key['file'] = buf

    url = file_response.upload_url
    if HAS_TOOLBELT and buf.seekable():
        # This streams from the open file buffer without holding the
        # contents in memory.
        en = MultipartEncoder(fields=form_key)
        # The refusal error from AWS states 5368730624 is the max size allowed
        if en.len >= 5 * 2 ** 30:  # 5 GB
            msg = "Cannot upload files greater than 5GB. Got {:d}."
            raise ValueError(msg.format(en.len))
        elif en.len <= 100 * 2 ** 20:  # 100 MB
            # Semi-arbitrary cutoff for "small" files.
            # Send these with requests directly because that uses less CPU
            response = requests.post(url, files=form_key)
        else:
            response = requests.post(url, data=en,
                                     headers={'Content-Type': en.content_type})
    else:
        response = requests.post(url, files=form_key)

    if not response.ok:
        # Amazon gives back informative error messages
        # http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        msg = _get_aws_error_message(response)
        raise HTTPError(msg, response=response)

    return file_response.id


@deprecate_param('v2.0.0', 'api_key')
def civis_to_file(file_id, buf, api_key=None, client=None):
    """Download a file from Civis.

    Parameters
    ----------
    file_id : int
        The Civis file ID.
    buf : file-like object
        The file or other buffer to write the contents of the Civis file
        into.
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
    >>> with open("my_file.txt", "wb") as f:
    ...    civis_to_file(file_id, f)
    """
    if client is None:
        client = APIClient(api_key=api_key)
    url = _get_url_from_file_id(file_id, client=client)
    if not url:
        raise EmptyResultError('Unable to locate file {}. If it previously '
                               'existed, it may have '
                               'expired.'.format(file_id))
    response = requests.get(url, stream=True)
    response.raise_for_status()
    chunk_size = 32 * 1024
    chunked = response.iter_content(chunk_size)
    for lines in chunked:
        buf.write(lines)


def _get_url_from_file_id(file_id, client):
    files_response = client.files.get(file_id)
    url = files_response.file_url
    return url


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
            six.raise_from(IOError('Could not find job/run ID {}/{}'
                           .format(job_id, run_id)), err)
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
    file_name = file_info.name
    if compression == 'infer':
        comp_exts = {'.gz': 'gzip', '.xz': 'xz', '.bz2': 'bz2', '.zip': 'zip'}
        ext = os.path.splitext(file_name)[-1]
        if ext in comp_exts:
            compression = comp_exts[ext]

    return pd.read_csv(file_url, compression=compression, **read_kwargs)


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
