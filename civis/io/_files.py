from collections import OrderedDict

import requests

from civis import APIClient
from civis.base import EmptyResultError


def file_to_civis(buf, name, api_key=None, **kwargs):
    """Upload a file to Civis.

    Parameters
    ----------
    buf : file-like object
        The file or other buffer that you wish to upload.
    name : str
        The name you wish to give the file.
    api_key : str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
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
    """
    client = APIClient(api_key=api_key)
    file_response = client.files.post(name, **kwargs)

    form = file_response.upload_fields
    # order matters here! key must be first
    form_key = OrderedDict(key=form.pop('key'))
    form_key.update(form)
    form_key['file'] = buf

    url = file_response.upload_url
    response = requests.post(url, files=form_key)
    response.raise_for_status()

    return file_response.id


def civis_to_file(file_id, buf, api_key=None):
    """Download a file from Civis.

    Parameters
    ----------
    file_id : int
        The Civis file ID.
    buf : file-like object
        The file or other buffer to write the contents of the Civis file
        into.
    api_key : str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.

    Returns
    -------
    None

    Examples
    --------
    >>> file_id = 100
    >>> with open("my_file.txt", "w") as f:
    ...    civis_to_file(file_id, f)
    """
    url = _get_url_from_file_id(file_id, api_key=api_key)
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


def _get_url_from_file_id(file_id, api_key=None):
    client = APIClient(api_key=api_key)
    files_response = client.files.get(file_id)
    url = files_response.file_url
    return url
