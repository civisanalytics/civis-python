"""Mock client creation and tooling
"""

from civis import APIClient
from civis.compat import mock
from civis.resources import CACHED_SPEC_PATH


def create_client_mock(cache=CACHED_SPEC_PATH):
    """Create an APIClient mock from a cache of the API spec

    Parameters
    ----------
    cache : str, optional
        Location of the API spec on the local filesystem

    Returns
    -------
    mock.Mock
        A `Mock` object which looks like an APIClient and which will
        error if any method calls have non-existent / misspelled parameters
    """
    # Create a client from the cache. We'll use this for
    # auto-speccing. Prevent it from trying to talk to the real API.
    with mock.patch('requests.Session', mock.MagicMock):
        real_client = APIClient(local_api_spec=cache, api_key='none',
                                resources='all')
    real_client._feature_flags = {'noflag': None}
    if hasattr(real_client, 'channels'):
        # Deleting "channels" causes the client to fall back on
        # regular polling for completion, which greatly eases testing.
        delattr(real_client, 'channels')

    mock_client = mock.create_autospec(real_client, spec_set=True)

    return mock_client
