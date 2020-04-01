"""Mock client creation and tooling
"""
from functools import lru_cache
import os
from unittest import mock

from civis import APIClient


TEST_SPEC = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                         "civis_api_spec.json")
TEST_SPEC_ALL = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             "civis_api_spec_channels.json")


def create_client_mock(cache=TEST_SPEC):
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
    # Create a client from the cache. We'll use this for auto-speccing.
    real_client = _real_client(cache)

    # Prevent the client from trying to talk to the real API when autospeccing
    with mock.patch('requests.Session', mock.MagicMock):
        mock_client = mock.create_autospec(real_client, spec_set=True)

    return mock_client


@lru_cache(maxsize=1)
def _real_client(local_api_spec):
    real_client = APIClient(local_api_spec=local_api_spec, api_key='none')
    real_client._feature_flags = {'noflag': None}
    if hasattr(real_client, 'channels'):
        # Deleting "channels" causes the client to fall back on
        # regular polling for completion, which greatly eases testing.
        delattr(real_client, 'channels')
    return real_client
