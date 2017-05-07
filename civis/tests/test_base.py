import requests

from civis.base import Endpoint, get_base_url
from civis.compat import mock


def test_base_url_default():
    assert get_base_url() == 'https://api.civisanalytics.com/'


def test_base_url_from_env():
    custom_url = 'https://api1.civisanalytics.com'
    with mock.patch.dict('os.environ', {'CIVIS_API_ENDPOINT': custom_url}):
        assert get_base_url() == custom_url + '/'


@mock.patch('civis.base.get_base_url', return_value='https://base.api.url/')
def test_endpoint_base_url(mock_get_base_url):
    session = mock.MagicMock(spec=requests.Session)
    endpoint = Endpoint(session)

    assert endpoint._base_url == 'https://base.api.url/'
