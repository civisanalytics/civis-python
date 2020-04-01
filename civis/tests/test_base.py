from unittest import mock

import requests

from civis.base import Endpoint, get_base_url


def test_base_url_default():
    assert get_base_url() == 'https://api.civisanalytics.com/'


def test_base_url_from_env():
    custom_url = 'https://api1.civisanalytics.com'
    with mock.patch.dict('os.environ', {'CIVIS_API_ENDPOINT': custom_url}):
        assert get_base_url() == custom_url + '/'


@mock.patch('civis.base.get_base_url', return_value='https://base.api.url/')
def test_endpoint_base_url(mock_get_base_url):
    session = mock.MagicMock(spec=requests.Session)
    endpoint = Endpoint(session, 'client')

    assert endpoint._base_url == 'https://base.api.url/'


def test_store_last_response():
    mock_client = mock.Mock()
    endpoint = Endpoint({}, client=mock_client, return_type='raw')

    returned_resp = {'value': 'response'}
    endpoint._make_request = mock.Mock(return_value=returned_resp)

    resp = endpoint._call_api('GET')
    assert resp == returned_resp
    assert mock_client.last_response is resp
