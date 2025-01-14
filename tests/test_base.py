from unittest import mock
from json.decoder import JSONDecodeError

import pytest
import requests

from civis.base import Endpoint, get_base_url, CivisAPIError


def test_base_url_default():
    assert get_base_url() == "https://api.civisanalytics.com/"


def test_base_url_from_env():
    custom_url = "https://api1.civisanalytics.com"
    with mock.patch.dict("os.environ", {"CIVIS_API_ENDPOINT": custom_url}):
        assert get_base_url() == custom_url + "/"


@mock.patch("civis.base.get_base_url", return_value="https://base.api.url/")
def test_endpoint_base_url(mock_get_base_url):
    session = mock.MagicMock(spec=requests.Session)
    endpoint = Endpoint(session, "client")

    assert endpoint._base_url == "https://base.api.url/"


def test_store_last_response():
    mock_client = mock.Mock()
    endpoint = Endpoint({}, client=mock_client, return_type="raw")

    returned_resp = {"value": "response"}
    endpoint._make_request = mock.Mock(return_value=returned_resp)

    resp = endpoint._call_api("GET")
    assert resp == returned_resp
    assert mock_client.last_response is resp


def test_civis_api_error_empty_response():
    # Fake response object, try to trigger error
    # Make sure response.json() gets the JSON decode error
    response = requests.Response()
    response._content = b"foobar"
    with pytest.raises(JSONDecodeError):
        response.json()

    error = CivisAPIError(response)
    assert error.error_message == "No Response Content from Civis API"


@pytest.mark.parametrize(
    "source_params, expected_params",
    [
        ({}, None),
        (None, None),
        (
            {"foo": 123, "bar": "hello", "baz": {"a": 1, "b": 2}},
            {"foo": 123, "bar": "hello", "baz": {"a": 1, "b": 2}},
        ),
        ({"foo": [1, 2, 3]}, {"foo[]": [1, 2, 3]}),
        ({"foo": (1, 2, 3)}, {"foo[]": [1, 2, 3]}),
        ({"foo": {1, 2, 3}}, {"foo[]": [1, 2, 3]}),
    ],
)
def test_array_params(source_params, expected_params):
    assert Endpoint._handle_array_params(source_params) == expected_params
