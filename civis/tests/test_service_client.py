from collections import OrderedDict
import json
from unittest import mock

from civis import response
from civis.base import CivisAPIError
from civis.service_client import ServiceClient, ServiceEndpoint, \
    _get_service, _parse_service_path, parse_service_api_spec
import pytest

MOCK_SERVICE_ID = 0

MOCK_URL = "www.survey-url.com"


@pytest.fixture
def mock_swagger():
    return {
            "info": {
                "title": "Test API Client",
                "version": "1.0"
            },
            "paths": {
                "/some-resources": {
                    "get": {
                        "description": "",
                        "responses": {
                            "200": {
                                "description": "Returns a list",
                            }
                        },
                        "summary": "List Resources",
                        "tags": [
                            "tag"
                        ]
                    }
                },
                "/some-resources/{id}": {
                    "get": {
                        "description": "",
                        "responses": {
                            "200": {
                                "description": "Returns a Resource",
                            }
                        },
                        "summary": "Get Resources",
                        "tags": [
                            "tag"
                        ]
                    },
                    "patch": {
                        "description": "",
                        "parameters": [
                            {
                                "description": "The id of the Resource",
                                "in": "path",
                                "name": "id",
                                "required": True,
                                "type": "integer"
                            },
                            {
                                "description": "The fields and values to edit",
                                "in": "body",
                                "name": "body",
                                "required": True,
                                "schema": {
                                    "properties": {
                                        "field": {
                                            "description": "a property value",
                                            "type": "string"
                                        }
                                    }
                                }
                            }
                        ],
                        "responses": {
                            "200": {
                                "description": "Edits Resource",
                            }
                        },
                        "summary": "Patch Resources",
                        "tags": [
                            "tag"
                        ]
                    }
                }
            },
            "swagger": "2.0"
            }


@pytest.fixture
def mock_operations(mock_swagger):
    ops_json = mock_swagger["paths"]["/some-resources"]
    mock_ops_str = str(json.dumps(ops_json))
    mock_operations = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(mock_ops_str)  # noqa: E501
    return mock_operations


@mock.patch('civis.service_client.ServiceClient.generate_classes_maybe_cached')
@mock.patch('civis.service_client.ServiceClient.get_base_url')
def test_service_client(url_mock, classes_mock):
    url_mock.return_value = MOCK_URL
    classes_mock.return_value = {}

    sc = ServiceClient(MOCK_SERVICE_ID)

    spec_endpoint = "/endpoints"

    assert sc._api_key is None
    assert sc._service_id == MOCK_SERVICE_ID
    assert sc._base_url == MOCK_URL
    assert sc._root_path is None
    assert sc._swagger_path == spec_endpoint

    # Custom root path
    sc = ServiceClient(MOCK_SERVICE_ID, root_path='/api')
    assert sc._root_path == '/api'

    # Custom Swagger path
    sc = ServiceClient(MOCK_SERVICE_ID, swagger_path='/spec')
    assert sc._swagger_path == "/spec"

    # Passed in API Key
    sc = ServiceClient(MOCK_SERVICE_ID, api_key="this_is_an_API_key")
    assert sc._api_key == "this_is_an_API_key"


def test_service_endpoint():
    service_client_mock = mock.Mock()
    se = ServiceEndpoint(service_client_mock)

    assert se._return_type == 'civis'
    assert se._client == service_client_mock


def test_parse_service_path(mock_operations):
    mock_path = '/some-resource/sub-resource/{id}'
    base_path, methods = _parse_service_path(mock_path, mock_operations)

    assert base_path == "some_resource"
    assert 'get_sub_resource' in methods[0]

    mock_path = '/some-resource/{id}'
    base_path, methods = _parse_service_path(mock_path, mock_operations)

    assert base_path == "some_resource"
    assert 'get' in methods[0]


def test_parse_path__with_root(mock_operations):
    root_path = '/some-resource'

    mock_path = '/some-resource/sub-resource/{id}'
    base_path, methods = _parse_service_path(mock_path, mock_operations,
                                             root_path=root_path)

    assert base_path == "sub_resource"
    assert 'get' in methods[0]


def test_parse_service_api_spec(mock_swagger):
    classes = parse_service_api_spec(mock_swagger)
    assert 'some_resources' in classes


@mock.patch('civis.service_client.requests.Session.get')
@mock.patch('civis.service_client.auth_service_session')
@mock.patch('civis.service_client.ServiceClient.generate_classes_maybe_cached')
@mock.patch('civis.service_client.ServiceClient.get_base_url')
def test_get_api_spec(url_mock, classes_mock,
                      auth_session_mock, mock_response, mock_swagger):
    mock_response.return_value = mock.Mock(ok=True)
    mock_response.return_value.json.return_value = mock_swagger

    url_mock.return_value = MOCK_URL
    classes_mock.return_value = {}

    sc = ServiceClient(MOCK_SERVICE_ID)

    spec = sc.get_api_spec()
    assert spec == mock_swagger


@mock.patch('civis.service_client.parse_service_api_spec')
@mock.patch('civis.service_client.ServiceClient.get_api_spec')
@mock.patch('civis.service_client.ServiceClient.get_base_url')
def test_generate_classes(url_mock, api_spec_mock,
                          parse_mock, mock_swagger):
    api_spec_mock.return_value = {}
    mock_class_function = (lambda client, return_type: "return")
    parse_mock.return_value = {'class': mock_class_function}
    url_mock.return_value = MOCK_URL

    sc = ServiceClient(MOCK_SERVICE_ID, root_path='/foo')

    classes = sc.generate_classes()
    parse_mock.assert_called_once_with(api_spec_mock.return_value,
                                       root_path='/foo')

    assert 'class' in classes


@mock.patch('civis.service_client.parse_service_api_spec')
@mock.patch('civis.service_client.ServiceClient.get_api_spec')
@mock.patch('civis.service_client.ServiceClient.get_base_url')
def test_generate_classes_maybe_cached(url_mock, api_spec_mock,
                                       parse_mock, mock_swagger):
    api_spec_mock.return_value = {}
    mock_class_function = (lambda client, return_type: "return")
    parse_mock.return_value = {'class': mock_class_function}
    url_mock.return_value = MOCK_URL

    sc = ServiceClient(MOCK_SERVICE_ID, root_path='/foo')

    mock_spec_str = str(json.dumps(mock_swagger))
    mock_spec = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(mock_spec_str)  # noqa: E501
    classes = sc.generate_classes_maybe_cached(mock_spec)

    parse_mock.assert_has_calls([
        # the call from generate_classes_maybe_cached in ServiceClient.__init__
        mock.call({}, root_path='/foo'),
        # the call from generate_classes_maybe_cached in this test
        mock.call(mock_spec, root_path='/foo')
    ])

    assert 'class' in classes


@mock.patch('civis.service_client.ServiceClient.generate_classes_maybe_cached')
@mock.patch('civis.service_client._get_service')
def test_get_base_url(get_service_mock, classes_mock):
    get_service_mock.return_value = {'current_url': MOCK_URL}
    classes_mock.return_value = {}

    sc = ServiceClient(MOCK_SERVICE_ID)

    assert sc._base_url == MOCK_URL
    get_service_mock.assert_called_once_with(sc)


@mock.patch('civis.service_client.ServiceClient.generate_classes_maybe_cached')
@mock.patch('civis.service_client.APIClient')
def test_get_service(mock_client, classes_mock):
    classes_mock.return_value = {}
    sc = ServiceClient(MOCK_SERVICE_ID)
    expected_service = {'current_url': MOCK_URL}
    mock_client.return_value.services.get.return_value = expected_service
    service = _get_service(sc)
    assert service == expected_service


@mock.patch('civis.service_client.ServiceClient.generate_classes_maybe_cached')
@mock.patch('civis.service_client.APIClient')
def test_get_service__not_found(mock_client, classes_mock):
    classes_mock.return_value = {}
    sc = ServiceClient(MOCK_SERVICE_ID)
    err_resp = response.Response({
        'status_code': 404,
        'error': 'not_found',
        'errorDescription': 'The requested resource could not be found.',
        'content': True})
    err_resp.json = lambda: err_resp.json_data

    mock_client.return_value.services.get.side_effect = CivisAPIError(err_resp)

    with pytest.raises(CivisAPIError) as excinfo:
        _get_service(sc)

    expected_error = ('(404) The requested resource could not be found.')
    assert str(excinfo.value) == expected_error


def test_build_path():
    service_client_mock = mock.Mock(_base_url='www.service_url.com',
                                    _root_path=None)
    se = ServiceEndpoint(service_client_mock)
    path = se._build_path('/resources')

    assert path == 'www.service_url.com/resources'


def test_build_path__with_root():
    service_client_mock = mock.Mock(_base_url='www.service_url.com',
                                    _root_path='/api')
    se = ServiceEndpoint(service_client_mock)
    path = se._build_path('/resources')

    assert path == 'www.service_url.com/api/resources'


@mock.patch('civis.service_client.requests.Session.request')
@mock.patch('civis.service_client.auth_service_session')
def test_make_request(auth_mock, request_mock):
    service_client_mock = mock.Mock(_base_url='www.service_url.com')
    se = ServiceEndpoint(service_client_mock)

    expected_value = [{'id': 1, 'url': 'www.survey_url.com/1'},
                      {'id': 2, 'url': 'www.survey_url.com/2'}]

    request_mock.return_value = mock.Mock(ok=True)
    request_mock.return_value.json = expected_value

    response = se._make_request('get', 'resources/resources')

    assert response.json == expected_value
