from collections import OrderedDict
import json

from civis.service_client import ServiceClient, ServiceEndpoint

from civis import response
from civis.base import CivisAPIError
import pytest
from unittest import mock

mock_service_id = 1

mock_survey_url = "www.survey-url.com"


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
    mock_ops_str = str(ops_json).replace('\'', '\"')
    mock_operations = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(mock_ops_str)  # noqa: E501
    return mock_operations


@mock.patch('civis.service_client.ServiceClient.generate_classes')
@mock.patch('civis.service_client.civis')
def test_service_client(mock_civis, classes_mock):
    mock_client = mock_civis.APIClient()
    mock_client.services.get.return_value = {"current_url": mock_survey_url}
    classes_mock.return_value = {}

    sc = ServiceClient(mock_service_id)

    assert sc._session_kwargs == {}
    assert sc._service_id == mock_service_id
    assert sc._base_url == mock_survey_url
    assert sc._root_path == None
    assert sc._swagger_path == "/endpoints"

    # Custom root path
    sc = ServiceClient(mock_service_id, root_path='/api')
    assert sc._root_path == '/api'

    # Custom Swagger path
    sc = ServiceClient(mock_service_id, swagger_path='/spec')
    assert sc._swagger_path == "/spec"


@mock.patch('civis.service_client.ServiceClient.generate_classes')
@mock.patch('civis.service_client.civis')
def test_parse_path(mock_civis, classes_mock, mock_operations):
    mock_client = mock_civis.APIClient()
    mock_client.services.get.return_value = {"current_url": mock_survey_url}
    classes_mock.return_value = {}
    sc = ServiceClient(mock_service_id)

    mock_path = '/some-resource/sub-resource/{id}'
    base_path, methods = sc.parse_path(mock_path, mock_operations)

    assert base_path == "some_resource"
    assert 'get_sub_resource' in methods[0]

    mock_path = '/some-resource/{id}'
    base_path, methods = sc.parse_path(mock_path, mock_operations)

    assert base_path == "some_resource"
    assert 'get' in methods[0]


@mock.patch('civis.service_client.ServiceClient.generate_classes')
@mock.patch('civis.service_client.civis')
def test_parse_path__with_root(mock_civis, classes_mock, mock_operations):
    mock_client = mock_civis.APIClient()
    mock_client.services.get.return_value = {"current_url": mock_survey_url}
    classes_mock.return_value = {}
    sc = ServiceClient(mock_service_id, root_path='/some-resource')

    mock_path = '/some-resource/sub-resource/{id}'
    base_path, methods = sc.parse_path(mock_path, mock_operations)

    assert base_path == "sub_resource"
    assert 'get' in methods[0]


@mock.patch('civis.service_client.ServiceClient.generate_classes')
@mock.patch('civis.service_client.civis')
def test_parse_api_spec(mock_civis, classes_mock, mock_swagger):
    mock_client = mock_civis.APIClient()
    mock_client.services.get.return_value = {"current_url": mock_survey_url}
    classes_mock.return_value = {}

    sc = ServiceClient(mock_service_id)

    classes = sc.parse_api_spec(mock_swagger)
    assert 'some_resources' in classes


@mock.patch('civis.service_client.requests.Session.get')
@mock.patch('civis.service_client.auth_service_session')
@mock.patch('civis.service_client.ServiceClient.generate_classes')
@mock.patch('civis.service_client.civis')
def test_get_api_spec(mock_civis, classes_mock,
                      auth_session_mock, mock_response, mock_swagger):
    mock_response.return_value = mock.Mock(ok=True)
    mock_response.return_value.json.return_value = mock_swagger

    mock_client = mock_civis.APIClient()
    mock_client.services.get.return_value = {"current_url": mock_survey_url}
    classes_mock.return_value = {}

    sc = ServiceClient(mock_service_id)

    spec = sc.get_api_spec()
    assert spec == mock_swagger


@mock.patch('civis.service_client.setattr')
@mock.patch('civis.service_client.ServiceClient.parse_api_spec')
@mock.patch('civis.service_client.ServiceClient.get_api_spec')
@mock.patch('civis.service_client.civis')
def test_generate_classes(mock_civis, api_spec_mock,
                          parse_mock, setattr_mock, mock_swagger):
    setattr_mock.return_value = {}
    api_spec_mock.return_value = {}
    mock_class_function = (lambda s, client, return_type, root_path: '/api')
    parse_mock.return_value = {'class': mock_class_function}
    mock_client = mock_civis.APIClient()
    mock_client.services.get.return_value = {"current_url": mock_survey_url}

    sc = ServiceClient(mock_service_id)

    classes = sc.generate_classes()

    assert 'class' in classes


@mock.patch('civis.service_client.ServiceClient.generate_classes')
@mock.patch('civis.service_client.civis')
def test_get_base_url(mock_civis, classes_mock):
    mock_client = mock_civis.APIClient()
    mock_client.services.get.return_value = {"current_url": mock_survey_url}
    classes_mock.return_value = {}

    sc = ServiceClient(mock_service_id)

    assert sc._base_url == mock_survey_url
    mock_client.services.get.assert_called_once_with(mock_service_id)


@mock.patch('civis.service_client.ServiceClient.generate_classes')
@mock.patch('civis.service_client.civis')
def test_get_base_url__not_found(mock_civis, classes_mock):

    err_resp = response.Response({
        'status_code': 404,
        'error': 'not_found',
        'errorDescription': 'The requested resource could not be found.',
        'content': True})
    err_resp.json = lambda: err_resp.json_data

    mock_client = mock_civis.APIClient()
    mock_client.services.get.side_effect = CivisAPIError(err_resp)
    classes_mock.return_value = {}

    with pytest.raises(ValueError) as excinfo:
        ServiceClient(mock_service_id)

    expected_error = f'There is no Civis Service with ID {mock_service_id}!'
    assert str(excinfo.value) == expected_error


def test_build_path():
    service_client_mock = mock.Mock(_base_url='www.service_url.com')
    se = ServiceEndpoint({}, service_client_mock)
    path = se._build_path('/resources')

    assert path == 'www.service_url.com/resources'


def test_build_path__with_root():
    service_client_mock = mock.Mock(_base_url='www.service_url.com')
    se = ServiceEndpoint({}, service_client_mock, root_path='/api')
    path = se._build_path('/resources')

    assert path == 'www.service_url.com/api/resources'


@mock.patch('civis.service_client.requests.Session.request')
@mock.patch('civis.service_client.auth_service_session')
@mock.patch('civis.service_client.ServiceClient.get_base_url')
def test_make_request(mock_base_url, auth_mock, request_mock):
    service_client_mock = mock.Mock(_base_url='www.service_url.com')
    se = ServiceEndpoint({}, service_client_mock)

    expected_value = [{'id': 1, 'url': 'www.survey_url.com/1'},
                      {'id': 2, 'url': 'www.survey_url.com/2'}]

    request_mock.return_value = mock.Mock(ok=True)
    request_mock.return_value.json = expected_value

    response = se._make_request('get', 'resources/resources')

    assert response.json == expected_value
