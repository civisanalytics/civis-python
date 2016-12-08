import pytest
from unittest import mock

import requests

try:
    import pandas as pd
    has_pandas = True
except ImportError:
    has_pandas = False

from civis.response import (
    CivisClientError, PaginatedResponse, _response_to_json,
    convert_response_data_type, Response
)


def _create_mock_response(data, headers, status_code=200):
    mock_response = mock.MagicMock(spec=requests.Response)
    mock_response.json.return_value = data
    mock_response.headers = headers
    mock_response.status_code = status_code
    return mock_response


def test_pagination():
    results = [
        [
            {'id': 1, 'name': 'job_1'},
            {'id': 2, 'name': 'job_2'},
            {'id': 3, 'name': 'job_3'},
        ],
        [
            {'id': 4, 'name': 'job_4'},
            {'id': 5, 'name': 'job_5'},
        ],
        []
    ]
    mock_endpoint = mock.MagicMock()
    mock_endpoint._make_request.side_effect = [
        _create_mock_response(result, {}) for result in results
    ]
    mock_endpoint._return_type = 'snake'

    path = '/objects'
    params = {'param': 'value'}
    paginator = iter(PaginatedResponse(path, params, mock_endpoint))

    # No API calls made yet.
    mock_endpoint._make_request.assert_not_called()

    all_data = []
    for indx, obj in enumerate(paginator):
        assert obj['id'] == indx + 1
        all_data.append(obj)

        # Test lazy evaluation. Should make only make one call up until the
        # first item of the second page is needed.
        if indx < 3:
            mock_endpoint._make_request.assert_called_once_with(
                'GET', path, dict(params, **{'page_num': 1}))
        else:
            mock_endpoint._make_request.assert_called_with(
                'GET', path, dict(params, **{'page_num': 2}))

    # One extra call is made. Pagination is stopped since the response is
    # empty.
    assert mock_endpoint._make_request.call_count == 3
    assert len(all_data) == 5


def test_response_to_json_no_error():
    raw_response = mock.MagicMock()
    raw_response.json.return_value = {'key': 'value'}
    assert _response_to_json(raw_response) == {'key': 'value'}


def test_response_to_json_parsing_error():
    raw_response = mock.MagicMock()
    raw_response.json.side_effect = ValueError('Invalid json')
    with pytest.raises(CivisClientError) as excinfo:
        _response_to_json(raw_response)
    assert 'Unable to parse JSON from response' in str(excinfo.value)


def test_convert_data_type_raw_unparsed():
    response = _create_mock_response({}, {'header1': 'val1'})
    data = convert_response_data_type(response, return_type='raw')

    assert isinstance(data, requests.Response)
    assert data.headers == {'header1': 'val1'}


def test_convert_data_type_raw_parsed():
    response = {'foo': 'bar'}
    data = convert_response_data_type(response, return_type='raw')

    assert isinstance(data, dict)
    assert data == {'foo': 'bar'}


@pytest.mark.skipif(not has_pandas, reason='pandas not installed')
def test_convert_data_type_pandas_series():
    response = _create_mock_response({'foo': 'bar'}, None)
    data = convert_response_data_type(response, return_type='pandas')

    assert isinstance(data, pd.Series)
    assert data.equals(pd.Series({'foo': 'bar'}))


@pytest.mark.skipif(not has_pandas, reason='pandas not installed')
def test_convert_data_type_pandas_df():
    response = _create_mock_response([{'foo': 'bar'}], None)
    data = convert_response_data_type(response, return_type='pandas')

    assert isinstance(data, pd.DataFrame)
    assert data.equals(pd.DataFrame.from_records([{'foo': 'bar'}]))


def test_convert_data_type_civis():
    response = _create_mock_response({'foo': 'bar'}, {'header': 'val'})
    data = convert_response_data_type(response, return_type='snake')

    assert isinstance(data, Response)
    assert data['foo'] == 'bar'
    assert data.headers == {'header': 'val'}


def test_convert_data_type_civis_list():
    response = _create_mock_response([{'foo': 'bar'}, {'fizz': 'buzz'}],
                                     {'header': 'val'})
    data = convert_response_data_type(response, return_type='snake')

    assert isinstance(data, list)
    assert len(data) == 2
    assert isinstance(data[0], Response)
    assert data[0]['foo'] == 'bar'
    assert data[0].headers == {'header': 'val'}

def test_convert_data_type_no_content():
    response = _create_mock_response(None, {'header': 'val'}, 204)
    response.json.side_effect = Exception('parse error')
    data = convert_response_data_type(response, return_type='snake')

    assert isinstance(data, Response)
    assert data.json_data == {}
