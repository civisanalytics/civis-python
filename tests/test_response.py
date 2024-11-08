import io
import pickle
import pprint
from string import ascii_lowercase
from unittest import mock

import pytest
import requests

from civis.response import (
    CivisClientError,
    PaginatedResponse,
    _response_to_json,
    convert_response_data_type,
    Response,
    CivisImmutableResponseError,
    find,
)
from civis._camel_to_snake import camel_to_snake


def _create_mock_response(data, headers):
    mock_response = mock.MagicMock(spec=requests.Response)
    mock_response.json.return_value = data
    mock_response.headers = headers
    mock_response.status_code = 200
    return mock_response


def _create_empty_response(code, headers):
    mock_response = mock.MagicMock(spec=requests.Response)
    mock_response.status_code = code
    mock_response.content = b""
    mock_response.headers = headers
    return mock_response


def _make_paginated_response(path, params):
    results = [
        [
            {"id": 1, "name": "job_1"},
            {"id": 2, "name": "job_2"},
            {"id": 3, "name": "job_3"},
        ],
        [
            {"id": 4, "name": "job_4"},
            {"id": 5, "name": "job_5"},
        ],
        [],
    ]
    mock_endpoint = mock.MagicMock()
    mock_endpoint._make_request.side_effect = [
        _create_mock_response(result, {}) for result in results
    ]
    mock_endpoint._return_type = "snake"

    paginator = PaginatedResponse(path, params, mock_endpoint)

    return paginator, mock_endpoint


def test_pagination():
    path = "/objects"
    params = {"param": "value"}
    paginator, mock_endpoint = _make_paginated_response(path, params)

    # No API calls made yet.
    mock_endpoint._make_request.assert_not_called()

    all_data = []
    for indx, obj in enumerate(paginator):
        assert obj["id"] == indx + 1
        all_data.append(obj)

        # Test lazy evaluation. Should make only make one call up until the
        # first item of the second page is needed.
        if indx < 3:
            mock_endpoint._make_request.assert_called_once_with(
                "GET", path, dict(params, **{"page_num": 1})
            )
        else:
            mock_endpoint._make_request.assert_called_with(
                "GET", path, dict(params, **{"page_num": 2})
            )

    # One extra call is made. Pagination is stopped since the response is
    # empty.
    assert mock_endpoint._make_request.call_count == 3
    assert len(all_data) == 5


def test_iterator_interface():
    # Make sure that the PaginatedResponse implements `next` as expected
    paginator, _ = _make_paginated_response("/objects", {"param": "value"})

    assert next(paginator)["id"] == 1
    assert next(paginator)["id"] == 2
    assert next(paginator)["id"] == 3
    assert next(paginator)["id"] == 4
    assert next(paginator)["id"] == 5
    with pytest.raises(StopIteration):
        next(paginator)


def test_response_to_json_no_error():
    raw_response = _create_mock_response({"key": "value"}, None)
    assert _response_to_json(raw_response) == {"key": "value"}


def test_response_to_no_content_snake():
    # Test empty response handling for codes where we're likely to see them.
    for code in [202, 204, 205]:
        raw_response = _create_empty_response(code, {"header1": "val1"})
        data = convert_response_data_type(raw_response, return_type="snake")

        assert isinstance(data, Response)
        assert data.json_data is None
        assert data.headers == {"header1": "val1"}


def test_response_to_json_parsing_error():
    raw_response = mock.MagicMock()
    raw_response.json.side_effect = ValueError("Invalid json")
    with pytest.raises(CivisClientError) as excinfo:
        _response_to_json(raw_response)
    assert "Unable to parse JSON from response" in str(excinfo.value)


def test_convert_data_type_raw_unparsed():
    response = _create_mock_response({}, {"header1": "val1"})
    data = convert_response_data_type(response, return_type="raw")

    assert isinstance(data, requests.Response)
    assert data.headers == {"header1": "val1"}


def test_convert_data_type_raw_parsed():
    response = {"foo": "bar"}
    data = convert_response_data_type(response, return_type="raw")

    assert isinstance(data, dict)
    assert data == {"foo": "bar"}


def test_convert_data_type_civis():
    response = _create_mock_response({"foo": "bar"}, {"header": "val"})
    data = convert_response_data_type(response, return_type="snake")

    assert isinstance(data, Response)
    assert data["foo"] == "bar"
    assert data.headers == {"header": "val"}


def test_convert_data_type_civis_list():
    response = _create_mock_response(
        [{"foo": "bar"}, {"fizz": "buzz"}], {"header": "val"}
    )
    data = convert_response_data_type(response, return_type="snake")

    assert isinstance(data, list)
    assert len(data) == 2
    assert isinstance(data[0], Response)
    assert data[0]["foo"] == "bar"
    assert data[0].headers == {"header": "val"}


def test_parse_column_names():
    """Check that responses that include 'update' as a key are parsed right."""
    resp_dict = {
        "columns": [
            {
                "valueDistributionPercent": {"update": 50.0, "foo": 50.0},
                "valueDistribution": {"update": 1, "foo": 1},
            }
        ]
    }
    resp = Response(resp_dict)
    assert resp.columns[0].value_distribution_percent["update"] == 50.0


@pytest.mark.parametrize(
    "headers, expected_calls_remaining, expected_rate_limit",
    [
        (None, None, None),
        ({}, None, None),
        ({"X-RateLimit-Remaining": "1", "X-RateLimit-Limit": "100"}, 1, 100),
    ],
)
def test_rate_limit(headers, expected_calls_remaining, expected_rate_limit):
    response = Response({"foo": "bar"}, headers=headers)
    assert response.headers == headers
    assert response.calls_remaining == expected_calls_remaining
    assert response.rate_limit == expected_rate_limit


def test_response_is_immutable():
    """Test that the Response object is immutable.

    Resolves https://github.com/civisanalytics/civis-python/issues/228
    """
    # JSON data from the Civis API is in camelCase.
    json_data = {"fooBar": {"barBaz": "whatever"}}
    response = Response(json_data)
    assert response.json_data == json_data

    with pytest.raises(CivisImmutableResponseError):
        response["foo_bar"] = "something else"
    with pytest.raises(CivisImmutableResponseError):
        response.foo_bar = "something else"
    with pytest.raises(CivisImmutableResponseError):
        response["foo_bar"]["bar_baz"] = "something else"
    with pytest.raises(CivisImmutableResponseError):
        response.foo_bar.bar_baz = "something else"


def test_response_cross_compatibility():
    """Test cross compatibility: snake vs camel case, getitem vs getattr.

    Resolves https://github.com/civisanalytics/civis-python/issues/317
    """
    msg = "Life is a long journey. Enjoy it!"
    # JSON data from the Civis API is in camelCase.
    json_data = {"fooBar": {"barBaz": msg}}
    response = Response(json_data)
    assert response.json_data == json_data

    #   16 combinations altogether
    # = 2 ** 4
    # = {2 levels deep under the response} ** {snake/camel * getitem/getattr}
    assert (
        msg
        == response.foo_bar.bar_baz
        == response.foo_bar["bar_baz"]
        == response.foo_bar.barBaz
        == response.foo_bar["barBaz"]
        == response["foo_bar"].bar_baz
        == response["foo_bar"]["bar_baz"]
        == response["foo_bar"].barBaz
        == response["foo_bar"]["barBaz"]
        == response.fooBar.bar_baz
        == response.fooBar["bar_baz"]
        == response.fooBar.barBaz
        == response.fooBar["barBaz"]
        == response["fooBar"].bar_baz
        == response["fooBar"]["bar_baz"]
        == response["fooBar"].barBaz
        == response["fooBar"]["barBaz"]
    )


@pytest.mark.parametrize("key", ["arguments", "environmentVariables"])
def test_response_keys_preserve_case(key):
    json_data = {key: {"FOO": 123, "FOO_BAR": 456}}
    response = Response(json_data)
    resp = getattr(response, camel_to_snake(key))
    assert resp.FOO == resp["FOO"] == 123
    assert resp.FOO_BAR == resp["FOO_BAR"] == 456
    with pytest.raises(AttributeError):
        resp.foo
    with pytest.raises(KeyError):
        resp["foo"]
    with pytest.raises(AttributeError):
        resp.foo_bar
    with pytest.raises(KeyError):
        resp["foo_bar"]


@pytest.mark.parametrize(
    "source, as_snake_case",
    [
        ({"foo": {"barBar": 1}}, {"foo": {"bar_bar": 1}}),
        (
            {"fooBar": 1, "arguments": {"FOO": 2, "FOO_BAR": 3}},
            {"foo_bar": 1, "arguments": {"FOO": 2, "FOO_BAR": 3}},
        ),
        (
            {"fooBar": [{"name": "a", "type": "b"}, {"name": "c", "type": "d"}]},
            {"foo_bar": [{"name": "a", "type": "b"}, {"name": "c", "type": "d"}]},
        ),
        ({"fooBar": ["a", "b", "c"]}, {"foo_bar": ["a", "b", "c"]}),
    ],
)
def test_json(source, as_snake_case):
    response = Response(source)
    assert response.json() == as_snake_case
    assert response.json(snake_case=False) == source


def test_json_no_data():
    response = Response(None)
    assert response.json() == {}
    assert response.json(snake_case=False) == {}


@pytest.mark.parametrize("snake_case", [True, False])
def test_json_preserve_original_dict(snake_case):
    # User may want to modify the dict from response.json().
    # Make sure the dict from response.json() is not the same object as
    # the original dict passed to Response.
    json_data = {"foo": 123, "bar": 456}
    id_original = id(json_data)
    response = Response(json_data)
    id_in_response = id(response.json(snake_case=snake_case))
    assert id_original != id_in_response


@pytest.mark.parametrize(
    "json_data, expected_length",
    [
        ({}, 0),
        ({"foo": 123}, 1),
        ({"foo": 123, "bar": 456}, 2),
    ],
)
def test_len(json_data, expected_length):
    response = Response(json_data)
    assert response.json_data == json_data
    assert len(response) == expected_length


@pytest.mark.parametrize(
    "json_data, expected_repr",
    [
        (None, "Response({})"),
        ({}, "Response({})"),
        ({"foo": 123}, "Response({'foo': 123})"),
        ({"foo": {"barBaz": 456}}, "Response({'foo': Response({'bar_baz': 456})})"),
        # repr() call doesn't wrap long lines.
        (
            {
                "foo": {ascii_lowercase[i]: i for i in range(3)},
                "fooBar": {ascii_lowercase[i]: i for i in range(15)},
            },
            "Response({'foo': Response({'a': 0, 'b': 1, 'c': 2}), 'foo_bar': Response({'a': 0, 'b': 1, 'c': 2, 'd': 3, 'e': 4, 'f': 5, 'g': 6, 'h': 7, 'i': 8, 'j': 9, 'k': 10, 'l': 11, 'm': 12, 'n': 13, 'o': 14})})",  # noqa: E501
        ),
    ],
)
def test_repr(json_data, expected_repr):
    response = Response(json_data)
    assert response.json_data == json_data
    assert repr(response) == expected_repr


def test_repr_with_empty_string_key():
    response = Response({"": 123})
    assert repr(response) == "Response({'': 123})"


@pytest.mark.parametrize(
    "json_data, expected",
    [
        # A "short" response's pprint looks the same as its repr.
        ({"foo": {"barBaz": 456}}, "Response({'foo': Response({'bar_baz': 456})})"),
        # A "long" response's pprint triggers line wrapping in pretty-printing.
        (
            {ascii_lowercase[i]: i for i in range(15)},
            "Response({'a': 0,\n"
            "          'b': 1,\n"
            "          'c': 2,\n"
            "          'd': 3,\n"
            "          'e': 4,\n"
            "          'f': 5,\n"
            "          'g': 6,\n"
            "          'h': 7,\n"
            "          'i': 8,\n"
            "          'j': 9,\n"
            "          'k': 10,\n"
            "          'l': 11,\n"
            "          'm': 12,\n"
            "          'n': 13,\n"
            "          'o': 14})",
        ),
        (
            {
                "foo": {ascii_lowercase[i]: i for i in range(3)},
                "fooBar": {ascii_lowercase[i]: i for i in range(15)},
            },
            "Response({'foo': Response({'a': 0, 'b': 1, 'c': 2}),\n"
            "          'foo_bar': Response({'a': 0,\n"
            "                               'b': 1,\n"
            "                               'c': 2,\n"
            "                               'd': 3,\n"
            "                               'e': 4,\n"
            "                               'f': 5,\n"
            "                               'g': 6,\n"
            "                               'h': 7,\n"
            "                               'i': 8,\n"
            "                               'j': 9,\n"
            "                               'k': 10,\n"
            "                               'l': 11,\n"
            "                               'm': 12,\n"
            "                               'n': 13,\n"
            "                               'o': 14})})",
        ),
    ],
)
def test_pprint(json_data, expected):
    response = Response(json_data)
    assert pprint.pformat(response) == expected


def test_jsonvalue_as_run_output():
    json_data = {"objectType": "JSONValue", "value": {"foo": 456}}
    response = Response(json_data)
    value = response.value
    assert isinstance(value, dict)
    assert value == {"foo": 456}


def test_jsonvalue_as_response():
    # json_data comes from a client.json_values.{get,post,patch} call.
    json_data = {"id": 12345, "name": "JSON Value 12345", "value": {"a": 123}}
    response = Response(json_data, from_json_values=True)
    value = response.value
    assert isinstance(value, dict)
    assert value == {"a": 123}


def test_get():
    # JSON data from the Civis API is in camelCase.
    json_data = {"foo": 123, "bar": {"bazQux": 456}}
    response = Response(json_data)
    assert response.json_data == json_data
    assert response.get("foo") == 123
    assert response.bar.get("baz_qux") == response.bar.get("bazQux") == 456
    assert response.get("doesn't exist") is None
    assert response.get("doesn't exist", "fallback value") == "fallback value"
    assert response.bar.get("doesn't exist") is None


def test_items():
    # JSON data from the Civis API is in camelCase.
    json_data = {"foo": 123, "bar": {"bazQux": 456}}
    response = Response(json_data)
    assert response.json_data == json_data
    for k, v in response.items():
        assert (k, v) in (("foo", 123), ("bar", Response({"bazQux": 456})))


def test_eq():
    # JSON data from the Civis API is in camelCase.
    json_data = {"foo": 123, "bar": {"bazQux": 456}}
    response = Response(json_data)

    response2 = Response(json_data)
    assert response == response2
    assert response == {"foo": 123, "bar": {camel_to_snake("bazQux"): 456}}

    for not_response_or_dict in (789, "str", ["list"], ("tuple",), {"set"}, None):
        assert response != not_response_or_dict


def test_response_is_pickleable():
    # JSON data from the Civis API is in camelCase.
    json_data = {"foo": 123, "bar": 456}
    response = Response(json_data)
    pickled = pickle.dumps(response)
    unpickled = pickle.load(io.BytesIO(pickled))
    assert response == unpickled


def test_find_filter_with_kwargs():
    r1 = Response({"foo": 0, "bar": "a", "baz": True})
    r2 = Response({"foo": 1, "bar": "b", "baz": True})
    r3 = Response({"foo": 2, "bar": "b", "baz": False})

    assert find([r1, r2, r3], wrong_attr="whatever") == []

    assert find([r1, r2, r3], foo=0) == [r1]
    assert find([r1, r2, r3], foo=1) == [r2]
    assert find([r1, r2, r3], foo=1, bar="b") == [r2]

    assert find([r1, r2, r3], bar="b") == [r2, r3]
    assert find([r1, r2, r3], bar="b", foo=1) == [r2]

    assert find([r1, r2, r3], foo=True) == []
    assert find([r1, r2, r3], foo=False) == []
    assert find([r1, r2, r3], bar=True) == []
    assert find([r1, r2, r3], bar=False) == []
    assert find([r1, r2, r3], baz=True) == [r1, r2]
    assert find([r1, r2, r3], baz=False) == [r3]

    assert find([r1, r2, r3], foo=int) == [r2, r3]
