import json
import os
import tempfile
import time
from collections import defaultdict, OrderedDict
from unittest import mock

import pytest
from jsonref import JsonRef
from requests.exceptions import HTTPError

from civis.base import Endpoint
from civis.resources import _resources, API_SPEC_PATH
from civis.resources._client_pyi import generate_client_pyi, CLIENT_PYI_PATH
from civis.tests import create_client_mock


with open(API_SPEC_PATH) as f:
    API_SPEC = json.load(f, object_pairs_hook=OrderedDict)


RESPONSE_DOC = """Returns
-------
:class:`civis.Response`
    - id : int
        The ID of the credential.
    - name : str
        The name identifying the credential
    - type : str
        The credential's type.
    - username : str
        The username for the credential.
    - description : str
        A long description of the credential.
    - owner : str
        The name of the user who this credential belongs to.
    - remote_host_id : int
        The ID of the remote host associated with this credential.
    - remote_host_name : str
        The name of the remote host associated with this credential.
    - created_at : str (time)
        The creation time for this credential.
    - updated_at : str (time)
        The last modification time for this credential."""  # noqa: E122


def test_create_method_iterator_kwarg():
    args = [
        {"name": "limit", "in": "query", "required": False, "doc": ""},
        {"name": "page_num", "in": "query", "required": False, "doc": ""},
        {"name": "order", "in": "query", "required": False, "doc": ""},
        {"name": "order_by", "in": "query", "required": False, "doc": ""},
    ]
    method = _resources.create_method(
        args,
        "get",
        "mock_name",
        "/objects",
        "deprecation",
        "param_doc",
        "resp_doc",
        "return_annotation",
    )
    mock_endpoint = mock.MagicMock()

    method(mock_endpoint, iterator=True)
    mock_endpoint._call_api.assert_called_once_with(
        "get", "/objects", {}, {}, "deprecation", iterator=True
    )


def test_create_method_no_iterator_kwarg():

    # Test that dynamically-created function errors when an
    # unexpected "iterator" parameter is passed in
    args = [{"name": "id", "in": "query", "required": True, "doc": ""}]
    method = _resources.create_method(
        args,
        "get",
        "mock_name",
        "/objects",
        "deprecation",
        "param_doc",
        "resp_doc",
        "return_annotation",
    )
    mock_endpoint = mock.MagicMock()

    with pytest.raises(TypeError) as excinfo:
        method(mock_endpoint, id=202, iterator=True)

    assert "keyword argument" in str(excinfo.value)

    # Dynamic functions handle optional argument through a different
    # code path; verify that this also rejects unexpected arguments.
    args2 = [{"name": "foo", "in": "query", "required": False, "doc": ""}]
    method2 = _resources.create_method(
        args2,
        "get",
        "mock_name",
        "/objects",
        "deprecation",
        "param_doc",
        "resp_doc",
        "return_annotation",
    )
    mock_endpoint2 = mock.MagicMock()
    with pytest.raises(TypeError) as excinfo:
        method2(mock_endpoint2, iterator=True)

    assert "keyword argument" in str(excinfo.value)


def test_exclude_resource():
    include = "tables/"
    exclude = "feature_flags/"
    assert _resources.exclude_resource(exclude, "1.0")
    assert not _resources.exclude_resource(include, "1.0")


def test_property_type():
    prop = {"type": "array"}
    prop2 = {"type": "object"}
    prop3 = {"type": "string", "format": "date"}
    assert _resources.property_type(prop) == "List"
    assert _resources.property_type(prop2) == "dict"
    assert _resources.property_type(prop3) == "str (date)"
    assert _resources.property_type(prop3, get_format=False) == "str"


def test_name_and_type_doc():
    prop = {"type": "string"}
    x = _resources.name_and_type_doc("A", prop, 0, True)
    y = _resources.name_and_type_doc("A", prop, 1, True)
    z = _resources.name_and_type_doc("A", prop, 0, False)
    assert x == "a : str, optional"
    assert y == "    - a : str, optional"
    assert z == "a : str"


def test_docs_from_property():
    prop = {"type": "array"}
    prop2 = {"type": "object", "properties": {"A": prop}}
    x = _resources.docs_from_property("A", prop, {}, 0, True)
    y = _resources.docs_from_property("B", prop2, {}, 0, False)
    assert sorted(x) == sorted(["a : List, optional"])
    assert sorted(y) == sorted(["b : dict", "    - a : List"])


def test_docs_from_properties():
    props = {"A": {"type": "string"}, "B": {"type": "integer"}}
    x = _resources.docs_from_properties(props, 0)
    y = _resources.docs_from_properties(props, 1)
    assert sorted(x) == sorted(["a : str", "b : int"])
    assert sorted(y) == sorted(["    - a : str", "    - b : int"])


def test_deprecated_notice():
    deprecation_warning = "This endpoint is no longer supported"
    notice = _resources.deprecated_notice(deprecation_warning)

    assert ".. warning::" in notice
    assert deprecation_warning in notice


def test_deprecated_notice_handles_none():
    assert _resources.deprecated_notice(None) == ""


def test_doc_from_responses():
    responses = OrderedDict(
        [
            (
                "200",
                OrderedDict(
                    [
                        ("description", "success"),
                        (
                            "schema",
                            OrderedDict(
                                [
                                    ("type", "array"),
                                    (
                                        "items",
                                        OrderedDict(
                                            [
                                                ("type", "object"),
                                                (
                                                    "properties",
                                                    OrderedDict(
                                                        [
                                                            (
                                                                "id",
                                                                OrderedDict(
                                                                    [
                                                                        (
                                                                            "description",  # noqa: E501
                                                                            "The ID of the credential.",  # noqa: E501
                                                                        ),
                                                                        (
                                                                            "type",
                                                                            "integer",
                                                                        ),
                                                                    ]
                                                                ),
                                                            ),
                                                            (
                                                                "name",
                                                                OrderedDict(
                                                                    [
                                                                        (
                                                                            "description",  # noqa: E501
                                                                            "The name identifying the credential",  # noqa: E501
                                                                        ),
                                                                        (
                                                                            "type",
                                                                            "string",
                                                                        ),
                                                                    ]
                                                                ),
                                                            ),
                                                            (
                                                                "type",
                                                                OrderedDict(
                                                                    [
                                                                        (
                                                                            "description",  # noqa: E501
                                                                            "The credential's type.",  # noqa: E501
                                                                        ),
                                                                        (
                                                                            "type",
                                                                            "string",
                                                                        ),
                                                                    ]
                                                                ),
                                                            ),
                                                            (
                                                                "username",
                                                                OrderedDict(
                                                                    [
                                                                        (
                                                                            "description",  # noqa: E501
                                                                            "The username for the credential.",  # noqa: E501
                                                                        ),
                                                                        (
                                                                            "type",
                                                                            "string",
                                                                        ),
                                                                    ]
                                                                ),
                                                            ),
                                                            (
                                                                "description",
                                                                OrderedDict(
                                                                    [
                                                                        (
                                                                            "description",  # noqa: E501
                                                                            "A long description of the credential.",  # noqa: E501
                                                                        ),
                                                                        (
                                                                            "type",
                                                                            "string",
                                                                        ),
                                                                    ]
                                                                ),
                                                            ),
                                                            (
                                                                "owner",
                                                                OrderedDict(
                                                                    [
                                                                        (
                                                                            "description",  # noqa: E501
                                                                            "The name of the user who this credential belongs to.",  # noqa: E501
                                                                        ),
                                                                        (
                                                                            "type",
                                                                            "string",
                                                                        ),
                                                                    ]
                                                                ),
                                                            ),
                                                            (
                                                                "remoteHostId",
                                                                OrderedDict(
                                                                    [
                                                                        (
                                                                            "description",  # noqa: E501
                                                                            "The ID of the remote host associated with this credential.",  # noqa: E501
                                                                        ),
                                                                        (
                                                                            "type",
                                                                            "integer",
                                                                        ),
                                                                    ]
                                                                ),
                                                            ),
                                                            (
                                                                "remoteHostName",
                                                                OrderedDict(
                                                                    [
                                                                        (
                                                                            "description",  # noqa: E501
                                                                            "The name of the remote host associated with this credential.",  # noqa: E501
                                                                        ),
                                                                        (
                                                                            "type",
                                                                            "string",
                                                                        ),
                                                                    ]
                                                                ),
                                                            ),
                                                            (
                                                                "createdAt",
                                                                OrderedDict(
                                                                    [
                                                                        (
                                                                            "description",  # noqa: E501
                                                                            "The creation time for this credential.",  # noqa: E501
                                                                        ),
                                                                        (
                                                                            "type",
                                                                            "string",
                                                                        ),
                                                                        (
                                                                            "format",
                                                                            "time",
                                                                        ),
                                                                    ]
                                                                ),
                                                            ),
                                                            (
                                                                "updatedAt",
                                                                OrderedDict(
                                                                    [
                                                                        (
                                                                            "description",  # noqa: E501
                                                                            "The last modification time for this credential.",  # noqa: E501
                                                                        ),
                                                                        (
                                                                            "type",
                                                                            "string",
                                                                        ),
                                                                        (
                                                                            "format",
                                                                            "time",
                                                                        ),
                                                                    ]
                                                                ),
                                                            ),
                                                        ]
                                                    ),
                                                ),
                                            ]
                                        ),
                                    ),
                                ]
                            ),
                        ),
                    ]
                ),
            )
        ]
    )  # noqa: E501
    x = _resources.doc_from_responses(responses, False)
    assert x == RESPONSE_DOC


def test_type_from_param():
    params = [
        {"type": "string"},
        {"type": "integer"},
        {"type": "array"},
        {"type": "array", "items": {"type": "integer"}},
        {"type": "array", "items": {"$ref": "#/definitions/Object0"}},
        {"type": "object"},
    ]
    assert _resources.type_from_param(params[0]) == "str"
    assert _resources.type_from_param(params[1]) == "int"
    assert _resources.type_from_param(params[2]) == "List"
    assert _resources.type_from_param(params[3]) == "List[int]"
    assert _resources.type_from_param(params[4]) == "List[dict]"
    assert _resources.type_from_param(params[4], skip_dict_item_type=True) == "List"
    assert _resources.type_from_param(params[5]) == "dict"
    assert (
        _resources.type_from_param(params[5], in_returned_object=True)
        == ":class:`civis.Response`"
    )  # noqa: E501


def test_iterable_method():
    assert _resources.iterable_method("get", ["limit", "page_num"])
    assert not _resources.iterable_method("get", ["page_num"])
    assert not _resources.iterable_method("post", ["limit", "page_num"])


def test_split_method_params():
    params = [
        {"name": "a", "required": True, "in": "body", "type": "integer"},
        {"name": "b", "required": True, "in": "path", "type": "integer"},
        {"name": "c", "required": True, "in": "query", "type": "string"},
        {"name": "d", "required": False, "in": "query", "type": "string"},
    ]
    x = _resources.split_method_params(params)
    args, kwargs, body_params, query_params, path_params = x
    assert sorted(args.keys()) == sorted(["a", "b", "c"])
    assert args["a"] == {"type": "integer"}
    assert kwargs == {"d": {"default": _resources.DEFAULT_ARG_VALUE, "type": "string"}}
    assert body_params == ["a"]
    assert sorted(query_params) == sorted(["c", "d"])
    assert path_params == ["b"]


def test_parse_param():
    param = {
        "name": "A",
        "in": "query",
        "required": True,
        "description": "yeah!",
        "type": "string",
    }
    x = _resources.parse_param(param)
    expected = [
        {
            "in": "query",
            "name": "a",
            "required": True,
            "doc": "a : str\n    yeah!\n",
            "type": "str",
        }
    ]
    assert x == expected


def test_parse_params():
    param = {
        "name": "A",
        "in": "query",
        "required": False,
        "description": "yeah!",
        "type": "string",
    }
    param2 = {
        "name": "B",
        "in": "path",
        "required": True,
        "description": "nah!",
        "type": "integer",
    }
    x, y = _resources.parse_params([param, param2], "summary!", "get")
    expect_x = [
        {
            "in": "query",
            "doc": "a : str, optional\n    yeah!\n",
            "required": False,
            "name": "a",
            "default": _resources.DEFAULT_ARG_VALUE,
            "type": "str",
        },
        {
            "in": "path",
            "doc": "b : int\n    nah!\n",
            "required": True,
            "name": "b",
            "type": "int",
        },
    ]
    expect_y = (
        "summary!\n\n"
        "Parameters\n"
        "----------\n"
        "b : int\n"
        "    nah!\n"
        "a : str, optional\n"
        "    yeah!\n"
    )
    assert x == expect_x
    assert y == expect_y


def test_parse_param_body():
    expected = [
        {
            "required": False,
            "name": "a",
            "in": "body",
            "doc": "a : List, optional\n",
            "default": _resources.DEFAULT_ARG_VALUE,
            "type": "List",
        }
    ]
    param_body = {"schema": {"properties": {"A": {"type": "array"}}}}
    x = _resources.parse_param_body(param_body)
    assert x == expected

    expected_with_default = [
        {
            "required": False,
            "name": "a",
            "in": "body",
            "doc": "a : List, optional\n",
            "default": 50,
            "type": "List",
        }
    ]
    param_body_with_default = {
        "schema": {"properties": {"A": {"type": "array", "default": 50}}}
    }  # noqa: E501
    x_with_default = _resources.parse_param_body(param_body_with_default)
    assert x_with_default == expected_with_default


def test_parse_method_name():
    x = _resources.parse_method_name("get", "url.com/containers")
    y = _resources.parse_method_name("get", "url.com/containers/{id}")
    z = _resources.parse_method_name("get", "url.com/containers/{id}/runs/{run_id}")
    a = _resources.parse_method_name("post", "url.com/containers/{id}/runs/{run_id}")
    b = _resources.parse_method_name("get", "url.com/containers/{id}/{run_id}")
    c = _resources.parse_method_name("get", "url.com/containers/{id}/{run_id}/shares")
    assert x == "list_containers"
    assert y == "get_containers"
    assert z == "get_containers_runs"
    assert a == "post_containers_runs"
    assert b == "get_containers_id"
    assert c == "list_containers_id_shares"


def test_duplicate_names_generated_from_api_spec():
    resolved_civis_api_spec = JsonRef.replace_refs(API_SPEC)
    paths = resolved_civis_api_spec["paths"]
    classes = defaultdict(list)
    for path, ops in paths.items():
        class_name, methods = _resources.parse_path(path, ops, "1.0")
        method_names = [x[0] for x in methods]
        classes[class_name].extend(method_names)
    for cls, names in classes.items():
        err_msg = "Duplicate methods in {}: {}".format(cls, sorted(names))
        assert len(set(names)) == len(names), err_msg


class MockExpiredKeyResponse:
    status_code = 401


mock_str = "civis.resources._resources.requests.Session.get"


@mock.patch(mock_str, return_value=MockExpiredKeyResponse)
def test_expired_api_key(mock_response):
    msg = "401 error downloading API specification. API key may be expired."
    http_error_raised = False
    try:
        _resources.get_api_spec("expired_key", "1.0")
    except HTTPError as err:
        http_error_raised = True
        assert str(err) == msg
    assert http_error_raised


def _create_mock_endpoint():
    args = [
        {"name": "foo", "in": "query", "required": True, "doc": ""},
        {"name": "bar", "in": "query", "required": False, "doc": ""},
    ]
    method = _resources.create_method(
        args,
        "get",
        "mock_name",
        "/objects",
        "deprecation",
        "param_doc",
        "resp_doc",
        "return_annotation",
    )
    mock_endpoint = mock.MagicMock()
    return mock_endpoint, method


def test_create_method_unexpected_kwargs():
    mock_endpoint, method = _create_mock_endpoint()

    # Method works without unexpected kwarg
    method(mock_endpoint, foo=0, bar=0)
    mock_endpoint._call_api.assert_called_once_with(
        "get", "/objects", {"foo": 0, "bar": 0}, {}, "deprecation", iterator=False
    )

    # Method raises TypeError with unexpected kwarg
    expected_msg = "mock_name() got an unexpected keyword argument(s) " "{'baz'}"
    with pytest.raises(TypeError) as excinfo:
        method(mock_endpoint, foo=0, bar=0, baz=0)
    assert str(excinfo.value) == expected_msg


def test_create_method_too_many_pos_args():
    mock_endpoint, method = _create_mock_endpoint()

    # Method raises TypeError with too many arguments
    with pytest.raises(TypeError) as excinfo:
        method(mock_endpoint, 0, 0, 0)
    assert str(excinfo.value) == "too many positional arguments"


def test_create_method_multiple_values():
    mock_endpoint, method = _create_mock_endpoint()

    # Method raises TypeError with multiple values for arguments
    with pytest.raises(TypeError) as excinfo:
        method(mock_endpoint, 0, foo=0)
    assert str(excinfo.value) == "multiple values for argument 'foo'"


def test_create_method_keyword_only():
    # Verify that optional arguments are keyword-only
    # (This language feature is only present in Python 3)
    mock_endpoint, method = _create_mock_endpoint()

    with pytest.raises(TypeError) as excinfo:
        method(mock_endpoint, 0, 0)
    assert str(excinfo.value) == "too many positional arguments"


def test_create_method_deprecation_warning():
    args = [{"name": "foo", "in": "query", "required": True, "doc": ""}]
    method = _resources.create_method(
        args,
        "get",
        "mock_name",
        "/objects",
        "deprecation",
        "param_doc",
        "resp_doc",
        "return_annotation",
    )
    mock_endpoint = Endpoint({"api_key": "abc"}, client=create_client_mock())
    mock_endpoint._make_request = mock.Mock()

    with pytest.warns(FutureWarning, match="deprecation"):
        method(mock_endpoint, foo=0)


@mock.patch("builtins.open", new_callable=mock.mock_open, read_data='{"test": true}')
@mock.patch("civis.resources._resources.generate_classes", autospec=True)
@mock.patch("civis.resources._resources.parse_api_spec", autospec=True)
def test_generate_classes_maybe_cached(mock_parse, mock_gen, mock_open):
    api_key = "mock"
    api_version = "1.0"

    # Calls generate_classes when no cache is passed
    _resources.generate_classes_maybe_cached(None, api_key, api_version)
    mock_gen.assert_called_once_with(api_key, api_version)
    mock_gen.reset_mock()

    # Handles OrderedDict
    spec = OrderedDict({"test": True})
    _resources.generate_classes_maybe_cached(spec, api_key, api_version)
    mock_parse.assert_called_once_with(spec, api_version)
    assert not mock_gen.called

    # Handles str
    mock_parse.reset_mock()
    _resources.generate_classes_maybe_cached("mock", api_key, api_version)
    mock_parse.assert_called_once_with(spec, api_version)
    assert not mock_gen.called

    # Error when a regular dict is passed
    bad_spec = {"test": True}
    with pytest.raises(ValueError):
        _resources.generate_classes_maybe_cached(bad_spec, api_key, api_version)


@mock.patch("civis.resources._resources.parse_method", autospec=True)
def test_parse_api_spec_names(mock_method):
    """Test that path parsing preserves underscore in resource name."""
    mock_method.return_value = ("method_a", lambda x: x)
    mock_ops = {"get": None, "post": None}
    mock_paths = {
        "/two_words/": mock_ops,
        "/oneword/": mock_ops,
        "/hyphen-words": mock_ops,
    }
    mock_api_spec = {"paths": mock_paths}
    classes = _resources.parse_api_spec(mock_api_spec, "test_api_version")
    assert sorted(classes.keys()) == ["hyphen_words", "oneword", "two_words"]
    assert classes["oneword"].__name__ == "Oneword"
    assert classes["two_words"].__name__ == "Two_Words"
    assert classes["hyphen_words"].__name__ == "Hyphen_Words"


def test_client_pyi_matches_api_spec():
    with tempfile.TemporaryDirectory() as temp_dir:
        test_client_pyi_path = os.path.join(temp_dir, "test_client.pyi")
        generate_client_pyi(test_client_pyi_path, API_SPEC_PATH)
        actual = open(CLIENT_PYI_PATH).read()
        expected = open(test_client_pyi_path).read()
        match = expected == actual
        # Avoid the more direct `assert expected == actual`,
        # or else pytest would print the unwieldy, long diffs for a mismatch.
        assert match, (
            "client.pyi doesn't match the API spec in the codebase. "
            "Run tools/update_civis_api_spec.py."
        )


@mock.patch("civis.resources._resources.generate_classes")
def test_generate_classes_with_ttl_cache__base_case(mock_gen):
    # Calling generate_classes_maybe_cached multiple times should only call
    # generate_classes once.
    for _ in range(5):
        _resources.generate_classes_maybe_cached(None, "api-key", "1.0")
    assert mock_gen.call_count == 1


@mock.patch("civis.resources._resources.generate_classes")
def test_generate_classes_with_ttl_cache__force_clear_cache(mock_gen):
    # Check that we can force clear the cache.
    for _ in range(5):
        _resources.generate_classes_maybe_cached(
            None, "api-key", "1.0", force_refresh_api_spec=True
        )
    assert mock_gen.call_count == 5


@mock.patch("civis.resources._resources._spec_expire_time")
@mock.patch("civis.resources._resources.generate_classes")
def test_generate_classes_with_ttl_cache__expire_cache(mock_gen, mock_expire_time):
    # Check that we can expire the cache.
    mock_expire_time.return_value = 0.001
    for _ in range(5):
        _resources.generate_classes_maybe_cached(None, "api-key", "1.0")
        # Sleep the same amount of time as the mock_expire_time,
        # so that we should trigger the cache clearing at every iteration.
        time.sleep(0.001)
    assert mock_gen.call_count == 5


@pytest.mark.parametrize("source, expected", [("ab_cd", "AbCd"), ("", "")])
def test_snake_to_camel(source, expected):
    assert _resources._snake_to_camel(source) == expected
