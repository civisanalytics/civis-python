from collections import defaultdict, OrderedDict
import pytest
from unittest import mock

from jsonref import JsonRef
from requests.exceptions import HTTPError

import civis
from civis.resources import _resources, API_SPEC
from civis.resources._resources import BASE_RESOURCES_V1


RESPONSE_DOC = (
"""Returns
-------
:class:`civis.response.Response`
    - id : integer
        The ID of the credential.
    - name : string
        The name identifying the credential
    - type : string
        The credential's type.
    - username : string
        The username for the credential.
    - description : string
        A long description of the credential.
    - owner : string
        The name of the user who this credential belongs to.
    - remote_host_id : integer
        The ID of the remote host associated with this credential.
    - remote_host_name : string
        The name of the remote host associated with this credential.
    - created_at : string/time
        The creation time for this credential.
    - updated_at : string/time
        The last modification time for this credential.""")  # noqa: E122


def test_create_method_iterator_kwarg():
    args = [{"name": 'limit', "in": 'query', "required": False, "doc": ""},
            {"name": 'page_num', "in": 'query', "required": False, "doc": ""},
            {"name": 'order', "in": 'query', "required": False, "doc": ""},
            {"name": 'order_by', "in": 'query', "required": False, "doc": ""}]
    method = _resources.create_method(args, 'get', 'mock_name', '/objects',
                                      'fake_doc')
    mock_endpoint = mock.MagicMock()

    method(mock_endpoint, iterator=True)
    mock_endpoint._call_api.assert_called_once_with(
        'get', '/objects', {}, {}, iterator=True)


def test_create_method_no_iterator_kwarg():

    # Test that dynamically-created function errors when an
    # unexpected "iterator" parameter is passed in
    args = [{"name": 'id', "in": 'query', "required": True, "doc": ""}]
    method = _resources.create_method(args, 'get', 'mock_name', '/objects',
                                      'fake_doc')
    mock_endpoint = mock.MagicMock()

    with pytest.raises(TypeError) as excinfo:
        method(mock_endpoint, id=202, iterator=True)

    assert 'keyword argument' in str(excinfo.value)

    # Dynamic functions handle optional argument through a different
    # code path; verify that this also rejects unexpected arguments.
    args2 = [{"name": 'foo', "in": 'query', "required": False, "doc": ""}]
    method2 = _resources.create_method(args2, 'get', 'mock_name', '/objects',
                                       'fake_doc')
    mock_endpoint2 = mock.MagicMock()
    with pytest.raises(TypeError) as excinfo:
        method2(mock_endpoint2, iterator=True)

    assert 'keyword argument' in str(excinfo.value)


def test_exclude_resource():
    include = "tables/"
    exclude = "excluded_in_base/"
    assert _resources.exclude_resource(exclude, "1.0", "base")
    assert not _resources.exclude_resource(include, "1.0", "base")
    assert not _resources.exclude_resource(exclude, "9.0", "base")
    assert not _resources.exclude_resource(exclude, "1.0", "all")


def test_property_type():
    prop = {"type": "array"}
    prop2 = {"type": "object"}
    prop3 = {"type": "string", "format": "date"}
    assert _resources.property_type(prop) == "list"
    assert _resources.property_type(prop2) == "dict"
    assert _resources.property_type(prop3) == "string/date"


def test_name_and_type_doc():
    prop = {"type": "string"}
    x = _resources.name_and_type_doc("A", prop, False, 0, True)
    y = _resources.name_and_type_doc("A", prop, False, 1, True)
    z = _resources.name_and_type_doc("A", prop, True, 0, False)
    assert x == "a : string, optional"
    assert y == "    - a : string, optional"
    assert z == "a : string::"


def test_docs_from_property():
    prop = {"type": "array"}
    prop2 = {"type": "object", "properties": {"A": prop}}
    x = _resources.docs_from_property("A", prop, {}, 0, True)
    y = _resources.docs_from_property("B", prop2, {}, 0, False)
    assert sorted(x) == sorted(["a : list, optional"])
    assert sorted(y) == sorted(["b : dict::", "    - a : list"])


def test_docs_from_properties():
    props = {"A": {"type": "string"}, "B": {"type": "integer"}}
    x = _resources.docs_from_properties(props, 0)
    y = _resources.docs_from_properties(props, 1)
    assert sorted(x) == sorted(['a : string', 'b : integer'])
    assert sorted(y) == sorted(['    - a : string', '    - b : integer'])


def test_deprecated_notice():
    deprecation_warning = "This endpoint is no longer supported"
    notice = _resources.deprecated_notice(deprecation_warning)

    assert "Deprecation warning!" in notice
    assert deprecation_warning in notice


def test_deprecated_notice_handles_none():
    assert _resources.deprecated_notice(None) == ""


def test_doc_from_responses():
    responses = OrderedDict([('200', OrderedDict([('description', 'success'), ('schema', OrderedDict([('type', 'array'), ('items', OrderedDict([('type', 'object'), ('properties', OrderedDict([('id', OrderedDict([('description', 'The ID of the credential.'), ('type', 'integer')])), ('name', OrderedDict([('description', 'The name identifying the credential'), ('type', 'string')])), ('type', OrderedDict([('description', "The credential's type."), ('type', 'string')])), ('username', OrderedDict([('description', 'The username for the credential.'), ('type', 'string')])), ('description', OrderedDict([('description', 'A long description of the credential.'), ('type', 'string')])), ('owner', OrderedDict([('description', 'The name of the user who this credential belongs to.'), ('type', 'string')])), ('remoteHostId', OrderedDict([('description', 'The ID of the remote host associated with this credential.'), ('type', 'integer')])), ('remoteHostName', OrderedDict([('description', 'The name of the remote host associated with this credential.'), ('type', 'string')])), ('createdAt', OrderedDict([('description', 'The creation time for this credential.'), ('type', 'string'), ('format', 'time')])), ('updatedAt', OrderedDict([('description', 'The last modification time for this credential.'), ('type', 'string'), ('format', 'time')]))]))]))]))]))])  # noqa: E501
    x = _resources.doc_from_responses(responses, False)
    assert x == RESPONSE_DOC


def test_iterable_method():
    assert _resources.iterable_method("get", ["limit", "page_num"])
    assert not _resources.iterable_method("get", ["page_num"])
    assert not _resources.iterable_method("post", ["limit", "page_num"])


def test_split_method_params():
    params = [{"name": "a", "required": True, "in": "body"},
              {"name": "b", "required": True, "in": "path"},
              {"name": "c", "required": True, "in": "query"},
              {"name": "d", "required": False, "in": "query"}]
    x = _resources.split_method_params(params)
    args, kwargs, body_params, query_params, path_params = x
    assert sorted(args) == sorted(["a", "b", "c"])
    assert kwargs == {"d": _resources.DEFAULT_STR}
    assert body_params == ["a"]
    assert sorted(query_params) == sorted(["c", "d"])
    assert path_params == ["b"]


def test_parse_param():
    param = {"name": "A", "in": "query", "required": True,
             "description": "yeah!", "type": "string"}
    x = _resources.parse_param(param)
    expected = [{'in': 'query', 'name': 'a', 'required': True,
                 'doc': 'a : string\n    yeah!\n'}]
    assert x == expected


def test_parse_params():
    param = {"name": "A", "in": "query", "required": False,
             "description": "yeah!", "type": "string"}
    param2 = {"name": "B", "in": "path", "required": True,
              "description": "nah!", "type": "integer"}
    x, y = _resources.parse_params([param, param2], "summary!", "get")
    expect_x, expect_y = ([{'in': 'query', 'doc': 'a : string, optional\n    yeah!\n', 'required': False, 'name': 'a', 'default': _resources.DEFAULT_STR}, {'in': 'path', 'doc': 'b : integer\n    nah!\n', 'required': True, 'name': 'b'}], 'summary!\n\nParameters\n----------\nb : integer\n    nah!\na : string, optional\n    yeah!\n')  # noqa: E501
    assert x == expect_x
    assert y == expect_y


def test_parse_param_body():
    expected = [{'required': False, 'name': 'a', 'in': 'body',
                 'doc': 'a : list, optional\n',
                 'default': _resources.DEFAULT_STR}]
    param_body = {"schema": {"properties": {"A": {"type": "array"}}}}
    x = _resources.parse_param_body(param_body)
    assert x == expected

    expected_with_default = [{'required': False, 'name': 'a', 'in': 'body',
                              'doc': 'a : list, optional\n', 'default': 50}]
    param_body_with_default = {"schema": {"properties": {"A": {"type": "array", "default": 50}}}}  # noqa: E501
    x_with_default = _resources.parse_param_body(param_body_with_default)
    assert x_with_default == expected_with_default


def test_parse_method_name():
    x = _resources.parse_method_name("get", "url.com/containers")
    y = _resources.parse_method_name("get", "url.com/containers/{id}")
    z = _resources.parse_method_name("get",
                                     "url.com/containers/{id}/runs/{run_id}")
    a = _resources.parse_method_name("post",
                                     "url.com/containers/{id}/runs/{run_id}")
    b = _resources.parse_method_name("get", "url.com/containers/{id}/{run_id}")
    c = _resources.parse_method_name("get",
                                     "url.com/containers/{id}/{run_id}/shares")
    assert x == "list_containers"
    assert y == "get_containers"
    assert z == "get_containers_runs"
    assert a == "post_containers_runs"
    assert b == "get_containers_id"
    assert c == "list_containers_id_shares"


def test_duplicate_names_generated_from_api_spec():
    resolved_civis_api_spec = JsonRef.replace_refs(API_SPEC)
    paths = resolved_civis_api_spec['paths']
    classes = defaultdict(list)
    for path, ops in paths.items():
        class_name, methods = _resources.parse_path(path, ops, "1.0", "all")
        method_names = [x[0] for x in methods]
        classes[class_name].extend(method_names)
    for cls, names in classes.items():
        err_msg = "Duplicate methods in {}: {}".format(cls, sorted(names))
        assert len(set(names)) == len(names), err_msg


class MockExpiredKeyResponse:
    status_code = 401


mock_str = 'civis.resources._resources.requests.Session.get'


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
    args = [{"name": 'foo', "in": 'query', "required": True, "doc": ""},
            {"name": 'bar', "in": 'query', "required": False, "doc": ""}]
    method = _resources.create_method(args, 'get', 'mock_name', '/objects',
                                      'fake_doc')
    mock_endpoint = mock.MagicMock()
    return mock_endpoint, method


def test_create_method_unexpected_kwargs():
    mock_endpoint, method = _create_mock_endpoint()

    # Method works without unexpected kwarg
    method(mock_endpoint, foo=0, bar=0)
    mock_endpoint._call_api.assert_called_once_with(
        'get', '/objects', {"foo": 0, "bar": 0}, {}, iterator=False)

    # Method raises TypeError with unexpected kwarg
    expected_msg = ("mock_name() got an unexpected keyword argument(s) "
                    "{'baz'}")
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


@mock.patch('builtins.open', new_callable=mock.mock_open,
            read_data='{"test": true}')
@mock.patch('civis.resources._resources.generate_classes', autospec=True)
@mock.patch('civis.resources._resources.parse_api_spec', autospec=True)
def test_generate_classes_maybe_cached(mock_parse, mock_gen, mock_open):
    api_key = "mock"
    api_version = "1.0"
    resources = "all"

    # Calls generate_classes when no cache is passed
    _resources.generate_classes_maybe_cached(None, api_key, api_version,
                                             resources)
    mock_gen.assert_called_once_with(api_key, api_version, resources)
    mock_gen.reset_mock()

    # Handles OrderedDict
    spec = OrderedDict({"test": True})
    _resources.generate_classes_maybe_cached(spec, api_key, api_version,
                                             resources)
    mock_parse.assert_called_once_with(spec, api_version, resources)
    assert not mock_gen.called

    # Handles str
    mock_parse.reset_mock()
    _resources.generate_classes_maybe_cached('mock', api_key, api_version,
                                             resources)
    mock_parse.assert_called_once_with(spec, api_version, resources)
    assert not mock_gen.called

    # Error when a regular dict is passed
    bad_spec = {"test": True}
    with pytest.raises(ValueError):
        _resources.generate_classes_maybe_cached(bad_spec, api_key,
                                                 api_version, resources)


@mock.patch('civis.resources._resources.parse_method', autospec=True)
def test_parse_api_spec_names(mock_method):
    """ Test that path parsing preserves underscore in resource name."""
    mock_method.return_value = ("method_a", lambda x: x)
    mock_ops = {"get": None, "post": None}
    mock_paths = {"/two_words/": mock_ops,
                  "/oneword/": mock_ops,
                  "/hyphen-words": mock_ops}
    mock_api_spec = {"paths": mock_paths}
    classes = _resources.parse_api_spec(mock_api_spec, "1.0", "all")
    assert sorted(classes.keys()) == ["hyphen_words", "oneword", "two_words"]
    assert classes["oneword"].__name__ == "Oneword"
    assert classes["two_words"].__name__ == "Two_Words"
    assert classes["hyphen_words"].__name__ == "Hyphen_Words"


def test_add_no_underscore_compatibility():
    classes = dict(match_targets=1,
                   feature_flags=2)
    new_classes = _resources._add_no_underscore_compatibility(classes)
    assert new_classes["matchtargets"] == 1
    assert new_classes["match_targets"] == 1
    assert new_classes.get("feature_flags") is None


def test_endpoints_from_base_resources_are_available_from_client():
    client = civis.APIClient(local_api_spec=API_SPEC, api_key="none")
    for endpoint in BASE_RESOURCES_V1:
        assert hasattr(client, endpoint), endpoint
