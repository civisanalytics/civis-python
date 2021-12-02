from collections import OrderedDict
import json
import os
from unittest import mock

import pytest

from civis.cli.__main__ import generate_cli, invoke, make_operation_name
from civis.cli._cli_commands import _str_table_result
from civis.resources import API_SPEC_PATH

THIS_DIR = os.path.dirname(os.path.realpath(__file__))


@mock.patch("civis.cli.__main__.add_extra_commands")
@mock.patch("civis.cli.__main__.retrieve_spec_dict")
def test_generate_cli_petstore(mock_retrieve_spec_dict,
                               mock_add_extra_commands):
    """Test loading the OpenAPI petstore example."""

    # From https://raw.githubusercontent.com/OAI/OpenAPI-Specification/4b1c1167b99844fd3ca19dc0055bbdb0c5eff094/examples/v2.0/json/petstore.json  # noqa: E501
    with open(os.path.join(THIS_DIR, "petstore.json")) as f:
        petstore_spec = json.load(f)

    mock_retrieve_spec_dict.return_value = petstore_spec
    cli = generate_cli()
    assert set(cli.commands.keys()) == {'pets'}
    assert set(cli.commands['pets'].commands.keys()) == {'list', 'post', 'get'}
    assert ({x.name for x in cli.commands['pets'].commands['list'].params} ==
            {'limit', 'json_output'})


@mock.patch("civis.cli.__main__.retrieve_spec_dict")
def test_generate_cli_civis(mock_retrieve_spec_dict):
    """Test loading the Civis API spec as of 2021-12-02."""
    with open(API_SPEC_PATH) as f:
        civis_spec = json.load(f, object_pairs_hook=OrderedDict)
    mock_retrieve_spec_dict.return_value = civis_spec

    with pytest.warns(None) as warn_rec:
        cli = generate_cli()
    assert len(warn_rec) == 0

    # Check a regular command.
    list_runs_cmd = cli.commands['scripts'].commands['list-containers-runs']
    expected_names = {'id', 'limit', 'page_num', 'order', 'order_dir',
                      'json_output'}
    assert {_.name for _ in list_runs_cmd.params} == expected_names
    assert list_runs_cmd.params[0].name == 'id'
    assert list_runs_cmd.params[0].required
    assert list_runs_cmd.params[1].name == 'limit'
    assert not list_runs_cmd.params[1].required

    # Check that the extra files upload command was added
    expected_names = {'path', 'name', 'expires_at'}
    files_upload_params = cli.commands['files'].commands['upload'].params
    assert {_.name for _ in files_upload_params} == expected_names

    # Check that the POST queries command, which uses an object in the body,
    # was parsed properly.
    pq_params = cli.commands['queries'].commands['post'].params
    expected_names = {
        'column_delimiter', 'compression', 'credential', 'database',
        'filename_prefix', 'include_header', 'interactive', 'preview_rows',
        'sql', 'unquoted', 'json_output', 'hidden'
    }
    assert {_.name for _ in pq_params} == expected_names
    for p in pq_params:
        if p.name == 'filename_prefix':
            assert not p.required
        if p.name == 'database':
            assert p.required


@mock.patch("civis.cli.__main__.open_session", autospec=True)
def test_blank_output(mock_session):
    """
    Test that endpoints that return blank results don't cause exceptions.
    """
    # The response object's json method will raise a ValueError when the output
    # is blank.
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.side_effect = ValueError()
    session_context.send.return_value.status_code = 200

    op = {"parameters": []}
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        invoke("WIBBLE", "/wobble/wubble", op)

    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0


@mock.patch("civis.cli.__main__.open_session", autospec=True)
def test_failure_exit_code(mock_session):
    """
    Test that we return a nonzero exit code when the API request fails.
    """
    # first test that we get a zero exit code when the API request succeeds
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.side_effect = ValueError()
    session_context.send.return_value.status_code = 200

    op = {"parameters": []}

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        invoke("WIBBLE", "/wobble/wubble", op)
    assert pytest_wrapped_e.value.code == 0

    # now test that we get a nonzero exit code when the API request fails
    session_context.send.return_value.status_code = 404

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        invoke("WIBBLE", "/wobble/wubble", op)
    assert pytest_wrapped_e.value.code != 0


@mock.patch("civis.cli.__main__.open_session", autospec=True)
@mock.patch("civis.cli.__main__.Request", autospec=True)
def test_parameter_case(mock_request, mock_session):
    """
    Test that parameter names are sent in camelCase rather than snake_case.
    """
    api_response = {'key': 'value'}
    session_context = mock_session.return_value.__enter__.return_value
    session_context.send.return_value.json.return_value = api_response
    session_context.send.return_value.status_code = 200

    # To avoid needing CIVIS_API_KEY set in the environment.
    op = {"parameters": [{'name': 'firstParameter', 'in': 'query'},
                         {'name': 'secondParameter', 'in': 'query'}]}
    with pytest.raises(SystemExit):
        invoke("WIBBLE", "/wobble/wubble", op,
               first_parameter='a', second_parameter='b')

    mock_session.call_args[1]['user_agent'] = 'civis-cli'

    mock_request.assert_called_with(
        url='https://api.civisanalytics.com/wobble/wubble',
        json={},
        params={'firstParameter': 'a', 'secondParameter': 'b'},
        method='WIBBLE')


@pytest.mark.parametrize(
    "path,method,resource_name,exp",
    [('/imports/files/{id}/runs/{run_id}', 'get', 'imports', 'get-files-runs'),
     ('/aliases/{object_type}/{alias}', 'get', 'aliases', 'get-object-type'),
     ('/workflows/', 'get', 'workflows', 'list'),
     ('/results/{id}/grants', 'delete', 'results', 'delete-grants'),
     ]
)
def test_make_operation_name(path, method, resource_name, exp):
    assert make_operation_name(path, method, resource_name) == exp


def test_str_table_result():
    cols = ['a', 'snake!']
    rows = [['2', '3'], ['1.1', None]]

    out = _str_table_result(cols, rows)
    assert out == "a   | snake!\n------------\n  2 |      3\n1.1 |       "
