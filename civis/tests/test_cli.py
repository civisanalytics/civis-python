from collections import OrderedDict
import json
import os

from civis.cli.__main__ import generate_cli, invoke
from civis.compat import mock
from civis.resources._resources import BASE_RESOURCES_V1
from civis.tests import TEST_SPEC

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
    """Test loading the Civis API spec as of 2017-02-02."""
    with open(TEST_SPEC) as f:
        civis_spec = json.load(f, object_pairs_hook=OrderedDict)
    mock_retrieve_spec_dict.return_value = civis_spec

    cli = generate_cli()
    expected_cli_keys = (
        {x.replace('_', '-') for x in BASE_RESOURCES_V1} |
        {'civis'}
    )
    assert sorted(cli.commands.keys()) == sorted(expected_cli_keys)

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
    session_context.request.return_value.json.side_effect = ValueError()

    op = {"parameters": []}
    invoke("WIBBLE", "/wobble/wubble", op)


@mock.patch("civis.cli.__main__.open_session", autospec=True)
def test_parameter_case(mock_session):
    """
    Test that parameter names are sent in camelCase rather than snake_case.
    """
    api_response = {'key': 'value'}
    session_context = mock_session.return_value.__enter__.return_value
    session_context.request.return_value.json.return_value = api_response

    # To avoid needing CIVIS_API_KEY set in the environment.
    op = {"parameters": [{'name': 'firstParameter', 'in': 'query'},
                         {'name': 'secondParameter', 'in': 'query'}]}
    invoke("WIBBLE", "/wobble/wubble", op,
           first_parameter='a', second_parameter='b')

    mock_session.call_args[1]['user_agent'] = 'civis-cli'
    session_context.request.assert_called_with(
        url='https://api.civisanalytics.com/wobble/wubble',
        json={},
        params={'firstParameter': 'a', 'secondParameter': 'b'},
        method='WIBBLE')
