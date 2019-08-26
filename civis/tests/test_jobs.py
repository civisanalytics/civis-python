import pytest
import civis

from civis.response import Response
from civis.tests.mocks import create_client_mock
from civis.utils._jobs import logging


def create_mock_client_with_job():
    mock_client = create_client_mock()

    job_response = Response({"id": 1, "name": "test"})
    run_post_response = Response({"id": 1})
    run_get_response = Response(
        {
            "id": run_post_response.id,
            "state": "succeeded",
            "is_cancel_requested": False,
            "error": None,
            "custom_id": run_post_response.id,
        }
    )
    mock_client.scripts.post_custom.return_value = job_response
    mock_client.scripts.post_custom_runs.return_value = run_post_response
    mock_client.scripts.get_custom_runs.return_value = run_get_response
    return mock_client


@pytest.fixture
def mock_client_single_json_output():
    mock_client = create_mock_client_with_job()
    mock_output = [
        Response(
            {
                "object_type": "JSONValue",
                "object_id": 10,
                "name": "output",
                "value": {"a": 1},
            }
        )
    ]
    mock_client.scripts.list_custom_runs_outputs.return_value = mock_output
    return mock_client


@pytest.fixture
def mock_client_multiple_json_output():
    mock_client = create_mock_client_with_job()
    mock_output = [
        Response(
            {
                "object_type": "JSONValue",
                "object_id": 10,
                "name": "output",
                "value": {"a": 1},
            }
        ),
        Response(
            {
                "object_type": "JSONValue",
                "object_id": 11,
                "name": "output2",
                "value": {"b": 2},
            }
        ),
    ]
    mock_client.scripts.list_custom_runs_outputs.return_value = mock_output
    return mock_client


@pytest.fixture
def mock_client_no_json_output():
    mock_client = create_mock_client_with_job()
    mock_output = [
        Response(
            {
                "object_type": int,
                "object_id": 10,
                "name": "output",
                "value": 100,
            }
        )
    ]
    mock_client.scripts.list_custom_runs_outputs.return_value = mock_output
    return mock_client


def test_run_template_json_output_fileids_returned(
    mock_client_single_json_output
):
    template_id = 1
    args = {"arg": 1}
    result = civis.utils.run_template(
        template_id, arguments=args, client=mock_client_single_json_output
    )
    assert result == {"output": 10}


def test_run_template_no_json_output_fileids_returned(
    mock_client_no_json_output
):
    template_id = 1
    args = {"arg": 1}
    result = civis.utils.run_template(
        template_id, arguments=args, client=mock_client_no_json_output
    )
    assert result == {"output": 10}


def test_run_template_multiple_json_output_fileids_returned(
    mock_client_multiple_json_output
):
    template_id = 1
    args = {"arg": 1}
    result = civis.utils.run_template(
        template_id, arguments=args, client=mock_client_multiple_json_output
    )
    assert result == {"output": 10, "output2": 11}


def test_run_template_json_returned(mock_client_single_json_output):
    template_id = 1
    args = {"arg": 1}
    result = civis.utils.run_template(
        template_id,
        arguments=args,
        JSONValue=True,
        client=mock_client_single_json_output,
    )
    assert result == {"a": 1}


def test_run_template_many_json_outputs(
    caplog, mock_client_multiple_json_output
):
    template_id = 1
    args = {"arg": 1}
    result = civis.utils.run_template(
        template_id,
        arguments=args,
        JSONValue=True,
        client=mock_client_multiple_json_output,
    )
    log_result = caplog.record_tuples
    assert log_result == [
        (
            "civis.utils._jobs",
            logging.WARNING,
            "More than 1 JSON output for template {} "
            "-- returning only the first one.".format(template_id),
        )
    ]
    assert result == {"a": 1}


def test_run_template_when_no_json_output(caplog, mock_client_no_json_output):
    template_id = 1
    args = {"arg": 1}
    result = civis.utils.run_template(
        template_id,
        arguments=args,
        JSONValue=True,
        client=mock_client_no_json_output,
    )
    log_result = caplog.record_tuples
    assert log_result == [
        (
            "civis.utils._jobs",
            logging.WARNING,
            "No JSON output for template {}".format(template_id),
        )
    ]
    assert result is None
