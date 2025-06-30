from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest

import civis
from civis.response import Response
from civis.tests.mocks import create_client_mock
from civis.utils._jobs import (
    _LOGS_PER_QUERY,
    _compute_effective_max_log_id,
    _job_finished_past_timeout,
    job_logs,
    logging,
)


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
def mock_client():
    return create_client_mock()


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


def test_run_template_json_output_fileids_returned(mock_client_single_json_output):
    template_id = 1
    args = {"arg": 1}
    result = civis.utils.run_template(
        template_id, arguments=args, client=mock_client_single_json_output
    )
    assert result == {"output": 10}


def test_run_template_no_json_output_fileids_returned(mock_client_no_json_output):
    template_id = 1
    args = {"arg": 1}
    result = civis.utils.run_template(
        template_id, arguments=args, client=mock_client_no_json_output
    )
    assert result == {"output": 10}


def test_run_template_multiple_json_output_fileids_returned(
    mock_client_multiple_json_output,
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


def test_run_template_many_json_outputs(caplog, mock_client_multiple_json_output):
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


@pytest.fixture
def sample_logs():
    return [
        {"id": 1, "message": "First log", "created_at": "2023-01-01T00:00:00Z"},
        {"id": 2, "message": "Second log", "created_at": "2023-01-01T00:00:01Z"},
        {"id": 3, "message": "Third log", "created_at": "2023-01-01T00:00:02Z"},
    ]


@patch("civis.utils._jobs.time")
def test_job_logs_single_batch(mock_time, mock_client, sample_logs):
    """Test when all logs are retrieved in a single batch."""
    mock_response = Mock()
    mock_response.json.return_value = sample_logs
    mock_response.headers = {"civis-max-id": "3", "civis-cache-control": "store"}

    mock_client.jobs.list_runs_logs.return_value = mock_response
    mock_time.time.return_value = datetime.fromisoformat("2025-01-01").timestamp()

    logs = list(job_logs(job_id=123, run_id=456, client=mock_client))

    assert len(logs) == 3
    assert logs == sample_logs
    mock_client.jobs.list_runs_logs.assert_called_once_with(
        123, 456, last_id=0, limit=_LOGS_PER_QUERY
    )


@patch("civis.utils._jobs.time")
def test_job_logs_multiple_batches(mock_time, mock_client):
    """Test when logs are retrieved in multiple batches."""
    first_batch = [
        {"id": 1, "message": "Log 1", "created_at": "2023-01-01T00:00:00Z"},
        {"id": 2, "message": "Log 2", "created_at": "2023-01-01T00:00:01Z"},
    ]
    second_batch = [{"id": 3, "message": "Log 3", "created_at": "2023-01-01T00:00:02Z"}]

    mock_response1 = Mock()
    mock_response1.json.return_value = first_batch
    mock_response1.headers = {"civis-max-id": "2", "civis-cache-control": "no-store"}

    mock_response2 = Mock()
    mock_response2.json.return_value = second_batch
    mock_response2.headers = {"civis-max-id": "3", "civis-cache-control": "store"}

    mock_client.jobs.list_runs_logs.side_effect = [mock_response1, mock_response2]
    mock_time.time.return_value = datetime.fromisoformat("2025-01-01").timestamp()

    logs = list(job_logs(job_id=123, run_id=456, client=mock_client))

    assert len(logs) == 3
    assert logs == first_batch + second_batch
    assert mock_client.jobs.list_runs_logs.call_count == 2
    mock_time.sleep.assert_called_once()


@patch("civis.utils._jobs.time")
def test_job_logs_no_logs_initially(mock_time, mock_client, sample_logs):
    """Test behavior when no logs are available."""

    mock_response1 = Mock()
    mock_response1.json.return_value = []
    mock_response1.headers = {"civis-cache-control": "store"}

    mock_response2 = Mock()
    mock_response2.json.return_value = sample_logs
    mock_response2.headers = {"civis-cache-control": "store", "civis-max-id": "3"}

    mock_client.jobs.list_runs_logs.side_effect = [mock_response1, mock_response2]
    mock_time.time.return_value = datetime.fromisoformat("2025-01-01").timestamp()

    logs = list(job_logs(job_id=123, run_id=456, client=mock_client))

    assert len(logs) == len(sample_logs)
    assert mock_client.jobs.list_runs_logs.call_count == 2
    mock_time.sleep.assert_called_once()


def test_job_logs_sorted_order(mock_client):
    """Test that logs are properly sorted by createdAt and id.

    Note: logs won't be sorted if they are out of order across different API calls.
    The sorting only works within a single API call.
    """
    unsorted_logs = [
        {"id": 2, "message": "Log 2", "created_at": "2023-01-01T00:00:01Z"},
        {"id": 1, "message": "Log 1", "created_at": "2023-01-01T00:00:01Z"},
        {"id": 3, "message": "Log 3", "created_at": "2023-01-01T00:00:00Z"},
    ]

    expected_order = [
        {"id": 3, "message": "Log 3", "created_at": "2023-01-01T00:00:00Z"},
        {"id": 1, "message": "Log 1", "created_at": "2023-01-01T00:00:01Z"},
        {"id": 2, "message": "Log 2", "created_at": "2023-01-01T00:00:01Z"},
    ]

    mock_response = Mock()
    mock_response.json.return_value = unsorted_logs
    mock_response.headers = {"civis-max-id": "3", "civis-cache-control": "store"}

    mock_client.jobs.list_runs_logs.return_value = mock_response

    logs = list(job_logs(job_id=123, run_id=456, client=mock_client))

    assert logs == expected_order


@patch("civis.utils._jobs.time")
def test_job_logs_no_duplicate_logs(mock_time, mock_client):
    """Test that duplicate log messages won't be yielded."""
    expected_logs = [
        {"id": 1, "message": "Log 1", "created_at": "2023-01-01T00:00:00Z"},
        {"id": 2, "message": "Log 2", "created_at": "2023-01-01T00:00:01Z"},
    ]

    mock_response1 = Mock()
    mock_response1.json.return_value = expected_logs
    mock_response1.headers = {"civis-max-id": "2", "civis-cache-control": "no-store"}

    mock_response2 = Mock()
    mock_response2.json.return_value = expected_logs
    mock_response2.headers = {"civis-max-id": "2", "civis-cache-control": "store"}

    mock_time.time.return_value = datetime.fromisoformat("2025-01-01").timestamp()
    mock_client.jobs.list_runs_logs.side_effect = (mock_response1, mock_response2)

    actual_logs = list(job_logs(job_id=123, run_id=456, client=mock_client))

    assert actual_logs == expected_logs


def test_compute_effective_max_log_id_empty_logs():
    assert _compute_effective_max_log_id([]) == 0


@patch("civis.utils._jobs.time")
def test_compute_effective_max_log_id_all_logs_before_cutoff(mock_time):
    dt_now = datetime.now()
    # The log messages were from more seconds ago than the cutoff.
    logs = [
        {"id": 1, "created_at": (dt_now - timedelta(seconds=400)).isoformat()},
        {"id": 2, "created_at": (dt_now - timedelta(seconds=350)).isoformat()},
    ]
    mock_time.time.side_effect = lambda: dt_now.timestamp()
    assert _compute_effective_max_log_id(logs) == 2


@patch("civis.utils._jobs.time")
def test_compute_effective_max_log_id_logs_within_cutoff(mock_time):
    dt_now = datetime.now()
    # Both log messages were from fewer seconds ago than the cutoff,
    # so they'll both be retrieved again to avoid skipping any.
    logs = [
        {"id": 1, "created_at": (dt_now - timedelta(seconds=200)).isoformat()},
        {"id": 2, "created_at": (dt_now - timedelta(seconds=150)).isoformat()},
    ]
    mock_time.time.side_effect = lambda: dt_now.timestamp()
    assert _compute_effective_max_log_id(logs) == 0


@patch("civis.utils._jobs.time")
def test_compute_effective_max_log_id_logs_exceed_refetch_count(mock_time):
    dt_now = datetime.now()
    logs = [
        {"id": i, "created_at": (dt_now - timedelta(seconds=i)).isoformat()}
        for i in range(110)
    ]
    mock_time.time.side_effect = lambda: dt_now.timestamp()
    assert _compute_effective_max_log_id(logs) == 10


def test_job_finished_past_timeout_no_timeout(mock_client):
    assert not _job_finished_past_timeout(123, 456, None, mock_client)


def test_job_finished_past_timeout_not_finished(mock_client):
    mock_client.jobs.get_runs.return_value.json.return_value = {"finished_at": None}
    assert not _job_finished_past_timeout(123, 456, 10, mock_client)


def test_job_finished_past_timeout_finished_recently(mock_client):
    finished_at = (datetime.now() - timedelta(seconds=5)).isoformat()
    mock_client.jobs.get_runs.return_value.json.return_value = {
        "finished_at": finished_at
    }
    assert not _job_finished_past_timeout(123, 456, 10, mock_client)


def test_job_finished_past_timeout_finished_long_ago(mock_client):
    finished_at = (datetime.now() - timedelta(seconds=20)).isoformat()
    mock_client.jobs.get_runs.return_value.json.return_value = {
        "finished_at": finished_at
    }
    assert _job_finished_past_timeout(123, 456, 10, mock_client)
