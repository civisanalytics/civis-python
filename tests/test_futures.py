import json
import warnings
from operator import itemgetter
from unittest import mock

import pytest
import requests

from civis import APIClient, response
from civis.base import CivisAPIError, CivisJobFailure
from civis.futures import (
    ContainerFuture,
    _ContainerShellExecutor,
    CustomScriptExecutor,
    _create_docker_command,
)

from civis.futures import CivisFuture
from civis.tests import create_client_mock, create_client_mock_for_container_tests


def _create_poller_mock(state: str) -> mock.Mock:
    api_result = mock.Mock(state=state)
    poller = mock.Mock(return_value=api_result)
    return poller


def test_check_message():
    mock_civis = create_client_mock()
    result = CivisFuture(lambda x: x, (1, 20), client=mock_civis)
    message = {"object": {"id": 1}, "run": {"id": 20, "state": "succeeded"}}
    assert result._check_message(message) is True


def test_check_message_with_different_run_id():
    mock_civis = create_client_mock()
    result = CivisFuture(lambda x: x, (1, 20), client=mock_civis)
    message = {"object": {"id": 2}, "run": {"id": 20, "state": "succeeded"}}
    assert result._check_message(message) is False


def test_check_message_when_job_is_running():
    mock_civis = create_client_mock()
    result = CivisFuture(lambda x: x, (1, 20), client=mock_civis)
    message = {"object": {"id": 1}, "run": {"id": 20, "state": "running"}}
    assert result._check_message(message) is False


def test_poller_call_count_poll_on_creation_true():
    mock_civis = create_client_mock()
    poller = _create_poller_mock("succeeded")
    CivisFuture(poller, (1, 2), poll_on_creation=True, client=mock_civis)
    assert poller.call_count == 1


def test_poller_call_count_poll_on_creation_false():
    mock_civis = create_client_mock()
    poller = _create_poller_mock("succeeded")
    CivisFuture(poller, (1, 2), poll_on_creation=False, client=mock_civis)
    assert poller.call_count == 0


def test_set_api_result_succeeded():
    mock_civis = create_client_mock()
    poller = _create_poller_mock("succeeded")
    result = CivisFuture(poller, (1, 2), client=mock_civis)
    assert result._state == "FINISHED"


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_set_api_result_failed(m_sleep):
    mock_civis = create_client_mock()
    poller = _create_poller_mock("failed")
    result = CivisFuture(poller, (1, 2), client=mock_civis)
    assert result._state == "FINISHED"
    with pytest.raises(CivisJobFailure):
        result.result()
    with pytest.raises(CivisJobFailure):
        result.outputs()


def test_outputs_succeeded():
    poller = _create_poller_mock("succeeded")
    mock_client = create_client_mock()
    expected_return = [{"test": "test_result"}]
    mock_client.jobs.list_runs_outputs.return_value = expected_return

    result = CivisFuture(poller, (1, 2), client=mock_client)
    assert result.outputs() == expected_return


def test_polling_interval():
    mock_client = create_client_mock()
    polling_interval = 30
    future = CivisFuture(
        lambda x: x, (1, 20), polling_interval=polling_interval, client=mock_client
    )
    assert future.polling_interval == polling_interval


def _check_executor(from_template_id=None):
    job_id, run_id = 42, 43
    c = _setup_client_mock(job_id, run_id, n_failures=0)
    mock_run = c.scripts.post_containers_runs()
    if from_template_id:
        bpe = CustomScriptExecutor(
            from_template_id=from_template_id, client=c, polling_interval=0.01
        )
        future = bpe.submit(my_param="spam")
    else:
        bpe = _ContainerShellExecutor(client=c, polling_interval=0.01)
        future = bpe.submit("foo")

    # Mock and test running, future.job_id, and done()
    mock_run_json = mock_run.json()
    mock_run_json["state"] = "running"
    mock_run = response.Response(mock_run_json)
    assert future.running(), "future is incorrectly marked as not running"
    assert future.job_id == job_id, "job_id not stored properly"
    assert not future.done(), "future is incorrectly marked as done"

    future.cancel()

    # Mock and test cancelled()
    assert future.cancelled(), "cancelled() did not return True as expected"
    assert not future.running(), "running() did not return False as expected"

    # Mock and test done()
    mock_run_json = mock_run.json()
    mock_run_json["state"] = "succeeded"
    mock_run = response.Response(mock_run_json)
    assert future.done(), "done() did not return True as expected"

    # Test cancelling all jobs.
    mock_run_json = mock_run.json()
    mock_run_json["state"] = "running"
    mock_run = response.Response(mock_run_json)
    bpe.cancel_all()
    assert future.cancelled(), "cancel_all() failed"

    # Test shutdown method.
    bpe.shutdown(wait=True)
    assert future.done(), "shutdown() failed"

    return c


@pytest.mark.parametrize(
    "poller_args,expected_job_id,expected_run_id",
    [((123, 456), 123, 456), ((123,), 123, None)],
)
def test_future_job_id_run_id(poller_args, expected_job_id, expected_run_id):
    result = CivisFuture(
        poller=_create_poller_mock("succeeded"),
        poller_args=poller_args,
        client=create_client_mock(),
    )
    assert result.job_id == expected_job_id
    assert result.run_id == expected_run_id
    assert result.job_url == (
        f"https://platform.civisanalytics.com/spa/#/jobs/{expected_job_id}"
    )


def test_container_future_job_id_run_id():
    job_id, run_id = 123, 456
    result = ContainerFuture(
        job_id=job_id,
        run_id=run_id,
        client=create_client_mock_for_container_tests(),
    )
    assert result.job_id == job_id
    assert result.run_id == run_id
    assert result.job_url == f"https://platform.civisanalytics.com/spa/#/jobs/{job_id}"


def test_container_scripts():
    c = _check_executor()
    assert c.scripts.post_custom.call_count == 0
    assert c.scripts.post_containers.call_count > 0


def test_custom_scripts():
    with mock.patch.dict("os.environ", {"CIVIS_JOB_ID": "12", "CIVIS_RUN_ID": "40"}):
        c = _check_executor(133)
    assert c.scripts.post_custom.call_count > 0
    assert c.scripts.post_containers.call_count == 0

    # Verify that this script's job and run ID are passed to arguments
    args = c.scripts.post_custom.call_args[1].get("arguments")
    for k, v in (("CIVIS_PARENT_JOB_ID", "12"), ("CIVIS_PARENT_RUN_ID", "40")):
        assert args.get(k) == v


@pytest.mark.parametrize("is_child_job", [False, True])
def test_container_script_param_injection(is_child_job):
    # Test that child jobs created by the shell executor have the
    # job and run IDs of the script which created them (if any).
    job_id, run_id = "123", "13"
    c = _setup_client_mock(42, 43, n_failures=0)
    mock_env = {"CIVIS_JOB_ID": job_id, "CIVIS_RUN_ID": run_id}

    with mock.patch.dict("os.environ", mock_env):
        init_kwargs = dict(client=c, polling_interval=0.01)
        if is_child_job:
            init_kwargs["params"] = [
                {"name": "CIVIS_PARENT_JOB_ID", "type": "integer", "value": "888"},
                {"name": "CIVIS_PARENT_RUN_ID", "type": "integer", "value": "999"},
            ]
        bpe = _ContainerShellExecutor(**init_kwargs)
        bpe.submit("foo")

    params = sorted(
        c.scripts.post_containers.call_args[1].get("params"), key=itemgetter("name")
    )
    assert params == [
        {"name": "CIVIS_PARENT_JOB_ID", "type": "integer", "value": job_id},
        {"name": "CIVIS_PARENT_RUN_ID", "type": "integer", "value": run_id},
    ], "The parent job parameters were not set correctly."


def test_create_docker_command():
    res = _create_docker_command(
        "foo.sh", "bar", "baz", wibble="wibble1", wobble="wobble1"
    )
    assert res == "foo.sh bar baz --wibble wibble1 --wobble wobble1"


# A function to raise fake API errors the first
# num_failures times it is called.
def _make_error_func(num_failures, failure_is_error=False):
    """Raise API errors multiple times before succeeding

    Test error-handling code by using this to mock
    calls to post_containers_runs or get_containers_runs.

    Parameters
    ----------
    num_failures: int
        Fail this many times before returning a success
    failure_is_error: bool
        If True, "failure" means raising a `CivisAPIError`.

    Returns
    -------
    MockRun
        Mock which imitates the result of a `post_containers_runs`
        or `get_containers_runs` call
    """
    counter = {"failures": 0}  # Use a dict so we can modify it in the closure

    def mock_api_error(job_id, run_id):
        if counter["failures"] < num_failures:
            counter["failures"] += 1
            if failure_is_error:
                raise CivisAPIError(mock.MagicMock())
            else:
                return response.Response(
                    {"id": run_id, "container_id": job_id, "state": "failed"}
                )
        else:
            return response.Response(
                {"id": run_id, "container_id": job_id, "state": "succeeded"}
            )

    return mock_api_error


def _setup_client_mock(job_id=-10, run_id=100, n_failures=8, failure_is_error=False):
    """Return a Mock set up for use in testing container scripts

    Parameters
    ----------
    job_id: int
        Mock-create containers with this ID when calling `post_containers`
        or `post_containers_runs`.
    run_id: int
        Mock-create runs with this ID when calling `post_containers_runs`.
    n_failures: int
        When calling `get_containers_runs`, fail this many times
        before succeeding.
    failure_is_error: bool
        If True, "failure" means raising a `CivisAPIError`.

    Returns
    -------
    `unittest.mock.Mock`
        With `post_containers`, `post_containers_runs`, and
        `get_containers_runs` methods set up.
    """
    c = mock.Mock()
    c.__class__ = APIClient

    mock_container = response.Response({"id": job_id})
    c.scripts.post_containers.return_value = mock_container
    c.scripts.post_custom.return_value = mock_container
    mock_container_run = response.Response(
        {"id": run_id, "container_id": job_id, "state": "queued"}
    )
    c.scripts.post_containers_runs.return_value = mock_container_run
    c.jobs.post_runs.return_value = mock_container_run
    c.scripts.get_containers_runs.side_effect = _make_error_func(
        n_failures, failure_is_error
    )

    def change_state_to_cancelled(job_id):
        mock_container_run_json = mock_container_run.json()
        mock_container_run_json["state"] = "cancelled"
        return response.Response(mock_container_run_json)

    c.scripts.post_cancel.side_effect = change_state_to_cancelled

    return c


def test_cancel_finished_job():
    # If we try to cancel a completed job, we get a 404 error.
    # That shouldn't be sent to the user.

    # Set up a mock client which will give an exception when
    # you try to cancel any job.
    c = _setup_client_mock()
    err_resp = requests.Response()
    status_code = 404
    err_resp.status_code = status_code
    err_resp._content = json.dumps(
        {
            "status_code": status_code,
            "error": "not_found",
            "errorDescription": "The requested resource could not be found.",
            "content": True,
        }
    ).encode()
    c.scripts.post_cancel.side_effect = CivisAPIError(err_resp)
    resp_json = c.scripts.post_containers_runs.return_value.json()
    resp_json["state"] = "running"
    c.scripts.post_containers_runs.return_value = response.Response(resp_json)
    fut = ContainerFuture(
        -10, 100, polling_interval=1, client=c, poll_on_creation=False
    )
    assert not fut.done()
    with warnings.catch_warnings():
        # Check that no warnings are raised.
        warnings.simplefilter("error")
        assert fut.cancel() is False


def test_future_no_retry_error():
    # Verify that with no retries, exceptions on job polling
    #  are raised to the user
    c = _setup_client_mock(failure_is_error=True)
    fut = ContainerFuture(-10, 100, polling_interval=0.001, client=c)
    with pytest.raises(CivisAPIError):
        fut.result()


def test_future_no_retry_failure():
    # Verify that with no retries, job failures are raised as
    # exceptions for the user
    c = _setup_client_mock(failure_is_error=False)
    fut = ContainerFuture(-10, 100, polling_interval=0.001, client=c)
    with pytest.raises(CivisJobFailure):
        fut.result()


def test_future_not_enough_retry_error():
    # Verify that if polling the run is still erroring after all retries
    # are exhausted, the error will be raised for the user.
    c = _setup_client_mock(failure_is_error=True)
    fut = ContainerFuture(-10, 100, max_n_retries=3, polling_interval=0.01, client=c)
    with pytest.raises(CivisAPIError):
        fut.result()


def test_future_not_enough_retry_failure():
    # Verify that if the job is still failing after all retries
    # are exhausted, the job failure will be raised for the user.
    c = _setup_client_mock(failure_is_error=False)
    fut = ContainerFuture(-10, 100, max_n_retries=3, polling_interval=0.01, client=c)
    with pytest.raises(CivisJobFailure):
        fut.result()


def test_future_retry_failure():
    # Verify that we can retry through API errors until a job succeeds
    c = _setup_client_mock(failure_is_error=False)
    fut = ContainerFuture(-10, 100, max_n_retries=10, polling_interval=0.01, client=c)
    assert fut.result().state == "succeeded"


def test_future_retry_error():
    # Verify that we can retry through job failures until it succeeds
    c = _setup_client_mock(failure_is_error=True)
    fut = ContainerFuture(-10, 100, max_n_retries=10, polling_interval=0.01, client=c)
    assert fut.result().state == "succeeded"


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_container_exception_no_result_logs(m_sleep):
    # If the job errored with no output but with logs,
    # we should return error logs with the future exception.
    mem_msg = "Run used approximately 2 millicores " "of its 256 millicore CPU limit"
    failed_msg = "Failed: The job container failed. Exit code 1"
    logs = [
        {"id": 111, "created_at": "abc", "message": mem_msg, "level": "info"},
        {"id": 222, "created_at": "def", "message": failed_msg, "level": "error"},
    ]
    mock_client = create_client_mock_for_container_tests(
        1, 2, state="failed", run_outputs=[], log_outputs=logs
    )
    fut = ContainerFuture(1, 2, client=mock_client)

    with pytest.raises(CivisJobFailure) as err:
        fut.result()
    expected_msg = "(From job 1 / run 2) " + "\n".join([failed_msg, mem_msg, ""])
    assert expected_msg == str(fut._exception.error_message)
    assert str(err.value) == expected_msg


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_container_exception_memory_error(m_sleep):
    err_msg = "Process ran out of its allowed 3000 MiB of " "memory and was killed."
    logs = [
        {
            "created_at": "2017-05-10T12:00:00.000Z",
            "id": 10005,
            "level": "error",
            "message": "Failed",
        },
        {
            "created_at": "2017-05-10T12:00:00.000Z",
            "id": 10003,
            "level": "error",
            "message": "Error on job: Process ended with an " "error, exiting: 137.",
        },
        {
            "created_at": "2017-05-10T12:00:00.000Z",
            "id": 10000,
            "level": "error",
            "message": err_msg,
        },
    ]
    mock_client = create_client_mock_for_container_tests(
        1, 2, state="failed", run_outputs=[], log_outputs=logs
    )
    fut = ContainerFuture(1, 2, client=mock_client)

    with pytest.raises(MemoryError) as err:
        fut.result()
    assert str(err.value) == f"(From job 1 / run 2) {err_msg}"
