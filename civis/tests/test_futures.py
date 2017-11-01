import os
import json
from collections import OrderedDict

import pytest

from civis import APIClient, response
from civis.base import CivisAPIError, CivisJobFailure
from civis.compat import mock
from civis.resources._resources import get_api_spec, generate_classes
from civis.futures import (ContainerFuture,
                           _ContainerShellExecutor,
                           CustomScriptExecutor,
                           _create_docker_command)

from civis.futures import (CivisFuture,
                           JobCompleteListener,
                           _LONG_POLLING_INTERVAL)
from civis.tests import TEST_SPEC
from pubnub.enums import PNStatusCategory

from civis.tests.testcase import CivisVCRTestCase

api_import_str = 'civis.resources._resources.get_api_spec'
THIS_DIR = os.path.dirname(os.path.realpath(__file__))
with open(TEST_SPEC) as f:
    civis_api_spec_base = json.load(f, object_pairs_hook=OrderedDict)

with open(os.path.join(THIS_DIR, "civis_api_spec_channels.json")) as f:
    civis_api_spec_channels = json.load(f, object_pairs_hook=OrderedDict)


def clear_lru_cache():
    # LRU cache persists between tests so these caches need to be cleared
    # when different api specs are used in different test cases
    get_api_spec.cache_clear()
    generate_classes.cache_clear()


def setup_listener_status_mocks(status_category):
    match = mock.Mock()
    callback = mock.Mock()
    disconnect = mock.Mock()
    listener = JobCompleteListener(match, callback, disconnect)
    status = mock.Mock()
    status.category = status_category
    return match, callback, disconnect, listener, status


class CivisFutureTests(CivisVCRTestCase):

    @classmethod
    def setUpClass(cls):
        clear_lru_cache()

    @classmethod
    def tearDownClass(cls):
        clear_lru_cache()

    def test_listener_calls_callback_when_message_matches(self):
        match = mock.Mock()
        match.return_value = True
        callback = mock.Mock()
        listener = JobCompleteListener(match, callback)
        message = mock.Mock()
        message.message.return_value = 'test message'

        listener.message(None, message)
        match.assert_called_with(message.message)
        self.assertEqual(callback.call_count, 1)

    def test_listener_does_not_call_callback(self):
        match = mock.Mock()
        match.return_value = False
        callback = mock.Mock()
        listener = JobCompleteListener(match, callback)
        message = mock.Mock()
        message.message.return_value = 'test message'

        listener.message(None, message)
        match.assert_called_with(message.message)
        self.assertEqual(callback.call_count, 0)

    def test_listener_calls_disconnect_callback_when_status_disconnect(self):
        disconnect_categories = [
            PNStatusCategory.PNTimeoutCategory,
            PNStatusCategory.PNNetworkIssuesCategory,
            PNStatusCategory.PNUnexpectedDisconnectCategory,
        ]
        for category in disconnect_categories:
            mocks = setup_listener_status_mocks(category)
            _, _, disconnect, listener, status = mocks
            listener.status(None, status)
            assert disconnect.call_count == 1

    def test_listener_does_note_call_disconnect_callback_on_other_status(self):
        nondisconnect_categories = [
            PNStatusCategory.PNAcknowledgmentCategory,
            PNStatusCategory.PNConnectedCategory,
            PNStatusCategory.PNReconnectedCategory,
        ]
        for category in nondisconnect_categories:
            mocks = setup_listener_status_mocks(category)
            _, _, disconnect, listener, status = mocks
            listener.status(None, status)
            assert disconnect.call_count == 0

    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_check_message(self, *mocks):
        result = CivisFuture(lambda x: x, (1, 20))
        message = {
            'object': {
                'id': 1
            },
            'run': {
                'id': 20,
                'state': 'succeeded'
            }
        }
        self.assertTrue(result._check_message(message))

    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_check_message_with_different_run_id(self, *mocks):
        result = CivisFuture(lambda x: x, (1, 20))
        message = {
            'object': {
                'id': 2
            },
            'run': {
                'id': 20,
                'state': 'succeeded'
            }
        }
        self.assertFalse(result._check_message(message))

    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_check_message_when_job_is_running(self, *mocks):
        result = CivisFuture(lambda x: x, (1, 20))
        message = {
            'object': {
                'id': 1
            },
            'run': {
                'id': 20,
                'state': 'running'
            }
        }
        self.assertFalse(result._check_message(message))

    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_set_api_result_result_succeeded(self, mock_subscribe, mock_api):
        mock_pubnub = mock.Mock()
        mock_pubnub.unsubscribe_all.return_value = None
        mock_subscribe.return_value = mock_pubnub
        poller = mock.Mock()
        api_result = mock.Mock()
        api_result.state = 'succeeded'

        result = CivisFuture(poller, (1, 2))
        result._set_api_result(api_result)
        assert poller.call_count == 0
        assert mock_pubnub.unsubscribe_all.call_count == 1
        assert result._state == 'FINISHED'

    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_set_api_result_failed(self, mock_subscribe, mock_api):
        mock_pubnub = mock.Mock()
        mock_pubnub.unsubscribe_all.return_value = None
        mock_subscribe.return_value = mock_pubnub
        poller = mock.Mock()
        api_result = mock.Mock()
        api_result.state = 'failed'

        result = CivisFuture(poller, (1, 2))
        result._set_api_result(api_result)
        assert mock_pubnub.unsubscribe_all.call_count == 1
        assert result._state == 'FINISHED'
        with pytest.raises(CivisJobFailure):
            result.result()

    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_subscribed_with_channels(self, *mocks):
        future = CivisFuture(lambda x: x,
                             (1, 20))
        future._pubnub.get_subscribed_channels.return_value = [1]
        assert future.subscribed is True

    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_subscribed_with_no_subscription(self, *mocks):
        future = CivisFuture(lambda x: x,
                             (1, 20))
        future._pubnub.get_subscribed_channels.return_value = []
        assert future.subscribed is False

    @mock.patch(api_import_str, return_value=civis_api_spec_base)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_subscribed_with_no_channels(self, *mocks):
        clear_lru_cache()
        future = CivisFuture(lambda x: x,
                             (1, 20))
        assert future.subscribed is False
        clear_lru_cache()

    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_overwrite_polling_interval_with_channels(self, *mocks):
        future = CivisFuture(lambda x: x, (1, 20))
        assert future.polling_interval == _LONG_POLLING_INTERVAL
        assert hasattr(future, '_pubnub')

    @mock.patch(api_import_str, return_value=civis_api_spec_channels)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_explicit_polling_interval_with_channels(self, *mocks):
        future = CivisFuture(lambda x: x, (1, 20), polling_interval=5)
        assert future.polling_interval == 5
        assert hasattr(future, '_pubnub')

    @mock.patch(api_import_str, return_value=civis_api_spec_base)
    @mock.patch.object(CivisFuture, '_subscribe')
    def test_polling_interval(self, *mocks):
        # This tests the fallback to polling when channels is not available.
        # It uses a different api spec than the other tests so it
        # should clear the cached values before and after
        clear_lru_cache()

        polling_interval = 30
        future = CivisFuture(lambda x: x,
                             (1, 20),
                             polling_interval=polling_interval)
        assert future.polling_interval == polling_interval
        assert hasattr(future, '_pubnub') is False

        clear_lru_cache()


def _check_executor(from_template_id=None):
    job_id, run_id = 42, 43
    c = _setup_client_mock(job_id, run_id, n_failures=0)
    mock_run = c.scripts.post_containers_runs()
    if from_template_id:
        bpe = CustomScriptExecutor(from_template_id=from_template_id,
                                   client=c, polling_interval=0.01)
        future = bpe.submit(my_param='spam')
    else:
        bpe = _ContainerShellExecutor(client=c, polling_interval=0.01)
        future = bpe.submit("foo")

    # Mock and test running, future.job_id, and done()
    mock_run.state = "running"
    assert future.running(), "future is incorrectly marked as not running"
    assert future.job_id == job_id, "job_id not stored properly"
    assert not future.done(), "future is incorrectly marked as done"

    future.cancel()

    # Mock and test cancelled()
    assert future.cancelled(), "cancelled() did not return True as expected"
    assert not future.running(), "running() did not return False as expected"

    # Mock and test done()
    mock_run.state = "succeeded"
    assert future.done(), "done() did not return True as expected"

    # Test cancelling all jobs.
    mock_run.state = "running"
    bpe.cancel_all()
    assert future.cancelled(), "cancel_all() failed"

    # Test shutdown method.
    bpe.shutdown(wait=True)
    assert future.done(), "shutdown() failed"

    return c


def test_container_scripts():
    c = _check_executor()
    assert c.scripts.post_custom.call_count == 0
    assert c.scripts.post_containers.call_count > 0


def test_custom_scripts():
    c = _check_executor(133)
    assert c.scripts.post_custom.call_count > 0
    assert c.scripts.post_containers.call_count == 0


def test_create_docker_command():
    res = _create_docker_command("foo.sh", "bar", "baz", wibble="wibble1",
                                 wobble="wobble1")
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
    counter = {'failures': 0}  # Use a dict so we can modify it in the closure

    def mock_api_error(job_id, run_id):
        if counter['failures'] < num_failures:
            counter['failures'] += 1
            if failure_is_error:
                raise CivisAPIError(mock.MagicMock())
            else:
                return response.Response({'id': run_id,
                                          'container_id': job_id,
                                          'state': 'failed'})
        else:
            return response.Response({'id': run_id,
                                      'container_id': job_id,
                                      'state': 'succeeded'})
    return mock_api_error


def _setup_client_mock(job_id=-10, run_id=100, n_failures=8,
                       failure_is_error=False):
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

    mock_container = response.Response({'id': job_id})
    c.scripts.post_containers.return_value = mock_container
    c.scripts.post_custom.return_value = mock_container
    mock_container_run = response.Response({'id': run_id,
                                            'container_id': job_id,
                                            'state': 'queued'})
    c.scripts.post_containers_runs.return_value = mock_container_run
    c.jobs.post_runs.return_value = mock_container_run
    c.scripts.get_containers_runs.side_effect = _make_error_func(
        n_failures, failure_is_error)

    def change_state_to_cancelled(job_id):
        mock_container_run.state = "cancelled"
        return mock_container_run

    c.scripts.post_cancel.side_effect = change_state_to_cancelled
    del c.channels  # Remove "channels" endpoint to fall back on polling

    return c


def test_cancel_finished_job():
    # If we try to cancel a completed job, we get a 404 error.
    # That shouldn't be sent to the user.

    # Set up a mock client which will give an exception when
    # you try to cancel any job.
    c = _setup_client_mock()
    err_resp = response.Response({
        'status_code': 404,
        'error': 'not_found',
        'errorDescription': 'The requested resource could not be found.',
        'content': True})
    err_resp.json = lambda: err_resp.json_data
    c.scripts.post_cancel.side_effect = CivisAPIError(err_resp)
    c.scripts.post_containers_runs.return_value.state = 'running'

    fut = ContainerFuture(-10, 100, polling_interval=1, client=c,
                          poll_on_creation=False)
    assert not fut.done()
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
    fut = ContainerFuture(-10, 100, max_n_retries=3, polling_interval=0.01,
                          client=c)
    with pytest.raises(CivisAPIError):
        fut.result()


def test_future_not_enough_retry_failure():
    # Verify that if the job is still failing after all retries
    # are exhausted, the job failure will be raised for the user.
    c = _setup_client_mock(failure_is_error=False)
    fut = ContainerFuture(-10, 100, max_n_retries=3, polling_interval=0.01,
                          client=c)
    with pytest.raises(CivisJobFailure):
        fut.result()


def test_future_retry_failure():
    # Verify that we can retry through API errors until a job succeeds
    c = _setup_client_mock(failure_is_error=False)
    fut = ContainerFuture(-10, 100, max_n_retries=10, polling_interval=0.01,
                          client=c)
    assert fut.result().state == 'succeeded'


def test_future_retry_error():
    # Verify that we can retry through job failures until it succeeds
    c = _setup_client_mock(failure_is_error=True)
    fut = ContainerFuture(-10, 100, max_n_retries=10, polling_interval=0.01,
                          client=c)
    assert fut.result().state == 'succeeded'
