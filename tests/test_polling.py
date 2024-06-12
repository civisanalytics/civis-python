"""Test the `civis.polling` module"""

import time
from concurrent import futures
import unittest
from unittest import mock

from civis.response import Response
from civis.polling import PollableResult, _ResultPollingThread

import pytest


class State:
    def __init__(self, state):
        self.state = state


def create_pollable_result(state, exception=None, result=None):
    f = PollableResult(State, (state,), polling_interval=0.001)
    f._exception = exception
    f._result = result
    return f


CANCELLED_RESULT = create_pollable_result(state="cancelled")
FINISHED_RESULT = create_pollable_result(state="success")
QUEUED_RESULT = create_pollable_result(state="queued")


class TestPolling(unittest.TestCase):
    def test_as_completed(self):
        my_futures = [QUEUED_RESULT, CANCELLED_RESULT, FINISHED_RESULT]
        fs = futures.as_completed(my_futures)
        f1 = next(fs)
        f2 = next(fs)
        finished_futures = set([f1, f2])

        self.assertEqual(finished_futures, set([FINISHED_RESULT, CANCELLED_RESULT]))

    def test_wait(self):
        done, not_done = futures.wait(
            [QUEUED_RESULT, FINISHED_RESULT], return_when=futures.FIRST_COMPLETED
        )
        self.assertEqual(set([FINISHED_RESULT]), done)
        self.assertEqual(set([QUEUED_RESULT]), not_done)

    def test_error_passthrough(self):
        pollable = PollableResult(
            mock.Mock(side_effect=[ZeroDivisionError()]), (), polling_interval=0.1
        )
        pytest.raises(ZeroDivisionError, pollable.result)

    def test_error_setting(self):
        pollable = PollableResult(
            mock.Mock(side_effect=[ZeroDivisionError()]), (), polling_interval=0.1
        )
        assert isinstance(pollable.exception(), ZeroDivisionError)

    def test_timeout(self):
        pollable = PollableResult(
            mock.Mock(return_value=Response({"state": "running"})),
            poller_args=(),
            polling_interval=0.1,
        )
        pytest.raises(futures.TimeoutError, pollable.result, timeout=0.05)

    def test_poll_on_creation(self):
        poller = mock.Mock(return_value=Response({"state": "running"}))
        pollable = PollableResult(
            poller, (), polling_interval=0.01, poll_on_creation=False
        )
        pollable.done()  # Check status once to start the polling thread
        assert poller.call_count == 0
        time.sleep(0.05)
        assert poller.call_count > 0

    def test_poller_returns_none(self):
        check_result = mock.Mock(
            side_effect=[None, None, Response({"state": "success"})]
        )
        pollable_result = mock.Mock()
        pollable_result._check_result = check_result
        pollable_result._next_polling_interval = 0.01
        polling_thread = _ResultPollingThread(pollable_result)
        polling_thread.run()
        assert check_result.call_count == 3

    def test_reset_polling_thread(self):
        pollable = PollableResult(
            mock.Mock(return_value=Response({"state": "running"})),
            poller_args=(),
            polling_interval=0.1,
        )
        initial_polling_thread = pollable._polling_thread
        assert pollable.polling_interval == 0.1
        assert pollable._next_polling_interval == 0.1
        pollable._reset_polling_thread(0.2)
        # Check that the polling interval was updated
        assert pollable.polling_interval == 0.2
        assert pollable._next_polling_interval == 0.2
        # Check that the _polling_thread is a new thread
        assert pollable._polling_thread != initial_polling_thread
        # Check that the old thread was stopped
        assert not initial_polling_thread.is_alive()

    def test_geometric_polling(self):
        # To test polling, we make the poller function spit out a timestamp every time
        # it is called. Then we check if these timestamps are what we'd expect.
        poller_timestamps = []

        def append_new_timestamp(*args, **kwargs):
            nonlocal poller_timestamps
            poller_timestamps.append(time.time())
            if len(poller_timestamps) < 5:
                return Response({"state": "running"})
            else:
                return Response({"state": "succeeded"})

        poller = mock.Mock()
        poller.side_effect = append_new_timestamp

        pollable = PollableResult(poller, (), poll_on_creation=False)
        start_time = time.time()
        pollable.result()

        assert len(poller_timestamps) == 5
        expected_intervals = [1, 1.2, 1.44, 1.728, 2.0736]
        actual_intervals = []
        for i, timestamp in enumerate(poller_timestamps):
            actual_intervals.append(
                timestamp - (poller_timestamps[i - 1] if i else start_time)
            )
        assert actual_intervals == pytest.approx(expected_intervals, abs=0.02)


if __name__ == "__main__":
    unittest.main()
