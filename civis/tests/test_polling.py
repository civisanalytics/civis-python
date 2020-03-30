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
    f = PollableResult(State, (state, ), polling_interval=0.001)
    f._exception = exception
    f._result = result
    return f


CANCELLED_RESULT = create_pollable_result(state='cancelled')
FINISHED_RESULT = create_pollable_result(state='success')
QUEUED_RESULT = create_pollable_result(state='queued')


class TestPolling(unittest.TestCase):
    def test_as_completed(self):
        my_futures = [QUEUED_RESULT, CANCELLED_RESULT, FINISHED_RESULT]
        fs = futures.as_completed(my_futures)
        f1 = next(fs)
        f2 = next(fs)
        finished_futures = set([f1, f2])

        self.assertEqual(finished_futures,
                         set([FINISHED_RESULT, CANCELLED_RESULT]))

    def test_wait(self):
        done, not_done = futures.wait([QUEUED_RESULT, FINISHED_RESULT],
                                      return_when=futures.FIRST_COMPLETED)
        self.assertEqual(set([FINISHED_RESULT]), done)
        self.assertEqual(set([QUEUED_RESULT]), not_done)

    def test_error_passthrough(self):
        pollable = PollableResult(mock.Mock(side_effect=[ZeroDivisionError()]),
                                  (),
                                  polling_interval=0.1)
        pytest.raises(ZeroDivisionError, pollable.result)

    def test_error_setting(self):
        pollable = PollableResult(mock.Mock(side_effect=[ZeroDivisionError()]),
                                  (),
                                  polling_interval=0.1)
        assert isinstance(pollable.exception(), ZeroDivisionError)

    def test_timeout(self):
        pollable = PollableResult(
            mock.Mock(return_value=Response({"state": "running"})),
            poller_args=(),
            polling_interval=0.1)
        pytest.raises(futures.TimeoutError, pollable.result, timeout=0.05)

    def test_poll_on_creation(self):
        poller = mock.Mock(return_value=Response({"state": "running"}))
        pollable = PollableResult(poller,
                                  (),
                                  polling_interval=0.01,
                                  poll_on_creation=False)
        pollable.done()  # Check status once to start the polling thread
        assert poller.call_count == 0
        time.sleep(0.02)
        assert poller.call_count > 0

    def test_poller_returns_none(self):
        poller = mock.Mock(side_effect=[None,
                                        None,
                                        Response({'state': 'success'})])
        polling_thread = _ResultPollingThread(poller,
                                              (),
                                              polling_interval=0.01)
        polling_thread.run()
        time.sleep(0.05)
        assert poller.call_count == 3

    def test_reset_polling_thread(self):
        pollable = PollableResult(
            mock.Mock(return_value=Response({"state": "running"})),
            poller_args=(),
            polling_interval=0.1
        )
        initial_polling_thread = pollable._polling_thread
        assert pollable.polling_interval == 0.1
        assert pollable._polling_thread.polling_interval == 0.1
        pollable._reset_polling_thread(0.2)
        # Check that the polling interval was updated
        assert pollable.polling_interval == 0.2
        assert pollable._polling_thread.polling_interval == 0.2
        # Check that the _polling_thread is a new thread
        assert pollable._polling_thread != initial_polling_thread
        # Check that the old thread was stopped
        assert not initial_polling_thread.is_alive()


if __name__ == '__main__':
    unittest.main()
