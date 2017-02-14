"""Test the `civis.polling` module"""
import time
from concurrent import futures
import unittest
from unittest import mock

from civis.response import Response
from civis.polling import PollableResult

import pytest


class State:
    def __init__(self, state):
        self.state = state


def func():
    pass


def create_pollable_result(state, exception=None, result=None):
    f = PollableResult(State, (state, ), polling_interval=0)
    f._exception = exception
    f._result = result
    return f


CANCELLED_RESULT = create_pollable_result(state='cancelled')
FINISHED_RESULT = create_pollable_result(state='success')
QUEUED_RESULT = create_pollable_result(state='queued')
# avoid the polling thread hanging
QUEUED_RESULT._wait_for_completion = func


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
        # Note: Something about the test framework seems to prevent the
        # Pollable result from being destroyed while the polling
        # thread is running. The test will hang if the PollableResult
        # never completes. I haven't seen the same problem in
        # the interpreter.
        pollable = PollableResult(
            mock.Mock(side_effect=[Response({"state": "running"}),
                                   ValueError()]), (),
            polling_interval=0.1)
        pytest.raises(futures.TimeoutError, pollable.result, timeout=0.05)

    def test_no_hanging(self):
        # Make sure that an error in the `_check_result` doesn't
        # cause an infinite loop.
        class PollableResultTester(PollableResult):
            def __init__(self, *args, **kwargs):
                self._poll_ct = 0
                super().__init__(*args, **kwargs)

            def _check_result(self):
                if self._poll_ct is not None:
                    self._poll_ct += 1
                    if self._poll_ct > 10:
                        self._poll_ct = None  # Disable the counter.
                        # Make the _wait_for_completion loop fail.
                        raise ZeroDivisionError()
                return super()._check_result()

        # The following should raise a CivisJobFailure before a TimeoutError.
        pollable = PollableResultTester(
            lambda: Response({"state": "running"}), (),
            polling_interval=0.1)
        pytest.raises(ZeroDivisionError, pollable.result, timeout=5)

    def test_poll_on_creation(self):
        poller = mock.Mock(side_effect=Response({"state": "running"}))
        pollable = PollableResult(poller,
                                  (),
                                  polling_interval=1,
                                  poll_on_creation=False)
        repr(pollable)
        assert poller.call_count == 0
        time.sleep(1.1)
        assert poller.call_count == 1


if __name__ == '__main__':
    unittest.main()
