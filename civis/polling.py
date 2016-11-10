from concurrent import futures
import time

from civis.base import CivisJobFailure
from civis.response import Response


FINISHED = ['success', 'succeeded']
FAILED = ['failed']
NOT_FINISHED = ['queued', 'running']
CANCELLED = ['cancelled']
DONE = FINISHED + FAILED + CANCELLED
_DEFAULT_POLLING_INTERVAL = 15

# Translate Civis state strings into `future` state strings
STATE_TRANS = {}
for name in FINISHED + FAILED:
    STATE_TRANS[name] = futures._base.FINISHED
for name in NOT_FINISHED:
    STATE_TRANS[name] = futures._base.RUNNING
for name in CANCELLED:
    STATE_TRANS[name] = futures._base.CANCELLED_AND_NOTIFIED


class PollableResult(futures.Future):
    """A class for tracking pollable results.

    This class will begin polling immediately upon creation, and poll for
    job completion once every `polling_interval` seconds until the job
    completes in Civis.

    Parameters
    ----------
    poller : func
        A function which returns an object that has a ``state`` attribute.
    poller_args : tuple
        The arguments with which to call the poller function.
    polling_interval : int or float
        The number of seconds between API requests to check whether a result
        is ready.
    """
    # this may not be friendly to a rate-limited api
    # Implementation notes: The `PollableResult` depends on some private
    # features of the `concurrent.futures.Future` class, so it's possible
    # that future versions of Python could break something here.
    # (It works under at least 3.4 and 3.5.)
    # We use the following `Future` implementation details
    # - The `Future` checks its state against predefined strings. We use
    #   `STATE_TRANS` to translate from the Civis platform states to `Future`
    #    states.
    # - `Future` uses a `_state` attribute to check its current condition
    # - `Future` handles event notification through `set_result` and
    #   `set_exception`, which we call from `_check_result`.
    # - We use the `Future` thread lock called `_condition`
    # - We assume that results of the Future are stored in `_result`.
    def __init__(self, poller, poller_args,
                 polling_interval=_DEFAULT_POLLING_INTERVAL):
        super().__init__()

        # Polling arguments. Never poll more often than the requested interval.
        self._poller = poller
        self._poller_args = poller_args
        self.polling_interval = polling_interval
        self._last_polled = None
        self._last_result = None

        self._self_polling_executor = None

    def __repr__(self):
        # Almost the same as the superclass's __repr__, except we use
        # the `_civis_state` rather than the `_state`.
        with self._condition:
            if self._civis_state in FINISHED + FAILED:
                if self.exception():
                    return '<%s at %#x state=%s raised %s>' % (
                        self.__class__.__name__,
                        id(self),
                        self._civis_state,
                        self._exception.__class__.__name__)
                else:
                    return '<%s at %#x state=%s returned %s>' % (
                        self.__class__.__name__,
                        id(self),
                        self._civis_state,
                        self.result().__class__.__name__)
            out = '<%s at %#x state=%s>' % (self.__class__.__name__,
                                            id(self),
                                            self._civis_state)
            return out

    def cancel(self):
        """Not currently implemented."""
        raise NotImplementedError("Running jobs cannot currently be cancelled")

    def succeeded(self):
        """Return ``True`` if the job completed in Civis with no error."""
        with self._condition:
            return self._civis_state in FINISHED

    def failed(self):
        """Return ``True`` if the Civis job failed."""
        with self._condition:
            return self._civis_state in FAILED

    def _wait_for_completion(self):
        """Poll the job every `polling_interval` seconds. Blocks until the
        job completes.
        """
        try:
            while self._civis_state not in DONE:
                time.sleep(self.polling_interval)
        except Exception as e:
            # Exceptions are caught in `_check_result`, so
            # we should never get here. If there were to be a
            # bug in `_check_result`, however, we would get stuck
            # in an infinite loop without setting the `_result`.
            with self._condition:
                self._result = Response({"state": FAILED[0]})
                self.set_exception(e)

    def _poll_wait_elapsed(self, now):
        # thie exists because it's easier to monkeypatch in testing
        return (now - self._last_polled) >= self.polling_interval

    def _check_result(self):
        """Return the job result from Civis. Once the job completes, store the
        result and never poll again."""

        # If we haven't started the polling thread, do it now.
        if self._self_polling_executor is None and self._result is None:
            # Start a single thread continuously polling. It will stop once the
            # job completes.
            self._self_polling_executor = futures.ThreadPoolExecutor(1)
            self._self_polling_executor.submit(self._wait_for_completion)

        with self._condition:
            if self._result is not None:
                # If the job is already completed, just return the stored
                # result.
                return self._result

            # Check to see if the job has finished, but don't poll more
            # frequently than the requested polling frequency.
            now = time.time()
            if not self._last_polled or self._poll_wait_elapsed(now):
                # Poll for a new result
                self._last_polled = now
                try:
                    self._last_result = self._poller(*self._poller_args)
                except Exception as e:
                    # The _poller can raise API exceptions
                    # Set those directly as this Future's exception
                    self._result = Response({"state": FAILED[0]})
                    self._last_result = self._result
                    self.set_exception(e)
                else:
                    # If the job has finished, then register completion and
                    # store the results. Because of the `if self._result` check
                    # up top, we will never get here twice.
                    if self._last_result.state in FAILED:
                        try:
                            err_msg = str(self._last_result['error'])
                        except:
                            err_msg = str(self._last_result)
                        self.set_exception(CivisJobFailure(err_msg,
                                           self._last_result))

                        self._result = self._last_result
                    elif self._last_result.state in DONE:
                        self.set_result(self._last_result)

            return self._last_result

    @property
    def _civis_state(self):
        """State as returned from Civis."""
        with self._condition:
            return self._check_result().state

    @property
    def _state(self):
        """State of the PollableResult in `future` language."""
        with self._condition:
            return STATE_TRANS[self._civis_state]

    @_state.setter
    def _state(self, value):
        # Ignore attempts to set the _state from the `Future` superclass
        pass
