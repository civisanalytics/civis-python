import time
import threading

from civis.base import CivisJobFailure, CivisAsyncResultBase, FAILED, DONE
from civis.response import Response


_MAX_POLLING_INTERVAL = 15


class _ResultPollingThread(threading.Thread):
    """Poll a function until it returns a Response with a DONE state"""

    # Inspired by `threading.Timer`

    def __init__(self, pollable_result):
        super().__init__(daemon=True)
        self.pollable_result = pollable_result
        self.finished = threading.Event()

    def cancel(self):
        """Stop the poller if it hasn't finished yet."""
        self.finished.set()

    def join(self, timeout=None):
        """Shut down the polling when the thread is terminated."""
        self.cancel()
        super().join(timeout=timeout)

    def run(self):
        """Poll until done."""
        while not self.finished.wait(self.pollable_result._next_polling_interval):
            # Spotty internet connectivity can result in polling functions
            # returning None. This treats None responses like responses which
            # have a non-DONE state.
            poller_result = self.pollable_result._check_result()
            if poller_result is not None and poller_result.state in DONE:
                self.finished.set()


class PollableResult(CivisAsyncResultBase):
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
    polling_interval : int or float, optional
        The number of seconds between API requests to check whether a result
        is ready. If an integer or float is provided, this number will be used
        as the polling interval. If ``None`` (the default), the polling interval will
        start at 1 second and increase geometrically up to 15 seconds. The ratio of
        the increase is 1.2, resulting in polling intervals in seconds of
        1, 1.2, 1.44, 1.728, etc. This default behavior allows for a faster return for
        a short-running job and a capped polling interval for longer-running jobs.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    poll_on_creation : bool, optional
        If ``True`` (the default), it will poll upon calling ``result()`` the
        first time. If ``False``, it will wait the number of seconds specified
        in `polling_interval` from object creation before polling.

    Examples
    --------
    >>> client = civis.APIClient()
    >>> database_id = client.get_database_id("my_database")
    >>> cred_id = client.default_database_credential_id
    >>> sql = "SELECT 1"
    >>> preview_rows = 10
    >>> response = client.queries.post(database_id, sql, preview_rows,
    >>>                                credential=cred_id)
    >>> job_id = response.id
    >>>
    >>> poller = client.queries.get
    >>> poller_args = (job_id, ) # (job_id, run_id) if poller requires run_id
    >>> polling_interval = 10
    >>> poll = PollableResult(poller, poller_args, polling_interval)
    """

    # this may not be friendly to a rate-limited api
    # Implementation notes: The `PollableResult` depends on some private
    # features of the `concurrent.futures.Future` class, so it's possible
    # that future versions of Python could break something here.
    # (It works under at least 3.6)
    # We use the following `Future` implementation details
    # - The `Future` checks its state against predefined strings. We use
    #   `STATE_TRANS` to translate from the Civis platform states to `Future`
    #    states.
    # - `Future` uses a `_state` attribute to check its current condition
    # - `Future` handles event notification through `set_result` and
    #   `set_exception`, which we call from `_check_result`.
    # - We use the `Future` thread lock called `_condition`
    # - We assume that results of the Future are stored in `_result`.
    def __init__(
        self,
        poller,
        poller_args,
        polling_interval=None,
        client=None,
        poll_on_creation=True,
    ):
        super().__init__()

        self.poller = poller
        self.poller_args = poller_args
        self.polling_interval = polling_interval
        self.client = client
        self.poll_on_creation = poll_on_creation

        if self.polling_interval is not None and self.polling_interval <= 0:
            raise ValueError("The polling interval must be positive.")

        self._next_polling_interval = 1
        self._use_geometric_polling = True

        # Polling arguments. Never poll more often than the requested interval.
        if poll_on_creation:
            self._last_polled = None
        else:
            self._last_polled = time.time()
        self._last_result = None

        self._begin_tracking()

    def _begin_tracking(self, start_thread=False):
        """Start monitoring the Civis Platform job"""
        with self._condition:
            if getattr(self, "poller", None) is None:
                raise RuntimeError(
                    "Internal error: Must set polling "
                    "function before initializing thread."
                )
            self._reset_polling_thread(self.polling_interval, start_thread)

    def _check_result(self):
        """Return the job result from Civis. Once the job completes, store the
        result and never poll again."""
        with self._condition:
            # Start a single thread continuously polling.
            # It will stop once the job completes.
            if not self._polling_thread.is_alive() and self._result is None:
                self._polling_thread.start()

            if self._result is not None:
                # If the job is already completed, just return the stored
                # result.
                return self._result

            # Check to see if the job has finished, but don't poll more
            # frequently than the requested polling frequency.
            now = time.time()
            if (
                not self._last_polled
                or (now - self._last_polled) >= self._next_polling_interval
            ):
                if self._use_geometric_polling:
                    # Choosing a common ratio of 1.2 for these polling intervals:
                    #   1, 1.2, 1.44, 1.73, 2.07, 2.49, 2.99, ..., and capped at 15.
                    # Within the first 15 secs by wall time, we call the poller 7 times,
                    # which gives a short-running job's future.result()
                    # a higher chance to return faster.
                    # For longer running jobs, the polling interval will be capped
                    # at 15 secs when by wall time 87 secs have passed.
                    self._next_polling_interval *= 1.2
                    if self._next_polling_interval > _MAX_POLLING_INTERVAL:
                        self._next_polling_interval = _MAX_POLLING_INTERVAL
                        self._use_geometric_polling = False
                # Poll for a new result
                self._last_polled = now
                try:
                    self._last_result = self.poller(*self.poller_args)
                except Exception as e:
                    # The _poller can raise API exceptions
                    # Set those directly as this Future's exception
                    self._set_api_exception(exc=e)
                else:
                    # If the job has finished, then register completion and
                    # store the results. Because of the `if self._result` check
                    # up top, we will never get here twice.
                    self._set_api_result(self._last_result)

            return self._last_result

    def _set_api_result(self, result):
        with self._condition:
            if result.state in FAILED:
                try:
                    err_msg = str(result["error"])
                except:  # NOQA
                    err_msg = str(result)
                job_id = getattr(self, "job_id", None)
                run_id = getattr(self, "run_id", None)
                self._set_api_exception(
                    exc=CivisJobFailure(err_msg, result, job_id, run_id),
                    result=result,
                )
            elif result.state in DONE:
                self.set_result(result)
                self.cleanup()

    def _set_api_exception(self, exc, result=None):
        with self._condition:
            if result is None:
                result = Response({"state": FAILED[0]})
            self._result = result
            self._last_result = self._result
            self.set_exception(exc)
            self.cleanup()

    def cleanup(self):
        # This gets called after the result is set.
        # Ensure that the polling thread shuts down when it's no longer needed.
        with self._condition:
            if self._polling_thread.is_alive():
                self._polling_thread.cancel()

    def _reset_polling_thread(self, polling_interval, start_thread=False):
        with self._condition:
            if (
                getattr(self, "_polling_thread", None) is not None
                and self._polling_thread.is_alive()
            ):
                self._polling_thread.cancel()
            self.polling_interval = polling_interval
            self._next_polling_interval = 1 if (pi := polling_interval) is None else pi
            self._use_geometric_polling = polling_interval is None
            self._polling_thread = _ResultPollingThread(self)
            if start_thread:
                self._polling_thread.start()
