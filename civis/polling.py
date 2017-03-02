from concurrent import futures
import time

from civis.base import CivisJobFailure, CivisAsyncResultBase, FAILED, DONE
from civis.response import Response


_DEFAULT_POLLING_INTERVAL = 15


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
    polling_interval : int or float
        The number of seconds between API requests to check whether a result
        is ready.
    api_key : str, optional
        This is not used by PollableResult, but is required to match the
        interface from CivisAsyncResultBase.
    poll_on_creation : bool, optional
        If ``True`` (the default), it will poll upon calling ``result()`` the
        first time. If ``False``, it will wait the number of seconds specified
        in `polling_interval` from object creation before polling.

    Examples
    --------
    >>> client = civis.APIClient()
    >>> database_id = client.get_database_id("my_database")
    >>> cred_id = client.default_credential
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
                 polling_interval=None, api_key=None,
                 poll_on_creation=True):
        if polling_interval is None:
            polling_interval = _DEFAULT_POLLING_INTERVAL
        super().__init__(poller,
                         poller_args,
                         polling_interval,
                         api_key,
                         poll_on_creation)

        # Polling arguments. Never poll more often than the requested interval.
        if poll_on_creation:
            self._last_polled = None
        else:
            self._last_polled = time.time()
        self._last_result = None

        self._self_polling_executor = None

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
                self._set_api_exception(exc=e)

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
                    err_msg = str(result['error'])
                except:
                    err_msg = str(result)
                self._set_api_exception(exc=CivisJobFailure(err_msg, result),
                                        result=result)
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
        # This gets called after the result is set
        pass
