from concurrent import futures
import time

from civis import APIClient
from civis.base import CivisJobFailure
from civis.response import Response

try:
    from pubnub.pubnub import PubNub
    from pubnub.pnconfiguration import PNConfiguration
    from pubnub.callbacks import SubscribeCallback
except ImportError:
    _has_pubnub = False
else:
    _has_pubnub = True


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

if _has_pubnub:
    class JobCompleteListener(SubscribeCallback):

        def __init__(self, job_id, callback_function):
            self.job_id = job_id
            self.callback_function = callback_function

        def message(self, pubnub, message):
            try:
                result = message.message
                if result['object']['id'] == self.job_id \
                        and result['run']['state'] in DONE:
                    self.callback_function()
            except:
                pass

        def status(self, pubnub, status):
            pass

        def presence(self, pubnub, presence):
            pass


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
                 polling_interval=_DEFAULT_POLLING_INTERVAL):
        super().__init__()

        # Polling arguments. Never poll more often than the requested interval.
        self.poller = poller
        self.poller_args = poller_args
        self.polling_interval = polling_interval
        self._last_polled = None
        self._last_result = None

        self._self_polling_executor = None
        self._pubnub = self._subscribe()

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

    def _subscribe(self):
        pubnub = None
        if _has_pubnub:
            client = APIClient(resources='all')
            me = client.users.list_me()
            if me.get('feature_flags').get('pubnub') and self.poller_args:
                channel = client.channels.list()
                channels = [chan['name'] for chan in channel['channels']]
                pnconfig = PNConfiguration()
                pnconfig.subscribe_key = channel['subscribe_key']
                pnconfig.cipher_key = channel['cipher_key']
                pnconfig.auth_key = channel['auth_key']
                pnconfig.ssl = True
                pnconfig.reconnect_policy = True

                pubnub = PubNub(pnconfig)
                job_id = self.poller_args[0]
                listener = JobCompleteListener(job_id, self._check_api_result)
                pubnub.add_listener(listener)
                pubnub.subscribe().channels(channels).execute()
        return pubnub

    def _check_api_result(self, result=None):
        with self._condition:
            if result is None:
                result = self.poller(*self.poller_args)
            if result.state in FAILED:
                if self._pubnub:
                    self._pubnub.unsubscribe_all()
                try:
                    err_msg = str(result['error'])
                except:
                    err_msg = str(result)
                self.set_exception(CivisJobFailure(err_msg,
                                                   result))
                self._result = result
            elif result.state in DONE:
                if self._pubnub:
                    self._pubnub.unsubscribe_all()
                self.set_result(result)

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
                    self._last_result = self.poller(*self.poller_args)
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
                    self._check_api_result(self._last_result)

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
