from __future__ import absolute_import
from builtins import super

import warnings
import time
from concurrent import futures
import threading

from civis import APIClient
from civis.base import CivisJobFailure, FINISHED, STATE_TRANS, FAILED, DONE
from civis.response import Response

from pubnub.pubnub import PubNub
from pubnub.pnconfiguration import PNConfiguration, PNReconnectionPolicy
from pubnub.enums import PNStatusCategory
from pubnub.callbacks import SubscribeCallback

_DEFAULT_POLLING_INTERVAL = 15

# Pubnub connections can recover missed messages upon reconnecting for up to 10
# minutes from the disconnect. Polling on a 9.5 minute interval is used as a
# fallback in case the job complete message is missed in an outage.
_LONG_POLLING_INTERVAL = 9.5 * 60


class JobCompleteListener(SubscribeCallback):
    _disconnect_categories = [
        PNStatusCategory.PNTimeoutCategory,
        PNStatusCategory.PNNetworkIssuesCategory,
        PNStatusCategory.PNUnexpectedDisconnectCategory,
    ]

    def __init__(self, match_function, callback_function,
                 disconnect_function=None):
        self.match_function = match_function
        self.callback_function = callback_function
        self.disconnect_function = disconnect_function

    def message(self, pubnub, message):
        if self.match_function(message.message):
            self.callback_function()

    def status(self, pubnub, status):
        if status.category in self._disconnect_categories:
            if self.disconnect_function:
                self.disconnect_function()

    def presence(self, pubnub, presence):
        pass


class _ResultPollingThread(threading.Thread):
    """Poll a function until it returns a Response with a DONE state
    """
    # Inspired by `threading.Timer`

    def __init__(self, poller, poller_args, polling_interval):
        super().__init__()
        self.polling_interval = polling_interval
        self.poller = poller
        self.poller_args = poller_args
        self.finished = threading.Event()

    def cancel(self):
        """Stop the poller if it hasn't finished yet.
        """
        self.finished.set()

    def join(self, timeout=None):
        """Shut down the polling when the thread is terminated.
        """
        self.cancel()
        super().join(timeout=timeout)

    def run(self):
        """Poll until done.
        """
        while not self.finished.wait(self.polling_interval):
            if self.poller(*self.poller_args).state in DONE:
                self.finished.set()


class CivisFuture(futures.Future):
    """
    A class for tracking future results.

    This class will attempt to subscribe to a Pubnub channel to listen for
    job completion events. If you don't have access to Pubnub channels, then
    it will fallback to polling.

    This is a subclass of :class:`python:concurrent.futures.Future` from the
    Python standard library. See:
    https://docs.python.org/3/library/concurrent.futures.html

    Parameters
    ----------
    poller : func
        A function which returns an object that has a ``state`` attribute.
    poller_args : tuple
        The arguments with which to call the poller function.
    polling_interval : int or float, optional
        The number of seconds between API requests to check whether a result
        is ready.
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`civis.APIClient`, optional
    poll_on_creation : bool, optional
        If ``True`` (the default), it will poll upon calling ``result()`` the
        first time. If ``False``, it will wait the number of seconds specified
        in `polling_interval` from object creation before polling.

    Examples
    --------
    This example is provided as a function at :func:`~civis.io.query_civis`.

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
    >>> future = CivisFuture(poller, poller_args, polling_interval)
    """
    # Only polling and not using pubnub may not be friendly to a
    # rate-limited api
    #
    # Implementation notes: The `CivisFuture` depends on some private
    # features of the `concurrent.futures.Future` class, so it's possible
    # that future versions of Python could break something here.
    # (It works under at least 3.4, 3.5, and 3.6)
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
                 polling_interval=None, api_key=None, client=None,
                 poll_on_creation=True):
        super().__init__()

        if api_key is not None:
            warnings.warn('The "api_key" parameter is deprecated and will be '
                          'removed in v2. Please use the `client` parameter '
                          'instead.', FutureWarning)
        self.client = client or APIClient(api_key=api_key, resources='all')

        if polling_interval is None:
            if hasattr(self.client, 'channels'):
                self.polling_interval = _LONG_POLLING_INTERVAL
            else:
                self.polling_interval = _DEFAULT_POLLING_INTERVAL
        else:
            self.polling_interval = polling_interval
        if self.polling_interval <= 0:
            raise ValueError("The polling interval must be positive.")

        self.poller = poller
        self.poller_args = poller_args
        self.poll_on_creation = poll_on_creation

        # Polling arguments. Never poll more often than the requested interval.
        if poll_on_creation:
            self._last_polled = None
        else:
            self._last_polled = time.time()
        self._last_result = None

        self._polling_thread = _ResultPollingThread(self._check_result, (),
                                                    polling_interval)

        if hasattr(self.client, 'channels'):
            config, channels = self._pubnub_config()
            self._pubnub = self._subscribe(config, channels)

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

    @property
    def subscribed(self):
        return (hasattr(self, '_pubnub') and
                len(self._pubnub.get_subscribed_channels()) > 0)

    @property
    def _civis_state(self):
        """State as returned from Civis."""
        with self._condition:
            if self._check_result():
                return self._check_result().state
            return 'running'

    @property
    def _state(self):
        """State of the CivisAsyncResultBase in `future` language."""
        with self._condition:
            return STATE_TRANS[self._civis_state]

    @_state.setter
    def _state(self, value):
        # Ignore attempts to set the _state from the `Future` superclass
        pass

    def _subscribe(self, pnconfig, channels):
        listener = JobCompleteListener(self._check_message,
                                       self._poll_and_set_api_result,
                                       self._reset_polling_thread)
        pubnub = PubNub(pnconfig)
        pubnub.add_listener(listener)
        pubnub.subscribe().channels(channels).execute()
        return pubnub

    def _pubnub_config(self):
        channel_config = self.client.channels.list()
        channels = [channel['name'] for channel in channel_config['channels']]
        pnconfig = PNConfiguration()
        pnconfig.subscribe_key = channel_config['subscribe_key']
        pnconfig.cipher_key = channel_config['cipher_key']
        pnconfig.auth_key = channel_config['auth_key']
        pnconfig.ssl = True
        pnconfig.reconnect_policy = PNReconnectionPolicy.LINEAR
        return pnconfig, channels

    def _check_message(self, message):
        try:
            # poller_args can be (job_id,) or (job_id, run_id)
            if len(self.poller_args) == 1:
                match = (message['object']['id'] == self.poller_args[0] and
                         message['run']['state'] in DONE)
            else:
                match = (message['object']['id'] == self.poller_args[0] and
                         message['run']['id'] == self.poller_args[1] and
                         message['run']['state'] in DONE)
        except KeyError:
            return False
        return match

    def _poll_and_set_api_result(self):
        with self._condition:
            try:
                result = self.poller(*self.poller_args)
                self._set_api_result(result)
            except Exception as e:
                self._set_api_exception(exc=e)

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
            if (not self._last_polled or
                    (now - self._last_polled) >= self.polling_interval):
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
        # This gets called after the result is set.
        # Ensure that the polling thread shuts down when it's no longer needed.
        with self._condition:
            if self._polling_thread.is_alive():
                self._polling_thread.cancel()
            if hasattr(self, '_pubnub'):
                self._pubnub.unsubscribe_all()

    def _reset_polling_thread(self,
                              polling_interval=_DEFAULT_POLLING_INTERVAL):
        with self._condition:
            if self._polling_thread.is_alive():
                self._polling_thread.cancel()
            self.polling_interval = polling_interval
            self._polling_thread = _ResultPollingThread(self._check_result, (),
                                                        polling_interval)


# keep this for backwards compatibility
PollableResult = CivisFuture
