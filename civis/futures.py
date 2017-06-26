from __future__ import absolute_import

from builtins import super
import logging
import time

from civis import APIClient
from civis.base import DONE
from civis.polling import PollableResult, _ResultPollingThread

try:
    from pubnub.pubnub import PubNub
    from pubnub.pnconfiguration import PNConfiguration, PNReconnectionPolicy
    from pubnub.enums import PNStatusCategory
    from pubnub.callbacks import SubscribeCallback
    has_pubnub = True
except ImportError:
    has_pubnub = False

log = logging.getLogger(__name__)

# Pubnub connections can recover missed messages upon reconnecting for up to 10
# minutes from the disconnect. Polling on a 9.5 minute interval is used as a
# fallback in case the job complete message is missed in an outage.
_LONG_POLLING_INTERVAL = 9.5 * 60

if has_pubnub:
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


class CivisFuture(PollableResult):
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
    def __init__(self, poller, poller_args,
                 polling_interval=None, api_key=None, client=None,
                 poll_on_creation=True):
        if client is None:
            client = APIClient(api_key=api_key, resources='all')

        if (polling_interval is None and
                has_pubnub and
                hasattr(client, 'channels')):
            polling_interval = _LONG_POLLING_INTERVAL

        super().__init__(poller=poller,
                         poller_args=poller_args,
                         polling_interval=polling_interval,
                         api_key=api_key,
                         client=client,
                         poll_on_creation=poll_on_creation)

        if has_pubnub and hasattr(client, 'channels'):
            config, channels = self._pubnub_config()
            self._pubnub = self._subscribe(config, channels)

    @property
    def subscribed(self):
        return (hasattr(self, '_pubnub') and
                len(self._pubnub.get_subscribed_channels()) > 0)

    def cleanup(self):
        with self._condition:
            super().cleanup()
            if hasattr(self, '_pubnub'):
                self._pubnub.unsubscribe_all()

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


class ContainerFuture(CivisFuture):
    """Encapsulates asynchronous execution of a Civis Container Script

    This object includes the ability to cancel a run in progress,
    as well as the option to automatically retry failed runs.
    Retries should only be used for idempotent scripts which might fail
    because of network or other random failures.

    Parameters
    ----------
    job_id: int
        The ID for the container/script/job.
    run_id : int
        The ID for the run to monitor
    max_n_retries : int, optional
        If the job generates an exception, retry up to this many times
    polling_interval: int or float, optional
        The number of seconds between API requests to check whether a result
        is ready. You should not set this if you're using ``pubnub``
        (the default if ``pubnub`` is installed).
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    poll_on_creation : bool, optional
        If ``True`` (the default), it will poll upon calling ``result()`` the
        first time. If ``False``, it will wait the number of seconds specified
        in `polling_interval` from object creation before polling.

    See Also
    --------
    civis.futures.CivisFuture
    """
    def __init__(self, job_id, run_id,
                 max_n_retries=0,
                 polling_interval=None,
                 client=None,
                 poll_on_creation=True):
        if client is None:
            client = APIClient(resources='all')
        super().__init__(client.scripts.get_containers_runs,
                         [int(job_id), int(run_id)],
                         polling_interval=polling_interval,
                         client=client,
                         poll_on_creation=poll_on_creation)
        self._max_n_retries = max_n_retries

    @property
    def job_id(self):
        return self.poller_args[0]

    @property
    def run_id(self):
        return self.poller_args[1]

    def _set_api_exception(self, exc, result=None):
        # Catch attempts to set an exception. If there's retries
        # remaining, retry the run instead of erroring.
        with self._condition:
            if self._max_n_retries > 0:
                # Start a new run of the script and update
                # the run ID used by the poller.
                self.cleanup()
                self._last_result = self.client.jobs.post_runs(self.job_id)
                orig_run_id = self.run_id
                self.poller_args[1] = run_id = self._last_result.id
                self._max_n_retries -= 1
                self._last_polled = time.time()

                # Threads can only be started once, and the last thread
                # stopped in cleanup. Start a new polling thread.
                # Note that it's possible to have a race condition if
                # you shut down the old thread too soon after starting it.
                # In practice this only happens when testing retries
                # with extremely short polling intervals.
                self._polling_thread = _ResultPollingThread(
                    self._check_result, (), self.polling_interval)
                self._polling_thread.start()

                if hasattr(self, '_pubnub'):
                    # Subscribe to the new run's notifications endpoint
                    self._pubnub = self._subscribe(*self._pubnub_config())
                log.debug('Job ID %d / Run ID %d failed. Retrying '
                          'with run %d. %d retries remaining.',
                          self.job_id, orig_run_id,
                          run_id, self._max_n_retries)
            else:
                super()._set_api_exception(exc=exc, result=result)

    def cancel(self):
        """Submit a request to cancel the container/script/run.

        Returns
        -------
        bool
            Whether or not the job is in a cancelled state.
        """
        with self._condition:
            if self.cancelled():
                return True
            elif not self.done():
                # Cancel the job and store the result of the cancellation in
                # the "finished result" attribute, `_result`.
                self._result = self.client.scripts.post_cancel(self.job_id)
                for waiter in self._waiters:
                    waiter.add_cancelled(self)
                self._condition.notify_all()
                self.cleanup()
                self._invoke_callbacks()
                return self.cancelled()
            return False
