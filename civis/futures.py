from civis import APIClient
from civis.base import DONE
from civis.polling import PollableResult

try:
    from pubnub.pubnub import PubNub
    from pubnub.pnconfiguration import PNConfiguration, PNReconnectionPolicy
    from pubnub.callbacks import SubscribeCallback
    has_pubnub = True
except ImportError:
    has_pubnub = False

# Pubnub connections can recover missed messages upon reconnecting for up to 10
# minutes from the disconnect. Polling on a 9.5 minute interval is used as a
# fallback in case the job complete message is missed in an outage.
_LONG_POLLING_INTERVAL = 9.5 * 60

if has_pubnub:
    class JobCompleteListener(SubscribeCallback):
        def __init__(self, match_function, callback_function):
            self.match_function = match_function
            self.callback_function = callback_function

        def message(self, pubnub, message):
            if self.match_function(message.message):
                self.callback_function()

        def status(self, pubnub, status):
            pass

        def presence(self, pubnub, presence):
            pass


class CivisFuture(PollableResult):
    """
    A class for tracking future results.

    This class will attempt to subscribe to a Pubnub channel to listen for
    job completion events. If you don't have access to Pubnub channels, then
    it will fallback to polling.

    This is a subclass of ``concurrent.futures.Future`` from the Python
    standard library. See:
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
    api_key : str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    poll_on_creation : bool, optional
        If ``True`` (the default), it will poll upon calling ``result()`` the
        first time. If ``False``, it will wait the number of seconds specified
        in `polling_interval` from object creation before polling.

    Examples
    --------
    This example is provided as a function at ``civis.io.query_civis``.
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
                 polling_interval=None, api_key=None,
                 poll_on_creation=True):
        client = APIClient(api_key=api_key, resources='all')
        if (polling_interval is None and
                has_pubnub and
                hasattr(client, 'channels')):
            polling_interval = _LONG_POLLING_INTERVAL

        super().__init__(poller,
                         poller_args,
                         polling_interval,
                         api_key,
                         poll_on_creation)

        if has_pubnub and hasattr(client, 'channels'):
            config, channels = self._pubnub_config()
            self._pubnub = self._subscribe(config, channels)

    @property
    def subscribed(self):
        return (hasattr(self, '_pubnub') and
                len(self._pubnub.get_subscribed_channels()) > 0)

    def cleanup(self):
        super().cleanup()
        if hasattr(self, '_pubnub'):
            self._pubnub.unsubscribe_all()

    def _subscribe(self, pnconfig, channels):
        listener = JobCompleteListener(self._check_message,
                                       self._poll_and_set_api_result)
        pubnub = PubNub(pnconfig)
        pubnub.add_listener(listener)
        pubnub.subscribe().channels(channels).execute()
        return pubnub

    def _pubnub_config(self):
        client = APIClient(api_key=self.api_key, resources='all')
        channel_config = client.channels.list()
        channels = [channel['name'] for channel in channel_config['channels']]
        pnconfig = PNConfiguration()
        pnconfig.subscribe_key = channel_config['subscribe_key']
        pnconfig.cipher_key = channel_config['cipher_key']
        pnconfig.auth_key = channel_config['auth_key']
        pnconfig.ssl = True
        pnconfig.reconnection_policy = PNReconnectionPolicy.LINEAR
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
