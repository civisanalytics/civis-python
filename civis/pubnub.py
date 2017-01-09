from concurrent import futures

try:
    from pubnub.pubnub import PubNub
    from pubnub.pnconfiguration import PNConfiguration
    from pubnub.callbacks import SubscribeCallback
    has_pubnub = True
except ImportError:
    has_pubnub = False

from civis.base import CivisJobFailure
from civis.polling import FINISHED, FAILED, DONE, STATE_TRANS


if has_pubnub:
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
            except KeyError:
                pass

        def status(self, pubnub, status):
            pass

        def presence(self, pubnub, presence):
            pass


class SubscribableResult(futures.Future):
    def __init__(self, poller, poller_args, client):
        super().__init__()

        self.poller = poller
        self.poller_args = poller_args
        self._client = client
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
        pnconfig, channels = self._pubnub_config()
        listener = JobCompleteListener(self.poller_args[0],
                                       self._check_api_result)
        pubnub = PubNub(pnconfig)
        pubnub.add_listener(listener)
        pubnub.subscribe().channels(channels).execute()
        return pubnub

    def _pubnub_config(self):
        channel_config = self._client.channels.list()
        channels = [channel['name'] for channel in channel_config['channels']]
        pnconfig = PNConfiguration()
        pnconfig.subscribe_key = channel_config['subscribe_key']
        pnconfig.cipher_key = channel_config['cipher_key']
        pnconfig.auth_key = channel_config['auth_key']
        pnconfig.ssl = True
        pnconfig.reconnect_policy = True
        return pnconfig, channels


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

    def _check_result(self):
        with self._condition:
            if self._result is not None:
                return self._result

    @property
    def _civis_state(self):
        """State as returned from Civis."""
        with self._condition:
            if self._check_result():
                return self._check_result().state
            return 'running'

    @property
    def _state(self):
        """State of the PollableResult in `future` language."""
        with self._condition:
            return STATE_TRANS[self._civis_state]

    @_state.setter
    def _state(self, value):
        # Ignore attempts to set the _state from the `Future` superclass
        pass
