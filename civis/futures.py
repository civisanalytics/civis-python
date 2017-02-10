from civis import APIClient
from civis.pubnub import SubscribableResult, has_pubnub
from civis.polling import (PollableResult,
                           _DEFAULT_POLLING_INTERVAL,
                           _LONG_POLLING_INTERVAL)


class CivisFuture(PollableResult, SubscribableResult):
    def __init__(self, poller, poller_args,
                 polling_interval=_DEFAULT_POLLING_INTERVAL, api_key=None):
        # If it is eligible to connect to Pubnub, use the long polling interval
        client = APIClient(api_key=api_key, resources='all')
        if has_pubnub and hasattr(client, 'channels'):
            polling_interval = _LONG_POLLING_INTERVAL

        super().__init__(poller, poller_args, polling_interval, api_key)
