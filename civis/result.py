from civis import APIClient
from civis.pubnub import has_pubnub, SubscribableResult
from civis.polling import PollableResult


def make_platform_future(poller, poller_args,
                         polling_interval=None,
                         api_key=None):
    """Return the best Future subclass for monitoring your Civis job

    This function checks the local environment to see if
    the `pubnub` package is installed (required to use the
    notifications API endpoint) and the "channels" API endpoint
    is accessible. If so, you get a
    :class:`~civis.pubnub.SubscribableResult`.
    This is faster and uses fewer API calls.
    If `pubnub` is not available, the result is a
    :class:`~civis.polling.PollableResult`, which uses API calls
    to determine when a run completes.

    Parameters
    ----------
    poller : func
        A function which returns an object that has a ``state`` attribute.
    poller_args : tuple
        The arguments with which to call the poller function.
    polling_interval : int or float, optional
        This is not used by SubscribableResult, but is required to match the
        interface from CivisAsyncResultBase.
    api_key : str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.

    Returns
    -------
    :class:`~civis.polling.PollableResult` or
    :class:`~civis.pubnub.SubscribableResult`

    See Also
    --------
    :class:`~civis.base.CivisAsyncResultBase`
    """
    client = APIClient(api_key=api_key, resources='all')
    if has_pubnub and hasattr(client, 'channels'):
        klass = SubscribableResult
    else:
        klass = PollableResult
    return klass(poller=poller,
                 poller_args=poller_args,
                 polling_interval=polling_interval,
                 api_key=api_key)
