from __future__ import absolute_import
from builtins import super

from civis.utils._deprecation import deprecate
from civis.futures import (  # noqa
    _ResultPollingThread,
    CivisFuture,
    _DEFAULT_POLLING_INTERVAL)


# keep this for backwards compatibility
@deprecate("v2.0.0")
class PollableResult(CivisFuture):
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
    api_key : DEPRECATED str, optional
        This is not used by PollableResult, but is required to match the
        interface from CivisAsyncResultBase.
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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # remove the pubnub attribute so we do not report
        # that we are subscribed
        delattr(self, '_pubnub')

    # override these methods to make pubnub not work
    def _pubnub_config(self, *args, **kwargs):
        return None, None

    def _subscribe(self, *args, **kwargs):
        return None
