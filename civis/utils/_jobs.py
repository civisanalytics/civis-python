from civis import APIClient
from civis.result import make_platform_future


def run_job(job_id, polling_interval=None, api_key=None):
    """Run a job.

    Parameters
    ----------
    job_id : str or int
        The ID of the job.
    polling_interval : int or float
        The number of seconds between API requests to check whether
        a result is ready. Used only if the notifications endpoint
        is not available.
    api_key : str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.

    Returns
    -------
    results : :class:`~civis.polling.PollableResult` or
    :class:`~civis.pubnub.SubscribableResult`
        A `PollableResult` or `SubscribableResult` object if a Pubnub
        connection is available.
    """
    client = APIClient(api_key=api_key, resources='all')
    run = client.jobs.post_runs(job_id)
    return make_platform_future(client.jobs.get_runs, (job_id, run['id']),
                                polling_interval=polling_interval,
                                api_key=api_key)
