from civis import APIClient
from civis.polling import PollableResult
from civis.pubnub import SubscribableResult, has_pubnub


def run_job(job_id, api_key=None):
    """Run a job.

    Parameters
    ----------
    job_id : str or int
        The ID of the job.
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
    if 'pubnub' in client.feature_flags and has_pubnub:
        return SubscribableResult(client.jobs.get_runs,
                                  (job_id, run['id']),
                                  api_key=api_key)
    return PollableResult(client.jobs.get_runs, (job_id, run['id']))
