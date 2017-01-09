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
    results : :class:`~civis.polling.PollableResult`
        A `PollableResult` object.
    """
    client = APIClient(api_key=api_key, resources='all')
    client.jobs.post_runs(job_id)
    if 'pubnub' in client.feature_flags and has_pubnub:
        return SubscribableResult(client.jobs.get, (job_id,), client)
    return PollableResult(client.jobs.get, (job_id,))
