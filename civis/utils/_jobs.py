from civis import APIClient
from civis.futures import CivisFuture


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
    results : :class:`~civis.futures.CivisFuture`
        A `CivisFuture` object.
    """
    client = APIClient(api_key=api_key, resources='all')
    run = client.jobs.post_runs(job_id)
    return CivisFuture(client.jobs.get_runs,
                       (job_id, run['id']),
                       api_key=api_key,
                       poll_on_creation=False)
