from civis import APIClient
from civis.futures import CivisFuture
from civis.utils._deprecation import deprecate_param


@deprecate_param('v2.0.0', 'api_key')
def run_job(job_id, api_key=None, client=None):
    """Run a job.

    Parameters
    ----------
    job_id : str or int
        The ID of the job.
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.

    Returns
    -------
    results : :class:`~civis.futures.CivisFuture`
        A `CivisFuture` object.
    """
    if client is None:
        client = APIClient(api_key=api_key, resources='all')
    run = client.jobs.post_runs(job_id)
    return CivisFuture(client.jobs.get_runs,
                       (job_id, run['id']),
                       client=client,
                       poll_on_creation=False)
