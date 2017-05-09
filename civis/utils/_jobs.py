from civis import APIClient
from civis.futures import CivisFuture
from civis.utils._deprecation import deprecate_param


@deprecate_param('v2.0.0', 'api_key')
def run_job(job_id, wait=False, api_key=None, client=None):
    """Run a job.

    A "job" could be a Container Script, a Query, a Python 3 script, etc.

    Parameters
    ----------
    job_id : str or int
        The ID of the job.
    wait : bool
        If True, block until the run completes.
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`~civis.APIClient`, optional
        If not provided, an :class:`~civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.

    Returns
    -------
    results : :class:`~civis.futures.CivisFuture`
        A `CivisFuture` object.
    """
    if client is None:
        client = APIClient(api_key=api_key, resources='all')
    run = client.jobs.post_runs(job_id)
    fut = CivisFuture(client.jobs.get_runs,
                      (job_id, run['id']),
                      client=client,
                      poll_on_creation=False)
    if wait:
        fut.result()
    return fut


def wait_for_run(job_id, run_id, timeout=None, client=None):
    """Wait for a run to complete

    This can be any kind of run.

    Parameters
    ----------
    job_id : str or int
        The ID of the job.
    run_id : str or int
        The ID of the run.
    timeout : float or int
        If provided, raise a TimeoutError after this many seconds
        if the run has not finished. Otherwise wait indefinitely.
    client : :class:`~civis.APIClient`, optional
        If not provided, an :class:`~civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.

    Returns
    -------
    results : :class:`~civis.response.Response`
        The result of the ``jobs.get_runs`` call after the run has finished
    """
    if client is None:
        client = APIClient(resources='all')
    fut = CivisFuture(client.jobs.get_runs,
                      (job_id, run_id),
                      client=client)
    return fut.result(timeout=timeout)
