import logging
import time

from civis import APIClient
from civis.futures import CivisFuture
from civis.utils._deprecation import deprecate_param

log = logging.getLogger(__name__)


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


def retry(exceptions, retries=5, delay=0.5, backoff=2):
    """
    Retry decorator

    Parameters
    ----------
    exceptions: Exception
        exceptions to trigger retry
    retries: int, optional
        number of retries to perform
    delay: float, optional
        delay before next retry
    backoff: int, optional
        factor used to increase delay after each retry

    Returns
    -------
    retry decorator

    Raises
    ------
    exception raised by decorator function
    """
    def deco_retry(f):
        def f_retry(*args, **kwargs):
            n_failed = 0
            new_delay = delay
            while True:
                try:
                    return f(*args, **kwargs)
                except exceptions as exc:
                    if n_failed < retries:
                        n_failed += 1
                        msg = "%s, Retrying in %d seconds..." % (str(exc), new_delay)
                        log.debug(msg)
                        time.sleep(new_delay)
                        new_delay *= backoff
                    else:
                        raise exc

        return f_retry

    return deco_retry
