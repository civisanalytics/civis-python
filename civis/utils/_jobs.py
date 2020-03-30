import logging

from civis import APIClient
from civis.futures import CivisFuture
from civis._deprecation import deprecate_param

log = logging.getLogger(__name__)


@deprecate_param("v2.0.0", "api_key")
def run_job(job_id, api_key=None, client=None, polling_interval=None):
    """Run a job.

    Parameters
    ----------
    job_id: str or int
        The ID of the job.
    api_key: DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client: :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    polling_interval : int or float, optional
        The number of seconds between API requests to check whether a result
        is ready.

    Returns
    -------
    results: :class:`~civis.futures.CivisFuture`
        A `CivisFuture` object.
    """
    if client is None:
        client = APIClient(api_key=api_key)
    run = client.jobs.post_runs(job_id)
    return CivisFuture(
        client.jobs.get_runs,
        (job_id, run["id"]),
        client=client,
        polling_interval=polling_interval,
        poll_on_creation=False,
    )


def run_template(id, arguments, JSONValue=False, client=None):
    """Run a template and return the results.

    Parameters
    ----------
    id: int
        The template id to be run.
    arguments: dict
        Dictionary of arguments to be passed to the template.
    JSONValue: bool, optional
        If True, will return the JSON output of the template.
        If False, will return the file ids associated with the
        output results.
    client: :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.

    Returns
    -------
    output: dict
        If JSONValue = False, dictionary of file ids with the keys
        being their output names.
        If JSONValue = True, JSON dict containing the results of the
        template run. Expects only a single JSON result. Will return
        nothing if either there is no JSON result or there is more
        than 1 JSON result.

    Examples
    --------
    >>> # Run template to return file_ids
    >>> run_template(my_template_id, arguments=my_dict_of_args)
    {'output': 1234567}
    >>> # Run template to return JSON output
    >>> run_template(my_template_id, arguments=my_dict_of_args, JSONValue=True)
    {'result1': 'aaa', 'result2': 123}
    """
    if client is None:
        client = APIClient()
    job = client.scripts.post_custom(id, arguments=arguments)
    run = client.scripts.post_custom_runs(job.id)
    fut = CivisFuture(
        client.scripts.get_custom_runs, (job.id, run.id), client=client
    )
    fut.result()
    outputs = client.scripts.list_custom_runs_outputs(job.id, run.id)
    if JSONValue:
        json_output = [
            o.value for o in outputs if o.object_type == "JSONValue"
        ]
        if len(json_output) == 0:
            log.warning("No JSON output for template {}".format(id))
            return
        if len(json_output) > 1:
            log.warning(
                "More than 1 JSON output for template {}"
                " -- returning only the first one.".format(id)
            )
        # Note that the cast to a dict is to convert
        # an expected Response object.
        return dict(json_output[0])
    else:
        file_ids = {o.name: o.object_id for o in outputs}
        return file_ids
