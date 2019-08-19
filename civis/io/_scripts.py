import logging

from civis import APIClient
from civis.futures import CivisFuture

log = logging.getLogger(__name__)


def run_template(id, arguments, JSONValue=False):
    """Run a template and return the results.

    Parameters
    ----------
    id : int
        The template id to be run.
    arguments : dict
        Dictionary of arguments to be passed to the template.
    JSONValue : bool
        If True, will return the JSON output of the template.
        If False, will return the file ids associated with the
        output results.

    Returns
    -------
    If JSONValue = False:
      file_ids : dict
        Dictionary of file ids with the keys being their output names.
    If JSONValue = True:
      json_output : civis.response.Response
        JSON object containing the results of the template run.
        Expects only a single JSON result. Will return nothing if
        either there is no JSON result or there is more than 1 JSON result.

    Examples
    --------
    >>> # Run template to return file_ids
    >>> run_template(my_template_id, arguments=my_dict_of_args)
    >>> # Run template to return JSON output
    >>> run_template(my_template_id, arguments=my_dict_of_args, JSONValue=True)
    """
    client = APIClient()
    job = client.scripts.post_custom(id, arguments=arguments)
    run = client.scripts.post_custom_runs(job.id)
    fut = CivisFuture(client.scripts.get_custom_runs, (job.id, run.id))
    fut.result()
    outputs = client.scripts.list_containers_runs_outputs(job.id, run.id)
    if JSONValue:
        json_output = [
            o.value for o in outputs if o.object_type == "JSONValue"
        ]
        if len(json_output) > 1:
            raise RuntimeError(
                'Error in returning JSON outputs for template {}'
                'run -- too many JSON outputs'.format(id)
            )
        if len(json_output) == 0:
            log.debug('No JSON output for template {}'.format(id))
            return
        return json_output[0]
    else:
        file_ids = {o.name: o.object_id for o in outputs}
        return file_ids
