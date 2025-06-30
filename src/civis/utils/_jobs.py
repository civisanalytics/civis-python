import logging
import operator
import time
from datetime import datetime

from civis import APIClient
from civis.futures import CivisFuture

log = logging.getLogger(__name__)

_FOLLOW_POLL_INTERVAL_SEC = 5
_LOG_REFETCH_CUTOFF_SECONDS = 300
_LOG_REFETCH_COUNT = 100
_LOGS_PER_QUERY = 250


def run_job(job_id, client=None, polling_interval=None):
    """Run a job.

    Parameters
    ----------
    job_id: str or int
        The ID of the job.
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
        client = APIClient()
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
    fut = CivisFuture(client.scripts.get_custom_runs, (job.id, run.id), client=client)
    fut.result()
    outputs = client.scripts.list_custom_runs_outputs(job.id, run.id)
    if JSONValue:
        json_output = [o.value for o in outputs if o.object_type == "JSONValue"]
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
        return json_output[0].json()
    else:
        file_ids = {o.name: o.object_id for o in outputs}
        return file_ids


def _timestamp_from_iso_str(s):
    """Return an integer POSIX timestamp for a given ISO date string.

    Note: Until Python 3.11, datetime.fromisoformat doesn't work
    with the format returned by Civis Platform.
    """
    try:
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        try:
            # This is the format that Civis Platform returns.
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
        except ValueError:
            # Another format, just in case.
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z").timestamp()


def _compute_effective_max_log_id(logs):
    """Find a max log ID use in order to avoid missing late messages.

    The order of log IDs may not be consistent with "created at" times
    since log entries are created by Civis Platform as well as the code
    for the job itself. This function looks through recent logs
    and finds a maximum ID that is at least as old as a set cutoff period,
    so that messages with lower IDs that show up a bit late won't be skipped.
    With this, it is still theoretically possible but extremely unlikely
    for some late log messages to be skipped in the job_logs function.
    """
    if not logs:
        return 0

    sorted_logs = sorted(logs, key=operator.itemgetter("id"))

    max_created_at_timestamp = _timestamp_from_iso_str(sorted_logs[-1]["created_at"])
    cutoff = time.time() - _LOG_REFETCH_CUTOFF_SECONDS
    if max_created_at_timestamp < cutoff:
        return sorted_logs[-1]["id"]
    elif len(sorted_logs) >= _LOG_REFETCH_COUNT:
        return sorted_logs[-_LOG_REFETCH_COUNT]["id"]

    return 0


def _job_finished_past_timeout(job_id, run_id, finished_timeout, client):
    """Return true if the run finished more than so many seconds ago."""
    if finished_timeout is None:
        return False

    run = client.jobs.get_runs(job_id, run_id)
    finished_at = run.json()["finished_at"]
    if finished_at is None:
        return False
    finished_at_ts = _timestamp_from_iso_str(finished_at)
    result = finished_at_ts < time.time() - finished_timeout
    return result


def job_logs(job_id, run_id=None, finished_timeout=None, client=None):
    """Return a generator of log message dictionaries for a given run.

    Parameters
    ----------
    job_id : int
        The ID of the job to retrieve log message for.
    run_id : int or None
        The ID of the run to retrieve log messages for.
        If None, the ID for the most recent run will be used.
    finished_timeout: int or None
        If not None, then this function will return once the run has
        been finished for the specified number of seconds.
        If None, then this function will wait until the API says there
        will be no more new log messages, which may take a few minutes.
        A timeout of 30-60 seconds is usually enough to retrieve all
        log messages.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.

    Yields
    ------
    dict
        A log message dictionary with "message", "created_at" and other attributes
        provided by the job logs endpoint. Note that this will block execution
        until the job has stopped and all log messages are retrieved.

    Examples
    --------
    >>> # Print all log messages from a job's most recent run
    >>> for log in job_logs(job_id=123456):
    ...     print(f"{log['created_at']}: {log['message']}")
    ...
    >>> # Get logs from a specific run with a 30 second timeout
    >>> for log in job_logs(job_id=123456, run_id=789, finished_timeout=30):
    ...     print(log['message'])
    """
    client = client or APIClient()

    if run_id is None:
        run_id = client.jobs.list_runs(
            job_id, limit=1, order="id", order_dir="desc"
        ).json()[0]["id"]

    local_max_log_id = 0
    continue_polling = True

    known_log_ids = set()

    while continue_polling:
        # This call gets a limited number of log messages since last_id,
        # ordered by log ID.
        response = client.jobs.list_runs_logs(
            job_id,
            run_id,
            last_id=local_max_log_id,
            limit=_LOGS_PER_QUERY,
        )
        if "civis-max-id" in response.headers:
            remote_max_log_id = int(response.headers["civis-max-id"])
        else:
            # Platform hasn't seen any logs at all yet
            remote_max_log_id = None
        logs = response.json()
        if logs:
            local_max_log_id = max(log["id"] for log in logs)
            logs.sort(key=operator.itemgetter("created_at", "id"))
        for log in logs:
            if log["id"] in known_log_ids:
                continue
            known_log_ids.add(log["id"])
            yield log

        log_finished = response.headers["civis-cache-control"] != "no-store"

        if remote_max_log_id is None:
            remote_has_more_logs_to_get_now = False
        elif local_max_log_id == remote_max_log_id:
            remote_has_more_logs_to_get_now = False
            local_max_log_id = _compute_effective_max_log_id(logs)
            if log_finished or _job_finished_past_timeout(
                job_id, run_id, finished_timeout, client
            ):
                continue_polling = False
        else:
            remote_has_more_logs_to_get_now = True

        if continue_polling and not remote_has_more_logs_to_get_now:
            time.sleep(_FOLLOW_POLL_INTERVAL_SEC)
