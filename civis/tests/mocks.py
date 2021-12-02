"""Mock client creation and tooling
"""
from functools import lru_cache
from unittest import mock

from civis import APIClient
from civis.resources import API_SPEC_PATH
from civis.response import Response


def create_client_mock(cache=API_SPEC_PATH):
    """Create an APIClient mock from a cache of the API spec

    Parameters
    ----------
    cache : str, optional
        Location of the API spec on the local filesystem

    Returns
    -------
    mock.Mock
        A `Mock` object which looks like an APIClient and which will
        error if any method calls have non-existent / misspelled parameters
    """
    # Create a client from the cache. We'll use this for auto-speccing.
    real_client = _real_client(cache)

    # Prevent the client from trying to talk to the real API when autospeccing
    with mock.patch('requests.Session', mock.MagicMock):
        mock_client = mock.create_autospec(real_client, spec_set=True)

    return mock_client


def create_client_mock_for_container_tests(
        script_id=-10, run_id=100, state='succeeded',
        run_outputs=None, log_outputs=None):
    """Returns a CivisAPIClient Mock set up for testing methods that use
    container scripts. Contains endpoint method mocks and return values
    for posting container jobs, retrieving outputs, and reading logs.
    Also contains the mocks to cancel the container when the state
    is set to 'failed'.

    Parameters
    ----------
    script_id: int
        Mock-create containers with this ID when calling `post_containers`
        or `post_containers_runs`.
    run_id: int
        Mock-create runs with this ID when calling `post_containers_runs`.
    state: str, optional
        The reported state of the container run
    run_outputs: list, optional
        List of Response objects returned as run outputs
    log_outputs : list, optional
        List of Response objects returned as log outputs

    Returns
    -------
    `unittest.mock.Mock`
        With scripts endpoints `post_containers`, `post_containers_runs`,
        `post_cancel`, and `get_containers_runs` set up.
    """
    c = create_client_mock()

    mock_container = Response({'id': script_id})
    c.scripts.post_containers.return_value = mock_container
    mock_container_run_start = Response({'id': run_id,
                                         'container_id': script_id,
                                         'state': 'queued'})
    mock_container_run = Response({'id': run_id,
                                   'container_id': script_id,
                                   'state': state})
    if state == 'failed':
        mock_container_run['error'] = 'None'
    c.scripts.post_containers_runs.return_value = mock_container_run_start
    c.scripts.get_containers_runs.return_value = mock_container_run
    c.scripts.list_containers_runs_outputs.return_value = (run_outputs or [])
    c.jobs.list_runs_logs.return_value = (log_outputs or [])

    def change_state_to_cancelled(script_id):
        mock_container_run.state = "cancelled"
        return mock_container_run

    c.scripts.post_cancel.side_effect = change_state_to_cancelled

    return c


@lru_cache(maxsize=1)
def _real_client(local_api_spec):
    real_client = APIClient(local_api_spec=local_api_spec, api_key='none')
    real_client._feature_flags = {'noflag': None}
    return real_client
