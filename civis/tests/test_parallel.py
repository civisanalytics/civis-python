from math import sqrt
import io
import pickle
from civis.compat import mock

import pytest
from joblib import delayed, Parallel
from joblib import parallel_backend, register_parallel_backend
from joblib.my_exceptions import TransportableException
from civis.base import CivisAPIError
from civis.response import Response
import requests

import civis.parallel
from civis.futures import ContainerFuture


@pytest.fixture
def mock_job():
    return Response(dict(from_template_id=None,
                         id='42',
                         required_resources={'cpu': 11},
                         params=[{'name': 'spam'}],
                         arguments={'spam': 'eggs'},
                         docker_image_name='image_name',
                         docker_image_tag='tag',
                         repo_http_uri='cabbage',
                         repo_ref='servant',
                         remote_host_credential_id=171,
                         git_credential_id=213,
                         cancel_timeout=23,
                         time_zone="America/Chicago",
                         ))


def test_retries():
    """Make sure that job submission retry behavior works."""

    # Test that submission doesn't fail when there are no mock API errors.
    _test_retries_helper(0, 0, False, None)
    _test_retries_helper(0, 5, False, None)

    # Test that submission fails when there are API errors and too few retries.
    _test_retries_helper(2, 0, True, None)
    _test_retries_helper(2, 1, True, None)
    _test_retries_helper(2, 2, True, None)

    # Test that submission doesn't fail when there are mock API errors and
    # sufficient retries.
    _test_retries_helper(2, 3, False, None)
    _test_retries_helper(2, 4, False, None)


def test_from_template_id():
    _test_retries_helper(0, 5, False, 13)
    _test_retries_helper(1, 5, False, 13)
    _test_retries_helper(1, 0, True, 13)


@mock.patch.object(civis.parallel, '_ContainerShellExecutor')
@mock.patch.object(civis.parallel, 'CustomScriptExecutor')
@mock.patch.object(civis.parallel, '_CivisBackendResult')
@mock.patch.object(civis.parallel.civis.io, 'file_to_civis', autospec=True)
def _test_retries_helper(num_failures, max_submit_retries,
                         should_fail, from_template_id,
                         mock_file_to_civis, mock_result_cls,
                         mock_custom_exec_cls, mock_executor_cls):

    mock_file_to_civis.return_value = 0
    mock_result_cls.get.return_value = 123

    # A function to raise fake API errors the first num_failures times it is
    # called.
    counter = {'n_failed': 0}

    def mock_submit(fn='', *args, **kwargs):
        if counter['n_failed'] < num_failures:
            counter['n_failed'] += 1
            raise CivisAPIError(mock.MagicMock())
        else:
            return mock.MagicMock(spec=ContainerFuture)

    mock_custom_exec_cls.return_value.submit.side_effect = mock_submit
    mock_executor_cls.return_value.submit.side_effect = mock_submit

    if from_template_id:
        factory = civis.parallel.make_backend_template_factory(
            from_template_id=from_template_id,
            max_submit_retries=max_submit_retries,
            client=mock.Mock())
    else:
        factory = civis.parallel.make_backend_factory(
            max_submit_retries=max_submit_retries, client=mock.Mock())
    register_parallel_backend('civis', factory)
    with parallel_backend('civis'):
        parallel = Parallel(n_jobs=5, pre_dispatch='n_jobs')
        if should_fail:
            with pytest.raises(civis.parallel.JobSubmissionError):
                parallel(delayed(sqrt)(i ** 2) for i in range(3))
        else:
            parallel(delayed(sqrt)(i ** 2) for i in range(3))


@mock.patch.object(civis.parallel, 'CustomScriptExecutor')
@mock.patch.object(civis.parallel, '_CivisBackendResult')
@mock.patch.object(civis.parallel.civis.io, 'file_to_civis', autospec=True)
def test_template_submit(mock_file, mock_result, mock_pool):
    # Verify that creating child jobs from a template looks like we expect
    file_id = 17
    mock_client = mock.Mock()
    mock_file.return_value = file_id

    factory = civis.parallel.make_backend_template_factory(
        from_template_id=1234, client=mock_client)

    n_calls = 3
    register_parallel_backend('civis', factory)
    with parallel_backend('civis'):
        parallel = Parallel(n_jobs=5, pre_dispatch='n_jobs')
        parallel(delayed(sqrt)(i ** 2) for i in range(n_calls))

    assert mock_file.call_count == 3, "Upload 3 functions to run"
    assert mock_pool().submit.call_count == n_calls, "Run 3 functions"
    for this_call in mock_pool().submit.call_args_list:
        assert this_call == mock.call(JOBLIB_FUNC_FILE_ID=file_id)
    assert mock_result.call_count == 3, "Create 3 results"


@mock.patch.object(civis.parallel, '_CivisBackend')
def test_make_template(mock_backend):
    # Verify that the input setup command is recognized
    func = civis.parallel.make_backend_template_factory(1234)

    assert mock_backend.call_count == 0
    func()
    assert mock_backend.call_count == 1
    assert mock_backend.call_args_list[0][1].get('from_template_id') == 1234


@mock.patch.object(civis.parallel, '_CivisBackend')
def test_setup_cmd(mock_backend):
    # Verify that the input setup command is recognized
    func = civis.parallel.make_backend_factory(setup_cmd='sample')

    assert mock_backend.call_count == 0
    func()
    assert mock_backend.call_count == 1
    assert mock_backend.call_args_list[0][1].get('setup_cmd') == 'sample'


@mock.patch.object(civis.parallel, '_CivisBackend')
def test_default_setup_cmd_no_repo(mock_backend):
    # Check that the setup command has the expected
    # default when the user does not input a GitHub repo
    func = civis.parallel.make_backend_factory()

    func()
    assert mock_backend.call_count == 1
    assert mock_backend.call_args_list[0][1].get('setup_cmd') == ":"


@mock.patch.object(civis.parallel, '_CivisBackend')
def test_default_setup_cmd_with_repo(mock_backend):
    # Check that the default setup command will attempt to install
    # a supplied GitHub repo.
    func = civis.parallel.make_backend_factory(repo_http_uri='potato')

    func()
    assert mock_backend.call_count == 1
    assert mock_backend.call_args_list[0][1].get('setup_cmd') == \
        "cd /app; python setup.py install; cd /"


@mock.patch.object(civis.parallel, 'make_backend_factory')
def test_infer_no_job_id_error(mock_make_factory, mock_job):
    # The `infer_backend_factory` should give a RuntimeError
    # if there's no CIVIS_JOB_ID in the environment.
    mock_client = mock.MagicMock()
    mock_client.scripts.get_containers.return_value = mock_job
    with mock.patch.dict('os.environ', {}, clear=True):
        with pytest.raises(RuntimeError):
            civis.parallel.infer_backend_factory(client=mock_client)


@mock.patch.object(civis.parallel, 'make_backend_factory')
def test_infer(mock_make_factory, mock_job):
    # Verify that `infer_backend_factory` passes through
    # the expected arguments to `make_backend_factory`.
    mock_client = mock.MagicMock()
    mock_client.scripts.get_containers.return_value = mock_job
    with mock.patch.dict('os.environ', {'CIVIS_JOB_ID': "test_job",
                                        'CIVIS_RUN_ID': "test_run"}):
        civis.parallel.infer_backend_factory(client=mock_client)

    expected = dict(mock_job)
    del expected['from_template_id']
    del expected['id']
    mock_make_factory.assert_called_once_with(
        client=mock_client,
        setup_cmd=None,
        polling_interval=None,
        max_submit_retries=0,
        max_job_retries=0,
        hidden=True,
        remote_backend='sequential',
        **expected)


@mock.patch.object(civis.parallel, 'make_backend_factory')
def test_infer_new_params(mock_make_factory, mock_job):
    # Test overwriting existing job parameters with new parameters
    mock_client = mock.MagicMock()
    mock_client.scripts.get_containers.return_value = mock_job
    new_params = [{'name': 'spam', 'type': 'fun'},
                  {'name': 'foo', 'type': 'bar'}]
    with mock.patch.dict('os.environ', {'CIVIS_JOB_ID': "test_job",
                                        'CIVIS_RUN_ID': "test_run"}):
        civis.parallel.infer_backend_factory(
            client=mock_client, params=new_params)

    assert mock_make_factory.call_args[1]['params'] == new_params


@mock.patch.object(civis.parallel, 'make_backend_factory')
def test_infer_extra_param(mock_make_factory, mock_job):
    # Test adding a new parameter and keeping
    # the existing parameter unchanged.
    mock_client = mock.MagicMock()
    mock_client.scripts.get_containers.return_value = mock_job
    new_params = [{'name': 'foo', 'type': 'bar'}]
    with mock.patch.dict('os.environ', {'CIVIS_JOB_ID': "test_job",
                                        'CIVIS_RUN_ID': "test_run"}):
        civis.parallel.infer_backend_factory(
            client=mock_client, params=new_params)

    assert mock_make_factory.call_args[1]['params'] == \
        [{'name': 'spam'}, {'name': 'foo', 'type': 'bar'}]


@mock.patch.object(civis.parallel, 'make_backend_factory')
def test_infer_update_resources(mock_make_factory, mock_job):
    # Verify that users can modify requested resources for jobs.
    mock_client = mock.MagicMock()
    mock_client.scripts.get_containers.return_value = mock_job
    with mock.patch.dict('os.environ', {'CIVIS_JOB_ID': "test_job",
                                        'CIVIS_RUN_ID': "test_run"}):
        civis.parallel.infer_backend_factory(
            client=mock_client, required_resources={'cpu': -11})

    assert mock_make_factory.call_args[1]['required_resources'] == \
        {'cpu': -11}


@mock.patch.object(civis.parallel, 'make_backend_factory')
def test_infer_update_args(mock_make_factory, mock_job):
    # Verify that users can modify the existing job's
    # arguments for sub-processes.
    mock_client = mock.MagicMock()
    mock_client.scripts.get_containers.return_value = mock_job
    with mock.patch.dict('os.environ', {'CIVIS_JOB_ID': "test_job",
                                        'CIVIS_RUN_ID': "test_run"}):
        civis.parallel.infer_backend_factory(
            client=mock_client, arguments={'foo': 'bar'})

    assert mock_make_factory.call_args[1]['arguments'] == \
        {'spam': 'eggs', 'foo': 'bar'}


@mock.patch.object(civis.parallel, 'make_backend_factory')
def test_infer_from_custom_job(mock_make_factory):
    # Test that `infer_backend_factory` can find needed
    # parameters if it's run inside a custom job created
    # from a template.
    mock_client = mock.MagicMock()
    mock_custom = Response(dict(from_template_id=999, id=42,
                                required_resources=None,
                                params=[{'name': 'spam'}],
                                arguments={'spam': 'eggs'},
                                docker_image_name='image_name',
                                docker_image_tag='tag',
                                repo_http_uri='cabbage', repo_ref='servant'))
    mock_script = mock_job()
    mock_template = Response(dict(id=999, script_id=171))

    def _get_container(job_id):
        if int(job_id) == 42:
            return mock_custom
        elif int(job_id) == 171:
            return mock_script
        else:
            raise ValueError("Got job_id {}".format(job_id))

    mock_client.scripts.get_containers.side_effect = _get_container
    mock_client.templates.get_scripts.return_value = mock_template
    with mock.patch.dict('os.environ', {'CIVIS_JOB_ID': "42",
                                        'CIVIS_RUN_ID': "test_run"}):
        civis.parallel.infer_backend_factory(
            client=mock_client)

    # We should have called `get_containers` twice now -- once for
    # the container we're running in, and a second time for the
    # container which backs the template this job was created from.
    # The backing script has settings which aren't visible from
    # the container which was created from it.
    assert mock_client.scripts.get_containers.call_count == 2
    mock_client.templates.get_scripts.assert_called_once_with(999)
    expected_kwargs = {'required_resources': {'cpu': 11},
                       'params': [{'name': 'spam'}],
                       'arguments': {'spam': 'eggs'},
                       'client': mock.ANY,
                       'polling_interval': mock.ANY,
                       'setup_cmd': None,
                       'max_submit_retries': mock.ANY,
                       'max_job_retries': mock.ANY,
                       'hidden': True,
                       'remote_backend': 'sequential'}
    for key in civis.parallel.KEYS_TO_INFER:
        expected_kwargs[key] = mock_script[key]
    mock_make_factory.assert_called_once_with(**expected_kwargs)


def make_to_file_mock(result, max_n_err=0, exc=None):
    cnt = {'err': 0}

    def mock_civis_to_file(file_id, buf, client=None):
        if cnt['err'] < max_n_err:
            cnt['err'] += 1
            raise exc
        else:
            buf.write(pickle.dumps(result))
    return mock_civis_to_file


@mock.patch.object(civis.parallel, 'civis')
def test_result_success(mock_civis):
    # Test that we can get a result back from a succeeded job.
    callback = mock.MagicMock()
    mock_civis.io.civis_to_file.side_effect = make_to_file_mock('spam')
    fut = ContainerFuture(1, 2, client=mock.MagicMock())
    fut.set_result(Response({'state': 'success'}))
    res = civis.parallel._CivisBackendResult(fut, callback)

    assert res.get() == 'spam'
    assert callback.call_count == 1


@mock.patch.object(civis.parallel, 'civis')
def test_result_callback_no_get(mock_civis):
    # Test that the completed callback happens even if we don't call `get`
    callback = mock.MagicMock()
    mock_civis.io.civis_to_file.side_effect = make_to_file_mock('spam')
    fut = ContainerFuture(1, 2, client=mock.MagicMock())
    fut.set_result(Response({'state': 'success'}))

    civis.parallel._CivisBackendResult(fut, callback)
    assert callback.call_count == 1


@mock.patch.object(civis.parallel, 'civis')
def test_result_exception(mock_civis):
    # An error in the job should be raised by the result
    callback = mock.MagicMock()
    exc = ZeroDivisionError()
    mock_civis.io.civis_to_file.side_effect = make_to_file_mock(exc)
    fut = ContainerFuture(1, 2, client=mock.MagicMock())
    fut._set_api_exception(Response({'state': 'failed'}))
    res = civis.parallel._CivisBackendResult(fut, callback)

    with pytest.raises(ZeroDivisionError):
        res.get()
    assert callback.call_count == 0


def test_result_exception_no_result():
    # If the job errored but didn't write an output, we should get
    # a generic TransportableException back.
    callback = mock.MagicMock()

    # Passing the client mock as an argument instead of globally
    # patching the client tests that the _CivisBackendResult
    # uses the client object on the input CivisFuture.
    mock_client = mock.MagicMock().APIClient()
    mock_client.scripts.list_containers_runs_outputs.return_value = []
    fut = ContainerFuture(1, 2, client=mock_client)
    fut._set_api_exception(Response({'state': 'failed'}))
    res = civis.parallel._CivisBackendResult(fut, callback)

    with pytest.raises(TransportableException) as exc:
        res.get()
    assert "{'state': 'failed'}" in str(exc.value)
    assert callback.call_count == 0


@mock.patch.object(civis.parallel, 'civis')
def test_result_callback_exception(mock_civis):
    # An error in the result retrieval should be raised by .get
    callback = mock.MagicMock()
    exc = ZeroDivisionError()
    mock_civis.io.civis_to_file.side_effect = exc
    fut = ContainerFuture(1, 2, client=mock.MagicMock())

    # We're simulating a job which succeeded but generated an
    # exception when we try to download the outputs.
    fut._set_api_exception(Response({'state': 'succeeded'}))
    res = civis.parallel._CivisBackendResult(fut, callback)

    with pytest.raises(ZeroDivisionError):
        res.get()
    assert callback.call_count == 0


@mock.patch.object(civis.parallel, 'civis')
def test_result_eventual_success(mock_civis):
    # Test that we can get a result back from a succeeded job,
    # even if we need to retry a few times to succeed with the download.
    callback = mock.MagicMock()
    exc = requests.ConnectionError()
    se = make_to_file_mock('spam', max_n_err=2, exc=exc)
    mock_civis.io.civis_to_file.side_effect = se
    fut = ContainerFuture(1, 2, client=mock.MagicMock())
    fut.set_result(Response({'state': 'success'}))
    res = civis.parallel._CivisBackendResult(fut, callback)

    assert res.get() == 'spam'
    assert callback.call_count == 1


@mock.patch.object(civis.parallel, 'civis')
def test_result_eventual_failure(mock_civis):
    # We will retry a connection error up to 5 times. Make sure
    # that we will get an error if it persists forever.
    callback = mock.MagicMock()
    exc = requests.ConnectionError()
    se = make_to_file_mock('spam', max_n_err=10, exc=exc)
    mock_civis.io.civis_to_file.side_effect = se
    fut = ContainerFuture(1, 2, client=mock.MagicMock())
    fut.set_result(Response({'state': 'success'}))
    res = civis.parallel._CivisBackendResult(fut, callback)

    with pytest.raises(requests.ConnectionError):
        res.get()
    assert callback.call_count == 0


@mock.patch.object(civis.parallel, 'civis')
@mock.patch.object(civis.parallel, '_sklearn_reg_para_backend')
@mock.patch.object(civis.parallel, '_joblib_reg_para_backend')
def test_setup_remote_backend(mock_jl, mock_sk, mock_civis):
    # Test that the civis backend is properly registered w/ joblib and sklearn.
    for no_sk in [True, False]:
        with mock.patch.object(civis.parallel, 'NO_SKLEARN', no_sk):
            backend = civis.parallel._CivisBackend()
            backend_name = civis.parallel._setup_remote_backend(backend)
            assert backend_name == 'civis'
            assert mock_jl.call_count == 1
            if no_sk:
                assert mock_sk.call_count == 0
            else:
                assert mock_sk.call_count == 1
            mock_jl.reset_mock()
            mock_sk.reset_mock()


def test_civis_backend_from_existing():
    # Test to make sure that making a new backend from an existing one makes
    # an exact copy.
    backend = civis.parallel._CivisBackend(
        setup_cmd='blah',
        from_template_id=-1,
        max_submit_retries=10,
        client='ha',
        remote_backend='cool',
        hidden=False)

    new_backend = civis.parallel._CivisBackend.from_existing(backend)

    assert new_backend.setup_cmd == 'blah'
    assert new_backend.from_template_id == -1
    assert new_backend.max_submit_retries == 10
    assert new_backend.client == 'ha'
    assert new_backend.remote_backend == 'cool'
    assert new_backend.executor_kwargs == {'hidden': False}


@mock.patch.object(civis.parallel, 'civis')
def test_civis_backend_pickles(mock_civis):
    # Test to make sure the backend will pickle.
    backend = civis.parallel._CivisBackend(
        setup_cmd='blah',
        from_template_id=-1,
        max_submit_retries=10,
        client='ha',
        remote_backend='cool',
        hidden=False)

    with parallel_backend(backend):
        Parallel(n_jobs=-1)([])

    buff = io.BytesIO()
    pickle.dump(backend, buff)
    buff.seek(0)
    new_backend = pickle.load(buff)

    with parallel_backend(new_backend):
        Parallel(n_jobs=-1)([])
