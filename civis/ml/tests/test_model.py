from builtins import super
from collections import namedtuple
from concurrent.futures import CancelledError
from six import BytesIO
import json
import os
import pickle
import tempfile

import joblib
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
try:
    import feather  # NOQA
    HAS_FEATHER = True
except ImportError:
    HAS_FEATHER = False
try:
    from sklearn.linear_model import LogisticRegression
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False
try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

from civis import APIClient
from civis._utils import camel_to_snake
from civis.base import CivisAPIError, CivisJobFailure
from civis.compat import mock, FileNotFoundError
from civis.response import Response
import pytest

from civis.ml import _model


def setup_client_mock(script_id=-10, run_id=100, state='succeeded',
                      run_outputs=None):
    """Return a Mock set up for use in testing container scripts

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

    Returns
    -------
    `unittest.mock.Mock`
        With scripts endpoints `post_containers`, `post_containers_runs`,
        `post_cancel`, and `get_containers_runs` set up.
    """
    c = mock.Mock()
    c.__class__ = APIClient

    mock_container = Response({'id': script_id})
    c.scripts.post_containers.return_value = mock_container
    mock_container_run_start = Response({'id': run_id,
                                         'container_id': script_id,
                                         'state': 'queued'})
    mock_container_run = Response({'id': run_id,
                                   'container_id': script_id,
                                   'state': state})
    c.scripts.post_containers_runs.return_value = mock_container_run_start
    c.scripts.get_containers_runs.return_value = mock_container_run
    c.scripts.list_containers_runs_outputs.return_value = (run_outputs or [])
    c.scripts.list_containers_runs_logs.return_value = []

    def change_state_to_cancelled(script_id):
        mock_container_run.state = "cancelled"
        return mock_container_run

    c.scripts.post_cancel.side_effect = change_state_to_cancelled

    # Avoid channels endpoint while testing here
    del c.channels

    return c


def test_check_is_fit_exception():
    mock_pipe = mock.MagicMock()
    mock_pipe.train_result_ = None

    @_model._check_fit_initiated
    def foo(arg):
        return 7

    with pytest.raises(ValueError):
        foo(mock_pipe)


def test_check_is_fit():
    mock_pipe = mock.MagicMock()
    mock_pipe.train_result_ = True

    @_model._check_fit_initiated
    def foo(arg):
        return 7

    assert foo(mock_pipe) == 7


@mock.patch.object(_model, "futures")
def test_block_and_handle_missing_exception(mock_fut):
    @_model._block_and_handle_missing
    def foo(arg):
        raise FileNotFoundError

    mock_super = mock.Mock()
    mock_super.exception.return_value = Exception

    with pytest.raises(Exception):
        foo(mock_super)

    mock_super.exception.return_value = None
    with pytest.raises(FileNotFoundError):
        foo(mock_super)


@mock.patch.object(_model, "futures")
def test_block_and_handle_missing(mock_fut):
    @_model._block_and_handle_missing
    def foo(arg):
        return 7
    assert foo('bar') == 7


@mock.patch.object(_model.cio, 'file_to_civis', return_value=-11)
def test_stash_local_data_from_file(mock_file):
    with tempfile.NamedTemporaryFile() as tempfname:
        fname = tempfname.name
        with open(fname, 'wt') as _fout:
            _fout.write("a,b,c\n1,2,3\n")

        assert _model._stash_local_file(fname) == -11
    mock_file.assert_called_once_with(mock.ANY,
                                      name='modelpipeline_data.csv',
                                      client=mock.ANY)


@pytest.mark.skipif(not HAS_PANDAS, reason="pandas not installed")
@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_stash_local_dataframe_multiindex_err():
    arrays = [np.array(['bar', 'bar', 'baz', 'baz',
                        'foo', 'foo', 'qux', 'qux']),
              np.array(['one', 'two', 'one', 'two',
                        'one', 'two', 'one', 'two'])]
    df = pd.DataFrame(np.random.randn(8, 4), index=arrays)
    with pytest.raises(TypeError):
        _model._stash_local_dataframe(df, 10000)


@pytest.mark.skipif(not HAS_PANDAS, reason="pandas not installed")
@mock.patch.object(_model.cio, 'file_to_civis', return_value=-11)
def test_stash_local_data_from_dataframe_csv(mock_file):
    df = pd.DataFrame({'a': [1], 'b': [2]})
    assert _model._stash_dataframe_as_csv(df, mock.Mock()) == -11
    mock_file.assert_called_once_with(mock.ANY, name='modelpipeline_data.csv',
                                      client=mock.ANY)
    assert isinstance(mock_file.call_args[0][0], BytesIO)


@pytest.mark.skipif(not HAS_PANDAS, reason="pandas not installed")
@pytest.mark.skipif(not HAS_FEATHER, reason="feather not installed")
@mock.patch.object(_model.cio, 'file_to_civis', return_value=-11)
def test_stash_local_data_from_dataframe_feather(mock_file):
    df = pd.DataFrame({'a': [1], 'b': [2]})
    assert _model._stash_local_dataframe(df, 10000) == -11
    mock_file.assert_called_once_with(
        mock.ANY, name='modelpipeline_data.feather', client=mock.ANY)


@mock.patch.object(_model, '_stash_dataframe_as_feather', autospec=True)
@mock.patch.object(_model, '_stash_dataframe_as_csv', autospec=True)
def test_stash_local_dataframe_format_select_csv(mock_csv, mock_feather):
    # Always store data as a CSV for CivisML versions <= 2.0.
    _model._stash_local_dataframe('df', 9969)
    assert mock_feather.call_count == 0
    assert mock_csv.call_count == 1


@mock.patch.object(_model, '_stash_dataframe_as_feather', autospec=True)
@mock.patch.object(_model, '_stash_dataframe_as_csv', autospec=True)
def test_stash_local_dataframe_format_select_feather(mock_csv, mock_feather):
    # Try to store data as Feather for CivisML versions > 2.0.
    _model._stash_local_dataframe('df', 10050)
    assert mock_feather.call_count == 1
    assert mock_csv.call_count == 0


@mock.patch.object(_model, '_retrieve_file', autospec=True)
def test_load_estimator(mock_retrieve):
    obj = {'spam': 'eggs'}

    def _retrieve_json(fname, job_id, run_id, local_dir, client=None):
        full_name = os.path.join(local_dir, fname)
        joblib.dump(obj, full_name)
        return full_name

    mock_retrieve.side_effect = _retrieve_json
    out = _model._load_estimator(13, 17, 'fname')
    assert out == obj


@mock.patch.object(_model.cio, 'file_to_dataframe', autospec=True)
@mock.patch.object(_model.cio, 'file_id_from_run_output', autospec=True)
def test_load_table_from_outputs(mock_fid, mock_f2df):
    # Test that _load_table_from_outputs is calling functions
    # correctly. Let `autospec` catch errors in arguments being passed.
    mock_client = setup_client_mock()
    _model._load_table_from_outputs(1, 2, 'fname', client=mock_client)


def test_show_civisml_warnings():
    warn_list = ["/path:13: UserWarning: A message\n",
                 "/module:42: RuntimeWarning: Profundity\n"]
    with pytest.warns(UserWarning) as warns:
        _model._show_civisml_warnings(warn_list)
    assert len(warns.list) == 2
    assert str(warns.list[0].message) == "A message"
    assert str(warns.list[1].message) == "Profundity"


def test_show_civisml_warnings_error():
    # If the warnings-parser fails, we should still get a sensible warning.
    warn_list = ["/path UserWarning: A message\n"]  # Malformed warning message
    with pytest.warns(RuntimeWarning) as warns:
        _model._show_civisml_warnings(warn_list)
    assert len(warns.list) == 1
    assert warn_list[0] in str(warns.list[0].message)
    assert "Remote warning from CivisML" in str(warns.list[0].message)


###################################
# Tests of ModelFuture below here #
###################################
@mock.patch.object(_model.ModelFuture, "_set_model_exception", autospec=True)
@mock.patch.object(_model.ModelFuture, "add_done_callback", autospec=True)
def test_modelfuture_constructor(mock_adc, mock_spe):
    c = setup_client_mock(7, 17)

    mf = _model.ModelFuture(job_id=7, run_id=17, client=c)
    assert mf.is_training is True
    assert mf.train_run_id == 17
    assert mf.train_job_id == 7

    mf = _model.ModelFuture(job_id=7, run_id=17,
                            train_job_id=23, train_run_id=29, client=c)
    assert mf.is_training is False
    assert mf.train_run_id == 29
    assert mf.train_job_id == 23


@mock.patch.object(_model.cio, "file_id_from_run_output",
                   mock.Mock(return_value=11, spec_set=True))
@mock.patch.object(_model.cio, "file_to_json",
                   mock.Mock(return_value={'run': {'status': 'succeeded'}}))
@mock.patch.object(_model, 'APIClient', return_value=setup_client_mock())
def test_modelfuture_pickle_smoke(mock_client):
    mf = _model.ModelFuture(job_id=7, run_id=13, client=setup_client_mock())
    mf.result()
    mf_pickle = pickle.dumps(mf)
    pickle.loads(mf_pickle)


def test_set_model_exception_metadata_exception():
    """Tests cases where accessing metadata throws exceptions
    """
    # State "running" prevents termination when the object is created.
    mock_client = setup_client_mock(1, 2, state='running')

    class ModelFutureRaiseExc(_model.ModelFuture):
        def __init__(self, exc, *args, **kwargs):
            self.__exc = exc
            super().__init__(*args, **kwargs)

        @property
        def metadata(self):
            raise self.__exc('What a spectacular failure, you say!')

    # exception types get caught!
    for exc in [FileNotFoundError, CivisJobFailure, CancelledError]:
        fut = ModelFutureRaiseExc(exc, 1, 2, client=mock_client)
        _model.ModelFuture._set_model_exception(fut)

    with pytest.warns(UserWarning):
        # The KeyError is caught, but sends a warning
        fut = ModelFutureRaiseExc(KeyError, 1, 2, client=mock_client)
        _model.ModelFuture._set_model_exception(fut)

    fut = ModelFutureRaiseExc(RuntimeError, 1, 2, client=mock_client)
    with pytest.raises(RuntimeError):
        _model.ModelFuture._set_model_exception(fut)


def test_set_model_exception_memory_error():
    mock_client = setup_client_mock(1, 2, state='failed')
    err_msg = ('Process ran out of its allowed 3000 MiB of '
               'memory and was killed.')
    logs = [{'created_at': '2017-05-10T12:00:00.000Z',
             'id': 10005,
             'level': 'error',
             'message': 'Failed'},
            {'created_at': '2017-05-10T12:00:00.000Z',
             'id': 10003,
             'level': 'error',
             'message': 'Error on job: Process ended with an '
                        'error, exiting: 137.'},
            {'created_at': '2017-05-10T12:00:00.000Z',
             'id': 10000,
             'level': 'error',
             'message': err_msg}]
    mock_client.scripts.list_containers_runs_logs.return_value = logs
    fut = _model.ModelFuture(1, 2, client=mock_client)
    with pytest.raises(MemoryError) as err:
        fut.result()
    assert str(err.value) == err_msg


def test_set_model_exception_unknown_error():
    # If we really don't recognize the error, at least give the
    # user a few lines of logs so they can maybe figure it out themselves.
    mock_client = setup_client_mock(1, 2, state='failed')
    logs = [{'created_at': '2017-05-10T12:00:00.000Z',
             'id': 10005,
             'level': 'error',
             'message': 'Failed'},
            {'created_at': '2017-05-10T12:00:00.000Z',
             'id': 10003,
             'level': 'error',
             'message': 'Error on job: Process ended with an '
                        'error, exiting: 137.'},
            {'created_at': '2017-05-10T12:00:00.000Z',
             'id': 10000,
             'level': 'error',
             'message': 'Oops'}]
    err_msg = '\n'.join([l['message'] for l in logs])
    mock_client.scripts.list_containers_runs_logs.return_value = logs
    fut = _model.ModelFuture(1, 2, client=mock_client)
    with pytest.raises(CivisJobFailure) as err:
        fut.result()
    assert str(err.value).startswith(err_msg)


@mock.patch.object(_model.cio, "file_to_json", autospec=True,
                   return_value={'run': {'status': 'succeeded',
                                         'stack_trace': None}})
def test_set_model_exception_no_exception(mock_f2j):
    # If nothing went wrong, we shouldn't set an exception
    ro = [{'name': 'model_info.json', 'object_id': 137, 'object_type': 'File'},
          {'name': 'metrics.json', 'object_id': 139, 'object_type': 'File'}]
    ro = [Response(o) for o in ro]
    mock_client = setup_client_mock(1, 2, state='succeeded', run_outputs=ro)
    fut = _model.ModelFuture(1, 2, client=mock_client)
    assert fut.exception() is None


class ModelFutureStub:

    def __init__(self, exc, trn, val):
        self._exception = exc
        self.metadata = trn
        self.validation_metadata = val
        self.is_training = True
        self._exception_handled = False

    def set_exception(self, exc):
        self._exception = exc


def _test_set_model_exc(trn, val, exc):
    fut = ModelFutureStub(exc, trn, val)

    assert isinstance(fut._exception, type(exc))
    _model.ModelFuture._set_model_exception(fut)
    assert isinstance(fut._exception, _model.ModelError)
    assert 'this is a trace' in str(fut._exception)

    # don't change attribute if it's already a ModelError
    fut._exception = _model.ModelError('undecipherable message')
    _model.ModelFuture._set_model_exception(fut)
    assert isinstance(fut._exception, _model.ModelError)
    assert 'undecipherable message' in str(fut._exception)


def test_set_model_exception_training_metadata():
    trn = {'run': {'status': 'exception', 'stack_trace': 'this is a trace'}}
    val = None
    exc = FileNotFoundError('Look, no mocks!')

    _test_set_model_exc(trn, val, exc)


def test_set_model_exception_validation_metadata():
    trn = {'run': {'status': 'succeeded'}}
    val = {'run': {'status': 'exception', 'stack_trace': 'this is a trace'}}
    exc = FileNotFoundError('Hahaha, zero mocks here!')

    _test_set_model_exc(trn, val, exc)


@mock.patch.object(_model.cio, "file_to_json",
                   mock.Mock(return_value='bar', spec_set=True))
@mock.patch.object(_model.ModelFuture, "_set_model_exception",
                   lambda *args: None)
def test_getstate():
    c = setup_client_mock(3, 7)

    mf = _model.ModelFuture(3, 7, client=c)
    ret = mf.__getstate__()
    assert ret['_done_callbacks'] == []
    assert not ret['_self_polling_executor']
    assert 'client' not in ret
    assert 'poller' not in ret
    assert '_condition' not in ret


@mock.patch.object(_model.cio, "file_id_from_run_output",
                   mock.Mock(return_value=11, spec_set=True))
@mock.patch.object(_model.cio, "file_to_json",
                   mock.Mock(spec_set=True,
                             return_value={'run': {'status': 'foo'}}))
def test_state():
    c = setup_client_mock(3, 7)

    mf = _model.ModelFuture(3, 7, client=c)
    ret = mf.state
    assert ret == 'foo'

    c.scripts.get_containers_runs.return_value = Response({'id': 7,
                                                           'container_id': 3,
                                                           'state': 'failed'})
    mf = _model.ModelFuture(3, 7, client=c)
    assert mf.state == 'failed'


@mock.patch.object(_model, "_load_table_from_outputs", return_value='bar')
@mock.patch.object(_model.ModelFuture, "result")
@mock.patch.object(_model.ModelFuture, "_set_model_exception", mock.Mock())
def test_table(mock_res, mock_lt):
    c = setup_client_mock(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)
    assert mf.table == 'bar'
    mock_lt.assert_called_once_with(3, 7, 'predictions.csv',
                                    index_col=0, client=c)


@mock.patch.object(_model, "_load_table_from_outputs")
@mock.patch.object(_model.ModelFuture, "result")
@mock.patch.object(_model.ModelFuture, "_set_model_exception", mock.Mock())
def test_table_None(mock_res, mock_lt):
    mock_lt.side_effect = FileNotFoundError()
    c = setup_client_mock(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)
    assert mf.table is None
    mock_lt.assert_called_once_with(3, 7, 'predictions.csv',
                                    index_col=0, client=c)


@mock.patch.object(_model.cio, "file_id_from_run_output",
                   mock.Mock(return_value=11, spec_set=True))
@mock.patch.object(_model.cio, "file_to_json", return_value={'foo': 'bar'})
@mock.patch.object(_model.ModelFuture, "_set_model_exception")
def test_metadata(mock_spec, mock_f2j):
    c = setup_client_mock(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)
    assert mf.metadata == {'foo': 'bar'}
    mock_f2j.assert_called_once_with(11, client=c)


def test_train_data_fname():
    # swap out the poller with a simple function that accepts *args, **kwargs
    # and returns a simple successful Response object
    def poller(*args, **kwargs):
        return Response({'state': 'succeeded'})
    mock_client = mock.MagicMock()
    mock_client.scripts.get_containers_runs = poller
    mf = _model.ModelFuture(job_id=1, run_id=2, client=mock_client)

    path = '/green/eggs/and/ham'
    training_meta = {'run': {'configuration': {'data': {'location': path}}}}
    mf._train_metadata = training_meta
    assert mf.train_data_fname == 'ham'


@mock.patch.object(_model, 'cio', autospec=True)
def test_train_data_(mock_cio):
    def poller(*args, **kwargs):
        return Response({'state': 'succeeded'})
    mock_client = mock.MagicMock()
    mock_client.scripts.get_containers_runs = poller

    path = '/green/eggs/and/ham'
    training_meta = {'run': {'configuration': {'data': {'location': path}},
                             'status': 'succeeded'}}
    mock_cio.file_to_json.return_value = training_meta

    mf = _model.ModelFuture(job_id=1, run_id=2, train_job_id=11,
                            train_run_id=13, client=mock_client)
    assert mf.training_metadata == training_meta
    mock_cio.file_id_from_run_output.assert_called_with(
        'model_info.json', 11, 13, client=mock_client)


@mock.patch.object(_model, "_load_table_from_outputs", autospec=True)
def test_train_data_exc_handling(mock_load_table):
    def poller(*args, **kwargs):
        return Response({'state': 'succeeded'})
    mock_client = mock.MagicMock()
    mock_client.scripts.get_containers_runs = poller
    mf = _model.ModelFuture(job_id=1, run_id=2, client=mock_client)
    mf._train_data_fname = 'placeholder.csv'

    # check we catch 404 error and raise some intelligible
    r = Response({'content': None, 'status_code': 404, 'reason': None})
    mock_load_table.side_effect = [CivisAPIError(r)]
    with pytest.raises(ValueError):
        mf.train_data


@mock.patch.object(_model, "_load_estimator", return_value='spam')
@mock.patch.object(_model.ModelFuture, "_set_model_exception", mock.Mock())
def test_estimator(mock_le):
    c = setup_client_mock(3, 7)

    mf = _model.ModelFuture(3, 7, client=c)
    assert mock_le.call_count == 0, "Estimator retrieval is lazy."
    assert mf.estimator == 'spam'
    assert mock_le.call_count == 1

    assert mf.estimator == 'spam'
    assert mock_le.call_count == 1,\
        "The Estimator is only downloaded once and cached."


@mock.patch.object(_model.cio, "file_id_from_run_output", autospec=True)
@mock.patch.object(_model.cio, "file_to_json", return_value='foo',
                   autospec=True)
@mock.patch.object(_model.ModelFuture, "_set_model_exception")
def test_validation_metadata_training(mock_spe, mock_f2f,
                                      mock_file_id_from_run_output):
    mock_file_id_from_run_output.return_value = 11
    c = setup_client_mock(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)

    assert mf.validation_metadata == 'foo'
    mock_f2f.assert_called_once_with(11, client=c)
    mock_file_id_from_run_output.assert_called_with('metrics.json', 3, 7,
                                                    client=mock.ANY)


@mock.patch.object(_model.cio, "file_id_from_run_output", autospec=True)
@mock.patch.object(_model.cio, "file_to_json", autospec=True)
@mock.patch.object(_model.ModelFuture, "_set_model_exception")
def test_validation_metadata_missing(mock_spe, mock_f2f,
                                     mock_file_id_from_run_output):
    # Make sure that missing validation metadata doesn't cause an error
    mock_file_id_from_run_output.side_effect = FileNotFoundError
    c = setup_client_mock(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)

    assert mf.validation_metadata is None
    assert mf.metrics is None
    assert mock_f2f.call_count == 0
    assert mock_file_id_from_run_output.call_count == 1


@mock.patch.object(_model.cio, "file_id_from_run_output", autospec=True)
@mock.patch.object(_model.cio, "file_to_json", return_value='foo',
                   autospec=True)
@mock.patch.object(_model.ModelFuture, "_set_model_exception")
def test_validation_metadata_prediction(mock_spe, mock_f2f,
                                        mock_file_id_from_run_output):
    mock_file_id_from_run_output.return_value = 11
    c = setup_client_mock(3, 7)
    mf = _model.ModelFuture(1, 2, 3, 7, client=c)

    assert mf.validation_metadata == 'foo'
    mock_f2f.assert_called_once_with(11, client=c)
    mock_file_id_from_run_output.assert_called_with('metrics.json', 3, 7,
                                                    client=mock.ANY)


@mock.patch.object(_model.cio, "file_id_from_run_output", autospec=True)
@mock.patch.object(_model.cio, "file_to_json",
                   mock.MagicMock(
                       return_value={'metrics': 'foo',
                                     'run': {'status': 'succeeded'}}))
def test_metrics_training(mock_file_id_from_run_output):
    mock_file_id_from_run_output.return_value = 11
    c = setup_client_mock(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)

    assert mf.metrics == 'foo'
    mock_file_id_from_run_output.assert_called_with('metrics.json', 3, 7,
                                                    client=mock.ANY)


@mock.patch.object(_model.cio, "file_id_from_run_output", autospec=True)
@mock.patch.object(_model.cio, "file_to_json")
def test_metrics_training_None(mock_file_to_json,
                               mock_file_id_from_run_output):
    mock_file_to_json.return_value = mock.MagicMock(
        return_value={'metrics': 'foo',
                      'run': {'status': 'succeeded'}})
    mock_file_id_from_run_output.return_value = 11
    c = setup_client_mock(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)
    # override validation metadata to be None, as though we ran
    # a train job without validation
    mf._val_metadata = None

    mock_file_to_json.return_value = None
    assert mf.metrics is None
    mock_file_id_from_run_output.assert_called_with('metrics.json', 3, 7,
                                                    client=mock.ANY)


@mock.patch.object(_model.cio, "file_id_from_run_output", autospec=True)
@mock.patch.object(_model.cio, "file_to_json",
                   mock.MagicMock(
                       return_value={'metrics': 'foo',
                                     'run': {'status': 'succeeded'}}))
def test_metrics_prediction(mock_file_id_from_run_output):
    mock_file_id_from_run_output.return_value = 11
    c = setup_client_mock(3, 7)
    mf = _model.ModelFuture(1, 2, 3, 7, client=c)

    assert mf.metrics == 'foo'
    mock_file_id_from_run_output.assert_called_with('metrics.json', 3, 7,
                                                    client=mock.ANY)


#####################################
# Tests of ModelPipeline below here #
#####################################
@pytest.fixture
def mp_setup():
    mock_api = setup_client_mock()
    mp = _model.ModelPipeline('wf', 'dv', client=mock_api)
    return mp


@pytest.mark.skipif(not HAS_SKLEARN, reason="scikit-learn not installed")
def test_modelpipeline_etl_init_err():
    # If users don't have access to >= v2.0 temlates, then passing
    # `etl` to a new ModelPipeline should produce a NotImplementedError.
    mock_client = mock.MagicMock()
    r = Response({'content': None, 'status_code': 9999, 'reason': None})

    def pre_2p0_template(id=None, **kwargs):
        if id > 9113:
            raise CivisAPIError(r)
        return {}
    mock_client.templates.get_scripts.side_effect = pre_2p0_template
    with pytest.raises(NotImplementedError):
        _model.ModelPipeline(LogisticRegression(), 'test',
                             etl=LogisticRegression(),
                             client=mock_client)
    # clean up
    _model._CIVISML_TEMPLATE = None


@pytest.mark.skipif(not HAS_SKLEARN, reason="scikit-learn not installed")
def test_modelpipeline_init_newest():
    _model._CIVISML_TEMPLATE = None
    mock_client = mock.MagicMock()
    mock_client.templates.get_scripts.return_value = {}
    etl = LogisticRegression()
    mp = _model.ModelPipeline(LogisticRegression(), 'test', etl=etl,
                              client=mock_client)
    assert mp.etl == etl
    assert mp.train_template_id == max(_model._PRED_TEMPLATES)
    # clean up
    _model._CIVISML_TEMPLATE = None


@mock.patch.object(_model, 'ModelFuture')
def test_modelpipeline_classmethod_constructor_errors(mock_future):
    # catch 404 error if model isn't found and throw ValueError
    mock_client = mock.Mock()
    response = namedtuple('Reponse', ['content', 'response', 'reason',
                                      'status_code'])(False, None, None, 404)
    mock_future.side_effect = CivisAPIError(response)
    with pytest.raises(ValueError):
        _model.ModelPipeline.from_existing(1, 1, client=mock_client)


@pytest.fixture()
def container_response_stub(from_template_id=8387):
    arguments = {
        'MODEL': 'sparse_logistic',
        'TARGET_COLUMN': 'brushes_teeth_much',
        'PRIMARY_KEY': 'voterbase_id',
        'CALIBRATION': 'sigmoid',
        'EXCLUDE_COLS': 'dog cat lizard frog',
        'CVPARAMS': '{}',
        'PARAMS': '{}',
        'REQUIRED_CPU': 1000,
        'REQUIRED_MEMORY': 9999,
        'REQUIRED_DISK_SPACE': -20,
        'DEPENDENCIES': 'A B C D',
        'GIT_CRED': 9876
    }
    notifications = {
        'urls': [],
        'failureEmailAddresses': [],
        'failureOn': True,
        'stallWarningMinutes': None,
        'successEmailAddresses': [],
        'successEmailBody': None,
        'successEmailSubject': None,
        'successOn': True
    }
    return Response(dict(arguments=arguments,
                         notifications=notifications,
                         required_resources={},
                         docker_image_tag=None,
                         docker_command=None,
                         repo_http_uri=None,
                         repo_ref=None,
                         name='Civis Model Train',
                         from_template_id=from_template_id,
                         ))


@mock.patch.object(_model, 'ModelFuture')
def test_modelpipeline_classmethod_constructor(mock_future,
                                               container_response_stub):
    mock_client = mock.Mock()
    mock_client.scripts.get_containers.return_value = \
        container = container_response_stub
    mock_client.credentials.get.return_value = Response({'name': 'Token'})

    resources = {'REQUIRED_CPU': 1000,
                 'REQUIRED_MEMORY': 9999,
                 'REQUIRED_DISK_SPACE': -20}

    # test everything is working fine
    mp = _model.ModelPipeline.from_existing(1, 1, client=mock_client)
    assert isinstance(mp, _model.ModelPipeline)
    assert mp.dependent_variable == [container.arguments['TARGET_COLUMN']]
    assert mp.primary_key == container.arguments['PRIMARY_KEY']
    excluded = container.arguments.get('EXCLUDE_COLS', None)
    assert mp.excluded_columns == excluded.split() if excluded else None
    assert mp.model == container.arguments['MODEL']
    assert mp.calibration == container.arguments['CALIBRATION']
    assert mp.cv_params == json.loads(container.arguments['CVPARAMS'])
    assert mp.parameters == json.loads(container.arguments['PARAMS'])
    assert mp.job_resources == resources
    assert mp.model_name == container.name[:-6]
    assert mp.notifications == {camel_to_snake(key): val for key, val
                                in container.notifications.items()}
    deps = container.arguments.get('DEPENDENCIES', None)
    assert mp.dependencies == deps.split() if deps else None
    assert mp.git_token_name == 'Token'


@mock.patch.object(_model, 'ModelFuture')
def test_modelpipeline_classmethod_constructor_defaults(
        mock_future, container_response_stub):
    del container_response_stub.arguments['PARAMS']
    del container_response_stub.arguments['CVPARAMS']
    mock_client = mock.Mock()
    mock_client.scripts.get_containers.return_value = container_response_stub
    mock_client.credentials.get.return_value = Response({'name': 'Token'})

    # test everything is working fine
    mp = _model.ModelPipeline.from_existing(1, 1, client=mock_client)
    assert mp.cv_params == {}
    assert mp.parameters == {}


@mock.patch.object(_model, 'ModelFuture', mock.Mock())
def test_modelpipeline_classmethod_constructor_future_train_version():
    # Test handling attempts to restore a model created with a newer
    # version of CivisML.
    current_max_template = max(_model._PRED_TEMPLATES)
    cont = container_response_stub(current_max_template + 1000)
    mock_client = mock.Mock()
    mock_client.scripts.get_containers.return_value = cont
    mock_client.credentials.get.return_value = Response({'name': 'Token'})

    # test everything is working fine
    with pytest.warns(RuntimeWarning):
        mp = _model.ModelPipeline.from_existing(1, 1, client=mock_client)
    exp_p_id = _model._PRED_TEMPLATES[current_max_template]
    assert mp.predict_template_id == exp_p_id


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_modelpipeline_classmethod_constructor_nonint_id():
    # Verify that we can still JSON-serialize job and run IDs even
    # if they're entered in a non-JSON-able format.
    # We need to turn them into JSON to set them as script arguments.
    mock_client = setup_client_mock(1, 2)
    mock_client.scripts.get_containers.return_value = container_response_stub()

    mp = _model.ModelPipeline.from_existing(np.int64(1), np.int64(2),
                                            client=mock_client)

    out = json.dumps({'job': mp.train_result_.job_id,
                      'run': mp.train_result_.run_id})
    assert out == '{"job": 1, "run": 2}' or out == '{"run": 2, "job": 1}'


@mock.patch.object(_model, 'ModelFuture', autospec=True)
def test_modelpipeline_classmethod_constructor_old_version(mock_future):
    # Test that we select the correct prediction template for different
    # versions of a training job.
    mock_client = setup_client_mock()
    mock_client.scripts.get_containers.return_value = \
        container_response_stub(from_template_id=8387)
    mp = _model.ModelPipeline.from_existing(1, 1, client=mock_client)
    assert mp.predict_template_id == 9113, "Predict template v1.1"

    # v0.5 training
    mock_client.scripts.get_containers.return_value = \
        container_response_stub(from_template_id=7020)
    mp = _model.ModelPipeline.from_existing(1, 1, client=mock_client)
    assert mp.predict_template_id == 7021, "Predict template v0.5"


@mock.patch.object(_model.ModelPipeline, "_create_custom_run")
def test_modelpipeline_train(mock_ccr, mp_setup):
    mp = mp_setup
    mock1, mock2 = mock.Mock(), mock.Mock()
    mock_ccr.return_value = 'res', mock1, mock2

    assert 'res' == mp.train(file_id=7)
    assert mp.train_result_ == 'res'


@pytest.mark.skipif(not HAS_SKLEARN, reason="scikit-learn not installed")
@mock.patch.object(_model, "APIClient", mock.Mock())
@mock.patch.object(_model.cio, "file_to_civis")
@mock.patch.object(_model.ModelPipeline, "_create_custom_run")
def test_modelpipeline_train_from_estimator(mock_ccr, mock_f2c):
    # Provide a model as a pre-made model and make sure we can train.
    mock_f2c.return_value = -21

    est = LogisticRegression()
    mp = _model.ModelPipeline(est, "dv")
    mock1, mock2 = mock.Mock(), mock.Mock()
    mock_ccr.return_value = 'res', mock1, mock2

    assert 'res' == mp.train(file_id=7)
    assert mp.train_result_ == 'res'
    assert mock_f2c.call_count == 1  # Called once to store input Estimator


@pytest.mark.skipif(not HAS_SKLEARN, reason="scikit-learn not installed")
@mock.patch.object(_model, "APIClient", mock.Mock())
@mock.patch.object(_model.cio, "file_to_civis")
@mock.patch.object(_model.ModelPipeline, "_create_custom_run")
def test_modelpipeline_train_custom_etl(mock_ccr, mock_f2c, mp_setup):
    # Provide a custom ETL estimator and make sure we can train.
    mock_api = setup_client_mock()
    etl = LogisticRegression()
    mp = _model.ModelPipeline('wf', 'dv', client=mock_api, etl=etl)
    mock_f2c.return_value = -21

    mock1, mock2 = mock.Mock(), mock.Mock()
    mock_ccr.return_value = 'res', mock1, mock2

    assert 'res' == mp.train(file_id=7)
    assert mp.train_result_ == 'res'
    assert mock_f2c.call_count == 1  # Called once to store input Estimator


@pytest.mark.skipif(not HAS_PANDAS, reason="pandas not installed")
@mock.patch.object(_model, "_stash_local_dataframe", return_value=-11,
                   autospec=True)
@mock.patch.object(_model.ModelPipeline, "_create_custom_run")
def test_modelpipeline_train_df(mock_ccr, mock_stash, mp_setup):
    mp = mp_setup
    mock1, mock2 = mock.Mock(), mock.Mock()
    mock_ccr.return_value = 'res', mock1, mock2

    train_data = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    assert 'res' == mp.train(train_data)
    mock_stash.assert_called_once_with(
        train_data, max(_model._PRED_TEMPLATES.keys()), client=mock.ANY)
    assert mp.train_result_ == 'res'


@mock.patch.object(_model, "_stash_local_file", return_value=-11)
@mock.patch.object(_model.ModelPipeline, "_create_custom_run")
def test_modelpipeline_train_file_name(mock_ccr, mock_stash, mp_setup):
    mp = mp_setup
    mock1, mock2 = mock.Mock(), mock.Mock()
    mock_ccr.return_value = 'res', mock1, mock2

    assert 'res' == mp.train(csv_path='meaning_of_life.csv')
    mock_stash.assert_called_once_with('meaning_of_life.csv', client=mock.ANY)
    assert mp.train_result_ == 'res'


def test_modelpipeline_train_value_no_input_error(mp_setup):
    mp = mp_setup
    with pytest.raises(ValueError) as exc:
        mp.train()
    assert str(exc.value) == "Provide a source of data."
    with pytest.raises(ValueError) as exc:
        mp.train(table_name='tab')
    assert str(exc.value) == "Provide a source of data."
    with pytest.raises(ValueError) as exc:
        mp.train(database_name='db')
    assert str(exc.value) == "Provide a source of data."


def test_modelpipeline_train_value_too_much_input_error(mp_setup):
    with pytest.raises(ValueError) as exc:
        mp_setup.train([[1, 2, 3]], table_name='tab', database_name='db')
    assert str(exc.value) == "Provide a single source of data."
    with pytest.raises(ValueError) as exc:
        mp_setup.train([[1, 2, 3]], file_id=12)
    assert str(exc.value) == "Provide a single source of data."
    with pytest.raises(ValueError) as exc:
        mp_setup.train(file_id=7, table_name='tab', database_name='db')
    assert str(exc.value) == "Provide a single source of data."


def test_modelpipeline_state(mp_setup):
    mp = mp_setup

    with pytest.raises(ValueError,
                       message="This model hasn't been trained yet."):
        mp.state

    mp.train_result_ = mock.Mock()
    mp.train_result_.state = 'foo'
    assert mp.state == 'foo'


def test_modelpipeline_estimator(mp_setup):
    mp = mp_setup
    with pytest.raises(ValueError,
                       message="This model hasn't been trained yet."):
        mp.estimator

    mp.train_result_ = mock.Mock()
    mp.train_result_.estimator = 'foo'
    assert mp.estimator == 'foo'


def test_modelpipeline_predict_value_error(mp_setup):
    mp = mp_setup
    with pytest.raises(ValueError,
                       message="This model hasn't been trained yet."):
        mp.predict()

    mp.train_result_ = mock.Mock()
    mp.train_result_.running.return_value = False

    with pytest.raises(ValueError) as exc:
        mp.predict()
    assert str(exc.value) == "Provide a source of data."
    with pytest.raises(ValueError) as exc:
        mp.predict(table_name='tab')
    assert str(exc.value) == "Provide a source of data."
    with pytest.raises(ValueError) as exc:
        mp.predict(database_name='db')
    assert str(exc.value) == "Provide a source of data."


def test_modelpipeline_predict_value_too_much_input_error(mp_setup):
    result = mock.Mock(spec_set=_model.ModelFuture)
    result.running.return_value = False
    mp_setup.train_result_ = result  # Make this look trained.

    with pytest.raises(ValueError) as exc:
        mp_setup.predict([[1, 2, 3]], table_name='tab', database_name='db')
    assert str(exc.value) == "Provide a single source of data."
    with pytest.raises(ValueError) as exc:
        mp_setup.predict([[1, 2, 3]], file_id=12)
    assert str(exc.value) == "Provide a single source of data."
    with pytest.raises(ValueError) as exc:
        mp_setup.predict(file_id=7, table_name='tab', database_name='db')
    assert str(exc.value) == "Provide a single source of data."
    with pytest.raises(ValueError) as exc:
        mp_setup.predict(file_id=7, manifest=123)
    assert str(exc.value) == "Provide a single source of data."
