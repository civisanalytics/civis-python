from collections import namedtuple
from io import BytesIO
import json
import os
import pickle
import tempfile
from unittest import mock

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
try:
    import joblib
    HAS_JOBLIB = True
except ImportError:
    HAS_JOBLIB = False

from civis import APIClient
from civis.base import CivisAPIError, CivisJobFailure
from civis.response import Response
import pytest

from civis.ml import _model


def setup_client_mock(script_id=-10, run_id=100):
    """Return a Mock set up for use in testing container scripts

    Parameters
    ----------
    script_id: int
        Mock-create containers with this ID when calling `post_containers`
        or `post_containers_runs`.
    run_id: int
        Mock-create runs with this ID when calling `post_containers_runs`.

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
                                   'state': 'succeeded'})
    c.scripts.post_containers_runs.return_value = mock_container_run_start
    c.scripts.get_containers_runs.return_value = mock_container_run

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

    @_model._check_is_fit
    def foo(arg):
        return 7

    with pytest.raises(ValueError):
        foo(mock_pipe)


def test_check_is_fit():
    mock_pipe = mock.MagicMock()
    mock_pipe.train_result_ = True

    @_model._check_is_fit
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
    with tempfile.TemporaryDirectory() as tempdir:
        fname = os.path.join(tempdir, 'filename')
        with open(fname, 'wt') as _fout:
            _fout.write("a,b,c\n1,2,3\n")
        assert _model._stash_local_data(fname) == -11
    mock_file.assert_called_once_with(mock.ANY,
                                      name='modelpipeline_data.csv',
                                      client=mock.ANY)


@pytest.mark.skipif(not HAS_PANDAS, reason="pandas not installed")
@mock.patch.object(_model.cio, 'file_to_civis', return_value=-11)
def test_stash_local_data_from_dataframe(mock_file):
    df = pd.DataFrame({'a': [1], 'b': [2]})
    assert _model._stash_local_data(df) == -11
    mock_file.assert_called_once_with(mock.ANY, name='modelpipeline_data.csv',
                                      client=mock.ANY)
    assert isinstance(mock_file.call_args[0][0], BytesIO)


@pytest.mark.skipif(not HAS_JOBLIB, reason="joblib not installed")
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
                   mock.Mock(return_value={'spam': 'eggs'}))
@mock.patch.object(_model, 'APIClient', setup_client_mock())
def test_modelfuture_pickle_smoke():
    mf = _model.ModelFuture(job_id=7, run_id=13, client=setup_client_mock())
    mf.result()
    mf_pickle = pickle.dumps(mf)
    pickle.loads(mf_pickle)


def test_set_model_exception_metadata_exception():
    """Tests cases where accessing metadata throws exceptions
    """
    class ModelFutureRaiseExc:
        def __init__(self, exc):
            self.exc = exc

        @property
        def metadata(self):
            raise self.exc('What a spectacular failure, you say!')

    # exception types get caught!
    for exc in [FileNotFoundError, CivisJobFailure, KeyError]:
        fut = ModelFutureRaiseExc(exc)
        _model.ModelFuture._set_model_exception(fut)

    fut = ModelFutureRaiseExc(RuntimeError)
    with pytest.raises(RuntimeError):
        _model.ModelFuture._set_model_exception(fut)


class ModelFutureStub:

    def __init__(self, exc, trn, val):
        self._exception = exc
        self.metadata = trn
        self.validation_metadata = val
        self.is_training = True

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


@mock.patch.object(_model.cio, "file_id_from_run_output",
                   mock.Mock(return_value=11, spec_set=True))
@mock.patch.object(_model.cio, "file_to_json", return_value='bar')
@mock.patch.object(_model.ModelFuture, "_set_model_exception")
def test_metadata(mock_spec, mock_f2j):
    c = setup_client_mock(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)
    assert mf.metadata == 'bar'
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


@mock.patch.object(_model, "_load_table_from_outputs", autospec=True)
def test_train_data(mock_load_table):
    def poller(*args, **kwargs):
        return Response({'state': 'succeeded'})
    mock_client = mock.MagicMock()
    mock_client.scripts.get_containers_runs = poller
    mf = _model.ModelFuture(job_id=1, run_id=2, client=mock_client)
    mf._train_data_fname = 'placeholder.csv'

    miscallaneous_string = 'one two three'
    mock_load_table.return_value = miscallaneous_string
    assert mf.train_data == miscallaneous_string


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


@mock.patch.object(_model.cio, "file_id_from_run_output",
                   mock.Mock(return_value=11, spec_set=True))
@mock.patch.object(_model.cio, "file_to_json", return_value='foo')
@mock.patch.object(_model.ModelFuture, "_set_model_exception")
def test_validation_metadata(mock_spe, mock_f2f):
    c = setup_client_mock(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)
    assert mf.validation_metadata == 'foo'
    mock_f2f.assert_called_once_with(11, client=c)


@mock.patch.object(_model.cio, "file_id_from_run_output",
                   mock.Mock(return_value=11, spec_set=True))
@mock.patch.object(_model.cio, "file_to_json",
                   mock.MagicMock(return_value={'metrics': 'foo'}))
def test_metrics():
    c = setup_client_mock(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)

    assert mf.metrics == 'foo'


#####################################
# Tests of ModelPipeline below here #
#####################################
@mock.patch.object(_model, 'ModelFuture')
@mock.patch.object(_model, 'APIClient', mock.Mock())
def test_modelpipeline_classmethod_constructor_errors(mock_future):
    # catch 404 error if model isn't found and throw ValueError
    response = namedtuple('Reponse', ['content', 'response', 'reason',
                                      'status_code'])(False, None, None, 404)
    mock_future.side_effect = CivisAPIError(response)
    with pytest.raises(ValueError):
        _model.ModelPipeline.from_existing(1, 1)


@pytest.fixture()
def container_response_stub():
    Container = namedtuple('Container', ['arguments',
                                         'required_resources',
                                         'docker_image_tag',
                                         'docker_command',
                                         'repo_http_uri',
                                         'repo_ref',
                                         'name'])
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
        'REQUIRED_DISK_SPACE': -20
    }
    return Container(arguments=arguments,
                     required_resources={},
                     docker_image_tag=None,
                     docker_command=None,
                     repo_http_uri=None,
                     repo_ref=None,
                     name='Civis Model Train')


@mock.patch.object(_model, 'ModelFuture')
@mock.patch.object(_model, 'APIClient')
def test_modelpipeline_classmethod_constructor(mock_client, mock_future,
                                               container_response_stub):
    mock_client.return_value.scripts.get_containers.return_value = \
        container = container_response_stub

    resources = {'REQUIRED_CPU': 1000,
                 'REQUIRED_MEMORY': 9999,
                 'REQUIRED_DISK_SPACE': -20}

    # test everything is working fine
    mp = _model.ModelPipeline.from_existing(1, 1)
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


@mock.patch.object(_model.ModelPipeline, "_create_custom_run")
def test_modelpipeline_train(mock_ccr, mp_setup):
    mp = mp_setup
    mock1, mock2 = mock.Mock(), mock.Mock()
    mock_ccr.return_value = 'res', mock1, mock2

    assert 'res' == mp.train(file_id=7)
    assert mp.train_result_ == 'res'


@mock.patch.object(_model, "APIClient", mock.Mock())
@mock.patch.object(_model, "file_to_civis")
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


@mock.patch.object(_model, "_stash_local_data", return_value=-11)
@mock.patch.object(_model.ModelPipeline, "_create_custom_run")
def test_modelpipeline_train_df(mock_ccr, mock_stash, mp_setup):
    mp = mp_setup
    mock1, mock2 = mock.Mock(), mock.Mock()
    mock_ccr.return_value = 'res', mock1, mock2

    train_data = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    assert 'res' == mp.train(train_data)
    mock_stash.assert_called_once_with(train_data, client=mock.ANY)
    assert mp.train_result_ == 'res'


@mock.patch.object(_model, "_stash_local_data", return_value=-11)
@mock.patch.object(_model.ModelPipeline, "_create_custom_run")
def test_modelpipeline_train_file_name(mock_ccr, mock_stash, mp_setup):
    mp = mp_setup
    mock1, mock2 = mock.Mock(), mock.Mock()
    mock_ccr.return_value = 'res', mock1, mock2

    assert 'res' == mp.train('meaning_of_life.csv')
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
        mp_setup.train(X=[[1, 2, 3]], table_name='tab', database_name='db')
    assert str(exc.value) == "Provide a single source of data."
    with pytest.raises(ValueError) as exc:
        mp_setup.train(X=[[1, 2, 3]], file_id=12)
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
        mp_setup.predict(X=[[1, 2, 3]], table_name='tab', database_name='db')
    assert str(exc.value) == "Provide a single source of data."
    with pytest.raises(ValueError) as exc:
        mp_setup.predict(X=[[1, 2, 3]], file_id=12)
    assert str(exc.value) == "Provide a single source of data."
    with pytest.raises(ValueError) as exc:
        mp_setup.predict(file_id=7, table_name='tab', database_name='db')
    assert str(exc.value) == "Provide a single source of data."
    with pytest.raises(ValueError) as exc:
        mp_setup.predict(file_id=7, manifest=123)
    assert str(exc.value) == "Provide a single source of data."


def test_modelpipeline_predict_runtime_error(mp_setup):
    mp = mp_setup
    mp.train_result_ = mock.Mock()
    mp.train_result_.running.return_value = True

    with pytest.raises(RuntimeError,
                       message='Wait for the training to finish.'):
        mp.predict()
