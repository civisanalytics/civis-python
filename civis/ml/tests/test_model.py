from builtins import super
from collections import namedtuple
from concurrent.futures import CancelledError
from io import StringIO
import json
import os
import pickle
from tempfile import TemporaryDirectory
from unittest import mock

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

from civis._utils import camel_to_snake
from civis.base import CivisAPIError, CivisJobFailure
from civis.response import Response
from civis.tests import (
    create_client_mock, create_client_mock_for_container_tests)
import pytest

from civis.ml import _model


TRAIN_ID_PROD, PREDICT_ID_PROD, REGISTRATION_ID_PROD = 654, 321, 789
TRAIN_ID_OLD, PREDICT_ID_OLD = 123, 456
TEST_TEMPLATE_ID_ALIAS_OBJECTS = [
    # Version-less aliases for the latest production code.
    Response(dict(alias='civis-civisml-training', object_id=TRAIN_ID_PROD)),
    Response(dict(alias='civis-civisml-prediction', object_id=PREDICT_ID_PROD)),  # noqa
    Response(dict(alias='civis-civisml-registration', object_id=789)),
    # Versioned aliases. Pretend that v2.3 is the latest production version.
    Response(dict(alias='civis-civisml-training-v2-3', object_id=TRAIN_ID_PROD)),  # noqa
    Response(dict(alias='civis-civisml-prediction-v2-3', object_id=PREDICT_ID_PROD)),  # noqa
    Response(dict(alias='civis-civisml-registration-v2-3', object_id=789)),
    # Versioned aliases. Versions older than v2.2 don't have registration ID.
    Response(dict(alias='civis-civisml-training-v1-4', object_id=TRAIN_ID_OLD)),  # noqa
    Response(dict(alias='civis-civisml-prediction-v1-4', object_id=PREDICT_ID_OLD)),  # noqa
    # Other special versions, e.g., "dev"
    Response(dict(alias='civis-civisml-training-dev', object_id=345)),
    Response(dict(alias='civis-civisml-prediction-dev', object_id=678)),
    Response(dict(alias='civis-civisml-registration-dev', object_id=901)),
]
TEST_TEMPLATE_IDS = {  # Must match TEST_TEMPLATE_ID_ALIAS_OBJECTS
    None: {'training': TRAIN_ID_PROD, 'prediction': PREDICT_ID_PROD, 'registration': REGISTRATION_ID_PROD},  # noqa
    'v2.3': {'training': TRAIN_ID_PROD, 'prediction': PREDICT_ID_PROD, 'registration': REGISTRATION_ID_PROD},  # noqa
    'v1.4': {'training': TRAIN_ID_OLD, 'prediction': PREDICT_ID_OLD, 'registration': None},  # noqa
    'dev': {'training': 345, 'prediction': 678, 'registration': 901},
}


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
    with TemporaryDirectory() as temp_dir:
        fname = os.path.join(temp_dir, 'tempfile')
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
    assert isinstance(mock_file.call_args[0][0], StringIO)


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
    mock_client = create_client_mock_for_container_tests()
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
@mock.patch.object(_model.ModelFuture, "_set_job_exception", autospec=True)
@mock.patch.object(_model.ModelFuture, "add_done_callback", autospec=True)
def test_modelfuture_constructor(mock_adc, mock_spe):
    c = create_client_mock_for_container_tests(7, 17)

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
@mock.patch.object(_model, 'APIClient',
                   return_value=create_client_mock_for_container_tests())
def test_modelfuture_pickle_smoke(mock_client):
    mf = _model.ModelFuture(job_id=7, run_id=13,
                            client=create_client_mock_for_container_tests())
    mf.result()
    mf_pickle = pickle.dumps(mf)
    pickle.loads(mf_pickle)


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_set_job_exception_metadata_exception(m_sleep):
    """Tests cases where accessing metadata throws exceptions
    """
    # State "running" prevents termination when the object is created.
    mock_client = create_client_mock_for_container_tests(1, 2, state='running')

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
        _model.ModelFuture._set_job_exception(fut)

    with pytest.warns(UserWarning):
        # The KeyError is caught, but sends a warning
        fut = ModelFutureRaiseExc(KeyError, 1, 2, client=mock_client)
        _model.ModelFuture._set_job_exception(fut)

    fut = ModelFutureRaiseExc(RuntimeError, 1, 2, client=mock_client)
    with pytest.raises(RuntimeError):
        _model.ModelFuture._set_job_exception(fut)


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_set_job_exception_memory_error(m_sleep):
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
    mock_client = create_client_mock_for_container_tests(
        1, 2, state='failed', log_outputs=logs)
    fut = _model.ModelFuture(1, 2, client=mock_client)
    with pytest.raises(MemoryError) as err:
        fut.result()
    assert str(err.value) == f"(From job 1 / run 2) {err_msg}"


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_set_job_exception_unknown_error(m_sleep):
    # If we really don't recognize the error, at least give the
    # user a few lines of logs so they can maybe figure it out themselves.
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
    mock_client = create_client_mock_for_container_tests(
        1, 2, state='failed', log_outputs=logs)
    err_msg = (
        "(From job 1 / run 2) "
        + '\n'.join([x['message'] for x in logs][::-1]))
    fut = _model.ModelFuture(1, 2, client=mock_client)
    with pytest.raises(CivisJobFailure) as err:
        fut.result()
    assert str(err.value).startswith(err_msg)


@mock.patch.object(_model.cio, "file_to_json", autospec=True,
                   return_value={'run': {'status': 'succeeded',
                                         'stack_trace': None}})
def test_set_job_exception_no_exception(mock_f2j):
    # If nothing went wrong, we shouldn't set an exception
    ro = [{'name': 'model_info.json', 'object_id': 137, 'object_type': 'File'},
          {'name': 'metrics.json', 'object_id': 139, 'object_type': 'File'}]
    ro = [Response(o) for o in ro]
    mock_client = create_client_mock_for_container_tests(
        1, 2, state='succeeded', run_outputs=ro)
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
    _model.ModelFuture._set_job_exception(fut)
    assert isinstance(fut._exception, _model.ModelError)
    assert 'this is a trace' in str(fut._exception)

    # don't change attribute if it's already a ModelError
    fut._exception = _model.ModelError('undecipherable message')
    _model.ModelFuture._set_job_exception(fut)
    assert isinstance(fut._exception, _model.ModelError)
    assert 'undecipherable message' in str(fut._exception)


def test_set_job_exception_training_metadata():
    trn = {'run': {'status': 'exception', 'stack_trace': 'this is a trace'}}
    val = None
    exc = FileNotFoundError('Look, no mocks!')

    _test_set_model_exc(trn, val, exc)


def test_set_job_exception_validation_metadata():
    trn = {'run': {'status': 'succeeded'}}
    val = {'run': {'status': 'exception', 'stack_trace': 'this is a trace'}}
    exc = FileNotFoundError('Hahaha, zero mocks here!')

    _test_set_model_exc(trn, val, exc)


@mock.patch.object(_model.cio, "file_to_json",
                   mock.Mock(return_value='bar', spec_set=True))
@mock.patch.object(_model.ModelFuture, "_set_job_exception",
                   lambda *args: None)
def test_getstate():
    c = create_client_mock_for_container_tests(3, 7)

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
    c = create_client_mock_for_container_tests(3, 7)

    mf = _model.ModelFuture(3, 7, client=c)
    ret = mf.state
    assert ret == 'foo'

    c.scripts.get_containers_runs.return_value = Response({'id': 7,
                                                           'container_id': 3,
                                                           'state': 'failed'})
    mf = _model.ModelFuture(3, 7, client=c)
    assert mf.state == 'failed'


@mock.patch.object(_model.ModelFuture, "metadata",
                   return_value={'run': {'configuration':
                                         {'data': {'primary_key': 'foo'}}}})
@mock.patch.object(_model, "_load_table_from_outputs", return_value='bar')
@mock.patch.object(_model.ModelFuture, "result")
@mock.patch.object(_model.ModelFuture, "_set_job_exception", mock.Mock())
def test_table(mock_res, mock_lt, mock_meta):
    c = create_client_mock_for_container_tests(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)
    assert mf.table == 'bar'
    mock_lt.assert_called_once_with(3, 7, 'predictions.csv',
                                    index_col=0, client=c)


@mock.patch.object(_model.ModelFuture, "metadata",
                   return_value={'run': {'configuration':
                                         {'data': {'primary_key': None}}}})
@mock.patch.object(_model, "_load_table_from_outputs", return_value='bar')
@mock.patch.object(_model.ModelFuture, "result")
@mock.patch.object(_model.ModelFuture, "_set_job_exception", mock.Mock())
def test_table_no_pkey(mock_res, mock_lt, mock_meta):
    c = create_client_mock_for_container_tests(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)
    assert mf.table == 'bar'
    mock_lt.assert_called_once_with(3, 7, 'predictions.csv',
                                    index_col=False, client=c)


@mock.patch.object(_model.ModelFuture, "metadata",
                   return_value={'run': {'configuration':
                                         {'data': {'primary_key': 'foo'}}}})
@mock.patch.object(_model, "_load_table_from_outputs")
@mock.patch.object(_model.ModelFuture, "result")
@mock.patch.object(_model.ModelFuture, "_set_job_exception", mock.Mock())
def test_table_None(mock_res, mock_lt, mock_meta):
    mock_lt.side_effect = FileNotFoundError()
    c = create_client_mock_for_container_tests(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)
    assert mf.table is None
    mock_lt.assert_called_once_with(3, 7, 'predictions.csv',
                                    index_col=0, client=c)


@mock.patch.object(_model.cio, "file_id_from_run_output",
                   mock.Mock(return_value=11, spec_set=True))
@mock.patch.object(_model.cio, "file_to_json", return_value={'foo': 'bar'})
@mock.patch.object(_model.ModelFuture, "_set_job_exception")
def test_metadata(mock_spec, mock_f2j):
    c = create_client_mock_for_container_tests(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)
    assert mf.metadata == {'foo': 'bar'}
    mock_f2j.assert_called_once_with(11, client=c)


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_train_data_fname(m_sleep):
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


@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
@mock.patch.object(_model, "_load_table_from_outputs", autospec=True)
def test_train_data_exc_handling(mock_load_table, m_sleep):
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
@mock.patch.object(_model.ModelFuture, "_set_job_exception", mock.Mock())
def test_estimator(mock_le):
    c = create_client_mock_for_container_tests(3, 7)

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
@mock.patch.object(_model.ModelFuture, "_set_job_exception")
def test_validation_metadata_training(mock_spe, mock_f2f,
                                      mock_file_id_from_run_output):
    mock_file_id_from_run_output.return_value = 11
    c = create_client_mock_for_container_tests(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)

    assert mf.validation_metadata == 'foo'
    mock_f2f.assert_called_once_with(11, client=c)
    mock_file_id_from_run_output.assert_called_with('metrics.json', 3, 7,
                                                    client=mock.ANY)


@mock.patch.object(_model.cio, "file_id_from_run_output", autospec=True)
@mock.patch.object(_model.cio, "file_to_json", autospec=True)
@mock.patch.object(_model.ModelFuture, "_set_job_exception")
def test_validation_metadata_missing(mock_spe, mock_f2f,
                                     mock_file_id_from_run_output):
    # Make sure that missing validation metadata doesn't cause an error
    mock_file_id_from_run_output.side_effect = FileNotFoundError
    c = create_client_mock_for_container_tests(3, 7)
    mf = _model.ModelFuture(3, 7, client=c)

    assert mf.validation_metadata is None
    assert mf.metrics is None
    assert mock_f2f.call_count == 0
    assert mock_file_id_from_run_output.call_count == 1


@mock.patch.object(_model.cio, "file_id_from_run_output", autospec=True)
@mock.patch.object(_model.cio, "file_to_json", return_value='foo',
                   autospec=True)
@mock.patch.object(_model.ModelFuture, "_set_job_exception")
def test_validation_metadata_prediction(mock_spe, mock_f2f,
                                        mock_file_id_from_run_output):
    mock_file_id_from_run_output.return_value = 11
    c = create_client_mock_for_container_tests(3, 7)
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
    c = create_client_mock_for_container_tests(3, 7)
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
    c = create_client_mock_for_container_tests(3, 7)
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
    c = create_client_mock_for_container_tests(3, 7)
    mf = _model.ModelFuture(1, 2, 3, 7, client=c)

    assert mf.metrics == 'foo'
    mock_file_id_from_run_output.assert_called_with('metrics.json', 3, 7,
                                                    client=mock.ANY)


###############################################
# Tests of utilities for CivisML template IDs #
###############################################
@pytest.mark.parametrize(
    'alias, expected_job_type, expected_version',
    [
        ('civis-civisml-training', 'training', None),
        ('civis-civisml-training-v2-3', 'training', 'v2.3'),
        ('civis-civisml-training-dev', 'training', 'dev'),
        ('civis-civisml-training-foo-bar', 'training', 'foo-bar'),
    ],
)
def test__get_job_type_version(alias, expected_job_type, expected_version):
    actual_job_type, actual_version = _model._get_job_type_version(alias)
    assert actual_job_type == expected_job_type
    assert actual_version == expected_version


@pytest.mark.parametrize(
    'invalid_alias',
    ['foobar', 'civis-civisml', 'civis-civisml-', 'civis-civisml-training-',
     'civis-civisml-training-foobar-']
)
def test__get_job_type_version_invalid_alias(invalid_alias):
    with pytest.raises(ValueError):
        _model._get_job_type_version(invalid_alias)


def test__get_template_ids_all_versions():
    m_client = create_client_mock()
    m_client.aliases.list.return_value = TEST_TEMPLATE_ID_ALIAS_OBJECTS
    actual_template_ids = _model._get_template_ids_all_versions(m_client)
    expected_template_ids = TEST_TEMPLATE_IDS
    assert actual_template_ids == expected_template_ids


@mock.patch.object(_model, '_get_template_ids_all_versions',
                   mock.Mock(return_value=TEST_TEMPLATE_IDS))
@pytest.mark.parametrize(
    'version, train_id, predict_id, register_id',
    [(version, ids['training'], ids['prediction'], ids['registration'])
     for version, ids in TEST_TEMPLATE_IDS.items()]
)
def test__get_template_ids(version, train_id, predict_id, register_id):
    actual_train_id, actual_predict_id, actual_register_id = (
        _model._get_template_ids(version, mock.ANY)
    )
    assert actual_train_id == train_id
    assert actual_predict_id == predict_id
    assert actual_register_id == register_id


@mock.patch.object(_model, '_get_template_ids_all_versions',
                   mock.Mock(return_value=TEST_TEMPLATE_IDS))
def test__get_template_ids_invalid_version():
    with pytest.raises(ValueError):
        _model._get_template_ids('not_a_version', mock.ANY)


#####################################
# Tests of ModelPipeline below here #
#####################################
@pytest.fixture
def mp_setup():
    mock_api = create_client_mock_for_container_tests()
    mock_api.aliases.list.return_value = TEST_TEMPLATE_ID_ALIAS_OBJECTS
    mp = _model.ModelPipeline('wf', 'dv', client=mock_api)
    return mp


@pytest.mark.skipif(not HAS_SKLEARN, reason="scikit-learn not installed")
@mock.patch.object(_model, '_get_template_ids_all_versions',
                   mock.Mock(return_value=TEST_TEMPLATE_IDS))
def test_modelpipeline_etl_init_err():
    # If users don't have access to >= v2.0 temlates, then passing
    # `etl` to a new ModelPipeline should produce a NotImplementedError.
    mock_client = mock.MagicMock()
    with pytest.raises(NotImplementedError):
        _model.ModelPipeline(LogisticRegression(), 'test',
                             etl=LogisticRegression(),
                             client=mock_client)


@mock.patch.object(_model, '_get_template_ids_all_versions',
                   mock.Mock(return_value=TEST_TEMPLATE_IDS))
@mock.patch.object(_model, 'ModelFuture')
def test_modelpipeline_classmethod_constructor_errors(mock_future):
    # catch 404 error if model isn't found and throw ValueError
    mock_client = mock.Mock()
    response = namedtuple('Reponse', ['content', 'response', 'reason',
                                      'status_code'])(False, None, None, 404)
    mock_future.side_effect = CivisAPIError(response)
    with pytest.raises(ValueError):
        _model.ModelPipeline.from_existing(1, 1, client=mock_client)


def _container_response_stub(from_template_id=8387):
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


@mock.patch.object(_model, '_get_template_ids_all_versions',
                   mock.Mock(return_value=TEST_TEMPLATE_IDS))
@mock.patch.object(_model, 'ModelFuture')
def test_modelpipeline_classmethod_constructor(mock_future):
    mock_client = mock.Mock()
    mock_client.scripts.get_containers.return_value = \
        container = _container_response_stub(TRAIN_ID_PROD)
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


@mock.patch.object(_model, '_get_template_ids_all_versions',
                   mock.Mock(return_value=TEST_TEMPLATE_IDS))
@mock.patch.object(_model, 'ModelFuture')
def test_modelpipeline_classmethod_constructor_defaults(mock_future):

    # checks that it works with a registration template and train template
    for template_id in [TRAIN_ID_PROD, REGISTRATION_ID_PROD]:
        container_response_stub = _container_response_stub(template_id)
        del container_response_stub.arguments['PARAMS']
        del container_response_stub.arguments['CVPARAMS']
        mock_client = mock.Mock()
        mock_client.scripts.get_containers.return_value = container_response_stub  # noqa
        mock_client.credentials.get.return_value = Response({'name': 'Token'})

        # test everything is working fine
        mp = _model.ModelPipeline.from_existing(1, 1, client=mock_client)
        assert mp.cv_params == {}
        assert mp.parameters == {}


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
@mock.patch.object(_model, '_get_template_ids_all_versions',
                   mock.Mock(return_value=TEST_TEMPLATE_IDS))
@mock.patch("civis.futures.time.sleep", side_effect=lambda x: None)
def test_modelpipeline_classmethod_constructor_nonint_id(m_sleep):
    # Verify that we can still JSON-serialize job and run IDs even
    # if they're entered in a non-JSON-able format.
    # We need to turn them into JSON to set them as script arguments.
    mock_client = create_client_mock_for_container_tests(1, 2)
    container_response_stub = _container_response_stub(TRAIN_ID_PROD)
    mock_client.scripts.get_containers.return_value = container_response_stub

    mp = _model.ModelPipeline.from_existing(np.int64(1), np.int64(2),
                                            client=mock_client)

    out = json.dumps({'job': mp.train_result_.job_id,
                      'run': mp.train_result_.run_id})
    assert out == '{"job": 1, "run": 2}' or out == '{"run": 2, "job": 1}'


@mock.patch.object(_model, '_get_template_ids_all_versions',
                   mock.Mock(return_value=TEST_TEMPLATE_IDS))
@pytest.mark.parametrize(
    'train_id, predict_id',
    [(TRAIN_ID_PROD, PREDICT_ID_PROD), (TRAIN_ID_OLD, PREDICT_ID_OLD)],
)
@mock.patch.object(_model, 'ModelFuture', autospec=True)
def test_modelpipeline_classmethod_constructor_old_version(
        mock_future, train_id, predict_id):
    # Test that we select the correct prediction template for different
    # versions of a training job.
    mock_client = create_client_mock_for_container_tests()
    mock_client.scripts.get_containers.return_value = \
        _container_response_stub(from_template_id=train_id)
    mp = _model.ModelPipeline.from_existing(1, 1, client=mock_client)
    assert mp.predict_template_id == predict_id


@mock.patch.object(_model.ModelPipeline, "_create_custom_run")
def test_modelpipeline_train(mock_ccr, mp_setup):
    mp = mp_setup
    mock1, mock2 = mock.Mock(), mock.Mock()
    mock_ccr.return_value = 'res', mock1, mock2

    assert 'res' == mp.train(file_id=7)
    assert mp.train_result_ == 'res'


@pytest.mark.skipif(not HAS_SKLEARN, reason="scikit-learn not installed")
@mock.patch.object(_model, '_get_template_ids_all_versions',
                   mock.Mock(return_value=TEST_TEMPLATE_IDS))
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
@mock.patch.object(_model, "_get_template_ids")
@mock.patch.object(_model.cio, "file_to_civis")
@mock.patch.object(_model.ModelPipeline, "_create_custom_run")
def test_modelpipeline_train_custom_etl(mock_ccr, mock_f2c, mock_template_ids):
    # Provide a custom ETL estimator and make sure we can train.
    mock_api = create_client_mock_for_container_tests()
    # training template ID 11111 >= 9968 for the etl arg to work
    mock_template_ids.return_value = 11111, 22222, 33333
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
        train_data, TRAIN_ID_PROD, client=mock.ANY)
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
                       match="This model hasn't been trained yet."):
        mp.state

    mp.train_result_ = mock.Mock()
    mp.train_result_.state = 'foo'
    assert mp.state == 'foo'


def test_modelpipeline_estimator(mp_setup):
    mp = mp_setup
    with pytest.raises(ValueError,
                       match="This model hasn't been trained yet."):
        mp.estimator

    mp.train_result_ = mock.Mock()
    mp.train_result_.estimator = 'foo'
    assert mp.estimator == 'foo'


def test_modelpipeline_predict_value_error(mp_setup):
    mp = mp_setup
    with pytest.raises(ValueError,
                       match="This model hasn't been trained yet."):
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


@mock.patch.object(_model, '_get_template_ids_all_versions',
                   mock.Mock(return_value=TEST_TEMPLATE_IDS))
@mock.patch.object(_model, "APIClient", mock.Mock())
@pytest.mark.parametrize(
    'version, train_id, predict_id',
    [('v2.3', TRAIN_ID_PROD, PREDICT_ID_PROD),
     ('v1.4', TRAIN_ID_OLD, PREDICT_ID_OLD)],
)
def test_modelpipeline_pickling_preserves_template_ids(
        version, train_id, predict_id):
    # Test that pickling a ModelPipeline object preserves the template IDs
    # that have already been set during object instantiation.
    with TemporaryDirectory() as temp_dir:
        mp = _model.ModelPipeline('wf', 'dv', civisml_version=version)

        # Before pickling, make sure the template IDs are set as expected
        assert mp.train_template_id == train_id
        assert mp.predict_template_id == predict_id

        pickle_path = os.path.join(temp_dir, 'model.pkl')

        with open(pickle_path, 'wb') as f:
            pickle.dump(mp, f)

        with open(pickle_path, 'rb') as f:
            mp_unpickled = pickle.load(f)

        # After unpickling, the template IDs should remain.
        assert mp_unpickled.train_template_id == train_id
        assert mp_unpickled.predict_template_id == predict_id
