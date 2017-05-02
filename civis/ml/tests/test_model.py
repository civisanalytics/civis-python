
from collections import namedtuple
from io import BytesIO
import json
import os
import pickle
import tempfile
from unittest import mock

import joblib
import numpy as np

from civis import APIClient
from civis.base import CivisAPIError, CivisJobFailure
from civis import futures
from civis.response import Response
import pandas as pd
import pytest
from sklearn.linear_model import LogisticRegression

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

    # No channels endpoint available
    del c.channels

    return c


def test_check_is_fit_exception():
    mock_pipe = mock.MagicMock()
    mock_pipe.train_result_ = None

    @_model.check_is_fit
    def foo(arg):
        return 7

    with pytest.raises(ValueError):
        foo(mock_pipe)


def test_check_is_fit():
    mock_pipe = mock.MagicMock()
    mock_pipe.train_result_ = True

    @_model.check_is_fit
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


@mock.patch.object(_model, 'open', new_callable=mock.mock_open)
@mock.patch.object(_model, 'file_to_civis', return_value=-11)
def test_stash_local_data_from_file(mock_file, mock_open):
    assert _model._stash_local_data('airspeed_velocity.csv') == -11
    mock_open.assert_called_once_with('airspeed_velocity.csv')
    mock_file.assert_called_once_with(mock_open.return_value,
                                      name='modelpipeline_data.csv',
                                      client=mock.ANY)


@mock.patch.object(_model, 'file_to_civis', return_value=-11)
def test_stash_local_data_from_dataframe(mock_file):
    df = pd.DataFrame({'a': [1], 'b': [2]})
    assert _model._stash_local_data(df) == -11
    mock_file.assert_called_once_with(mock.ANY, name='modelpipeline_data.csv',
                                      client=mock.ANY)
    assert isinstance(mock_file.call_args[0][0], BytesIO)


@mock.patch.object(_model, 'retrieve_file', autospec=True)
def test_load_estimator(mock_retrieve):
    obj = {'spam': 'eggs'}

    def _retrieve_json(fname, job_id, run_id, local_dir, client=None):
        full_name = os.path.join(local_dir, fname)
        joblib.dump(obj, full_name)
        return full_name

    mock_retrieve.side_effect = _retrieve_json
    out = _model.load_estimator(13, 17, 'fname')
    assert out == obj


###################################
# Tests of ModelFuture below here #
###################################
@mock.patch.object(_model.ModelFuture, "_set_model_exception", autospec=True)
@mock.patch.object(_model.ModelFuture, "add_done_callback", autospec=True)
@mock.patch.object(futures, 'APIClient')
@mock.patch.object(_model, 'APIClient')
def test_modelfuture_constructor(m_api, m_f_api, mock_adc, mock_spe):
    c = setup_client_mock(7, 17)
    m_api.return_value = m_f_api.return_value = c

    mf = _model.ModelFuture(job_id=7, run_id=17)
    assert mf.is_training is True
    assert mf.train_run_id == 17
    assert mf.train_job_id == 7

    mf = _model.ModelFuture(job_id=7, run_id=17,
                            train_job_id=23, train_run_id=29)
    assert mf.is_training is False
    assert mf.train_run_id == 29
    assert mf.train_job_id == 23


@mock.patch.object(_model, "load_dict",
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


@mock.patch.object(_model, "load_dict", mock.Mock(return_value='bar'))
@mock.patch.object(futures, 'APIClient')
@mock.patch.object(_model, 'APIClient')
@mock.patch.object(api.ModelFuture, "_set_model_exception", lambda *args: None)
def test_getstate(m_api, m_f_api):
    c = setup_client_mock(3, 7)
    m_api.return_value = m_f_api.return_value = c

    mf = _model.ModelFuture(3, 7)
    ret = mf.__getstate__()
    assert ret['_done_callbacks'] == []
    assert not ret['_self_polling_executor']
    assert 'client' not in ret
    assert 'poller' not in ret
    assert '_condition' not in ret


@mock.patch.object(_model, 'load_dict', autospec=True,
                   return_value={'run': {'status': 'foo'}})
def test_state(mock_load_dict):
    c = setup_client_mock(3, 7)

    mf = _model.ModelFuture(3, 7, client=c)
    ret = mf.state
    assert ret == 'foo'

    c.scripts.get_containers_runs.return_value = Response({'id': 7,
                                                           'container_id': 3,
                                                           'state': 'failed'})
    mf = _model.ModelFuture(3, 7, client=c)
    assert mf.state == 'failed'


@mock.patch.object(futures, "APIClient")
@mock.patch.object(_model, "APIClient")
@mock.patch.object(_model, "load_table_from_outputs", return_value='bar')
@mock.patch.object(_model.ModelFuture, "result")
@mock.patch.object(_model.ModelFuture, "_set_model_exception", mock.Mock())
def test_table(mock_res, mock_lt, m_api, m_f_api):
    c = setup_client_mock(3, 7)
    m_api.return_value = m_f_api.return_value = c
    mf = api.ModelFuture(3, 7)
    assert mf.table == 'bar'
    mock_lt.assert_called_once_with(3, 7, 'predictions.csv',
                                    index_col=0, client=mock.ANY)


@mock.patch.object(_model, "load_dict", return_value='bar')
@mock.patch.object(futures, 'APIClient')
@mock.patch.object(_model, "APIClient")
@mock.patch.object(_model.ModelFuture, "_set_model_exception")
def test_metadata(mock_spe, m_api, m_f_api, mock_ld):
    c = setup_client_mock(3, 7)
    m_api.return_value = m_f_api.return_value = c
    mf = _model.ModelFuture(3, 7)
    assert mf.metadata == 'bar'
    mock_ld.assert_called_once_with(3, 7, 'model_info.json', client=mock.ANY)


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


@mock.patch.object(_model, "load_table_from_outputs", autospec=True)
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


@mock.patch.object(_model, "load_table_from_outputs", autospec=True)
def test_train_data_exc_handling(mock_load_table):
    def poller(*args, **kwargs):
        return Response({'state': 'succeeded'})
    mock_client = mock.MagicMock()
    mock_client.scripts.get_containers_runs = poller
    mf = _model.ModelFuture(job_id=1, run_id=2, client=mock_client)
    mf._train_data_fname = 'placeholder.csv'

    # check we catch 404 error and raise some intelligible
    r = Response({'content': None, 'status_code': 404, 'reason': None})
    mock_load_table.side_effect = CivisAPIError(r)
    with pytest.raises(ValueError):
        mf.train_data


@mock.patch.object(_model, "load_estimator", return_value='spam')
@mock.patch.object(futures, 'APIClient')
@mock.patch.object(_model, "APIClient")
@mock.patch.object(_model.ModelFuture, "_set_model_exception", mock.Mock())
def test_estimator(m_api, m_f_api, mock_le):
    c = setup_client_mock(3, 7)
    m_api.return_value = m_f_api.return_value = c

    mf = _model.ModelFuture(3, 7)
    assert mock_le.call_count == 0, "Estimator retrieval is lazy."
    assert mf.estimator == 'spam'
    assert mock_le.call_count == 1

    assert mf.estimator == 'spam'
    assert mock_le.call_count == 1,\
        "The Estimator is only downloaded once and cached."


@mock.patch.object(_model, "load_dict", return_value='foo')
@mock.patch.object(futures, 'APIClient')
@mock.patch.object(_model, "APIClient")
@mock.patch.object(api.ModelFuture, "_set_model_exception")
def test_validation_metadata(mock_spe, m_api, m_f_api, mock_ld):
    c = setup_client_mock(3, 7)
    m_api.return_value = m_f_api.return_value = c
    mf = _model.ModelFuture(3, 7)
    assert mf.validation_metadata == 'foo'
    mock_ld.assert_called_once_with(3, 7, 'metrics.json', client=mock.ANY)


@mock.patch.object(_model, "load_dict",
                   mock.MagicMock(return_value={'metrics': 'foo'}))
@mock.patch.object(futures, 'APIClient')
@mock.patch.object(_model, "APIClient")
def test_metrics(m_api, m_f_api):
    c = setup_client_mock(3, 7)
    m_api.return_value = m_f_api.return_value = c
    mf = _model.ModelFuture(3, 7)

    assert mf.metrics == 'foo'
