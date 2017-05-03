"""Run CivisML jobs and retrieve the results
"""
import io
import os
import tempfile
import threading
import warnings
from concurrent import futures
from functools import wraps

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

from civis import APIClient, find_one
from civis.base import CivisAPIError, CivisJobFailure
import civis.io as cio
from civis.futures import CivisFuture
from civis.polling import _ResultPollingThread

__all__ = ['ModelFuture', 'ModelError']


class ModelError(RuntimeError):
    def __init__(self, msg, estimator=None, metadata=None):
        self.metadata = metadata
        self.estimator = estimator
        super().__init__(msg)


def _check_is_fit(method):
    """Makes sure that the ModelPipeline's been trained"""
    @wraps(method)
    def wrapper(*args, **kwargs):
        self = args[0]
        if not self.train_result_:
            raise ValueError("This model hasn't been trained yet.")
        return method(*args, **kwargs)
    return wrapper


def _block_and_handle_missing(method):
    """For ModelFuture file-retrieving property methods.
    Block until completion and attempt to retrieve result.
    Raise exception only if the result isn't found.
    """
    @wraps(method)
    def wrapper(self):
        futures.wait((self,))  # Block until done
        try:
            return method(self)
        except FileNotFoundError:
            # We get here if the modeling job failed to produce
            # any output and we don't have metadata.
            if self.exception():
                raise self.exception() from None
            else:
                raise
    return wrapper


def _stash_local_data(X, client=None):
    """Store data in a temporary Civis File and return the file ID
    """
    civis_fname = 'modelpipeline_data.csv'
    if HAS_PANDAS and isinstance(X, pd.DataFrame):
        buf = io.BytesIO()
        txt = io.TextIOWrapper(buf, encoding='utf-8')
        X.to_csv(txt, encoding='utf-8', index=False)
        txt.flush()
        buf.seek(0)
        file_id = cio.file_to_civis(buf, name=civis_fname, client=client)
    else:
        with open(X) as _fin:
            file_id = cio.file_to_civis(_fin, name=civis_fname, client=client)

    return file_id


def _decode_train_run(train_job_id, train_run_id, client):
    """Determine correct run ID for use for a given training job ID"""
    try:
        return int(train_run_id)
    except ValueError:
        container = client.scripts.get_containers(train_job_id)
        if train_run_id == 'active':
            train_run_id = container.arguments.get('ACTIVE_BUILD', find_one(
                container.params, name='ACTIVE_BUILD'))['default']

        if train_run_id == 'latest':
            return container.last_run.id

        try:
            return int(train_run_id)
        except Exception as exc:
            raise ValueError('Please provide valid train_run_id! Needs to be '
                             'integer corresponding to a training run ID '
                             'or one of "active" or "latest".') from exc


def _retrieve_file(fname, job_id, run_id, local_dir, client=None):
    """Download a Civis file using a reference on a previous run"""
    file_id = cio.file_id_from_run_output(fname, job_id, run_id, client=client)
    fpath = os.path.join(local_dir, fname)
    os.makedirs(os.path.dirname(fpath), exist_ok=True)
    with open(fpath, 'wb') as down_file:
        cio.civis_to_file(file_id, down_file, client=client)
    return fpath


def _load_table_from_outputs(job_id, run_id, filename, client=None,
                             **table_kwargs):
    """Load a table from a run output directly into a ``DataFrame``"""
    client = APIClient(resources='all') if client is None else client
    file_id = cio.file_id_from_run_output(filename, job_id, run_id, client,
                                          regex=True)
    return cio.file_to_dataframe(file_id, client=client, **table_kwargs)


def _load_estimator(job_id, run_id, filename='estimator.pkl', client=None):
    """Load a joblib-serialized Estimator from run outputs"""
    if not HAS_JOBLIB:
        raise ImportError('Install joblib to download models '
                          'from Civis Platform.')
    with tempfile.TemporaryDirectory() as tempdir:
        path = _retrieve_file(filename, job_id, run_id, tempdir, client=client)
        return joblib.load(path)


class ModelFuture(CivisFuture):
    """Encapsulates asynchronous execution of a CivisML job

    This object knows where to find modeling outputs
    from CivisML jobs. All data attributes are
    lazily retrieved and block on job completion.
    This object can be pickled.

    Parameters
    ----------
    job_id : int
        ID of the modeling job
    run_id : int
        ID of the modeling run
    train_job_id : int, optional
        If not provided, this object is assumed to encapsulate a training
        job, and ``train_job_id`` will equal ``job_id``.
    train_run_id : int, optional
        If not provided, this object is assumed to encapsulate a training
        run, and ``train_run_id`` will equal ``run_id``.
    polling_interval : int or float, optional
        The number of seconds between API requests to check whether a result
        is ready. The default intelligently switches between a short
        interval if ``pubnub`` is not available and a long interval
        for ``pubnub`` backup if that library is installed.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    poll_on_creation : bool, optional
        If ``True`` (the default), it will poll upon calling ``result()`` the
        first time. If ``False``, it will wait the number of seconds specified
        in `polling_interval` from object creation before polling.

    Attributes
    ----------
    [Accessing these attributes blocks until the Platform run completes]
    metadata : dict
        The metadata associated with this modeling job
    metrics : dict
        Validation metrics from this job's training
    validation_metadata : dict
        Metadata from this modeling job's validation run
    train_metadata : dict
        Metadata from this modeling job's training run
        (will be identical to `metadata` if this is a training run)
    estimator : Pipeline
        The fitted scikit-learn Pipeline resulting from this model run
    table : pandas.DataFrame
        The table output from this modeling job: out-of-sample
        predictions on the training set for a training job, or
        a table of predictions for a prediction job.
        If the prediction job was split into multiple files
        (this happens automatically for large tables),
        this attribute will provide only predictions for the first file.

    [These attributes are non-blocking]
    state : str
        The current state of the Civis Platform run
    job_id : int
    run_id : int
    train_job_id : int
        Container ID for the training job -- identical to ``job_id``
        if this is a training job.
    train_run_id : int
        As ``train_job_id`` but for runs
    is_training : bool
        True if this ``ModelFuture`` corresponds to a train-validate job.

    Methods
    -------
    cancel()
        Cancels the corresponding Platform job before completion
    succeeded()
        (Non-blocking) Is the job a success?
    failed()
        (Non-blocking) Did the job fail?
    cancelled()
        (Non-blocking) Was the job cancelled?
    running()
        (Non-blocking) Is the job still running?
    done()
        (Non-blocking) Is the job finished?
    result()
        (Blocking) Return the final status of the Civis Platform job.

    See Also
    --------
    :class:`~civis.futures.CivisFuture`
    :class:`~concurrent.futures.Future`
    """
    def __init__(self, job_id, run_id, train_job_id=None, train_run_id=None,
                 polling_interval=None, client=None, poll_on_creation=True):
        if client is None:
            client = APIClient(resources='all')
        super().__init__(client.scripts.get_containers_runs,
                         [job_id, run_id],
                         polling_interval=polling_interval,
                         client=client,
                         poll_on_creation=poll_on_creation)
        if train_job_id and train_run_id:
            self.is_training = False
            self.train_job_id = train_job_id
            self.train_run_id = train_run_id
        else:
            self.is_training = True
            self.train_job_id = self.job_id
            self.train_run_id = self.run_id
        self._metadata, self._val_metadata = None, None
        self._train_data, self._train_data_fname = None, None
        self._train_metadata = None
        self._table, self._estimator = None, None
        self.add_done_callback(self._set_model_exception)

    @property
    def job_id(self):
        return self.poller_args[0]

    @property
    def run_id(self):
        return self.poller_args[1]

    @staticmethod
    def _set_model_exception(fut):
        """Callback: On job completion, check the metadata.
        If it indicates an exception, replace the generic
        ``CivisJobFailure`` by a more informative ``ModelError``.
        """
        try:
            meta = fut.metadata
            if fut.is_training and meta['run']['status'] == 'succeeded':
                # if training job and job succeeded, check validation job
                meta = fut.validation_metadata
            if (meta['run']['status'] == 'exception' and
                    not isinstance(fut._exception, ModelError)):
                # `set_exception` invokes callbacks, so make sure
                # we haven't already set a `ModelError` to avoid
                # infinite recursion.
                try:
                    # This will fail if the user doesn't have joblib installed
                    est = fut.estimator
                except Exception:  # NOQA
                    est = None
                fut.set_exception(
                      ModelError('Model run failed with stack trace:\n'
                                 '{}'.format(meta['run']['stack_trace']),
                                 est, meta))
        except (FileNotFoundError, CivisJobFailure):
            # If there's no metadata file
            # (we get FileNotFound or CivisJobFailure),
            # then there's no improvements to make on
            # any existing exception.
            pass
        except KeyError:
            # KeyErrors always represent a bug in the modeling code,
            # but showing the resulting KeyError can be confusing and
            # mask the real error.
            warnings.warn("Received malformed metadata from Civis Platform. "
                          "Something went wrong with execution. "
                          "Please report this error.")

    def cancel(self):
        """Submit a request to cancel the container/script/run.

        .. note:: If this object represents a prediction run,
                  ``cancel`` will only cancel the parent job.
                  Child jobs will remain active.

        Returns
        -------
        bool
            Whether or not the run is in a cancelled state.
        """
        with self._condition:
            if self.cancelled():
                return True
            elif not self.done():
                # Cancel the job and store the result of the cancellation in
                # the "finished result" attribute, `_result`.
                self._result = self.client.scripts.post_cancel(self.job_id)
                for waiter in self._waiters:
                    waiter.add_cancelled(self)
                self._condition.notify_all()
                self._invoke_callbacks()
                self.cleanup()
                return self.cancelled()
            return False

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_polling_thread']
        del state['client']
        del state['poller']
        del state['_condition']
        if '_pubnub' in state:
            state['_pubnub'] = True  # Replace with a boolean flag
        state['_done_callbacks'] = []
        state['_self_polling_executor'] = None

        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self.client = APIClient(resources='all')
        if getattr(self, '_pubnub', None) is True:
            # Re-subscribe to notifications channel
            self._pubnub = self._subscribe(*self._pubnub_config())
        self._polling_thread = _ResultPollingThread(self._check_result, (),
                                                    self.polling_interval)
        self.poller = self.client.scripts.get_containers_runs
        self._condition = threading.Condition()
        self.add_done_callback(self._set_model_exception)

    @property
    def state(self):
        state = self._civis_state
        if state == 'succeeded':
            state = self.metadata['run']['status']
        return state

    @property
    def table_fname(self):
        return 'predictions.csv'

    @property
    def metadata_fname(self):
        return 'model_info.json'

    @property
    def train_data_fname(self):
        if self._train_data_fname is None:
            self._train_data_fname = os.path.basename(self.training_metadata
                                                      .get('run')
                                                      .get('configuration')
                                                      .get('data')
                                                      .get('location'))
        return self._train_data_fname

    @property
    def train_data(self):
        if self._train_data is None:
            try:
                self._train_data = _load_table_from_outputs(
                    self.train_job_id,
                    self.train_run_id,
                    self.train_data_fname,
                    client=self.client)
            except CivisAPIError as err:
                if err.status_code == 404:
                    raise ValueError('There is no training data stored for '
                                     'this job!') from err
                else:
                    raise

        return self._train_data

    @property
    def table(self):
        self.result()  # Block and raise errors if any
        if self._table is None:
            if self.is_training:
                # Training jobs only have one output table, the OOS scores
                self._table = _load_table_from_outputs(self.job_id,
                                                       self.run_id,
                                                       self.table_fname,
                                                       index_col=0,
                                                       client=self.client)
            else:
                # Prediction jobs may have many output tables.
                output_ids = self.metadata['output_file_ids']
                if len(output_ids) > 1:
                    print('This job output {} files. Retrieving only the '
                          'first. Find the full list at `metadata'
                          '["output_file_ids"]`.'.format(len(output_ids)))
                self._table = cio.file_to_dataframe(output_ids[0],
                                                    client=self.client,
                                                    index_col=0)
        return self._table

    @property
    @_block_and_handle_missing
    def metadata(self):
        if self._metadata is None:
            fid = cio.file_id_from_run_output('model_info.json', self.job_id,
                                              self.run_id, client=self.client)
            self._metadata = cio.file_to_json(fid, client=self.client)
        return self._metadata

    @property
    @_block_and_handle_missing
    def estimator(self):
        if self._estimator is None:
            self._estimator = _load_estimator(self.train_job_id,
                                              self.train_run_id,
                                              client=self.client)
        return self._estimator

    @property
    @_block_and_handle_missing
    def validation_metadata(self):
        if self._val_metadata is None:
            fid = cio.file_id_from_run_output('metrics.json', self.job_id,
                                              self.run_id, client=self.client)
            self._val_metadata = cio.file_to_json(fid, client=self.client)
        return self._val_metadata

    @property
    def metrics(self):
        return self.validation_metadata['metrics']

    @property
    @_block_and_handle_missing
    def training_metadata(self):
        if self._train_metadata is None:
            fid = cio.file_id_from_run_output('model_info.json', self.job_id,
                                              self.run_id, client=self.client)
            self._train_metadata = cio.file_to_json(fid, client=self.client)
        return self._train_metadata
