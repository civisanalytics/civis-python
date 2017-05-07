"""Run CivisML jobs and retrieve the results
"""
from collections import namedtuple
import io
import json
import logging
import os
import six
import tempfile
import threading
import warnings
from concurrent import futures
from functools import wraps

try:
    import joblib
    HAS_JOBLIB = True
except ImportError:
    HAS_JOBLIB = False
try:
    from sklearn.base import BaseEstimator
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

from civis import APIClient, find_one
from civis.base import CivisAPIError, CivisJobFailure
import civis.io as cio
from civis.futures import CivisFuture
from civis.polling import _ResultPollingThread

__all__ = ['ModelFuture', 'ModelError', 'ModelPipeline']
log = logging.getLogger(__name__)

# sentinel value for default primary key value
SENTINEL = namedtuple('Sentinel', [])()

# Map training template to prediction template so that we
# always use a compatible version for predictions.
_PRED_TEMPLATES = {8387: 8388,  # v1.0
                   7020: 7021,  # v0.5
                   }


class ModelError(RuntimeError):
    def __init__(self, msg, estimator=None, metadata=None):
        self.metadata = metadata
        self.estimator = estimator
        super().__init__(msg)


def _check_fit_initiated(method):
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
                six.raise_from(self.exception(), None)
            else:
                raise
    return wrapper


def _stash_local_dataframe(df, client=None):
    """Store data in a temporary Civis File and return the file ID"""
    civis_fname = 'modelpipeline_data.csv'
    buf = io.BytesIO()
    txt = io.TextIOWrapper(buf, encoding='utf-8')
    df.to_csv(txt, encoding='utf-8', index=False)
    txt.flush()
    buf.seek(0)
    file_id = cio.file_to_civis(buf, name=civis_fname, client=client)

    return file_id


def _stash_local_file(csv_path, client=None):
    """Store data in a temporary Civis File and return the file ID"""
    civis_fname = 'modelpipeline_data.csv'
    with open(csv_path) as _fin:
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
            msg = ('Please provide valid train_run_id! Needs to be '
                   'integer corresponding to a training run ID '
                   'or one of "active" or "latest".')
            six.raise_from(ValueError(msg), exc)


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
    file_id = cio.file_id_from_run_output(filename, job_id, run_id,
                                          client=client, regex=True)
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
    metadata : dict, blocking
        The metadata associated with this modeling job
    metrics : dict, blocking
        Validation metrics from this job's training
    validation_metadata : dict, blocking
        Metadata from this modeling job's validation run
    train_metadata : dict, blocking
        Metadata from this modeling job's training run
        (will be identical to `metadata` if this is a training run)
    estimator : :class:`sklearn.pipeline.Pipeline`, blocking
        The fitted scikit-learn Pipeline resulting from this model run
    table : :class:`pandas.DataFrame`, blocking
        The table output from this modeling job: out-of-sample
        predictions on the training set for a training job, or
        a table of predictions for a prediction job.
        If the prediction job was split into multiple files
        (this happens automatically for large tables),
        this attribute will provide only predictions for the first file.
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
    civis.futures.CivisFuture
    concurrent.futures.Future
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
                          "Something went wrong with job execution.")

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
                    msg = 'There is no training data stored for this job!'
                    six.raise_from(ValueError(msg), err)
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
            fid = cio.file_id_from_run_output('model_info.json',
                                              self.train_job_id,
                                              self.train_run_id,
                                              client=self.client)
            self._train_metadata = cio.file_to_json(fid, client=self.client)
        return self._train_metadata


class ModelPipeline:
    """Interface for scikit-learn modeling in the Civis Platform

    Each ModelPipeline corresponds to a scikit-learn
    :class:`~sklearn.pipeline.Pipeline` which will run in Civis Platform.

    Parameters
    ----------
    model : string or Estimator
        Either the name of a pre-defined model
        (e.g. "sparse_logistic" or "gradient_boosting_classifier")
        or else a pre-existing Estimator object.
    dependent_variable : string or List[str]
        The dependent variable of the training dataset.
        For a multi-target problem, this should be a list of
        column names of dependent variables.
    primary_key : string, optional
        The unique ID (primary key) of the training dataset.
        This will be used to index the out-of-sample scores.
    parameters : dict, optional
        Specify parameters for the final stage estimator in a
        predefined model, e.g. ``{'C': 2}`` for a "sparse_logistic"
        model.
    cross_validation_parameters : dict, optional
        Cross validation parameter grid for learner parameters, e.g.
        ``{{'n_estimators': [100, 200, 500], 'learning_rate': [0.01, 0.1],
        'max_depth': [2, 3]}}``.
    model_name : string, optional
        The prefix of the Platform modeling jobs. It will have
        " Train" or " Predict" added to become the Script title.
    calibration : {None, "sigmoid", "isotonic"}
        If not None, calibrate output probabilities with the selected method.
        Valid only with classification models.
    excluded_columns : array, optional
        A list of columns which will be considered ineligible to be
        independent variables.
    client : :class:`~civis.APIClient`, optional
        If not provided, an :class:`~civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    cpu_requested : int, optional
        Number of CPU shares requested in the Civis Platform for
        training jobs. 1024 shares = 1 CPU.
    memory_requested : int, optional
        Memory requested from Civis Platform for training jobs, in MiB
    disk_requested : float, optional
        Disk space requested on Civis Platform for training jobs, in GB
    verbose : bool, optional
        If True, supply debug outputs in Platform logs and make
        prediction child jobs visible.

    Methods
    -------
    train()
        Train the model on data in Civis Platform; outputs
        :class:`~civis.ml.ModelFuture`
    predict()
        Make predictions on new data; outputs :class:`~civis.ml.ModelFuture`
    from_existing()
        Class method; use to create a :class:`~civis.ml.ModelPipeline`
        from an existing model training run

    Attributes
    ----------
    estimator : :class:`~sklearn.pipeline.Pipeline`
        The trained scikit-learn Pipeline
    train_result_ : :class:`~civis.ml.ModelFuture`
        :class:`~civis.ml.ModelFuture` encapsulating this model's training run
    state : str
        Status of the training job (non-blocking)

    Examples
    --------
    >>> from civis.ml import ModelPipeline
    >>> model = ModelPipeline('gradient_boosting_classifier', 'depvar',
    ...                       primary_key='voterbase_id')
    >>> train = model.train(table_name='schema.survey_data',
    ...                     fit_params={'sample_weight': 'survey_weight'},
    ...                     database_name='My Redshift Cluster',
    ...                     oos_scores='scratch.survey_depvar_oos_scores')
    >>> train
    <ModelFuture at 0x11be7ae10 state=queued>
    >>> train.running()
    True
    >>> train.done()
    False
    >>> df = train.table  # Read OOS scores from its Civis File. Blocking.
    >>> meta = train.metadata  # Metadata from training run
    >>> train.metrics['roc_auc']
    0.88425
    >>> pred = model.predict(table_name='schema.demographics_table ',
    ...                      database_name='My Redshift Cluster',
    ...                      output_table='schema.predicted_survey_response',
    ...                      if_exists='drop',
    ...                      n_jobs=50)
    >>> df_pred = pred.table  # Blocks until finished
    # Modify the parameters of the base estimator in a default model:
    >>> model = ModelPipeline('sparse_logistic', 'depvar',
    ...                       primary_key='voterbase_id',
    ...                       parameters={'C': 2})
    # Grid search over hyperparameters in the base estimator:
    >>> model = ModelPipeline('sparse_logistic', 'depvar',
    ...                       primary_key='voterbase_id',
    ...                       cross_validation_parameters={'C': [0.1, 1, 10]})

    See Also
    --------
    civis.ml.ModelFuture
    """
    # These are the v1.0 templates
    train_template_id = 8387
    predict_template_id = 8388

    def __init__(self, model, dependent_variable, *,
                 primary_key=None, parameters=None,
                 cross_validation_parameters=None, model_name=None,
                 calibration=None, excluded_columns=None, client=None,
                 cpu_requested=None, memory_requested=None,
                 disk_requested=None, verbose=False):
        self.model = model
        self._input_model = model  # In case we need to modify the input
        if isinstance(dependent_variable, str):
            # Standardize the dependent variable as a list.
            dependent_variable = [dependent_variable]
        self.dependent_variable = dependent_variable

        # optional but common parameters
        self.primary_key = primary_key
        self.parameters = parameters or {}
        self.cv_params = cross_validation_parameters or {}
        self.model_name = model_name  # None lets Platform use template name
        self.excluded_columns = excluded_columns
        self.calibration = calibration
        self.job_resources = {'REQUIRED_CPU': cpu_requested,
                              'REQUIRED_MEMORY': memory_requested,
                              'REQUIRED_DISK_SPACE': disk_requested}
        self.verbose = verbose

        if client is None:
            client = APIClient(resources='all')
        self._client = client
        self.train_result_ = None

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        del state['_client']
        return state

    def __setstate__(self, state: dict):
        self.__dict__ = state
        self._client = APIClient(resources='all')

    @classmethod
    def from_existing(cls, train_job_id, train_run_id='latest', client=None):
        """Create a :class:`ModelPipeline` object from existing model IDs

        Parameters
        ----------
        train_job_id : int
            The ID of the CivisML job in the Civis Platform
        train_run_id : int or string, optional
            Location of the model run, either

            * an explicit run ID,
            * "latest" : The most recent run
            * "active" : The run designated by the training job's
              "active build" parameter
        client : :class:`~civis.APIClient`, optional
            If not provided, an :class:`~civis.APIClient` object will be
            created from the :envvar:`CIVIS_API_KEY`.

        Returns
        -------
        :class:`~civis.ml.ModelPipeline`
            A :class:`~civis.ml.ModelPipeline` which refers to
            a previously-trained model

        Examples
        --------
        >>> from civis.ml import ModelPipeline
        >>> model = ModelPipeline.from_existing(job_id)
        >>> model.train_result_.metrics['roc_auc']
        0.843
        """
        if client is None:
            client = APIClient(resources='all')
        train_run_id = _decode_train_run(train_job_id, train_run_id, client)
        try:
            fut = ModelFuture(train_job_id, train_run_id, client=client)
            container = client.scripts.get_containers(train_job_id)
        except CivisAPIError as api_err:
            if api_err.status_code == 404:
                msg = ('There is no Civis Platform job with '
                       'script ID {} and run ID {}!'.format(train_job_id,
                                                            train_run_id))
                six.raise_from(ValueError(msg), api_err)
            raise

        args = container.arguments

        # Older templates used "WORKFLOW" instead of "MODEL"
        model = args.get('MODEL', args.get('WORKFLOW'))
        dependent_variable = args['TARGET_COLUMN'].split()
        primary_key = args.get('PRIMARY_KEY')
        parameters = json.loads(args.get('PARAMS', {}))
        cross_validation_parameters = json.loads(args.get('CVPARAMS', {}))
        calibration = args.get('CALIBRATION')
        excluded_columns = args.get('EXCLUDE_COLS', None)
        if excluded_columns:
            excluded_columns = excluded_columns.split()
        cpu_requested = args.get('REQUIRED_CPU')
        memory_requested = args.get('REQUIRED_MEMORY')
        disk_requested = args.get('REQUIRED_DISK_SPACE')
        name = container.name
        if name.endswith(' Train'):
            # Strip object-applied suffix
            name = name[:-len(' Train')]

        klass = cls(model=model,
                    dependent_variable=dependent_variable,
                    primary_key=primary_key,
                    model_name=name,
                    parameters=parameters,
                    cross_validation_parameters=cross_validation_parameters,
                    calibration=calibration,
                    excluded_columns=excluded_columns,
                    client=client,
                    cpu_requested=cpu_requested,
                    disk_requested=disk_requested,
                    memory_requested=memory_requested,
                    verbose=args.get('DEBUG', False))
        klass.train_result_ = fut

        # Set prediction template corresponding to training template
        template_id = int(container['from_template_id'])
        klass.predict_template_id = _PRED_TEMPLATES.get(template_id)

        return klass

    def train(self, df=None, csv_path=None, table_name=None,
              database_name=None, file_id=None,
              sql_where=None, sql_limit=None, oos_scores=None,
              oos_scores_db=None, if_exists='fail', fit_params=None,
              polling_interval=None):
        """Start a Civis Platform job to train your model

        Provide input through one of
        a :class:`~pandas.DataFrame` (``df``),
        a local CSV (``csv_path``),
        a Civis Table (``table_name`` and ``database_name``), or
        a Civis File containing a CSV (``file_id``).

        Model outputs will always contain out-of-sample scores
        (accessible through :attr:`ModelFuture.table` on this function's
        output), and you may chose to store these out-of-sample scores
        in a Civis Table with the ``oos_scores``, ``oos_scores_db``,
        and ``if_exists`` parameters.

        Parameters
        ----------
        df : pd.DataFrame, optional
            A :class:`~pandas.DataFrame` of training data.
            The :class:`~pandas.DataFrame` will be uploaded to a Civis file so
            that CivisML can access it.
            Note that the index of the :class:`~pandas.DataFrame` will be
            ignored -- use ``df.reset_index()`` if you want your
            index column to be included with the data passed to CivisML.
        csv_path : str, optional
            The location of a CSV of data on the local disk.
            It will be uploaded to a Civis file.
        table_name : str, optional
            The qualified name of the table containing the training set from
            which to build the model.
        database_name : str, optional
            Name of the database holding the training set table used to
            build the model. E.g., 'My Cluster Name'.
        file_id : int, optional
            If the training data are stored in a Civis file,
            provide the integer file ID.
        sql_where : str, optional
            A SQL WHERE clause used to scope the rows of the training set
            (used for table input only)
        sql_limit : int, optional
            SQL LIMIT clause for querying the training set
            (used for table input only)
        oos_scores : str, optional
            If provided, store out-of-sample predictions on
            training set data to this Redshift "schema.tablename".
        oos_scores_db : str, optional
            If not provided, store OOS predictions in the same database
            which holds the training data.
        if_exists : {'fail', 'append', 'drop', 'truncate'}
            Action to take if the out-of-sample prediction table
            already exists.
        fit_params: Dict[str, str]
            Mapping from parameter names in the model's ``fit`` method
            to the column names which hold the data, e.g.
            ``{'sample_weight': 'survey_weight_column'}``.
        polling_interval : float, optional
            Check for job completion every this number of seconds.
            Do not set if using the notifications endpoint.

        Returns
        -------
        :class:`~civis.ml.ModelFuture`
        """
        if ((table_name is None or database_name is None) and
                file_id is None and df is None and csv_path is None):
            raise ValueError('Provide a source of data.')
        if sum((bool(table_name and database_name),
                bool(file_id), df is not None, csv_path is not None)) > 1:
            raise ValueError('Provide a single source of data.')

        if df is not None:
            file_id = _stash_local_dataframe(df, client=self._client)
        elif csv_path:
            file_id = _stash_local_file(csv_path, client=self._client)

        train_args = {'TARGET_COLUMN': ' '.join(self.dependent_variable),
                      'PRIMARY_KEY': self.primary_key,
                      'PARAMS': json.dumps(self.parameters),
                      'CVPARAMS': json.dumps(self.cv_params),
                      'CALIBRATION': self.calibration,
                      'IF_EXISTS': if_exists}
        if oos_scores:
            train_args['OOSTABLE'] = oos_scores
        if oos_scores_db:
            oos_db_id = self._client.get_database_id(oos_scores_db)
            train_args['OOSDB'] = {'database': oos_db_id}
        if sql_where:
            train_args['WHERESQL'] = sql_where
        if sql_limit:
            train_args['LIMITSQL'] = sql_limit
        if self.excluded_columns:
            train_args['EXCLUDE_COLS'] = ' '.join(self.excluded_columns)
        if fit_params:
            train_args['FIT_PARAMS'] = json.dumps(fit_params)

        if (HAS_SKLEARN and HAS_JOBLIB and
                isinstance(self.model, BaseEstimator)):
            with tempfile.TemporaryDirectory() as tempdir:
                fout = os.path.join(tempdir, 'estimator.pkl')
                joblib.dump(self.model, fout, compress=3)
                with open(fout, 'rb') as _fout:
                    n = self.model_name if self.model_name else "CivisML"
                    estimator_file_id = cio.file_to_civis(
                        _fout, 'Estimator for ' + n, client=self._client)
                self._input_model = self.model  # Keep the estimator
            self.model = str(estimator_file_id)
        train_args['MODEL'] = self.model

        name = self.model_name + ' Train' if self.model_name else None
        # Clear the existing training result so we can make a new one.
        self.train_result_ = None

        result, container, run = self._create_custom_run(
              self.train_template_id,
              job_name=name,
              table_name=table_name,
              database_name=database_name,
              file_id=file_id,
              args=train_args,
              resources=self.job_resources,
              polling_interval=polling_interval)

        self.train_result_ = result

        return result

    def _create_custom_run(self, template_id, job_name=None, table_name=None,
                           database_name=None, file_id=None, args=None,
                           resources=None, polling_interval=None):

        script_arguments = {'TABLE_NAME': table_name,
                            'CIVIS_FILE_ID': file_id,
                            'DEBUG': self.verbose}
        if database_name:
            db_id = self._client.get_database_id(database_name)
            script_arguments['DB'] = {'database': db_id}
        resources = resources or {}
        for key, value in resources.items():
            if value:
                # Default resources are set on the template. Only
                # modify via arguments if users give a non-default value.
                script_arguments[key] = value

        script_arguments.update(args or {})

        container = self._client.scripts.post_custom(
            from_template_id=template_id,
            name=job_name,
            arguments=script_arguments)
        log.info('Created custom script %s.', container.id)

        run = self._client.scripts.post_custom_runs(container.id)
        log.debug('Started job %s, run %s.', container.id, run.id)

        train_kwargs = {}
        if self.train_result_ is not None:
            train_kwargs = {'train_job_id': self.train_result_.job_id,
                            'train_run_id': self.train_result_.run_id}
        fut = ModelFuture(
              container.id,
              run.id,
              client=self._client,
              polling_interval=polling_interval,
              poll_on_creation=False,
              **train_kwargs)

        return fut, container, run

    @property
    @_check_fit_initiated
    def state(self) -> str:
        return self.train_result_.state

    @property
    @_check_fit_initiated
    def estimator(self):
        return self.train_result_.estimator

    @_check_fit_initiated
    def predict(self, df=None, csv_path=None,
                table_name=None, database_name=None,
                manifest=None, file_id=None, sql_where=None, sql_limit=None,
                primary_key=SENTINEL, output_table=None, output_db=None,
                if_exists='fail', n_jobs=None, polling_interval=None):
        """Make predictions on a trained model

        Provide input through one of
        a :class:`~pandas.DataFrame` (``df``),
        a local CSV (``csv_path``),
        a Civis Table (``table_name`` and ``database_name``),
        a Civis File containing a CSV (``file_id``), or
        a Civis File containing a manifest file (``manifest``).

        A "manifest file" is JSON which specifies the location of
        many shards of the data to be used for prediction.
        A manifest file is the output of a Civis
        export job with ``force_multifile=True`` set,
        e.g. from :func:`civis.io.civis_to_multifile_csv`.
        Large Civis Tables (provided using ``table_name``)
        will automatically be exported to manifest files.

        Prediction outputs will always be stored as gzipped
        CSVs in one or more Civis Files. You can find a list of
        File ID numbers for output files at the "output_file_ids"
        key in the metadata returned by the prediction job.
        Provide an ``output_table`` (and optionally an ``output_db``,
        if it's different from ``database_name``) to copy these
        predictions into a Civis Table.

        Parameters
        ----------
        df : pd.DataFrame, optional
            A :class:`~pandas.DataFrame` of data for prediction.
            The :class:`~pandas.DataFrame` will be uploaded to a Civis file so
            that CivisML can access it.
            Note that the index of the :class:`~pandas.DataFrame` will be
            ignored -- use ``df.reset_index()`` if you want your
            index column to be included with the data passed to CivisML.
        csv_path : str, optional
            The location of a CSV of data on the local disk.
            It will be uploaded to a Civis file.
        table_name : str, optional
            The qualified name of the table containing your data
        database_name : str, optional
            Name of the database holding the
            data, e.g., 'My Redshift Cluster'.
        manifest : int, optional
            ID for a manifest file stored as a Civis file.
            (Note: if the manifest is not a Civis Platform-specific manifest,
            like the one returned from :func:`civis.io.civis_to_multfile_csv`,
            this must be used in conjunction with table_name and database_name
            due to the need for column discovery via Redshift.)
        file_id : int, optional
            If the data are a CSV stored in a Civis file,
            provide the integer file ID.
        sql_where : str, optional
            A SQL WHERE clause used to scope the rows to be predicted
        sql_limit : int, optional
            SQL LIMIT clause to restrict the size of the prediction set
        primary_key : str, optional
            Primary key of the prediction table. Defaults to
            the primary key of the training data. Use ``None`` to
            indicate that the prediction data don't have a
            primary key column.
        output_table: str, optional
            The table in which to put the predictions.
        output_db : str, optional
            Database of the output table. Defaults to the database
            of the input table.
        if_exists : {'fail', 'append', 'drop', 'truncate'}
            Action to take if the prediction table already exists.
        n_jobs : int, optional
            Number of concurrent Platform jobs to use
            for multi-file / large table prediction.
        polling_interval : float, optional
            Check for job completion every this number of seconds.
            Do not set if using the notifications endpoint.

        Returns
        -------
        :class:`~civis.ml.ModelFuture`
        """
        self.train_result_.result()  # Blocks and raises training errors

        if ((table_name is None or database_name is None) and
                file_id is None and df is None and csv_path is None and
                manifest is None):
            raise ValueError('Provide a source of data.')
        if sum((bool(table_name and database_name) or (manifest is not None),
                bool(file_id), df is not None, csv_path is not None)) > 1:
            raise ValueError('Provide a single source of data.')

        if df is not None:
            file_id = _stash_local_dataframe(df, client=self._client)
        elif csv_path:
            file_id = _stash_local_file(csv_path, client=self._client)

        if primary_key is SENTINEL:
            primary_key = self.primary_key

        predict_args = {'TRAIN_JOB': self.train_result_.job_id,
                        'TRAIN_RUN': self.train_result_.run_id,
                        'PRIMARY_KEY': primary_key,
                        'IF_EXISTS': if_exists}
        if output_table:
            predict_args['OUTPUT_TABLE'] = output_table
        if output_db:
            output_db_id = self._client.get_database_id(output_db)
            predict_args['OUTPUT_DB'] = {'database': output_db_id}
        if manifest:
            predict_args['MANIFEST'] = manifest
        if sql_where:
            predict_args['WHERESQL'] = sql_where
        if sql_limit:
            predict_args['LIMITSQL'] = sql_limit
        if n_jobs:
            predict_args['N_JOBS'] = n_jobs

        # If n_jobs is 1, we'll do computation in the leader job.
        # Otherwise, rely on the default resources in the template.
        if n_jobs == 1:
            resources = {'REQUIRED_CPU': 1024,
                         'REQUIRED_MEMORY': 3000,
                         'REQUIRED_DISK_SPACE': 30}
        else:
            resources = None

        name = self.model_name + ' Predict' if self.model_name else None
        result, container, run = self._create_custom_run(
            self.predict_template_id,
            job_name=name,
            table_name=table_name,
            database_name=database_name,
            file_id=file_id,
            args=predict_args,
            resources=resources,
            polling_interval=polling_interval)

        return result
