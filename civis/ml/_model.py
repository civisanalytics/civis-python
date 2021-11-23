"""Run CivisML jobs and retrieve the results
"""
import builtins
from builtins import super
import collections
from functools import lru_cache
import io
import json
import logging
import os
import re
import shutil
import tempfile
import threading
import warnings
from concurrent import futures
from functools import wraps

import joblib
try:
    from sklearn.base import BaseEstimator
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

from civis import APIClient, find, find_one
from civis._utils import camel_to_snake
from civis.base import CivisAPIError, CivisJobFailure
import civis.io as cio
from civis.futures import ContainerFuture
from civis.response import Response


__all__ = ['ModelFuture', 'ModelError', 'ModelPipeline']
log = logging.getLogger(__name__)

# sentinel value for default primary key value
SENTINEL = collections.namedtuple('Sentinel', [])()


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
                raise self.exception() from None
            else:
                raise
    return wrapper


def _stash_local_dataframe(df, template_id, client=None):
    """Store data in a temporary Civis File and return the file ID"""
    # Standard dataframe indexes do not have a "levels" attribute,
    # but multiindexes do. Checking for this attribute means we don't
    # need to import pandas to do error handling here.
    if getattr(getattr(df, "index", None), "levels", None) is not None:
        raise TypeError("CivisML does not support multi-indexed data frames. "
                        "Try calling `.reset_index` on your data to convert "
                        "it into a CivisML-friendly format.")
    try:
        if template_id > 9969:
            return _stash_dataframe_as_feather(df, client)
        else:
            return _stash_dataframe_as_csv(df, client)
    except (ImportError, AttributeError) as exc:
        if (df.dtypes == 'category').any():
            # The original exception should tell users if they need
            # to upgrade pandas (an AttributeError)
            # # or if they need to install "feather-format" (ImportError).
            raise ValueError(
                'Categorical columns can only be handled with pandas '
                'version >= 0.20 and `feather-format` installed.') from exc
        return _stash_dataframe_as_csv(df, client)


def _stash_dataframe_as_feather(df, client):
    civis_fname = 'modelpipeline_data.feather'
    with tempfile.TemporaryDirectory() as tdir:
        path = os.path.join(tdir, civis_fname)
        df.to_feather(path)
        file_id = cio.file_to_civis(path, name=civis_fname, client=client)
    return file_id


def _stash_dataframe_as_csv(df, client):
    civis_fname = 'modelpipeline_data.csv'
    txt = io.StringIO()
    df.to_csv(txt, encoding='utf-8', index=False)
    txt.flush()
    txt.seek(0)
    file_id = cio.file_to_civis(txt, name=civis_fname, client=client)

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
        container = client.scripts.get_containers(int(train_job_id))
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
            raise ValueError(msg) from exc


def _retrieve_file(fname, job_id, run_id, local_dir, client=None):
    """Download a Civis file using a reference on a previous run"""
    file_id = cio.file_id_from_run_output(fname, job_id, run_id, client=client)
    fpath = os.path.join(local_dir, fname)
    # fname may contain a path
    output_dir = os.path.dirname(fpath)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with open(fpath, 'wb') as down_file:
        cio.civis_to_file(file_id, down_file, client=client)
    return fpath


def _load_table_from_outputs(job_id, run_id, filename, client=None,
                             **table_kwargs):
    """Load a table from a run output directly into a ``DataFrame``"""
    client = APIClient() if client is None else client
    file_id = cio.file_id_from_run_output(filename, job_id, run_id,
                                          client=client, regex=True)
    return cio.file_to_dataframe(file_id, client=client, **table_kwargs)


def _load_estimator(job_id, run_id, filename='estimator.pkl', client=None):
    """Load a joblib-serialized Estimator from run outputs"""
    try:
        tempdir = tempfile.mkdtemp()
        path = _retrieve_file(filename, job_id, run_id, tempdir, client=client)
        obj = joblib.load(path)
    finally:
        shutil.rmtree(tempdir)
    return obj


def _parse_warning(warn_str):
    """Reverse-engineer a warning string

    Parameters
    ----------
    warn_str : string

    Returns
    -------
    (str, Warning, str, int)
        message, category, filename, lineno
    """
    tokens = warn_str.rstrip('\n').split(" ")

    # The first token is
    # "[filename]:[lineno]:"
    filename, lineno, _ = tokens[0].split(':')

    # The second token is
    # "[category name]:"
    category = getattr(builtins, tokens[1][:-1], RuntimeWarning)

    message = " ".join(tokens[2:])

    return message, category, filename, int(lineno)


def _show_civisml_warnings(warn_list):
    """Re-raise warnings recorded during a CivisML run

    Parameters
    ----------
    warn_list : list of str
        A list of warnings generated during a CivisML run
    """
    for warn_str in warn_list:
        try:
            warnings.warn_explicit(*_parse_warning(warn_str))
        except Exception:  # NOQA
            warn_str = "Remote warning from CivisML:\n" + warn_str
            warnings.warn(warn_str, RuntimeWarning)


def _get_job_type_version(alias):
    """Derive the job type and version from the given alias.

    Parameters
    ----------
    alias : str
        CivisML alias

    Returns
    -------
    str
        Job type, one of {training, prediction, registration}.
    str
        CivisML version, e.g., "v2.2".
    """
    # A version-less alias for production, e.g., "civis-civisml-training"
    match_production = re.search(r'\Acivis-civisml-(\w+)\Z', alias)
    # A versioned alias, e.g., "civis-civisml-training-v2-3"
    match_v = re.search(r'\Acivis-civisml-(\w+)-v(\d+)-(\d+)\Z', alias)
    # A special-version alias, e.g., "civis-civisml-training-dev"
    match_special = re.search(r'\Acivis-civisml-(\w+)-(\S+[^-])\Z', alias)

    if match_production:
        job_type = match_production.group(1)
        version = None
    elif match_v:
        job_type = match_v.group(1)
        version = 'v%s.%s' % match_v.group(2, 3)
    elif match_special:
        job_type = match_special.group(1)
        version = match_special.group(2)
    else:
        msg = ('Unable to parse the job type and version '
               'from the CivisML alias "%r"')
        raise ValueError(msg % alias)

    return job_type, version


@lru_cache()
def _get_template_ids_all_versions(client):
    """Get templates IDs for all accessible CivisML versions.

    Parameters
    ----------
    client : APIClient
        Civis API client object

    Returns
    -------
    Dict[str, Dict[str, int]]
        Mapping between versions (e.g., "v2.2") and template IDs for the given
        version (e.g., {'training': 1, 'prediction': 2, 'registration': 3}).
    """
    template_alias_objects = client.aliases.list(
        object_type='template_script', iterator=True
    )
    civisml_template_alias_objects = find(
        template_alias_objects,
        alias=lambda alias: alias.startswith('civis-civisml-')
    )
    ids = collections.defaultdict(
        lambda: {'training': None, 'prediction': None, 'registration': None}
    )
    for alias_obj in civisml_template_alias_objects:
        try:
            job_type, version = _get_job_type_version(alias_obj.alias)
        except ValueError:
            msg = (
                '%r looks like a CivisML alias for the prefix "civis-civisml-"'
                ', but it is impossible to parse its job type and version'
            )
            log.debug(msg % alias_obj)
            continue
        ids[version][job_type] = alias_obj.object_id
    if not ids:
        r = Response({'status_code': 404,
                      'reason': 'No CivisML template IDs are accessible.',
                      'content': None})
        raise CivisAPIError(r)
    # Disallow a defaultdict in the output, so that a non-existent CivisML
    # version as key should trigger a KeyError.
    ids = dict(ids)
    return ids


def _get_template_ids(civisml_version, client):
    """Get template IDs for the specified CivisML version.

    Parameters
    ----------
    civisml_version : str
        CivisML version
    client : APIClient
        Civis API client object

    Returns
    -------
    int
        Template ID for training
    int
        Template ID for prediction
    int
        Template ID for pre-trained model registration
    """
    template_ids_all_versions = _get_template_ids_all_versions(client)
    try:
        ids = template_ids_all_versions[civisml_version]
    except KeyError:
        msg = (
            '"{civisml_version}" is an invalid CivisML version. '
            'Either this version does not exist, or you do not have access '
            'to this version. '
            'Versions accessible to you are {{{accessible_versions}}}, '
            'as well as `None` for the latest production version.'
        ).format(
            civisml_version=civisml_version,
            accessible_versions=', '.join(
                '"%s"' % v
                # Don't include None, or else it would crash sorted()
                for v in sorted(
                    v for v in template_ids_all_versions.keys() if v
                )
            )
        )
        raise ValueError(msg)
    return ids['training'], ids['prediction'], ids['registration']


class ModelFuture(ContainerFuture):
    """Encapsulates asynchronous execution of a CivisML job

    This object knows where to find modeling outputs
    from CivisML jobs. All data attributes are
    lazily retrieved and block on job completion.

    This object can be pickled, but it does not store the state
    of the attached :class:`~civis.APIClient` object. An unpickled
    ModelFuture will use the API key from the user's environment.

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
        is ready.
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
    civis.futures.ContainerFuture
    concurrent.futures.Future
    """

    def __init__(self, job_id, run_id, train_job_id=None, train_run_id=None,
                 polling_interval=None, client=None, poll_on_creation=True):
        if train_job_id and train_run_id:
            self.is_training = False
            self.train_job_id = train_job_id
            self.train_run_id = train_run_id
        else:
            self.is_training = True
            self.train_job_id = job_id
            self.train_run_id = run_id
        self._metadata, self._val_metadata = None, None
        self._train_data, self._train_data_fname = None, None
        self._train_metadata = None
        self._table, self._estimator = None, None
        super().__init__(job_id, run_id,
                         polling_interval=polling_interval,
                         client=client,
                         poll_on_creation=poll_on_creation)

    @staticmethod
    def _set_job_exception(fut):
        """Callback: On job completion, check the metadata.
        If it indicates an exception, replace the generic
        ``CivisJobFailure`` by a more informative ``ModelError``.
        """
        # Prevent infinite recursion: this function calls `set_exception`,
        # which triggers callbacks (i.e. re-calls this function).
        if fut._exception_handled:
            return
        else:
            fut._exception_handled = True

        try:
            meta = fut.metadata
            if fut.is_training and meta['run']['status'] == 'succeeded':
                # if training job and job succeeded, check validation job
                meta = fut.validation_metadata
            if meta is not None and meta['run']['status'] == 'exception':
                try:
                    # This will fail if the user doesn't have joblib installed
                    est = fut.estimator
                except Exception:  # NOQA
                    est = None
                fut.set_exception(
                    ModelError('Model run failed with stack trace:\n'
                               '{}'.format(meta['run']['stack_trace']),
                               est, meta))
        except (FileNotFoundError, CivisJobFailure) as exc:
            # If there's no metadata file
            # (we get FileNotFound or CivisJobFailure),
            # check the tail of the log for a clearer exception.
            exc = fut._exception_from_logs(exc)
            fut.set_exception(exc)
        except futures.CancelledError:
            # We don't need to change the exception if the run was cancelled
            pass
        except KeyError:
            # KeyErrors always represent a bug in the modeling code,
            # but showing the resulting KeyError can be confusing and
            # mask the real error.
            warnings.warn("Received malformed metadata from Civis Platform. "
                          "Something went wrong with job execution.")

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_polling_thread']
        del state['client']
        del state['poller']
        del state['_condition']
        state['_done_callbacks'] = []
        state['_self_polling_executor'] = None

        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._condition = threading.Condition()
        self.client = APIClient()
        self.poller = self.client.scripts.get_containers_runs
        self._begin_tracking()
        self.add_done_callback(self._set_job_exception)

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
                    raise ValueError(msg) from err
                else:
                    raise

        return self._train_data

    def _table_primary_key(self):
        # metadata path to input parameters is different
        # for training and prediction
        if self.is_training:
            pkey = self.metadata[
                'run']['configuration']['data']['primary_key']
        else:
            pkey = self.metadata[
                'jobs'][0]['run']['configuration']['data']['primary_key']
        return pkey

    @property
    def table(self):
        self.result()  # Block and raise errors if any
        if self._table is None:
            # An index column will only be present if primary key is
            if self._table_primary_key() is None:
                index_col = False
            else:
                index_col = 0

            if self.is_training:
                try:
                    # Training jobs only have one output table, the OOS scores
                    self._table = _load_table_from_outputs(self.job_id,
                                                           self.run_id,
                                                           self.table_fname,
                                                           index_col=index_col,
                                                           client=self.client)
                except FileNotFoundError:
                    # Just pass here, because we want the table to stay None
                    # if it does not exist
                    pass
            else:
                # Prediction jobs may have many output tables.
                output_ids = self.metadata['output_file_ids']
                if len(output_ids) > 1:
                    print('This job output {} files. Retrieving only the '
                          'first. Find the full list at `metadata'
                          '["output_file_ids"]`.'.format(len(output_ids)))
                self._table = cio.file_to_dataframe(output_ids[0],
                                                    client=self.client,
                                                    index_col=index_col)
        return self._table

    @property
    @_block_and_handle_missing
    def metadata(self):
        if self._metadata is None:
            fid = cio.file_id_from_run_output('model_info.json', self.job_id,
                                              self.run_id, client=self.client)
            self._metadata = cio.file_to_json(fid, client=self.client)
            _show_civisml_warnings(self._metadata.get('warnings', []))
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
            try:
                fid = cio.file_id_from_run_output('metrics.json',
                                                  self.train_job_id,
                                                  self.train_run_id,
                                                  client=self.client)
            except FileNotFoundError:
                # Use an empty dictionary to indicate that
                # we've already checked for metadata.
                self._val_metadata = {}
            else:
                self._val_metadata = cio.file_to_json(fid, client=self.client)
        if not self._val_metadata:
            # Convert an empty dictionary to None
            return None
        else:
            return self._val_metadata

    @property
    def metrics(self):
        if self.validation_metadata:
            return self.validation_metadata['metrics']
        else:
            return None

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

    Note that this object can be safely pickled and unpickled, but it
    does not store the state of any attached :class:`~civis.APIClient` object.
    An unpickled ModelPipeline will use the API key from the user's
    environment.

    Parameters
    ----------
    model : string or Estimator
        Either the name of a pre-defined model
        (e.g. "sparse_logistic" or "gradient_boosting_classifier")
        or else a pre-existing Estimator object.
    dependent_variable : string or List[str]
        The dependent variable of the training dataset.
        For a multi-target problem, this should be a list of
        column names of dependent variables. Nulls in a single
        dependent variable will automatically be dropped.
    primary_key : string, optional
        The unique ID (primary key) of the training dataset.
        This will be used to index the out-of-sample scores.
    parameters : dict, optional
        Specify parameters for the final stage estimator in a
        predefined model, e.g. ``{'C': 2}`` for a "sparse_logistic"
        model.
    cross_validation_parameters : dict or string, optional
        Options for cross validation. For grid search, supply a
        parameter grid as a dictionary, e.g.,
        ``{{'n_estimators': [100, 200, 500], 'learning_rate': [0.01, 0.1],
        'max_depth': [2, 3]}}``. For hyperband, pass the string "hyperband".
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
    notifications : dict
        See :func:`~civis.resources._resources.Scripts.post_custom` for
        further documentation about email and URL notification.
    dependencies : array, optional
        List of packages to install from PyPI or git repository (e.g., Github
        or Bitbucket). If a private repo is specified, please include a
        ``git_token_name`` argument as well (see below). Make sure to pin
        dependencies to a specific version, since dependencies will be
        reinstalled during every training and predict job.
    git_token_name : str, optional
        Name of remote git API token stored in Civis Platform as the password
        field in a custom platform credential. Used only when installing
        private git repositories.
    verbose : bool, optional
        If True, supply debug outputs in Platform logs and make
        prediction child jobs visible.
    etl : Estimator, optional
        Custom ETL estimator which overrides the default ETL, and
        is run before training and validation.
    civisml_version : str, optional
        CivisML version to use for training and prediction.
        If not provided, the latest version in production is used.

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
    ...                      if_exists='drop')
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
    def __init__(self, model, dependent_variable,
                 primary_key=None, parameters=None,
                 cross_validation_parameters=None, model_name=None,
                 calibration=None, excluded_columns=None, client=None,
                 cpu_requested=None, memory_requested=None,
                 disk_requested=None, notifications=None,
                 dependencies=None, git_token_name=None, verbose=False,
                 etl=None, civisml_version=None):
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
        self.notifications = notifications or {}
        self.dependencies = dependencies
        self.git_token_name = git_token_name
        self.verbose = verbose

        if client is None:
            client = APIClient()
        self._client = client
        self.train_result_ = None

        template_ids = _get_template_ids(civisml_version, self._client)
        self.train_template_id, self.predict_template_id, _ = template_ids

        self.etl = etl
        if self.train_template_id < 9968 and self.etl is not None:
            # This is a pre-v2.0 CivisML template
            raise NotImplementedError("The etl argument is not implemented"
                                      " in this version of CivisML.")

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_client']
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._client = APIClient()

    @classmethod
    def register_pretrained_model(cls, model, dependent_variable=None,
                                  features=None, primary_key=None,
                                  model_name=None, dependencies=None,
                                  git_token_name=None,
                                  skip_model_check=False, verbose=False,
                                  client=None, civisml_version=None):
        """Use a fitted scikit-learn model with CivisML scoring

        Use this function to set up your own fitted scikit-learn-compatible
        Estimator object for scoring with CivisML. This function will
        upload your model to Civis Platform and store enough metadata
        about it that you can subsequently use it with a CivisML scoring job.

        The only required input is the model itself, but you are strongly
        recommended to also provide a list of feature names. Without a list
        of feature names, CivisML will have to assume that your scoring
        table contains only the features needed for scoring (perhaps also
        with a primary key column), in all in the correct order.

        Parameters
        ----------
        model : sklearn.base.BaseEstimator or int
            The model object. This must be a fitted scikit-learn compatible
            Estimator object, or else the integer Civis File ID of a
            pickle or joblib-serialized file which stores such an object.
            If an Estimator object is provided, it will be uploaded to the
            Civis Files endpoint and set to be available indefinitely.
        dependent_variable : string or List[str], optional
            The dependent variable of the training dataset.
            For a multi-target problem, this should be a list of
            column names of dependent variables.
        features : string or List[str], optional
            A list of column names of features which were used for training.
            These will be used to ensure that tables input for prediction
            have the correct features in the correct order.
        primary_key : string, optional
            The unique ID (primary key) of the scoring dataset
        model_name : string, optional
            The name of the Platform registration job. It will have
            " Predict" added to become the Script title for predictions.
        dependencies : array, optional
            List of packages to install from PyPI or git repository (e.g.,
            GitHub or Bitbucket). If a private repo is specified, please
            include a ``git_token_name`` argument as well (see below).
            Make sure to pin dependencies to a specific version, since
            dependencies will be reinstalled during every predict job.
        git_token_name : str, optional
            Name of remote git API token stored in Civis Platform as
            the password field in a custom platform credential.
            Used only when installing private git repositories.
        skip_model_check : bool, optional
            If you're sure that your model will work with CivisML, but it
            will fail the comprehensive verification, set this to True.
        verbose : bool, optional
            If True, supply debug outputs in Platform logs and make
            prediction child jobs visible.
        client : :class:`~civis.APIClient`, optional
            If not provided, an :class:`~civis.APIClient` object will be
            created from the :envvar:`CIVIS_API_KEY`.
        civisml_version : str, optional
            CivisML version to use.
            If not provided, the latest version in production is used.

        Returns
        -------
        :class:`~civis.ml.ModelPipeline`

        Examples
        --------
        This example assumes that you already have training data
        ``X`` and ``y``, where ``X`` is a :class:`~pandas.DataFrame`.

        >>> from civis.ml import ModelPipeline
        >>> from sklearn.linear_model import Lasso
        >>> est = Lasso().fit(X, y)
        >>> model = ModelPipeline.register_pretrained_model(
        ...     est, 'concrete', features=X.columns)
        >>> model.predict(table_name='my.table', database_name='my-db')
        """
        client = client or APIClient()

        if isinstance(dependent_variable, str):
            dependent_variable = [dependent_variable]
        if isinstance(features, str):
            features = [features]
        if isinstance(dependencies, str):
            dependencies = [dependencies]
        if not model_name:
            model_name = ("Pretrained {} model for "
                          "CivisML".format(model.__class__.__name__))
            model_name = model_name[:255]  # Max size is 255 characters

        if isinstance(model, (int, float, str)):
            model_file_id = int(model)
        else:
            try:
                tempdir = tempfile.mkdtemp()
                fout = os.path.join(tempdir, 'model_for_civisml.pkl')
                joblib.dump(model, fout, compress=3)
                with open(fout, 'rb') as _fout:
                    # NB: Using the name "estimator.pkl" means that
                    # CivisML doesn't need to copy this input to a file
                    # with a different name.
                    model_file_id = cio.file_to_civis(
                        _fout, 'estimator.pkl', expires_at=None, client=client)
            finally:
                shutil.rmtree(tempdir)

        args = {'MODEL_FILE_ID': str(model_file_id),
                'SKIP_MODEL_CHECK': skip_model_check,
                'DEBUG': verbose}
        if dependent_variable is not None:
            args['TARGET_COLUMN'] = ' '.join(dependent_variable)
        if features is not None:
            args['FEATURE_COLUMNS'] = ' '.join(features)
        if dependencies is not None:
            args['DEPENDENCIES'] = ' '.join(dependencies)
        if git_token_name:
            creds = find(client.credentials.list(),
                         name=git_token_name,
                         type='Custom')
            if len(creds) > 1:
                raise ValueError("Unique credential with name '{}' for "
                                 "remote git hosting service not found!"
                                 .format(git_token_name))
            args['GIT_CRED'] = creds[0].id

        _, _, template_id = _get_template_ids(civisml_version, client)
        if template_id is None:
            msg = (
                'No registration template ID is available. '
                'Pre-trained model registration is available for CivisML '
                'v2.2 (for which `civisml_version` would be "v2.2") or above, '
                'but you have specified CivisML version "%r"'
            )
            raise ValueError(msg % civisml_version)
        container = client.scripts.post_custom(
            from_template_id=template_id,
            name=model_name,
            arguments=args)
        log.info('Created custom script %s.', container.id)

        run = client.scripts.post_custom_runs(container.id)
        log.debug('Started job %s, run %s.', container.id, run.id)

        fut = ModelFuture(container.id, run.id, client=client,
                          poll_on_creation=False)
        fut.result()
        log.info('Model registration complete.')

        mp = ModelPipeline.from_existing(fut.job_id, fut.run_id, client)
        mp.primary_key = primary_key
        return mp

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
        train_job_id = int(train_job_id)  # Convert np.int to int
        if client is None:
            client = APIClient()
        train_run_id = _decode_train_run(train_job_id, train_run_id, client)
        try:
            fut = ModelFuture(train_job_id, train_run_id, client=client)
            container = client.scripts.get_containers(train_job_id)
        except CivisAPIError as api_err:
            if api_err.status_code == 404:
                msg = ('There is no Civis Platform job with '
                       'script ID {} and run ID {}!'.format(train_job_id,
                                                            train_run_id))
                raise ValueError(msg) from api_err
            raise

        args = container.arguments

        # Older templates used "WORKFLOW" instead of "MODEL"
        model = args.get('MODEL', args.get('WORKFLOW'))
        dependent_variable = args['TARGET_COLUMN'].split()
        primary_key = args.get('PRIMARY_KEY')
        parameters = json.loads(args.get('PARAMS', "{}"))
        cross_validation_parameters = json.loads(args.get('CVPARAMS', "{}"))
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
        notifications = {camel_to_snake(key): val for key, val
                         in container.notifications.items()}
        dependencies = args.get('DEPENDENCIES', None)
        if dependencies:
            dependencies = dependencies.split()
        git_token_name = args.get('GIT_CRED', None)
        if git_token_name:
            git_token_name = client.credentials.get(git_token_name).name

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
                    notifications=notifications,
                    dependencies=dependencies,
                    git_token_name=git_token_name,
                    verbose=args.get('DEBUG', False))
        klass.train_result_ = fut

        # Set prediction template corresponding to training
        # or registration template
        template_id = int(container['from_template_id'])
        ids = find_one(
            _get_template_ids_all_versions(client).values(),
            lambda ids: ids['training'] == template_id or ids['registration'] == template_id  # noqa
        )
        p_id = ids['prediction']
        klass.predict_template_id = p_id

        return klass

    def train(self, df=None, csv_path=None, table_name=None,
              database_name=None, file_id=None,
              sql_where=None, sql_limit=None, oos_scores=None,
              oos_scores_db=None, if_exists='fail', fit_params=None,
              polling_interval=None, validation_data='train', n_jobs=None):
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
            NB: You must install ``feather-format`` if your
            :class:`~pandas.DataFrame` contains :class:`~pandas.Categorical`
            columns, to ensure that CivisML preserves data types.
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
        validation_data : str, optional
            Source for validation data. There are currently two options:
            `'train'` (the default), which cross-validates over training data
            for validation; and `'skip'`, which skips the validation step.
        n_jobs : int, optional
            Number of jobs to use for training and validation. Defaults to
            `None`, which allows CivisML to dynamically calculate an
            appropriate number of workers to use (in general, as many as
            possible without using all resources in the cluster).
            Increase n_jobs to parallelize over many hyperparameter
            combinations in grid search/hyperband, or decrease to use fewer
            computational resources at once.

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
            file_id = _stash_local_dataframe(df, self.train_template_id,
                                             client=self._client)
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
        if self.dependencies:
            train_args['DEPENDENCIES'] = ' '.join(self.dependencies)
        if self.train_template_id >= 9968:
            if validation_data:
                train_args['VALIDATION_DATA'] = validation_data
            if n_jobs:
                train_args['N_JOBS'] = n_jobs

        if HAS_SKLEARN and isinstance(self.model, BaseEstimator):
            try:
                tempdir = tempfile.mkdtemp()
                fout = os.path.join(tempdir, 'estimator.pkl')
                joblib.dump(self.model, fout, compress=3)
                with open(fout, 'rb') as _fout:
                    n = self.model_name if self.model_name else "CivisML"
                    estimator_file_id = cio.file_to_civis(
                        _fout, 'Estimator for ' + n, client=self._client)
                self._input_model = self.model  # Keep the estimator
                self.model = str(estimator_file_id)
            finally:
                shutil.rmtree(tempdir)
        train_args['MODEL'] = self.model

        if HAS_SKLEARN and self.train_template_id >= 9968:
            if isinstance(self.etl, BaseEstimator):
                try:
                    tempdir = tempfile.mkdtemp()
                    fout = os.path.join(tempdir, 'ETL.pkl')
                    joblib.dump(self.etl, fout, compress=3)
                    with open(fout, 'rb') as _fout:
                        etl_file_id = cio.file_to_civis(
                            _fout, 'ETL Estimator', client=self._client)
                    train_args['ETL'] = str(etl_file_id)
                finally:
                    shutil.rmtree(tempdir)

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
        # Handle int-like but non-Python-integer types such as np.int64
        file_id = int(file_id) if file_id is not None else file_id
        script_arguments = {'TABLE_NAME': table_name,
                            'CIVIS_FILE_ID': file_id,
                            'DEBUG': self.verbose}
        if database_name:
            if template_id < 8000:
                # v0 jobs used a different database parameter
                script_arguments['DB_NAME'] = database_name
            else:
                db_id = self._client.get_database_id(database_name)
                script_arguments['DB'] = {'database': db_id}
        resources = resources or {}
        for key, value in resources.items():
            if value:
                # Default resources are set on the template. Only
                # modify via arguments if users give a non-default value.
                script_arguments[key] = value
        if self.git_token_name:
            creds = find(self._client.credentials.list(),
                         name=self.git_token_name,
                         type='Custom')
            if len(creds) > 1:
                raise ValueError("Unique credential with name '{}' for "
                                 "remote git hosting service not found!"
                                 .format(self.git_token_name))
            script_arguments['GIT_CRED'] = creds[0].id

        script_arguments.update(args or {})

        container = self._client.scripts.post_custom(
            from_template_id=template_id,
            name=job_name,
            arguments=script_arguments,
            notifications=self.notifications)
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
    def state(self):
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
                if_exists='fail', n_jobs=None, polling_interval=None,
                cpu=None, memory=None, disk_space=None,
                dvs_to_predict=None):
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
            NB: You must install ``feather-format`` if your
            :class:`~pandas.DataFrame` contains :class:`~pandas.Categorical`
            columns, to ensure that CivisML preserves data types.
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
            for multi-file / large table prediction. Defaults to
            `None`, which allows CivisML to dynamically calculate an
            appropriate number of workers to use (in general, as many as
            possible without using all resources in the cluster).
        polling_interval : float, optional
            Check for job completion every this number of seconds.
            Do not set if using the notifications endpoint.
        cpu : int, optional
            CPU shares requested by the user for a single job.
        memory : int, optional
            RAM requested by the user for a single job.
        disk_space : float, optional
            disk space requested by the user for a single job.
        dvs_to_predict : list of str, optional
            If this is a multi-output model, you may list a subset of
            dependent variables for which you wish to generate predictions.
            This list must be a subset of the original `dependent_variable`
            input. The scores for the returned subset will be identical to
            the scores which those outputs would have had if all outputs
            were written, but ignoring some of the model's outputs will
            let predictions complete faster and use less disk space.
            The default is to produce scores for all DVs.

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
            file_id = _stash_local_dataframe(df, self.predict_template_id,
                                             client=self._client)
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
            if self.predict_template_id == 7021:
                # v0 jobs used a different database parameter
                predict_args['OUTPUT_DB'] = output_db
            else:
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
        if dvs_to_predict:
            if isinstance(dvs_to_predict, str):
                dvs_to_predict = [dvs_to_predict]
            if self.predict_template_id > 10583:
                # This feature was added in v2.2; 10583 is the v2.1 template
                predict_args['TARGET_COLUMN'] = ' '.join(dvs_to_predict)
        if self.predict_template_id >= 9969:
            if cpu:
                predict_args['CPU'] = cpu
            if memory:
                predict_args['MEMORY'] = memory
            if disk_space:
                predict_args['DISK_SPACE'] = disk_space

        name = self.model_name + ' Predict' if self.model_name else None
        result, container, run = self._create_custom_run(
            self.predict_template_id,
            job_name=name,
            table_name=table_name,
            database_name=database_name,
            file_id=file_id,
            args=predict_args,
            polling_interval=polling_interval)

        return result
