"""Parallel computations using the Civis Platform infrastructure
"""

from concurrent.futures import wait
from datetime import datetime, timedelta
from io import BytesIO
import logging
import os
import pickle  # nosec
from tempfile import TemporaryDirectory
import time
import warnings


import cloudpickle
from joblib._parallel_backends import ParallelBackendBase
from joblib.my_exceptions import TransportableException
from joblib import register_parallel_backend as _joblib_reg_para_backend
import requests

import civis
from civis.base import CivisAPIError

from civis.futures import _ContainerShellExecutor, CustomScriptExecutor

try:
    with warnings.catch_warnings():
        # Ignore the warning: "DeprecationWarning: sklearn.externals.joblib is
        # deprecated in 0.21 and will be removed in 0.23. Please import this
        # functionality directly from joblib, which can be installed with:
        # pip install joblib. If this warning is raised when loading pickled
        # models, you may need to re-serialize those models with
        # scikit-learn 0.21+."
        warnings.simplefilter('ignore', DeprecationWarning)
        # sklearn 0.22 has switched from DeprecationWarning to FutureWarning
        warnings.simplefilter('ignore', FutureWarning)
        from sklearn.externals.joblib import (
            register_parallel_backend as _sklearn_reg_para_backend)

        # NO_SKLEARN_BACKEND would be a better name here since it'll be true
        # for future scikit-learn versions that won't include the joblib
        # module as well as when scikit-learn isn't installed, but changing
        # the name would technically be a breaking change.
        NO_SKLEARN = False
except ImportError:
    NO_SKLEARN = True
    _sklearn_reg_para_backend = None

log = logging.getLogger(__name__)
_THIS_DIR = os.path.dirname(os.path.realpath(__file__))
_DEFAULT_SETUP_CMD = ":"  # An sh command that does nothing.
_DEFAULT_REPO_SETUP_CMD = "cd /app; python setup.py install; cd /"
_ALL_JOBS = 50  # Give the user this many jobs if they request "all of them"

# When creating a remote execution environment from an existing
# Container Script, read these keys.
KEYS_TO_INFER = ['docker_image_name', 'docker_image_tag', 'repo_http_uri',
                 'repo_ref', 'remote_host_credential_id', 'git_credential_id',
                 'cancel_timeout', 'time_zone']


def infer_backend_factory(required_resources=None,
                          params=None,
                          arguments=None,
                          client=None,
                          polling_interval=None,
                          setup_cmd=None,
                          max_submit_retries=0,
                          max_job_retries=0,
                          hidden=True,
                          remote_backend='sequential',
                          **kwargs):
    """Infer the container environment and return a backend factory.

    This function helps you run additional jobs from code which executes
    inside a Civis container job. The function reads settings for
    relevant parameters (e.g. the Docker image) of the container
    it's running inside of.

    Jobs created through this backend will have environment variables
    "CIVIS_PARENT_JOB_ID" and "CIVIS_PARENT_RUN_ID" with the contents
    of the "CIVIS_JOB_ID" and "CIVIS_RUN_ID" of the environment which
    created them. If the code doesn't have "CIVIS_JOB_ID" and "CIVIS_RUN_ID"
    environment variables available, the child will not have
    "CIVIS_PARENT_JOB_ID" and "CIVIS_PARENT_RUN_ID" environment variables.

    .. note:: This function will read the state of the parent
              container job at the time this function executes. If the
              user has modified the container job since the run started
              (e.g. by changing the GitHub branch in the container's GUI),
              this function may infer incorrect settings for the child jobs.

    Keyword arguments inferred from the existing script's state are
    %s

    Parameters
    ----------
    required_resources : dict or None, optional
        The resources needed by the container. See the
        `container scripts API documentation
        <https://platform.civisanalytics.com/api#resources-scripts>`
        for details. Resource requirements not specified will
        default to the requirements of the current job.
    params : list or None, optional
        A definition of the parameters this script accepts in the
        arguments field. See the `container scripts API documentation
        <https://platform.civisanalytics.com/api#resources-scripts>`
        for details.

        Parameters of the child jobs will default to the parameters
        of the current job. Any parameters provided here will override
        parameters of the same name from the current job.
    arguments : dict or None, optional
        Dictionary of name/value pairs to use to run this script.
        Only settable if this script has defined params. See the `container
        scripts API documentation
        <https://platform.civisanalytics.com/api#resources-scripts>`
        for details.

        Arguments will default to the arguments of the current job.
        Anything provided here will override portions of the current job's
        arguments.
    client : `civis.APIClient` instance or None, optional
        An API Client object to use.
    polling_interval : int, optional
        The polling interval, in seconds, for checking container script status.
        If you have many jobs, you may want to set this higher (e.g., 300) to
        avoid `rate-limiting <https://platform.civisanalytics.com/api#basics>`.
    setup_cmd : str, optional
        A shell command or sequence of commands for setting up the environment.
        These will precede the commands used to run functions in joblib.
        This is primarily for installing dependencies that are not available
        in the dockerhub repo (e.g., "cd /app && python setup.py install"
        or "pip install gensim").

        With no GitHub repo input, the setup command will
        default to a command that does nothing. If a ``repo_http_uri``
        is provided, the default setup command will attempt to run
        "python setup.py install". If this command fails, execution
        will still continue.
    max_submit_retries : int, optional
        The maximum number of retries for submitting each job. This is to help
        avoid a large set of jobs failing because of a single 5xx error. A
        value higher than zero should only be used for jobs that are idempotent
        (i.e., jobs whose result and side effects are the same regardless of
        whether they are run once or many times).
    max_job_retries : int, optional
        Retry failed jobs this number of times before giving up.
        Even more than with ``max_submit_retries``, this should only
        be used for jobs which are idempotent, as the job may have
        caused side effects (if any) before failing.
        These retries assist with jobs which may have failed because
        of network or worker failures.
    hidden: bool, optional
        The hidden status of the object. Setting this to true
        hides it from most API endpoints. The object can still
        be queried directly by ID. Defaults to True.
    remote_backend : str or object, optional
        The name of a joblib backend or a joblib backend itself. This parameter
        is the joblib backend to use when executing code within joblib in the
        container. The default of 'sequential' uses the joblib sequential
        backend in the container. The value 'civis' uses an exact copy of the
        Civis joblib backend that launched the container. Note that with the
        value 'civis', one can potentially use more jobs than specified by
        ``n_jobs``.
    **kwargs:
        Additional keyword arguments will be passed directly to
        :func:`~civis.APIClient.scripts.post_containers`, potentially
        overriding the values of those arguments in the parent environment.

    Raises
    ------
    RuntimeError
        If this function is not running inside a Civis container job.

    See Also
    --------
    civis.parallel.make_backend_factory
    """
    if client is None:
        client = civis.APIClient()

    if not os.environ.get('CIVIS_JOB_ID'):
        raise RuntimeError('This function must be run '
                           'inside a container job.')
    state = client.scripts.get_containers(os.environ['CIVIS_JOB_ID'])
    if state.from_template_id:
        # If this is a Custom Script from a template, we need the
        # backing script. Make sure to save the arguments from
        # the Custom Script: those are the only user-settable parts.
        template = client.templates.get_scripts(state.from_template_id)
        try:
            custom_args = state.arguments
            state = client.scripts.get_containers(template.script_id)
            state.arguments = custom_args
        except civis.base.CivisAPIError as err:
            if err.status_code == 404:
                raise RuntimeError('Unable to introspect environment from '
                                   'your template\'s backing script. '
                                   'You may not have permission to view '
                                   'script ID {}.'.format(template.script_id))
            else:
                raise

    # Default to this container's resource requests, but
    # allow users to override it.
    state.required_resources.update(required_resources or {})

    # Update parameters with user input
    params = params or []
    for input_param in params:
        for param in state.params:
            if param['name'] == input_param['name']:
                param.update(input_param)
                break
        else:
            state.params.append(input_param)

    # Update arguments with input
    state.arguments.update(arguments or {})

    # Set defaults on other keyword arguments with
    # values from the current script
    for key in KEYS_TO_INFER:
        kwargs.setdefault(key, state[key])

    # Don't include parent job params since they're added automatically
    # in _ContainerShellExecutor.__init__.
    filtered_params = [p for p in state.params if p['name'].upper()
                       not in ('CIVIS_PARENT_JOB_ID', 'CIVIS_PARENT_RUN_ID')]

    return make_backend_factory(required_resources=state.required_resources,
                                params=filtered_params,
                                arguments=state.arguments,
                                client=client,
                                polling_interval=polling_interval,
                                setup_cmd=setup_cmd,
                                max_submit_retries=max_submit_retries,
                                max_job_retries=max_job_retries,
                                hidden=hidden,
                                remote_backend=remote_backend,
                                **kwargs)


infer_backend_factory.__doc__ = infer_backend_factory.__doc__ % KEYS_TO_INFER


def make_backend_factory(docker_image_name="civisanalytics/datascience-python",
                         client=None,
                         polling_interval=None,
                         setup_cmd=None,
                         max_submit_retries=0,
                         max_job_retries=0,
                         hidden=True,
                         remote_backend='sequential',
                         **kwargs):
    """Create a joblib backend factory that uses Civis Container Scripts

    Jobs created through this backend will have environment variables
    "CIVIS_PARENT_JOB_ID" and "CIVIS_PARENT_RUN_ID" with the contents
    of the "CIVIS_JOB_ID" and "CIVIS_RUN_ID" of the environment which
    created them. If the code doesn't have "CIVIS_JOB_ID" and "CIVIS_RUN_ID"
    environment variables available, the child will not have
    "CIVIS_PARENT_JOB_ID" and "CIVIS_PARENT_RUN_ID" environment variables.

    .. note:: The total size of function parameters in `Parallel()`
              calls on this backend must be less than 5 GB due to
              AWS file size limits.

    .. note:: The maximum number of concurrent jobs in the Civis Platform
              is controlled by both the ``n_jobs`` and ``pre_dispatch``
              parameters of ``joblib.Parallel``.
              Set ``pre_dispatch="n_jobs"`` to have a maximum of
              ``n_jobs`` processes running at once.
              (The default is ``pre_dispatch="2*n_jobs"``.)

    Parameters
    ----------
    docker_image_name : str, optional
        The image for the container script. You may also wish to specify
        a ``docker_image_tag`` in the keyword arguments.
    client : `civis.APIClient` instance or None, optional
        An API Client object to use.
    polling_interval : int, optional
        The polling interval, in seconds, for checking container script status.
        If you have many jobs, you may want to set this higher (e.g., 300) to
        avoid `rate-limiting <https://platform.civisanalytics.com/api#basics>`.
    setup_cmd : str, optional
        A shell command or sequence of commands for setting up the environment.
        These will precede the commands used to run functions in joblib.
        This is primarily for installing dependencies that are not available
        in the dockerhub repo (e.g., "cd /app && python setup.py install"
        or "pip install gensim").

        With no GitHub repo input, the setup command will
        default to a command that does nothing. If a `repo_http_uri`
        is provided, the default setup command will attempt to run
        "python setup.py install". If this command fails, execution
        will still continue.
    max_submit_retries : int, optional
        The maximum number of retries for submitting each job. This is to help
        avoid a large set of jobs failing because of a single 5xx error. A
        value higher than zero should only be used for jobs that are idempotent
        (i.e., jobs whose result and side effects are the same regardless of
        whether they are run once or many times).
    max_job_retries : int, optional
        Retry failed jobs this number of times before giving up.
        Even more than with `max_submit_retries`, this should only
        be used for jobs which are idempotent, as the job may have
        caused side effects (if any) before failing.
        These retries assist with jobs which may have failed because
        of network or worker failures.
    hidden: bool, optional
        The hidden status of the object. Setting this to true
        hides it from most API endpoints. The object can still
        be queried directly by ID. Defaults to True.
    remote_backend : str or object, optional
        The name of a joblib backend or a joblib backend itself. This parameter
        is the joblib backend to use when executing code within joblib in the
        container. The default of 'sequential' uses the joblib sequential
        backend in the container. The value 'civis' uses an exact copy of the
        Civis joblib backend that launched the container. Note that with the
        value 'civis', one can potentially use more jobs than specified by
        ``n_jobs``.
    **kwargs:
        Additional keyword arguments will be passed
        directly to :func:`~civis.APIClient.scripts.post_containers`.

    Examples
    --------
    >>> # Without joblib:
    >>> from math import sqrt
    >>> print([sqrt(i ** 2) for i in range(10)])
    [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]

    >>> # Using the default joblib backend:
    >>> from joblib import delayed, Parallel
    >>> parallel = Parallel(n_jobs=5)
    >>> print(parallel(delayed(sqrt)(i ** 2) for i in range(10)))
    [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]

    >>> # Using the Civis backend:
    >>> from joblib import parallel_backend, register_parallel_backend
    >>> from civis.parallel import make_backend_factory
    >>> register_parallel_backend('civis', make_backend_factory(
    ...     required_resources={"cpu": 512, "memory": 256}))
    >>> with parallel_backend('civis'):
    ...    parallel = Parallel(n_jobs=5, pre_dispatch='n_jobs')
    ...    print(parallel(delayed(sqrt)(i ** 2) for i in range(10)))
    [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]

    >>> # Using scikit-learn with the Civis backend:
    >>> from sklearn.externals.joblib import \
    ...     register_parallel_backend as sklearn_register_parallel_backend
    >>> from sklearn.externals.joblib import \
    ...     parallel_backend as sklearn_parallel_backend
    >>> from sklearn.model_selection import GridSearchCV
    >>> from sklearn.ensemble import GradientBoostingClassifier
    >>> from sklearn.datasets import load_digits
    >>> digits = load_digits()
    >>> param_grid = {
    ...     "max_depth": [1, 3, 5, None],
    ...     "max_features": ["sqrt", "log2", None],
    ...     "learning_rate": [0.1, 0.01, 0.001]
    ... }
    >>> # Note: n_jobs and pre_dispatch specify the maximum number of
    >>> # concurrent jobs.
    >>> gs = GridSearchCV(GradientBoostingClassifier(n_estimators=1000,
    ...                                              random_state=42),
    ...                   param_grid=param_grid,
    ...                   n_jobs=5, pre_dispatch="n_jobs")
    >>> sklearn_register_parallel_backend('civis', make_backend_factory(
    ...     required_resources={"cpu": 512, "memory": 256}))
    >>> with sklearn_parallel_backend('civis'):
    ...     gs.fit(digits.data, digits.target)

    Notes
    -----
    Joblib's :func:`joblib.parallel.register_parallel_backend`
    (see example above) expects a callable that returns a
    :class:`joblib.parallel.ParallelBackendBase` instance. This function
    allows the user to specify the Civis container script setting that will be
    used when that backend creates container scripts to run jobs.

    The specified Docker image (optionally, with a GitHub repo and setup
    command) must have basically the same environment as the one in which this
    module is used to submit jobs. The worker jobs need to be able to
    deserialize the jobs they are given, including the data and all the
    necessary Python objects (e.g., if you pass a Pandas data frame, the image
    must have Pandas installed). You may use functions and classes
    dynamically defined in the code (e.g. lambda functions), but
    if your joblib-parallized function calls code imported from another
    module, that module must be installed in the remote environment.

    See Also
    --------
    civis.APIClient.scripts.post_containers
    """
    if setup_cmd is None:
        if kwargs.get('repo_http_uri'):
            setup_cmd = _DEFAULT_REPO_SETUP_CMD
        else:
            setup_cmd = _DEFAULT_SETUP_CMD

    def backend_factory():
        return _CivisBackend(docker_image_name=docker_image_name,
                             client=client,
                             polling_interval=polling_interval,
                             setup_cmd=setup_cmd,
                             max_submit_retries=max_submit_retries,
                             max_n_retries=max_job_retries,
                             hidden=hidden,
                             remote_backend=remote_backend,
                             **kwargs)

    return backend_factory


def make_backend_template_factory(from_template_id,
                                  arguments=None,
                                  client=None,
                                  polling_interval=None,
                                  max_submit_retries=0,
                                  max_job_retries=0,
                                  hidden=True):
    """Create a joblib backend factory that uses Civis Custom Scripts.

    If your template has settable parameters "CIVIS_PARENT_JOB_ID" and
    "CIVIS_PARENT_RUN_ID", then this executor will fill them with the contents
    of the "CIVIS_JOB_ID" and "CIVIS_RUN_ID" of the environment which
    created them. If the code doesn't have "CIVIS_JOB_ID" and "CIVIS_RUN_ID"
    environment variables available, the child will not have
    "CIVIS_PARENT_JOB_ID" and "CIVIS_PARENT_RUN_ID" environment variables.

    Parameters
    ----------
    from_template_id: int
        Create jobs as Custom Scripts from the given template ID.
        When using the joblib backend with templates,
        the template must have a very specific form. Refer
        to the documentation for details.
    arguments : dict or None, optional
        Dictionary of name/value pairs to use to run this script.
        Only settable if this script has defined params. See the `container
        scripts API documentation
        <https://platform.civisanalytics.com/api#resources-scripts>`
        for details.
    client : `civis.APIClient` instance or None, optional
        An API Client object to use.
    polling_interval : int, optional
        The polling interval, in seconds, for checking container script status.
        If you have many jobs, you may want to set this higher (e.g., 300) to
        avoid `rate-limiting <https://platform.civisanalytics.com/api#basics>`.
    max_submit_retries : int, optional
        The maximum number of retries for submitting each job. This is to help
        avoid a large set of jobs failing because of a single 5xx error. A
        value higher than zero should only be used for jobs that are idempotent
        (i.e., jobs whose result and side effects are the same regardless of
        whether they are run once or many times).
    max_job_retries : int, optional
        Retry failed jobs this number of times before giving up.
        Even more than with `max_submit_retries`, this should only
        be used for jobs which are idempotent, as the job may have
        caused side effects (if any) before failing.
        These retries assist with jobs which may have failed because
        of network or worker failures.
    hidden: bool, optional
        The hidden status of the object. Setting this to true
        hides it from most API endpoints. The object can still
        be queried directly by ID. Defaults to True.
    """
    def backend_factory():
        return _CivisBackend(from_template_id=from_template_id,
                             arguments=arguments,
                             client=client,
                             polling_interval=polling_interval,
                             max_submit_retries=max_submit_retries,
                             max_n_retries=max_job_retries,
                             hidden=hidden)

    return backend_factory


class JobSubmissionError(Exception):
    pass


def _robust_pickle_download(output_file_id, client=None,
                            n_retries=5, delay=0.0):
    """Download and deserialize the result from output_file_id

    Retry network errors `n_retries` times with `delay` seconds between calls

    Parameters
    ----------
    output_file_id : int
        ID of the file to download
    client : civis.APIClient, optional
    n_retries : int, optional
        Retry the upload this many times before raising an error.
    delay : float, optional
        If provided, wait this many seconds between retries.

    Returns
    -------
    obj
        Any Python object; the result of calling ``cloudpickle.load`` on the
        downloaded file

    See Also
    --------
    cloudpickle.load
    """
    client = client or civis.APIClient()
    retry_exc = (requests.HTTPError,
                 requests.ConnectionError,
                 requests.ConnectTimeout)
    n_failed = 0
    while True:
        buffer = BytesIO()
        try:
            civis.io.civis_to_file(output_file_id, buffer, client=client)
        except retry_exc as exc:
            buffer.close()
            if n_failed < n_retries:
                n_failed += 1
                log.debug("Download failure %s due to %s; retrying.",
                          n_failed, str(exc))
                time.sleep(delay)
            else:
                raise
        else:
            buffer.seek(0)
            return cloudpickle.load(buffer)


def _robust_file_to_civis(buf, name, client=None, n_retries=5,
                          delay=0.0, **kwargs):
    """Upload the contents of an input file-like buffer

    Call :func:`~civis.io.file_to_civis`, and retry a specified
    number of times before giving up. This will abandon
    Civis files created for failed uploads. Thoase files may
    be partially filled; it's necessary to create new files
    to ensure that the contents are exactly as requested.

    .. note:: This function starts by calling ``.seek(0)`` on the
              buffer, and will do so before every retry.

    Parameters
    ----------
    buf : File
        File-like bytes object to send to a Civis File
    name : str
        Name of the new Civis File
    client : civis.APIClient, optional
    n_retries : int, optional
        Retry the upload this many times before raising an error.
    delay : float, optional
        If provided, wait this many seconds between retries.
    kwargs :
        Extra keyword arguments will be passed to ``io.file_to_civis``

    Returns
    -------
    int
        ID of the new Civis File

    See Also
    --------
    civis.io.file_to_civis
    """
    client = client or civis.APIClient()
    retry_exc = (requests.HTTPError,
                 requests.ConnectionError,
                 requests.ConnectTimeout)
    n_failed = 0
    while True:
        buf.seek(0)
        try:
            file_id = civis.io.file_to_civis(buf, name=name,
                                             client=client, **kwargs)
        except retry_exc as exc:
            if n_failed < n_retries:
                n_failed += 1
                log.debug("Upload failure %s due to %s; retrying.",
                          n_failed, str(exc))
                time.sleep(delay)
            else:
                raise
        else:
            return file_id


def _setup_remote_backend(remote_backend):
    """Setup the remote backend while in a Civis container.

    Parameters
    ----------
    remote_backend : str or object
        The name of a joblib backend or a joblib backend itself. If the object
        is an instance of `_CivisBackend`, it is registered with joblib.

    Returns
    -------
    backend : str
        The name of the backend to use.
    """
    if isinstance(remote_backend, _CivisBackend):
        def backend_factory():
            return _CivisBackend.from_existing(remote_backend)
        # joblib and global state: fun!
        #
        # joblib internally maintains a global list of backends and
        # specifically tracks which backend is currently in use. Further,
        # sklearn ships its own COPY of the entire joblib package at
        # `sklearn.externals.joblib`. Thus there are TWO copies of joblib
        # in use (the joblib package and the one in sklearn) and thus different
        # global states that need to be handeled. Whew.
        #
        # Therefore, we have to register our backend with both copies in order
        # to allow our containers to run `Parallel` objects from both copies
        # of joblib. Yay!
        _joblib_reg_para_backend('civis', backend_factory)
        if not NO_SKLEARN:
            _sklearn_reg_para_backend('civis', backend_factory)
        return 'civis'
    else:
        return remote_backend


class _CivisBackendResult:
    """A wrapper for results of joblib tasks

    This wrapper makes results look like the results from multiprocessing
    pools that joblib expects.  This retrieves the results for a completed
    job (i.e., container script) from Civis.

    Parameters
    ----------
    future : :class:`~civis.futures.ContainerFuture`
        A Future which represents a Civis job. Created by a
        :class:`~_ContainerShellExecutor`.
    callback : callable
        A `joblib`-provided callback function which should be
        called on successful job completion. It will launch the
        next job in line. See `joblib.parallel.Parallel._dispatch`
        for the creation of this callback function.
        It takes a single input, the output of the remote function call.

    Notes
    -----
    * This is similar to a Future object except with ``get`` instead of
      ``result``, and with a callback specified.
    * This is only intended to work within joblib and with the Civis backend.
    * Joblib calls ``get`` on one result at a time, in order of submission.
    * Exceptions should only be raised inside ``get`` so that joblib can
        handle them properly.
    """
    def __init__(self, future, callback):
        self._future = future
        self._callback = callback
        self.result = None
        if hasattr(future, 'client'):
            self._client = future.client
        else:
            self._client = civis.APIClient()

        # Download results and trigger the next job as a callback
        # so that we don't have to wait for `get` to be called.
        # Note that the callback of a `concurrent.futures.Future`
        # (which self._future is a subclass of) is called with a
        # single argument, the Future itself.
        self._future.remote_func_output = None  # `get` reads results from here
        self._future.result_fetched = False  # Did we get the result?
        self._future.add_done_callback(
            self._make_fetch_callback(self._callback, self._client))

    @staticmethod
    def _make_fetch_callback(joblib_callback, client):
        """Create a closure for use as a callback on the ContainerFuture"""
        def _fetch_result(fut):
            """Retrieve outputs from the remote function.
            Run the joblib callback only if there were no errors.

            Parameters
            ----------
            fut : :class:`~civis.futures.ContainerFuture`
                A Future which represents a Civis job. Created by a
                :class:`~_ContainerShellExecutor`.

            Note
            ----
            We can't return data from a callback, so the remote
            function output is attached to the Future object
            as a new attribute ``remote_func_output``.
            """
            if fut.succeeded():
                log.debug(
                    "Ran job through Civis. Job ID: %d, run ID: %d;"
                    " job succeeded!", fut.job_id, fut.run_id)
            elif fut.cancelled():
                log.debug(
                    "Ran job through Civis. Job ID: %d, run ID: %d;"
                    " job cancelled!", fut.job_id, fut.run_id)
            else:
                log.error(
                    "Ran job through Civis. Job ID: %d, run ID: %d;"
                    " job failure!", fut.job_id, fut.run_id)

            try:
                # Find the output file ID from the run outputs.
                run_outputs = client.scripts.list_containers_runs_outputs(
                    fut.job_id, fut.run_id)
                if run_outputs:
                    output_file_id = run_outputs[0]['object_id']
                    res = _robust_pickle_download(output_file_id, client,
                                                  n_retries=5, delay=1.0)
                    fut.remote_func_output = res
                    log.debug("Downloaded and deserialized the result.")
            except BaseException as exc:
                # If something went wrong when fetching outputs, record the
                # exception so we can re-raise it in the parent process.
                # Catch BaseException so we can also re-raise a
                # KeyboardInterrupt where it can be properly handled.
                log.debug('Exception during result download: %s', str(exc))
                fut.remote_func_output = exc
            else:
                fut.result_fetched = True
                cancelled = fut.cancelled()
                try:
                    # After requesting cancellation, a script stays in a
                    # running state and sets _result.is_cancel_requested
                    # to True to allow for clean up logic. Here, we make sure
                    # to treat these runs the same as cancelled runs.
                    cancelled |= fut._result.is_cancel_requested
                except AttributeError:
                    pass
                if not cancelled and not fut.exception():
                    # The next job will start when this callback is called.
                    # Only run it if the job was a success.
                    joblib_callback(fut.remote_func_output)

        return _fetch_result

    def get(self, timeout=None):
        """Block and return the result of the job

        Parameters
        ----------
        timeout: float, optional
            If provided, wait this many seconds before issuing a TimeoutError

        Returns
        -------
        The output of the function which ``joblib`` ran via Civis
            NB: ``joblib`` expects that ``get`` will always return an iterable.
        The remote function(s) should always be wrapped in
        ``joblib.parallel.BatchedCalls``, which does always return a list.

        Raises
        ------
        TransportableException
            Any error in the remote job will result in a
            ``TransportableException``, to be handled by ``Parallel.retrieve``.
        futures.CancelledError
            If the remote job was cancelled before completion
        """
        if self.result is None:
            # Wait for the script to complete.
            wait([self._future], timeout=timeout)
            self.result = self._future.remote_func_output

        if self._future.exception() or not self._future.result_fetched:
            # If the job errored, we may have been able to return
            # an exception via the run outputs. If not, fall back
            # to the API exception.
            # Note that a successful job may still have an exception
            # result if job output retrieval failed.
            if self.result is not None:
                raise self.result
            else:
                # Use repr for the message because the API exception
                # typically has str(exc)==None.
                exc = self._future.exception()
                raise TransportableException(repr(exc), type(exc))

        return self.result


class _CivisBackend(ParallelBackendBase):
    """The backend class that tells joblib how to use Civis to run jobs

    Users should interact with this through ``make_backend_factory``.
    """
    uses_threads = False
    supports_sharedmem = False
    supports_timeout = True

    def __init__(self, setup_cmd=_DEFAULT_SETUP_CMD,
                 from_template_id=None,
                 max_submit_retries=0,
                 client=None,
                 remote_backend='sequential',
                 nesting_level=0,
                 **executor_kwargs):
        self.setup_cmd = setup_cmd
        self.from_template_id = from_template_id
        self.max_submit_retries = max_submit_retries
        self.client = client
        self.remote_backend = remote_backend
        self.executor_kwargs = executor_kwargs
        self.nesting_level = nesting_level
        self._init_civis_backend()

    @classmethod
    def from_existing(cls, klass):
        """Build a new `_CivisBackend` from an existing one."""
        return cls(
            setup_cmd=klass.setup_cmd,
            from_template_id=klass.from_template_id,
            max_submit_retries=klass.max_submit_retries,
            client=klass.client,
            remote_backend=klass.remote_backend,
            **klass.executor_kwargs)

    def _init_civis_backend(self):
        """init the Civis API client and the executors"""
        self.using_template = (self.from_template_id is not None)

        if self.max_submit_retries < 0:
            raise ValueError(
                "max_submit_retries cannot be negative (value = %d)" %
                self.max_submit_retries)

        self.client = self.client or civis.APIClient()
        if self.from_template_id:
            self.executor = CustomScriptExecutor(self.from_template_id,
                                                 client=self.client,
                                                 **self.executor_kwargs)
        else:
            self.executor = _ContainerShellExecutor(client=self.client,
                                                    **self.executor_kwargs)

    def effective_n_jobs(self, n_jobs):
        if n_jobs is None:
            n_jobs = 1
        if n_jobs == -1:
            n_jobs = _ALL_JOBS
        if n_jobs <= 0:
            raise ValueError("Please request a positive number of jobs, "
                             "or use \"-1\" to request a default "
                             "of {} jobs.".format(_ALL_JOBS))
        return n_jobs

    def abort_everything(self, ensure_ready=True):
        # This method is called when a job has raised an exception.
        # In that case, we're not going to finish computations, so
        # we should free up Platform resources in any remaining jobs.
        self.executor.cancel_all()
        if not ensure_ready:
            self.executor.shutdown(wait=False)

    def terminate(self):
        """Shutdown the workers and free the shared memory."""
        return self.abort_everything(ensure_ready=True)

    def apply_async(self, func, callback=None):
        """Schedule func to be run
        """
        # Serialize func to a temporary file and upload it to a Civis File.
        # Make the temporary files expire in a week.
        expires_at = (datetime.now() + timedelta(days=7)).isoformat()
        with TemporaryDirectory() as tempdir:
            temppath = os.path.join(tempdir, "civis_joblib_backend_func")
            with open(temppath, "wb") as tmpfile:
                cloudpickle.dump(
                    (func,
                     self if self.remote_backend == 'civis'
                     else self.remote_backend),
                    tmpfile,
                    pickle.HIGHEST_PROTOCOL)
            with open(temppath, "rb") as tmpfile:
                func_file_id = \
                    _robust_file_to_civis(tmpfile,
                                          "civis_joblib_backend_func",
                                          n_retries=5,
                                          delay=0.5,
                                          expires_at=expires_at,
                                          client=self.client)
                log.debug("uploaded serialized function to File: %d",
                          func_file_id)

            # Use the Civis CLI client to download the job runner script into
            # the container, and then run it on the uploaded job.
            # Only download the runner script if it doesn't already
            # exist in the destination environment.
            runner_remote_path = "civis_joblib_worker"
            cmd = ("{setup_cmd} && "
                   "if command -v {runner_remote_path} >/dev/null; "
                   "then exec {runner_remote_path} {func_file_id}; "
                   "else pip install civis=={civis_version} && "
                   "exec {runner_remote_path} {func_file_id}; fi "
                   .format(civis_version=civis.__version__,
                           runner_remote_path=runner_remote_path,
                           func_file_id=func_file_id,
                           setup_cmd=self.setup_cmd))

            # Try to submit the command, with optional retrying for certain
            # error types.
            for n_retries in range(1 + self.max_submit_retries):
                try:
                    if self.using_template:
                        args = {'JOBLIB_FUNC_FILE_ID': func_file_id}
                        future = self.executor.submit(**args)
                        log.debug("Started custom script from template "
                                  "%s with arguments %s",
                                  self.executor.from_template_id, args)
                    else:
                        future = self.executor.submit(fn=cmd)
                        log.debug("started container script with "
                                  "command: %s", cmd)
                    # Stop retrying if submission was successful.
                    break
                except CivisAPIError as e:
                    # If we've retried the maximum number of times already,
                    # then raise an exception.
                    retries_left = self.max_submit_retries - n_retries - 1
                    if retries_left < 1:
                        raise JobSubmissionError(e)

                    log.debug("Retrying submission. %d retries left",
                              retries_left)

                    # Sleep with exponentially increasing intervals in case
                    # the issue persists for a while.
                    time.sleep(2 ** n_retries)

            if self.executor.max_n_retries:
                # Start the ContainerFuture polling.
                # This will use more API calls, but will
                # allow the ContainerFuture to launch
                # retries if necessary.
                # (This is only relevant if we're not using the
                # notifications endpoint.)
                future.done()

            result = _CivisBackendResult(future, callback)

        return result

    def __getstate__(self):
        """override pickle to remove threading and civis APIClient objects"""
        state = self.__dict__.copy()
        if 'client' in state:
            state['client'] = None
        if 'executor' in state:
            del state['executor']
        # the parallel attribute gets added by the parent class when the
        # backend is in use.
        if 'parallel' in state:
            state['parallel'] = None
        return state

    def __setstate__(self, state):
        """re-init the backend when unpickling"""
        self.__dict__.update(state)
        self._init_civis_backend()
