from __future__ import absolute_import

from abc import ABCMeta, abstractmethod
from builtins import super
from concurrent.futures import Executor
from concurrent import futures
import copy
import datetime
import logging
import time
import threading
import warnings

import six

from civis import APIClient
from civis.base import CivisAPIError, DONE
from civis.polling import PollableResult

from pubnub.pubnub import PubNub
from pubnub.pnconfiguration import PNConfiguration, PNReconnectionPolicy
from pubnub.enums import PNStatusCategory
from pubnub.callbacks import SubscribeCallback

log = logging.getLogger(__name__)

# Pubnub connections can recover missed messages upon reconnecting for up to 10
# minutes from the disconnect. Polling on a 9.5 minute interval is used as a
# fallback in case the job complete message is missed in an outage.
_LONG_POLLING_INTERVAL = 9.5 * 60


class JobCompleteListener(SubscribeCallback):
    _disconnect_categories = [
        PNStatusCategory.PNTimeoutCategory,
        PNStatusCategory.PNNetworkIssuesCategory,
        PNStatusCategory.PNUnexpectedDisconnectCategory,
    ]

    def __init__(self, match_function, callback_function,
                 disconnect_function=None):
        self.match_function = match_function
        self.callback_function = callback_function
        self.disconnect_function = disconnect_function

    def message(self, pubnub, message):
        if self.match_function(message.message):
            self.callback_function()

    def status(self, pubnub, status):
        if status.category in self._disconnect_categories:
            if self.disconnect_function:
                self.disconnect_function()

    def presence(self, pubnub, presence):
        pass


class CivisFuture(PollableResult):
    """
    A class for tracking future results.

    This class will attempt to subscribe to a Pubnub channel to listen for
    job completion events. If you don't have access to Pubnub channels, then
    it will fallback to polling.

    This is a subclass of :class:`python:concurrent.futures.Future` from the
    Python standard library. See:
    https://docs.python.org/3/library/concurrent.futures.html

    Parameters
    ----------
    poller : func
        A function which returns an object that has a ``state`` attribute.
    poller_args : tuple
        The arguments with which to call the poller function.
    polling_interval : int or float, optional
        The number of seconds between API requests to check whether a result
        is ready.
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`civis.APIClient`, optional
    poll_on_creation : bool, optional
        If ``True`` (the default), it will poll upon calling ``result()`` the
        first time. If ``False``, it will wait the number of seconds specified
        in `polling_interval` from object creation before polling.

    Examples
    --------
    This example is provided as a function at :func:`~civis.io.query_civis`.

    >>> client = civis.APIClient()
    >>> database_id = client.get_database_id("my_database")
    >>> cred_id = client.default_credential
    >>> sql = "SELECT 1"
    >>> preview_rows = 10
    >>> response = client.queries.post(database_id, sql, preview_rows,
    >>>                                credential=cred_id)
    >>> job_id = response.id
    >>>
    >>> poller = client.queries.get
    >>> poller_args = (job_id, ) # (job_id, run_id) if poller requires run_id
    >>> polling_interval = 10
    >>> future = CivisFuture(poller, poller_args, polling_interval)
    """
    def __init__(self, poller, poller_args,
                 polling_interval=None, api_key=None, client=None,
                 poll_on_creation=True):
        if client is None:
            client = APIClient(api_key=api_key, resources='all')

        if polling_interval is None and hasattr(client, 'channels'):
            polling_interval = _LONG_POLLING_INTERVAL

        super().__init__(poller=poller,
                         poller_args=poller_args,
                         polling_interval=polling_interval,
                         api_key=api_key,
                         client=client,
                         poll_on_creation=poll_on_creation)

    def _begin_tracking(self, start_thread=False):
        # Be sure to subscribe to the PubNub channel before polling.
        # Otherwise the job might complete after polling and before
        # we subscribe, causing us to miss the notification.
        with self._condition:
            if hasattr(self.client, 'channels') and not self.subscribed:
                config, channels = self._pubnub_config()
                self._pubnub = self._subscribe(config, channels)
            super()._begin_tracking(start_thread)  # superclass does polling

    @property
    def subscribed(self):
        return (hasattr(self, '_pubnub') and
                len(self._pubnub.get_subscribed_channels()) > 0)

    def cleanup(self):
        with self._condition:
            super().cleanup()
            if hasattr(self, '_pubnub'):
                self._pubnub.unsubscribe_all()

                # Pubnub doesn't close its open session, so do that ourselves
                # to free up sockets. We have to access a private attribute,
                # but this exists at least in (4.0.0 <= versions <= 4.0.11).
                # After closing the Session, remove it so that PubNub
                # can't reopen it.
                self._pubnub._request_handler.session.close()
                del self._pubnub._request_handler.session

                # The PubNub object is no longer usable.
                # Note that it can't be garbage collected because of circular
                # references, so this represents a (small) memory leak.
                del self._pubnub

    def _subscribe(self, pnconfig, channels):
        listener = JobCompleteListener(self._check_message,
                                       self._poll_and_set_api_result,
                                       self._reset_polling_thread)
        pubnub = PubNub(pnconfig)
        pubnub.add_listener(listener)

        # Start our subscription 30 seconds in the past to catch any
        # missed messages.
        # https://www.pubnub.com/docs/python/api-reference-misc#time
        token = int((time.time() - 30) * 10**7)
        pubnub.subscribe().channels(channels).with_timetoken(token).execute()
        return pubnub

    def _pubnub_config(self):
        channel_config = self.client.channels.list()
        channels = [channel['name'] for channel in channel_config['channels']]
        pnconfig = PNConfiguration()
        pnconfig.subscribe_key = channel_config['subscribe_key']
        pnconfig.cipher_key = channel_config['cipher_key']
        pnconfig.auth_key = channel_config['auth_key']
        pnconfig.ssl = True
        pnconfig.reconnect_policy = PNReconnectionPolicy.EXPONENTIAL
        return pnconfig, channels

    def _check_message(self, message):
        try:
            # poller_args can be (job_id,) or (job_id, run_id)
            if len(self.poller_args) == 1:
                match = (message['object']['id'] == self.poller_args[0] and
                         message['run']['state'] in DONE)
            else:
                match = (message['object']['id'] == self.poller_args[0] and
                         message['run']['id'] == self.poller_args[1] and
                         message['run']['state'] in DONE)
        except KeyError:
            return False
        return match

    def _poll_and_set_api_result(self):
        with self._condition:
            try:
                result = self.poller(*self.poller_args)
                self._set_api_result(result)
            except Exception as e:
                self._set_api_exception(exc=e)


class ContainerFuture(CivisFuture):
    """Encapsulates asynchronous execution of a Civis Container Script

    This object includes the ability to cancel a run in progress,
    as well as the option to automatically retry failed runs.
    Retries should only be used for idempotent scripts which might fail
    because of network or other random failures.

    Parameters
    ----------
    job_id: int
        The ID for the container/script/job.
    run_id : int
        The ID for the run to monitor
    max_n_retries : int, optional
        If the job generates an exception, retry up to this many times
    polling_interval: int or float, optional
        The number of seconds between API requests to check whether a result
        is ready. You should not set this if you're using ``pubnub``
        (the default if ``pubnub`` is installed).
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    poll_on_creation : bool, optional
        If ``True`` (the default), it will poll upon calling ``result()`` the
        first time. If ``False``, it will wait the number of seconds specified
        in `polling_interval` from object creation before polling.

    See Also
    --------
    civis.futures.CivisFuture
    """
    def __init__(self, job_id, run_id,
                 max_n_retries=0,
                 polling_interval=None,
                 client=None,
                 poll_on_creation=True):
        if client is None:
            client = APIClient(resources='all')
        super().__init__(client.scripts.get_containers_runs,
                         [int(job_id), int(run_id)],
                         polling_interval=polling_interval,
                         client=client,
                         poll_on_creation=poll_on_creation)
        self._max_n_retries = max_n_retries

    @property
    def job_id(self):
        return self.poller_args[0]

    @property
    def run_id(self):
        return self.poller_args[1]

    def _set_api_exception(self, exc, result=None):
        # Catch attempts to set an exception. If there's retries
        # remaining, retry the run instead of erroring.
        with self._condition:
            if self._max_n_retries > 0:
                # Start a new run of the script and update
                # the run ID used by the poller.
                self.cleanup()
                self._last_result = self.client.jobs.post_runs(self.job_id)
                orig_run_id = self.run_id
                self.poller_args[1] = run_id = self._last_result.id
                self._max_n_retries -= 1
                self._last_polled = time.time()

                # Threads can only be started once, and the last thread
                # stopped in cleanup. Start a new polling thread.
                # Note that it's possible to have a race condition if
                # you shut down the old thread too soon after starting it.
                # In practice this only happens when testing retries
                # with extremely short polling intervals.
                self._begin_tracking(start_thread=True)

                log.debug('Job ID %d / Run ID %d failed. Retrying '
                          'with run %d. %d retries remaining.',
                          self.job_id, orig_run_id,
                          run_id, self._max_n_retries)
            else:
                super()._set_api_exception(exc=exc, result=result)

    def cancel(self):
        """Submit a request to cancel the container/script/run.

        Returns
        -------
        bool
            Whether or not the job is in a cancelled state.
        """
        with self._condition:
            if self.cancelled():
                return True
            elif not self.done():
                # Cancel the job and store the result of the cancellation in
                # the "finished result" attribute, `_result`.
                try:
                    self._result = self.client.scripts.post_cancel(self.job_id)
                except CivisAPIError as exc:
                    if exc.status_code == 404:
                        # The most likely way to get this error
                        # is for the job to already be completed.
                        return False
                    else:
                        warnings.warn("Unexpected error when attempting to "
                                      "cancel job ID %d / run ID %d:\n%s" %
                                      (self.job_id, self.run_id, str(exc)))
                        return False
                for waiter in self._waiters:
                    waiter.add_cancelled(self)
                self._condition.notify_all()
                self.cleanup()
                self._invoke_callbacks()
                return self.cancelled()
            return False


def _create_docker_command(*args, **kwargs):
    """
    Returns a string with the ordered arguments args in order,
    followed by the keyword arguments kwargs (in sorted order, for
    consistency), separated by spaces.

    For example,
    ``_create_docker_command('./myprogram', 5, 6, wibble=7, wobble=8)``
    returns ``"./myprogram 5 6 --wibble 7 --wobble 8"``.
    """
    return " ".join([str(x) for x in args] +
                    ["--{} {}".format(k, v)
                     for k, v in sorted(kwargs.items())])


@six.add_metaclass(ABCMeta)
class _CivisExecutor(Executor):
    def __init__(self,
                 name=None,
                 hidden=True,
                 max_n_retries=0,
                 client=None,
                 polling_interval=None,
                 inc_script_names=False):
        self.max_n_retries = max_n_retries
        self.hidden = hidden
        self.name = name
        self.polling_interval = polling_interval
        self.inc_script_names = inc_script_names
        self._script_name_counter = 0

        self._shutdown_lock = threading.Lock()
        self._shutdown_thread = False

        if client is None:
            client = APIClient(resources='all')
        self.client = client

        # A list of ContainerFuture objects for submitted jobs.
        self._futures = []

    def _make_future(self, job_id, run_id):
        """Instantiates a :class:`~civis.futures.ContainerFuture`,
        adds it to the internal list of futures, and returns it.
        This is a helper method for :func:`submit`.
        """
        future = ContainerFuture(job_id, run_id,
                                 polling_interval=self.polling_interval,
                                 max_n_retries=self.max_n_retries,
                                 client=self.client,
                                 poll_on_creation=False)

        self._futures.append(future)

        # Return a ContainerFuture object with the job ID.
        return future

    def submit(self, fn, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        This creates a container script with the command
        determined by the arguments.

        Parameters
        ----------
        fn: str or callable
            If this is a callable, it ``fn(*args, **kwargs)`` should return a
            ``str`` for the command to run in docker.  If ``None``, then
            ``_create_docker_command`` will be used.
        *args: args
            Additional arguments passed to ``fn``.
        arguments: dict, optional
            If provided, the created script will use the
            `arguments` dictionary from the class initialization
            updated with the dictionary provided to `submit`.
        **kwargs: kwargs
            Additional keyword arguments passed to ``fn``.

        Returns
        -------
        :class:`~civis.futures.ContainerFuture`
            Note that the ``Future`` returned by ``submit`` will
            provide the final status of your Container Script as
            its ``.result()``. The user is responsible for downloading
            outputs produced by the script, if any.
        """
        arguments = kwargs.pop('arguments', None)
        with self._shutdown_lock:
            if self._shutdown_thread:
                raise RuntimeError('cannot schedule new '
                                   'futures after shutdown')

            if isinstance(fn, six.string_types):
                cmd = fn
            else:
                if fn is None:
                    fn = _create_docker_command
                cmd = fn(*args, **kwargs)

            name = self.name
            if self.inc_script_names:
                name = "{} {}".format(name, self._script_name_counter)
                self._script_name_counter += 1

            job = self._create_job(name=name,
                                   arguments=arguments,
                                   cmd=cmd)
            run = self.client.jobs.post_runs(job.id)
            log.debug('Container "{}" created with script ID {} and '
                      'run ID {}'.format(name, job.id, run.id))

            return self._make_future(job.id, run.id)

    @abstractmethod
    def _create_job(self, name, arguments=None, cmd=None):
        raise NotImplementedError("Implement in the child class")

    def shutdown(self, wait=True):
        """Wait until all Civis jobs started by this are in done states

        Parameters
        ----------
        wait: bool
            If ``True``, then this will wait until all jobs are in a
            done (i.e., finished or cancelled) state.
        """
        with self._shutdown_lock:
            self._shutdown_thread = True

        if wait:
            futures.wait(self._futures)

    def cancel_all(self):
        """Send cancel requests for all running Civis jobs"""
        for f in self._futures:
            # The ContainerFuture is smart enough to only cancel the run
            # if the run is still in progress.
            f.cancel()


class _ContainerShellExecutor(_CivisExecutor):
    """Parallel computation with Container Scripts in the Civis Platform

    Create and run new Container Scripts in the Civis Platform.
    A Container Script is a command which runs inside a Docker container
    on the Civis Platform. Containers launched by the Executor may
    have either different shell commands or different arguments.

    This class follows the implementations in :ref:`concurrent.futures`,
    with necessary changes for parallelizing over different Container
    Script inputs rather than over functions.

    .. note:: If you expect to run a large number of jobs, you may
              wish to set automatic retries of failed jobs
              (via `max_n_retries`) to protect against network and
              infrastructure failures. Be careful with this if your
              jobs cause side effects other than returning a result;
              retries may cause any operations executed by your jobs
              to be run twice.

    Parameters
    ----------
    docker_image_name: str, optional
        The name of the Docker image to be used by Civis. You may also
        wish to specify a ``docker_image_tag`` in the keyword arguments.
    name: str, optional
        The name for containers in Civis.
        Defaults to "ContainerShellExecutorScript" followed by the date.
    required_resources: dict, optional
        A dictionary specifying what resources the job needs.
        See :func:`~APIClient.scripts.post_containers` for details.
        Defaults to 1 CPU and 1 GiB of RAM.
    hidden: bool, optional
        The hidden status of the object. Setting this to ``True`` hides it
        from most API endpoints. The object can still be queried
        directly by ID. Defaults to ``True``.
    max_n_retries: int, optional
        Retry failed jobs this many times before giving up.
        Retried jobs will be restarted with exactly the same parameters
        as they used the first time; only use this if you expect
        that your code is functional and errors would come from
        e.g. network problems.
    client: APIClient, optional
        The :class:`~civis.APIClient` object to use for interacting with the
        API. If not specified, a new one will be instantiated.
    polling_interval: int or float, optional
        The number of seconds between API requests to check whether a result
        is ready.  This will be passed to the
        :class:`~ContainerFuture` objects that are created. You should
        only set this if you aren't using ``pubnub`` notifications.
    inc_script_names: bool, optional
        If ``True``, a counter will be added to the ``name`` to create
        the script names for each submission.
    **kwargs:
        Additional keyword arguments will be passed
        directly to :func:`~civis.APIClient.scripts.post_containers`.

    See Also
    --------
    civis.APIClient.scripts.post_containers
    """
    def __init__(self, docker_image_name="civisanalytics/datascience-python",
                 name=None,
                 required_resources=None,
                 hidden=True,
                 max_n_retries=0,
                 client=None,
                 polling_interval=None,
                 inc_script_names=False,
                 **kwargs):
        self.docker_image_name = docker_image_name
        self.container_kwargs = kwargs

        if required_resources is None:
            required_resources = {'cpu': 1024, 'memory': 1024}
        self.required_resources = required_resources

        if name is None:
            date_str = datetime.datetime.today().strftime("%Y-%m-%d")
            name = "ContainerShellExecutorScript {}".format(date_str)

        super().__init__(name=name,
                         hidden=hidden,
                         client=client,
                         max_n_retries=max_n_retries,
                         polling_interval=polling_interval,
                         inc_script_names=inc_script_names)

    def _create_job(self, name, arguments=None, cmd=None):
        # Combine instance and input arguments into one dictionary.
        # Use `None` instead of an empty dictionary.
        kwargs = copy.deepcopy(self.container_kwargs)
        kwargs.setdefault('arguments', {}).update(arguments or {})
        if not kwargs['arguments']:
            del kwargs['arguments']

        # Submit a request to Civis to make the container script object.
        job = self.client.scripts.post_containers(
            name=name,
            required_resources=self.required_resources,
            docker_command=cmd,
            docker_image_name=self.docker_image_name,
            hidden=self.hidden,
            **self.container_kwargs
        )

        return job


class CustomScriptExecutor(_CivisExecutor):
    """Manage a pool of Custom Scripts in the Civis Platform

    Each Custom Script will be created from the same template, but may
    use different arguments. This class follows the implementations in
    :ref:`concurrent.futures`.

    .. note:: If you expect to run a large number of jobs, you may
              wish to set automatic retries of failed jobs
              (via `max_n_retries`) to protect against network and
              infrastructure failures. Be careful with this if your
              jobs cause side effects other than returning a result;
              retries may cause any operations executed by your jobs
              to be run twice.

    Parameters
    ----------
    from_template_id: int
        Create jobs as Custom Scripts from the given template ID.
    name: str
        The name for containers in Civis.
    hidden: bool, optional
        The hidden status of the object. Setting this to ``True`` hides it
        from most API endpoints. The object can still be queried
        directly by ID. Defaults to ``True``.
    arguments: dict, optional
        See :func:`~civis.APIClient.scripts.post_containers` for details.
    max_n_retries: int, optional
        Retry failed jobs this many times before giving up.
        Retried jobs will be restarted with exactly the same parameters
        as they used the first time; only use this if you expect
        that your code is functional and errors would come from
        e.g. network problems.
    client: APIClient, optional
        The :class:`~civis.APIClient` object to use for interacting with the
        API. If not specified, a new one will be instantiated.
    polling_interval: int or float, optional
        The number of seconds between API requests to check whether a result
        is ready.  This will be passed to the
        :class:`~ContainerFuture` objects that are created. You should
        only set this if you aren't using ``pubnub`` notifications.
    inc_script_names: bool, optional
        If ``True``, a counter will be added to the ``name`` to create
        the script names for each submission.

    See Also
    --------
    civis.APIClient.scripts.post_custom
    """
    def __init__(self, from_template_id,
                 name=None,
                 hidden=True,
                 arguments=None,
                 max_n_retries=0,
                 client=None,
                 polling_interval=None,
                 inc_script_names=False):
        self.from_template_id = from_template_id
        self.arguments = arguments

        if name is None:
            date_str = datetime.datetime.today().strftime("%Y-%m-%d")
            name = "CustomScriptExecutorScript {}".format(date_str)

        super().__init__(name=name,
                         hidden=hidden,
                         client=client,
                         max_n_retries=max_n_retries,
                         polling_interval=polling_interval,
                         inc_script_names=inc_script_names)

    def submit(self, **arguments):
        """Submit a Custom Script with the given arguments

        Parameters
        ----------
        arguments: dict, optional
            The created script will use the `arguments` dictionary
            from the class initialization updated with any additional
            keywords provided here.

        Returns
        -------
        :class:`~civis.futures.ContainerFuture`
        """
        return super().submit(fn=None, arguments=arguments)

    def _create_job(self, name, arguments=None, cmd=None):
        # Combine instance and input arguments into one dictionary.
        # Use `None` instead of an empty dictionary.
        combined_args = (self.arguments or {}).copy()
        combined_args.update((arguments or {}))
        if not combined_args:
            combined_args = None

        job = self.client.scripts.post_custom(
            self.from_template_id,
            name=name,
            arguments=combined_args,
            hidden=self.hidden)
        return job
