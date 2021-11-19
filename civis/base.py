import os
from posixpath import join
import threading
from concurrent import futures
import warnings
from requests import Request

from civis.response import PaginatedResponse, convert_response_data_type
from civis._utils import open_session, retry_request, MAX_RETRIES

FINISHED = ['success', 'succeeded']
FAILED = ['failed']
NOT_FINISHED = ['queued', 'running']
CANCELLED = ['cancelled']
DONE = FINISHED + FAILED + CANCELLED

# Translate Civis state strings into `future` state strings
STATE_TRANS = {}
for name in FINISHED + FAILED:
    STATE_TRANS[name] = futures._base.FINISHED
for name in NOT_FINISHED:
    STATE_TRANS[name] = futures._base.RUNNING
for name in CANCELLED:
    STATE_TRANS[name] = futures._base.CANCELLED_AND_NOTIFIED


DEFAULT_API_ENDPOINT = 'https://api.civisanalytics.com/'


def tostr_urljoin(*x):
    return join(*map(str, x))


class CivisJobFailure(Exception):
    def __init__(self, err_msg, response=None, job_id=None, run_id=None):
        self.job_id = job_id
        self.run_id = run_id
        self._original_err_msg = err_msg
        self.error_message = _err_msg_with_job_run_ids(
            err_msg, job_id, run_id)
        self.response = response

    def __str__(self):
        return self.error_message


def _err_msg_with_job_run_ids(err_msg, job_id, run_id) -> str:
    if job_id is None and run_id is None:
        return err_msg
    elif run_id is None:
        return f"(From job {job_id}) {err_msg}"
    else:
        return f"(From job {job_id} / run {run_id}) {err_msg}"


class CivisAPIError(Exception):
    def __init__(self, response):
        if response.content:  # the API itself gave an error response
            json = response.json()
            self.error_message = json["errorDescription"]
        else:  # this was something like a 502
            self.error_message = response.reason

        self.status_code = response.status_code
        self._response = response

    def __str__(self):
        if self.status_code:
            return "({}) {}".format(self.status_code, self.error_message)
        else:
            return self.error_message


class EmptyResultError(Exception):
    pass


class CivisAPIKeyError(Exception):
    pass


class CivisImportError(Exception):
    pass


def get_base_url():
    base_url = os.environ.get('CIVIS_API_ENDPOINT', DEFAULT_API_ENDPOINT)
    if not base_url.endswith('/'):
        base_url += '/'
    return base_url


class Endpoint(object):

    _lock = threading.Lock()

    def __init__(self, session_kwargs, client, return_type='civis'):
        self._session_kwargs = session_kwargs
        self._return_type = return_type
        self._base_url = get_base_url()
        self._client = client

    def _build_path(self, path):
        if not path:
            return self._base_url
        return tostr_urljoin(self._base_url, path.strip("/"))

    def _make_request(self, method, path=None, params=None, data=None,
                      **kwargs):
        url = self._build_path(path)

        with self._lock:
            with open_session(**self._session_kwargs) as sess:
                request = Request(method, url, json=data,
                                  params=params, **kwargs)
                pre_request = sess.prepare_request(request)
                response = retry_request(method, pre_request,
                                         sess, MAX_RETRIES)

        if response.status_code == 401:
            auth_error = response.headers["www-authenticate"]
            raise CivisAPIKeyError(auth_error) from CivisAPIError(response)

        if not response.ok:
            raise CivisAPIError(response)

        return response

    def _call_api(self, method, path=None, params=None, data=None, **kwargs):
        iterator = kwargs.pop('iterator', False)

        if iterator:
            resp = PaginatedResponse(path, params, self)
        else:
            resp = self._make_request(method, path, params, data, **kwargs)
            resp = convert_response_data_type(resp,
                                              return_type=self._return_type)
        self._client.last_response = resp
        return resp


class CivisAsyncResultBase(futures.Future):
    """A base class for tracking asynchronous results.

    Sub-classes needs to call either the `set_result` method to set a result
    when it finishes or call `set_exception` if there is an error.

    The `_result` attribute can also be set to change the current state of
    the result. The `_result_` object needs to be set to an object with a
    ``state`` attribute. Alternatively the `_check_result` method can be
    overwritten to change how the state of the object is returned.

    Parameters
    ----------
    poller : func
        A function which returns an object that has a ``state`` attribute.
    poller_args : tuple
        The arguments with which to call the poller function.
    polling_interval : int or float
        The number of seconds between API requests to check whether a result
        is ready.
    api_key : DEPRECATED str, optional
        Your Civis API key. If not given, the :envvar:`CIVIS_API_KEY`
        environment variable will be used.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    poll_on_creation : bool, optional
        If ``True`` (the default), it will poll upon calling ``result()`` the
        first time. If ``False``, it will wait the number of seconds specified
        in `polling_interval` from object creation before polling.
    """
    def __init__(self, poller, poller_args,
                 polling_interval=None, api_key=None, client=None,
                 poll_on_creation=True):
        super().__init__()
        self.poller = poller
        self.poller_args = poller_args
        self.polling_interval = polling_interval
        if api_key is not None:
            warnings.warn('The "api_key" parameter is deprecated and will be '
                          'removed in v2. Please use the `client` parameter '
                          'instead.', FutureWarning)
        self.client = client
        self.poll_on_creation = poll_on_creation

    def __repr__(self):
        # Almost the same as the superclass's __repr__, except we use
        # the `_civis_state` rather than the `_state`.
        with self._condition:
            if self._civis_state in FINISHED + FAILED:
                if self.exception():
                    return '<%s at %#x state=%s raised %s>' % (
                        self.__class__.__name__,
                        id(self),
                        self._civis_state,
                        self._exception.__class__.__name__)
                else:
                    return '<%s at %#x state=%s returned %s>' % (
                        self.__class__.__name__,
                        id(self),
                        self._civis_state,
                        self.result().__class__.__name__)
            out = '<%s at %#x state=%s>' % (self.__class__.__name__,
                                            id(self),
                                            self._civis_state)
            return out

    def cancel(self):
        """Not currently implemented."""
        raise NotImplementedError("Running jobs cannot currently be cancelled")

    def succeeded(self):
        """Return ``True`` if the job completed in Civis with no error."""
        with self._condition:
            return self._civis_state in FINISHED

    def failed(self):
        """Return ``True`` if the Civis job failed."""
        with self._condition:
            return self._civis_state in FAILED

    def _check_result(self):
        with self._condition:
            if self._result is not None:
                return self._result

    @property
    def _civis_state(self):
        """State as returned from Civis."""
        with self._condition:
            if self._check_result():
                return self._check_result().state
            return 'running'

    @property
    def _state(self):
        """State of the CivisAsyncResultBase in `future` language."""
        with self._condition:
            return STATE_TRANS[self._civis_state]

    @_state.setter
    def _state(self, value):
        # Ignore attempts to set the _state from the `Future` superclass
        pass

    def set_result(self, result):
        """Sets the return value of work associated with the future.

        This is adapted from
        https://github.com/python/cpython/blob/3.8/Lib/concurrent/futures/_base.py#L517-L530
        This version does not try to change the _state or check that the
        initial _state is running since the Civis implementation has _state
        depend on the Platform job state.
        """
        with self._condition:
            self._result = result
            for waiter in self._waiters:
                waiter.add_result(self)
            self._condition.notify_all()
        self._invoke_callbacks()

    def set_exception(self, exception):
        """Sets the result of the future as being the given exception.

        This is adapted from
        https://github.com/python/cpython/blob/3.8/Lib/concurrent/futures/_base.py#L532-L545
        This version does not try to change the _state or check that the
        initial _state is running since the Civis implementation has _state
        depend on the Platform job state.
        """
        with self._condition:
            self._exception = exception
            for waiter in self._waiters:
                waiter.add_exception(self)
            self._condition.notify_all()
        self._invoke_callbacks()
