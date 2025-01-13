import os
import sys
import threading
import warnings
from concurrent import futures
from json.decoder import JSONDecodeError
from posixpath import join

import requests

import civis
from civis.response import PaginatedResponse, convert_response_data_type
from civis._utils import retry_request, DEFAULT_RETRYING

FINISHED = ["success", "succeeded"]
FAILED = ["failed"]
NOT_FINISHED = ["queued", "running"]
CANCELLED = ["cancelled"]
DONE = FINISHED + FAILED + CANCELLED

# Translate Civis state strings into `future` state strings
STATE_TRANS = {}
for name in FINISHED + FAILED:
    STATE_TRANS[name] = futures._base.FINISHED
for name in NOT_FINISHED:
    STATE_TRANS[name] = futures._base.RUNNING
for name in CANCELLED:
    STATE_TRANS[name] = futures._base.CANCELLED_AND_NOTIFIED


DEFAULT_API_ENDPOINT = "https://api.civisanalytics.com/"


def tostr_urljoin(*x):
    return join(*map(str, x))


class CivisJobFailure(Exception):
    def __init__(self, err_msg, response=None, job_id=None, run_id=None):
        self.job_id = job_id
        self.run_id = run_id
        self._original_err_msg = err_msg
        self.error_message = _err_msg_with_job_run_ids(err_msg, job_id, run_id)
        self.response = response

    def __str__(self):
        return self.error_message


def _err_msg_with_job_run_ids(err_msg, job_id, run_id) -> str:
    if job_id is None and run_id is None:
        return str(err_msg)
    elif run_id is None:
        return f"(From job {job_id}) {err_msg}"
    else:
        return f"(From job {job_id} / run {run_id}) {err_msg}"


class CivisAPIError(Exception):
    def __init__(self, response):
        if response.content:  # the API itself gave an error response
            try:
                json = response.json()
            except JSONDecodeError as e:
                if "Expecting value: line 1 column 1 (char 0)" in str(e):
                    self.error_message = "No Response Content from Civis API"
                else:
                    self.error_message = (
                        f"Response Content: " f"{(response.content or b'').decode()}"
                    )
            else:
                self.error_message = (
                    json.get("errorDescription") or "No Error Message Available"
                )
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
    base_url = os.environ.get("CIVIS_API_ENDPOINT", DEFAULT_API_ENDPOINT)
    if not base_url.endswith("/"):
        base_url += "/"
    return base_url


class Endpoint:

    _lock = threading.Lock()

    def __init__(self, session_kwargs, client, return_type="raw"):
        self._session_kwargs = session_kwargs
        self._return_type = return_type
        self._base_url = get_base_url()
        self._client = client

    def _build_path(self, path):
        if not path:
            return self._base_url
        return tostr_urljoin(self._base_url, path.strip("/"))

    @staticmethod
    def _handle_array_params(params):
        """Convert array-like parameters to the format that Civis API expects.

        Different APIs expect array-like parameters in different formats.
        For Civis API, an array parameter `foo` needs to be passed in as `foo[]`.
        Related reference: https://stackoverflow.com/a/23347265
        """
        if not params:
            return
        new_params = {}
        for key, value in params.items():
            if hasattr(value, "__iter__") and not isinstance(value, (dict, str)):
                new_params[f"{key}[]"] = list(value)
            else:
                new_params[key] = value
        return new_params

    def _make_request(self, method, path=None, params=None, data=None, **kwargs):
        url = self._build_path(path)
        params = self._handle_array_params(params)

        with self._lock:
            if self._client._retrying is None:
                retrying = self._session_kwargs.pop("retrying", None)
                self._client._retrying = retrying if retrying else DEFAULT_RETRYING
            with open_session(**self._session_kwargs) as sess:
                request = requests.Request(
                    method, url, json=data, params=params, **kwargs
                )
                pre_request = sess.prepare_request(request)
                response = retry_request(
                    method, pre_request, sess, self._client._retrying
                )

        if response.status_code == 401:
            auth_error = response.headers["www-authenticate"]
            raise CivisAPIKeyError(auth_error) from CivisAPIError(response)

        if not response.ok:
            raise CivisAPIError(response)

        return response

    def _call_api(
        self,
        method,
        path=None,
        params=None,
        data=None,
        deprecation_warning=None,
        **kwargs,
    ):
        if deprecation_warning:
            # stacklevel=3 to point to the call just outside civis-python
            warnings.warn(deprecation_warning, FutureWarning, stacklevel=3)

        iterator = kwargs.pop("iterator", False)

        if iterator:
            resp = PaginatedResponse(path, params, self)
        else:
            resp = self._make_request(method, path, params, data, **kwargs)
            resp = convert_response_data_type(
                resp,
                return_type=self._return_type,
                from_json_values=(path or "").startswith("json_values"),
            )
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
    """

    def __repr__(self):
        # Almost the same as the superclass's __repr__, except we use
        # the `_civis_state` rather than the `_state`.
        with self._condition:
            if self._civis_state in FINISHED + FAILED:
                if self.exception():
                    return "<%s at %#x state=%s raised %s>" % (
                        self.__class__.__name__,
                        id(self),
                        self._civis_state,
                        self._exception.__class__.__name__,
                    )
                else:
                    return "<%s at %#x state=%s returned %s>" % (
                        self.__class__.__name__,
                        id(self),
                        self._civis_state,
                        self.result().__class__.__name__,
                    )
            out = "<%s at %#x state=%s>" % (
                self.__class__.__name__,
                id(self),
                self._civis_state,
            )
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
            return "running"

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


def open_session(api_key, user_agent="civis-python"):
    """Create a new Session which can connect with the Civis API"""
    civis_version = civis.__version__
    session = requests.Session()
    session.auth = (api_key, "")
    session_agent = session.headers.get("User-Agent", "")
    ver_string = "{}.{}.{}".format(
        sys.version_info.major, sys.version_info.minor, sys.version_info.micro
    )
    user_agent = "{}/Python v{} Civis v{} {}".format(
        user_agent, ver_string, civis_version, session_agent
    )
    headers = {"User-Agent": user_agent.strip()}
    job_id, run_id = os.getenv("CIVIS_JOB_ID"), os.getenv("CIVIS_RUN_ID")
    if job_id:
        headers.update({"X-Civis-Job-ID": job_id, "X-Civis-Run-ID": run_id})
    session.headers.update(headers)

    return session
