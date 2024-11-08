import logging
import os

import tenacity
from tenacity.wait import wait_base


log = logging.getLogger(__name__)

_RETRY_CODES = [429, 502, 503, 504]
_RETRY_VERBS = ["HEAD", "TRACE", "GET", "PUT", "OPTIONS", "DELETE"]
_POST_RETRY_CODES = [429, 503]


# Defining the default tenacity.Retrying as a user-friendly code string
# so that it can be shown in civis.APIClient's docstring.
DEFAULT_RETRYING_STR = """
tenacity.Retrying(
    wait=tenacity.wait_random_exponential(multiplier=2, max=60),
    stop=(tenacity.stop_after_delay(600) | tenacity.stop_after_attempt(10)),
    retry_error_callback=lambda retry_state: retry_state.outcome.result(),
)
"""

# Explicitly set the available globals and locals
# to mitigate risk of unwanted code execution
DEFAULT_RETRYING = eval(  # nosec
    DEFAULT_RETRYING_STR,
    {"tenacity": tenacity, "__builtins__": {}},  # globals
    {},  # locals
)


def get_api_key(api_key):
    """Pass-through if `api_key` is not None otherwise tries the CIVIS_API_KEY
    environment variable.
    """
    if api_key is not None:  # always prefer user given one
        return api_key
    api_key = os.environ.get("CIVIS_API_KEY", None)
    if api_key is None:
        raise EnvironmentError(
            "No Civis API key found. Please store in "
            "CIVIS_API_KEY environment variable"
        )
    return api_key


def retry_request(method, prepared_req, session, retrying=None):
    retry_conditions = None
    retrying = retrying if retrying else DEFAULT_RETRYING

    def _make_request(req, sess):
        """send the prepared session request"""
        response = sess.send(req)
        return response

    if method.upper() == "POST":
        retry_conditions = tenacity.retry_if_result(
            lambda res: res.status_code in _POST_RETRY_CODES
        )
    elif method.upper() in _RETRY_VERBS:
        retry_conditions = tenacity.retry_if_result(
            lambda res: res.status_code in _RETRY_CODES
        )

    if retry_conditions:
        retrying.retry = retry_conditions
        retrying.wait = wait_for_retry_after_header(fallback=retrying.wait)
        response = retrying(_make_request, prepared_req, session)
        return response

    response = _make_request(prepared_req, session)
    return response


class wait_for_retry_after_header(wait_base):
    """Wait strategy that first looks for Retry-After header. If not
    present it uses the fallback strategy as the wait param"""

    def __init__(self, fallback):
        self.fallback = fallback

    def __call__(self, retry_state):
        # retry_state is an instance of tenacity.RetryCallState.
        # The .outcome property contains the result/exception
        # that came from the underlying function.
        result_headers = retry_state.outcome._result.headers
        retry_after = result_headers.get("Retry-After") or result_headers.get(
            "retry-after"
        )

        try:
            log.info("Retrying after {} seconds".format(retry_after))
            return int(retry_after)
        except (TypeError, ValueError):
            pass
        return self.fallback(retry_state)
