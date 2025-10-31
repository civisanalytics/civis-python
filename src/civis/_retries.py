import logging

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
    reraise=True,
)
"""


def get_default_retrying():
    """Return a new instance of the default tenacity.Retrying."""
    # Explicitly set the available globals and locals
    # to mitigate risk of unwanted code execution
    return eval(  # nosec
        DEFAULT_RETRYING_STR,
        {"tenacity": tenacity, "__builtins__": {}},  # globals
        {},  # locals
    )


def retry_request(method, prepared_req, session, retrying=None):
    retry_conditions = None

    # New tenacity.Retrying instance needed, whether it's a copy of the user-provided
    # one or it's one based on civis-python's default settings.
    retrying = retrying.copy() if retrying else get_default_retrying()

    # If retries are exhausted,
    # raise the last exception encountered, not tenacity's RetryError.
    retrying.reraise = True

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
        retrying.wait = wait_at_least_retry_after_header(base=retrying.wait)
        response = retrying(_make_request, prepared_req, session)
        return response

    response = _make_request(prepared_req, session)
    return response


class wait_at_least_retry_after_header(wait_base):
    """Wait strategy for at least `Retry-After` seconds (if present from header)"""

    def __init__(self, base):
        self.base = base

    def __call__(self, retry_state):
        # retry_state is an instance of tenacity.RetryCallState.
        # The .outcome property contains the result/exception
        # that came from the underlying function.
        headers = retry_state.outcome._result.headers

        try:
            retry_after = float(
                headers.get("Retry-After") or headers.get("retry-after") or "0.0"
            )
        except (TypeError, ValueError):
            retry_after = 0.0
        # Wait at least retry_after seconds (compared to the user-specified wait).
        return max(retry_after, self.base(retry_state))
