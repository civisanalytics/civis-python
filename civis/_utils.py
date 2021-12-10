import logging
import os
import re
import sys
import time
import uuid
from random import random

import requests
from tenacity import (Retrying, retry_if_result, stop_after_attempt,
                      stop_after_delay, wait_random_exponential)
from tenacity.wait import wait_base

import civis

log = logging.getLogger(__name__)
UNDERSCORER1 = re.compile(r'(.)([A-Z][a-z]+)')
UNDERSCORER2 = re.compile('([a-z0-9])([A-Z])')
MAX_RETRIES = 10


def maybe_get_random_name(name):
    if not name:
        name = uuid.uuid4().hex
    return name


def camel_to_snake(word):
    # https://gist.github.com/jaytaylor/3660565
    word = UNDERSCORER1.sub(r'\1_\2', word)
    return UNDERSCORER2.sub(r'\1_\2', word).lower()


def to_camelcase(s):
    return re.sub(r'(^|_)([a-zA-Z])', lambda m: m.group(2).upper(), s)


def get_api_key(api_key):
    """Pass-through if `api_key` is not None otherwise tries the CIVIS_API_KEY
    environmental variable.
    """
    if api_key is not None:  # always prefer user given one
        return api_key
    api_key = os.environ.get("CIVIS_API_KEY", None)
    if api_key is None:
        raise EnvironmentError("No Civis API key found. Please store in "
                               "CIVIS_API_KEY environment variable")
    return api_key


def open_session(api_key, user_agent="civis-python"):
    """Create a new Session which can connect with the Civis API"""
    civis_version = civis.__version__
    session = requests.Session()
    session.auth = (api_key, '')
    session_agent = session.headers.get('User-Agent', '')
    ver_string = "{}.{}.{}".format(sys.version_info.major,
                                   sys.version_info.minor,
                                   sys.version_info.micro)
    user_agent = "{}/Python v{} Civis v{} {}".format(
        user_agent, ver_string, civis_version, session_agent)
    headers = {"User-Agent": user_agent.strip()}
    job_id, run_id = os.getenv("CIVIS_JOB_ID"), os.getenv("CIVIS_RUN_ID")
    if job_id:
        headers.update({"X-Civis-Job-ID": job_id, "X-Civis-Run-ID": run_id})
    session.headers.update(headers)

    return session


def retry_request(method, prepared_req, session, max_retries=10):
    retry_conditions = None

    def _make_request(req, sess):
        """send the prepared session request"""
        response = sess.send(req)
        return response

    def _return_last_value(retry_state):
        """return the result of the last call attempt
        and let code pick up the error"""
        return retry_state.outcome.result()

    if method.upper() == 'POST':
        retry_conditions = (
            retry_if_result(
                lambda res: res.status_code in civis.civis.POST_RETRY_CODES)
        )
    elif method.upper() in civis.civis.RETRY_VERBS:
        retry_conditions = (
            retry_if_result(
                lambda res: res.status_code in civis.civis.RETRY_CODES)
        )

    if retry_conditions:
        retry_config = Retrying(
            retry=retry_conditions,
            wait=wait_for_retry_after_header(
                fallback=wait_random_exponential(multiplier=2, max=60)),
            stop=(stop_after_delay(600) | stop_after_attempt(max_retries)),
            retry_error_callback=_return_last_value,
        )
        response = retry_config(_make_request, prepared_req, session)
        return response

    response = _make_request(prepared_req, session)
    return response


def retry(exceptions, retries=5, delay=0.5, backoff=2):
    """
    Retry decorator

    Parameters
    ----------
    exceptions: Exception
        exceptions to trigger retry
    retries: int, optional
        number of retries to perform
    delay: float, optional
        delay before next retry
    backoff: int, optional
        factor used to calculate the exponential increase
        delay after each retry

    Returns
    -------
    retry decorator

    Raises
    ------
    exception raised by decorator function
    """
    def deco_retry(f):
        def f_retry(*args, **kwargs):
            n_failed = 0
            new_delay = delay
            while True:
                try:
                    return f(*args, **kwargs)
                except exceptions as exc:
                    if n_failed < retries:
                        n_failed += 1
                        msg = "%s, Retrying in %d seconds..." % \
                              (str(exc), new_delay)
                        log.debug(msg)
                        time.sleep(new_delay)
                        new_delay = min(
                            (pow(2, n_failed) / 4) *
                            (random() + backoff), 50 + 10 * random()  # nosec
                        )
                    else:
                        raise exc

        return f_retry

    return deco_retry


class BufferedPartialReader(object):
    def __init__(self, buf, max_bytes):
        self.buf = buf
        self.max_bytes = max_bytes
        self.bytes_read = 0
        self.len = max_bytes

    def read(self, size=-1):
        if self.bytes_read >= self.max_bytes:
            return b''
        bytes_left = self.max_bytes - self.bytes_read
        if size < 0:
            bytes_to_read = bytes_left
        else:
            bytes_to_read = min(size, bytes_left)
        data = self.buf.read(bytes_to_read)
        self.bytes_read += len(data)
        return data


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
        retry_after = result_headers.get("Retry-After") or \
            result_headers.get("retry-after")

        try:
            log.info('Retrying after {} seconds'.format(retry_after))
            return int(retry_after)
        except (TypeError, ValueError):
            pass
        return self.fallback(retry_state)
