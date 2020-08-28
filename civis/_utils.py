import logging
import os
import re
import sys
import time
import uuid

import requests
import tenacity
from tenacity import (
    Retrying,
    retry_if_result,
    stop_after_attempt,
    stop_after_delay,
    wait_random_exponential
)
# TEAROUT
from tenacity import after_log, before_log
logging.basicConfig(stream=tenacity.sys.stderr, level=logging.DEBUG)
logger = logging.getLogger(__name__)

import civis


log = logging.getLogger(__name__)
UNDERSCORER1 = re.compile(r'(.)([A-Z][a-z]+)')
UNDERSCORER2 = re.compile('([a-z0-9])([A-Z])')
MAX_RETRIES = 2


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
    session.headers.update({"User-Agent": user_agent.strip()})

    return session


# Retry-After header is present, we use that value for the retry interval.
def retry_request(method, prepared_req, session, max_retries=10):

    def _make_request(req, sess):
        """send the prepared session request"""
        response = sess.send(req)
        return response

    def _return_last_value(retry_state):
        """return the result of the last call attempt and let code pick up the error"""
        return retry_state.outcome.result()

    if method == 'post':
        retry_conditions = (retry_if_result(lambda res: res.status_code in civis.civis.POST_RETRY_CODES))
    elif method in civis.civis.RETRY_VERBS:
        retry_conditions = (retry_if_result(lambda res: res.status_code in civis.civis.RETRY_CODES))

    if retry_conditions:
        retry_config = Retrying(
            retry=retry_conditions,
            wait=wait_random_exponential(multiplier=2, max=60),
            stop=(stop_after_delay(600) | stop_after_attempt(max_retries)),
            retry_error_callback=_return_last_value,
            # using for testing
            # TEAROUT
            before=before_log(logger, logging.INFO),
            after=after_log(logger, logging.INFO),
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
        factor used to increase delay after each retry

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
                        new_delay *= backoff
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
