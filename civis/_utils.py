from builtins import super
import logging
import os
import re
import sys
# import time
import uuid

import requests
# from requests import HTTPError
import tenacity
import logging
logging.basicConfig(stream=tenacity.sys.stderr, level=logging.DEBUG)
logger = logging.getLogger(__name__)
# from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util import Retry

import civis

log = logging.getLogger(__name__)
UNDERSCORER1 = re.compile(r'(.)([A-Z][a-z]+)')
UNDERSCORER2 = re.compile('([a-z0-9])([A-Z])')

RETRY_EXCEPTIONS = (requests.exceptions.HTTPError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ConnectTimeout)


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

def open_session(api_key, max_retries=5, user_agent="civis-python"):
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
    # tearout
    # max_retries = AggressiveRetry(max_retries, backoff_factor=.75,
    #                               status_forcelist=civis.civis.RETRY_CODES)
    # adapter = HTTPAdapter(max_retries=max_retries)
    # session.mount("https://", adapter)

    return session


# Retry-After header is present, we use that value for the retry interval.
def retry_configuration(max_retries=10):
    r = tenacity.Retrying(
        retry=(tenacity.retry_if_exception_type(civis.base.APIRetryError)),
        # Randomly wait up to 2^x * 2 seconds between each retry until the range reaches 60 seconds, then randomly up to 60 seconds afterwards
        wait=tenacity.wait_random_exponential(multiplier=2, max=60),
        stop=(
                tenacity.stop_after_delay(600) |
                tenacity.stop_after_attempt(max_retries)
        ),
        # using for testing
        before=tenacity.before_log(logger, logging.ERROR))

    return r


def check_retry_valid(method, status_code):
    if method in civis.civis.RETRY_VERBS and status_code in civis.civis.RETRY_CODES:
        return True
    elif method == 'post' and status_code in civis.civis.POST_RETRY_CODES:
        return True
    return False

# @retry(
#     retry=(retry_if_exception_type(requests.exceptions.HTTPError)),
#     # Randomly wait up to 2^x * 2 seconds between each retry until the range reaches 60 seconds, then randomly up to 60 seconds afterwards
#     wait=wait_random_exponential(multiplier=2, max=60),
#     stop=(
#             stop_after_delay(600) |
#             stop_after_attempt(5)
#     ),
#     # using for testing
#     before=before_log(logger, logging.ERROR),
# )
# tearout
# class AggressiveRetry(Retry):
#     # Subclass Retry so that it retries more things. In particular,
#     # always retry API requests with a Retry-After header, regardless
#     # of the verb.
#     def is_retry(self, method, status_code, has_retry_after=False):
#         """ Is this method/status code retryable? (Based on whitelists and control
#         variables such as the number of total retries to allow, whether to
#         respect the Retry-After header, whether this header is present, and
#         whether the returned status code is on the list of status codes to
#         be retried upon on the presence of the aforementioned header)
#         """
#         if (self.total and
#                 self.respect_retry_after_header and
#                 has_retry_after and
#                 (status_code in self.RETRY_AFTER_STATUS_CODES)):
#             return True
#
#         else:
#             return super().is_retry(method=method, status_code=status_code,
#                                     has_retry_after=has_retry_after)
#
#
# tearout
# def retry(exceptions, retries=5, delay=0.5, backoff=2):
#     """
#     Retry decorator
#
#     Parameters
#     ----------
#     exceptions: Exception
#         exceptions to trigger retry
#     retries: int, optional
#         number of retries to perform
#     delay: float, optional
#         delay before next retry
#     backoff: int, optional
#         factor used to increase delay after each retry
#
#     Returns
#     -------
#     retry decorator
#
#     Raises
#     ------
#     exception raised by decorator function
#     """
#     def deco_retry(f):
#         def f_retry(*args, **kwargs):
#             n_failed = 0
#             new_delay = delay
#             while True:
#                 try:
#                     return f(*args, **kwargs)
#                 except exceptions as exc:
#                     if n_failed < retries:
#                         n_failed += 1
#                         msg = "%s, Retrying in %d seconds..." % \
#                               (str(exc), new_delay)
#                         log.debug(msg)
#                         time.sleep(new_delay)
#                         new_delay *= backoff
#                     else:
#                         raise exc
#
#         return f_retry
#
#     return deco_retry


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
