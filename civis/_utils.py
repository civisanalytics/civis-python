from __future__ import absolute_import
from builtins import super
import os
import re
import uuid

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import Retry

import civis


UNDERSCORER1 = re.compile(r'(.)([A-Z][a-z]+)')
UNDERSCORER2 = re.compile('([a-z0-9])([A-Z])')


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
    user_agent = "{}/{} {}".format(user_agent, civis_version, session_agent)
    session.headers.update({"User-Agent": user_agent.strip()})
    max_retries = AggressiveRetry(max_retries, backoff_factor=.75,
                                  status_forcelist=civis.civis.RETRY_CODES)
    adapter = HTTPAdapter(max_retries=max_retries)
    session.mount("https://", adapter)

    return session


class AggressiveRetry(Retry):
    # Subclass Retry so that it retries more things. In particular,
    # always retry API requests with a Retry-After header, regardless
    # of the verb.
    def is_retry(self, method, status_code, has_retry_after=False):
        """ Is this method/status code retryable? (Based on whitelists and control
        variables such as the number of total retries to allow, whether to
        respect the Retry-After header, whether this header is present, and
        whether the returned status code is on the list of status codes to
        be retried upon on the presence of the aforementioned header)
        """
        if (self.total and
                self.respect_retry_after_header and
                has_retry_after and
                (status_code in self.RETRY_AFTER_STATUS_CODES)):
            return True

        else:
            return super().is_retry(method=method, status_code=status_code,
                                    has_retry_after=has_retry_after)
