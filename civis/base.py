from __future__ import absolute_import
import os
from posixpath import join
import threading
from concurrent import futures
import six

from civis.response import PaginatedResponse, convert_response_data_type

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
    def __init__(self, err_msg, response=None):
        self.error_message = err_msg
        self.response = response

    def __str__(self):
        return self.error_message


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


def get_base_url():
    base_url = os.environ.get('CIVIS_API_ENDPOINT', DEFAULT_API_ENDPOINT)
    if not base_url.endswith('/'):
        base_url += '/'
    return base_url


class Endpoint(object):

    _lock = threading.Lock()

    def __init__(self, session, return_type='civis'):
        self._session = session
        self._return_type = return_type
        self._base_url = get_base_url()

    def _build_path(self, path):
        if not path:
            return self._base_url
        return tostr_urljoin(self._base_url, path.strip("/"))

    def _make_request(self, method, path=None, params=None, data=None,
                      **kwargs):
        url = self._build_path(path)

        with self._lock:
            response = self._session.request(method, url, json=data,
                                             params=params, **kwargs)

        if response.status_code == 401:
            auth_error = response.headers["www-authenticate"]
            six.raise_from(CivisAPIKeyError(auth_error),
                           CivisAPIError(response))

        if not response.ok:
            raise CivisAPIError(response)

        return response

    def _call_api(self, method, path=None, params=None, data=None, **kwargs):
        iterator = kwargs.pop('iterator', False)

        if iterator:
            return PaginatedResponse(path, params, self)
        else:
            resp = self._make_request(method, path, params, data, **kwargs)
            resp = convert_response_data_type(resp,
                                              return_type=self._return_type)
            return resp
