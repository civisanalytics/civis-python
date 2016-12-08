from posixpath import join
import threading

from civis.response import PaginatedResponse, convert_response_data_type


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


class Endpoint:

    _base_url = "https://api.civisanalytics.com/"
    _lock = threading.Lock()

    def __init__(self, session, return_type='civis'):
        self._session = session
        self._return_type = return_type

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
            raise CivisAPIKeyError(auth_error) from CivisAPIError(response)

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
