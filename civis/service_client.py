import civis
from collections import OrderedDict
import re
import six
import warnings

from jsonref import JsonRef
import requests

from civis.base import Endpoint, CivisAPIError, CivisAPIKeyError, tostr_urljoin
from civis.resources._resources import parse_method
from civis._utils import to_camelcase


def auth_service_session(session, service_id):
    # civis api error?
    service = civis.APIClient().services.get(service_id)
    auth_url = service['current_deployment']['displayUrl']
    # Make request for Authentication Cookie
    session.get(auth_url)


class ServiceEndpoint(Endpoint):

    def __init__(self, session_kwargs, client,
                 return_type='civis', root_path=None):
        self._session_kwargs = session_kwargs
        self._return_type = return_type
        self._client = client
        self._root_path = root_path

    def _build_path(self, path):
        if not path:
            return self._client._base_url
        if self._root_path is None:
            return tostr_urljoin(self._client._base_url, path.strip("/"))
        return tostr_urljoin(self._client._base_url,
                             self._root_path.strip("/"),
                             path.strip("/"))

    def _make_request(self, method, path=None, params=None, data=None,
                      **kwargs):
        url = self._build_path(path)

        with requests.Session() as sess:
            auth_service_session(sess, self._client._service_id)
            with self._lock:
                response = sess.request(method, url, json=data,
                                        params=params, **kwargs)

        if response.status_code == 401:
            auth_error = response.headers["www-authenticate"]
            six.raise_from(CivisAPIKeyError(auth_error),
                           CivisAPIError(response))

        if not response.ok:
            raise CivisAPIError(response)

        return response


class ServiceClient():

    def __init__(self, service_id, root_path=None, swagger_path="/endpoints"):
        return_type = 'snake'
        self._session_kwargs = {}
        self._service_id = service_id
        self._base_url = self.get_base_url()
        self._root_path = root_path
        self._swagger_path = swagger_path
        # Catch deprecation warnings from generate_classes_maybe_cached and
        # the functions it calls until the `resources` argument is removed.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=FutureWarning,
                module='civis')
            classes = self.generate_classes()
        for class_name, cls in classes.items():
            setattr(self, class_name, cls(self._session_kwargs, client=self,
                                          return_type=return_type,
                                          root_path=root_path))

    def parse_path(self, path, operations):
        """ Parse an endpoint into a class where each valid http request
        on that endpoint is converted into a convenience function and
        attached to the class as a method.
        """
        if self._root_path is not None:
            path = path.replace(self._root_path, '')
        path = path.strip('/')
        modified_base_path = re.sub("-", "_", path.split('/')[0].lower())
        methods = []
        for verb, op in operations.items():
            method = parse_method(verb, op, path)
            if method is None:
                continue
            methods.append(method)
        return modified_base_path, methods

    def parse_api_spec(self, api_spec):
        paths = api_spec['paths']
        classes = {}
        for path, ops in paths.items():
            base_path, methods = self.parse_path(path, ops)
            class_name = to_camelcase(base_path)
            if methods and classes.get(base_path) is None:
                classes[base_path] = type(str(class_name),
                                          (ServiceEndpoint,),
                                          {})
            for method_name, method in methods:
                setattr(classes[base_path], method_name, method)
        return classes

    def get_api_spec(self):
        swagger_url = self._base_url + self._swagger_path

        # add swagger validation ?

        with requests.Session() as sess:
            auth_service_session(sess, self._service_id)
            response = sess.get(swagger_url)
            response.raise_for_status()
        spec = response.json(object_pairs_hook=OrderedDict)
        return spec

    def generate_classes(self):
        raw_spec = self.get_api_spec()
        spec = JsonRef.replace_refs(raw_spec)
        return self.parse_api_spec(spec)

    def get_base_url(self):
        try:
            client = civis.APIClient()
            service = client.services.get(self._service_id)
        except CivisAPIError as api_err:
            if api_err.status_code == 404:
                msg = ('There is no Civis Service with '
                       'ID {}!'.format(self._service_id))
                six.raise_from(ValueError(msg), api_err)
            raise
        return service['current_url']
