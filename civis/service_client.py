from __future__ import absolute_import

from collections import OrderedDict
import re

from jsonref import JsonRef
import requests
import six

from civis import APIClient
from civis.base import Endpoint, tostr_urljoin
from civis.resources._resources import parse_method
from civis._utils import to_camelcase


def _get_service(client):
    if client._api_key:
        api_client = APIClient(client._api_key)
    else:
        api_client = APIClient()
    service = api_client.services.get(client._service_id)
    return service


def auth_service_session(session, client):
    service = _get_service(client)
    auth_url = service['current_deployment']['displayUrl']
    # Make request for adding Authentication Cookie to session
    session.get(auth_url)


class ServiceEndpoint(Endpoint):

    def __init__(self, client,
                 return_type='civis'):
        self._return_type = return_type
        self._client = client

    def _build_path(self, path):
        if not path:
            return self._client._base_url
        if not self._client._root_path:
            return tostr_urljoin(self._client._base_url, path.strip("/"))
        return tostr_urljoin(self._client._base_url,
                             self._client._root_path.strip("/"),
                             path.strip("/"))

    def _make_request(self, method, path=None, params=None, data=None,
                      **kwargs):
        url = self._build_path(path)

        with requests.Session() as sess:
            auth_service_session(sess, self._client)
            with self._lock:
                response = sess.request(method, url, json=data,
                                        params=params, **kwargs)

        if not response.ok:
            six.raise_from(ValueError(response.text),
                           ValueError)

        return response


class ServiceClient():

    def __init__(self, service_id, root_path=None,
                 swagger_path="/endpoints", api_key=None,
                 return_type='snake'):
        """Create an API Client from a Civis service.

        Parameters
        ----------
        service_id : str, required
            The Id for the service that will be used to generate the client.
        root_path : str, optional
            An additional path for APIs that are not hosted on the service's
            root level. An example root_path would be '/api' for an app with
            resource endpoints that all begin with '/api'.
        swagger_path : str, optional
            The endpoint path that will be used to download the API Spec.
            The default value is '/endpoints' but another common path
            might be '/spec'. The API Spec must be compliant with Swagger
            2.0 standards.
        api_key : str, optional
            Your API key obtained from the Civis Platform. If not given, the
            client will use the :envvar:`CIVIS_API_KEY` environment variable.
            This API key will need to be authorized to access the service
            used for the client.
        return_type : str, optional
            The following types are implemented:

            - ``'raw'`` Returns the raw :class:`requests:requests.Response`
            object.
            - ``'snake'`` Returns a :class:`civis.response.Response` object
            for the json-encoded content of a response. This maps the
            top-level json keys to snake_case.
            - ``'pandas'`` Returns a :class:`pandas:pandas.DataFrame` for
            list-like responses and a :class:`pandas:pandas.Series` for
            single a json response.
        """
        if return_type not in ['snake', 'raw', 'pandas']:
            raise ValueError("Return type must be one of 'snake', 'raw', "
                             "'pandas'")
        self._api_key = api_key
        self._service_id = service_id
        self._base_url = self.get_base_url()
        self._root_path = root_path
        self._swagger_path = swagger_path
        classes = self.generate_classes()
        for class_name, klass in classes.items():
            setattr(self, class_name, klass(client=self,
                                            return_type=return_type))

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

        with requests.Session() as sess:
            auth_service_session(sess, self)
            response = sess.get(swagger_url)
            response.raise_for_status()
        spec = response.json(object_pairs_hook=OrderedDict)
        return spec

    def generate_classes(self):
        raw_spec = self.get_api_spec()
        spec = JsonRef.replace_refs(raw_spec)
        return self.parse_api_spec(spec)

    def get_base_url(self):
        service = _get_service(self)
        return service['current_url']
