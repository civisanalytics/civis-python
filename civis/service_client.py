from collections import OrderedDict
from functools import lru_cache
import json
from jsonref import JsonRef
import re
import requests
import warnings

from civis import APIClient
from civis.base import CivisAPIError, Endpoint, tostr_urljoin
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


def _parse_service_path(path, operations, root_path=None):
    """ Parse an endpoint into a class where each valid http request
    on that endpoint is converted into a convenience function and
    attached to the class as a method.
    """
    if root_path is not None:
        path = path.replace(root_path, '')
    path = path.strip('/')
    modified_base_path = re.sub("-", "_", path.split('/')[0].lower())
    methods = []
    for verb, op in operations.items():
        method = parse_method(verb, op, path)
        if method is None:
            continue
        methods.append(method)
    return modified_base_path, methods


def parse_service_api_spec(api_spec, root_path=None):
    """Dynamically create classes to interface with a Civis Service API.

    Parse an OpenAPI (Swagger) specification into a dictionary of classes
    where each class represents an endpoint resource and contains
    methods to make http requests on that resource.

    Parameters
    ----------
    api_spec : OrderedDict
        The Civis Service API specification to parse.  References should be
        resolved before passing, typically using jsonref.JsonRef().
    root_path : str, optional
        An additional path for APIs that are not hosted on the service's
        root level. An example root_path would be '/api' for an app with
        resource endpoints that all begin with '/api'.
    """
    paths = api_spec['paths']
    classes = {}
    for path, ops in paths.items():
        base_path, methods = _parse_service_path(
            path, ops, root_path=root_path)
        class_name = to_camelcase(base_path)
        if methods and classes.get(base_path) is None:
            classes[base_path] = type(str(class_name),
                                      (ServiceEndpoint,),
                                      {})
        for method_name, method in methods:
            setattr(classes[base_path], method_name, method)
    return classes


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
            raise CivisAPIError(response)

        return response


class ServiceClient():

    def __init__(self, service_id, root_path=None,
                 swagger_path="/endpoints", api_key=None,
                 return_type='snake', local_api_spec=None):
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
        local_api_spec : collections.OrderedDict or string, optional
            The methods on this class are dynamically built from the Service
            API specification, which can be retrieved from the /endpoints
            endpoint. When local_api_spec is None, the default, this
            specification is downloaded the first time APIClient is
            instantiated. Alternatively, a local cache of the specification
            may be passed as either an OrderedDict or a filename which
            points to a json file.
        """
        if return_type not in ['snake', 'raw', 'pandas']:
            raise ValueError("Return type must be one of 'snake', 'raw', "
                             "'pandas'")
        self._api_key = api_key
        self._service_id = service_id
        self._base_url = self.get_base_url()
        self._root_path = root_path
        self._swagger_path = swagger_path
        classes = self.generate_classes_maybe_cached(local_api_spec)
        for class_name, klass in classes.items():
            setattr(self, class_name, klass(client=self,
                                            return_type=return_type))

    def parse_path(self, path, operations):
        """ Parse an endpoint into a class where each valid http request
        on that endpoint is converted into a convenience function and
        attached to the class as a method.
        """
        warnings.warn("This method is deprecated and will be removed in "
                      "v2.0.0. Use the `_parse_service_path` function "
                      "instead.")
        return _parse_service_path(path, operations, root_path=self._root_path)

    def parse_api_spec(self, api_spec):
        warnings.warn("This method is deprecated and will be removed in "
                      "v2.0.0. Use the `parse_service_api_spec` function "
                      "instead.")
        return parse_service_api_spec(api_spec, root_path=self._root_path)

    @lru_cache(maxsize=4)
    def get_api_spec(self):
        swagger_url = self._base_url + self._swagger_path

        with requests.Session() as sess:
            auth_service_session(sess, self)
            response = sess.get(swagger_url)
            response.raise_for_status()
        spec = response.json(object_pairs_hook=OrderedDict)
        return spec

    @lru_cache(maxsize=4)
    def generate_classes(self):
        raw_spec = self.get_api_spec()
        spec = JsonRef.replace_refs(raw_spec)
        return parse_service_api_spec(spec, root_path=self._root_path)

    def get_base_url(self):
        service = _get_service(self)
        return service['current_url']

    def generate_classes_maybe_cached(self, cache):
        """Generate class objects either from /endpoints or a local cache."""
        if cache is None:
            classes = self.generate_classes()
        else:
            if isinstance(cache, OrderedDict):
                raw_spec = cache
            elif isinstance(cache, str):
                with open(cache, "r") as f:
                    raw_spec = json.load(f, object_pairs_hook=OrderedDict)
            else:
                msg = "cache must be an OrderedDict or str, given {}"
                raise ValueError(msg.format(type(cache)))
            spec = JsonRef.replace_refs(raw_spec)
            classes = parse_service_api_spec(spec, root_path=self._root_path)
        return classes
