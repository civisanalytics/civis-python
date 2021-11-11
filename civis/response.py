import requests

from civis._utils import camel_to_snake


_RETURN_TYPES = frozenset({'snake', 'raw', 'pandas'})


class CivisClientError(Exception):
    def __init__(self, message, response):
        self.status_code = response.status_code
        self.error_message = message

    def __str__(self):
        return self.error_message


def _response_to_json(response):
    """Parse a raw response to a dict.

    Parameters
    ----------
    response: requests.Response
        A raw response returned by an API call.

    Returns
    -------
    dict | None
        The data in the response body or None if the response has no
        content.

    Raises
    ------
    CivisClientError
        If the data in the raw response cannot be parsed.
    """
    if response.content == b'':
        return None
    else:
        try:
            return response.json()
        except ValueError:
            raise CivisClientError("Unable to parse JSON from response",
                                   response)


def convert_response_data_type(response, headers=None, return_type='snake'):
    """Convert a raw response into a given type.

    Parameters
    ----------
    response : list, dict, or `requests.Response`
        Convert this object into a different response object.
    headers : dict, optional
        If given and the return type supports it, attach these headers to the
        converted response. If `response` is a `requests.Response`, the headers
        will be inferred from it.
    return_type : string, {'snake', 'raw', 'pandas'}
        Convert the response to this type. See documentation on
        `civis.APIClient` for details of the return types.

    Returns
    -------
    list, dict, `civis.response.Response`, `requests.Response`,
    `pandas.DataFrame`, or `pandas.Series`
        Depending on the value of `return_type`.
    """
    if return_type not in _RETURN_TYPES:
        raise ValueError(
            f"Return type not one of {set(_RETURN_TYPES)}: {return_type}"
        )

    if return_type == 'raw':
        return response

    if isinstance(response, requests.Response):
        headers = response.headers
        data = _response_to_json(response)
    else:
        data = response

    if return_type == 'pandas':
        import pandas as pd
        if isinstance(data, list):
            return pd.DataFrame.from_records(data)

        # there may be nested objects or arrays in this series
        return pd.Series(data)

    elif return_type == 'snake':
        if isinstance(data, list):
            return [Response(d, headers=headers) for d in data]

        return Response(data, headers=headers)


class Response(dict):
    """Custom Civis response object.

    Attributes
    ----------
    json_data : dict | None
        This is `json_data` as it is originally returned to the user without
        the key names being changed. See Notes. None is used if the original
        response returned a 204 No Content response.
    headers : dict
        This is the header for the API call without changing the key names.
    calls_remaining : int
        Number of API calls remaining before rate limit is reached.
    rate_limit : int
        Total number of calls per API rate limit period.

    Notes
    -----
    The main features of this class are that it maps camelCase to snake_case
    at the top level of the json object and attaches keys as attributes.
    Nested object keys are not changed.
    """
    def __init__(self, json_data, snake_case=True, headers=None):
        self.json_data = json_data
        if headers is not None:
            # this circumvents recursive calls
            self.headers = headers
            self.calls_remaining = headers.get('X-RateLimit-Remaining')
            self.rate_limit = headers.get('X-RateLimit-Limit')

        # Keys to update for this response object.
        self_updates = {}

        if json_data is not None:
            for key, v in json_data.items():
                if snake_case:
                    key = camel_to_snake(key)

                if isinstance(v, dict):
                    val = Response(v, False)
                elif isinstance(v, list):
                    val = [Response(o) if isinstance(o, dict) else o
                           for o in v]
                else:
                    val = v

                self_updates[key] = val

        self.update(self_updates)
        # Update self.__dict__ at the end to avoid replacing the update method.
        self.__dict__.update(self_updates)


class PaginatedResponse:
    """A response object which is an iterator

    Parameters
    ----------
    path : str
        Make GET requests to this path.
    initial_params : dict
        Query params that should be passed along with each request. Note that
        if `initial_params` contains the keys `page_num` or `limit`, they will
        be ignored. The given dict is not modified.
    endpoint : `civis.base.Endpoint`
        An endpoint used to make API requests.

    Notes
    -----
    This response is returned automatically by endpoints which support
    pagination when the `iterator` kwarg is specified.

    Examples
    --------
    >>> client = civis.APIClient()
    >>> queries = client.queries.list(iterator=True)
    >>> for query in queries:
    ...    print(query['id'])
    """
    def __init__(self, path, initial_params, endpoint):
        self._path = path
        self._params = initial_params.copy()
        self._endpoint = endpoint

        # We are paginating through all items, so start at the beginning and
        # let the API determine the limit.
        self._params['page_num'] = 1
        self._params.pop('limit', None)

        self._iter = None

    def __iter__(self):
        return self

    def _get_iter(self):
        while True:
            response = self._endpoint._make_request('GET',
                                                    self._path,
                                                    self._params)
            page_data = _response_to_json(response)
            if len(page_data) == 0:
                return

            for data in page_data:
                converted_data = convert_response_data_type(
                    data,
                    headers=response.headers,
                    return_type=self._endpoint._return_type
                )
                yield converted_data

            self._params['page_num'] += 1

    def __next__(self):
        if self._iter is None:
            self._iter = self._get_iter()
        return next(self._iter)
