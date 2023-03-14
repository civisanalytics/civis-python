import requests

from civis._utils import camel_to_snake


_RETURN_TYPES = frozenset({'snake', 'raw', 'pandas'})


class CivisClientError(Exception):
    def __init__(self, message, response):
        self.status_code = response.status_code
        self.error_message = message

    def __str__(self):
        return self.error_message


class CivisImmutableResponseError(Exception):
    pass


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


def _raise_response_immutable_error():
    raise CivisImmutableResponseError("Response object is not mutable")


class Response:
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
    """
    def __init__(self, json_data, *, headers=None):
        self.json_data = json_data
        self.headers = headers
        self.calls_remaining = (
            int(x)
            if (x := (headers or {}).get('X-RateLimit-Remaining')) else x
        )
        self.rate_limit = (
            int(x)
            if (x := (headers or {}).get('X-RateLimit-Limit')) else x
        )

        self._data_camel = {}
        self._data_snake = {}

        if json_data is not None:
            for key, v in json_data.items():

                if isinstance(v, dict):
                    val = Response(v)
                elif isinstance(v, list):
                    val = [Response(o) if isinstance(o, dict) else o
                           for o in v]
                else:
                    val = v

                self._data_camel[key] = val
                self._data_snake[camel_to_snake(key)] = val

    def __setattr__(self, key, value):
        if key == "__dict__":
            self.__dict__.update(value)
        elif key in ("json_data", "headers", "calls_remaining", "rate_limit",
                     "_data_camel", "_data_snake"):
            self.__dict__[key] = value
        else:
            _raise_response_immutable_error()

    def __setitem__(self, key, value):
        _raise_response_immutable_error()

    def __getitem__(self, item):
        try:
            return self._data_snake[item]
        except KeyError:
            return self._data_camel[item]

    def __getattr__(self, item):
        try:
            return self.__getitem__(item)
        except KeyError as e:
            raise AttributeError(f"Response object has no attribute {str(e)}")

    def __len__(self):
        return len(self._data_snake)

    def __repr__(self):
        return repr(self._data_snake)

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def items(self):
        return self._data_snake.items()

    def __eq__(self, other):
        if isinstance(other, dict):
            return self._data_snake == other
        elif isinstance(other, Response):
            return self._data_snake == other._data_snake
        else:
            raise TypeError(f"Response and {type(other)} can't be compared")

    def __setstate__(self, state):
        """Set the state when unpickling, to avoid RecursionError."""
        self.__dict__ = state

    def _replace(self, key, value):
        """Only used within this repo; `key` assumed to be in snake_case."""
        self._data_snake[key] = value


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
