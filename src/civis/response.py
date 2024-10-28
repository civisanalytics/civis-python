import json
import pprint

import requests

from civis._camel_to_snake import camel_to_snake


_RETURN_TYPES = frozenset({"snake", "raw"})

# "arguments": Script arguments are often environment variables in
# ALL_CAPS that we don't want to convert to snake_case.
# "environmentVariables": from objects under the `Services` endpoint
_RESPONSE_KEYS_PRESERVE_CASE = frozenset({"arguments", "environmentVariables"})


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
    if response.content == b"":
        return None
    else:
        try:
            return response.json()
        except ValueError:
            raise CivisClientError("Unable to parse JSON from response", response)


def convert_response_data_type(
    response, headers=None, return_type="snake", from_json_values=False
):
    """Convert a raw response into a given type.

    Parameters
    ----------
    response : list, dict, or `requests.Response`
        Convert this object into a different response object.
    headers : dict, optional
        If given and the return type supports it, attach these headers to the
        converted response. If `response` is a `requests.Response`, the headers
        will be inferred from it.
    return_type : string, {'snake', 'raw'}
        Convert the response to this type. See documentation on
        `civis.APIClient` for details of the return types.
    from_json_values : bool, optional
        If True, the `response` comes from the `json_values` endpoint.

    Returns
    -------
    list, dict, `civis.Response`, or `requests.Response`
        Depending on the value of `return_type`.
    """
    if return_type == "raw":
        return response

    elif return_type == "snake":
        if isinstance(response, requests.Response):
            headers = response.headers
            data = _response_to_json(response)
        else:
            data = response

        if isinstance(data, list):
            return [
                Response(d, headers=headers, from_json_values=from_json_values)
                for d in data
            ]
        else:
            return Response(data, headers=headers, from_json_values=from_json_values)

    else:
        raise ValueError(f"Return type not one of {set(_RETURN_TYPES)}: {return_type}")


def _raise_response_immutable_error():
    raise CivisImmutableResponseError(
        "Response object is read-only. "
        "Did you want to call .json() for a dictionary that you can modify?"
    )


class Response:
    """Custom Civis response object.

    Attributes
    ----------
    json_data : dict | None
        This is `json_data` as it is originally returned to the user without
        the key names being changed. None is used if the original
        response returned a 204 No Content response.
    headers : dict
        This is the header for the API call without changing the key names.
    calls_remaining : int
        Number of API calls remaining before rate limit is reached.
    rate_limit : int
        Total number of calls per API rate limit period.
    """

    def __init__(
        self, json_data, *, headers=None, snake_case=True, from_json_values=False
    ):
        self.json_data = json_data
        self.headers = headers
        self.calls_remaining = (
            int(x) if (x := (headers or {}).get("X-RateLimit-Remaining")) else x
        )
        self.rate_limit = (
            int(x) if (x := (headers or {}).get("X-RateLimit-Limit")) else x
        )

        # Note that these two dicts can have Response objects as values.
        self._data_camel = {}
        self._data_snake = {}

        if json_data is not None:
            for key, v in json_data.items():

                if key == "value" and (
                    from_json_values or json_data.get("objectType") == "JSONValue"
                ):
                    # When json_data represents a JSONValue (either from one of the
                    # methods under the `json_values` endpoint or from a script's run
                    # output), `v` is the deserialized JSON.
                    val = v
                elif isinstance(v, dict):
                    if key in _RESPONSE_KEYS_PRESERVE_CASE:
                        val = Response(v, snake_case=False)
                    else:
                        val = Response(v)
                elif isinstance(v, list):
                    val = [Response(o) if isinstance(o, dict) else o for o in v]
                else:
                    val = v

                key_snake = camel_to_snake(key) if snake_case else key

                self._data_camel[key] = val
                self._data_snake[key_snake] = val

    def json(self, snake_case=True):
        """Return the JSON data.

        Parameters
        ----------
        snake_case : bool, optional
            If True (the default), return the keys in snake case.
            If False, return the keys in camel case.

        Returns
        -------
        dict
        """
        if self.json_data is None:
            return {}
        elif snake_case:
            return self._to_dict_with_snake_case_keys()
        else:
            return self.json_data.copy()

    def _to_dict_with_snake_case_keys(self):
        result = {}
        for k, v in self._data_snake.items():
            if isinstance(v, list):
                result[k] = [
                    o._to_dict_with_snake_case_keys() if isinstance(o, Response) else o
                    for o in v
                ]
            elif isinstance(v, Response):
                result[k] = v._to_dict_with_snake_case_keys()
            else:
                result[k] = v
        return result

    def __setattr__(self, key, value):
        if key == "__dict__":
            self.__dict__.update(value)
        elif key in (
            "json_data",
            "headers",
            "calls_remaining",
            "rate_limit",
            "_data_camel",
            "_data_snake",
        ):
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
        except KeyError:
            raise AttributeError(f"Response object has no attribute {item!r}")

    def __len__(self):
        return len(self._data_snake)

    def __repr__(self):
        return f"Response({repr(self._data_snake)})"

    def __hash__(self):
        return hash(json.dumps(self.json_data))

    def _repr_pretty_(self, p, cycle):
        """Pretty-print the response object in IPython and Jupyter.

        https://ipython.readthedocs.io/en/stable/api/generated/IPython.lib.pretty.html#extending
        """
        if cycle:
            p.text("Response(...)")
        else:
            p.text(pprint.pformat(self))

    def get(self, key, default=None):
        """Get the value for the given key."""
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def items(self):
        """Return an iterator of the key-value pairs in the response."""
        return self._data_snake.items()

    def __eq__(self, other):
        if isinstance(other, dict):
            return self._data_snake == other
        elif isinstance(other, Response):
            return self._data_snake == other._data_snake
        else:
            return False

    def __setstate__(self, state):
        """Set the state when unpickling, to avoid RecursionError."""
        self.__dict__ = state


class _safe_key:
    """Helper function for key functions when sorting unorderable objects.

    The wrapped-object will fallback to a Py2.x style comparison for
    unorderable types (sorting first comparing the type name and then by
    the obj ids).  Does not work recursively, so dict.items() must have
    _safe_key applied to both the key and the value.

    Source: https://github.com/python/cpython/blob/3.13/Lib/pprint.py#L80-L100
    """

    __slots__ = ["obj"]

    def __init__(self, obj):
        self.obj = obj

    def __lt__(self, other):
        try:
            return self.obj < other.obj
        except TypeError:
            return (str(type(self.obj)), id(self.obj)) < (
                str(type(other.obj)),
                id(other.obj),
            )


def _safe_tuple(t):
    """Helper function for comparing 2-tuples

    Source: https://github.com/python/cpython/blob/3.13/Lib/pprint.py#L102-L104
    """
    return _safe_key(t[0]), _safe_key(t[1])


def _pprint_response(self, object, stream, indent, allowance, context, level):
    """Pretty-print a Response object.

    Inspired by https://stackoverflow.com/a/52521743
    Based on python's dict pprint:
    https://github.com/python/cpython/blob/3.7/Lib/pprint.py#L180-L192
    """
    write = stream.write
    object = object._data_snake
    write("Response({")
    if self._indent_per_level > 1:
        write((self._indent_per_level - 1) * " ")
    length = len(object)
    if length:
        if self._sort_dicts:
            items = sorted(object.items(), key=_safe_tuple)
        else:
            items = object.items()
        # The 9 in `indent + 9` is the length of "Response(".
        self._format_dict_items(
            items, stream, indent + 9, allowance + 1, context, level
        )
    write("})")


pprint.PrettyPrinter._dispatch[Response.__repr__] = _pprint_response


class PaginatedResponse:
    """A response object which is an iterator

    Parameters
    ----------
    path : str
        Make GET requests to this path.
    initial_params : dict
        Query params that should be passed along with each request. Note that
        if `initial_params` contains the key `page_num`, it will
        be ignored. The given dict is not modified.
    endpoint : `civis.base.Endpoint`
        An endpoint used to make API requests.

    Notes
    -----
    This response is returned automatically by endpoints which support
    pagination when the `iterator` kwarg is specified.

    Examples
    --------
    >>> import civis
    >>> client = civis.APIClient()
    >>> queries = client.queries.list(iterator=True)
    >>> for query in queries:
    ...    print(query['id'])
    """

    def __init__(self, path, initial_params, endpoint):
        self._path = path
        self._params = initial_params.copy()
        self._endpoint = endpoint

        # We are paginating through all items, so start at the beginning.
        self._params["page_num"] = 1

        self._iter = None

    def __iter__(self):
        return self

    def _get_iter(self):
        while True:
            response = self._endpoint._make_request("GET", self._path, self._params)
            page_data = _response_to_json(response)
            if len(page_data) == 0:
                return

            for data in page_data:
                converted_data = convert_response_data_type(
                    data,
                    headers=response.headers,
                    return_type=self._endpoint._return_type,
                    from_json_values=(self._path or "").startswith("json_values"),
                )
                yield converted_data

            self._params["page_num"] += 1

    def __next__(self):
        if self._iter is None:
            self._iter = self._get_iter()
        return next(self._iter)


def find(object_list, filter_func=None, **kwargs):
    """Filter :class:`civis.Response` objects.

    Parameters
    ----------
    object_list : iterable
        An iterable of arbitrary objects, particularly those with attributes
        that can be targeted by the filters in `kwargs`. A major use case is
        an iterable of :class:`civis.Response` objects.
    filter_func : callable, optional
        A one-argument function. If specified, `kwargs` are ignored.
        An `object` from the input iterable is kept in the returned list
        if and only if ``bool(filter_func(object))`` is ``True``.
    **kwargs
        Key-value pairs for more fine-grained filtering; they cannot be used
        in conjunction with ``filter_func``. All keys must be strings.
        For an object ``obj`` from the input iterable to be included in the
        returned list, all the keys must be attributes of ``obj``, plus
        any one of the following conditions for a given key:

        - ``value`` is a one-argument function and
          ``bool(value(getattr(obj, key)))`` is equal to ``True``
        - ``value`` is either ``True`` or ``False``, and
          ``getattr(obj, key) is value`` is ``True``
        - ``getattr(obj, key) == value`` is ``True``

    Returns
    -------
    list

    Examples
    --------
    >>> import civis
    >>> client = civis.APIClient()
    >>> # creds is a list of civis.Response objects
    >>> creds = client.credentials.list()
    >>> # target_creds contains civis.Response objects
    >>> # with the attribute 'name' == 'username'
    >>> target_creds = find(creds, name='username')

    See Also
    --------
    civis.find_one
    """
    _func = filter_func
    if not filter_func:

        def default_filter(o):
            for k, v in kwargs.items():
                if not hasattr(o, k):
                    return False
                elif callable(v):
                    if not v(getattr(o, k, None)):
                        return False
                elif isinstance(v, bool):
                    if getattr(o, k) is not v:
                        return False
                elif v != getattr(o, k, None):
                    return False
            return True

        _func = default_filter

    return [o for o in object_list if _func(o)]


def find_one(object_list, filter_func=None, **kwargs):
    """Return one satisfying :class:`civis.Response` object.

    The arguments are the same as those for :func:`civis.find`.
    If more than one object satisfies the filtering criteria,
    the first one is returned.
    If no satisfying objects are found, ``None`` is returned.

    Returns
    -------
    object or None

    See Also
    --------
    civis.find
    """
    results = find(object_list, filter_func, **kwargs)

    return results[0] if results else None
