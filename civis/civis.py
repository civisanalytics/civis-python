from functools import lru_cache
import logging
import warnings

import civis
from civis.resources import generate_classes_maybe_cached
from civis._utils import get_api_key
from civis._deprecation import deprecate_param
from civis.response import _RETURN_TYPES


log = logging.getLogger(__name__)

RETRY_CODES = [429, 502, 503, 504]
RETRY_VERBS = ['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE']
POST_RETRY_CODES = [429, 503]


def find(object_list, filter_func=None, **kwargs):
    """Filter :class:`civis.response.Response` objects.

    Parameters
    ----------
    object_list : iterable
        An iterable of arbitrary objects, particularly those with attributes
        that can be targeted by the filters in `kwargs`. A major use case is
        an iterable of :class:`civis.response.Response` objects.
    filter_func : callable, optional
        A one-argument function. If specified, `kwargs` are ignored.
        An `object` from the input iterable is kept in the returned list
        if and only if ``bool(filter_func(object))`` is ``True``.
    **kwargs
        Key-value pairs for more fine-grained filtering; they cannot be used
        in conjunction with `filter_func`. All keys must be strings.
        For an `object` from the input iterable to be included in the
        returned list, all the `key`s must be attributes of `object`, plus
        any one of the following conditions for a given `key`:

        - `value` is a one-argument function and
          ``bool(value(getattr(object, key)))`` is ``True``
        - `value` is ``True``
        - ``getattr(object, key)`` is equal to ``value``

    Returns
    -------
    list

    Examples
    --------
    >>> import civis
    >>> client = civis.APIClient()
    >>> # creds is a list of civis.response.Response objects
    >>> creds = client.credentials.list()
    >>> # target_creds contains civis.response.Response objects
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
                    if hasattr(o, k) != v:
                        return False
                elif v != getattr(o, k, None):
                    return False
            return True

        _func = default_filter

    return [o for o in object_list if _func(o)]


def find_one(object_list, filter_func=None, **kwargs):
    """Return one satisfying :class:`civis.response.Response` object.

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


class MetaMixin():

    @lru_cache(maxsize=128)
    def get_database_id(self, database):
        """Return the database ID for a given database name.

        Parameters
        ----------
        database : str or int
            If an integer ID is given, passes through. If a str is given
            the database ID corresponding to that database name is returned.

        Returns
        -------
        database_id : int
            The ID of the database.

        Raises
        ------
        ValueError
            If the database can't be found.
        """
        if isinstance(database, int):
            return database
        db = find_one(self.databases.list(), name=database)
        if not db:
            raise ValueError("Database {} not found.".format(database))

        return db["id"]

    @lru_cache(maxsize=128)
    def get_database_credential_id(self, username, database_name):
        """Return the credential ID for a given username in a given database.

        Parameters
        ----------
        username : str or int
            If an integer ID is given, this passes through directly. If a
            str is given, return the ID corresponding to the database
            credential with that username.
        database_name : str or int
            Return the ID of the database credential with username
            `username` for this database name or ID.

        Returns
        -------
        database_credential_id : int
            The ID of the database credentials.

        Raises
        ------
        ValueError
            If the credential can't be found.

        Examples
        --------
        >>> import civis
        >>> client = civis.APIClient()
        >>> client.get_database_credential_id('jsmith', 'redshift-general')
        1234

        >>> client.get_database_credential_id(1111, 'redshift-general')
        1111
        """
        if isinstance(username, int):
            return username
        else:
            creds = self.credentials.list(type="Database")
            filter_kwargs = {'username': username}
            if isinstance(database_name, int):
                filter_kwargs['remote_host_id'] = database_name
            else:
                filter_kwargs['remote_host_name'] = database_name
            my_creds = find_one(creds, **filter_kwargs)
            if my_creds is None:
                raise ValueError("Credential ID for {} on {} not "
                                 "found.".format(username, database_name))

        return my_creds["id"]

    @lru_cache(maxsize=128)
    def get_aws_credential_id(self, cred_name, owner=None):
        """Find an AWS credential ID.

        Parameters
        ----------
        cred_name : str or int
            If an integer ID is given, this passes through directly. If a
            str is given, return the ID corresponding to the AWS credential
            with that name.
        owner : str, optional
            Return the credential with this owner. If not provided, search
            for credentials under your username to disambiguate multiple
            credentials with the same name. Note that this function cannot
            return credentials which are not associated with an owner.

        Returns
        -------
        aws_credential_id : int
            The ID number of the AWS credentials.

        Raises
        ------
        ValueError
            If the AWS credential can't be found.

        Examples
        --------
        >>> import civis
        >>> client = civis.APIClient()
        >>> client.get_aws_credential_id('jsmith')
        1234

        >>> client.get_aws_credential_id(1111)
        1111

        >>> client.get_aws_credential_id('shared-cred',
        ...                              owner='research-group')
        99
        """
        if isinstance(cred_name, int):
            return cred_name
        else:
            creds = self.credentials.list(type="Amazon Web Services S3")
            my_creds = find(creds, name=cred_name)
            if owner is not None:
                my_creds = find(my_creds, owner=owner)

            if not my_creds:
                own_str = "" if owner is None else " owned by {}".format(owner)
                msg = "AWS credential ID for {}{} cannot be found"
                raise ValueError(msg.format(cred_name, own_str))
            elif len(my_creds) > 1:
                if owner is None:
                    # If the user didn't specify an owner, see if we can
                    # narrow down to just credentials owned by this user.
                    owner = self.username
                    my_creds = find(my_creds, owner=owner)
                if len(my_creds) > 1:
                    log.warning("Found %d AWS credentials with name %s and "
                                "owner %s. Returning the first.",
                                len(my_creds), cred_name, owner)
            my_creds = my_creds[0]

        return my_creds["id"]

    @lru_cache(maxsize=128)
    def get_table_id(self, table, database):
        """Return the table ID for a given database and table name.

        Parameters
        ----------
        table : str
            The name of the table in format schema.tablename.
            Either schema or tablename, or both, can be double-quoted to
            correctly parse special characters (such as '.').
        database : str or int
            The name or ID of the database.

        Returns
        -------
        table_id : int
            The ID of the table.

        Raises
        ------
        ValueError
            If a table match can't be found.

        Examples
        --------
        >>> import civis
        >>> client = civis.APIClient()
        >>> client.get_table_id('foo.bar', 'redshift-general')
        123
        >>> client.get_table_id('"schema.has.periods".bar', 'redshift-general')
        456
        """
        database_id = self.get_database_id(database)
        schema, name = civis.io.split_schema_tablename(table)
        tables = self.tables.list(database_id=database_id, schema=schema,
                                  name=name)
        if not tables:
            msg = "No tables found for {} in database {}"
            raise ValueError(msg.format(table, database))

        return tables[0].id

    @lru_cache(maxsize=128)
    def get_storage_host_id(self, storage_host):
        """Return the storage host ID for a given storage host name.

        Parameters
        ----------
        storage_host : str or int
            If an integer ID is given, passes through. If a str is given
            the storage host ID corresponding to that storage host is returned.

        Returns
        -------
        storage_host_id : int
            The ID of the storage host.

        Raises
        ------
        ValueError
            If the storage host can't be found.

        Examples
        --------
        >>> import civis
        >>> client = civis.APIClient()
        >>> client.get_storage_host_id('test host')
        1234

        >>> client.get_storage_host_id(1111)
        1111
        """
        if isinstance(storage_host, int):
            return storage_host
        sh = find_one(self.storage_hosts.list(), name=storage_host)
        if not sh:
            raise ValueError("Storage Host {} not found.".format(storage_host))

        return sh["id"]

    @property
    @lru_cache(maxsize=128)
    def default_credential(self):
        """The current user's default credential."""
        # NOTE: this should be optional to endpoints...so this could go away
        creds = self.credentials.list(default=True)
        return creds[0]['id'] if len(creds) > 0 else None

    @property
    @lru_cache(maxsize=128)
    def username(self):
        """The current user's username."""
        return self.users.list_me().username


class APIClient(MetaMixin):
    """The Civis API client.

    Parameters
    ----------
    api_key : str, optional
        Your API key obtained from the Civis Platform. If not given, the
        client will use the :envvar:`CIVIS_API_KEY` environment variable.
    return_type : str, optional
        The following types are implemented:

        - ``'raw'`` Returns the raw :class:`requests:requests.Response` object.
        - ``'snake'`` Returns a :class:`civis.response.Response` object for the
          json-encoded content of a response. This maps the top-level json
          keys to snake_case.
        - ``'pandas'`` Returns a :class:`pandas:pandas.DataFrame` for
          list-like responses and a :class:`pandas:pandas.Series` for single a
          json response.
    retry_total : DEPRECATED int, optional
        A number indicating the maximum number of retries for 429, 502, 503, or
        504 errors. This parameter no longer has any effect since v1.15.0,
        as retries are automatically handled. This parameter will be removed
        at version 2.0.0.
    api_version : string, optional
        The version of endpoints to call. May instantiate multiple client
        objects with different versions. Currently only "1.0" is supported.
    resources : string, optional
        When set to "base", only the default endpoints will be exposed in the
        client object. Set to "all" to include all endpoints available for
        a given user, including those that may be in development and subject
        to breaking changes at a later date. This will be removed in a future
        version of the API client.
    local_api_spec : collections.OrderedDict or string, optional
        The methods on this class are dynamically built from the Civis API
        specification, which can be retrieved from the /endpoints endpoint.
        When local_api_spec is None, the default, this specification is
        downloaded the first time APIClient is instantiated. Alternatively,
        a local cache of the specification may be passed as either an
        OrderedDict or a filename which points to a json file.
    """
    @deprecate_param('v2.0.0', 'retry_total', 'resources')
    def __init__(self, api_key=None, return_type='snake',
                 retry_total=6, api_version="1.0", resources="all",
                 local_api_spec=None):
        if retry_total != 6:
            warnings.warn(
                "Setting the retry_total parameter no longer has any effect, "
                "as retries are now handled automatically.",
                FutureWarning
            )
        if return_type not in _RETURN_TYPES:
            raise ValueError(
                f"Return type must be one of {set(_RETURN_TYPES)}: "
                f"{return_type}"
            )
        self._feature_flags = ()
        session_auth_key = get_api_key(api_key)
        self._session_kwargs = {'api_key': session_auth_key}
        self.last_response = None

        # Catch deprecation warnings from generate_classes_maybe_cached and
        # the functions it calls until the `resources` argument is removed.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=FutureWarning,
                module='civis')
            classes = generate_classes_maybe_cached(local_api_spec,
                                                    session_auth_key,
                                                    api_version,
                                                    resources)
        for class_name, cls in classes.items():
            setattr(self, class_name, cls(self._session_kwargs, client=self,
                                          return_type=return_type))

    @property
    def feature_flags(self):
        if self._feature_flags:
            return self._feature_flags
        me = self.users.list_me()
        self._feature_flags = tuple(flag for flag, value
                                    in me['feature_flags'].items() if value)
        return self._feature_flags

    def __getstate__(self):
        raise RuntimeError("The APIClient object can't be pickled.")
