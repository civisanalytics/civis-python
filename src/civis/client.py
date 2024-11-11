from __future__ import annotations

from functools import lru_cache
import logging
import textwrap
import warnings
from typing import TYPE_CHECKING

import civis
from civis.resources import generate_classes_maybe_cached
from civis._utils import get_api_key, DEFAULT_RETRYING_STR
from civis.response import _RETURN_TYPES, find, find_one

if TYPE_CHECKING:
    import collections
    import tenacity


_log = logging.getLogger(__name__)


class APIClient:
    """The Civis API client.

    Parameters
    ----------
    api_key : str, optional
        Your API key obtained from the Civis Platform. If not given, the
        client will use the :envvar:`CIVIS_API_KEY` environment variable.
    return_type : str, optional
        The following types are implemented:

        - ``'raw'`` Returns the raw :class:`requests:requests.Response` object.
        - ``'snake'`` Returns a :class:`civis.Response` object for the
          json-encoded content of a response. This maps the top-level json
          keys to snake_case.
    api_version : string, optional
        The version of endpoints to call. May instantiate multiple client
        objects with different versions. Currently only "1.0" is supported.
    local_api_spec : collections.OrderedDict or string, optional
        The methods on this class are dynamically built from the Civis API
        specification, which can be retrieved from the /endpoints endpoint.
        When local_api_spec is None, the default, this specification is
        downloaded the first time APIClient is instantiated. Alternatively,
        a local cache of the specification may be passed as either an
        OrderedDict or a filename which points to a json file.
    force_refresh_api_spec : bool, optional
        Whether to force re-downloading the API spec,
        even if the cached version for the given API key hasn't expired.
    retries : tenacity.Retrying, optional
        Provide a :class:`tenacity.Retrying` instance for retries.
        If ``None`` or not provided, the following default will be used:

        .. code-block:: python

           {default_retrying}

        If you're providing a :class:`tenacity.Retrying` instance,
        please note that you should leave the ``retry`` attribute unspecified,
        because the conditions under which retries apply are pre-determined
        -- see :ref:`retries` for details.
    """

    def __init__(
        self,
        api_key: str | None = None,
        return_type: str = "snake",
        api_version: str = "1.0",
        local_api_spec: collections.OrderedDict | str | None = None,
        force_refresh_api_spec: bool = False,
        retries: tenacity.Retrying | None = None,
    ):
        if return_type not in _RETURN_TYPES:
            raise ValueError(
                f"Return type must be one of {set(_RETURN_TYPES)}: " f"{return_type}"
            )
        self._feature_flags = ()
        session_auth_key = get_api_key(api_key)
        self._session_kwargs = {"api_key": session_auth_key, "retrying": retries}
        self.last_response = None

        classes = generate_classes_maybe_cached(
            local_api_spec, session_auth_key, api_version, force_refresh_api_spec
        )
        for class_name, cls in classes.items():
            setattr(
                self,
                class_name,
                cls(self._session_kwargs, client=self, return_type=return_type),
            )

        # Don't create the `tenacity.Retrying` instance until we make the first
        # API call with this `APIClient` instance.
        # Once that happens, we keep re-using this `tenacity.Retrying` instance.
        self._retrying = None

    @property
    def feature_flags(self):
        if self._feature_flags:
            return self._feature_flags
        me = self.users.list_me()
        self._feature_flags = tuple(
            flag for flag, value in me["feature_flags"].items() if value
        )
        return self._feature_flags

    def __getstate__(self):
        raise RuntimeError("The APIClient object can't be pickled.")

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
        warnings.warn(
            "The method `get_database_credential_id` is deprecated and will be removed "
            "at civis-python v3.0.0. Its continued usage is strongly discouraged. "
            "Given the way Civis Platform has evolved over the years, "
            "there's currently no reliable way to get a database credential ID "
            "from a username and database name. No replacement for this method is "
            "being planned. If you need to programmatically access a database "
            "credential ID that is or may likely be the default credential, "
            "consider the property `default_database_credential_id`.",
            FutureWarning,
            stacklevel=2,  # Point to the user code that calls this method.
        )
        if isinstance(username, int):
            return username
        else:
            creds = self.credentials.list(type="Database")
            filter_kwargs = {"username": username}
            if isinstance(database_name, int):
                filter_kwargs["remote_host_id"] = database_name
            else:
                filter_kwargs["remote_host_name"] = database_name
            my_creds = find_one(creds, **filter_kwargs)
            if my_creds is None:
                raise ValueError(
                    "Credential ID for {} on {} not "
                    "found.".format(username, database_name)
                )

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
                    _log.warning(
                        "Found %d AWS credentials with name %s and "
                        "owner %s. Returning the first.",
                        len(my_creds),
                        cred_name,
                        owner,
                    )
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
        tables = self.tables.list(database_id=database_id, schema=schema, name=name)
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
        warnings.warn(
            "The property `default_credential` is deprecated and will be removed "
            "at civis-python v3.0.0. "
            "Please use `default_database_credential_id` instead.",
            FutureWarning,
            stacklevel=2,  # Point to the user code that calls this method.
        )
        creds = self.credentials.list(default=True)
        return creds[0]["id"] if len(creds) > 0 else None

    @property
    @lru_cache(maxsize=128)
    def default_database_credential_id(self):
        """The current user's default database credential ID."""
        creds = self.credentials.list(
            default=True, type="Database", remote_host_id=None
        )
        return creds[0]["id"] if len(creds) > 0 else None

    @property
    @lru_cache(maxsize=128)
    def username(self):
        """The current user's username."""
        return self.users.list_me().username


APIClient.__doc__ = APIClient.__doc__.format(
    default_retrying=textwrap.indent(DEFAULT_RETRYING_STR.strip(), " " * 11).strip()
)
