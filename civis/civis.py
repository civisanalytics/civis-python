import functools
import logging
import os

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import Retry

import civis
from civis.resources import generate_classes


log = logging.getLogger(__name__)

RETRY_CODES = [429, 502, 503, 504]


def _get_api_key(api_key):
    """Pass-through if `api_key` is not None otherwise tries the CIVIS_API_KEY
    environmental variable.
    """
    if api_key is not None:  # always prefer user given one
        return api_key
    api_key = os.environ.get("CIVIS_API_KEY", None)
    if api_key is None:
        raise EnvironmentError("No Civis API key found. Please store in "
                               "CIVIS_API_KEY environment variable")
    return api_key


def find(object_list, filter_func=None, **kwargs):
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
    results = find(object_list, filter_func, **kwargs)

    return results[0] if results else None


class MetaMixin():

    @functools.lru_cache()
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

    @functools.lru_cache()
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

    @functools.lru_cache()
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

    @functools.lru_cache()
    def get_table_id(self, table, database):
        """Return the table ID for a given database and table name.

        Parameters
        ----------
        table : str
            The name of the table in format schema.table.
        database : str or int
            The name or ID of the database.

        Returns
        -------
        table_id : int
            The ID of the table. Only returns exact match to specified
            table.

        Raises
        ------
        ValueError
            If an exact table match can't be found.
        """
        database_id = self.get_database_id(database)
        schema, name = table.split('.')
        tables = self.tables.list(database_id=database_id, schema=schema,
                                  name=name)
        if not tables:
            msg = "No tables found for {} in database {}"
            raise ValueError(msg.format(table, database))
        found_table = ".".join((tables[0].schema, tables[0].name))
        if table != found_table:
            msg = "Given table {} is not an exact match for returned table {}."
            raise ValueError(msg.format(table, found_table))

        return tables[0].id

    @property
    @functools.lru_cache()
    def default_credential(self):
        """The current user's default credential."""
        # NOTE: this should be optional to endpoints...so this could go away
        creds = self.credentials.list(default=True)
        return creds[0]['id'] if len(creds) > 0 else None

    @property
    @functools.lru_cache()
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
    retry_total : int, optional
        A number indicating the maximum number of retries for 429, 502, 503, or
        504 errors.
    api_version : string, optional
        The version of endpoints to call. May instantiate multiple client
        objects with different versions. Currently only "1.0" is supported.
    resources : string, optional
        When set to "base", only the default endpoints will be exposed in the
        client object. Set to "all" to include all endpoints available for
        a given user, including those that may be in development and subject
        to breaking changes at a later date.
    """
    def __init__(self, api_key=None, return_type='snake',
                 retry_total=6, api_version="1.0", resources="base"):
        if return_type not in ['snake', 'raw', 'pandas']:
            raise ValueError("Return type must be one of 'snake', 'raw', "
                             "'pandas'")
        self._feature_flags = ()
        session_auth_key = _get_api_key(api_key)
        self._session = session = requests.session()
        session.auth = (session_auth_key, '')

        civis_version = civis.__version__
        session_agent = session.headers.get('User-Agent', '')
        user_agent = "civis-python/{} {}".format(civis_version, session_agent)
        session.headers.update({"User-Agent": user_agent.strip()})

        max_retries = Retry(retry_total, backoff_factor=.75,
                            status_forcelist=RETRY_CODES)
        adapter = HTTPAdapter(max_retries=max_retries)

        session.mount("https://", adapter)

        classes = generate_classes(api_key=session_auth_key,
                                   user_agent=user_agent,
                                   api_version=api_version,
                                   resources=resources)
        for class_name, cls in classes.items():
            setattr(self, class_name, cls(session, return_type))

    @property
    def feature_flags(self):
        if self._feature_flags:
            return self._feature_flags
        me = self.users.list_me()
        self._feature_flags = tuple(flag for flag, value
                                    in me['feature_flags'].items() if value)
        return self._feature_flags
