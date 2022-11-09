:orphan:

.. _user_guide:

User Guide
##########

Getting Started
===============

After installing the Civis API Python client and setting up your API key, you
can now import the package ``civis``:

.. code-block:: python

   >>> import civis

There are two entrypoints for working with the Civis API. The first is
the ``civis`` namespace, which contains tools for typical workflows in a user
friendly manner. For example, you may want to perform some transformation on
your data in Python that might be tricky to code in SQL. This code downloads
data from Civis, calculates the correlation between all the columns and then
uploads the data back into Civis:

.. code-block:: python

   >>> df = civis.io.read_civis(table="my_schema.my_table",
   ...                          database="database",
   ...                          use_pandas=True)
   >>> correlation_matrix = df.corr()
   >>> correlation_matrix["corr_var"] = correlation_matrix.index
   >>> fut = civis.io.dataframe_to_civis(df=correlation_matrix,
   ...                                   database="database",
   ...                                   table="my_schema.my_correlations")
   >>> fut.result()


Civis Futures
=============

In the code above, :func:`~civis.io.dataframe_to_civis` returns a special
:class:`~civis.futures.CivisFuture` object. Making a request to the Civis
API usually results in a long running job. To account for this, various
functions in the ``civis`` namespace return a
:class:`CivisFuture <civis.futures.CivisFuture>` to allow you to
process multiple long running jobs simultaneously. For instance, you may
want to start many jobs in parallel and wait for them all to finish rather
than wait for each job to finish before starting the next one.

The :class:`CivisFuture <civis.futures.CivisFuture>` follows the
:class:`python:concurrent.futures.Future` API fairly closely. For example,
calling ``result()`` on ``fut`` above forces the program to wait for the
job started with :func:`~civis.io.dataframe_to_civis` to finish and
returns the result or raises an exception.

You can create :class:`CivisFuture <civis.futures.CivisFuture>` objects for
many tasks (e.g., scripts, imports).  Here, we will create a container
script that does the simple task of printing the text "HELLO WORLD", execute
it, and then wait for it to finish.

.. code-block:: python

   >>> import civis
   >>> import concurrent.futures
   >>>
   >>> client = civis.APIClient()
   >>>
   >>> # Create a container script. This is just a simple example. Futures can
   >>> # also be used with SQL queries, imports, etc.
   >>> response_script = client.scripts.post_containers(
   ...     required_resources={'cpu': 512, 'memory': 1024},
   ...     docker_command="echo 'HELLO WORLD'",
   ...     docker_image_name='civisanalytics/datascience-python')
   >>> script_id = response_script.id
   >>>
   >>> # Create a run in order to execute the script.
   >>> response_run = client.scripts.post_containers_runs(script_id)
   >>> run_id = response_run.id
   >>>
   >>> # Create a future to represent the result of the run.
   >>> future = civis.futures.CivisFuture(
   ...     client.scripts.get_containers_runs, (script_id, run_id))
   >>>
   >>> # You can then have your code block and wait for the future to be done as
   >>> # follows. Note that this does not raise an exception on error like
   >>> # `future.result()`.
   >>> concurrent.futures.wait([future])
   >>>
   >>> # Alternatively, you can call `future.result()` to block and get the
   >>> # status of the run once it finishes. If the run is already completed, the
   >>> # result will be returned immediately.
   >>> result = future.result()
   >>>
   >>> # Alternatively, one can start a run and get a future for it with the helper
   >>> # function `civis.utils.run_job`:
   >>> future2 = civis.utils.run_job(script_id)
   >>> future2.result()

Working Directly with the Client
================================

Although many common workflows are included in the Civis API Python client,
projects often require direct calls to the Civis API. For convenience,
the Civis API Python client implements an :class:`~civis.APIClient` object
to make these API calls with Python syntax rather than a manually crafted HTTP
request. To make a call, first instantiate an :class:`~civis.APIClient` object:

.. code-block:: python

   >>> client = civis.APIClient()

.. note::

   Creating an instance of :class:`~civis.APIClient` makes an HTTP request to
   determine the functions to attach to the object.  You must have an
   API key and internet connection to create an :class:`~civis.APIClient`
   object.

With the client object instantiated, you can now make API requests like listing
your user information:

.. code:: python

   >>> client.users.list_me()
   {'email': 'user@email.com',
    'feature_flags': {'left_nav_basic': True,
                      'results': True,
                      'scripts_notify': True,
                      'table_person_matching': True},
    'id': 1,
    'initials': 'UN',
    'name': 'User Name',
    'username': 'uname'}

For a complete list of the API endpoints and their methods,
check out :ref:`api_resources`.

Suppose we did not have the ``civis.io`` namespace. This is how we might export
a CSV file from Civis. As you will see, this can be quite involved and the
``civis`` namespace entrypoint should be preferred whenever possible.

First, we get the ID for our database then we get the default credential for
the current user.

.. code:: python

   >>> db_id = client.get_database_id('cluster-name')
   >>> cred_id = client.default_credential

In order to export a table, we need to write some SQL that will generate the
data to export. Then we create the export job and run it.

.. code:: python

   >>> generate_table = "select * from schema.tablename"
   >>> export_job = client.scripts.post_sql(name="our export job",
                                            remote_host_id=db_id,
                                            credential_id=cred_id,
                                            sql=generate_table)
   >>> export_run = client.scripts.post_sql_runs(export_job.id)

We can then poll and wait for the export to be completed.

.. code:: python

   >>> import time
   >>> export_state = client.scripts.get_sql_runs(export_job.id,
   ...                                            export_run.id)
   >>> while export_state.state in ['queued', 'running']:
   ...    time.sleep(60)
   ...    export_state = client.scripts.get_sql_runs(export_job.id,
   ...                                               export_run.id)

Now, we can get the URL of the exported csv. First, we grab the result of our
export job.

.. code:: python

   >>> export_result = client.scripts.get_sql_runs(export_job.id,
   ...                                             export_run.id)

In the future, a script may export multiple jobs, so the output of this is a
list.

The path returned will have a gzipped csv file, which we could load, for
example, with pandas.

.. code:: python

   >>> url = export_result.output[0].path


API Response Types and Functions
================================

Many API requests via an :class:`~civis.APIClient` instance return an iterable
of :class:`civis.response.Response` objects.
For endpoints that support pagination when the `iterator` kwarg is specified,
a :class:`civis.response.PaginatedResponse` object is returned.
To facilitate working with :class:`civis.response.Response` objects,
the helper functions :func:`civis.find` and :func:`civis.find_one` are defined.


Testing Your Code
================================

Once you've written code that uses :class:`~civis.APIClient`,
you've got to test it. Because you want a testing environment
not dependent upon an API key or an internet connection, you will
employ the mocking technique.

To this end, :func:`civis.tests.create_client_mock` will
create a mock object that looks like an API client object.
This mock object is configured to error if any method calls
have non-existent /
misspelled parameters.

Suppose this function is in your code:

.. code:: python

    def get_timestamps_from_table(..., client=None, ...):
        ...
        client = client if client else civis.APIClient()
        ...
        df = civis.io.read_civis_sql(
            ...,
            client=client,
            ...,
        )
        ...
        return ...

Whatever function you define, it needs to have a ``client`` argument.
If it's not provided, an actual API client object will be created.
Throughout this function, the ``client`` object
has to be used to interact with the Civis API.
It is through this argument that you as a developer can pass in
a custom API client object.


When you're testing your functions in your test suite,
you might have code like this:

.. code:: python

    from civis.tests import create_client_mock

    from <your-package> import get_timestamps_from_table

    def test_get_timestamps_from_table():
        mock_client = create_client_mock()

        mock_client.scripts.get_sql_runs.return_value = {
            "output": [
                {
                    "path": ...
                    "file_id": ...
                }
            ]
        }
        actual_timestamps = get_timestamps_from_table(
            ...
            client=mock_client,
            ...
        )

        expected_timestamps = ...

        # Run assertion tests as necessary
        assert actual_timestamps == expected_timestamps

Once you've created a mock client object, you have to define
its behavior based on expected API calls from the function you've defined.
Also, be sure to use ``mock_client`` so you don't actually have to process an actual API call in your test.
