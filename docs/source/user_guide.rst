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
   >>> poller = civis.io.dataframe_to_civis(df=df,
   ...                                      database="database")
   ...                                      table="my_schema.my_correlations")
   >>> poller.result()


Pollable Results
================

In the code above, :func:`~civis.io.dataframe_to_civis` returns a special
:class:`~civis.polling.PollableResult` object. Making a request to the Civis
API usually results in a long running job. To account for this, various
functions in the ``civis`` namespace return a
:class:`PollableResult <civis.polling.PollableResult>` to allow you to
process multiple long running jobs simultaneously. For instance, you may
want to start many jobs in parallel and wait for them all to finish rather
than wait for each job to finish before starting the next one.

The :class:`PollableResult <civis.polling.PollableResult>` follows the
:class:`python:concurrent.futures.Future` API fairly closely. For example,
calling ``result()`` on ``poller`` above forces the program to wait for the
job started with :func:`~civis.io.dataframe_to_civis` to finish and
returns the result.


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
   object. By default, the functions attached to the object come from a base
   set of Civis API endpoints. Based on your user profile, you may have access
   to a set of developmental endpoints.  To access these, instantiate the
   client with ``client = civis.APIClient(resources='all')``.

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
