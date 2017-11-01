Civis API Python Client
=======================

Introduction
------------

The Civis API Python client is a Python package that helps analysts and
developers interact with the Civis Platform. The package includes a set of
tools around common workflows as well as a convenient interface to make
requests directly to the Civis API. See the
`full documentation <https://civis-python.readthedocs.io>`_ for more details.


Installation
------------

1. Get a Civis API key `(instructions) <https://civis.zendesk.com/hc/en-us/articles/216341583-Generating-an-API-Key>`_.
2. Add a ``CIVIS_API_KEY`` environment variable.
3. You can add the following to ``.bash_profile`` for bash::

    export CIVIS_API_KEY="alphaNumericApiK3y"

4. Source your ``.bash_profile``
5. Install the package::

    pip install civis

6. Optionally, install ``pandas``, and ``pubnub`` to enable some functionality in ``civis-python``::

    pip install pandas
    pip install pubnub

   Installation of ``pandas`` will allow some functions to return ``DataFrame`` outputs.
   Installation of ``pubnub`` will improve performance in all functions which
   wait for a Civis Platform job to complete.

Usage
-----

``civis-python`` includes a number of wrappers around the Civis API for
common workflows.

.. code-block:: python

    import civis
    df = civis.io.read_civis(table="my_schema.my_table",
                             database="database",
                             use_pandas=True)

The Civis API may also be directly accessed via the ``APIClient`` class.

.. code-block:: python

    import civis
    client = civis.APIClient()
    database = client.databases.list()

See the `full documentation <https://civis-python.readthedocs.io>`_ for a more
complete user guide.

Retries
~~~~~~~

The API client will automatically retry for certain API error responses.

If the error is one of [413, 429, 503] and the API client is told how long it needs
to wait before it's safe to retry (this is always the case with 429s, which are
rate limit errors), then the client will wait the specified amount of time
before retrying the request.

If the error is one of [429, 502, 503, 504] and the request is not a ``patch*`` or ``post*``
method, then the API client will retry the request several times, with a delay,
to see if it will succeed.

Build Documentation Locally
---------------------------

To build the API documentation locally::

    cd docs
    make html

Then open ``docs/build/html/index.html``.

Note that this will use your API key in the ``CIVIS_API_KEY`` environment
variable so it will generate documentation for all the endpoints that you have access to.

Contributing
------------

See ``CONTRIBUTING.md`` for information about contributing to this project.


License
-------

BSD-3

See ``LICENSE.md`` for details.
