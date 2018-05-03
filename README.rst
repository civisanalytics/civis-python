Civis API Python Client
=======================

|Travis| |PyPI| |PyVersions|

.. |Travis| image:: https://img.shields.io/travis/civisanalytics/civis-python/master.svg
   :alt: Build status
   :target: https://travis-ci.org/civisanalytics/civis-python

.. |PyPI| image:: https://img.shields.io/pypi/v/civis.svg
   :target: https://pypi.org/project/civis/
   :alt: Latest version on PyPI

.. |PyVersions| image:: https://img.shields.io/pypi/pyversions/civis.svg
   :target: https://pypi.org/project/civis/
   :alt: Supported python versions for civis-python

Introduction
------------

The Civis API Python client is a Python package that helps analysts and
developers interact with the Civis Platform. The package includes a set of
tools around common workflows as well as a convenient interface to make
requests directly to the Civis API. See the
`full documentation <https://civis-python.readthedocs.io>`_ for more details.


API Keys
--------

Usage of ``civis-python`` requires a valid Civis API key, which can be created
by following the instructions
`here <https://civis.zendesk.com/hc/en-us/articles/216341583-Generating-an-API-Key>`_.
API keys have a set expiration date and new keys will need to be created at
least every 30 days. ``civis-python`` will look for a ``CIVIS_API_KEY``
environmental variable to access your API key, so after creating a new API key,
follow the steps below for your operating system to set up your environment.

Linux / MacOS
~~~~~~~~~~~~~

1. Add the following to ``.bash_profile`` for bash::

    export CIVIS_API_KEY="alphaNumericApiK3y"

2. Source your ``.bash_profile`` (or restart your terminal).

Windows
~~~~~~~

1. Navigate to ``Settings`` -> type "environment" in search bar ->
   ``Edit environment variables for your account``. This can also be found
   in ``System Properties`` -> ``Advanced`` -> ``Environment Variables...``.
2. If ``CIVIS_API_KEY`` already exists in the list of environment variables,
   click on it and press ``Edit...``. Otherwise, click ``New..``.
3. Enter CIVIS_API_KEY as the ``Variable name``.
4. Enter your API key as the ``Variable value``.  Your API key should look
   like a long string of letters and numbers.


Installation
------------

After creating an API key and setting the ``CIVIS_API_KEY`` environmental
variable, install ``civis-python`` with::

    pip install civis

Optionally, install ``pandas``, and ``pubnub`` to enable some functionality in ``civis-python``::

    pip install pandas
    pip install pubnub

Installation of ``pandas`` will allow some functions to return ``DataFrame`` outputs.
Installation of ``pubnub`` will improve performance in all functions which
wait for a Civis Platform job to complete.

You can test your installation by running

.. code-block:: python

    import civis
    client = civis.APIClient()
    print(client.users.list_me()['username'])

If ``civis-python`` was installed correctly, this will print your Civis
username.


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

To install dependencies for building the documentation::

    pip install Sphinx
    pip install sphinx_rtd_theme
    pip install numpydoc

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
