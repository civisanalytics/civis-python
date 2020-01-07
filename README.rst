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

**Deprecation Warning:** Civis will no longer support Python 2.7 or
Python 3.4 as of April 1, 2020. The first Civis API Python Client
release made after that date will remove Python 2 support.


Introduction
------------

.. start-include-marker-introductory-paragraph

The Civis API Python client is a Python package that helps analysts and
developers interact with the Civis Platform. The package includes a set of
tools around common workflows as well as a convenient interface to make
requests directly to the Civis API.

.. end-include-marker-introductory-paragraph

Please see the
`full documentation <https://civis-python.readthedocs.io>`_ for more details.

.. start-include-marker-api-keys-section

API Keys
--------

In order to make requests to the Civis API,
you will need a Civis Platform API key that is unique to you.
Instructions for creating a new key are found
`here <https://civis.zendesk.com/hc/en-us/articles/216341583-Generating-an-API-Key>`_.
API keys have a set expiration date and new keys will need to be created at
least every 30 days. The API client will look for a ``CIVIS_API_KEY``
environmental variable to access your API key, so after creating a new API key,
follow the steps below for your operating system to set up your environment.

Linux / MacOS
~~~~~~~~~~~~~

1. Add the following to ``.bash_profile`` (or ``.bashrc`` for Linux) for bash::

    export CIVIS_API_KEY="alphaNumericApiK3y"

2. Source your ``.bash_profile`` (or restart your terminal).

Windows 10
~~~~~~~~~~

1. Navigate to "Settings" -> type "environment" in search bar ->
   "Edit environment variables for your account". This can also be found
   in "System Properties" -> "Advanced" -> "Environment Variables...".
2. In the user variables section, if ``CIVIS_API_KEY`` already exists in
   the list of environment variables, click on it and press "Edit...".
   Otherwise, click "New..".
3. Enter CIVIS_API_KEY as the "Variable name".
4. Enter your API key as the "Variable value".  Your API key should look
   like a long string of letters and numbers.

.. end-include-marker-api-keys-section

.. start-include-marker-installation-section

Installation
------------

After creating an API key and setting the ``CIVIS_API_KEY`` environmental
variable, install the Python package ``civis`` with the recommended method via ``pip``::

    pip install civis

Alternatively, if you are interested in the latest functionality not yet released through ``pip``,
you may clone the code from GitHub and build from source:

.. code-block:: bash

   git clone https://github.com/civisanalytics/civis-python.git
   cd civis-python
   python setup.py install

You can test your installation by running

.. code-block:: python

    import civis
    client = civis.APIClient()
    print(client.users.list_me()['username'])

If ``civis`` was installed correctly, this will print your Civis
Platform username.

The client has a soft dependency on ``pandas`` to support features such as
data type parsing.  If you are using the ``io`` namespace to read or write
data from Civis, it is highly recommended that you install ``pandas`` and
set ``use_pandas=True`` in functions that accept that parameter.  To install
``pandas``:

.. code-block:: bash

   pip install pandas

Machine learning features in the ``ml`` namespace have a soft dependency on
``scikit-learn`` and ``pandas``. Install ``scikit-learn`` to
export your trained models from the Civis Platform or to
provide your own custom models. Use ``pandas`` to download model predictions
from the Civis Platform. The ``civis.ml`` code
optionally uses the `feather <https://github.com/wesm/feather>`_
format to transfer data from your local computer to Civis
Platform. Install these dependencies with

.. code-block:: bash

   pip install scikit-learn
   pip install pandas
   pip install feather-format


Some CivisML models have open-source dependencies in
addition to ``scikit-learn``, which you may need if you want to
download the model object. These dependencies are
``civisml-extensions``, ``glmnet``, and ``muffnn``. Install these
dependencies with

.. code-block:: bash

   pip install civisml-extensions
   pip install glmnet
   pip install muffnn

.. end-include-marker-installation-section

.. start-include-marker-python-version-support-section

Python version support
----------------------

Python 2.7, 3.4, 3.5, 3.6, and 3.7

.. end-include-marker-python-version-support-section

Usage
-----

``civis`` includes a number of wrappers around the Civis API for
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


.. start-include-marker-retries-section

Retries
-------

The API client will automatically retry for certain API error responses.

If the error is one of [413, 429, 503] and the API client is told how long it needs
to wait before it's safe to retry (this is always the case with 429s, which are
rate limit errors), then the client will wait the specified amount of time
before retrying the request.

If the error is one of [429, 502, 503, 504] and the request is not a ``patch*`` or ``post*``
method, then the API client will retry the request several times, with a delay,
to see if it will succeed.

.. end-include-marker-retires-section


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
