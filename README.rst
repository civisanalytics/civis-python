Civis API Python Client
=======================

.. start-include-marker-introductory-paragraph

|PyPI| |PyVersions| |CircleCI| |Documentation|

.. |CircleCI| image:: https://circleci.com/gh/civisanalytics/civis-python.svg?style=shield
   :target: https://circleci.com/gh/civisanalytics/civis-python
   :alt: CircleCI build status

.. |PyPI| image:: https://img.shields.io/pypi/v/civis.svg
   :target: https://pypi.org/project/civis/
   :alt: Latest version on PyPI

.. |PyVersions| image:: https://img.shields.io/pypi/pyversions/civis.svg
   :target: https://pypi.org/project/civis/
   :alt: Supported python versions for civis-python

.. |Documentation| image:: https://readthedocs.org/projects/civis-python/badge/?version=latest
    :target: https://civis-python.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

The Civis API Python client is a Python package that helps analysts
and developers interact with Civis Platform programmatically. The package includes a set of
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
environment variable to access your API key, so after creating a new API key,
follow the steps below for your operating system to set up your environment.

Linux / MacOS
~~~~~~~~~~~~~

1. Add the following to your shell configuration file (``~/.zshrc`` for MacOS or ``~/.bashrc`` for Linux, by default)::

    export CIVIS_API_KEY="alphaNumericApiK3y"

2. Source your shell configuration file (or restart your terminal).

Windows
~~~~~~~

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

After creating an API key and setting the ``CIVIS_API_KEY`` environment
variable, install the Python package ``civis`` with the recommended method via ``pip``::

    pip install civis

Alternatively, if you are interested in the latest functionality not yet released through ``pip``,
you may clone the code from GitHub and build from source (``git`` assumed to be available):

.. code-block:: bash

   pip install git+https://github.com/civisanalytics/civis-python.git

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

See the `documentation <https://civis-python.readthedocs.io>`_ for a more
complete user guide.


Building Documentation
----------------------

Background:

* We use the Sphinx framework. The documentation source files are in ``docs/``.
* All auto-generated files, including the HTML pages, are explicitly not versioned
  (see ``.gitignore``).

For the public documentation at https://civis-python.readthedocs.io:

* The doc build is configured by ``.readthedocs.yaml``.
  Normally, even when we need to update the documentation or make a new release of civis-python,
  neither this configuration YAML file nor Civis's account on the Read the Docs site need
  any updates.
* To update the documentation, the files under ``docs/`` can be updated as needed.
  If the "API Resources" pages need to be updated because the upstream Civis API has been updated,
  then the following need to happen:
  (i) the new Civis API updates must be accessible by a "standard" Civis Platform user,
  i.e., not behind a feature flag, and
  (ii) you'll need to locally run ``python tools/update_civis_api_spec.py`` to update
  ``civis_api_spec.json`` inside the ``civis`` Python package codebase.
  It is this JSON file that's the basis for the Civis API information on the "API Resources" pages.
  Regardless of which Civis API key you use to run ``python tools/update_civis_api_spec.py``,
  the updated ``civis_api_spec.json`` only contains Civis API information available to
  a standard Civis Platform user.
* The URL https://civis-python.readthedocs.io auto-redirects to
  the "stable" URL https://civis-python.readthedocs.io/en/stable/ which reflects
  the most recent released civis-python version
  (every GitHub release with the tag "vX.Y.Z" triggers a new "stable" doc build
  on the Read The Docs site).
  In contrast, the "latest" URL https://civis-python.readthedocs.io/en/latest/ reflects
  the most recent commit to the upstream ``main`` branch of the civis-python codebase on GitHub.
  If there are doc changes (e.g., new or removed Civis API methods) that we'd really like to
  show up at the "stable" URL sooner rather than waiting for the next release with other code changes,
  we can make a patch release (i.e., increment the "Z" in "vX.Y.Z").

The doc build has its full dependencies listed in ``docs/requirements.txt``.
To update this file:

* Install the latest version of ``pip-tools``: ``pip install --upgrade pip-tools``.
* Run the ``pip-compile`` command at the top of ``docs/requirements.txt``, with the flag
  ``--upgrade`` added to upgrade all transitive dependencies as well.

To build the documentation locally, for testing and development:

* Install the full doc-related dependencies: ``pip install -r docs/requirements.txt``.
* Run ``sphinx-build -b html docs docs/build``.
  In case you would like for the "API Resources" page to locally show what a specific
  Civis Platform user would see from the Civis API
  (rather than use the available ``civis_api_spec.json`` for a standard Civis Platform user),
  set the environment variable ``CIVIS_API_KEY`` to this user's key
  and prepend this command with ``FETCH_REMOTE_RESOURCES=true``.


Command-line Interface (CLI)
----------------------------

After installing the Python package, you'll also have a ``civis`` command accessible from your shell. It surfaces a commandline interface to all of the regular Civis API endpoints, plus a few helpers. To get started, run ``civis --help``.
Please see the `CLI documentation <https://civis-python.readthedocs.io/en/stable/cli.html>`_ for more details.


Contributing
------------

See `CONTRIBUTING.md <CONTRIBUTING.md>`_ for information about contributing to this project.


License
-------

BSD-3

See `LICENSE.md <LICENSE.md>`_ for details.


For Maintainers
---------------

The `tools <tools/>`_ directory contains scripts that civis-python maintainers can
use (and maintain...). Please see their docstrings for usage.
Non-public information can be found by searching the internal documentation system
or consulting the current maintainers.
