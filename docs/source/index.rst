.. Civis Client documentation master file

Civis API Python Client
=======================

The Civis API Python client is a Python package that helps analysts and
developers interact with the Civis Platform. The package includes a set of
tools around common workflows as well as a convenient interface to make
requests directly to the Civis API.


Installation
------------

The recommended install method is pip:

.. code-block:: bash

   pip install civis

Alternatively, you may clone the code from github and build from source:

.. code-block:: bash

   git clone https://github.com/civisanalytics/civis-python.git
   cd civis-python
   python setup.py install

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
from the Civis Platform. Install these dependencies with

.. code-block:: bash

   pip install scikit-learn
   pip install pandas


Python version support
----------------------

Python 2.7, 3.4, 3.5, and 3.6


Authentication
--------------

In order to make requests to the Civis API, you will need an API key that is
unique to you. Instructions for creating a new key are found here:
https://civis.zendesk.com/hc/en-us/articles/216341583-Generating-an-API-Key.
By default, the Python client will look for your key in the environment
variable ``CIVIS_API_KEY``. To add the API key to your environment, copy
the key you generated to your clipboard and follow the instructions below
for your operating system.

**Mac**

Open ``.bash_profile`` in TextEdit:

.. code-block:: bash

   cd ~/
   touch .bash_profile
   open -e .bash_profile

Then add the following line, replacing ``api_key_here`` with your key::

   export CIVIS_API_KEY="api_key_here"

**Linux**

Open ``.bash_profile`` in your favorite editor (nano is used here):

.. code-block:: bash

   cd ~/
   nano .bash_profile

Then add the following line, replacing ``api_key_here`` with your key::

   export CIVIS_API_KEY="api_key_here"


User Guide
----------

For a more detailed walkthrough, see the :ref:`user_guide`.


Client API Reference
--------------------

.. toctree::
   :maxdepth: 1

   user_guide
   io
   ml
   client
   cli


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
