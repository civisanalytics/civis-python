API Client
==========

:class:`~civis.APIClient` is a class for handling requests to the Civis API.
An instantiated :class:`~civis.APIClient` contains a set of resources
(listed below) where each resource is an object with methods. By convention,
an instantiated :class:`~civis.APIClient` object is named ``client`` and API
requests are made with the following syntax:

.. code-block:: python

   client = civis.APIClient()
   response = client.resource.method(params)

The methods on :class:`~civis.APIClient` are created dynamically at runtime
by parsing an :class:`python:collections.OrderedDict` representation of the
Civis API specification.
The methods are generated based on the path and HTTP method used with each
endpoint. For example, ``GET /workflows/1`` can be accessed with
``client.workflows.get(1)``. ``GET`` endpoints that don’t end in a parameter
use a ``list`` method instead.
Below are examples of endpoints and how they map to API Client methods:

+-----------------------------------+-------------------------------------------+
| Endpoint                          | API Client Method                         |
+===================================+===========================================+
| ``GET /workflows``                | ``client.workflows.list()``               |
+-----------------------------------+-------------------------------------------+
| ``GET /workflows/1``              | ``client.workflows.get(1)``               |
+-----------------------------------+-------------------------------------------+
| ``GET /workflows/1/executions``   | ``client.workflows.list_executions(1)``   |
+-----------------------------------+-------------------------------------------+
| ``PATCH /workflows/1``            | ``client.workflows.patch(1)``             |
+-----------------------------------+-------------------------------------------+
| ``POST /workflows/1/executions``  | ``client.workflows.post_executions(1)``   |
+-----------------------------------+-------------------------------------------+
| ``GET /workflows/1/executions/2`` | ``client.workflows.get_executions(1, 2)`` |
+-----------------------------------+-------------------------------------------+
| ``DELETE /workflows/1``           | ``client.workflows.delete(1)``            |
+-----------------------------------+-------------------------------------------+

By default, the Civis API specification specification is downloaded from
the ``/endpoints`` endpoint the first time :class:`~civis.APIClient` is
instantiated (and cached in memory for the remainder of the program's run).
In some circumstances, it may be useful to use a local cache of the API
specification rather than downloading the spec.  This can be done by passing
the specification to the client through the parameter ``local_api_spec`` as
either the :class:`python:collections.OrderedDict` or a filename where the
specification has been saved.

.. code-block:: python

   api_key = os.environ['CIVIS_API_KEY']
   spec = civis.resources.get_api_spec(api_key)

   # From OrderedDict
   client = civis.APIClient(local_api_spec=spec)

   # From file
   with open('local_api_spec.json', 'w') as f:
       json.dump(spec, f)
   client = civis.APIClient(local_api_spec='local_api_spec.json')

.. currentmodule:: civis

.. autoclass:: civis.APIClient
   :inherited-members:

.. toctree::
   responses
   api_resources
