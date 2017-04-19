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
Civis API specification.  By default, this specification is downloaded from
the ``/endpoints`` endpoint the first time :class:`~civis.APIClient` is
instantiated (and cached in memory for the remainder of the program's run).
In some circumstances, it may be useful to use a local cache of the API
specification rather than downloading the spec.  This can be done by passing
the specification to the client through the parameter ``local_api_spec`` as
either the :class:`python:collections.OrderedDict` or a filename where the
specification has been saved.

.. code-block:: python

   api_key = os.environ['CIVIS_API_KEY']
   spec = civis.resources.get_swagger_spec(api_key)

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
