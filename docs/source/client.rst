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

.. currentmodule:: civis

.. autoclass:: civis.APIClient
   :inherited-members:

.. toctree::
   responses
   api_resources
