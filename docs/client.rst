API Client
==========

:class:`~civis.APIClient` is a class for handling requests to the Civis API.
An instantiated :class:`~civis.APIClient` contains a set of resources
(listed in :ref:`api_resources`) where each resource is an object with methods. By convention,
an instantiated :class:`~civis.APIClient` object is named ``client`` and API
requests are made with the following syntax:

.. code-block:: python

   client = civis.APIClient()
   response = client.resource.method(params)


.. toctree::
   :maxdepth: 1

   api_resources
   responses


Dynamically Created Resources and Methods
-----------------------------------------

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
| ``PATCH /workflows/1``            | ``client.workflows.patch(1, ...)``        |
+-----------------------------------+-------------------------------------------+
| ``POST /workflows/1/executions``  | ``client.workflows.post_executions(1)``   |
+-----------------------------------+-------------------------------------------+
| ``GET /workflows/1/executions/2`` | ``client.workflows.get_executions(1, 2)`` |
+-----------------------------------+-------------------------------------------+

If your code editor has auto-completion functionality (as many heavy IDEs do),
typing ``client.`` or ``client.workflows.`` should trigger the display of
the available resources or methods, respectively.
If you're running Python interactively
(e.g., the regular Python interactive shell, IPython, or a Jupyter notebook),
Python's built-in ``help`` function can be used to see lists of
available endpoints for a resource (e.g., ``help(client.workflows)``) or to get
documentation for a specific endpoint function (e.g.,
``help(client.workflows.list)``). The ``?`` operator in IPython (e.g., ``?client.workflows``) and the ``shift-tab``
hotkey in a Jupyter notebook also cause documentation to be displayed.

By default, the Civis API specification is downloaded from
the ``/endpoints`` endpoint the first time an :class:`~civis.APIClient` object is
instantiated.
To reduce overhead due to re-downloading the same API spec multiple times when multiple
``client`` instances are created, this API spec is cached in memory for a set amount of time.
If you're running Python interactively
(e.g., the regular Python interactive shell, IPython, or a Jupyter notebook),
the cached spec expires in 15 minutes,
and if you're running a script, the spec expires in 24 hours.
When a cached spec expires and a new ``client`` instance is created,
a new spec is downloaded from the Civis API
(so that updates to the Civis API, if any, are available to the new ``client``).
If you want to force a new spec to be downloaded, you can pass
``force_refresh_api_spec=True`` to the :class:`~civis.APIClient` constructor.
Note that for a given :class:`~civis.APIClient` object, the auto-generated resources and methods
attached to it are never refreshed, even if the Civis API is updated during the lifetime of this object.

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


.. _retries:

Retries
-------

The API client will automatically retry for certain API error responses.

If the error is one of [413, 429, 503] and the API client is told how long it needs
to wait before it's safe to retry (this is always the case with 429s, which are
rate limit errors), then the client will wait the specified amount of time
before retrying the request.

If the error is one of [429, 502, 503, 504] and the request is not a ``patch*`` or ``post*``
method, then the API client will retry the request several times, with an exponential delay,
to see if it will succeed. If the request is of type ``post*`` it will retry with the same parameters
for error codes [429, 503].

While the conditions under which retries are attempted are set as described above,
the behavior of the retries is customizable by passing in a :class:`tenacity.Retrying` instance
to the ``retries`` kwarg of :class:`civis.APIClient`.


Object Reference
----------------

.. currentmodule:: civis

.. autoclass:: civis.APIClient
   :members:
