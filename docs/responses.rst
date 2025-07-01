.. _responses:

Responses
=========

A Civis API call from ``client.<endpoint>.<method>`` returns one of the "response" objects:

* :class:`civis.PaginatedResponse` if ``<method>`` is a "list" call with ``iterator=True``,
* :class:`civis.ListResponse` if ``<method>`` is a "list" call that either doesn't have an ``iterator`` argument or has ``iterator=False``, or
* :class:`civis.Response`.

.. code-block:: python

   >>> import civis
   >>> client = civis.APIClient()
   >>> response = client.scripts.get(12345)
   >>> response
   Response({'id': 12345,
             'name': 'some script name',
             'created_at': '2018-06-11T20:43:07.000Z',
             'updated_at': '2018-06-11T20:43:19.000Z',
             'author': Response({'id': 67890,
                                 'name': 'Platform User Name',
                                 'username': 'platformusername',
                                 'initials': 'PUN',
                                 'online': False}),
             ...

To retrieve information from a :class:`civis.Response` object,
use the attribute syntax:

.. code-block:: python

   >>> response.id
   12345
   >>> response.name
   'some script name'
   >>> response.author
   Response({'id': 67890,
             'name': 'Platform User Name',
             'username': 'platformusername',
             'initials': 'PUN',
             'online': False})
   >>> response.author.username
   'platformusername'

:class:`civis.APIClient` is type-annotated for the returned :class:`civis.Response` object
of a given Civis API endpoint's method, including the expected attributes.
These type annotations facilitate code development and testing:

* If your IDE has auto-complete support, typing ``response.`` from the example above
  prompts possible attributes ``{id, name, author, ...}``.
* Type checking (by tools such as ``mypy``) in test suites and continuous integration
  helps to catch issues such as typos and unexpected attributes.

Alternatively, the "getitem" syntax can also be used:

.. code-block:: python

   >>> response['id']
   12345
   >>> response['author']
   Response({'id': 67890,
             'name': 'Platform User Name',
             'username': 'platformusername',
             'initials': 'PUN',
             'online': False})

Although the "getitem" syntax would lose the benefits of the attribute syntax
listed above, the "getitem" syntax is more user-friendly when an attribute name
is available programmatically,
e.g., ``response[foo]`` versus ``getattr(response, foo)``.

Note that :class:`civis.Response` objects are read-only.
If you need to modify information from a response object,
call :func:`civis.Response.json` to get a dictionary representation of the response object.
You can then modify this dictionary as needed:

.. code-block:: python

   >>> response.arguments = ...  # !!! Raises CivisImmutableResponseError
   >>> response['arguments'] = ...  # !!! Raises CivisImmutableResponseError
   >>>
   >>> response_json = response.json()
   >>> response_json['arguments'] = {'new_arg_for_a_similar_script': 'some_value'}
   >>> # use response_json downstream, e.g., to create a new Civis Platform script

Response Types
--------------

.. autoclass:: civis.Response
   :members:

.. autoclass:: civis.PaginatedResponse
   :members:

.. autoclass:: civis.ListResponse
   :members: json
..
   use ":members: json" because ListResponse subclasses `list`, and we don't want to show methods for `list`.

.. autoclass:: civis.futures.CivisFuture
   :members:

Helper Functions
----------------

.. autofunction:: civis.find
.. autofunction:: civis.find_one
