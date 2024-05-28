API Responses
=============

A Civis API call from ``client.<endpoint>.<method>`` returns a :class:`civis.response.Response` object
(or a :class:`civis.response.PaginatedResponse` object, if ``<method>`` is a "list" call):

.. code-block:: python

   >>> import civis
   >>> client = civis.APIClient()
   >>> response = client.scripts.get(12345)
   >>> response
   Response(id=12345,
            name='some script name',
            created_at='2018-06-11T20:43:07.000Z',
            updated_at='2018-06-11T20:43:19.000Z',
            author=(id=67890,
                    name='Platform User Name',
                    username='platformusername',
                    initials='PUN',
                    online=False),
            ...

To retrieve information from a :class:`civis.response.Response` object,
either the "getattr" or "getitem" syntax can be used:

.. code-block:: python

   >>> response.id
   12345
   >>> response['id']
   12345
   >>> response['name']
   'some script name'
   >>> response.name
   'some script name'

Nested response objects can be accessed using the same syntax:

.. code-block:: python

   >>> response.author['name']
   'Platform User Name'
   >>> response.author.name
   'Platform User Name'

Note that :class:`civis.response.Response` objects are read-only.
If you need to modify information from a response object,
call :func:`civis.response.Response.json` to get a dictionary representation of the response object.
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

.. autoclass:: civis.response.Response
   :members:

.. autoclass:: civis.response.PaginatedResponse
   :members:

.. autoclass:: civis.futures.CivisFuture
   :members:

Helper Functions
----------------

.. autofunction:: civis.find
.. autofunction:: civis.find_one
