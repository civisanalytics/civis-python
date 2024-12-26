Data Import and Export
======================

The ``civis.io`` namespace provides several functions for moving data in and
out of Civis.

Tables
------

Often, your data will be in structured format like a table in a relational
database, a CSV, or a dataframe. The following functions handle moving
structured data to and from Civis. When using these functions, it is
recommended to have ``pandas`` installed and to pass ``use_pandas=True`` in
the appropriate functions. If ``pandas`` is not installed, data returned
from Civis will all be treated as strings.

.. currentmodule:: civis.io

.. autosummary::
   :toctree: generated

   civis_to_csv
   civis_to_multifile_csv
   civis_file_to_table
   csv_to_civis
   dataframe_to_civis
   read_civis
   read_civis_sql
   export_to_civis_file
   split_schema_tablename

Files
-----

These functions will pass flat files to and from Civis.  This is useful
if you have data stored in binary or JSON format. Any type of file can
be stored in platform via the files endpoint.

.. currentmodule:: civis.io

.. autosummary::
   :toctree: generated

   civis_to_file
   dataframe_to_file
   file_id_from_run_output
   file_to_civis
   file_to_dataframe
   file_to_json
   json_to_file

Databases
---------

These functions move data from one database to another and expose an interface
to run SQL in the database. Use :func:`~civis.io.query_civis` when you need to
execute SQL that does not return data (for example, a ``GRANT`` or
``DROP TABLE`` statement).

.. currentmodule:: civis.io

.. autosummary::
   :toctree: generated

   transfer_table
   query_civis
