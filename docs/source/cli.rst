Command Line Interface
======================

A command line interface (CLI) to Civis is provided. This can be invoked by
typing the command ``civis`` in the shell (sh, bash, zsh, etc.). It can also
be used in Civis container scripts where the Docker image has this client
installed.  Here's a simple example of printing the types of scripts.

.. code-block:: bash

   > civis scripts list-types
   - name: sql
   - name: python3
   - name: javascript
   - name: r
   - name: containers

Not all API endpoints are available through the CLI since some take complex
data types (e.g., arrays, objects/dictionaries) as input. However,
functionality is available for getting information about scripts, logs, etc.,
as well as executing already created scripts.

There are a few extra, CLI-only commands that wrap the Files API
endpoints to make uploading and downloading files easier:
``civis files upload $PATH`` and ``civis files download $FILEID $PATH``.

The default output format is YAML, but the ``--json-output`` allows you to
get output in JSON.
