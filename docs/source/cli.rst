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

You can find out more information about a command by adding a ``--help`` option,
like ``civis scripts list --help``.

Job Logs
--------

These commands show job run logs in the format: "datetime message\\n" where
datetime is in ISO8601 format, like "2020-02-14T20:28:18.722Z".
If the job is still running, this command will continue outputting logs
until the run is done and then exit. If the run is already finished, it
will output all the logs from that run and then exit.

NOTE: These commands could miss some log entries from a currently-running
job. It does not re-fetch logs that might have been saved out of order, to
preserve the chronological order of the logs and without duplication.

- ``civis jobs follow-log $JOB_ID``

  Output live log from the most recent job run for the given job ID.

- ``civis jobs follow-run-log $JOB_ID $RUN_ID``

  Output live log from the given job and run ID.

Notebooks
---------

The following CLI-only commands make it easier to use Civis Platform as a
backend for your Jupyter notebooks.

- ``civis notebooks download $NOTEBOOK_ID $PATH``

  Download a notebook from Civis Platform to the requested file on the local filesystem.

- ``civis notebooks new [$LANGUAGE] [--mem $MEMORY] [--cpu $CPU]``

  Create a new notebook, allocate resources for it, and open it in a tab
  of your default web browser. This command is the most similar to ``jupyter notebook``.
  By default, Civis Platform will create a Python 3 notebook, but you can
  request any other language. Optional resource parameters let you allocate
  more memory or CPU to your notebook.

- ``civis notebooks up $NOTEBOOK_ID [--mem $MEMORY] [--cpu $CPU]``

  Allocate resources for a notebook which already exists in Civis Platform
  and open it in a tab of your default browser. Optional resource
  arguments allow you to change resources allocated to your notebook
  (default to using the same resources as the previous run).

- ``civis notebooks down $NOTEBOOK_ID``

  Stop a running notebook and free up the resources allocated to it.

- ``civis notebooks open $NOTEBOOK_ID``

  Open an existing notebook (which may or may not be running) in your default browser.

SQL
---

The Civis CLI allows for easy running of SQL queries on Civis Platform
through the following commands:

- ``civis sql [-n $MAX_LINES] -d $DATABASE_NAME -f $FILE_NAME``

  Read a SQL query from a text file and run it on the specified database.
  The results of the query, if any, will be shown after it completes
  (up to a maximum of $MAX_LINES rows, defaulting to 100).

- ``civis sql [-n $MAX_LINES] -d $DATABASE_NAME -c [$SQL_QUERY]``

  Instead of reading from a file, read query text from a command line
  argument. If you do not provide a query on the command line,
  the query text will be taken from stdin.

- ``civis sql -d $DATABASE_NAME [-f $SQL_FILE_NAME] -o $OUTPUT_FILE_NAME``

  With the `-o` or `--output` option specified, the complete results
  of the query will be downloaded to a CSV file at the requested location
  after the query completes.
