#!/usr/bin/env python3

"""
Additional commands to add to the CLI beyond the OpenAPI spec.
"""
from __future__ import print_function
import functools
import os
import sys

import click
import requests
import webbrowser

import civis
from civis.io import file_to_civis, civis_to_file


# From http://patorjk.com/software/taag/#p=display&f=3D%20Diagonal&t=CIVIS
_CIVIS_ASCII_ART = r"""
  ,----..     ,---,               ,---,  .--.--.
 /   /   \ ,`--.' |       ,---.,`--.' | /  /    '.
|   :     :|   :  :      /__./||   :  :|  :  /`. /
.   |  ;. /:   |  ' ,---.;  ; |:   |  ';  |  |--`
.   ; /--` |   :  |/___/ \  | ||   :  ||  :  ;_
;   | ;    '   '  ;\   ;  \ ' |'   '  ; \  \    `.
|   : |    |   |  | \   \  \: ||   |  |  `----.   \
.   | '___ '   :  ;  ;   \  ' .'   :  ;  __ \  \  |
'   ; : .'||   |  '   \   \   '|   |  ' /  /`--'  /
'   | '/  :'   :  |    \   `  ;'   :  |'--'.     /
|   :    / ;   |.'      :   \ |;   |.'   `--'---'
 \   \ .'  '---'         '---" '---'
  `---`
"""


@click.command('upload')
@click.argument('path')
@click.option('--name', type=str, default=None,
              help="A name for the Civis File (defaults to the base file name")
@click.option('--expires-at', type=str, default=None,
              help="The date and time the file will expire "
                   "(ISO-8601 format, e.g., \"2017-01-15\" or "
                   "\"2017-01-15T15:25:10Z\"). "
                   "Set \"never\" for the file to not expire."
                   "The default is the default in Civis (30 days).")
def files_upload_cmd(path, name, expires_at):
    """Upload a local file to Civis and get back the File ID."""

    if name is None:
        name = os.path.basename(path)

    if expires_at is None:
        # Use the default in Civis platform (30 days).
        expires_kwarg = {}
    elif expires_at.lower() == "never":
        expires_kwarg = {"expires_at": None}
    else:
        expires_kwarg = {"expires_at": expires_at}

    with open(path, 'rb') as f:
        file_id = file_to_civis(f, name=name, **expires_kwarg)
    print(file_id)


@click.command('download')
@click.argument('file_id', type=int)
@click.argument('path')
def files_download_cmd(file_id, path):
    """Download a Civis File to a specified local path."""
    with open(path, 'wb') as f:
        civis_to_file(file_id, f)


@click.command('sql')
@click.option('--dbname', '-d', type=str, required=True,
              help='Execute the query on this Civis Platform database')
@click.option('--command', '-c', type=str, default=None,
              help='Execute a single input command string')
@click.option('--filename', '-f', type=click.Path(exists=True),
              help='Execute a query read from the given file')
@click.option('--output', '-o', type=click.Path(),
              help='Download query results to this file')
@click.option('--quiet', '-q', is_flag=True, help='Suppress screen output')
@click.option('-n', type=int, default=100,
              help="Display up to this many rows of the result. Max 100.")
def sql_cmd(dbname, command, filename, output, quiet, n):
    """\b Execute a SQL query in Civis Platform

    If neither a command nor an input file is specified, read
    the SQL command from stdin.
    If writing to an output file, use a Civis SQL script and write the
    entire query output to the specified file.
    If not writing to an output file, use a Civis Query, and return a
    preview of the results, up to a maximum of 100 rows.
    """
    if filename:
        with open(filename, 'rt') as f:
            sql = f.read()
    elif not command:
        # Read the SQL query from user input. This also allows use of a heredoc
        lines = []
        while True:
            try:
                _i = input()
            except (KeyboardInterrupt, EOFError):
                # The end of a heredoc produces an EOFError.
                break
            if not _i:
                break
            else:
                lines.append(_i)
        sql = '\n'.join(lines)
    else:
        sql = command

    if not sql:
        # If the user didn't enter a query, exit.
        if not quiet:
            print('Did not receive a SQL query.', file=sys.stderr)
        return

    if not quiet:
        print('\nExecuting query...', file=sys.stderr)
    if output:
        fut = civis.io.civis_to_csv(output, sql, database=dbname)
        fut.result()  # Block for completion and raise exceptions if any
        if not quiet:
            print("Downloaded the result of the query to %s." % output,
                  file=sys.stderr)
    else:
        fut = civis.io.query_civis(sql, database=dbname,
                                   preview_rows=n, polling_interval=3)
        cols = fut.result()['result_columns']
        rows = fut.result()['result_rows']
        if not quiet:
            print('...Query complete.\n', file=sys.stderr)
            print(_str_table_result(cols, rows))


def _str_table_result(cols, rows):
    """Turn a Civis Query result into a readable table."""
    # Determine the maximum width of each column.
    # First find the width of each element in each row, then find the max
    # width in each position.
    max_len = functools.reduce(
        lambda x, y: [max(z) for z in zip(x, y)],
        [[len(_v) for _v in _r] for _r in [cols] + rows])

    header_str = " | ".join("{0:<{width}}".format(_v, width=_l)
                            for _l, _v in zip(max_len, cols))
    tb_strs = [header_str, len(header_str) * '-']
    for row in rows:
        tb_strs.append(" | ".join("{0:>{width}}".format(_v, width=_l)
                                  for _l, _v in zip(max_len, row)))
    return '\n'.join(tb_strs)


@click.command('download')
@click.argument('notebook_id', type=int)
@click.argument('path')
def notebooks_download_cmd(notebook_id, path):
    """Download a notebook to a specified local path."""
    client = civis.APIClient()
    info = client.notebooks.get(notebook_id)
    response = requests.get(info['notebook_url'], stream=True)
    response.raise_for_status()
    chunk_size = 32 * 1024
    chunked = response.iter_content(chunk_size)
    with open(path, 'wb') as f:
        for lines in chunked:
            f.write(lines)


@click.command('new')
@click.argument('language', type=click.Choice(['python3', 'python2', 'r']),
                default='python3')
@click.option('--mem', type=int, default=None,
              help='Memory allocated for this notebook in MiB.')
@click.option('--cpu', type=int, default=None,
              help='CPU available for this notebook in 1/1000 of a core.')
def notebooks_new_cmd(language='python3', mem=None, cpu=None):
    """Create a new notebook and open it in the browser."""
    client = civis.APIClient()
    kwargs = {'memory': mem, 'cpu': cpu}
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    new_nb = client.notebooks.post(language=language, **kwargs)
    print("Created new {language} notebook with ID {id} in Civis Platform"
          " (https://platform.civisanalytics.com/#/notebooks/{id})."
          .format(language=language, id=new_nb.id))
    _notebooks_up(new_nb.id)
    _notebooks_open(new_nb.id)


@click.command('up')
@click.argument('notebook_id', type=int)
@click.option('--mem', type=int, default=None,
              help='Memory allocated for this notebook in MiB.')
@click.option('--cpu', type=int, default=None,
              help='CPU available for this notebook in 1/1000 of a core.')
def notebooks_up(notebook_id, mem=None, cpu=None):
    """Start an existing notebook and open it in the browser."""
    client = civis.APIClient()
    kwargs = {'memory': mem, 'cpu': cpu}
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    client.notebooks.patch(notebook_id, **kwargs)
    _notebooks_up(notebook_id)
    _notebooks_open(notebook_id)


def _notebooks_up(notebook_id):
    client = civis.APIClient()
    return client.notebooks.post_deployments(notebook_id)


@click.command('down')
@click.argument('notebook_id', type=int)
def notebooks_down(notebook_id):
    """Shut down a running notebook."""
    client = civis.APIClient()
    nb = client.notebooks.get(notebook_id)
    state = nb['most_recent_deployment']['state']
    if state not in ['running', 'pending']:
        print('Notebook is in state "{}" and can\'t be stopped.'.format(state))
    deployment_id = nb['most_recent_deployment']['deploymentId']
    client.notebooks.delete_deployments(notebook_id, deployment_id)


@click.command('open')
@click.argument('notebook_id', type=int)
def notebooks_open(notebook_id):
    """Open an existing notebook in the browser."""
    _notebooks_open(notebook_id)


def _notebooks_open(notebook_id):
    url = 'https://platform.civisanalytics.com/#/notebooks/{}?fullscreen=true'
    url = url.format(notebook_id)
    webbrowser.open(url, new=2, autoraise=True)


@click.command('civis', help="Print Civis")
def civis_ascii_art():
    print(_CIVIS_ASCII_ART)
