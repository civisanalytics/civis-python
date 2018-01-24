#!/usr/bin/env python3

"""
Additional commands to add to the CLI beyond the OpenAPI spec.
"""

import os

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
