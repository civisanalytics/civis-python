#!/usr/bin/env python3

"""
Additional commands to add to the CLI beyond the OpenAPI spec.
"""

import os

import click

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
    """Download a Civis File to the specified local path."""
    with open(path, 'wb') as f:
        civis_to_file(file_id, f)


@click.command('civis', help="Print Civis")
def civis_ascii_art():
    print(_CIVIS_ASCII_ART)
