#!/usr/bin/env python3

"""
Civis Command Line Interface

This is based on https://github.com/zalando/openapi-cli-client,
which has an Apache 2.0 License:
https://github.com/zalando/openapi-cli-client/blob/master/LICENSE
"""

import calendar
from collections import OrderedDict
from functools import partial
import json
import logging
import os
import re
import sys
import time
from warnings import warn

import click
from jsonref import JsonRef
import yaml
from requests import Request
from civis.cli._cli_commands import (
    civis_ascii_art, files_download_cmd, files_upload_cmd,
    jobs_follow_log, jobs_follow_run_log, notebooks_download_cmd,
    notebooks_new_cmd, notebooks_up, notebooks_down, notebooks_open, sql_cmd)
from civis.resources import get_api_spec, CACHED_SPEC_PATH
from civis.resources._resources import parse_method_name
from civis._utils import open_session, retry_request, MAX_RETRIES


_REPLACEABLE_COMMAND_CHARS = re.compile(r'[^A-Za-z0-9]+')
_BASE_API_URL = "https://api.civisanalytics.com"
CLI_USER_AGENT = 'civis-cli'


class YAMLParamType(click.ParamType):
    """
    A click parameter type for YAML/JSON files.
    See http://click.pocoo.org/5/parameters/#implementing-custom-types.
    """

    name = 'yamlpath'

    def convert(self, value, param, ctx):
        if value is None:
            return value

        try:
            with open(value) as f:
                result = yaml.safe_load(f)
                return result
        except Exception:
            self.fail("Could not load YAML from path: %s" % value, param, ctx)


YAML = YAMLParamType()


# A map from OpenAPI type strings to python types.
# See https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#data-types.  # noqa: E501
_TYPE_MAP = {
    "integer": int,
    "number": float,
    "boolean": bool,
    "string": str,
    "object": YAML,
    "array": YAML
}


def get_api_key():
    try:
        return os.environ["CIVIS_API_KEY"]
    except KeyError:
        print("You must set the CIVIS_API_KEY environment variable.",
              file=sys.stderr)
        sys.exit(1)


def get_base_api_url():
    return os.getenv('CIVIS_API_ENDPOINT') or _BASE_API_URL


def camel_to_snake(s):
    return re.sub(r'([A-Z]+)', r'_\1', s).lower()


def munge_name(s):
    """Replace non-alphanumeric characters with dashes."""
    result = _REPLACEABLE_COMMAND_CHARS.sub('-', s.lower()).strip('-')
    return result


def make_operation_name(path, method, resource_name):
    """Create an appropriate CLI command for an operation.

    Examples
    --------
    >>> make_operation_name('/scripts/r/{id}/runs/{run_id}', 'get', 'scripts')
    get-r-runs
    """
    path = path.lower().lstrip('/')

    # Remove resource prefix. Note that the path name for some operations is
    # just the resource name (e.g., /databases).
    if path.startswith(resource_name):
        path = path[len(resource_name):].strip("-")

    name = parse_method_name(method, path).replace("_", "-")
    return name


def param_case_map(param_names):
    # Return a map from snake_case parameter names (for click) to camelCase
    # versions for the API. We need this mapping because the API
    # expects/prefers camelCase but click prefers snake_case.
    # We could have make a snake_to_camel function, but ensuring that that
    # was in sync with camel_to_snake might be difficult because there are some
    # edge cases like provideAPIKey <-> provide_api_key.
    result = {camel_to_snake(k): k for k in param_names}
    return result


def invoke(method, path, op, *args, **kwargs):
    """
    If json_output is in `kwargs` then the output is json. Otherwise, it is
    yaml.
    """
    # Remove None b/c click passes everything in as None if it's not set.
    kwargs = {k: v for k, v in kwargs.items()
              if v is not None}
    json_output = kwargs.pop('json_output', False)

    # Construct the body of the request.
    body = {}
    body_params = [p for p in op['parameters'] if p['in'] == 'body']
    if body_params:
        if len(body_params) != 1:
            raise ValueError(
                "There can be only one body parameter, "
                f"but {len(body_params)} are found: {body_params}"
            )
        props = body_params[0]['schema']['properties']
        param_map = param_case_map(props.keys())
        body = {param_map[k]: v for k, v in kwargs.items()
                if k in param_map}

    # Construct the query part of the request.
    query_names = {p['name'] for p in op['parameters'] if p['in'] == 'query'}
    param_map = param_case_map(query_names)
    query = {param_map[k]: v for k, v in kwargs.items()
             if k in param_map}

    # Make the request.
    request_info = dict(
        params=query,
        json=body,
        url=get_base_api_url() + path.format(**kwargs),
        method=method
    )
    with open_session(get_api_key(), user_agent=CLI_USER_AGENT) as sess:
        request = Request(**request_info)
        pre_request = sess.prepare_request(request)
        response = retry_request(method, pre_request, sess, MAX_RETRIES)

    # Print the response to stderr and set exit code to 1 if there was an error
    output_file = sys.stdout
    exit_code = 0
    if not (200 <= response.status_code < 300):
        output_file = sys.stderr
        exit_code = 1

    # Print the output, if there is any.
    # For commands such as DELETE /scripts/containers/{script_id}/runs/{id},
    # response ends up being " " here.
    try:
        if json_output:
            json.dump(response.json(), output_file)
        else:
            yaml.safe_dump(response.json(), output_file,
                           default_flow_style=False)
        output_file.flush()

    # json throws a ValueError if it is passed a blank string to load.
    except ValueError as e:
        # If the message was not blank, print an error message.
        # Otherwise, do nothing.
        if response.text.strip():
            print("Error parsing response: {}".format(e), file=sys.stderr)

    sys.exit(exit_code)


def retrieve_spec_dict(api_version="1.0"):
    """Retrieve the API specification from a cached version or from Civis."""

    refresh_spec = True
    now_timestamp = calendar.timegm(time.gmtime())

    try:
        # If the cached spec is from the last 24 hours, use it.
        modified_time = os.path.getmtime(CACHED_SPEC_PATH)
        if now_timestamp - modified_time < 24 * 3600:
            refresh_spec = False
            with open(CACHED_SPEC_PATH) as f:
                spec_dict = json.load(f, object_pairs_hook=OrderedDict)
    except (FileNotFoundError, ValueError):
        # If the file doesn't exist or we can't parse it, just keep going.
        refresh_spec = True

    # Download the spec and cache it in the user's home directory.
    if refresh_spec:
        spec_dict = get_api_spec(get_api_key(), api_version=api_version,
                                 user_agent=CLI_USER_AGENT)
        with open(CACHED_SPEC_PATH, "w") as f:
            json.dump(spec_dict, f)
    return spec_dict


def add_extra_commands(cli):
    """Add useful commands that are not in the OpenAPI spec."""
    files_cmd = cli.commands['files']
    files_cmd.add_command(files_download_cmd)
    files_cmd.add_command(files_upload_cmd)
    notebooks_cmd = cli.commands['notebooks']
    notebooks_cmd.add_command(notebooks_download_cmd)
    notebooks_cmd.add_command(notebooks_new_cmd)
    notebooks_cmd.add_command(notebooks_up)
    notebooks_cmd.add_command(notebooks_down)
    notebooks_cmd.add_command(notebooks_open)
    jobs_cmd = cli.commands['jobs']
    jobs_cmd.add_command(jobs_follow_log)
    jobs_cmd.add_command(jobs_follow_run_log)
    cli.add_command(civis_ascii_art)

    cli.add_command(sql_cmd)


def configure_log_level():
    if os.getenv('CIVIS_LOG_LEVEL'):
        logging.basicConfig(level=os.getenv('CIVIS_LOG_LEVEL'))


def generate_cli():
    configure_log_level()
    spec = retrieve_spec_dict()

    # Replace references in the spec so that we don't have to worry about them
    # when making the CLI.
    spec = JsonRef.replace_refs(spec)

    cli = click.Group()

    # Iterate through top-level resources (e.g., Scripts, Files, Models).
    groups = {}
    for path, path_dict in spec['paths'].items():
        resource = path.strip('/').split('/')[0]
        grp = groups.get(resource)
        if grp is None:
            grp = click.Group(munge_name(resource),
                              short_help='Manage {}'.format(resource))
            cli.add_command(grp)
            groups[resource] = grp

        add_path_commands(path, path_dict, grp, resource)

    add_extra_commands(cli)

    return cli


def add_path_commands(path, path_dict, grp, resource):
    """Add commands for the given resource."""

    for method, op_dict in path_dict.items():

        # Set up a command with the appropriate name and help info.
        name = make_operation_name(path, method, resource)
        summary = op_dict.get('summary', '')
        op_help = summary
        description = op_dict.get('description', '')
        if description:
            op_help += '\n\n' + description
        op_help += '\n\n' + path
        callback = partial(invoke, method=method, path=path, op=op_dict)
        cmd = click.Command(name,
                            callback=callback,
                            short_help=summary,
                            help=op_help)

        add_command_params(cmd, op_dict)

        # Add an option for JSON output.
        cmd.params.append(click.Option(['--json-output'], is_flag=True,
                                       help="output in JSON instead of YAML"))

        if cmd.name in grp.commands:
            warn('conflicting command name "%s" for path "%s"' %
                 (cmd.name, path))

        grp.add_command(cmd)


def add_command_params(cmd, op_dict):
    """Add parameters to the click command for an API operation."""

    # Extract properties of objects in body to make click params for them.
    parameters_orig = op_dict.get('parameters', [])
    parameters = []
    for p in parameters_orig:
        if p['in'] == 'body':
            body_params = []
            req = p['schema'].get('required', [])
            for prop_name, prop_info in p['schema']['properties'].items():
                p_new = dict(
                    name=camel_to_snake(prop_name),
                    type=prop_info['type'],
                    description=prop_info.get('description', ''),
                    required=req == 'all' or prop_name in req,
                )
                body_params.append(p_new)

            # Sort the parameters since they don't have an order as properties.
            body_params = sorted(body_params, key=lambda x: x['name'])

            parameters.extend(body_params)
        else:
            parameters.append(p)

    # Specify the parameters for this command.
    for p in parameters:
        param_type_spec = p.get('type', 'string')
        param_type = _TYPE_MAP[param_type_spec]
        description = p.get('description', '')

        if p['required']:
            cmd.help += "\n\n{} ({}) - {}".format(
                p['name'].upper(), param_type_spec, description)
            arg = click.Argument([p['name'].lower()],
                                 type=param_type)
            cmd.params.append(arg)
        else:
            arg = click.Option(['--' + munge_name(p['name'].lower())],
                               help=description,
                               type=param_type)
            cmd.params.append(arg)


def main():
    # Note: this needs to be its own function so that setup.py can make it an
    # entry point.
    cli = generate_cli()
    cli()


if __name__ == '__main__':
    main()
