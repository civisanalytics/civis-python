# Mistral Workflow Language v2:
#   https://docs.openstack.org/mistral/latest/user/wf_lang_v2.html#workflows
# Civis Platform workflows:
#   https://support.civisanalytics.com/hc/en-us/articles/115004172983-Workflows-Basics

from __future__ import annotations

import inspect

from civis import APIClient
from civis.resources import API_SPEC_PATH


_CLIENT = APIClient(local_api_spec=API_SPEC_PATH)


def _endpoint_method_params(endpoint: str, method: str) -> tuple[list[str], list[str]]:
    endpt = getattr(_CLIENT, endpoint)
    meth = getattr(endpt, method)
    method_params = inspect.signature(meth).parameters
    required, optional = [], []
    for name, param in method_params.items():
        if param.default == inspect.Parameter.empty:
            required.append(name)
        else:
            optional.append(name)
    return required, optional


def _if_then_create_script(action: str) -> dict:
    endpoint, script_type = action.removeprefix("civis.").split(".")
    if script_type == "container":
        script_type = "containers"
    required, optional = _endpoint_method_params(endpoint, f"post_{script_type}")
    if "name" in required:
        # Civis Platform allows the workflow task name to be the script name.
        required.remove("name")
    return {
        "if": {"properties": {"action": {"const": action}}},
        "then": {
            "properties": {
                "input": {
                    "type": "object",
                    "properties": {name: {} for name in required + optional},
                    "required": required,
                    "additionalProperties": False,
                }
            }
        },
    }


def _if_then_import() -> dict:
    required_post, optional_post = _endpoint_method_params("imports", "post")
    required_post_syncs, optional_post_syncs = _endpoint_method_params(
        "imports", "post_syncs"
    )
    if "name" in required_post:
        # Civis Platform allows the workflow task name to be the script name.
        required_post.remove("name")
    properties = {
        **{name: {} for name in required_post + optional_post},
        "syncs": {
            "type": "object",
            "properties": {
                name: {} for name in required_post_syncs + optional_post_syncs
            },
            "required": required_post_syncs,
            "additionalProperties": False,
        },
    }
    return {
        "if": {"properties": {"action": {"const": "civis.import"}}},
        "then": {
            "properties": {
                "input": {
                    "type": "object",
                    "properties": properties,
                    "required": required_post,
                    "additionalProperties": False,
                }
            }
        },
    }


def _if_then_execute(action: str, id_name: str) -> dict:
    return {
        "if": {"properties": {"action": {"const": action}}},
        "then": {
            "properties": {
                "input": {
                    "type": "object",
                    "properties": {id_name: {}},
                    "required": [id_name],
                    "additionalProperties": False,
                }
            }
        },
    }


TASK_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "name": {"type": "string", "maxLength": 255},
        "description": {"type": "string"},
        "action": {
            "type": "string",
            "enum": [
                "civis.scripts.python3",
                "civis.scripts.r",
                "civis.scripts.sql",
                "civis.scripts.javascript",
                "civis.scripts.container",
                "civis.scripts.dbt",
                "civis.scripts.custom",
                "civis.enhancements.cass_ncoa",
                "civis.import",
                "civis.run_job",
                "civis.workflows.execute",
                "std.async_noop",
                "std.echo",
                "std.fail",
                "std.noop",
            ],
        },
        "input": {"type": "object"},
        "publish": {"type": "object"},
        "publish-on-error": {"type": "object"},
        "on-success": {"type": "array"},
        "on-error": {"type": "array"},
        "on-complete": {"type": "array"},
        "join": {
            "oneOf": [
                {"const": "all"},
                {"type": "integer", "minimum": 1},
            ],
        },
        "requires": {"type": "array"},
        "with-items": {
            "oneOf": [
                {"type": "string"},
                {"type": "array", "items": {"type": "string"}},
            ]
        },
        "keep-result": {"type": "boolean"},
        "target": {"type": "string"},
        "pause-before": {"type": "boolean"},
        "wait-before": {"type": "number", "minimum": 0},
        "wait-after": {"type": "number", "minimum": 0},
        "fail-on": {},
        "timeout": {"type": "number", "minimum": 0},
        "retry": {
            "oneOf": [
                {"type": "string"},
                {
                    "type": "object",
                    "properties": {
                        "count": {"type": "number", "minimum": 0},
                        "delay": {"type": "number", "minimum": 0},
                        "break-on": {"type": "string"},
                        "continue-on": {"type": "string"},
                    },
                },
            ]
        },
        "concurrency": {"type": "number", "minimum": 1},
        "safe-rerun": {"type": "boolean"},
    },
    "required": ["action"],
    "allOf": [
        # If "action" is one of the Civis-defined ones,
        # then the allowed properties under "input" closely mirror the relevant
        # API endpoint method.
        _if_then_create_script("civis.scripts.python3"),
        _if_then_create_script("civis.scripts.r"),
        _if_then_create_script("civis.scripts.sql"),
        _if_then_create_script("civis.scripts.javascript"),
        _if_then_create_script("civis.scripts.container"),
        # TODO: dbt job type is upcoming!
        # _if_then_create_script("civis.scripts.dbt"),
        _if_then_create_script("civis.scripts.custom"),
        _if_then_create_script("civis.enhancements.cass_ncoa"),
        _if_then_execute("civis.run_job", "job_id"),
        _if_then_execute("civis.workflows.execute", "workflow_id"),
        _if_then_import(),
    ],
    "additionalProperties": False,
}


WORKFLOW_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {"version": {"const": "2.0"}},
    "patternProperties": {
        "^(?:(?!version).)*$": {
            "type": "object",
            "properties": {
                "type": {"type": "string"},
                "description": {"type": "string"},
                "input": {
                    "type": "array",
                    "items": {"oneOf": [{"type": "string"}, {"type": "object"}]},
                },
                "output": {},
                "output-on-error": {},
                "task-defaults": {
                    k: v for k, v in TASK_SCHEMA.items() if k != "required"
                },
                "tasks": {
                    "type": "object",
                    "patternProperties": {"^.*$": TASK_SCHEMA},
                },
            },
            "required": ["tasks"],
            "additionalProperties": True,  # Allow anchor definitions.
        },
    },
    "minProperties": 2,
    "maxProperties": 2,
}