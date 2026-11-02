"""Schemas for Civis Platform workflow definitions.

Civis Platform workflows:
https://support.civisanalytics.com/hc/en-us/articles/115004172983-Workflows-Basics

Mistral Workflow Language v2:
https://docs.openstack.org/mistral/latest/user/wf_lang_v2.html#workflows
"""

from __future__ import annotations

import inspect
from typing import Any

from civis import APIClient
from civis.resources import API_SPEC_PATH

_CLIENT = APIClient(local_api_spec=API_SPEC_PATH, api_key="no-key-needed")

# Civis Platform's Mistral only supports YAQL expressions, not Jinja.
_YAQL_EXPRESSION_PATTERN = r"(?s)^<%.*?%>\s*$"
_YAQL_EXPRESSION = {"type": "string", "pattern": _YAQL_EXPRESSION_PATTERN}


def _allow_yaql_expression(schema: dict) -> dict:
    return {
        **schema,
        "type": [schema["type"], "string"],
        "pattern": _YAQL_EXPRESSION_PATTERN,
    }


_BOOLEAN_OR_YAQL = _allow_yaql_expression({"type": "boolean"})
_NON_NEGATIVE_INTEGER_OR_YAQL = _allow_yaql_expression(
    {"type": "integer", "minimum": 0}
)
_NONEMPTY_STRING = {"type": "string", "minLength": 1}
_NONEMPTY_OBJECT = {"type": "object", "minProperties": 1}
_UNIQUE_STRING_LIST = {
    "type": "array",
    "items": _NONEMPTY_STRING,
    "uniqueItems": True,
    "minItems": 1,
}
_WITH_ITEMS_ITEM = {
    "type": "string",
    # e.g., "x in [1, 2, 3]" or "x in <% $.xs %>"
    "pattern": r"^\s*[\w\-]+\s*in\s*(\[.+\]|<%.*?%>)",
}


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


def _if_then_input(action: str, properties: dict, required: list[str]) -> dict:
    then: dict = {
        "properties": {
            "input": _allow_yaql_expression(
                {
                    "type": "object",
                    "properties": properties,
                    "required": required,
                    "additionalProperties": False,
                }
            ),
        },
    }
    if required:
        then["required"] = ["input"]
    return {"if": {"properties": {"action": {"const": action}}}, "then": then}


def _if_then_create_script(action: str) -> dict:
    endpoint, script_type = action.removeprefix("civis.").split(".")
    if script_type == "container":
        script_type = "containers"
    required, optional = _endpoint_method_params(endpoint, f"post_{script_type}")
    if "name" in required:
        # Civis Platform allows the workflow task name to be the script name.
        required.remove("name")
        optional.append("name")
    # Although we have type annotations for each key name,
    # leave the value unspecified as {} to allow YAQL expressions.
    properties: dict = {name: {} for name in required + optional}
    return _if_then_input(action, properties, required)


def _if_then_import() -> dict:
    required_post, optional_post = _endpoint_method_params("imports", "post")
    required_post_syncs, optional_post_syncs = _endpoint_method_params(
        "imports", "post_syncs"
    )
    if "name" in required_post:
        # Civis Platform allows the workflow task name to be the script name.
        required_post.remove("name")
        optional_post.append("name")
    if "id" in required_post_syncs:
        # The "id" will come from the job ID of the "post" call.
        required_post_syncs.remove("id")
    properties = {
        **{name: {} for name in required_post + optional_post},
        "syncs": {
            "type": "array",
            "items": {
                "type": "object",
                # Although we have type annotations for each key name,
                # leave the value unspecified as {} to allow YAQL expressions.
                "properties": {
                    name: {} for name in required_post_syncs + optional_post_syncs
                },
                "required": required_post_syncs,
                "additionalProperties": False,
            },
        },
    }
    return _if_then_input("civis.import", properties, required_post)


def _if_then_execute(action: str, id_name: str) -> dict:
    # Although the ID should be an integer,
    # leave it unspecified as {} to allow YAQL expressions.
    return _if_then_input(action, {id_name: {}}, [id_name])


# A transition target is a task name or an engine command ("fail", etc.).
_NEXT_TASK = {"type": "string", "pattern": r"^\S+$"}

# Mistral also allows a single {task: guard} object or a {"publish", "next"} object,
# but Civis Platform either rejects those or errors out on them.
TASK_TRANSITION_SCHEMA = {
    "oneOf": [
        _NEXT_TASK,
        {
            "type": "array",
            "items": {
                "oneOf": [
                    _NEXT_TASK,
                    # A task name with a YAQL guard expression.
                    {
                        "type": "object",
                        "minProperties": 1,
                        "maxProperties": 1,
                        "propertyNames": _NEXT_TASK,
                        "additionalProperties": _YAQL_EXPRESSION,
                    },
                ],
            },
            "uniqueItems": True,
            "minItems": 1,
        },
    ],
}

TASK_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        # Mistral overwrites "name" and "type" with the task key and workflow type.
        "name": {},
        "type": {},
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
                "std.fail",
                "std.noop",
            ],
        },
        "input": _allow_yaql_expression({"type": "object", "minProperties": 1}),
        "publish": _NONEMPTY_OBJECT,
        "publish-on-error": _NONEMPTY_OBJECT,
        "on-success": TASK_TRANSITION_SCHEMA,
        "on-error": TASK_TRANSITION_SCHEMA,
        "on-complete": TASK_TRANSITION_SCHEMA,
        "join": {
            "oneOf": [
                {"enum": ["all", "one"]},
                {"type": "integer", "minimum": 0},
            ],
        },
        "requires": _UNIQUE_STRING_LIST,
        "with-items": {
            "oneOf": [
                _WITH_ITEMS_ITEM,
                {
                    "type": "array",
                    "items": _WITH_ITEMS_ITEM,
                    "uniqueItems": True,
                    "minItems": 1,
                },
            ],
        },
        "keep-result": _BOOLEAN_OR_YAQL,
        "target": _NONEMPTY_STRING,
        "pause-before": _BOOLEAN_OR_YAQL,
        "wait-before": _NON_NEGATIVE_INTEGER_OR_YAQL,
        "wait-after": _NON_NEGATIVE_INTEGER_OR_YAQL,
        "fail-on": _BOOLEAN_OR_YAQL,
        "timeout": _NON_NEGATIVE_INTEGER_OR_YAQL,
        "retry": {
            "oneOf": [
                _NONEMPTY_STRING,
                {
                    "type": "object",
                    "properties": {
                        "count": _NON_NEGATIVE_INTEGER_OR_YAQL,
                        "delay": _NON_NEGATIVE_INTEGER_OR_YAQL,
                        "break-on": _YAQL_EXPRESSION,
                        "continue-on": _YAQL_EXPRESSION,
                    },
                    "required": ["count", "delay"],
                    "additionalProperties": False,
                },
            ],
        },
        "concurrency": _NON_NEGATIVE_INTEGER_OR_YAQL,
        "safe-rerun": _BOOLEAN_OR_YAQL,
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
        _if_then_create_script("civis.scripts.dbt"),
        _if_then_create_script("civis.scripts.custom"),
        _if_then_create_script("civis.enhancements.cass_ncoa"),
        _if_then_execute("civis.run_job", "job_id"),
        _if_then_execute("civis.workflows.execute", "workflow_id"),
        _if_then_import(),
    ],
    "additionalProperties": False,
}

TASK_DEFAULTS_SCHEMA = {
    "type": "object",
    "properties": {
        key: TASK_SCHEMA["properties"][key]
        for key in (
            "retry",
            "wait-before",
            "wait-after",
            "timeout",
            "pause-before",
            "concurrency",
            "fail-on",
            "on-success",
            "on-error",
            "on-complete",
            "safe-rerun",
            "requires",
        )
    },
    "minProperties": 1,
    "additionalProperties": False,
}

WORKFLOW_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {"version": {"enum": ["2.0", 2.0]}},
    # The one key other than "version" is the workflow's name.
    "additionalProperties": {
        "type": "object",
        "properties": {
            "type": {"enum": ["direct", "reverse"]},
            "description": {"type": "string"},
            "input": {
                "type": "array",
                "items": {
                    "oneOf": [
                        _NONEMPTY_STRING,
                        {"type": "object", "minProperties": 1, "maxProperties": 1},
                    ],
                },
                "uniqueItems": True,
                "minItems": 1,
            },
            "output": _NONEMPTY_OBJECT,
            "output-on-error": _NONEMPTY_OBJECT,
            "vars": _NONEMPTY_OBJECT,
            "tags": _UNIQUE_STRING_LIST,
            "task-defaults": TASK_DEFAULTS_SCHEMA,
            "tasks": {
                "type": "object",
                "propertyNames": {"maxLength": 255},
                "additionalProperties": TASK_SCHEMA,
                "minProperties": 1,
            },
        },
        "required": ["tasks"],
        "additionalProperties": True,  # Allow anchor definitions.
    },
    "required": ["version"],
    "minProperties": 2,
    "maxProperties": 2,
}
