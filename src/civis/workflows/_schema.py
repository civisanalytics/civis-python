# Mistral Workflow Language v2:
#   https://docs.openstack.org/mistral/latest/user/wf_lang_v2.html#workflows
# Civis Platform workflows:
#   https://support.civisanalytics.com/hc/en-us/articles/115004172983-Workflows-Basics

WORKFLOW_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "task": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "maxLength": 255},
                "description": {"type": "string"},
                "action": {
                    "type": "string",
                    "enum": [
                        "civis.run_job",
                        "civis.scripts.python3",
                        "civis.scripts.r",
                        "civis.scripts.sql",
                        "civis.scripts.container",
                        "civis.scripts.dbt",
                        "civis.scripts.custom",
                        "civis.media.optimization",
                        "civis.enhancements.cass_ncoa",
                        "civis.import",
                        "civis.workflows.execute",
                        "std.async_noop",
                        "std.echo",
                        "std.fail",
                        "std.noop",
                    ],
                },
                # TODO: Validate "input" depending on what "action" is.
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
            "additionalProperties": False,
        },
    },
    "type": "object",
    "properties": {
        "version": {"const": "2.0"},
    },
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
                "task-defaults": {"$ref": "#/$defs/task"},
                "tasks": {
                    "type": "object",
                    "patternProperties": {
                        "^.*$": {"$ref": "#/$defs/task", "required": ["action"]},
                    },
                },
            },
            "required": ["tasks"],
            "additionalProperties": True,  # Allow anchor definitions.
        },
    },
}
