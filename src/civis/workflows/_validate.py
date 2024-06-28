"""Validation for Civis Platform workflow definitions."""

import jsonschema
import yaml

from ._schemas import WORKFLOW_SCHEMA


class WorkflowValidationError(Exception):
    pass


def validate_workflow_yaml(wf_def: str, /) -> None:
    """Validate a YAML-formatted workflow definition.

    Parameters
    ----------
    wf_def : str
        YAML-formatted workflow definition.

    Raises
    ------
    WorkflowValidationError
        If the workflow definition is invalid.
    """
    _validate_workflow(yaml.safe_load(wf_def))


def _validate_workflow(wf: dict) -> None:
    try:
        jsonschema.validate(wf, WORKFLOW_SCHEMA)
    except jsonschema.ValidationError as e:
        raise WorkflowValidationError(e)
