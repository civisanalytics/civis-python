import jsonschema
import yaml

from ._schema import WORKFLOW_SCHEMA


class WorkflowValidationError(Exception):
    pass


def validate_workflow_yaml(wf_def: str, /) -> None:
    """Validate a YAML-formated workflow definition.

    Parameters
    ----------
    wf_def : str
        YAML-formated workflow definition.

    Raises
    ------
    WorkflowValidationError
        If the workflow definition is invalid.
    """
    _validate(yaml.safe_load(wf_def))


def _validate(wf: dict) -> None:
    try:
        jsonschema.validate(wf, WORKFLOW_SCHEMA)
    except jsonschema.ValidationError as e:
        raise WorkflowValidationError(e)
