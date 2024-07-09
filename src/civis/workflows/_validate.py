"""Validation for Civis Platform workflow definitions."""

from __future__ import annotations

import jsonschema
import yaml

from ._schemas import WORKFLOW_SCHEMA


_TASK_TRANSITION_ENGINE_COMMANDS = frozenset(["pause", "succeed", "fail"])


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
    _validate_workflow_tasks(wf)


def _get_next_task_names(next_tasks: str | list | dict | None) -> list[str]:
    """Next task names under {'on-success', 'on-error', 'on-complete'}."""
    if next_tasks is None:
        return []
    elif isinstance(next_tasks, str):
        return [next_tasks]
    elif isinstance(next_tasks, list) and isinstance(next_tasks[0], str):
        return next_tasks
    elif isinstance(next_tasks, list) and isinstance(next_tasks[0], dict):
        return [list(nt.keys())[0] for nt in next_tasks]
    elif isinstance(next_tasks, dict):
        return _get_next_task_names(next_tasks.get("next"))


def _validate_workflow_tasks(wf: dict) -> None:
    """Custom checks for workflow tasks that aren't amenable to jsonschema validation"""
    key = None
    for k in wf.keys():
        if k != "version":
            key = k
            break
    try:
        tasks = wf[key]["tasks"]
    except KeyError:
        raise WorkflowValidationError("No workflow tasks found")
    for task_name, task in tasks.items():
        recognized_task_names = set(tasks.keys()) | _TASK_TRANSITION_ENGINE_COMMANDS
        for next_task_group_name in ("on-success", "on-error", "on-complete"):
            next_task_names = _get_next_task_names(task.get(next_task_group_name))
            if task_name in next_task_names:
                raise WorkflowValidationError(
                    "A task cannot transition to itself. "
                    f"{task_name!r} transitions to itself in {next_task_group_name!r}."
                )
            for next_task_name in next_task_names:
                if next_task_name not in recognized_task_names:
                    raise WorkflowValidationError(
                        f"Task {task_name!r} transitions "
                        f"to an undefined task {next_task_name!r} "
                        f"in {next_task_group_name!r}."
                    )
