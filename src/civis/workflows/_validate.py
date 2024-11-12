"""Validation for Civis Platform workflow definitions."""

from __future__ import annotations

import io

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
    _validate_workflow_yaml_ascii_only(wf_def)
    wf_def_dict = yaml.safe_load(wf_def)
    _validate_workflow_by_schema(wf_def_dict)
    _validate_workflow_tasks(wf_def_dict)


def _validate_workflow_by_schema(wf: dict) -> None:
    try:
        jsonschema.validate(wf, WORKFLOW_SCHEMA)
    except jsonschema.ValidationError as e:
        raise WorkflowValidationError(e)


def _validate_workflow_yaml_ascii_only(wf_def: str) -> None:
    for line_no, line in enumerate(io.StringIO(wf_def), 1):
        for char_no, char in enumerate(line, 1):
            if not char.isascii():
                raise WorkflowValidationError(
                    "Workflow definition YAML cannot contain non-ASCII characters: "
                    f"(line {line_no}) {line!r}, (character {char_no}) {char!r}"
                )


def _get_next_task_names(next_tasks: str | list | dict | None) -> list[str]:
    """Next task names under {'on-success', 'on-error', 'on-complete'}."""
    if next_tasks is None:
        return []
    elif isinstance(next_tasks, str):
        return [next_tasks]
    elif isinstance(next_tasks, list):
        task_names = []
        for task in next_tasks:
            if isinstance(task, str):
                task_names.append(task)
            elif isinstance(task, dict):
                task_names.append(list(task.keys())[0])
            else:
                raise WorkflowValidationError(
                    "each item in next task list must be either of type str or dict: "
                    f"{type(task)} ({task})"
                )
        return task_names
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
