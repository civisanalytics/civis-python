"""Validation for Civis Platform workflow definitions."""

from __future__ import annotations

import graphlib
import io
import re
from collections import Counter
from collections.abc import Iterator
from typing import Any

import jsonschema
import yaml

from ._schemas import WORKFLOW_SCHEMA

_TASK_TRANSITION_ENGINE_COMMANDS = frozenset(["pause", "succeed", "fail"])
_TASK_TRANSITION_KEYS = ("on-success", "on-error", "on-complete")
_MAX_LENGTH_JOIN_TASK_NAME = 208

# Mistral's parsing of inline action parameters, e.g., "civis.run_job job_id=123".
_INLINE_ACTION = re.compile(r"[\w\.]+[^=\(\s\"]*")
_INLINE_ACTION_PARAM = re.compile(
    r"([-_\w]+)=" r"(\"[^\"]*\"\s*|'[^']*'\s*|<%.*?%>|\[.*\]\s*|true|false|null|\d+)"
)
_TASKS_FUNCTION = re.compile(r"tasks\s*\(")


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
    try:
        # Civis Platform converts tabs to spaces before parsing the YAML.
        wf_def_dict = yaml.safe_load(wf_def.replace("\t", "  "))
    except yaml.YAMLError as e:
        raise WorkflowValidationError(f"Invalid YAML: {e}") from e
    _move_inline_action_params_to_input(wf_def_dict)
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


def _move_inline_action_params_to_input(wf: Any) -> None:
    """Turn {"action": "civis.run_job job_id=1"} into the equivalent
    {"action": "civis.run_job", "input": {"job_id": "1"}}, as Mistral does."""
    if not isinstance(wf, dict):
        return
    for key, workflow in wf.items():
        if key == "version" or not isinstance(workflow, dict):
            continue
        tasks = workflow.get("tasks")
        if not isinstance(tasks, dict):
            continue
        for task in tasks.values():
            if not isinstance(task, dict):
                continue
            action = task.get("action")
            if not isinstance(action, str) or " " not in action:
                continue
            if match := _INLINE_ACTION.match(action):
                task["action"] = match.group()
            params = dict(_INLINE_ACTION_PARAM.findall(action))
            task_input = task.get("input", {})
            if params and isinstance(task_input, dict):
                task["input"] = {**task_input, **params}


def _get_next_task_names(next_tasks: str | list | None) -> list[str]:
    """Next task names under {'on-success', 'on-error', 'on-complete'}."""
    if next_tasks is None:
        return []
    elif isinstance(next_tasks, str):
        return [next_tasks]
    else:
        return [
            task if isinstance(task, str) else next(iter(task)) for task in next_tasks
        ]


def _validate_workflow_tasks(wf: dict) -> None:
    """Custom checks for workflow tasks that aren't amenable to jsonschema validation"""
    workflow = next(v for k, v in wf.items() if k != "version")
    tasks = workflow["tasks"]
    task_defaults = workflow.get("task-defaults", {})
    if workflow.get("type") == "reverse":
        _validate_reverse_workflow_tasks(tasks, task_defaults)
    else:
        _validate_direct_workflow_tasks(tasks, task_defaults)
    _validate_no_tasks_function(workflow)


def _get_transitions(
    task_name: str, task: dict, task_defaults: dict
) -> Iterator[tuple[str, bool, list[str]]]:
    """Yield (transition key, whether from task-defaults, next task names).

    As Mistral does, a task without its own transitions under a key
    falls back on the task-defaults ones, excluding the task itself.
    """
    for key in _TASK_TRANSITION_KEYS:
        if next_task_names := _get_next_task_names(task.get(key)):
            yield key, False, next_task_names
        else:
            next_task_names = _get_next_task_names(task_defaults.get(key))
            yield key, True, [name for name in next_task_names if name != task_name]


def _get_next_task_names_for_cycle_check(
    task_name: str, task: dict, task_defaults: dict
) -> set[str]:
    """Next task names as Civis Platform's cycle check sees them.

    Unlike Mistral, it ignores engine commands, and it skips a task-defaults
    transition entirely for a task listed in it.
    """
    next_task_names = set()
    for key in _TASK_TRANSITION_KEYS:
        transitions = task.get(key)
        if transitions is None and task_name not in task_defaults.get(key, []):
            transitions = task_defaults.get(key)
        next_task_names.update(_get_next_task_names(transitions))
    return next_task_names - _TASK_TRANSITION_ENGINE_COMMANDS


def _validate_direct_workflow_tasks(tasks: dict, task_defaults: dict) -> None:
    recognized_task_names = set(tasks.keys()) | _TASK_TRANSITION_ENGINE_COMMANDS
    inbound_counts: Counter[str] = Counter()
    for task_name, task in tasks.items():
        if "requires" in task:
            raise WorkflowValidationError(
                f"Task {task_name!r} cannot use 'requires', "
                "which is only for workflows of type 'reverse'."
            )
        all_next_task_names = set()
        for key, from_defaults, next_task_names in _get_transitions(
            task_name, task, task_defaults
        ):
            source = f"{key!r} of 'task-defaults'" if from_defaults else repr(key)
            if task_name in next_task_names:
                raise WorkflowValidationError(
                    "A task cannot transition to itself. "
                    f"{task_name!r} transitions to itself in {source}."
                )
            for next_task_name in next_task_names:
                if next_task_name not in recognized_task_names:
                    raise WorkflowValidationError(
                        f"Task {task_name!r} transitions "
                        f"to an undefined task {next_task_name!r} "
                        f"in {source}."
                    )
            all_next_task_names.update(next_task_names)
        inbound_counts.update(all_next_task_names & tasks.keys())

    if all(inbound_counts[task_name] for task_name in tasks):
        raise WorkflowValidationError(
            "There must be at least one task without an inbound transition "
            "to start the workflow."
        )
    _validate_join_tasks(tasks, task_defaults, inbound_counts)
    _validate_no_cycles(
        {
            task_name: _get_next_task_names_for_cycle_check(
                task_name, task, task_defaults
            )
            & tasks.keys()
            for task_name, task in tasks.items()
        }
    )


def _validate_join_tasks(
    tasks: dict, task_defaults: dict, inbound_counts: dict[str, int]
) -> None:
    for task_name, task in tasks.items():
        if "join" not in task:
            continue
        pause_before = task.get("pause-before", task_defaults.get("pause-before"))
        if pause_before not in (None, False):
            # https://bugs.launchpad.net/mistral/+bug/1833273
            raise WorkflowValidationError(
                f"Task {task_name!r} cannot use 'pause-before' (including from "
                "'task-defaults') together with 'join', "
                "because Civis Platform would ignore the pause."
            )
        if not (join := task["join"]):
            continue
        if len(str(task_name)) > _MAX_LENGTH_JOIN_TASK_NAME:
            raise WorkflowValidationError(
                f"The name of a task with 'join' cannot exceed "
                f"{_MAX_LENGTH_JOIN_TASK_NAME} characters: {task_name!r}"
            )
        min_inbound = {"all": 1, "one": 1}.get(join, join)
        if inbound_counts[task_name] < min_inbound:
            raise WorkflowValidationError(
                f"Task {task_name!r} has 'join: {join}' but only "
                f"{inbound_counts[task_name]} inbound task(s)."
            )


def _validate_reverse_workflow_tasks(tasks: dict, task_defaults: dict) -> None:
    default_requires = task_defaults.get("requires", [])
    for required_task_name in default_requires:
        if required_task_name not in tasks:
            raise WorkflowValidationError(
                f"'task-defaults' requires an undefined task {required_task_name!r}."
            )
    graph: dict[str, set[str]] = {task_name: set() for task_name in tasks}
    for task_name, task in tasks.items():
        for key in (*_TASK_TRANSITION_KEYS, "join"):
            if key in task:
                raise WorkflowValidationError(
                    f"Task {task_name!r} cannot use {key!r} "
                    "in a workflow of type 'reverse'."
                )
        requires = task.get("requires", [])
        for required_task_name in requires:
            if required_task_name not in tasks:
                raise WorkflowValidationError(
                    f"Task {task_name!r} requires an undefined task "
                    f"{required_task_name!r}."
                )
        # Civis Platform ignores task-defaults "requires" for a task listed in it.
        if task_name not in default_requires:
            requires = requires or default_requires
        for required_task_name in requires:
            graph[required_task_name].add(task_name)
    _validate_no_cycles(graph)


def _validate_no_cycles(graph: dict[str, set[str]]) -> None:
    """Check a graph that maps each task name to its next task names."""
    try:
        graphlib.TopologicalSorter(graph).prepare()
    except graphlib.CycleError as e:
        # graphlib reads the graph as task name -> previous task names,
        # so it reports the cycle in reverse.
        cycle = reversed(e.args[1])
        raise WorkflowValidationError(
            f"Workflow cannot contain a cycle: {' -> '.join(cycle)}"
        )


def _validate_no_tasks_function(workflow: dict) -> None:
    """Civis Platform reserves the YAQL and Jinja function "tasks()"."""
    values: list[Any] = [workflow]
    # YAML aliases can make containers shared or even self-referencing.
    seen_container_ids = set()
    while values:
        value = values.pop()
        if isinstance(value, str):
            if _TASKS_FUNCTION.search(value):
                raise WorkflowValidationError(
                    "Civis Platform doesn't allow the reserved function 'tasks()' "
                    f"anywhere in a workflow definition: {value!r}"
                )
        elif isinstance(value, (dict, list)) and id(value) not in seen_container_ids:
            seen_container_ids.add(id(value))
            values.extend(value.values() if isinstance(value, dict) else value)
