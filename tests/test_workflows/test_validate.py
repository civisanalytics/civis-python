import os
import re
import tempfile
import textwrap
import zipfile

import requests
import pytest

from civis.workflows import validate_workflow_yaml, WorkflowValidationError

_VALID_WORKFLOW_YAML = """
version: "2.0"
workflow:
  tasks:
    task_1:
      action: civis.scripts.container
      input:
        required_resources:
          cpu: 1024
          memory: 1024
          disk_space: 10
        docker_image_name: civisanalytics/datascience-python
        docker_command: echo 'hello world'
"""


def test_valid_workflow_yaml():
    validate_workflow_yaml(_VALID_WORKFLOW_YAML)


@pytest.mark.parametrize(
    "replacee, replacer, error_message_contains",
    [
        ('version: "2.0"', "", "'version' is a required property"),
        ("tasks:", "foobar:", "'tasks' is a required property"),
        (
            "civis.scripts.container",
            "civis.script.container",
            "'civis.script.container' is not one of",
        ),
        ("      action:", "      foo: bar\n      action:", "'foo' was unexpected"),
        (
            "      input:",
            "      on-success:\n        - task_1\n      input:",
            "A task cannot transition to itself",
        ),
        (
            "      input:",
            "      on-success:\n        - undefined_task\n      input:",
            "undefined task",
        ),
        ("hello world", "hëlló wòrld", "cannot contain non-ASCII characters"),
    ],
)
def test_invalid_workflow_yaml(replacee, replacer, error_message_contains):
    """Break a valid workflow yaml, which should raise a WorkflowValidationError."""
    if replacee not in _VALID_WORKFLOW_YAML:
        raise ValueError(f"{replacee!r} not in the workflow yaml to be tested")
    invalid_wf_yaml = _VALID_WORKFLOW_YAML.replace(replacee, replacer)
    with pytest.raises(WorkflowValidationError, match=error_message_contains):
        validate_workflow_yaml(invalid_wf_yaml)


def _workflow_yaml(tasks: str, workflow_extras: str = "", version="'2.0'") -> str:
    tasks = textwrap.indent(textwrap.dedent(tasks).strip(), "    ")
    workflow_extras = textwrap.indent(textwrap.dedent(workflow_extras).strip(), "  ")
    return f"version: {version}\nworkflow:\n{workflow_extras}\n  tasks:\n{tasks}\n"


_NOOP = "action: std.noop"


@pytest.mark.parametrize(
    "wf_yaml",
    [
        pytest.param(
            textwrap.dedent("""
                version: '2.0'
                workflow:
                  input:
                  - pause_before_submit: false
                  tasks:
                    prep:
                      action: civis.scripts.python3
                      input:
                        name: prep
                        source: print("Preparing submission")
                      on-success:
                      - submit
                    submit:
                      action: civis.scripts.python3
                      pause-before: <% $.pause_before_submit in [true, "true"] %>
                      input:
                        name: submit
                        source: print("Submitting")
                """),
            id="CIVIS-14543",
        ),
        pytest.param(
            _workflow_yaml("a:\n  action: std.noop\n  pause-before: <% false %>"),
            id="pause-before",
        ),
        pytest.param(
            _workflow_yaml("a:\n  action: std.noop\n  keep-result: <% true %>"),
            id="keep-result",
        ),
        pytest.param(
            _workflow_yaml("a:\n  action: std.noop\n  safe-rerun: <% true %>"),
            id="safe-rerun",
        ),
        pytest.param(
            _workflow_yaml("""
                a:
                  action: std.noop
                  wait-before: <% 1 %>
                  wait-after: <% 1 %>
                  timeout: <% 60 %>
                  concurrency: <% 2 %>
                """),
            id="wait-before, wait-after, timeout, and concurrency",
        ),
        pytest.param(
            _workflow_yaml("""
                a:
                  action: std.noop
                  retry:
                    count: <% 2 %>
                    delay: <% 10 %>
                """),
            id="retry count and delay",
        ),
        pytest.param(
            _workflow_yaml("a:\n  action: std.noop\n  input: <% $.x %>", "input:\n- x"),
            id="input",
        ),
        pytest.param(
            _workflow_yaml(
                "a:\n  action: civis.run_job\n  input: <% $.x %>", "input:\n- x"
            ),
            id="input for a Civis action",
        ),
        pytest.param(
            _workflow_yaml(
                "a:\n  action: std.noop", "task-defaults:\n  pause-before: <% false %>"
            ),
            id="task-defaults",
        ),
        pytest.param(
            _workflow_yaml("""
                a:
                  action: std.noop
                  pause-before: |
                    <% $.x and
                       $.y %>
                """),
            id="multi-line expression",
        ),
    ],
)
def test_valid_workflow_yaml_yaql_expressions(wf_yaml):
    validate_workflow_yaml(wf_yaml)


@pytest.mark.parametrize(
    "task_extras, error_message_contains",
    [
        ("pause-before: 'yes'", "'yes' does not match"),
        ("pause-before: '{{ false }}'", "'{{ false }}' does not match"),
        ("pause-before: 5", "5 is not of type 'boolean', 'string'"),
        ("timeout: soon", "'soon' does not match"),
        ("timeout: -1", "-1 is less than the minimum of 0"),
    ],
)
def test_invalid_workflow_yaml_task_policies(task_extras, error_message_contains):
    wf_yaml = _workflow_yaml(f"a:\n  action: std.noop\n  {task_extras}")
    with pytest.raises(
        WorkflowValidationError, match=re.escape(error_message_contains)
    ):
        validate_workflow_yaml(wf_yaml)


def test_invalid_workflow_yaml_civis_action_input_not_an_expression():
    wf_yaml = _workflow_yaml("a:\n  action: civis.run_job\n  input: foo")
    with pytest.raises(WorkflowValidationError, match="'foo' does not match"):
        validate_workflow_yaml(wf_yaml)


@pytest.mark.parametrize(
    "wf_yaml",
    [
        pytest.param(
            _workflow_yaml(f"a:\n  {_NOOP}\n  fail-on: true"), id="fail-on boolean"
        ),
        pytest.param(
            _workflow_yaml(f"a:\n  {_NOOP}\n  fail-on: <% true %>"),
            id="fail-on expression",
        ),
        pytest.param(
            _workflow_yaml(f"""
                a:
                  {_NOOP}
                  concurrency: 0
                  retry:
                    count: <% 2 %>
                    delay: 1
                    break-on: <% false %>
                """),
            id="policies",
        ),
        pytest.param(
            _workflow_yaml(f"a:\n  {_NOOP}\n  retry: count=3 delay=1"),
            id="retry as a string",
        ),
        pytest.param(
            _workflow_yaml(f"""
                a:
                  {_NOOP}
                  on-success: [c]
                b:
                  {_NOOP}
                  on-success: [c]
                c:
                  {_NOOP}
                  join: one
                """),
            id="join one",
        ),
        pytest.param(
            _workflow_yaml(
                f"a:\n  {_NOOP}\n  on-success: [b]\nb:\n  {_NOOP}\n  join: 0"
            ),
            id="join 0",
        ),
        pytest.param(
            _workflow_yaml(f"""
                a:
                  {_NOOP}
                  on-success: [b]
                b:
                  {_NOOP}
                  join: all
                  pause-before: false
                """),
            id="join with pause-before false",
        ),
        pytest.param(
            _workflow_yaml(f"""
                a:
                  {_NOOP}
                  on-success:
                    - b
                    - c: <% true %>
                b:
                  {_NOOP}
                c:
                  {_NOOP}
                """),
            id="transitions with and without a guard",
        ),
        pytest.param(
            _workflow_yaml(f"a:\n  {_NOOP}\n  on-error: fail"),
            id="transition to an engine command",
        ),
        pytest.param(
            _workflow_yaml(f"a:\n  {_NOOP}", version="2.0"), id="version float"
        ),
        pytest.param(_workflow_yaml(f"a:\n  {_NOOP}", version="2"), id="version int"),
        pytest.param(
            _workflow_yaml("a:\n  action: civis.run_job job_id=123"),
            id="inline action params",
        ),
        pytest.param(
            _workflow_yaml(
                "a:\n  action: civis.scripts.python3 source=\"print('hi')\" name='a b'"
            ),
            id="inline action params with quotes",
        ),
        pytest.param(
            "version: '2.0'\nworkflow:\n\ttasks:\n\t\ta:\n\t\t\taction: std.noop\n",
            id="tabs",
        ),
        pytest.param(
            _workflow_yaml(f"a:\n  {_NOOP}\n  name: noop\n  type: whatever"),
            id="task name and type",
        ),
        pytest.param(
            _workflow_yaml(
                f"a:\n  {_NOOP}\n  with-items:\n"
                "    - x in [1, 2]\n    - y in <% $.ys %>",
                "input:\n  - ys: [1]",
            ),
            id="with-items",
        ),
        pytest.param(
            _workflow_yaml(
                f"""
                a:
                  {_NOOP}
                  on-success: [b]
                b:
                  {_NOOP}
                c:
                  {_NOOP}
                """,
                "task-defaults:\n  on-error: [c]",
            ),
            id="task-defaults transition excludes the task itself",
        ),
        pytest.param(
            _workflow_yaml(
                f"a:\n  {_NOOP}\nx:\n  {_NOOP}\ny:\n  {_NOOP}",
                "task-defaults:\n  on-complete: [x, y]",
            ),
            id="task-defaults transition is skipped for a task listed in it",
        ),
        pytest.param(
            _workflow_yaml(f"""
                s:
                  {_NOOP}
                  on-success: [a]
                a:
                  {_NOOP}
                  on-error: fail
                fail:
                  {_NOOP}
                  on-success: [a]
                """),
            id="cycle check ignores a task named like an engine command",
        ),
        pytest.param(
            _workflow_yaml(f"a:\n  {_NOOP}", "anchor: &recursive [*recursive]"),
            id="recursive anchor",
        ),
        pytest.param(
            _workflow_yaml(
                f"a:\n  {_NOOP}\nb:\n  {_NOOP}\n  requires: [a]", "type: reverse"
            ),
            id="reverse workflow",
        ),
        pytest.param(_workflow_yaml(f"fail:\n  {_NOOP}"), id="task named fail"),
        pytest.param(_workflow_yaml(f"{'a' * 255}:\n  {_NOOP}"), id="long task name"),
        pytest.param(
            "version: '2.0'\nworkflow:\n  anchor: &a\n    action: std.noop\n"
            "  tasks:\n    a: *a\n",
            id="anchor",
        ),
    ],
)
def test_valid_workflow_yaml_variants(wf_yaml):
    validate_workflow_yaml(wf_yaml)


@pytest.mark.parametrize(
    "wf_yaml, error_message_contains",
    [
        ("", "None is not of type 'object'"),
        ("version: '2.0'\nworkflow: [", "Invalid YAML"),
        (
            _workflow_yaml(f"a:\n  {_NOOP}\n  fail-on: 'yes'"),
            "'yes' does not match",
        ),
        (
            _workflow_yaml(f"a:\n  {_NOOP}\n  timeout: 1.5"),
            "1.5 is not of type 'integer', 'string'",
        ),
        (
            _workflow_yaml(f"a:\n  {_NOOP}\n  retry:\n    count: 2"),
            "'delay' is a required property",
        ),
        (
            _workflow_yaml(
                f"a:\n  {_NOOP}\n  on-success:\n    next: [b]\nb:\n  {_NOOP}"
            ),
            "is not valid under any of the given schemas",
        ),
        (
            _workflow_yaml(
                f"a:\n  {_NOOP}\n  on-success:\n    - b: true\nb:\n  {_NOOP}"
            ),
            "True is not of type 'string'",
        ),
        (
            _workflow_yaml(f"a:\n  {_NOOP}\n  on-success: [b, b]\nb:\n  {_NOOP}"),
            "has non-unique elements",
        ),
        (
            _workflow_yaml(f"a:\n  {_NOOP}\n  on-success: [noop]"),
            "undefined task 'noop'",
        ),
        (
            _workflow_yaml(f'a:\n  {_NOOP}\n  on-success:\n    - fail msg="x"'),
            "does not match",
        ),
        (
            _workflow_yaml("a:\n  action: std.echo\n  input:\n    output: hi"),
            "'std.echo' is not one of",
        ),
        (
            _workflow_yaml("a:\n  action: civis.scripts.python3"),
            "'input' is a required property",
        ),
        (
            _workflow_yaml("a:\n  action: civis.run_job foo=1"),
            "'job_id' is a required property",
        ),
        (_workflow_yaml(f"a:\n  {_NOOP}\n  input: {{}}"), "should be non-empty"),
        (_workflow_yaml(f"a:\n  {_NOOP}\n  publish: {{}}"), "should be non-empty"),
        (_workflow_yaml(f"a:\n  {_NOOP}\n  with-items: foo"), "does not match"),
        (
            _workflow_yaml(f"a:\n  {_NOOP}", "input:\n  - x\n  - x"),
            "has non-unique elements",
        ),
        (
            _workflow_yaml(f"a:\n  {_NOOP}", "task-defaults:\n  input:\n    x: 1"),
            "'input' was unexpected",
        ),
        (
            _workflow_yaml(f"a:\n  {_NOOP}", "type: foo"),
            "'foo' is not one of ['direct', 'reverse']",
        ),
        (
            _workflow_yaml("a:\n  action: std.bogus").replace(
                "workflow:", "conversion_workflow:"
            ),
            "'std.bogus' is not one of",
        ),
        (_workflow_yaml(f"{'a' * 256}:\n  {_NOOP}"), "is too long"),
        (
            _workflow_yaml(
                f"a:\n  {_NOOP}\n  on-success: [{'j' * 209}]\n{'j' * 209}:\n"
                f"  {_NOOP}\n  join: all"
            ),
            "cannot exceed 208 characters",
        ),
        (
            _workflow_yaml(f"a:\n  {_NOOP}\n  join: all"),
            "Task 'a' has 'join: all' but only 0 inbound task(s).",
        ),
        (
            _workflow_yaml(f"""
                a:
                  {_NOOP}
                  on-success: [c]
                b:
                  {_NOOP}
                  on-success: [c]
                c:
                  {_NOOP}
                  join: 3
                """),
            "Task 'c' has 'join: 3' but only 2 inbound task(s).",
        ),
        (
            _workflow_yaml(f"""
                a:
                  {_NOOP}
                  on-success: [b]
                b:
                  {_NOOP}
                  join: all
                  pause-before: true
                """),
            "Task 'b' cannot use 'pause-before'",
        ),
        (
            _workflow_yaml(
                f"a:\n  {_NOOP}\n  on-success: [b]\nb:\n  {_NOOP}\n  join: all",
                "task-defaults:\n  pause-before: <% $.pause %>",
            ),
            "Task 'b' cannot use 'pause-before'",
        ),
        (
            _workflow_yaml(f"a:\n  {_NOOP}", "task-defaults:\n  on-error: [undefined]"),
            "undefined task 'undefined' in 'on-error' of 'task-defaults'",
        ),
        (
            _workflow_yaml(
                f"a:\n  {_NOOP}\n  on-success: [b]\nb:\n  {_NOOP}\n  on-success: [a]"
            ),
            "There must be at least one task without an inbound transition",
        ),
        (
            _workflow_yaml(f"""
                a:
                  {_NOOP}
                  on-success: [b]
                b:
                  {_NOOP}
                  on-success: [c]
                c:
                  {_NOOP}
                  on-success: [d]
                d:
                  {_NOOP}
                  on-success: [b]
                """),
            "Workflow cannot contain a cycle: b -> c -> d -> b",
        ),
        (
            _workflow_yaml(
                f"a:\n  {_NOOP}\nn:\n  {_NOOP}",
                "task-defaults:\n  on-error:\n    - n: <% true %>",
            ),
            "Workflow cannot contain a cycle: n -> n",
        ),
        (
            _workflow_yaml(f"a:\n  {_NOOP}\n  publish:\n    x: <% tasks() %>"),
            "reserved function 'tasks()'",
        ),
        (
            _workflow_yaml(f"a:\n  {_NOOP}\nb:\n  {_NOOP}\n  requires: [a]"),
            "Task 'b' cannot use 'requires'",
        ),
        (
            _workflow_yaml(f"a:\n  {_NOOP}\n  requires: a", "type: reverse"),
            "'a' is not of type 'array'",
        ),
        (
            _workflow_yaml(f"a:\n  {_NOOP}\n  requires: [b]", "type: reverse"),
            "Task 'a' requires an undefined task 'b'.",
        ),
        (
            _workflow_yaml(
                f"a:\n  {_NOOP}", "type: reverse\ntask-defaults:\n  requires: [b]"
            ),
            "'task-defaults' requires an undefined task 'b'.",
        ),
        (
            _workflow_yaml(
                f"a:\n  {_NOOP}\n  on-success: [b]\nb:\n  {_NOOP}", "type: reverse"
            ),
            "Task 'a' cannot use 'on-success' in a workflow of type 'reverse'.",
        ),
        (
            _workflow_yaml(
                f"a:\n  {_NOOP}\n  requires: [b]\nb:\n  {_NOOP}\n  requires: [a]",
                "type: reverse",
            ),
            "Workflow cannot contain a cycle",
        ),
    ],
)
def test_invalid_workflow_yaml_variants(wf_yaml, error_message_contains):
    with pytest.raises(
        WorkflowValidationError, match=re.escape(error_message_contains)
    ):
        validate_workflow_yaml(wf_yaml)


def test_workflows_public_repo():
    """All example workflow YAML files from the workflows-public repo should pass."""
    wf_repo_zip_url = (
        "https://github.com/civisanalytics/workflows-public/archive/refs/heads/main.zip"
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        wf_repo_zip_path = os.path.join(temp_dir, "workflows-public.zip")
        wf_repo_unzip_dir = os.path.join(temp_dir, "workflows-public")

        # Download the workflows-public repo as a zip file.
        with requests.get(wf_repo_zip_url, stream=True) as r:
            with open(wf_repo_zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024):
                    f.write(chunk)
        assert os.path.isfile(wf_repo_zip_path), "no file found"

        # Unzip repo content.
        with zipfile.ZipFile(wf_repo_zip_path) as zfile:
            zfile.extractall(wf_repo_unzip_dir)

        wf_repo_dir = os.path.join(wf_repo_unzip_dir, "workflows-public-main")
        yaml_filenames = [f for f in os.listdir(wf_repo_dir) if f.endswith(".yml")]
        assert yaml_filenames, f"no yaml files: {os.listdir(wf_repo_dir)}"

        # Validate each workflow yaml in the repo.
        for filename in yaml_filenames:
            wf_yaml_path = os.path.join(wf_repo_dir, filename)

            try:
                with open(wf_yaml_path) as f:
                    validate_workflow_yaml(f.read())
            except WorkflowValidationError as e:
                print("Failed workflow yaml:", filename)
                raise e
