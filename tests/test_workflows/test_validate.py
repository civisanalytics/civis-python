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
    "replace_to_break",
    [
        ('version: "2.0"', ""),  # "version" is required
        ("tasks:", "foobar:"),  # "tasks" is required
        ("civis.scripts.container", "civis.script.container"),  # invalid "action"
        ("      action:", "      foo: bar\n      action:"),  # invalid "task" property
    ],
)
def test_invalid_workflow_yaml(replace_to_break):
    invalid_wf_yaml = _VALID_WORKFLOW_YAML.replace(*replace_to_break)
    with pytest.raises(WorkflowValidationError):
        validate_workflow_yaml(invalid_wf_yaml)
