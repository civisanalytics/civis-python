import os
import tempfile
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
    "replace_to_break",
    [
        # "version" is required
        ('version: "2.0"', ""),
        # "tasks" is required
        ("tasks:", "foobar:"),
        # invalid "action"
        ("civis.scripts.container", "civis.script.container"),
        # invalid "task" property
        ("      action:", "      foo: bar\n      action:"),
        # task transitioning to itself
        (
            "      input:",
            "      on-success:\n        - task_1\n      input:",
        ),
        # task transitioning to an undefined task
        (
            "      input:",
            "      on-success:\n        - undefined_task\n      input:",
        ),
        # non-ASCII characters are not allowed
        ("hello world", "hëlló wòrld"),
    ],
)
def test_invalid_workflow_yaml(replace_to_break):
    """Break a valid workflow yaml, which should raise a WorkflowValidationError."""
    replacee, replacer = replace_to_break
    if replacee not in _VALID_WORKFLOW_YAML:
        raise ValueError(f"{replacee!r} not in the workflow yaml to be tested")
    invalid_wf_yaml = _VALID_WORKFLOW_YAML.replace(replacee, replacer)
    with pytest.raises(WorkflowValidationError):
        validate_workflow_yaml(invalid_wf_yaml)


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
