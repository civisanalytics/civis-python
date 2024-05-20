import shutil


def test_civis_joblib_worker_command_available():
    command = "civis_joblib_worker"
    assert shutil.which(command), f"The `{command}` command is not available."
