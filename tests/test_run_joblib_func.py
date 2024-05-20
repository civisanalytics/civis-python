import shutil


def test_civis_joblib_worker_command_available():
    assert shutil.which("civis_joblib_worker"), (
        "The `civis_joblib_worker` command is not available."
    )
