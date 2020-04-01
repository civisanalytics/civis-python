import os
import shutil
import tempfile

import pytest


def test_filenotfounderror():
    # Make certain we're treating the output of a failed call to
    # os.path.getmtime correctly in different Python versions.
    # This should output a FileNotFoundError.
    tempdir = tempfile.mkdtemp()
    try:
        missing_file = os.path.join(tempdir, 'does_not_exist.txt')
        with pytest.raises(FileNotFoundError):
            os.path.getmtime(missing_file)
    finally:
        shutil.rmtree(tempdir)
