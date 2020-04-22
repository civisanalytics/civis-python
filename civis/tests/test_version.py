import os
import re

import civis


_THIS_DIR = os.path.dirname(os.path.realpath(__file__))
_REPO_DIR = os.path.dirname(os.path.dirname(_THIS_DIR))


def test_version_number_match_with_changelog():
    """__version__ and CHANGELOG.md match for the latest version number."""
    changelog = open(os.path.join(_REPO_DIR, 'CHANGELOG.md')).read()
    version_in_changelog = (
        re.search(r'##\s+(\d+\.\d+\.\d+)', changelog).groups()[0])
    assert civis.__version__ == version_in_changelog, (
        'Make sure both __version__ and CHANGELOG are updated to match the '
        'latest version number')
