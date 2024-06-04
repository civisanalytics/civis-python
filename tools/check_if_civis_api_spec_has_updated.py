"""This script checks if the Civis API spec has been updated.

This script is set up to run in a scheduled Civis Platform job (see internal docs).
If updates are detected, the job fails and notifies civis-python maintainers.
"""

import os
import pprint
import sys
import tempfile

from civis.resources import API_SPEC_PATH
from civis.resources._api_spec import download_latest_api_spec, compare_api_specs


def main():
    if len(sys.argv) > 1:
        api_spec_path_current = sys.argv[1]  # for testing this script
    else:
        api_spec_path_current = API_SPEC_PATH
    print("Current API spec path:", api_spec_path_current)
    with tempfile.TemporaryDirectory() as tempdir:
        latest_api_spec_path = os.path.join(tempdir, "civis_api_spec.json")
        download_latest_api_spec(latest_api_spec_path)
        added, removed, changed = compare_api_specs(
            api_spec_path_current, latest_api_spec_path
        )
    if any(any(diffs.values()) for diffs in (added, removed, changed)):
        raise RuntimeError(
            "The Civis API spec has been updated. "
            "Please run tools/update_civis_api_spec.py.\n----------------\n"
            f"Added:\n{pprint.pformat(added)}\n----------------\n"
            f"Removed:\n{pprint.pformat(removed)}\n----------------\n"
            f"Changed:\n{pprint.pformat(changed)}"
        )
    else:
        print("The Civis API spec hasn't been updated.")


if __name__ == "__main__":
    main()
