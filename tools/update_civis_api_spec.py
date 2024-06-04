"""This script downloads and updates the Civis API spec.

* `civis_api_spec.json` contains information about the publicly available
  API endpoints. This spec is used in both testing and generating
  the public Sphinx docs.
* `client.pyi` is a Python stub file that provides type hints for a
  `civis.APIClient` object. This file matches the API spec in `civis_api_spec.json`.
"""

from civis.resources import API_SPEC_PATH
from civis.resources._client_pyi import generate_client_pyi, CLIENT_PYI_PATH
from civis.resources._api_spec import download_latest_api_spec


if __name__ == "__main__":
    download_latest_api_spec(API_SPEC_PATH)
    print("downloaded civis_api_spec.json")

    # If we update civis_api_spec.json,
    # then client.pyi must also be updated to match it.
    generate_client_pyi(CLIENT_PYI_PATH, API_SPEC_PATH)
    print("updated client.pyi")
