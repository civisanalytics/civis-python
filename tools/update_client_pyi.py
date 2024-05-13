"""This script updates client.pyi based on the API spec in the codebase."""

from civis.resources import API_SPEC_PATH
from civis.resources._client_pypi import generate_client_pyi, CLIENT_PYI_PATH


if __name__ == "__main__":
    generate_client_pyi(CLIENT_PYI_PATH, API_SPEC_PATH)
    print("updated client.pyi")
