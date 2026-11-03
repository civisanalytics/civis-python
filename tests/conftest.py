import pytest


@pytest.fixture(autouse=True)
def _no_civis_api_key_from_environment(monkeypatch):
    monkeypatch.delenv("CIVIS_API_KEY", raising=False)
