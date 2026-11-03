import pytest


@pytest.fixture(autouse=True)
def _no_civis_api_key_from_environment(monkeypatch):
    """Keep tests from depending on, or using, a developer's real API key."""
    monkeypatch.delenv("CIVIS_API_KEY", raising=False)
