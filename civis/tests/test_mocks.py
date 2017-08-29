"""Tests for the test tooling
"""
import pytest

from civis.tests import mocks


def test_client_mock_attributeerror():
    mock_client = mocks.create_client_mock()
    with pytest.raises(AttributeError):
        mock_client.not_an_endpoint()


def test_client_mock_bad_parameter():
    mock_client = mocks.create_client_mock()
    mock_client.tables.list(database_id=1)  # Valid parameter
    with pytest.raises(TypeError):
        mock_client.tables.list(db_id=1)  # Invalid parameter
