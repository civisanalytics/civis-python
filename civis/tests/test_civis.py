from unittest import mock

import civis

import pytest


@pytest.mark.parametrize('schema_tablename', [
    'foo.bar', '"foo".bar', 'foo."bar"', '"foo"."bar"'
])
def test_get_table_id(schema_tablename):
    """Check that get_table_id handles quoted schema.tablename correctly."""
    mock_client = mock.Mock()
    mock_client.get_database_id.return_value = 123

    mock_tables = mock.MagicMock()
    mock_tables.__getitem__.side_effect = {0: mock.Mock()}.__getitem__

    mock_client.tables.list.return_value = [mock_tables]

    civis.APIClient.get_table_id(mock_client,
                                 table=schema_tablename, database=123)

    mock_client.tables.list.assert_called_once_with(
        database_id=123,
        schema='foo',
        name='bar'
    )
