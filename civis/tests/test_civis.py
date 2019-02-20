import civis
from civis.compat import mock
from civis.tests.mocks import TEST_SPEC

import pytest


@pytest.mark.parametrize('schema_tablename', [
    'foo.bar', '"foo".bar', 'foo."bar"', '"foo"."bar"'
])
def test_get_table_id(schema_tablename):
    """Check that get_table_id handles quoted schema.tablename correctly."""
    client = civis.APIClient(local_api_spec=TEST_SPEC, api_key='none')
    client.get_database_id = mock.Mock(return_value=123)

    mock_tables = mock.MagicMock()
    mock_tables.__getitem__.side_effect = {0: mock.Mock()}.__getitem__

    client.tables.list = mock.Mock(return_value=mock_tables)

    client.get_table_id(table=schema_tablename, database=123)

    client.tables.list.assert_called_once_with(
        database_id=123,
        schema='foo',
        name='bar'
    )
