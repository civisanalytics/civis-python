from unittest import mock

import civis
from civis import find
from civis.resources import API_SPEC
from civis.response import Response

import pytest


def test_find_filter_with_kwargs():
    r1 = Response({"foo": 0, "bar": "a", "baz": True})
    r2 = Response({"foo": 1, "bar": "b", "baz": True})
    r3 = Response({"foo": 2, "bar": "b", "baz": False})

    assert find([r1, r2, r3], wrong_attr="whatever") == []

    assert find([r1, r2, r3], foo=0) == [r1]
    assert find([r1, r2, r3], foo=1) == [r2]
    assert find([r1, r2, r3], foo=1, bar="b") == [r2]

    assert find([r1, r2, r3], bar="b") == [r2, r3]
    assert find([r1, r2, r3], bar="b", foo=1) == [r2]

    assert find([r1, r2, r3], foo=True) == []
    assert find([r1, r2, r3], foo=False) == []
    assert find([r1, r2, r3], bar=True) == []
    assert find([r1, r2, r3], bar=False) == []
    assert find([r1, r2, r3], baz=True) == [r1, r2]
    assert find([r1, r2, r3], baz=False) == [r3]

    assert find([r1, r2, r3], foo=int) == [r2, r3]


@pytest.mark.parametrize('schema_tablename', [
    'foo.bar', '"foo".bar', 'foo."bar"', '"foo"."bar"'
])
def test_get_table_id(schema_tablename):
    """Check that get_table_id handles quoted schema.tablename correctly."""
    client = civis.APIClient(local_api_spec=API_SPEC, api_key='none')
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


def test_get_storage_host_id():
    client = civis.APIClient(local_api_spec=API_SPEC, api_key='none')

    class StorageHost:
        def __init__(self, id, name):
            self.id = id
            self.name = name

        def __getitem__(self, key):
            return getattr(self, key)

    storage_hosts = [StorageHost(1234, 'test'), StorageHost(5678, 'othertest')]
    client.storage_hosts.list = mock.Mock(return_value=storage_hosts)

    assert client.get_storage_host_id('test') == 1234

    client.storage_hosts.list.assert_called_once_with()

    assert client.get_storage_host_id(4732) == 4732
    with pytest.raises(ValueError, match="Storage Host invalidname not found"):
        client.get_storage_host_id('invalidname')
