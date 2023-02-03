from unittest import mock

from civis import APIClient
from civis.resources import API_SPEC
from civis.resources._resources import get_api_spec, generate_classes
from civis.tests.testcase import CivisVCRTestCase

api_import_str = 'civis.resources._resources.get_api_spec'


@mock.patch('civis.resources._resources.get_api_spec', return_value=API_SPEC)
def test_feature_flags(mock_spec):
    client = APIClient()

    class FakeUsersEndpoint:
        def list_me(self):
            return {'feature_flags': {'foo': True, 'bar': True, 'baz': False}}
    setattr(client, 'users', FakeUsersEndpoint())

    assert client.feature_flags == ('foo', 'bar')


class ClientTests(CivisVCRTestCase):

    @classmethod
    def setUpClass(cls):
        get_api_spec.cache_clear()
        generate_classes.cache_clear()

    @classmethod
    def tearDownClass(cls):
        get_api_spec.cache_clear()
        generate_classes.cache_clear()

    @mock.patch(api_import_str, return_value=API_SPEC)
    def test_feature_flags2(self, *mocks):
        client = APIClient()
        feature_flags = client.feature_flags
        expected = ('python_3_scripts', 'container_scripts', 'pubnub')
        self.assertCountEqual(feature_flags, expected)

    @mock.patch(api_import_str, return_value=API_SPEC)
    def test_feature_flags_memoized(self, *mocks):
        client = APIClient()
        with mock.patch.object(client.users, 'list_me',
                               wraps=client.users.list_me):
            client.feature_flags
            client.feature_flags
            self.assertEqual(client.users.list_me.call_count, 1)
