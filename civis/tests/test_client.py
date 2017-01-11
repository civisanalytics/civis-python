from collections import OrderedDict
import os
import json
from unittest.mock import patch

from civis import APIClient
from civis.tests.testcase import CivisVCRTestCase

swagger_import_str = 'civis.resources._resources.get_swagger_spec'
THIS_DIR = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(THIS_DIR, "civis_api_spec.json")) as f:
    civis_api_spec = json.load(f, object_pairs_hook=OrderedDict)


class ClientTests(CivisVCRTestCase):

    @patch(swagger_import_str, return_value=civis_api_spec)
    def test_feature_flags(self, *mocks):
        client = APIClient()
        feature_flags = client.feature_flags
        expected = ('python_3_scripts', 'container_scripts', 'pubnub')
        self.assertCountEqual(feature_flags, expected)

    @patch(swagger_import_str, return_value=civis_api_spec)
    def test_feature_flags_memoized(self, *mocks):
        client = APIClient()
        with patch.object(client.users, 'list_me', wraps=client.users.list_me):
            client.feature_flags
            client.feature_flags
            self.assertEqual(client.users.list_me.call_count, 1)
