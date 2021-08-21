from collections import OrderedDict
import json
from unittest import mock

from civis import APIClient
from civis.tests import TEST_SPEC

api_import_str = 'civis.resources._resources.get_api_spec'
with open(TEST_SPEC) as f:
    civis_api_spec = json.load(f, object_pairs_hook=OrderedDict)


@mock.patch("requests.Session")
def test_feature_flags(m_session):
    client = APIClient(
        local_api_spec=TEST_SPEC, api_key="no_internet", return_type="raw"
    )
    assert m_session.call_count == 0

    assert client.feature_flags == ()
    assert m_session.call_count == 1

    client.feature_flags  # Feature flags cached, no more API calls
    assert m_session.call_count == 1
