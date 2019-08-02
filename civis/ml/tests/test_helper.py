import pytest

from civis.response import Response
from civis.ml import list_models
from civis.tests.mocks import create_client_mock


def test_list_models_bad_job_type():
    with pytest.raises(ValueError):
        list_models(job_type="fake")


def test_list_models():
    resp = [Response({'id': 2834, 'name': 'RFC model'})]
    m_client = create_client_mock()
    m_client.scripts.list_custom.return_value = resp
    out = list_models(job_type='train', client=m_client)
    assert out == resp

    out = list_models(job_type='predict', client=m_client)
    assert out == resp

    out = list_models(job_type=None, client=m_client)
    assert out == resp
