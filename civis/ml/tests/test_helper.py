import mock
import pytest

from civis.response import Response
from civis.ml import list_models

TRAIN_TEMPLATES = '11219, 11221, 10582, 9968, 9112, 8387, 7020'
PRED_TEMPLATES = '11220, 10583, 9969, 9113, 8388, 7021'


def test_list_models_bad_job_type():
    with pytest.raises(ValueError):
        list_models(job_type="fake")


def test_list_models():
    resp = [Response({'id': 2834, 'name': 'RFC model'})]
    m_client = mock.Mock()
    m_client.scripts.list_custom.return_value = resp
    m_client.users.list_me.return_value = Response({'id': 2834})
    out = list_models(job_type='train', client=m_client)
    assert out == resp

    out = list_models(job_type='predict', client=m_client)
    assert out == resp

    out = list_models(job_type=None, client=m_client)
    assert out == resp
