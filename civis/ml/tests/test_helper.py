from unittest import mock

import pytest

from civis.response import Response
from civis.ml import (list_models, put_models_shares_groups,
                      put_models_shares_users, delete_models_shares_groups,
                      delete_models_shares_users)
from civis.ml import _helper as helper
from civis.ml.tests import test_model
from civis.tests.mocks import create_client_mock


def test_list_models_bad_job_type():
    with pytest.raises(ValueError):
        list_models(job_type="fake")


def test_list_models():
    resp = [Response({'id': 2834, 'name': 'RFC model'})]
    m_client = create_client_mock()
    m_client.aliases.list.return_value = test_model.TEST_TEMPLATE_ID_ALIAS_OBJECTS  # noqa
    m_client.scripts.list_custom.return_value = resp
    out = list_models(job_type='train', client=m_client)
    assert out == resp

    out = list_models(job_type='predict', client=m_client)
    assert out == resp

    out = list_models(job_type=None, client=m_client)
    assert out == resp


def _create_share_model_client_mock(run_ids):
    m_client = create_client_mock()
    m_client.scripts.put_containers_shares_users.return_value = 'usershare'
    m_client.scripts.put_containers_shares_groups.return_value = 'groupshare'
    m_client.scripts.delete_containers_shares_users.return_value = 'userdel'
    m_client.scripts.delete_containers_shares_groups.return_value = 'groupdel'
    m_client.scripts.list_containers_runs.return_value = [
        Response({'id': _id}) for _id in run_ids]
    m_client.scripts.list_containers_runs_outputs.return_value = [
        Response({'object_id': 117, 'object_type': 'File', 'name': 'fname'}),
        Response({'object_id': 31, 'object_type': 'Project'}),
        Response({'object_id': 37, 'object_type': 'JSONValue'}),
    ]
    return m_client


def test_share_model_users():
    m_client = _create_share_model_client_mock([11])

    resp = helper._share_model(3, [7, 8], 'write', 'users', client=m_client,
                               send_shared_email=True)
    assert resp == 'usershare'
    m_client.scripts.put_containers_shares_users.assert_called_once_with(
        3, [7, 8], 'write', send_shared_email=True)
    m_client.files.put_shares_users.assert_called_once_with(
        117, [7, 8], 'write', send_shared_email=False)
    m_client.projects.put_shares_users.assert_called_once_with(
        31, [7, 8], 'write', send_shared_email=False)
    m_client.json_values.put_shares_users.assert_called_once_with(
        37, [7, 8], 'write', send_shared_email=False)


def test_share_model_groups():
    m_client = _create_share_model_client_mock([11])

    resp = helper._share_model(3, [7, 8], 'write', 'groups', client=m_client,
                               send_shared_email=True)
    assert resp == 'groupshare'
    m_client.scripts.put_containers_shares_groups.assert_called_once_with(
        3, [7, 8], 'write', send_shared_email=True)
    m_client.files.put_shares_groups.assert_called_once_with(
        117, [7, 8], 'write', send_shared_email=False)
    m_client.projects.put_shares_groups.assert_called_once_with(
        31, [7, 8], 'write', send_shared_email=False)
    m_client.json_values.put_shares_groups.assert_called_once_with(
        37, [7, 8], 'write', send_shared_email=False)


def test_share_model_tworuns():
    # Check that we grant permission on run outputs for each run
    m_client = _create_share_model_client_mock([11, 13])

    helper._share_model(3, [7, 8], 'write', 'users', client=m_client)

    m_client.scripts.put_containers_shares_users.assert_called_once_with(
        3, [7, 8], 'write')

    assert m_client.files.put_shares_users.call_count == 2
    assert m_client.projects.put_shares_users.call_count == 2
    assert m_client.json_values.put_shares_users.call_count == 2


def test_share_model_project_permissions():
    # Grant "write" permission on the internal project when
    # overall "read" permission is requested.
    m_client = _create_share_model_client_mock([11])

    helper._share_model(3, [7, 8], 'read', 'groups', client=m_client)

    m_client.projects.put_shares_groups.assert_called_once_with(
        31, [7, 8], 'write', send_shared_email=False)


def test_unshare_model_users():
    m_cl = _create_share_model_client_mock([11])

    resp = helper._unshare_model(3, 7, 'users', client=m_cl)
    assert resp == 'userdel'
    m_cl.scripts.delete_containers_shares_users.assert_called_once_with(3, 7)
    m_cl.files.delete_shares_users.assert_called_once_with(117, 7)
    m_cl.projects.delete_shares_users.assert_called_once_with(31, 7)
    m_cl.json_values.delete_shares_users.assert_called_once_with(37, 7)


def test_unshare_model_groups():
    m_cl = _create_share_model_client_mock([11])

    resp = helper._unshare_model(3, 7, 'groups', client=m_cl)
    assert resp == 'groupdel'
    m_cl.scripts.delete_containers_shares_groups.assert_called_once_with(3, 7)
    m_cl.files.delete_shares_groups.assert_called_once_with(117, 7)
    m_cl.projects.delete_shares_groups.assert_called_once_with(31, 7)
    m_cl.json_values.delete_shares_groups.assert_called_once_with(37, 7)


def test_unshare_model_tworuns():
    # Check that we grant permission on run outputs for each run
    m_cl = _create_share_model_client_mock([11, 13])

    helper._unshare_model(3, 7, 'users', client=m_cl)

    m_cl.scripts.delete_containers_shares_users.assert_called_once_with(3, 7)

    assert m_cl.files.delete_shares_users.call_count == 2
    assert m_cl.projects.delete_shares_users.call_count == 2
    assert m_cl.json_values.delete_shares_users.call_count == 2


@mock.patch('civis.ml._helper._share_model', autospec=True)
def test_put_models_shares_groups(mock_share):
    mock_share.return_value = 'retval'
    out = put_models_shares_groups(1, [7, 8], 'read')

    assert out == 'retval'
    mock_share.assert_called_once_with(1, [7, 8], 'read', entity_type='groups',
                                       client=None)


@mock.patch('civis.ml._helper._share_model', autospec=True)
def test_put_models_shares_users(mock_share):
    mock_share.return_value = 'retval'
    out = put_models_shares_users(1, [7, 8], 'read')

    assert out == 'retval'
    mock_share.assert_called_once_with(1, [7, 8], 'read', entity_type='users',
                                       client=None)


@mock.patch('civis.ml._helper._unshare_model', autospec=True)
def test_delete_models_shares_groups(m_unshare):
    m_unshare.return_value = 'retval'
    out = delete_models_shares_groups(1, 7)

    assert out == 'retval'
    m_unshare.assert_called_once_with(1, 7, entity_type='groups', client=None)


@mock.patch('civis.ml._helper._unshare_model', autospec=True)
def test_delete_models_shares_users(m_unshare):
    m_unshare.return_value = 'retval'
    out = delete_models_shares_users(1, 7)

    assert out == 'retval'
    m_unshare.assert_called_once_with(1, 7, entity_type='users', client=None)
