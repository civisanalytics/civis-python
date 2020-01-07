from collections import namedtuple
import logging

from civis import APIClient
from civis.ml._model import _get_template_ids_all_versions

__all__ = ['list_models', 'put_models_shares_groups',
           'put_models_shares_users', 'delete_models_shares_groups',
           'delete_models_shares_users']
log = logging.getLogger(__name__)

# sentinel value for default author value
SENTINEL = namedtuple('Sentinel', [])()


def list_models(job_type="train", author=SENTINEL, client=None, **kwargs):
    """List a user's CivisML models.

    Parameters
    ----------
    job_type : {"train", "predict", None}
        The type of model job to list. If "train", list training jobs
        only (including registered models trained outside of CivisML).
        If "predict", list prediction jobs only. If None, list both.
    author : int, optional
        User id of the user whose models you want to list. Defaults to
        the current user. Use ``None`` to list models from all users.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    **kwargs : kwargs
        Extra keyword arguments passed to `client.scripts.list_custom()`

    See Also
    --------
    APIClient.scripts.list_custom
    """
    if job_type == "train":
        job_types = ('training',)
    elif job_type == "predict":
        job_types = ('prediction',)
    elif job_type is None:
        job_types = ('training', 'prediction')
    else:
        raise ValueError("Parameter 'job_type' must be None, 'train', "
                         "or 'predict'.")

    if client is None:
        client = APIClient()

    template_id_list = [
        ids[_job_type]
        for _job_type in job_types
        for ids in _get_template_ids_all_versions(client).values()
    ]
    # Applying set() because we don't want repeated IDs
    # between the version-less production alias and the versioned alias.
    template_id_str = ', '.join(str(tmp) for tmp in set(template_id_list))

    if author is SENTINEL:
        author = client.users.list_me().id

    # default to showing most recent models first
    kwargs.setdefault('order_dir', 'desc')

    models = client.scripts.list_custom(from_template_id=template_id_str,
                                        author=author,
                                        **kwargs)
    return models


def put_models_shares_users(id, user_ids, permission_level,
                            client=None,
                            share_email_body='DEFAULT',
                            send_shared_email='DEFAULT'):
    """Set the permissions users have on this object

    Use this on both training and scoring jobs.
    If used on a training job, note that "read" permission is
    sufficient to score the model.

    Parameters
    ----------
    id : integer
        The ID of the resource that is shared.
    user_ids : list
        An array of one or more user IDs.
    permission_level : string
        Options are: "read", "write", or "manage".
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    share_email_body : string, optional
        Custom body text for e-mail sent on a share.
    send_shared_email : boolean, optional
        Send email to the recipients of a share.

    Returns
    -------
    readers : dict::
        - users : list::
            - id : integer
            - name : string
        - groups : list::
            - id : integer
            - name : string
    writers : dict::
        - users : list::
            - id : integer
            - name : string
        - groups : list::
            - id : integer
            - name : string
    owners : dict::
        - users : list::
            - id : integer
            - name : string
        - groups : list::
            - id : integer
            - name : string
    total_user_shares : integer
        For owners, the number of total users shared. For writers and readers,
        the number of visible users shared.
    total_group_shares : integer
        For owners, the number of total groups shared. For writers and readers,
        the number of visible groups shared.
    """
    kwargs = {}
    if send_shared_email != 'DEFAULT':
        kwargs['send_shared_email'] = send_shared_email
    if share_email_body != 'DEFAULT':
        kwargs['share_email_body'] = share_email_body
    return _share_model(id, user_ids, permission_level, entity_type='users',
                        client=client, **kwargs)


def put_models_shares_groups(id, group_ids, permission_level,
                             client=None,
                             share_email_body='DEFAULT',
                             send_shared_email='DEFAULT'):
    """Set the permissions groups have on this model.

    Use this on both training and scoring jobs.
    If used on a training job, note that "read" permission is
    sufficient to score the model.

    Parameters
    ----------
    id : integer
        The ID of the resource that is shared.
    group_ids : list
        An array of one or more group IDs.
    permission_level : string
        Options are: "read", "write", or "manage".
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.
    share_email_body : string, optional
        Custom body text for e-mail sent on a share.
    send_shared_email : boolean, optional
        Send email to the recipients of a share.

    Returns
    -------
    readers : dict::
        - users : list::
            - id : integer
            - name : string
        - groups : list::
            - id : integer
            - name : string
    writers : dict::
        - users : list::
            - id : integer
            - name : string
        - groups : list::
            - id : integer
            - name : string
    owners : dict::
        - users : list::
            - id : integer
            - name : string
        - groups : list::
            - id : integer
            - name : string
    total_user_shares : integer
        For owners, the number of total users shared. For writers and readers,
        the number of visible users shared.
    total_group_shares : integer
        For owners, the number of total groups shared. For writers and readers,
        the number of visible groups shared.
    """
    kwargs = {}
    if send_shared_email != 'DEFAULT':
        kwargs['send_shared_email'] = send_shared_email
    if share_email_body != 'DEFAULT':
        kwargs['share_email_body'] = share_email_body
    return _share_model(id, group_ids, permission_level, entity_type='groups',
                        client=client, **kwargs)


def _share_model(job_id, entity_ids, permission_level, entity_type,
                 client=None, **kwargs):
    """Share a container job and all run outputs with requested entities"""
    client = client or APIClient()
    if entity_type not in ['groups', 'users']:
        raise ValueError("'entity_type' must be one of ['groups', 'users']. "
                         "Got '{0}'.".format(entity_type))

    log.debug("Sharing object %d with %s %s at permission level %s.",
              job_id, entity_type, entity_ids, permission_level)
    _func = getattr(client.scripts, "put_containers_shares_" + entity_type)
    result = _func(job_id, entity_ids, permission_level, **kwargs)

    # CivisML relies on several run outputs attached to each model run.
    # Go through and share all outputs on each run.
    runs = client.scripts.list_containers_runs(job_id, iterator=True)
    for run in runs:
        log.debug("Sharing outputs on %d, run %s.", job_id, run.id)
        outputs = client.scripts.list_containers_runs_outputs(job_id, run.id)
        for _output in outputs:
            if _output['object_type'] == 'File':
                _func = getattr(client.files, "put_shares_" + entity_type)
                obj_permission = permission_level
            elif _output['object_type'] == 'Project':
                _func = getattr(client.projects, "put_shares_" + entity_type)
                if permission_level == 'read':
                    # Users must be able to add to projects to use the model
                    obj_permission = 'write'
                else:
                    obj_permission = permission_level
            elif _output['object_type'] == 'JSONValue':
                _func = getattr(client.json_values,
                                "put_shares_" + entity_type)
                obj_permission = permission_level
            else:
                log.debug("Found a run output of type %s, ID %s; not sharing "
                          "it.", _output['object_type'],  _output['object_id'])
                continue
            _oid = _output['object_id']
            # Don't send share emails for any of the run outputs.
            _func(_oid, entity_ids, obj_permission, send_shared_email=False)

    return result


def delete_models_shares_users(id, user_id, client=None):
    """Revoke the permissions a user has on this object

    Use this function on both training and scoring jobs.

    Parameters
    ----------
    id : integer
        The ID of the resource that is shared.
    user_id : integer
        The ID of the user.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.

    Returns
    -------
    None
        Response code 204: success
    """
    return _unshare_model(id, user_id, entity_type='users', client=client)


def delete_models_shares_groups(id, group_id, client=None):
    """Revoke the permissions a group has on this object

    Use this function on both training and scoring jobs.

    Parameters
    ----------
    id : integer
        The ID of the resource that is shared.
    group_id : integer
        The ID of the group.
    client : :class:`civis.APIClient`, optional
        If not provided, an :class:`civis.APIClient` object will be
        created from the :envvar:`CIVIS_API_KEY`.

    Returns
    -------
    None
        Response code 204: success
    """
    return _unshare_model(id, group_id, entity_type='groups', client=client)


def _unshare_model(job_id, entity_id, entity_type, client=None):
    """Revoke permissions on a container job and all run outputs
    for the requested entity (singular)
    """
    client = client or APIClient()
    if entity_type not in ['groups', 'users']:
        raise ValueError("'entity_type' must be one of ['groups', 'users']. "
                         "Got '{0}'.".format(entity_type))

    log.debug("Revoking permissions on object %d for %s %s.",
              job_id, entity_type, entity_id)
    _func = getattr(client.scripts, "delete_containers_shares_" + entity_type)
    result = _func(job_id, entity_id)

    # CivisML relies on several run outputs attached to each model run.
    # Go through and revoke permissions for outputs on each run.
    runs = client.scripts.list_containers_runs(job_id, iterator=True)
    endpoint_name = "delete_shares_" + entity_type
    for run in runs:
        log.debug("Unsharing outputs on %d, run %s.", job_id, run.id)
        outputs = client.scripts.list_containers_runs_outputs(job_id, run.id)
        for _output in outputs:
            if _output['object_type'] == 'File':
                _func = getattr(client.files, endpoint_name)
            elif _output['object_type'] == 'Project':
                _func = getattr(client.projects, endpoint_name)
            elif _output['object_type'] == 'JSONValue':
                _func = getattr(client.json_values, endpoint_name)
            else:
                log.debug("Found run output of type %s, ID %s; not unsharing "
                          "it.", _output['object_type'],  _output['object_id'])
                continue
            _func(_output['object_id'], entity_id)

    return result
