from collections import namedtuple

from civis import APIClient
from civis.ml._model import _PRED_TEMPLATES

__all__ = ['list_models']

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
        template_id_list = list(_PRED_TEMPLATES.keys())
    elif job_type == "predict":
        # get a unique list of prediction ids
        template_id_list = list(set(_PRED_TEMPLATES.values()))
    elif job_type is None:
        # use sets to make sure there's no duplicate ids
        template_id_list = list(set(_PRED_TEMPLATES.keys()).union(
                            set(_PRED_TEMPLATES.values())))
    else:
        raise ValueError("Parameter 'job_type' must be None, 'train', "
                         "or 'predict'.")
    template_id_str = ', '.join([str(tmp) for tmp in template_id_list])

    if client is None:
        client = APIClient()

    if author is SENTINEL:
        author = client.users.list_me().id

    # default to showing most recent models first
    kwargs.setdefault('order_dir', 'desc')

    models = client.scripts.list_custom(from_template_id=template_id_str,
                                        author=author,
                                        **kwargs)
    return models
