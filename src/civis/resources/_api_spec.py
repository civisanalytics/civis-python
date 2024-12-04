from collections import defaultdict
from inspect import signature, isfunction

import civis
from civis.resources import generate_classes_maybe_cached


def download_latest_api_spec(path):
    client = civis.APIClient()
    try:
        job = client.scripts.post_custom(from_template_id=13448)
    except civis.base.CivisAPIError as e:
        if e.status_error == 404:
            raise EnvironmentError(
                "This script can only be run by a Civis employee with their "
                "regular Civis Platform account's API key."
            )
        else:
            raise
    fut = civis.utils.run_job(job.id, client=client, polling_interval=5)
    fut.result()
    print(f"custom script {fut.job_id} run {fut.run_id} has succeeded")
    outputs = client.scripts.list_custom_runs_outputs(fut.job_id, fut.run_id)
    file_id = civis.find_one(outputs, name="civis_api_spec.json").object_id
    with open(path, "wb") as f:
        civis.io.civis_to_file(file_id, f, client=client)


def _get_methods(cls) -> dict:
    return {
        method_name: method
        for method_name, method in vars(cls).items()
        if not method_name.startswith("_") and isfunction(method)
    }


def _compare(reference: dict, compared: dict) -> tuple[dict, dict]:
    new = {
        "endpoints": set(),
        "methods": defaultdict(set),
    }
    changed = {
        "method parameters": defaultdict(set),
        "method docstrings": defaultdict(set),
    }
    for endpoint_name in set(compared.keys()) - set(reference.keys()):
        new["endpoints"].add(endpoint_name)
    for endpoint_name in set(compared.keys()) & set(reference.keys()):
        methods_compared = _get_methods(compared[endpoint_name])
        methods_reference = _get_methods(reference[endpoint_name])
        if meth_names := (set(methods_compared.keys()) - set(methods_reference.keys())):
            for meth_name in meth_names:
                new["methods"][endpoint_name].add(meth_name)
        for meth_name in set(methods_compared.keys()) & set(methods_reference.keys()):
            method_compared = methods_compared[meth_name]
            method_reference = methods_reference[meth_name]
            if (
                signature(method_compared).parameters
                != signature(method_reference).parameters
            ):
                changed["method parameters"][endpoint_name].add(meth_name)
            if method_compared.__doc__ != method_reference.__doc__:
                changed["method docstrings"][endpoint_name].add(meth_name)
    # Convert defaultdicts to regular dicts for nicer pprinting.
    new["methods"] = dict(new["methods"])
    changed["method parameters"] = dict(changed["method parameters"])
    changed["method docstrings"] = dict(changed["method docstrings"])
    return new, changed


def compare_api_specs(path_current: str, path_upstream: str) -> tuple[dict, dict, dict]:
    """Compare two Civis API specs for whether there's a difference.

    Parameters
    ----------
    path_current : str
        Path of the current Civis API spec versioned in the civis-python codebase.
    path_upstream : str
        Path of the latest Civis API spec fetched from upstream.

    Returns
    -------
    tuple[dict, dict, dict]
        Dicts of added, removed, and changed endpoints and methods.
    """
    endpoints_current = generate_classes_maybe_cached(
        path_current, api_key="no_key_needed", api_version="1.0"
    )
    endpoints_upstream = generate_classes_maybe_cached(
        path_upstream, api_key="no_key_needed", api_version="1.0"
    )
    added, changed = _compare(endpoints_current, endpoints_upstream)
    removed, _ = _compare(endpoints_upstream, endpoints_current)
    return added, removed, changed
