"""This script downloads and updates civis_api_spec.json.

civis_api_spec.json contains information about the publicly available
API endpoints. This spec is used in both testing and generating
the public Sphinx docs.
"""

import civis
from civis.tests import TEST_SPEC


if __name__ == "__main__":
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
    with open(TEST_SPEC, "wb") as f:
        civis.io.civis_to_file(file_id, f, client=client)
    print("downloaded civis_api_spec.json")
