import inspect
import os
import textwrap

from civis.resources import generate_classes_maybe_cached


CLIENT_PYI_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
    "client.pyi",
)


def _get_endpoint_class_name(endpoint_name):
    # Factor out this helper function for consistency.
    return f"_{endpoint_name.title()}"


def _get_annotation(param):
    if param.name == "self":
        return ""
    elif param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
        return param.annotation
    else:
        return f"{param.annotation} | None"


def generate_client_pyi(client_pyi_path, api_spec_path):
    classes = generate_classes_maybe_cached(
        api_spec_path, api_key="not_needed", api_version="1.0"
    )

    with open(client_pyi_path, "w") as f:
        f.write(
            """
# This file is auto-generated by tools/update_client_pyi.py.
# Do not edit it by hand.

from collections import OrderedDict
from civis.response import Response, PaginatedResponse

"""
        )

        for endpoint_name, endpoint_class in classes.items():
            f.write(f"class {_get_endpoint_class_name(endpoint_name)}:\n")
            for method_name, method in vars(endpoint_class).items():
                if method_name.startswith("_"):
                    continue
                params = inspect.signature(method).parameters
                if "iterator" in params:
                    return_type = "PaginatedResponse"
                else:
                    return_type = "Response"
                f.write(f"    def {method_name}(\n")
                for param_name, param in params.items():
                    annotation = _get_annotation(param)
                    if param_name == "self":
                        f.write("        self,\n")
                    elif param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                        f.write(f"        {param_name}: {annotation},\n")
                    else:
                        f.write(
                            "        "
                            f"{param_name}: {annotation} = ...,\n"
                        )
                f.write(f"    ) -> {return_type}:\n")
                method_doc = textwrap.indent(method.__doc__, " " * 8).lstrip()
                f.write(
                    f'        """{method_doc}\n'
                    f'        """\n'
                    f'        ...\n'
                )
            f.write("\n")

        f.write(
            """
# Need the individual endpoint classes defined first as above,
# before we can define APIClient to use them.
class APIClient:
    default_credential: int | None
    username: str
    feature_flags: tuple[str]
    last_response: APIClient | None
    def __init__(
        self,
        api_key: str | None = ...,
        return_type: str = ...,
        api_version: str = ...,
        local_api_spec: OrderedDict | str | None = ...,
    ): ...
    def get_aws_credential_id(
        self,
        cred_name: str | int,
        owner: str | None = None,
    ) -> int: ...
    def get_database_credential_id(
        self,
        username: str | int,
        database_name: str | int,
    ) -> int: ...
    def get_database_id(
        self,
        database: str | int,
    ) -> int: ...
    def get_storage_host_id(
        self,
        storage_host: str | int,
    ) -> int: ...
    def get_table_id(
        self,
        table: str,
        database: str | int,
    ) -> int: ...
"""
        )
        for endpoint_name in classes:
            f.write(
                f"    {endpoint_name} = {_get_endpoint_class_name(endpoint_name)}()\n"  # noqa: E501
            )
