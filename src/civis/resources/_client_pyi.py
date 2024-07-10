import inspect
import os
import textwrap
import typing

from civis.resources import generate_classes_maybe_cached
from civis.response import Response


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


def _extract_nested_response_classes(response_classes, return_type):
    response_classes[return_type.__name__] = return_type
    for typ in return_type.__annotations__.values():
        if isinstance(typ, str):
            continue
        if isinstance(typ, typing._GenericAlias):
            typ = typing.get_args(typ)[0]
        response_classes = _extract_nested_response_classes(response_classes, typ)
    return response_classes


def generate_client_pyi(client_pyi_path, api_spec_path):
    classes = generate_classes_maybe_cached(
        api_spec_path, api_key="not_needed", api_version="1.0"
    )

    with open(client_pyi_path, "w") as f:
        f.write(
            """# This file is auto-generated by tools/update_civis_api_spec.py.
# Do not edit it by hand.

from collections import OrderedDict
from collections.abc import Iterator
from typing import Any, List

from civis.response import Response

"""
        )

        response_classes = {}

        for endpoint_name, endpoint_class in classes.items():
            f.write(f"class {_get_endpoint_class_name(endpoint_name)}:\n")
            method_defs = []
            for method_name, method in vars(endpoint_class).items():
                method_def = ""
                if method_name.startswith("_"):
                    continue
                signature = inspect.signature(method)
                return_type = signature.return_annotation
                if return_type is not Response and not isinstance(
                    return_type,
                    (str, typing._SpecialGenericAlias, typing._GenericAlias),
                ):
                    response_classes = _extract_nested_response_classes(
                        response_classes, return_type
                    )
                params = inspect.signature(method).parameters
                method_def += f"    def {method_name}(\n"
                for param_name, param in params.items():
                    annotation = _get_annotation(param)
                    if param_name == "self":
                        method_def += "        self,\n"
                    elif param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                        method_def += f"        {param_name}: {annotation},\n"
                    else:
                        method_def += f"        {param_name}: {annotation} = ...,\n"
                # TODO: when return_str is 'Iterator', add the subscript response class.
                return_str = rt if isinstance(rt := return_type, str) else rt.__name__
                method_def += f"    ) -> {return_str}:\n"
                method_doc = textwrap.indent(method.__doc__, " " * 8).lstrip()
                method_def += f'        """{method_doc}\n        """\n        ...\n'
                method_defs.append(method_def)
            f.write("\n".join(method_defs))
            f.write("\n")

        for response_class in response_classes.values():
            if len(line1 := f"class {response_class.__name__}(Response):") <= 88:
                f.write(f"{line1}\n")
            elif len(line2 := f"class {response_class.__name__}(") <= 88:
                f.write(f"{line2}\n    Response\n):\n")
            else:
                f.write(
                    f"class {response_class.__name__}(  # noqa: E501\n"
                    "    Response\n):\n"
                )
            for name, anno in response_class.__annotations__.items():
                if isinstance(anno, str):
                    anno_str = anno
                elif isinstance(anno, typing._GenericAlias):
                    anno_str = f"{anno.__name__}[{typing.get_args(anno)[0].__name__}]"
                else:
                    anno_str = anno.__name__
                if len(line := f"    {name}: {anno_str}") <= 88:
                    f.write(f"{line}\n")
                else:
                    f.write(f"    {name}: (\n        {anno_str}\n    )\n")
            f.write("\n")

        f.write(
            """# Need the individual endpoint classes defined first as above,
# before we can define APIClient to use them.
class APIClient:
    default_credential: int | None
    username: str
    feature_flags: tuple[str]
    last_response: Any
    def __init__(
        self,
        api_key: str | None = ...,
        return_type: str = ...,
        api_version: str = ...,
        local_api_spec: OrderedDict | str | None = ...,
        force_refresh_api_spec: bool = ...,
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
