from __future__ import annotations

import os
import uuid
from typing import TypeAlias


TypePathLike: TypeAlias = str | bytes | os.PathLike


def maybe_get_random_name(name):
    if not name:
        name = uuid.uuid4().hex
    return name
