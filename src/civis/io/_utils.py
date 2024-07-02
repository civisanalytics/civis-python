import uuid


def maybe_get_random_name(name):
    if not name:
        name = uuid.uuid4().hex
    return name
