from functools import wraps
from inspect import signature
import warnings


def deprecate(version_removed, parameter_name):
    def decorator(func):
        sig = signature(func)
        if parameter_name not in sig.parameters:
            raise ValueError('"{}" is not a parameter of '
                             '{}.'.format(parameter_name, str(func)))
        i_arg = list(sig.parameters.keys()).index(parameter_name)

        @wraps(func)
        def wrapper(*args, **kwargs):
            if len(args) > i_arg or parameter_name in kwargs:
                f_name = '{}.{}'.format(func.__module__, func.__name__)
                warnings.warn('The "{}" parameter of {} is deprecated and '
                              'will be removed in {}'.format(parameter_name,
                                                             f_name,
                                                             version_removed),
                              FutureWarning)
            return func(*args, **kwargs)
        return wrapper
    return decorator
