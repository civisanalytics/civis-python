from functools import wraps
from inspect import signature
import warnings


def deprecate_param(version_removed, parameter_name):
    """Create a decorator which warns of parameter deprecation

    Use this to create a decorator which will watch for use of a
    deprecated parameter and issue a ``FutureWarning`` if the parameter
    is used. (Use a ``FutureWarning`` because Python does not display
    ``DeprecationWarning`` by default.) The decorator introspects the
    wrapped function's signature so that it catches both keyword
    and positional argument use. The default value of the parameter
    will not be affected.

    Parameters
    ----------
    version_removed: str
        The version in which this parameter will no longer be an allowed
        input to the function, e.g. "v2.0.0".
    parameter_name: str
        The name of the parameter to be deprecated, as it appears in the
        function signature.

    Returns
    -------
    A decorator function

    Raises
    ------
    ValueError
        If the named parameter is not
        an argument of the wrapped function

    Examples
    --------
    >>> @deprecate_param('v2.0.0', 'param2')
    ... def adder(param1, param2=0, param3=0):
    ...     return param1 + param2 + param3
    >>> adder(1, 2, 3)
    /Users/username/src/civis-python/civis/utils/deprecation.py:68:
    FutureWarning: The "param2" parameter of "__main__.adder" is deprecated
    and will be removed in v2.0.0.
      FutureWarning)
    6
    >>> adder(1, param3=13)
    14
    """
    def decorator(func):
        # Introspect the wrapped function so that we can find
        # where the parameter is in the order of the function's inputs.
        # Signature.parameters is a subclass of OrderedDict.
        sig = signature(func)
        if parameter_name not in sig.parameters:
            raise ValueError('"{}" is not a parameter of '
                             '{}.'.format(parameter_name, str(func)))
        i_arg = list(sig.parameters.keys()).index(parameter_name)

        @wraps(func)
        def wrapper(*args, **kwargs):
            if len(args) > i_arg or parameter_name in kwargs:
                f_name = '{}.{}'.format(func.__module__, func.__name__)
                warnings.warn('The "{}" parameter of "{}" is deprecated and '
                              'will be removed in {}.'.format(parameter_name,
                                                              f_name,
                                                              version_removed),
                              FutureWarning)
            return func(*args, **kwargs)
        return wrapper
    return decorator
