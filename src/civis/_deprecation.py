from functools import wraps
from inspect import signature
import warnings


def deprecate_param(version_removed, parameter_name, *additional_names):
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
    *additional_names
        Use additional positional arguments to indicate multiple parameters
        to deprecate.

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
    all_names = [parameter_name] + list(additional_names)

    def decorator(func):
        # Introspect the wrapped function so that we can find
        # where the parameter is in the order of the function's inputs.
        # Signature.parameters is a subclass of OrderedDict.
        sig = signature(func)
        i_args = []
        for name in all_names:
            if name not in sig.parameters:
                raise ValueError('"{}" is not a parameter of '
                                 '{}.'.format(parameter_name, str(func)))
            i_args.append(list(sig.parameters.keys()).index(parameter_name))

        @wraps(func)
        def wrapper(*args, **kwargs):
            warn_list = []
            for name, i_arg in zip(all_names, i_args):
                # The len(args) check looks to see if the user has tried
                # to call the deprecated parameter as a positional argument.
                if len(args) > i_arg or name in kwargs:
                    f_name = '{}.{}'.format(func.__module__, func.__name__)
                    msg = ('The "{}" parameter of "{}" is deprecated and '
                           'will be removed in {}.'.format(name,
                                                           f_name,
                                                           version_removed))
                    warn_list.append(msg)
            if warn_list:
                warnings.warn('\n'.join(warn_list), FutureWarning)
            return func(*args, **kwargs)
        return wrapper
    return decorator
