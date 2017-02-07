from collections import OrderedDict
import re
import textwrap
try:
    from inspect import Signature, Parameter
except ImportError:
    from funcsigs import Signature, Parameter
try:
    from functools import lru_cache
except ImportError:
    from functools32 import lru_cache

from jsonref import JsonRef
import requests

from civis.base import Endpoint
from civis._utils import camel_to_snake, to_camelcase


API_VERSIONS = ["1.0"]
BASE_RESOURCES_V1 = ["credentials", "databases", "files", "imports",
                     "jobs", "models", "predictions", "projects",
                     "queries", "reports", "scripts", "tables", "users"]
TYPE_MAP = {"array": "list", "object": "dict"}
ITERATOR_PARAM_DESC = (
    "iterator : bool, optional\n"
    "    If True, return a generator to iterate over all responses. Use when\n"
    "    more results than the maximum allowed by limit are needed. When\n"
    "    True, limit and page_num are ignored. Defaults to False.\n")


def exclude_resource(path, api_version, resources):
    if api_version == "1.0" and resources == "base":
        include = any([path.startswith(x) for x in BASE_RESOURCES_V1])
    else:
        include = True
    return not include


def get_properties(x):
    return x.get("properties") or x.get("items", {}).get("properties")


def property_type(props):
    t = TYPE_MAP.get(props['type'], props['type'])
    fmt = props.get("format")
    return "{}/{}".format(t, fmt) if fmt else t


def name_and_type_doc(name, prop, child, level, optional=False):
    """ Create a doc string element that includes a parameter's name
    and its type. This is intented to be combined with another
    doc string element that gives a description of the parameter.
    """
    prop_type = property_type(prop)
    snake_name = camel_to_snake(name)
    indent = " " * level * 4
    dash = "- " if level > 0 else ""
    colons = "::" if child else ""
    opt_str = ", optional" if optional else ""
    doc = "{}{}{} : {}{}{}"
    return doc.format(indent, dash, snake_name, prop_type, opt_str, colons)


def docs_from_property(name, prop, properties, level, optional=False):
    """ Create a list of doc string elements from a single property
    object. Avoids infinite recursion when a property contains a
    circular reference to its parent.
    """
    docs = []
    child_properties = get_properties(prop)
    child = None if child_properties == properties else child_properties
    docs.append(name_and_type_doc(name, prop, child, level, optional))
    doc_str = prop.get("description")
    if doc_str:
        indent = 4 * (level + 1) * " "
        doc_wrap = textwrap.fill(doc_str,
                                 initial_indent=indent,
                                 subsequent_indent=indent,
                                 width=79)
        docs.append(doc_wrap)
    if child:
        child_docs = docs_from_properties(child, level + 1)
        docs.append("\n".join(child_docs))
    return docs


def docs_from_properties(properties, level=0):
    """ Return doc string elements from a dictionary of properties."""
    docs = []
    for name, prop in properties.items():
        doc_list = docs_from_property(name, prop, properties, level)
        docs.extend(doc_list)
    return docs


def doc_from_responses(responses):
    """ Return a doc string element from a responses object. The
    doc string describes the returned objects of a function.
    """
    response_code, response_object = next(iter(responses.items()))
    schema = response_object.get('schema', {})
    properties = get_properties(schema)
    if properties:
        result_doc = "\n".join(docs_from_properties(properties))
    else:
        description = response_object['description']
        result_doc_fmt = "None\n    Response code {}: {}"
        result_doc = result_doc_fmt.format(response_code, description)
    return "Returns\n-------\n" + result_doc


def join_doc_elements(*args):
    return "\n".join(args).rstrip()


def doc_from_param(param):
    """ Return a doc string element for a single parameter.
    Intended to be joined with other doc string elements to
    form a complete docstring of the accepted parameters of
    a function.
    """
    snake_name = camel_to_snake(param['name'])
    param_type = param['type']
    desc = param.get('description')
    optional = "" if param["required"] else ", optional"
    doc_body = ""
    if desc:
        indent = " " * 4
        doc_wrap = textwrap.fill(desc,
                                 initial_indent=indent,
                                 subsequent_indent=indent,
                                 width=79)
        doc_body += doc_wrap
        doc_body += "\n"
    doc_head = "{} : {}{}\n".format(snake_name, param_type, optional)
    return doc_head + doc_body


def iterable_method(method, params):
    """Determine whether it is possible for this endpoint to return an iterated
    response.
    """
    required_params = ('limit', 'page_num')
    params_present = all(param in params for param in required_params)
    return (method.lower() == 'get' and params_present)


def create_signature(args, kwargs):
    """ Dynamically create a signature for a function from strings.

    This function can be used to create a signature for a dynamically
    generated function without generating a string representation of
    the function code and making an explicit eval call.

    Parameters
    ----------
    args : list
        List of strings that name the required arguments of a function.
    kwargs : list
        List of strings that name the optional arguments of a function.

    Returns
    -------
    Signature(p) : inspect.Signature instance
        A Signature object that can be used to validate arguments in
        a dynamically created function.
    """
    p = [Parameter(x, Parameter.POSITIONAL_OR_KEYWORD) for x in args]
    if kwargs:
        p.append(Parameter('kwargs', Parameter.VAR_KEYWORD))
    return Signature(p)


def split_method_params(params):
    args = []
    kwargs = []
    body_params = []
    query_params = []
    path_params = []
    for param in params:
        name = param["name"]
        if param["required"]:
            args.append(name)
        else:
            kwargs.append(name)
        if param["in"] == "body":
            body_params.append(name)
        elif param["in"] == "query":
            query_params.append(name)
        elif param["in"] == "path":
            path_params.append(name)
    return args, kwargs, body_params, query_params, path_params


def create_method(params, verb, method_name, path, doc):
    """ Dynamically create a function to make an API call.

    The returned function accepts required parameters as positional arguments
    and optional parameters as kwargs.  The function passes these parameters
    into the appropriate place (path, query or body) in the API call,
    depending on the criteria of the API endpoint.

    Parameters
    ----------
    params : list
        A list of dictionaries that specify the arguments used
        by the returned function f
    verb : str
        HTTP verb to call
    method_name : str
        The name to give the returned function f
    path : str
        Endpoint path, possibly including replacement fields
        (i.e. scripts/{id})
    doc : str
        Documentation string for the returned function f


    Returns
    ------
    f : function
        A function which will make an API call
    """
    elements = split_method_params(params)
    sig_args, sig_kwargs, body_params, query_params, path_params = elements
    sig = create_signature(sig_args, sig_kwargs)
    is_iterable = iterable_method(verb, query_params)

    def f(self, *args, **kwargs):
        arguments = sig.bind(*args, **kwargs).arguments
        if arguments.get("kwargs"):
            arguments.update(arguments.pop("kwargs"))
        raise_for_unexpected_kwargs(method_name, arguments, sig_args,
                                    sig_kwargs, is_iterable)
        body = {x: arguments[x] for x in body_params if x in arguments}
        query = {x: arguments[x] for x in query_params if x in arguments}
        path_vals = {x: arguments[x] for x in path_params if x in arguments}
        url = path.format(**path_vals) if path_vals else path
        iterator = arguments.get('iterator', False)
        return self._call_api(verb, url, query, body, iterator=iterator)

    # Add signature to function, including 'self' for class method
    sig_self = create_signature(["self"] + sig_args, sig_kwargs)
    f.__signature__ = sig_self
    f.__doc__ = doc
    f.__name__ = method_name
    return f


def raise_for_unexpected_kwargs(method_name, arguments, sig_args, sig_kwargs,
                                is_iterable):
    """Raise TypeError if arguments are not in sig_args or sig_kwargs."""
    expected = set(sig_args) | set(sig_kwargs)
    if is_iterable:
        expected |= {'iterator'}
    unexpected = set(arguments.keys()) - expected
    if unexpected:
        msg_fmt = "{}() got an unexpected keyword argument(s) {}"
        raise TypeError(msg_fmt.format(method_name, str(unexpected)))


def bracketed(x):
    return re.search("^{.*}$", x)


def parse_param(param):
    """ Parse a parameter into a list of dictionaries which can
    be used to add the parameter to a dynamically generated function.
    """
    doc = ""
    args = []
    param_in = param['in']
    if param_in == 'body':
        body_args = parse_param_body(param)
        args.extend(body_args)
    else:
        snake_name = camel_to_snake(param['name'])
        req = param['required']
        doc = doc_from_param(param)
        a = {"name": snake_name, "in": param_in, "required": req, "doc": doc}
        args.append(a)
    return args


def parse_params(parameters, summary, verb):
    """ Parse the parameters of a function specification into a list
    of dictionaries which are used to generate the function at runtime.
    """
    args = []
    for param in parameters:
        args.extend(parse_param(param))
    if iterable_method(verb, (x["name"] for x in args)):
        iter_arg = {"name": "iterator", "in": None,
                    "required": False, "doc": ITERATOR_PARAM_DESC}
        args.append(iter_arg)
    req_docs = [x["doc"] for x in args if x["required"]]
    opt_docs = [x["doc"] for x in args if not x["required"]]
    param_docs = "".join(req_docs + opt_docs)
    if summary:
        summary_str = "{}\n".format(textwrap.fill(summary, width=79))
    else:
        summary_str = ""
    if param_docs:
        docs = "{}\nParameters\n----------\n{}".format(summary_str, param_docs)
    elif summary:
        docs = summary_str
    return args, docs


def parse_param_body(parameter):
    """ Parse the nested element of a parameter into a list of dictionaries
    which can be used to add the parameter to a dynamically generated
    function.
    """
    schema = parameter['schema']
    properties = schema['properties']
    req = schema.get('required', [])
    arguments = []
    for name, prop in properties.items():
        snake_name = camel_to_snake(name)
        is_req = name in req
        doc_list = docs_from_property(name, prop, properties, 0, not is_req)
        doc = "\n".join(doc_list) + "\n"
        a = {"name": snake_name, "in": "body", "required": is_req, "doc": doc}
        arguments.append(a)
    return arguments


def parse_method_name(verb, path):
    """ Create method name from endpoint path

    Create method name as the http verb (method) followed by
    any static path parameters. As there is ambiguity where
    a path may optionally have a path parameter passed (i.e.
    GET reports/ and GET reports/{id}), any GET method is changed
    to "list" for endpoints where the final path parameter is
    static.

    Examples
    --------
    >>> parse_method_name("get", "url.com/containers")
    list_containers
    >>> parse_method_name("get", "url.com/containers/{id}")
    get_containers
    >>> parse_method_name("get", "url.com/containers/{id}/runs/{run_id}")
    get_containers_runs
    >>> parse_method_name("post", "containers/{id}/runs/{run_id}")
    post_containers_runs
    """
    path_elems = path.split("/")[1:]
    name_elems = []
    for i, elem in enumerate(path_elems):
        prev_elem = path_elems[i - 1] if i > 0 else None
        if not bracketed(elem):
            name_elems.append(elem)
        elif prev_elem and bracketed(prev_elem):
            name_elems.append(prev_elem.strip('{|}'))
    final_elem = path_elems[-1] if path_elems else ""
    verb = "list" if verb == "get" and (not bracketed(final_elem)) else verb
    path_name = "_".join(name_elems)
    method_name = "_".join((verb, path_name)) if path_name else verb
    return re.sub("-", "_", method_name)


def parse_method(verb, operation, path):
    """ Generate a python function from a specification of that function."""
    summary = operation["summary"]
    params = operation["parameters"]
    responses = operation["responses"]
    deprecated = operation.get('deprecated', False)
    if 'deprecated' in summary.lower() or deprecated:
        return None

    args, param_doc = parse_params(params, summary, verb)
    response_doc = doc_from_responses(responses)
    docs = join_doc_elements(param_doc, response_doc)
    name = parse_method_name(verb, path)

    method = create_method(args, verb, name, path, docs)
    return name, method


def parse_path(path, operations, api_version, resources):
    """ Parse an endpoint into a class where each valid http request
    on that endpoint is converted into a convenience function and
    attached to the class as a method.
    """
    path = path.strip('/')
    base_path = path.split('/')[0]
    class_name = to_camelcase(base_path)
    methods = []
    if exclude_resource(path, api_version, resources):
        return class_name, methods
    for verb, op in operations.items():
        method = parse_method(verb, op, path)
        if method is None:
            continue
        methods.append(method)
    return class_name, methods


def parse_swagger(swagger, api_version, resources):
    """ Parse a swagger specifiction into a dictionary of classes
    where each class represents an endpoint resource and contains
    methods to make http requests on that resource.
    """
    paths = swagger['paths']
    classes = {}
    for path, ops in paths.items():
        class_name, methods = parse_path(path, ops, api_version, resources)
        class_name_lower = class_name.lower()
        if methods and classes.get(class_name_lower) is None:
            classes[class_name_lower] = type(class_name, (Endpoint,), {})
        for method_name, method in methods:
            setattr(classes[class_name_lower], method_name, method)
    return classes


@lru_cache(maxsize=4)
def get_swagger_spec(api_key, user_agent, api_version):
    session = requests.Session()
    session.auth = (api_key, '')
    session.headers.update({"User-Agent": user_agent.strip()})
    if api_version == "1.0":
        response = session.get("{}endpoints".format(Endpoint._base_url))
    else:
        msg = "swagger spec for api version {} cannot be found"
        raise ValueError(msg.format(api_version))
    if response.status_code in (401, 403):
        msg = "{} error downloading API specification. API key may be expired."
        raise requests.exceptions.HTTPError(msg.format(response.status_code))
    response.raise_for_status()
    spec = response.json(object_pairs_hook=OrderedDict)
    return spec


@lru_cache(maxsize=4)
def generate_classes(api_key, user_agent, api_version="1.0", resources="base"):
    """ Dynamically create classes to interface with the Civis API.

    The Civis API documents behavior using an OpenAPI/Swagger specification.
    This function parses the specification and returns a dictionary of
    classes to provide a convenient method to make requests to the Civis
    API and to view documentation.

    https://github.com/OAI/OpenAPI-Specification

    Parameters
    ----------
    api_key : str
        Your API key obtained from the Civis Platform. If not given, the
        client will use the :envvar:`CIVIS_API_KEY` environment variable.
    user_agent : str
        The user agent used in the request to get api endpoint specification.
    api_version : string, optional
        The version of endpoints to call. May instantiate multiple client
        objects with different versions.  Currently only "1.0" is supported.
    resources : string, optional
        When set to "base", only the default endpoints will be exposed in the
        client object.  Set to "all" to include all endpoints available for
        a given user, including those that may be in development and subject
        to breaking changes at a later date.
    """
    assert api_version in API_VERSIONS, (
        "APIClient api_version must be one of {}".format(API_VERSIONS))
    assert resources in ["base", "all"], (
        "resources must be one of {}".format(["base", "all"]))
    raw_swagger = get_swagger_spec(api_key, user_agent, api_version)
    swagger = JsonRef.replace_refs(raw_swagger)
    return parse_swagger(swagger, api_version, resources)
