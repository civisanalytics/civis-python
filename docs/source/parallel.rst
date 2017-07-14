********************
Parallel Computation
********************

The Civis Platform manages a pool of cloud computing resources.
You can access these resources with the tools in the :mod:`civis.parallel`
and :mod:`civis.futures` modules.

Joblib backend
==============
`joblib <https://pythonhosted.org/joblib/index.html>`_ is a tool which facilitates
parallel processing in Python. The :func:`~civis.parallel.make_backend_factory`,
:func:`~civis.parallel.infer_backend_factory`, and 
:func:`~civis.parallel.make_backend_template_factory` functions allow you
to define a "civis" parallel computation backend which will transparently
distribute computation in cloud resources managed by the Civis Platform.

How to use
----------
Begin by defining the backend. The Civis joblib backend creates and runs 
Container Scripts, and the :func:`~civis.parallel.make_backend_factory`
function accepts several arguments which will be passed to
:func:`~civis.resources._resources.Scripts.post_containers`.
Use the ``docker_image_name``, ``docker_image_tag``, ``repo_http_uri``,
and ``repo_ref`` parameters to define the environment in which your
code will be run. Make sure that this environment includes all of the code
which you're parallelizing.

The :func:`~civis.parallel.make_backend_factory` function will return a
backend factory which should be given to the :func:`joblib.register_parallel_backend`
function. For example::

    >>> from joblib import register_parallel_backend
    >>> from civis.parallel import make_backend_factory
    >>> be_factory = make_backend_factory()
    >>> register_parallel_backend('civis', be_factory)
    
Direct ``joblib`` to use a custom backend by entering a :func:`joblib.parallel_backend`
context::

    >>> from joblib import parallel_backend
    >>> with parallel_backend('civis'):
    ...     # Do joblib parallel computation here.

You can find more about custom joblib backends in the 
`joblib documentation <http://pythonhosted.org/joblib/parallel.html#custom-backend-api-experimental>`_.

Note that :class:`joblib.Parallel` takes both a ``n_jobs`` and ``pre_dispatch``
parameter. The Civis joblib backend doesn't queue submitted jobs itself,
so it will run ``pre_dispatch`` jobs at once. The default value of
``pre_dispatch`` is "2*n_jobs", which will run a maximum of ``2 * n_jobs`` jobs
at once in the Civis Platform. Set ``pre_dispatch="n_jobs"`` in your
:class:`~joblib.Parallel` call to run at most ``n_jobs`` jobs.

Infer backend parameters
------------------------
If you're writing code which will run inside a Civis Container Script, then
the :func:`~civis.parallel.infer_backend_factory` function returns a backend
factory with environment parameters pre-populated by inspecting the state of
your container script at run time. Use :func:`~civis.parallel.infer_backend_factory`
anywhere you would use :func:`~civis.parallel.make_backend_factory`,
and you don't need to specify a Docker image or GitHub repository.

Templated Scripts
-----------------
The :func:`~civis.parallel.make_backend_template_factory` is intended for
developers who are writing code which may be run by users who don't have
permissions to create new container scripts with the necessary environment.

Instead of defining and creating new container scripts with
:func:`~civis.parallel.make_backend_factory`, you can use
:func:`~civis.parallel.make_backend_template_factory` to launch custom scripts from
a templated script. To use the template factory, your backing container script must
have the Civis Python client installed, and its run command must finish 
by calling ``civis_joblib_worker`` with no arguments. The template must accept the
parameter "JOBLIB_FUNC_FILE_ID". The Civis joblib backend will use this parameter
to transport your remote work.

Examples
--------
Parallel computation using the default joblib backend 
(this uses processes on your local computer)::

    >>> from joblib import delayed, Parallel
    >>> parallel = Parallel(n_jobs=5)
    >>> print(parallel(delayed(sqrt)(i ** 2) for i in range(10)))
    [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]

You can do the the same parallel computation using the Civis backend
by creating and registering a backend factory and entering a
``with parallel_backend('civis')`` context::

    >>> from joblib import parallel_backend, register_parallel_backend
    >>> from civis.parallel import make_backend_factory
    >>> register_parallel_backend('civis', make_backend_factory(
    ...     required_resources={"cpu": 512, "memory": 256}))
    >>> with parallel_backend('civis'):
    ...    parallel = Parallel(n_jobs=5, pre_dispatch='n_jobs')
    ...    print(parallel(delayed(sqrt)(i ** 2) for i in range(10)))
    [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]

You can use the Civis joblib backend to parallelize any code which
uses joblib internally, such as scikit-learn::

    >>> from joblib import parallel_backend, register_parallel_backend
    >>> from sklearn.model_selection import GridSearchCV
    >>> from sklearn.ensemble import GradientBoostingClassifier
    >>> from sklearn.datasets import load_digits
    >>> digits = load_digits()
    >>> param_grid = {
    ...     "max_depth": [1, 3, 5, None],
    ...     "max_features": ["sqrt", "log2", None],
    ...     "learning_rate": [0.1, 0.01, 0.001]
    ... }
    >>> # Note: n_jobs and pre_dispatch specify the maximum number of
    >>> # concurrent jobs.
    >>> gs = GridSearchCV(GradientBoostingClassifier(n_estimators=1000,
    ...                                              random_state=42),
    ...                   param_grid=param_grid,
    ...                   n_jobs=5, pre_dispatch="n_jobs")
    >>> register_parallel_backend('civis', make_backend_factory(
    ...     required_resources={"cpu": 512, "memory": 256}))
    >>> with parallel_backend('civis'):
    ...     gs.fit(digits.data, digits.target)

Debugging
---------
Any (non-retried) errors in child jobs will cause the entire parallel call to
fail. ``joblib`` will transport the first exception from a remote job and
raise it in the parent process so that you can debug.

If your remote jobs are failing because of network problems (e.g. occasional
500 errors), you can make your parallel call more likely to succeed by using
a ``max_job_retries`` value above 0 when creating your backend factory.
This will automatically retry a job (potentially more than once) before giving
up and keeping an exception.

Logging: The Civis joblib backend uses the standard library 
`logging module <https://docs.python.org/3/library/logging.html>`_, 
with debug emits for events which might help you diagnose errors.
See also the "verbose" argument to :class:`joblib.Parallel`, which
prints information to either stdout or stderr.

Mismatches between your local environment and the environment in the
Civis container script jobs are a common source of errors.
To run a function in the Civis platform, that function must be 
importable from a Python interpreter running in the container script.
For example, if you define a function::

    def my_func(x):
        return 2*x
        
and run this with :func:`joblib.Parallel`, you'll get an error like::

    AttributeError: module '__main__' has no attribute 'my_func'
    
which signifies that the function you're trying to run doesn't exist
in the remote environment. Install it in your remote environment by
putting it into a GitHub repository and using the ``repo_http_uri``
parameter of :func:`~civis.parallel.make_backend_factory`.

Object Reference
================

.. automodule:: civis.parallel
    :members:
