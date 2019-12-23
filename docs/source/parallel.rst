********************
Parallel Computation
********************

The Civis Platform manages a pool of cloud computing resources.
You can access these resources with the tools in the :mod:`civis.parallel`
and :mod:`civis.futures` modules.

Joblib backend
==============
If you can divide your work into multiple independent chunks, each of which takes
at least several minutes to run, you can reduce the time your job takes to finish
by running each chunk simultaneously in Civis Platform. The Civis joblib
backend is a software tool which makes it easier to run many jobs simultaneously.

Things to keep in mind when deciding if the Civis joblib backend is the right
tool for your code:

- Each function call which is parallelized with the Civis joblib backend will run
  in a different Civis Platform script. Creating a new script comes with some overhead.
  It will take between a few seconds and a few minutes for each script to start,
  depending on whether Civis Platform needs to provision additional resources.
  If you expect that each function call will complete quickly, instead consider either
  running them in serial or using extra processes in the same Civis Platform script.
- Because function calls run in different scripts, function inputs and outputs
  must be uploaded to Civis Platform from their origin script and downloaded into
  their destination. If your functions take very large inputs and/or produce very
  large outputs, moving the data around will cause additional overhead.
  Consider either using a different tool or refactoring your code so that
  the function to be parallelized is no longer moving around large amounts of data.
- Some open-source libraries, such as ``scikit-learn``, use ``joblib`` to do
  computations in parallel. If you're working with such a library, the Civis
  joblib backend provides an easy way to run these parallel computations in
  different Civis Platform scripts.

Joblib
------
`joblib <https://joblib.readthedocs.io/en/latest/>`_ is an open source
Python library which facilitates parallel processing in Python.
Joblib uses Python's ``multiprocessing`` library to run functions
in parallel, but it also allows users to define their own
"back end" for parallel computation. The Civis Python API client takes
advantage of this to let you easily run your own code in parallel
through Civis Platform.

The :func:`~civis.parallel.make_backend_factory`,
:func:`~civis.parallel.infer_backend_factory`, and
:func:`~civis.parallel.make_backend_template_factory` functions allow you
to define a "civis" parallel computation backend which will transparently
distribute computation in cloud resources managed by the Civis Platform.

See the `joblib user guide <https://joblib.readthedocs.io/en/latest/parallel.html>`_
for examples of using joblib to do parallel computation.
Note that the descriptions of "memmapping" aren't relevant to using
Civis Platform as a backend, since your jobs will potentially run
on different computers and can't share memory.
Using the Civis joblib backend to run jobs in parallel in the cloud
looks the same as running jobs in parallel on your local computer,
except that you first need to set up the "civis" backend.

How to use
----------
Begin by defining the backend. The Civis joblib backend creates and runs
Container Scripts, and the :func:`~civis.parallel.make_backend_factory`
function accepts several arguments which will be passed to
:func:`~civis.resources._resources.Scripts.post_containers`.
For example, you could pass a ``repo_http_uri`` or ``repo_ref``
to clone a repository from GitHub into the container which will run
your function. Use the ``docker_image_name`` and ``docker_image_tag``
to select a custom Docker image for your job.
You can provide a ``setup_cmd`` to run setup in bash before your
function executes in Python. The default ``setup_cmd`` will run
``python setup.py install`` in the base directory of any ``repo_http_uri``
which you include in your backend setup.
Make sure that the environment you define for your Civis backend
includes all of the code which your parallel function will call.

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
`joblib documentation <https://joblib.readthedocs.io/en/latest/parallel.html#custom-backend-api-experimental>`_.

Note that :class:`joblib.Parallel` takes both a ``n_jobs`` and ``pre_dispatch``
parameter. The Civis joblib backend doesn't queue submitted jobs itself,
so it will run ``pre_dispatch`` jobs at once. The default value of
``pre_dispatch`` is "2*n_jobs", which will run a maximum of ``2 * n_jobs`` jobs
at once in the Civis Platform. Set ``pre_dispatch="n_jobs"`` in your
:class:`~joblib.Parallel` call to run at most ``n_jobs`` jobs.

The Civis joblib backend uses `cloudpickle <https://github.com/cloudpipe/cloudpickle>`_
to transport code and data from the parent environment to the Civis Platform.
This means that you may parallelize dynamically-defined functions and classes,
including lambda functions.

The joblib backend will automatically add environment variables called
"CIVIS_PARENT_JOB_ID" and "CIVIS_PARENT_RUN_ID", holding the values
of the job and run IDs of the Civis Platform job in which you're
running the joblib backend (if any). Your functions could use these
to communicate with the parent job or to recognize that they're in
a process which has been created by another Civis Platform job.
However, where possible you should let the joblib backend itself
transport the return value of the function it's running back to the parent.

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

    >>> def expensive_calculation(num1, num2):
    ...     return 2 * num1 + num2
    >>> from joblib import delayed, Parallel
    >>> parallel = Parallel(n_jobs=5)
    >>> args = [(0, 1), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1)]
    >>> print(parallel(delayed(expensive_calculation)(*a) for a in args))
    [1, 3, 5, 7, 9, 11, 13]

You can do the the same parallel computation using the Civis backend
by creating and registering a backend factory and entering a
``with parallel_backend('civis')`` context. The code below will start
seven different jobs in Civis Platform (with up to five running at once).
Each job will call the function ``expensive_calculation`` with a
different set of arguments from the list ``args``.::

    >>> def expensive_calculation(num1, num2):
    ...     return 2 * num1 + num2
    >>> from joblib import delayed, Parallel
    >>> from joblib import parallel_backend, register_parallel_backend
    >>> from civis.parallel import make_backend_factory
    >>> register_parallel_backend('civis', make_backend_factory(
    ...     required_resources={"cpu": 512, "memory": 256}))
    >>> args = [(0, 1), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1)]
    >>> with parallel_backend('civis'):
    ...    parallel = Parallel(n_jobs=5, pre_dispatch='n_jobs')
    ...    print(parallel(delayed(expensive_calculation)(*a) for a in args))
    [1, 3, 5, 7, 9, 11, 13]

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
To run a function in the Civis platform, any modules called by
that function must be importable from a Python interpreter running
in the container script. For example, if you use :class:`joblib.Parallel`
with :func:`numpy.sqrt`, the joblib backend must be set to run
your function in a container which has :mod:`numpy` installed.
If you see an error such as::

    ModuleNotFoundError: No module named 'numpy'

this signifies that the function you're trying to run doesn't exist
in the remote environment. Select a Docker container with the module installed,
or install it in your remote environment by using the ``repo_http_uri``
parameter of :func:`~civis.parallel.make_backend_factory` to install it from GitHub.

Object Reference
================

.. automodule:: civis.parallel
    :members:
