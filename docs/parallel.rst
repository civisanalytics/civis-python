********************
Parallel Computation
********************

Civis Platform manages a pool of cloud computing resources.
You can access these resources with the tools in the :mod:`civis.futures`
and :mod:`civis.parallel` modules.

.. warning::
    :mod:`civis.parallel` is deprecated since civis-python v2.10.0 and will be
    removed at civis-python v3.0.0 (scheduled for release in February 2027).
    For parallel computation via Civis Platform, please migrate to
    :class:`~civis.futures.CivisFuture`, as described in `Civis futures`_ below.

.. _civis_futures_parallel:

Civis futures
=============

A :class:`~civis.futures.CivisFuture` is a
:class:`python:concurrent.futures.Future` that tracks a job running in
Civis Platform. Because it follows the standard library's ``Future`` API,
you can start many Civis Platform jobs at once and collect their results with
the usual :mod:`python:concurrent.futures` helpers, such as
:func:`python:concurrent.futures.as_completed` and
:func:`python:concurrent.futures.wait`.

Fanning out many jobs
---------------------

Submit one job per chunk of work, keep the futures around, and consume them as
they finish. It starts several Civis Platform
container scripts, each computing ``2 * num1 + num2`` for a different pair of
arguments:

.. code-block:: python

    import concurrent.futures

    import civis

    client = civis.APIClient()


    def submit(num1, num2):
        """Start a container script and return a CivisFuture for its run."""
        script = client.scripts.post_containers(
            required_resources={"cpu": 512, "memory": 256},
            docker_image_name="civisanalytics/datascience-python",
            docker_command=f"python -c 'print(2 * {num1} + {num2})'",
        )
        return civis.utils.run_job(script.id, client=client)


    args = [(0, 1), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1)]

    # Map each future back to the arguments it was created with, so that
    # out-of-order results can be re-associated with their inputs.
    futures = {submit(*a): a for a in args}

    for future in concurrent.futures.as_completed(futures):
        num1, num2 = futures[future]
        run = future.result()  # blocks until this one job is done
        print(f"({num1}, {num2}) finished with state {run.state}")

:func:`~civis.utils.run_job` is a convenience wrapper that starts a run for an
existing job and hands back a :class:`~civis.futures.CivisFuture`.
:func:`python:concurrent.futures.as_completed` gives you
each result as soon
as its job finishes, rather than after all jobs have finished. If you want the
results in submission order instead, iterate over the futures directly and call
``.result()`` on each one.

Passing inputs and outputs
--------------------------

:class:`~civis.futures.CivisFuture` does not ship a function, its arguments,
or its return value between the parent process and a child job. Instead:

- The code you want to run must already exist in the child job's environment,
  either baked into the Docker image or cloned from GitHub via the
  ``repo_http_uri`` and ``repo_ref`` parameters of
  :func:`~civis.resources._resources.Scripts.post_containers`.
- You are responsible for moving data. Small inputs can go directly into the
  ``docker_command``. Larger inputs, and all outputs, should travel through
  Civis Files.

.. code-block:: python

    import io

    import civis

    client = civis.APIClient()

    # Upload the input for one child job.
    input_file_id = civis.io.file_to_civis(
        io.BytesIO(b"some input data"), name="my-input", client=client
    )

    script = client.scripts.post_containers(
        required_resources={"cpu": 512, "memory": 256},
        docker_image_name="civisanalytics/datascience-python",
        docker_command=f"python /app/my_script.py {input_file_id}",
        repo_http_uri="github.com/my-org/my-repo.git",
    )
    future = civis.utils.run_job(script.id, client=client)
    future.result()

    # Download what the child job produced.
    output_file_id = civis.io.file_id_from_run_output(
        "my-output", future.job_id, future.run_id, client=client
    )
    buf = io.BytesIO()
    civis.io.civis_to_file(output_file_id, buf, client=client)

For the parent to find the output that way, the child job has to register it as
a run output. Inside ``my_script.py``, that looks like:

.. code-block:: python

    import os

    import civis

    client = civis.APIClient()
    output_file_id = civis.io.file_to_civis(buf, name="my-output", client=client)
    client.scripts.post_containers_runs_outputs(
        os.environ["CIVIS_JOB_ID"],
        os.environ["CIVIS_RUN_ID"],
        "File",
        output_file_id,
    )

A child job can also read the ``CIVIS_JOB_ID`` and ``CIVIS_RUN_ID`` environment
variables to identify itself, and you can pass the parent's own job and run IDs
down to it if the child needs to communicate back to the parent:

.. code-block:: python

    import os

    import civis

    client = civis.APIClient()

    # The parent job's own identity, forwarded to the child.
    parent_job_id = os.environ["CIVIS_JOB_ID"]
    parent_run_id = os.environ["CIVIS_RUN_ID"]

    script = client.scripts.post_containers(
        required_resources={"cpu": 512, "memory": 256},
        docker_image_name="civisanalytics/datascience-python",
        docker_command=(
            f"python /app/my_script.py {parent_job_id} {parent_run_id}"
        ),
        repo_http_uri="github.com/my-org/my-repo.git",
    )
    future = civis.utils.run_job(script.id, client=client)
    future.result()

Inside ``my_script.py``, the child job reads those IDs from its arguments
and can use them to, for example, attach one of its own outputs to the
parent's run instead of its own:

.. code-block:: python

    import sys

    import civis

    parent_job_id, parent_run_id = sys.argv[1], sys.argv[2]

    client = civis.APIClient()
    output_file_id = civis.io.file_to_civis(
        buf, name="status-from-child", client=client
    )
    client.scripts.post_containers_runs_outputs(
        parent_job_id,
        parent_run_id,
        "File",
        output_file_id,
    )

Errors, retries, and concurrency
--------------------------------

Calling ``.result()`` on a future for a job that failed raises a
``civis.base.CivisJobFailure``.
You may catch failures and choose how to handle them:

.. code-block:: python

    for future in concurrent.futures.as_completed(futures):
        try:
            run = future.result()
        except Exception as e:
            print(f"{futures[future]} failed: {e}")

To stop as soon as anything fails, use
:func:`python:concurrent.futures.wait` with
``return_when=concurrent.futures.FIRST_EXCEPTION``, and then cancel the runs
that are still going by calling ``.cancel()`` on each remaining
:class:`~civis.futures.CivisFuture`.

Joblib backend
==============

.. warning::
    :mod:`civis.parallel`, and therefore the Civis joblib backend described in
    this section, is deprecated since civis-python v2.10.0 and will be removed
    at civis-python v3.0.0 (scheduled for release in February 2027).
    For parallel computation via Civis Platform, please migrate to
    :class:`~civis.futures.CivisFuture`, as described in `Civis futures`_ above.

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
While Joblib comes with its own parallel computation tools,
it also allows users to define their own
"back end". The Civis Python API client takes
advantage of this to let you easily run your own code in parallel
through Civis Platform.

The :func:`~civis.parallel.make_backend_factory`,
:func:`~civis.parallel.infer_backend_factory`, and
:func:`~civis.parallel.make_backend_template_factory` functions allow you
to define a "civis" parallel computation backend which will transparently
distribute computation in cloud resources managed by Civis Platform.

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
``pip install .`` in the base directory of any ``repo_http_uri``
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

Direct ``joblib`` to use a custom backend by entering a :func:`joblib.parallel_config`
context::

    >>> from joblib import parallel_config
    >>> with parallel_config('civis'):
    ...     # Do joblib parallel computation here.

You can find more about custom joblib backends in the
`joblib documentation <https://joblib.readthedocs.io/en/latest/parallel.html#custom-backend-api>`_.

Note that :class:`joblib.Parallel` takes both a ``n_jobs`` and ``pre_dispatch``
parameter. The Civis joblib backend doesn't queue submitted jobs itself,
so it will run ``pre_dispatch`` jobs at once.

.. note::
    Since joblib v1.3.0, ``n_jobs=1`` will disable the use of the
    specified parallel backend, and will simply run the given function
    with a regular ``for`` loop. ``n_jobs=1`` would be useful for testing
    your code locally before running it in Civis Platform.

.. note::
    The default value of
    ``pre_dispatch`` is ``"2*n_jobs"``, which will run a maximum of ``2 * n_jobs`` jobs
    at once on Civis Platform. Set ``pre_dispatch="n_jobs"`` in your
    :class:`joblib.Parallel` call to run at most ``n_jobs`` jobs.

The Civis joblib backend uses `cloudpickle <https://github.com/cloudpipe/cloudpickle>`_
to transport code and data from the parent environment to Civis Platform.
This means that you may parallelize dynamically-defined functions and classes,
including lambda functions.

The joblib backend will automatically add environment variables called
``CIVIS_PARENT_JOB_ID`` and ``CIVIS_PARENT_RUN_ID``, holding the values
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

Script Templates
----------------
The :func:`~civis.parallel.make_backend_template_factory` is intended for
developers who are writing code which may be run by users who don't have
permissions to create new container scripts with the necessary environment.

Instead of defining and creating new container scripts with
:func:`~civis.parallel.make_backend_factory`, you can use
:func:`~civis.parallel.make_backend_template_factory` to launch custom scripts from
a script template. To use the template factory, your backing container script must
have the Civis Python client installed, and its run command must finish
by calling ``civis_joblib_worker`` with no arguments. The template must accept the
parameter ``JOBLIB_FUNC_FILE_ID``. The Civis joblib backend will use this parameter
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

You can do the same parallel computation using the Civis backend
by creating and registering a backend factory and entering a
``with parallel_config('civis')`` context. The code below will start
seven different jobs in Civis Platform (with up to five running at once).
Each job will call the function ``expensive_calculation`` with a
different set of arguments from the list ``args``::

    >>> def expensive_calculation(num1, num2):
    ...     return 2 * num1 + num2
    >>> from joblib import delayed, Parallel
    >>> from joblib import parallel_config, register_parallel_backend
    >>> from civis.parallel import make_backend_factory
    >>> register_parallel_backend('civis', make_backend_factory(
    ...     required_resources={"cpu": 512, "memory": 256}))
    >>> args = [(0, 1), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1)]
    >>> with parallel_config('civis'):
    ...     parallel = Parallel(n_jobs=5, pre_dispatch='n_jobs')
    ...     print(parallel(delayed(expensive_calculation)(*a) for a in args))
    [1, 3, 5, 7, 9, 11, 13]

Since joblib v1.4.2, :class:`joblib.Parallel` has a ``return_as`` parameter
that can accept ``'generator'`` or ``'generator_unordered'``
(default is ``'list'``, whose behavior is shown in the examples above).
Returning a generator, especially the "unordered" version, instead of a list
is useful for getting (partial) results back from Civis Platform faster
as soon as any child job finishes (as opposed to having to waiting for `all` child jobs
to finish before you get a resulting list). With ``return_as='generator_unordered'``,
you might want to keep track of the ordering of the child jobs' results
using :func:`enumerate`:

.. code-block:: python

    >>> import inspect
    >>> import time
    >>> def expensive_calculation(order, num):
    ...     # lower order for a longer sleep to simulate a longer job
    ...     time.sleep(10 ** (2 - order))
    ...     return order, num
    >>> from joblib import delayed, Parallel
    >>> from joblib import parallel_config, register_parallel_backend
    >>> from civis.parallel import make_backend_factory
    >>> register_parallel_backend('civis', make_backend_factory(
    ...     required_resources={"cpu": 512, "memory": 256}))
    >>> args = enumerate(['foo', 'bar', 'baz'])  # `enumerate` to get an order index
    >>> with parallel_config('civis'):
    ...     parallel = Parallel(
    ...         n_jobs=5, pre_dispatch='n_jobs',
    ...         return_as='generator_unordered',  # yields a result from a child job as soon as it's ready
    ...     )
    ...     results = parallel(delayed(expensive_calculation)(*a) for a in args)
    ...     assert inspect.isgenerator(results)
    ...     for order, num in results:
    ...         print(order, num)
    2 baz
    1 bar
    0 foo

Lastly, you can use the Civis joblib backend to parallelize any code which
uses joblib internally, such as scikit-learn::

    >>> from joblib import parallel_config, register_parallel_backend
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
    >>> with parallel_config('civis'):
    ...     gs.fit(digits.data, digits.target)

Debugging
---------
Any (non-retried) errors in child jobs will cause the entire parallel call to
fail. ``joblib`` will transport the first exception from a remote job and
raise it in the parent process so that you can debug.

If your remote jobs are failing because of network problems (e.g., occasional
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
To run a function in Civis platform, any modules called by
that function must be importable from a Python interpreter running
in the container script. For example, if you use :class:`joblib.Parallel`
with :func:`numpy.sqrt`, the joblib backend must be set to run
your function in a container which has :mod:`numpy` installed.
If you see an error such as::

    ModuleNotFoundError: No module named 'numpy'

this signifies that the module you're trying to use doesn't exist
in the remote environment. Select a Docker container with the module installed,
or install it in your remote environment by using the ``repo_http_uri``
parameter of :func:`~civis.parallel.make_backend_factory` to install it from GitHub.

Object Reference
================

.. automodule:: civis.parallel
    :members:
