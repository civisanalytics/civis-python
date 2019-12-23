****************
Machine Learning
****************

CivisML uses the Civis Platform to train machine learning models
and parallelize their predictions over large datasets.
It contains best-practice models for general-purpose classification
and regression modeling as well as model quality evaluations and
visualizations. All CivisML models use the `scikit-learn <http://scikit-learn.org/>`_
API for interoperability with other platforms and to allow you to leverage
resources in the open-source software community when creating machine learning models.


Optional dependencies
=====================

You do not need any external libraries installed to use CivisML, but
the following pip-installable dependencies enhance the capabilities of the
:class:`~civis.ml.ModelPipeline`:

- pandas
- scikit-learn
- glmnet
- feather-format
- civisml-extensions
- muffnn


Install :mod:`pandas` if you wish to download tables of predictions.
You can also model on :class:`~pandas.DataFrame` objects in your interpreter.

If you wish to use the :class:`~civis.ml.ModelPipeline` code to model on
:class:`~pandas.DataFrame` objects in your local environment, the
`feather-format <https://github.com/wesm/feather>`_ package (requires `pandas` >= 0.20)
will improve data transfer speeds and guarantee that your data types are correctly
detected by CivisML. You must install `feather-format` if you wish to use
`pd.Categorical` columns in your `DataFrame` objects, since that type information
is lost when writing data as a CSV.

If you wish to use custom models or download trained models,
you'll need scikit-learn installed.

Several pre-defined models rely on public Civis Analytics
libraries. The "sparse_logistic", "sparse_linear_regressor",
"sparse_ridge_regressor", "stacking_classifier", and "stacking_regressor" models
all use the ``glmnet`` library. Pre-defined MLP models
("multilayer_perceptron_classifier" and
"multilayer_perceptron_regressor") depend on the ``muffnn``
library. Finally, models which use the default CivisML ETL,
along with models which use stacking or hyperband, depend on
``civisml-extensions``. Install these packages if you wish to download
the pre-defined models that depend on them.

Define Your Model
=================

Start the modeling process by defining your model. Do this by creating an instance
of the :class:`~civis.ml.ModelPipeline` class. Each :class:`~civis.ml.ModelPipeline` corresponds to a
scikit-learn :class:`~sklearn.pipeline.Pipeline` which will run in Civis Platform.
A :class:`~sklearn.pipeline.Pipeline` allows you to combine multiple
modeling steps (such as missing value imputation and feature selection) into a
single model. The :class:`~sklearn.pipeline.Pipeline` is treated as a unit -- for example,
cross-validation happens over all steps together.

You can define your model in two ways, either by selecting a pre-defined algorithm
or by providing your own scikit-learn
:class:`~sklearn.pipeline.Pipeline` or :class:`~sklearn.base.BaseEstimator` object.
Note that whichever option you chose, CivisML will pre-process your
data using either its default ETL, or ETL that you provide (see :ref:`custom-etl`).

If you have already trained a scikit-learn model outside of Civis Platform,
you can register it with Civis Platform as a CivisML model so that you can
score it using CivisML. Read :ref:`model-registration` for how to do this.

Pre-Defined Models
------------------

You can use the following pre-defined models with CivisML.
All models start by imputing missing values with the mean of non-null
values in a column. The "sparse_*" models include a LASSO regression step
(using the `glmnet <https://github.com/civisanalytics/python-glmnet>`_ package)
to do feature selection before passing data to the final model.
In some models, CivisML uses default parameters different from those in scikit-learn,
as indicated in the "Altered Defaults" column. All models also have
``random_state=42``.

================================  ================    ==================================================================================================================================   ==================================
Name                              Model Type          Algorithm                                                                                                                            Altered Defaults
================================  ================    ==================================================================================================================================   ==================================
sparse_logistic                   classification      `LogisticRegression <http://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html>`_                ``C=499999950, tol=1e-08``
gradient_boosting_classifier      classification      `GradientBoostingClassifier <http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.GradientBoostingClassifier.html>`_    ``n_estimators=500, max_depth=2``
random_forest_classifier          classification      `RandomForestClassifier <http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestClassifier.html>`_            ``n_estimators=500, max_depth=7``
extra_trees_classifier            classification      `ExtraTreesClassifier <http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.ExtraTreesClassifier.html>`_                ``n_estimators=500, max_depth=7``
multilayer_perceptron_classifier  classification      `muffnn.MLPClassifier <https://github.com/civisanalytics/muffnn>`_
stacking_classifier               classification      `civismlext.StackedClassifier <https://github.com/civisanalytics/civisml-extensions>`_
sparse_linear_regressor           regression          `LinearRegression <http://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LinearRegression.html>`_
sparse_ridge_regressor            regression          `Ridge <http://scikit-learn.org/stable/modules/generated/sklearn.linear_model.Ridge.html>`_
gradient_boosting_regressor       regression          `GradientBoostingRegressor <http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.GradientBoostingRegressor.html>`_      ``n_estimators=500, max_depth=2``
random_forest_regressor           regression          `RandomForestRegressor <http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestRegressor.html>`_              ``n_estimators=500, max_depth=7``
extra_trees_regressor             regression          `ExtraTreesRegressor <http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.ExtraTreesRegressor.html>`_                  ``n_estimators=500, max_depth=7``
multilayer_perceptron_regressor   regression          `muffnn.MLPRegressor <https://github.com/civisanalytics/muffnn>`_
stacking_regressor                regression          `civismlext.StackedRegressor <https://github.com/civisanalytics/civisml-extensions>`_
================================  ================    ==================================================================================================================================   ==================================

The "stacking_classifier" model stacks
the "gradient_boosting_classifier",
and "random_forest_classifier" predefined models together with a
``glmnet.LogitNet(alpha=0, n_splits=4, max_iter=10000, tol=1e-5,
scoring='log_loss')``. The models are combined using a
:class:`~sklearn.pipeline.Pipeline` containing a `Normalizer <http://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.Normalizer.html#sklearn.preprocessing.Normalizer>`_
step, followed by `LogisticRegressionCV <http://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegressionCV.html>`_
with ``penalty='l2'`` and ``tol=1e-08``. The
"stacking_regressor" works similarly, stacking together the
"gradient_boosting_regressor" and "random_forest_regressor" models
and a ``glmnet.ElasticNet(alpha=0, n_splits=4, max_iter=10000,
tol=1e-5, scoring='r2')``, combining them using
`NonNegativeLinearRegression
<https://github.com/civisanalytics/civisml-extensions>`_. The
estimators that are being stacked have the same names as the
associated pre-defined models, and the meta-estimator steps are named
"meta-estimator". Note that although default parameters are provided
for multilayer perceptron models, it is highly recommended that
multilayer perceptrons be run using hyperband.

Custom Models
-------------

You can create your own :class:`~sklearn.pipeline.Pipeline` instead of using one of
the pre-defined ones. Create the object and pass it as the ``model`` parameter
of the :class:`~civis.ml.ModelPipeline`. Your model must follow the
scikit-learn API, and you will need to include any dependencies as
:ref:`custom-dependencies` if they are not already installed in
CivisML. Preinstalled libraries available for your use include:

- `scikit-learn <http://scikit-learn.org>`_ v0.19.1
- `glmnet <https://github.com/civisanalytics/python-glmnet>`_ v2.0.0
- `xgboost <http://xgboost.readthedocs.io>`_ v0.6a2
- `muffnn <https://github.com/civisanalytics/muffnn>`_ v1.2.0
- `civisml-extensions <https://github.com/civisanalytics/civisml-extensions>`_ v.0.1.6

When you're assembling your own model, remember that you'll have to make certain that
either you add a missing value imputation step or that your data doesn't have any
missing values. If you're making a classification model, the model must have a ``predict_proba``
method. If the class you're using doesn't have a ``predict_proba`` method,
you can add one by wrapping it in a :class:`~sklearn.calibration.CalibratedClassifierCV`.

..  _custom-etl:

Custom ETL
----------

By default, CivisML pre-processes data using the
:class:`~civismlext.preprocessing.DataFrameETL` class, with ``cols_to_drop``
equal to the ``excluded_columns`` parameter. You can replace this
with your own ETL by creating an object of class
:class:`~sklearn.base.BaseEstimator` and passing it as the ``etl``
parameter during training.

By default, :class:`~civismlext.preprocessing.DataFrameETL`
automatically one-hot encodes all categorical columns in the
dataset. If you are passing a custom ETL estimator, you will have to
ensure that no categorical columns remain after the ``transform``
method is called on the dataset.

.. _hyperparam-search:

Hyperparameter Tuning
---------------------

You can tune hyperparamters using one of two methods: grid search or
hyperband. CivisML will perform grid search if you pass a dictionary
of hyperparameters to the ``cross_validation_parameters`` parameter, where the keys are
hyperparameter names, and the values are lists of hyperparameter
values to grid search over. You can run hyperparameter tuning in parallel by
setting the ``n_jobs``
parameter to however many jobs you would like to run in
parallel. By default, ``n_jobs`` is dynamically calculated based on
the resources available on your cluster, such that a modeling job will
never take up more than 90% of the cluster resources at once.

`Hyperband <https://arxiv.org/abs/1603.06560>`_
is an efficient approach to hyperparameter optimization, and
*recommended over grid search where possible*. CivisML will perform
hyperband optimization for a pre-defined model  if you pass the string
``'hyperband'`` to ``cross_validation_parameters``. Hyperband is
currently only supported for the following models:
``gradient_boosting_classifier``, ``random_forest_classifier``,
``extra_trees_classifier``, ``multilayer_perceptron_classifier``,
``stacking_classifier``, ``gradient_boosting_regressor``,
``random_forest_regressor``, ``extra_trees_regressor``,
``multilayer_perceptron_regressor``, and
``stacking_regressor``. Although hyperband is supported for stacking
models, stacking itself is a kind of model tuning, and the combination
of stacking and hyperband is likely too computationally intensive to
be useful in many cases.

Hyperband cannot be used to tune GLMs. For this reason, preset GLMs do
not have a hyperband option. Similarly, when
``cross_validation_parameters='hyperband'`` and the model is
``stacking_classifier`` or ``stacking_regressor``, only the GBT and
random forest steps of the stacker are tuned using hyperband.
Note that if you want to use hyperband with a custom model, you will need to
wrap your estimator in a
:class:`civismlext.hyperband.HyperbandSearchCV` estimator yourself.

CivisML runs pre-defined models with hyperband using the following
distributions:

+------------------------------------+--------------------+-----------------------------------------------------------------------------+
| Models                             | Cost               | Hyperband                                                                   |
|                                    | Parameter          | Distributions                                                               |
+====================================+====================+=============================================================================+
| | gradient_boosting_classifier     | | ``n_estimators`` | | ``max_depth: randint(low=1, high=5)``                                     |
| | gradient_boosting_regressor      | | ``min = 100,``   | | ``max_features: [None, 'sqrt', 'log2', 0.5, 0.3, 0.1, 0.05, 0.01]``       |
| | GBT step in stacking_classifier  | | ``max = 1000``   | | ``learning_rate: truncexpon(b=5, loc=.0003, scale=1./167.)``              |
| | GBT step in stacking_regressor   |                    |                                                                             |
+------------------------------------+--------------------+-----------------------------------------------------------------------------+
| | random_forest_classifier         | | ``n_estimators`` | | ``criterion: ['gini', 'entropy']``                                        |
| | random_forest_regressor          | | ``min = 100,``   | | ``max_features: truncexpon(b=10., loc=.01, scale=1./10.11)``              |
| | extra_trees_classifier           | | ``max = 1000``   | | ``max_depth: [1, 2, 3, 4, 6, 10]``                                  |
| | extra_trees_regressor            |                    |                                                                             |
| | RF step in stacking_classifier   |                    |                                                                             |
| | RF step in stacking_regressor    |                    |                                                                             |
+------------------------------------+--------------------+-----------------------------------------------------------------------------+
| | multilayer_perceptron_classifier | | ``n_epochs``     | | ``keep_prob: uniform()``                                                  |
| | multilayer_perceptron_regressor  | | ``min = 5,``     | | ``hidden_units: [(), (16,), (32,), (64,), (64, 64), (64, 64, 64),``       |
|                                    | | ``max = 50``     | |                  ``(128,), (128, 128), (128, 128, 128), (256,),``         |
|                                    |                    | |                  ``(256, 256), (256, 256, 256), (512, 256, 128, 64),``    |
|                                    |                    | |                  ``(1024, 512, 256, 128)]``                               |
|                                    |                    | | ``learning_rate: [1e-2, 2e-2, 5e-2, 8e-2, 1e-3, 2e-3, 5e-3, 8e-3, 1e-4]`` |
+------------------------------------+--------------------+-----------------------------------------------------------------------------+

The truncated exponential distribution for the gradient boosting
classifier and regressor was chosen to skew the distribution toward
small values, ranging between .0003 and .03, with a mean close to
.006. Similarly, the truncated exponential distribution for the random
forest and extra trees models skews toward small values, ranging
between .01 and 1, and with a mean close to .1.

.. _custom-dependencies:

Custom Dependencies
-------------------

Installing packages from PyPI is straightforward. You can specify a `dependencies`

argument to :class:`~civis.ml.ModelPipeline` which will install the
dependencies in your runtime environment. VCS support is also enabled
(see `docs
<https://pip.pypa.io/en/stable/reference/pip_install/#vcs-support>`_.)
Installing a remote git repository from, say, Github only requires passing the HTTPS
URL in the form of, for example, ``git+https://github.com/scikit-learn/scikit-learn``.

CivisML will run ``pip install [your package here]``. We strongly encourage you to pin
package versions for consistency. Example code looks like:

.. code-block:: python

  from civis.ml import ModelPipeline
  from pyearth import Earth
  deps = ['git+https://github.com/scikit-learn-contrib/py-earth.git@da856e11b2a5d16aba07f51c3c15cef5e40550c7']
  est = Earth()
  model = ModelPipeline(est, dependent_variable='age', dependencies=deps)
  train = model.train(table_name='donors.from_march', database_name='client')


Additionally, you can store a remote git host's API token in the Civis Platform as a
credential to use for installing private git repositores. For example, you can go to
Github at the ``https://github.com/settings/tokens`` URL, copy your token into the
password field of a credential, and pass the credential name to the ``git_token_name``
argument in :class:`~civis.ml.ModelPipeline`. This also works with other hosting services.
A simple example of how to do this with API looks as follows

.. code-block:: python


  import civis
  password = 'abc123'  # token copied from https://github.com/settings/tokens
  username = 'user123'  # Github username
  git_token_name = 'Github credential'

  client = civis.APIClient()
  credential = client.credentials.post(password=password,
                                       username=username,
                                       name=git_token_name,
                                       type="Custom")

  pipeline = civis.ml.ModelPipeline(..., git_token_name=git_token_name)


Note, installing private dependencies with submodules is not supported.


Asynchronous Execution
======================

All calls to a :class:`~civis.ml.ModelPipeline` object are non-blocking, i.e. they immediately
provide a result without waiting for the job in the Civis Platform to complete.
Calls to :meth:`civis.ml.ModelPipeline.train` and :meth:`civis.ml.ModelPipeline.predict` return
a :class:`~civis.ml.ModelFuture` object, which is a subclass of
:class:`~concurrent.futures.Future` from the Python standard library.
This behavior lets you train multiple models at once, or generate predictions
from models, while still doing other work while waiting for your jobs to complete.

The :class:`~civis.ml.ModelFuture` can find and retrieve outputs from your CivisML jobs,
such as trained :class:`~sklearn.pipeline.Pipeline` objects or out-of-sample predictions.
The :class:`~civis.ml.ModelFuture` only downloads outputs when you request them.


Model Persistence
=================

Civis Platform permanently stores all models, indexed by the job ID and the run ID
(also called a "build") of the training job. If you wish to use an existing
model, call :meth:`civis.ml.ModelPipeline.from_existing` with the job ID of the training job.
You can find the job ID with the :attr:`~civis.ml.ModelFuture.train_job_id`
attribute of a :class:`~civis.ml.ModelFuture`,
or by looking at the URL of your model on the
`Civis Platform models page <https://platform.civisanalytics.com/#/models>`_.
If the training job has multiple runs, you may also provide a run ID to select
a run other than the most recent.
You can list all model runs of a training job by calling
``civis.APIClient().jobs.get(train_job_id)['runs']``.
You may also store the :class:`~civis.ml.ModelPipeline` itself with the :mod:`pickle` module.


Examples
========

:class:`~concurrent.futures.Future` objects have the method
:meth:`~concurrent.futures.Future.add_done_callback`.
This is called as soon as the run completes. It takes a single argument, the
:class:`~concurrent.futures.Future` for the completed job.
You can use this method to chain jobs together:


.. code-block:: python

  from concurrent import futures
  from civis.ml import ModelPipeline
  import pandas as pd
  df = pd.read_csv('data.csv')
  training, predictions = [], []
  model = ModelPipeline('sparse_logistic', dependent_variable='type')
  training.append(model.train(df))
  training[-1].add_done_callback(lambda fut: predictions.append(model.predict(df)))
  futures.wait(training)  # Blocks until all training jobs complete
  futures.wait(predictions)  # Blocks until all prediction jobs complete


You can create and train multiple models at once to find the best approach
for solving a problem. For example:


.. code-block:: python

  from civis.ml import ModelPipeline
  algorithms = ['gradient_boosting_classifier', 'sparse_logistic', 'random_forest_classifier']
  pkey = 'person_id'
  depvar = 'likes_cats'
  models = [ModelPipeline(alg, primary_key=pkey, dependent_variable=depvar) for alg in algorithms]
  train = [model.train(table_name='schema.name', database_name='My DB') for model in models]
  aucs = [tr.metrics['roc_auc'] for tr in train]  # Code blocks here

..  _model-registration:

Registering Models Trained Outside of Civis
===========================================

Instead of using CivisML to train your model, you may train any
scikit-learn-compatible model outside of Civis Platform and use
:meth:`civis.ml.ModelPipeline.register_pretrained_model` to register it
as a CivisML model in Civis Platform. This will let you use Civis Platform
to make predictions using your model, either to take advantage of distributed
predictions on large datasets, or to create predictions as part of
a workflow or service in Civis Platform.

When registering a model trained outside of Civis Platform, you are
strongly advised to provide an ordered list of feature names used
for training. This will allow CivisML to ensure that tables of data
input for predictions have the correct features in the correct order.
If your model has more than one output, you should also provide a list
of output names so that CivisML knows how many outputs to expect and
how to name them in the resulting table of model predictions.

If your model uses dependencies which aren't part of the default CivisML
execution environment, you must provide them to the ``dependencies``
parameter of the :meth:`~civis.ml.ModelPipeline.register_pretrained_model`
function, just as with the :class:`~civis.ml.ModelPipeline` constructor.

.. _model-sharing:

Sharing Models
==============

Models produced by CivisML can't be shared directly through the Civis Platform
UI or API. The :module:`~civis.ml` namespace provides functions which will
let you share your CivisML models with other Civis Platform users.
To share your models, use the functions

- :func:`~civis.ml.put_models_shares_users`
- :func:`~civis.ml.put_models_shares_groups`
- :func:`~civis.ml.delete_models_shares_users`
- :func:`~civis.ml.delete_models_shares_groups`


Object reference
================

.. autoclass:: civis.ml.ModelPipeline
	       :members:


.. autoclass:: civis.ml.ModelFuture
	       :members:
	       :inherited-members:
