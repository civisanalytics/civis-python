****************
Machine Learning
****************

CivisML uses the Civis Platform to train machine learning models
and parallelize their predictions over large datasets.
It contains best-practice models for general-purpose classification
and regression modeling as well as model quality evaluations and
visualizations. All CivisML models use `scikit-learn <http://scikit-learn.org/>`_
for interoperability with other platforms and to allow you to leverage
resources in the open-source software community when creating machine learning models.


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
Note that whichever option you chose, CivisML will pre-process your data to
one-hot-encode categorical features (the non-numerical columns) to binary indicator columns
before sending the features to the :class:`~sklearn.pipeline.Pipeline`.

       
Pre-Defined Models
------------------

You can use the following pre-defined models with CivisML.
All models start by imputing missing values with the mean of non-null
values in a column. The "sparse_*" models include a LASSO regression step
(using the `glmnet <https://github.com/civisanalytics/python-glmnet>`_ package)
to do feature selection before passing data to the final model.
In some models, CivisML uses default parameters different from those in scikit-learn,
as indicated in the "Altered Defaults" column. All models also have ``random_state=42``.

=============================  ================    ==================================================================================================================================   ==================================
Name                           Model Type          Algorithm                                                                                                                            Altered Defaults
=============================  ================    ==================================================================================================================================   ==================================
sparse_logistic                classification      `LogisticRegression <http://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html>`_                ``C=499999950, tol=1e-08``
gradient_boosting_classifier   classification      `GradientBoostingClassifier <http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.GradientBoostingClassifier.html>`_    ``n_estimators=500, max_depth=2``
random_forest_classifier       classification      `RandomForestClassifier <http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestClassifier.html>`_            ``n_estimators=500``

extra_trees_classifier         classification      `ExtraTreesClassifier <http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.ExtraTreesClassifier.html>`_                ``n_estimators=500``
sparse_linear_regressor        regression          `LinearRegression <http://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LinearRegression.html>`_ 
sparse_ridge_regressor         regression          `Ridge <http://scikit-learn.org/stable/modules/generated/sklearn.linear_model.Ridge.html>`_ 
gradient_boosting_regressor    regression          `GradientBoostingRegressor <http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.GradientBoostingRegressor.html>`_      ``n_estimators=500, max_depth=2``
random_forest_regressor        regression          `RandomForestRegressor <http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestRegressor.html>`_              ``n_estimators=500``
extra_trees_regressor          regression          `ExtraTreesRegressor <http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.ExtraTreesRegressor.html>`_                  ``n_estimators=500``
=============================  ================    ==================================================================================================================================   ==================================


Custom Models
-------------

You can create your own :class:`~sklearn.pipeline.Pipeline` instead of using one of
the pre-defined ones. Create the object and pass it as the ``model`` parameter
of the :class:`~civis.ml.ModelPipeline`. Your model must be built from libraries which CivisML
recognizes. You can use code from

- `scikit-learn <http://scikit-learn.org>`_ v0.18.1
- `glmnet <https://github.com/civisanalytics/python-glmnet>`_ v2.0.0
- `xgboost <http://xgboost.readthedocs.io>`_ v0.6a2
- `muffnn <https://github.com/civisanalytics/muffnn>`_ v1.1.1

When you're assembling your own model, remember that you'll have to make certain that
either you add a missing value imputation step or that your data doesn't have any
missing values. If you're making a classification model, the model must have a ``predict_proba``
method. If the class you're using doesn't have a ``predict_proba`` method,
you can add one by wrapping it in a :class:`~sklearn.calibration.CalibratedClassifierCV`.


Custom Dependencies
-------------------

Installing packages from PyPI is straightforward. You can specify a `dependencies`
argument to `~civis.ml.ModelPipeline` which will install the dependencies in your runtime
environment. VCS support is also enabled (see [docs](https://pip.pypa.io/en/stable/reference/pip_install/#vcs-support).)
Installing a remote git repository from, say, Github only requires passing the HTTPS 
URL in the form of, for example, `git+https://github.com/scikit-learn/scikit-learn`.

CivisML will run `pip install [your package here]`. We strongly encourage you to pin
package versions for consistency. Example code looks like:::

  from civis.ml import ModelPipeline
  from pyearth import Earth
  deps = ['git+https://github.com/scikit-learn-contrib/py-earth.git@da856e11b2a5d16aba07f51c3c15cef5e40550c7']
  est = Earth()
  model = ModelPipeline(est, dependent_variable='age', dependencies=deps)
  train = model.train(table_name='donors.from_march', database_name='client')

Additionally, you can store a remote git host's API token in the Civis Platform as a
credential to use for installing private git repositores. For example, you can go to
Github at the `https://github.com/settings/tokens` URL, copy your token into the
password field of a credential, and pass the credential name to the `git_token_name`
argument in `~civis.ml.ModelPipeline`. This also works with other hosting services.
A simple example of how to do this with API looks as follows::

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
You can use this method to chain jobs together::

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
for solving a problem. For example::

  from civis.ml import ModelPipeline
  algorithms = ['gradient_boosting_classifier', 'sparse_logistic', 'random_forest_classifier']
  pkey = 'person_id'
  depvar = 'likes_cats'
  models = [ModelPipeline(alg, primary_key=pkey, dependent_variable=depvar) for alg in algorithms]
  train = [model.train(table_name='schema.name', database_name='My DB') for model in models]
  aucs = [tr.metrics['roc_auc'] for tr in train]  # Code blocks here

Optional dependencies
=====================

You do not need any external libraries installed to use CivisML, but
the following pip-installable dependencies enhance the capabilities of the
:class:`~civis.ml.ModelPipeline`:

- pandas
- scikit-learn
- joblib
- glmnet
- pubnub

Install :mod:`pandas` if you wish to download tables of predictions.
You can also model on :class:`~pandas.DataFrame` objects in your interpreter.

If you wish to use custom models or download trained models,
you'll need scikit-learn installed.

We use the ``joblib`` library to move scikit-learn models back and forth from Platform.
Install it if you wish to use custom models or download trained models.

The "sparse_logistic", "sparse_linear_regressor", and "sparse_ridge_regressor" models
all use the public Civis Analytics ``glmnet`` library. Install it if you wish to download
a model created from one of these pre-defined models.

If you install ``pubnub``, the Civis Platform API client can use the notifications endpoint instead of
polling for job completion. This gives faster results and uses fewer API calls.

Object reference
================

.. autoclass:: civis.ml.ModelPipeline
	       :members:


.. autoclass:: civis.ml.ModelFuture
	       :members:
	       :inherited-members:
