========
CodePack
========

.. image:: https://github.com/ihnokim/codepack/workflows/Test/badge.svg?event=push&branch=master
  :target: https://github.com/ihnokim/codepack/actions?query=workflow%3ATest+event%3Apush+branch%3Amaster
  :alt: Test Status
.. image:: https://coveralls.io/repos/github/ihnokim/codepack/badge.svg
  :target: https://coveralls.io/github/ihnokim/codepack
  :alt: Code Coverage
.. image:: https://img.shields.io/pypi/v/codepack
  :target: https://pypi.org/project/codepack/
  :alt: PyPI - Package Version
.. image:: https://img.shields.io/pypi/pyversions/codepack
  :target: https://pypi.org/project/codepack/
  :alt: PyPI - Python Version

CodePack is a Python package to easily make, run, and manage workflows.
You can write a task which is called Code with pure python code.
By simply making linkages between the Codes, you can easily build a workflow which is called CodePack.
The CodePack can be either executed or scheduled
(via `APScheduler <https://apscheduler.readthedocs.io/en/master/?badge=latest>`_)
with a set of arguments which is called ArgPack.
The Code, CodePack, and ArgPack support JSON serialization and deserialization,
so they can be saved to a file or external storage and recycled to create different combinations.

CodePack is one of the good choices to represent flexible workflows.
Business environments often require frequent changes to workflows to quickly meet volatile customer requirements.
CodePack provides various plugins to configure the whole system.
For example, file systems, MongoDB, and AWS S3 can be options
for transferring data between Codes or storing serialized objects.
You can easily setup the various components by modifying few lines in the configuration file
or adding some OS environment variables.

Simple is the best ;) but sometimes...
you may need helps from rich user interfaces and powerful utilities
to troubleshoot complex workflows and expand your system.
It is recommended to convert CodePacks
into the DAGs used in `Apache Airflow <https://airflow.apache.org/docs/apache-airflow/stable/>`_
especially when you deal with workflows that are mostly static and slowly changing.
CodePack is also planning to provide rich APIs and user interfaces, so please stay tuned!

The key features are:

- **Easy**: Designed to be easy to use with pure python code. There is nothing new to learn!
- **Simple**: Minimize the effort of configuring the entire system to your taste.
- **Responsive**: Get workflow results immediately or later by sync/async execution or scheduling
  right after making changes in the workflow and its arguments.
- **Transformable**: Convert almost everything into JSON, so you can easily extend its use to other solutions.

Requirements
------------

CodePack is tested with Python 3.6, 3.7, 3.8, and 3.9.

Installation
------------

Use :code:`pip install` command to install CodePack into your Python environment.
Check `PyPI <https://pypi.org/project/codepack/>`_ for more details.

.. code-block::

  $ pip install codepack

This will install CodePack with minimal dependencies.
You can activate full features by using :code:`pip install codepack[all]` command.

If you want to try sample apps provided in `apps <https://github.com/ihnokim/codepack/tree/master/apps>`_,
you need to install additional packages.

.. code-block::

  $ pip install codepack[all] jupyter uvicorn[standard] fastapi

To run unit tests, use following commands.

.. code-block::

  $ pip install pytest pytest-cov mongomock
  $ pytest --cov codepack tests

Example
-------

**1. Instantiate and run Code**

Just wrap a normal python function with Code and use it the way you are used to it.
A Code has its own id.
If you do not specify an id when creating a Code,
the given function name is automatically set to id.

.. code-block:: python

  from codepack import Code

  def add3(a, b=1, c=2):
    return a + b + c

  code = Code(add3)
  result = code(3, b=5)  # result == 10

In order to improve readability of the Code, you can use type annotations and docstrings.

Several classes in :code:`typing` module (e.g. :code:`Any`, :code:`Optional`, etc.) can be added to the annotations.
However, Code does not understand user-defined and 3rd party modules.
To use these modules in annotations, it is recommended to enclose them in quotation marks.
For example, use :code:`'pandas.core.frame.DataFrame'` and :code:`'numpy.ndarray'` instead of
:code:`DataFrame` and :code:`ndarray`.

The docstring in the function is stored in the :code:`description` attribute in the Code instance.
The longer, the better!

.. code-block:: python

  from typing import Optional, Any, Union

  def my_function(a: str, b: list, c: Optional['my_module.MyClass'] = None, **kwargs: Any) -> Union[str, dict]:
    """ You can find this message in code.description. """
    if c is None:
      return dict()
    else:
      return str()

  code = Code(my_function)
  print(code)  # Code(id: my_function, function: my_function, params: (a: str, b: list, c: Union[ForwardRef('my_module.MyClass'), NoneType] = None, **kwargs: Any) -> Union[str, dict])
  print(code.description)  # You can find this message in code.description.

**2. Link Codes**

You can create links between Codes with :code:`>>` operator and break links with :code:`//` operator.
The linkage represents the order of execution between Codes.
If you want to transfer the result of a preceding Code to the following Code,
use :code:`receive` method and :code:`<<` operator to represent the data dependency.

.. code-block:: python

  code1 >> code2 >> code3
  code3 >> [code4, code5]
  code3 // code5

  code2.receive('a') << code1
  code4.receive('c') << code3

**3. Make a CodePack**

Tag any one of the connected Codes to a CodePack in :code:`code`,
and specify the Code to return the final result in :code:`subscribe`.
If you don't specify the subscription, the CodePack returns :code:`None`.

.. code-block:: python

  from codepack import CodePack

  codepack = CodePack('my_codepack', code=code1, subscribe=code4)

**4. Set arguments and run CodePack**

Don't bother yourself to remember all argument specifications in Codes.
You can extract an empty ArgPack from the CodePack.
ArgPack even understands the default keyword arguments in Codes.
All you need to do is just fill out the blanks!

.. code-block:: python

  from codepack import ArgPack

  argpack = codepack.make_argpack()

  argpack['add2'](x=3, y=5)
  argpack['add3'](a=3)  # receive c from add2
  argpack['mul2'](x2=2)  # receive x1 from add3

  result = codepack(argpack)  # subscribe mul2

**5. Save and load**

You can save Code, CodePack, and ArgPack into somewhere you specified in configuration.
Each element has its own id, so it can be retrieved and reused by querying the id.

.. code-block:: python

  # Code
  code.save()
  code = Code.load('my_code')

  # CodePack
  codepack.save()
  codepack = CodePack.load('my_codepack')

  # ArgPack (1)
  codepack.save_argpack(argpack)  # codepack.id == argpack.id
  argpack = codepack.load_argpack('my_codepack')

  # ArgPack (2)
  argpack.save()
  argpack = ArgPack.load('my_argpack')

**6. JSON serialization and deserialization**

Code, CodePack, and ArgPack are interchangeable with JSON.
Use these guys everywhere!

.. code-block:: python

  code_json = code.to_json()
  code_py = Code.from_json(code_json)

  codepack_json = codepack.to_json()
  codepack_py = CodePack.from_json(codepack_json)

  argpack_json = argpack.to_json()
  argpack_py = ArgPack.from_json(argpack_json)

**7. Set configuration**

There are many plugins to manage Codes, CodePacks, and ArgPacks.
You can easily configure the plugins by using a
`configuration file <https://github.com/ihnokim/codepack/blob/master/config/sample.ini>`_
or adding some OS environment variables.
If you don't specify any configuration files,
CodePack uses the
`default configuration file <https://github.com/ihnokim/codepack/blob/master/codepack/utils/config/default/default.ini>`_
located at :code:`codepack/utils/config/default/default.ini`.
The default configuration file can be replaced with your custom configuration file
by adding an OS environment variable: :code:`CODEPACK_CONFIG_PATH=/path/to/your-configuration-file.ini`.

Let's say you want the following scenario:

- Save and load Codes through MongoDB.
- Exchange data between Codes through files.

Your configuration file should include:

.. code-block::

  [code]
  source = mongodb
  db = codepack
  collection = codes

  [delivery]
  source = file
  path = /data/delivery

  [mongodb]
  host = server1.ip,server2.ip,server3.ip
  port = 27017
  username = admin
  password = ***
  replicaSet = test

When the external service like MongoDB or AWS S3 is set to :code:`source`,
the connection information should also be included.

If it is too annoying to modify the configuration file,
you can overwrite configuration values with OS environment variables:
:code:`CODEPACK__<SECTION>__<KEY>=<VALUE>`.
For example, if the IP address of the MongoDB has changed to localhost,
set :code:`CODEPACK__MONGODB__HOST=localhost`.

The configuration is accessible from code via :code:`Config` and :code:`Default` classes.

.. code-block:: python

  from codepack import Config, Default
  from codepack.interfaces import MongoDB

  config = Config()
  mongodb_config = config.get_config('mongodb')
  mongodb = MongoDB(mongodb_config)
  document = mongodb.test_db.test_collection.find_one({'_id': 'test'})

  code_storage_service = Default.get_service('code', 'storage_service')
  delivery_service = Default.get_service('delivery', 'delivery_service')
  scheduler = Default.get_scheduler()
  logger = Default.get_logger()

If the default configuration bothers you, pass :code:`default=False` to :code:`get_config`.

.. code-block:: python

  import os

  os.environ['CODEPACK__SSH__CUSTOM_KEY'] = 'custom_value'
  config.get_config('ssh')  # {'ssh_host': 'localhost', 'ssh_port': '22', 'custom_key': 'custom_value'}
  config.get_config('ssh', default=False)  # {'custom_key': 'custom_value'}
  os.environ.pop('CODEPACK__SSH__CUSTOM_KEY', None)
  config.get_config('ssh', default=False)  # {}

**8. To use in other machines...**

A Code should be executable in isolated environments different from where it was created.
In order to avoid the python package dependency issue,
put import statements into the function.

.. code-block:: python

  def calc_numpy_array_sum(x):
    import numpy as np
    return np.sum(np.array(x))

This little trick is very important when setting up distributed systems
where each Code in a CodePack runs on a different machine.

**9. The other features**

For more information on schedulers, asynchronous workers, APIs, and other features,
please see Documentation and `Github <https://github.com/ihnokim/codepack>`_.

Source code
-----------

The source can be browsed at `Github <https://github.com/ihnokim/codepack>`_.

Contributing
------------

Want to help CodePack?
Feel free to use `Issues <https://github.com/ihnokim/codepack/issues>`_
and `Discussions <https://github.com/ihnokim/codepack/discussions>`_ to unleash your imagination!

License
-------

This project is licensed under the terms of the MIT license.
