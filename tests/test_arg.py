from codepack.arg import Arg
from codepack.async_arg import AsyncArg
import pytest


test_params = ("arg_class", [Arg, AsyncArg])


@pytest.mark.parametrize(*test_params)
def test_empty_arg_serialization(arg_class):
    arg = arg_class()
    assert arg.to_dict() == {'_id': None,
                             'owner': None,
                             'description': None,
                             'name': None,
                             'version': None,
                             'args': [],
                             'kwargs': {}}


@pytest.mark.parametrize(*test_params)
def test_arg_serialization(arg_class):
    arg = arg_class(name='test-arg',
                    version='0.0.1',
                    owner='test-user',
                    description='test-description',
                    args=[1, 2, 3],
                    kwargs={'a': 5, 'b': 6})
    assert arg.to_dict() == {'_id': 'test-arg@0.0.1',
                             'owner': 'test-user',
                             'description': 'test-description',
                             'name': 'test-arg',
                             'version': '0.0.1',
                             'args': [1, 2, 3],
                             'kwargs': {'a': 5, 'b': 6}}


@pytest.mark.parametrize(*test_params)
def test_arg_deserialization(arg_class):
    arg1 = arg_class(name='test-arg',
                     version='0.0.1',
                     owner='test-user',
                     description='test-description',
                     args=[1, 2, 3],
                     kwargs={'a': 5, 'b': 6})
    serialized_arg = arg1.to_json()
    arg2 = arg_class.from_json(serialized_arg)
    assert arg1.name == arg2.name
    assert arg1.version == arg2.version
    assert arg1.owner == arg2.owner
    assert arg1.description == arg2.description
    assert arg1.args == arg2.args
    assert arg1.kwargs == arg2.kwargs


'''
def test_arg_from_code():
    code = Code(add2)
    arg = Arg.from_code(code=code)
    assert arg.to_dict() == {'_id': code.get_id(),
                             'owner': None,
                             'description': None,
                             'name': code.name,
                             'version': code.version,
                             'args': [],
                             'kwargs': {}}
'''


@pytest.mark.parametrize(*test_params)
def test_amending_kwargs(arg_class):
    arg = arg_class(name='test-arg',
                    version='0.0.1',
                    owner='test-user',
                    description='test-description',
                    args=[1, 2, 3],
                    kwargs={'a': 5, 'b': 6})
    assert arg.args == [1, 2, 3]
    assert arg.kwargs == {'a': 5, 'b': 6}
    assert arg['b'] == 6
    arg['b'] = 7
    assert arg['b'] == 7
    assert arg.args == [1, 2, 3]
    assert arg.kwargs == {'a': 5, 'b': 7}


@pytest.mark.parametrize(*test_params)
def test_adding_kwargs(arg_class):
    arg = arg_class(name='test-arg',
                    version='0.0.1',
                    owner='test-user',
                    description='test-description',
                    args=[1, 2, 3],
                    kwargs={'a': 5, 'b': 6})
    assert arg.args == [1, 2, 3]
    assert arg.kwargs == {'a': 5, 'b': 6}
    with pytest.raises(KeyError):
        arg['c']
    arg['c'] = 7
    assert arg['c'] == 7
    assert arg.args == [1, 2, 3]
    assert arg.kwargs == {'a': 5, 'b': 6, 'c': 7}


@pytest.mark.parametrize(*test_params)
def test_resetting_args_and_kwargs(arg_class):
    arg = arg_class(name='test-arg',
                    version='0.0.1',
                    owner='test-user',
                    description='test-description',
                    args=[1, 2, 3],
                    kwargs={'a': 5, 'b': 6})
    assert arg.args == [1, 2, 3]
    assert arg.kwargs == {'a': 5, 'b': 6}
    arg(10, 11, c=12, d=13)
    assert arg.args == [10, 11]
    assert arg.kwargs == {'c': 12, 'd': 13}


@pytest.mark.parametrize(*test_params)
def test_getting_kwargs_by_attr(arg_class):
    arg = arg_class(name='test-arg',
                    version='0.0.1',
                    owner='test-user',
                    description='test-description',
                    args=[1, 2, 3],
                    kwargs={'a': 5, 'b': 6})
    with pytest.raises(AttributeError):
        arg.a
