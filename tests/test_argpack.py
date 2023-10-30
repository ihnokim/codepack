from codepack.arg import Arg
from codepack.async_arg import AsyncArg
from codepack.argpack import ArgPack
from codepack.async_argpack import AsyncArgPack
import pytest


test_params = ("arg_class,argpack_class", [(Arg, ArgPack),
                                           (AsyncArg, AsyncArgPack),
                                           (Arg, AsyncArgPack),
                                           (AsyncArg, ArgPack)])


@pytest.mark.parametrize(*test_params)
def test_argpack_initialization(arg_class, argpack_class):
    arg1 = arg_class(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    arg2 = arg_class(name='test-arg2', version='0.0.2', args=[5, 6], kwargs={'c': 7, 'd': 8})
    argpack = argpack_class(args=[arg1, arg2])
    assert argpack.version is None
    assert argpack.owner is None
    assert argpack.description is None
    assert len(argpack.args) == 2
    assert set(argpack.args.keys()) == {'test-arg1@0.0.1', 'test-arg2@0.0.2'}


@pytest.mark.parametrize(*test_params)
def test_argpack_serialization(arg_class, argpack_class):
    arg1 = arg_class(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    arg2 = arg_class(name='test-arg2', version='0.0.2', args=[5, 6], kwargs={'c': 7, 'd': 8})
    argpack = argpack_class(args=[arg1, arg2])
    argpack.map_code(code_id='test-code', arg_id=arg1.get_id())
    serialized_argpack = argpack.to_dict()
    assert set(serialized_argpack.keys()) == {'_id', 'name', 'version', 'owner', 'description', 'args', 'code_map'}
    assert serialized_argpack['version'] is None
    assert serialized_argpack['owner'] is None
    assert serialized_argpack['description'] is None
    assert type(serialized_argpack['args']) == list
    assert len(serialized_argpack['args']) == 2
    assert sorted(serialized_argpack['args'], key=lambda x: x['_id']) == \
        sorted([arg1.to_dict(), arg2.to_dict()], key=lambda x: x['_id'])
    assert serialized_argpack['code_map'] == argpack.get_code_map() == {'test-code': arg1.get_id()}


@pytest.mark.parametrize(*test_params)
def test_argpack_deserialization(arg_class, argpack_class):
    arg1 = arg_class(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    arg2 = arg_class(name='test-arg2', version='0.0.2', args=[5, 6], kwargs={'c': 7, 'd': 8})
    argpack1 = argpack_class(args=[arg1, arg2])
    argpack1.map_code(code_id='test-code', arg_id=arg1.get_id())
    serialized_argpack = argpack1.to_json()
    argpack2 = argpack_class.from_json(serialized_argpack)
    assert argpack1.name == argpack2.name
    assert argpack1.version == argpack2.version
    assert argpack1.owner == argpack2.owner
    assert argpack1.description == argpack2.description
    assert len(argpack2.args.keys()) == 2
    assert 'test-arg1@0.0.1' in argpack2.args.keys()
    assert 'test-arg2@0.0.2' in argpack2.args.keys()
    assert argpack1.args['test-arg1@0.0.1'] != argpack2.args['test-arg1@0.0.1']
    assert argpack1.args['test-arg1@0.0.1'].to_dict() == argpack2.args['test-arg1@0.0.1'].to_dict()
    assert argpack1.args['test-arg2@0.0.2'] != argpack2.args['test-arg2@0.0.2']
    assert argpack1.args['test-arg2@0.0.2'].to_dict() == argpack2.args['test-arg2@0.0.2'].to_dict()
    assert argpack1.get_code_map() == argpack2.get_code_map()


@pytest.mark.parametrize(*test_params)
def test_get_arg(arg_class, argpack_class):
    arg1 = arg_class(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    arg2 = arg_class(name='test-arg2', version='0.0.2', args=[5, 6], kwargs={'c': 7, 'd': 8})
    argpack = argpack_class(args=[arg1, arg2])
    assert argpack['test-arg1@0.0.1'] == arg1
    assert argpack['test-arg2@0.0.2'] == arg2


@pytest.mark.parametrize(*test_params)
def test_argpack_initialization_without_any_arg(arg_class, argpack_class):
    with pytest.raises(ValueError):
        _ = argpack_class(args=[])


@pytest.mark.parametrize(*test_params)
def test_argpack_iterator(arg_class, argpack_class):
    arg1 = arg_class(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    arg2 = arg_class(name='test-arg2', version='0.0.2', args=[5, 6], kwargs={'c': 7, 'd': 8})
    arg3 = arg_class(name='test-arg3', version='0.0.3', args=[9, 10], kwargs={'c': 11, 'd': 12})
    argpack = argpack_class(args=[arg1, arg2])
    args = set()
    for arg in argpack:
        args.add(arg.get_id())
    assert args == {arg1.get_id(), arg2.get_id()}
    assert arg1 in argpack
    assert arg2 in argpack
    assert arg3 not in argpack


@pytest.mark.parametrize(*test_params)
def test_get_arg_by_code_id_when_nothing_is_mapped(arg_class, argpack_class):
    arg = arg_class(name='test-arg')
    argpack = argpack_class(args=[arg])
    assert argpack.get_arg_by_code_id(code_id='test-code') is None


@pytest.mark.parametrize(*test_params)
def test_mapping_code_when_arg_is_not_included_in_argpack(arg_class, argpack_class):
    arg = arg_class(name='test-arg')
    argpack = argpack_class(args=[arg])
    with pytest.raises(KeyError):
        argpack.map_code(code_id='test-code', arg_id='arg')


@pytest.mark.parametrize(*test_params)
def test_code_mapping(arg_class, argpack_class):
    arg1 = arg_class(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    arg2 = arg_class(name='test-arg2', version='0.0.2', args=[5, 6], kwargs={'a': 7, 'b': 8})
    argpack = argpack_class(args=[arg1, arg2])
    assert argpack.get_code_map() == {}
    argpack.map_code(code_id='test-code1', arg_id=arg1.get_id())
    assert argpack.get_code_map() == {'test-code1': arg1.get_id()}
    argpack.map_code(code_id='test-code2', arg_id=arg2.get_id())
    assert argpack.get_code_map() == {'test-code1': arg1.get_id(), 'test-code2': arg2.get_id()}
    argpack.unmap_code('test-code1')
    assert argpack.get_code_map() == {'test-code2': arg2.get_id()}
    argpack.unmap_code('test-code2')
    assert argpack.get_code_map() == {}
    argpack.unmap_code('test-code3')
    assert argpack.get_code_map() == {}
