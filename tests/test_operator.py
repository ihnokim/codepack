from codepack.code import Code
from codepack.codepack import CodePack
from codepack.arg import Arg
from codepack.argpack import ArgPack
from tests import add2, mul2, add3, get_codepack_from_code
import pytest


def test_if_shallow_link_is_added_correctly():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    code1 > code2
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 0
    assert list(code1.downstream)[0] == code2
    assert list(code2.upstream)[0] == code1


def test_if_reversed_shallow_link_is_added_correctly():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    code2 < code1
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 0
    assert list(code1.downstream)[0] == code2
    assert list(code2.upstream)[0] == code1


def test_if_deep_link_is_added_correctly():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    code1 >> code2
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 0
    code1 >> code2 | 'a'
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 1


def test_if_reversed_deep_link_is_added_correctly():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    code2 << code1
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 0
    code2 << code1 | 'a'
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 1


def test_if_dependency_is_added_when_param_is_set():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    dependency = code1 >> code2
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 0
    dependency | 'a'
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 1
    assert code1 == code2.dependencies['a']


def test_if_reversed_dependency_is_added_when_param_is_set():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    dependency = code2 << code1
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 0
    dependency | 'a'
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 1
    assert code1 == code2.dependencies['a']


def test_if_chaining_shallow_link_works_correctly():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    _ = CodePack(codes=[code2])
    code1 > code2 > code3
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 1
    assert len(code3.upstream) == 1
    assert len(code3.downstream) == 0


def test_if_reversed_chaining_shallow_link_works_correctly():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    _ = CodePack(codes=[code2])
    code3 < code2 < code1
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 1
    assert len(code3.upstream) == 1
    assert len(code3.downstream) == 0


def test_if_complex_chaining_shallow_link_works_correctly1():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    _ = CodePack(codes=[code2])
    code1 > code2 < code3
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 2
    assert len(code2.downstream) == 0
    assert len(code3.upstream) == 0
    assert len(code3.downstream) == 1


def test_if_complex_chaining_shallow_link_works_correctly2():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    _ = CodePack(codes=[code2])
    code1 < code2 > code3
    assert len(code1.upstream) == 1
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 2
    assert len(code3.upstream) == 1
    assert len(code3.downstream) == 0


def test_if_shallow_link_with_iterable_works_correctly():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    _ = CodePack(codes=[code2])
    code1 > [code2, code3]
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 2
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(code3.upstream) == 1
    assert len(code3.downstream) == 0


def test_if_reversed_shallow_link_with_iterable_works_correctly():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    _ = CodePack(codes=[code2])
    code1 < [code2, code3]
    assert len(code1.upstream) == 2
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 1
    assert len(code3.upstream) == 0
    assert len(code3.downstream) == 1


def test_if_shallow_link_removed_correctly1():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    code1 > code2
    code1 / code2
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 0


def test_if_shallow_link_removed_correctly2():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    code1 > code2
    code1 // code2
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 0


def test_if_deep_link_removed_correctly1():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    code1 >> code2 | 'a'
    code1 / code2
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 0


def test_if_deep_link_removed_correctly2():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    code1 >> code2 | 'a'
    code1 // code2
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 0


def test_if_reversed_shallow_link_is_not_removed1():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    code1 > code2
    code2 / code1
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 0


def test_if_reversed_shallow_link_is_not_removed2():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    code1 > code2
    code2 // code1
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 0


def test_if_reversed_deep_link_is_not_removed1():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    code1 >> code2 | 'a'
    code2 / code1
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 1


def test_if_reversed_deep_link_is_not_removed2():
    code1 = Code(add2)
    code2 = Code(mul2)
    _ = CodePack(codes=[code1])
    code1 >> code2 | 'a'
    code2 // code1
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 1
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(code1.dependencies.keys()) == 0
    assert len(code2.dependencies.keys()) == 1


def test_if_chaining_shallow_link_is_removed1():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    _ = CodePack(codes=[code2])
    code1 > code2 > code3
    code1 / code2 / code3
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 0
    assert len(code3.upstream) == 0
    assert len(code3.downstream) == 0


def test_if_chaining_shallow_link_is_removed2():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    _ = CodePack(codes=[code2])
    code1 > code2 > code3
    code1 // code2 // code3
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 0
    assert len(code3.upstream) == 0
    assert len(code3.downstream) == 0


def test_if_shallow_link_with_iterable_is_removed1():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    _ = CodePack(codes=[code2])
    code1 > [code2, code3]
    code1 / [code2, code3]
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 0
    assert len(code3.upstream) == 0
    assert len(code3.downstream) == 0


def test_if_shallow_link_with_iterable_is_removed2():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    _ = CodePack(codes=[code2])
    code1 > [code2, code3]
    code1 // [code2, code3]
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 0
    assert len(code3.upstream) == 0
    assert len(code3.downstream) == 0


def test_if_complex_chaining_shallow_link_is_removed_correctly1():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    _ = CodePack(codes=[code2])
    code1 < code2 > code3
    code1 / code2 / code3
    assert len(code1.upstream) == 1
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 1
    assert len(code3.upstream) == 0
    assert len(code3.downstream) == 0


def test_if_complex_chaining_shallow_link_is_removed_correctly2():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    _ = CodePack(codes=[code2])
    code1 < code2 > code3
    code1 // code2 // code3
    assert len(code1.upstream) == 1
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 1
    assert len(code3.upstream) == 0
    assert len(code3.downstream) == 0


def test_single_code_addition_to_codepack():
    code1 = Code(add2)
    code2 = Code(mul2)
    codepack = CodePack(codes=[code1])
    codepack + code2
    assert len(codepack.codes) == 2
    assert code1.get_id() in codepack.codes.keys()
    assert code2.get_id() in codepack.codes.keys()
    assert get_codepack_from_code(code=code1) == codepack
    assert get_codepack_from_code(code=code2) == codepack


def test_single_code_removal_from_codepack():
    code1 = Code(add2)
    code2 = Code(mul2)
    codepack = CodePack(codes=[code1])
    codepack + code2
    codepack - code2
    assert code1.get_id() in codepack.codes.keys()
    assert code2.get_id() not in codepack.codes.keys()
    assert get_codepack_from_code(code=code1) == codepack
    assert get_codepack_from_code(code=code2) is None


def test_multiple_code_addition_to_codepack():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    codepack = CodePack(codes=[code1])
    codepack + code2 + code3
    assert len(codepack.codes) == 3
    assert code1.get_id() in codepack.codes.keys()
    assert code2.get_id() in codepack.codes.keys()
    assert code3.get_id() in codepack.codes.keys()
    assert get_codepack_from_code(code=code1) == codepack
    assert get_codepack_from_code(code=code2) == codepack
    assert get_codepack_from_code(code=code3) == codepack


def test_multiple_code_removal_from_codepack1():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    codepack = CodePack(codes=[code1])
    codepack + code2 + code3
    codepack - code2
    assert code1.get_id() in codepack.codes.keys()
    assert code2.get_id() not in codepack.codes.keys()
    assert code3.get_id() in codepack.codes.keys()
    assert get_codepack_from_code(code=code1) == codepack
    assert get_codepack_from_code(code=code2) is None
    assert get_codepack_from_code(code=code3) == codepack


def test_multiple_code_removal_from_codepack2():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    codepack = CodePack(codes=[code1])
    codepack + code2 + code3
    codepack - code2 - code1
    assert code1.get_id() not in codepack.codes.keys()
    assert code2.get_id() not in codepack.codes.keys()
    assert code3.get_id() in codepack.codes.keys()
    assert get_codepack_from_code(code=code1) is None
    assert get_codepack_from_code(code=code2) is None
    assert get_codepack_from_code(code=code3) == codepack


def test_removing_final_code_from_codepack1():
    code = Code(add2)
    codepack = CodePack(codes=[code])
    codepack - code
    assert get_codepack_from_code(code=code) is None
    assert len(codepack.codes) == 0


def test_removing_final_code_from_codepack2():
    code1 = Code(add2)
    code2 = Code(mul2)
    code1 > code2
    codepack = CodePack(codes=[code1, code2])
    code1 / code2
    codepack - code1 - code2
    assert get_codepack_from_code(code=code1) is None
    assert get_codepack_from_code(code=code2) is None
    assert len(codepack.codes) == 0


def test_removing_wrong_code_from_codepack():
    code1 = Code(add2)
    code2 = Code(mul2)
    codepack1 = CodePack(codes=[code1])
    codepack2 = CodePack(codes=[code2])
    with pytest.raises(KeyError):
        codepack2 - code1
    codepack1 - code1
    assert len(codepack1.codes) == 0
    assert len(codepack2.codes) == 1


def test_arg_addition_to_argpack():
    arg1 = Arg(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    arg2 = Arg(name='test-arg2', version='0.0.2', args=[5, 6], kwargs={'c': 7, 'd': 8})
    argpack = ArgPack(args=[arg1])
    assert len(argpack.args) == 1
    argpack + arg2
    assert len(argpack.args) == 2
    assert set(argpack.args.keys()) == {'test-arg1@0.0.1', 'test-arg2@0.0.2'}


def test_arg_subtraction_to_argpack():
    arg1 = Arg(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    arg2 = Arg(name='test-arg2', version='0.0.2', args=[5, 6], kwargs={'c': 7, 'd': 8})
    argpack = ArgPack(args=[arg1, arg2])
    assert len(argpack.args) == 2
    argpack - arg1
    assert len(argpack.args) == 1
    assert set(argpack.args.keys()) == {'test-arg2@0.0.2'}


def test_multiple_arg_addition_to_argpack():
    arg1 = Arg(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    arg2 = Arg(name='test-arg2', version='0.0.2', args=[5, 6], kwargs={'c': 7, 'd': 8})
    arg3 = Arg(name='test-arg3', version='0.0.3', args=[9, 10], kwargs={'c': 11, 'd': 12})
    argpack = ArgPack(args=[arg1])
    argpack + arg2 + arg3
    assert len(argpack.args) == 3
    assert set(argpack.args.keys()) == {'test-arg1@0.0.1', 'test-arg2@0.0.2', 'test-arg3@0.0.3'}


def test_multiple_arg_subtraction_from_argpack():
    arg1 = Arg(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    arg2 = Arg(name='test-arg2', version='0.0.2', args=[5, 6], kwargs={'c': 7, 'd': 8})
    arg3 = Arg(name='test-arg3', version='0.0.3', args=[9, 10], kwargs={'c': 11, 'd': 12})
    argpack = ArgPack(args=[arg1, arg2, arg3])
    argpack - arg1 - arg2
    assert len(argpack.args) == 1
    assert set(argpack.args.keys()) == {'test-arg3@0.0.3'}


def test_applying_multiple_arg_operations_to_argpack():
    arg1 = Arg(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    arg2 = Arg(name='test-arg2', version='0.0.2', args=[5, 6], kwargs={'c': 7, 'd': 8})
    arg3 = Arg(name='test-arg3', version='0.0.3', args=[9, 10], kwargs={'c': 11, 'd': 12})
    argpack = ArgPack(args=[arg1])
    argpack + arg3 - arg1 + arg2
    assert len(argpack.args) == 2
    assert set(argpack.args.keys()) == {'test-arg2@0.0.2', 'test-arg3@0.0.3'}


def test_meaningless_arg_addition_to_argpack():
    arg1 = Arg(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    argpack = ArgPack(args=[arg1])
    argpack + arg1
    assert len(argpack.args) == 1
    assert set(argpack.args.keys()) == {'test-arg1@0.0.1'}


def test_meaningless_arg_subtraction_from_argpack():
    arg1 = Arg(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    arg2 = Arg(name='test-arg2', version='0.0.2', args=[5, 6], kwargs={'c': 7, 'd': 8})
    argpack = ArgPack(args=[arg1])
    argpack - arg2
    assert len(argpack.args) == 1
    assert set(argpack.args.keys()) == {'test-arg1@0.0.1'}


def test_forbidden_arg_subtraction_from_argpack():
    arg1 = Arg(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    argpack = ArgPack(args=[arg1])
    with pytest.raises(ValueError):
        argpack - arg1
    assert len(argpack.args) == 1
    assert set(argpack.args.keys()) == {'test-arg1@0.0.1'}


def test_if_error_occurs_during_applying_multiple_arg_operations_to_argpack():
    arg1 = Arg(name='test-arg1', version='0.0.1', args=[1, 2], kwargs={'a': 3, 'b': 4})
    arg2 = Arg(name='test-arg2', version='0.0.2', args=[5, 6], kwargs={'c': 7, 'd': 8})
    arg3 = Arg(name='test-arg3', version='0.0.3', args=[9, 10], kwargs={'c': 11, 'd': 12})
    argpack = ArgPack(args=[arg1])
    with pytest.raises(ValueError):
        argpack - arg1 + arg2 + arg3
    assert len(argpack.args) == 1
    assert set(argpack.args.keys()) == {'test-arg1@0.0.1'}


def test_deep_link_to_iterable_obejcts():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    params = code1 >> [code2, code3]
    assert len(params) == 2
    assert len(code1.upstream) == 0
    assert len(code1.downstream) == 2
    assert len(code2.upstream) == 1
    assert len(code2.downstream) == 0
    assert len(code3.upstream) == 1
    assert len(code3.downstream) == 0
    assert len(code1.dependencies) == 0
    assert len(code2.dependencies) == 0
    assert len(code3.dependencies) == 0
    params[0] | 'a'
    params[1] | 'b'
    assert len(code1.dependencies) == 0
    assert len(code2.dependencies) == 1
    assert 'a' in code2.dependencies
    assert len(code3.dependencies) == 1
    assert 'b' in code3.dependencies


def test_reversed_deep_link_to_iterable_objects():
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    params = code1 << [code2, code3]
    assert len(params) == 2
    assert len(code1.upstream) == 2
    assert len(code1.downstream) == 0
    assert len(code2.upstream) == 0
    assert len(code2.downstream) == 1
    assert len(code3.upstream) == 0
    assert len(code3.downstream) == 1
    assert len(code1.dependencies) == 0
    assert len(code2.dependencies) == 0
    assert len(code3.dependencies) == 0
    params[0] | 'a'
    params[1] | 'b'
    assert len(code1.dependencies) == 2
    assert 'a' in code1.dependencies
    assert 'b' in code1.dependencies
    assert len(code2.dependencies) == 0
    assert len(code3.dependencies) == 0
