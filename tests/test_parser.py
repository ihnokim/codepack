import pytest
from codepack.utils.parser import Parser


def test_parse_bool():
    assert Parser.parse_bool('True') is True
    assert Parser.parse_bool('true') is True
    assert Parser.parse_bool('TrUe') is True
    assert Parser.parse_bool('False') is False
    assert Parser.parse_bool('false') is False
    assert Parser.parse_bool('falSe') is False
    with pytest.raises(TypeError):
        Parser.parse_bool('frue')


def test_parse_list():
    assert Parser.parse_list('test1,test2') == ['test1', 'test2']
    assert Parser.parse_list('test1, test2') == ['test1', 'test2']
    assert Parser.parse_list(' tes t1, test2  ') == ['tes t1', 'test2']


def test_parse_none():
    assert Parser.parse_none('None') is None
    assert Parser.parse_none('none') is None
    assert Parser.parse_none('NoNe') is None
    assert Parser.parse_none('Null') is None
    assert Parser.parse_none('null') is None
    assert Parser.parse_none('NuLl') is None
    with pytest.raises(TypeError):
        Parser.parse_none('noll')


def test_parse_int():
    assert Parser.parse_int('123') == 123
    with pytest.raises(ValueError):
        Parser.parse_int('abc')


def test_parse_float():
    assert Parser.parse_float('12.3') == 12.3
    with pytest.raises(ValueError):
        Parser.parse_float('abc')


def test_parse_tuple():
    assert Parser.parse_tuple('a,b, c') == ('a', 'b', 'c')
    assert Parser.parse_tuple('a,2') == ('a', '2')


def test_parse_set():
    assert Parser.parse_set('a,b, c') == {'a', 'b', 'c'}
    assert Parser.parse_set('a,b,b, c , d') == {'a', 'b', 'c', 'd'}
