from codepack.utils.double_key_map import DoubleKeyMap
import pytest


def test_initialization_with_map():
    map = {'a->b': None, 'c->d': 'e'}
    dkm = DoubleKeyMap(map=map)
    assert set(dkm.keys()) == {('a', 'b'), ('c', 'd')}


def test_initialization_with_wrong_delimiter():
    map = {'a@b': 1, 'c@d': 2}
    with pytest.raises(ValueError):
        _ = DoubleKeyMap(map=map)
    dkm = DoubleKeyMap(map=map, delimiter='@')
    assert set(dkm.keys()) == {('a', 'b'), ('c', 'd')}


def test_removing_item_by_key():
    map = {'a@b': 1, 'c@d': 2}
    dkm = DoubleKeyMap(map=map, delimiter='@')
    dkm.remove(key1='c', key2='d')
    assert set(dkm.keys()) == {('a', 'b')}


def test_iteration():
    map = {'a@b': 1, 'c@d': 2}
    dkm = DoubleKeyMap(map=map, delimiter='@')
    assert {k for k in dkm} == {'a@b', 'c@d'}
    assert 'c@d' in dkm
