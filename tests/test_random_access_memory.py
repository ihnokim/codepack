from codepack.interfaces.random_access_memory import RandomAccessMemory
from collections import OrderedDict
import pytest


def test_initialization_with_config():
    config1 = {'keep_order': 'True'}
    ram1 = RandomAccessMemory.get_instance(config=config1)
    assert ram1.keep_order is True
    assert isinstance(ram1.get_session(), OrderedDict)
    config2 = {'keep_order': 'False'}
    ram2 = RandomAccessMemory.get_instance(config=config2)
    assert ram2.keep_order is False
    assert not isinstance(ram2.get_session(), OrderedDict)


def test_initialization_with_invalid_config():
    with pytest.raises(AttributeError):
        _ = RandomAccessMemory.get_instance(config={'keep_order': True})
    with pytest.raises(TypeError):
        _ = RandomAccessMemory.get_instance(config={'keep_order': 'test'})


def test_initialization_with_lowercase_config():
    ram1 = RandomAccessMemory.get_instance(config={'keep_order': 'true'})
    assert ram1.keep_order is True
    ram2 = RandomAccessMemory.get_instance(config={'keep_order': 'false'})
    assert ram2.keep_order is False


@pytest.mark.parametrize("keep_order", [True, False])
def test_if_memory_is_empty_after_closing(keep_order):
    ram = RandomAccessMemory(keep_order=keep_order)
    mem = ram.get_session()
    mem['key'] = 'value'
    assert not ram.is_closed()
    assert len(mem) != 0
    ram.close()
    assert ram.is_closed()
    assert len(mem) == 0
