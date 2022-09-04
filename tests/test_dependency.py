from codepack import Dependency


def test_eq():
    dependency1 = Dependency(param='a', id='add3', serial_number='1234')
    dependency2 = Dependency(param='a', id='add3', serial_number='1234')
    assert dependency1 == dependency2
    dependency_dict2 = dependency2.to_dict()
    assert dependency1 == dependency_dict2
    dependency2.param = 'b'
    assert dependency1 != dependency2
    assert dependency2 != dependency_dict2


def test_dependency_to_dict():
    dependency = Dependency(param='a', id='add2', serial_number='1234')
    d = dependency.to_dict()
    assert set(d.keys()) == {'id', 'serial_number', 'param'}
