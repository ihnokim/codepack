from codepack.dependency import DependencyState


def test_eq():
    assert DependencyState.RESOLVED == 'RESOLVED'
    assert DependencyState.RESOLVED != DependencyState.PENDING
    assert DependencyState.RESOLVED == 0
