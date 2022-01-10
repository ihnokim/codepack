from codepack.utils.dependency_state import DependencyState


def test_eq(default_os_env):
    assert DependencyState.READY == 'READY'
    assert DependencyState.READY != DependencyState.NOT_READY
    assert DependencyState.READY == 0
