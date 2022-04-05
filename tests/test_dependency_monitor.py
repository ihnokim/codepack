from codepack import Code, DependencyMonitor
from tests import *
import time


def load_and_run_code_from_snapshot(snapshot):
    code = Code.from_snapshot(snapshot)
    code(*snapshot['args'], **snapshot['kwargs'])


def load_and_run_code_from_snapshot_and_raise_exception(snapshot):
    code = Code.from_snapshot(snapshot)
    code(*snapshot['args'], **snapshot['kwargs'])
    raise KeyboardInterrupt


def test_dependency_monitor(default_os_env):
    dm = DependencyMonitor(callback=load_and_run_code_from_snapshot_and_raise_exception, interval=0.5, background=False)
    c1 = Code(add3)
    c2 = Code(mul2)
    c3 = Code(combination)
    c4 = Code(linear)
    c1 >> c3
    c2 >> c3
    c3 >> c4
    c3.receive('c') << c1
    c3.receive('d') << c2
    c4.receive('c') << c3
    c1.save()
    c2.save()
    c3.save()
    c4.save()
    c3(2, b=3)
    c4(3, 6)
    c2(3, 5)
    c1(1, 2, 3)
    assert c1.get_state() == 'TERMINATED'
    assert c2.get_state() == 'TERMINATED'
    assert c3.get_state() == 'WAITING'
    assert c4.get_state() == 'WAITING'
    dm.start()
    assert c1.get_state() == 'TERMINATED'
    assert c2.get_state() == 'TERMINATED'
    assert c3.get_state() == 'TERMINATED'
    assert c4.get_state() == 'WAITING'
    dm.start()
    assert c1.get_state() == 'TERMINATED'
    assert c2.get_state() == 'TERMINATED'
    assert c3.get_state() == 'TERMINATED'
    assert c4.get_state() == 'TERMINATED'
    assert c4.get_result() == 114
    dm.stop()


def test_dependency_monitor_in_background(default_os_env):
    dm = DependencyMonitor(callback=load_and_run_code_from_snapshot, interval=0.5, background=True)
    c1 = Code(add3)
    c2 = Code(mul2)
    c3 = Code(combination)
    c4 = Code(linear)
    c5 = Code(print_x)
    c1 >> c3
    c2 >> c3
    c3 >> [c4, c5]
    c3.receive('c') << c1
    c3.receive('d') << c2
    c4.receive('c') << c3
    c5.receive('x') << c3
    c1.save()
    c2.save()
    c3.save()
    c4.save()
    c5.save()
    c3(2, b=3)
    c5()
    c4(3, 6)
    c2(3, 5)
    c1(1, 2, 3)
    assert c1.get_state() == 'TERMINATED'
    assert c2.get_state() == 'TERMINATED'
    assert c3.get_state() == 'WAITING'
    assert c4.get_state() == 'WAITING'
    assert c5.get_state() == 'WAITING'
    dm.start()
    time.sleep(1)
    assert c1.get_state() == 'TERMINATED'
    assert c2.get_state() == 'TERMINATED'
    assert c3.get_state() == 'TERMINATED'
    assert c4.get_state() == 'TERMINATED'
    assert c5.get_state() == 'TERMINATED'
    assert c4.get_result() == 114
    dm.stop()
