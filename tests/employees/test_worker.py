from codepack import Config, Default, Code
from codepack.storages import MemoryMessenger
from unittest.mock import patch
import os
from tests import *


def dummy_callback_function(x):
    print(x)


def test_memory_worker_run_snapshot_with_nothing():
    worker = Default.get_employee('worker')
    assert isinstance(worker.messenger, MemoryMessenger)
    code = Code(add2)
    assert code.get_state() == 'UNKNOWN'
    sn = worker.run_snapshot(code.to_snapshot(kwargs={'a': 3, 'b': 5}))
    assert sn == code.serial_number
    assert code.get_state() == 'TERMINATED'
    assert code.get_result() == 8
    worker.stop()


@patch('subprocess.run')
def test_memory_worker_run_snapshot_with_env(mock_subprocess_run):
    worker = Default.get_employee('worker')
    assert isinstance(worker.messenger, MemoryMessenger)
    code = Code(add2, env='test_env', image='dummy')
    assert code.get_state() == 'UNKNOWN'
    sn = worker.run_snapshot(code.to_snapshot(kwargs={'a': 3, 'b': 5}))
    assert sn == code.serial_number
    default_config_dir = Config.get_default_config_dir()
    script_dir = os.path.join(default_config_dir, 'scripts')
    mock_subprocess_run.assert_called_once_with(
        [os.path.join(worker.interpreter_manager.path, 'test_env', 'bin', 'python'),
         os.path.join(script_dir, 'run_snapshot.py'),
         os.path.join(script_dir, '%s.json' % sn),
         '-p', script_dir,
         '-l', 'worker-logger'])
    worker.stop()


@patch('subprocess.run')
def test_memory_worker_run_snapshot_with_env_and_callback(mock_subprocess_run):
    worker = Default.get_employee('worker')
    worker.callback = dummy_callback_function
    assert isinstance(worker.messenger, MemoryMessenger)
    code = Code(add2, env='test_env', image='dummy')
    assert code.get_state() == 'UNKNOWN'
    sn = worker.run_snapshot(code.to_snapshot(kwargs={'a': 3, 'b': 5}))
    assert sn == code.serial_number
    default_config_dir = Config.get_default_config_dir()
    script_dir = os.path.join(default_config_dir, 'scripts')
    mock_subprocess_run.assert_called_once_with(
        [os.path.join(worker.interpreter_manager.path, 'test_env', 'bin', 'python'),
         os.path.join(script_dir, 'run_snapshot.py'),
         os.path.join(script_dir, '%s.json' % sn),
         '-p', script_dir,
         '-l', 'worker-logger',
         '-c', 'dummy_callback_function'])
    worker.stop()


@patch('docker.DockerClient')
def test_memory_worker_run_snapshot_with_image(mock_docker_client):
    worker = Default.get_employee('worker')
    assert isinstance(worker.messenger, MemoryMessenger)
    code = Code(add2, image='dummy')
    assert code.get_state() == 'UNKNOWN'
    sn = worker.run_snapshot(code.to_snapshot(kwargs={'a': 3, 'b': 5}))
    assert sn == code.serial_number
    default_config_dir = Config.get_default_config_dir()
    script_dir = os.path.join(default_config_dir, 'scripts')
    mock_docker_client.return_value.containers.run.assert_called_once_with(
        auto_remove=True,
        command=['python', 'run_snapshot.py', '%s.json' % sn, '-p', '.', '-l', 'worker-logger'],
        dns=['8.8.8.8'], environment=['CODEPACK_LOGGER_LOG_DIR=/usr/logs'],
        image='dummy', name=id(worker.docker_manager),
        volumes=['%s:/usr/src/codepack' % script_dir, '%s:/usr/logs' % os.path.abspath(Config.get_log_dir())],
        working_dir='/usr/src/codepack')
    worker.stop()


@patch('docker.DockerClient')
def test_memory_worker_run_snapshot_with_image_and_callback(mock_docker_client):
    worker = Default.get_employee('worker')
    worker.callback = dummy_callback_function
    assert isinstance(worker.messenger, MemoryMessenger)
    code = Code(add2, image='dummy')
    assert code.get_state() == 'UNKNOWN'
    sn = worker.run_snapshot(code.to_snapshot(kwargs={'a': 3, 'b': 5}))
    assert sn == code.serial_number
    default_config_dir = Config.get_default_config_dir()
    script_dir = os.path.join(default_config_dir, 'scripts')
    mock_docker_client.return_value.containers.run.assert_called_once_with(
        auto_remove=True,
        command=['python', 'run_snapshot.py', '%s.json' % sn, '-p', '.', '-l', 'worker-logger',
                 '-c', 'dummy_callback_function'],
        dns=['8.8.8.8'], environment=['CODEPACK_LOGGER_LOG_DIR=/usr/logs'],
        image='dummy', name=id(worker.docker_manager),
        volumes=['%s:/usr/src/codepack' % script_dir, '%s:/usr/logs' % os.path.abspath(Config.get_log_dir())],
        working_dir='/usr/src/codepack')
    worker.stop()
