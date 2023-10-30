from codepack.interfaces.file_interface import FileInterface
from codepack.interfaces.async_file_interface import AsyncFileInterface
import os
import pytest
from tests import run_function


def test_if_initialization_makes_directory_when_given_path_does_not_exist(testdir):
    assert not FileInterface.exists(path=testdir)
    _ = FileInterface(path=testdir)
    assert FileInterface.exists(path=testdir)


@pytest.mark.parametrize('file_interface_class', [FileInterface, AsyncFileInterface])
def test_initialization_with_config(file_interface_class, testdir):
    config = {'path': testdir}
    fi = file_interface_class.get_instance(config=config)
    assert fi.exists(path=testdir)
    assert isinstance(fi.get_session(), file_interface_class)


def test_if_closing_does_not_effect_anything(testdir):
    fi = FileInterface(path=testdir)
    assert not fi.is_closed()
    fi.close()
    assert fi.is_closed()
    assert fi.exists(path=testdir)


def test_additional_operations(testdir):
    assert not FileInterface.exists(path=testdir)
    FileInterface.mkdir(path=testdir)
    assert FileInterface.exists(path=testdir)
    assert len(os.listdir(path=testdir)) == 0
    with open(os.path.join(testdir, 'test.txt'), 'w+') as f:
        f.write('Hello, World!')
    assert len(os.listdir(path=testdir)) == 1
    FileInterface.mkdir(path=os.path.join(testdir, 'test'))
    with open(os.path.join(testdir, 'test', 'test2.txt'), 'w+') as f:
        f.write('Hello, World!')
    assert len(os.listdir(path=testdir)) == 2
    assert len(os.listdir(path=os.path.join(testdir, 'test'))) == 1
    FileInterface.emptydir(path=os.path.join(testdir, 'test'))
    assert len(os.listdir(path=os.path.join(testdir, 'test'))) == 0
    assert len(os.listdir(path=testdir)) == 2
    FileInterface.rmdir(path=testdir)
    assert not FileInterface.exists(path=testdir)


def test_emptydir(testdir):
    FileInterface.mkdir(path=testdir)
    with open(os.path.join(testdir, 'test.txt'), 'w+') as f:
        f.write('Hello, World!')
    FileInterface.mkdir(path=os.path.join(testdir, 'test'))
    with open(os.path.join(testdir, 'test', 'test2.txt'), 'w+') as f:
        f.write('Hello, World!')
    assert len(os.listdir(path=testdir)) == 2
    assert len(os.listdir(path=os.path.join(testdir, 'test'))) == 1
    FileInterface.emptydir(path=testdir)
    assert len(os.listdir(path=testdir)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('file_interface_class', [FileInterface, AsyncFileInterface])
async def test_save_and_load(file_interface_class, testdir):
    file_interface = file_interface_class(path=testdir)
    await run_function(file_interface.save_file, dirname=testdir, filename='test.txt', data='Hello, world!')
    assert file_interface.exists(os.path.join(testdir, 'test.txt'))
    data = await run_function(file_interface.load_file, dirname=testdir, filename='test.txt')
    assert data == 'Hello, world!'
