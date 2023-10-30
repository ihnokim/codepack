from codepack.code import Code
from codepack.async_code import AsyncCode
from codepack.arg import Arg
from tests import add2
from codepack.jobs.code_snapshot import CodeSnapshot
from codepack.jobs.async_code_snapshot import AsyncCodeSnapshot
import pytest


test_params = ("code_class,code_snapshot_class", [(Code, CodeSnapshot),
                                                  (Code, AsyncCodeSnapshot),
                                                  (AsyncCode, CodeSnapshot),
                                                  (AsyncCode, AsyncCodeSnapshot)])


@pytest.mark.parametrize(*test_params)
def test_creating_code_snapshot_from_code(code_class, code_snapshot_class):
    code = code_class(add2)
    snapshot = code_snapshot_class(function=code.function, name=code.get_id())
    assert snapshot.name == code.get_id()
    assert snapshot.function == code.function


@pytest.mark.parametrize(*test_params)
def test_if_code_snapshot_serial_number_is_kept_when_change_is_made(code_class, code_snapshot_class):
    code = code_class(add2)
    snapshot: CodeSnapshot = code_snapshot_class(function=code)
    original_serial_number = snapshot.serial_number
    original_timestamp = snapshot.timestamp
    arg = Arg('test')
    snapshot.set_arg(arg=arg)
    assert original_serial_number == snapshot.serial_number
    assert original_timestamp == snapshot.timestamp


@pytest.mark.parametrize(*test_params)
def test_creating_code_snapshot_from_code_with_arg(code_class, code_snapshot_class):
    code = code_class(add2)
    arg = Arg('test')
    snapshot = code_snapshot_class(function=code.function,
                                   name=code.get_id(),
                                   arg=arg(2, 4))
    assert snapshot.name == code.get_id()
    assert snapshot.function == code.function
    assert snapshot.arg == arg


def test_if_sync_and_async_code_snapshot_are_same():
    code = Code(add2)
    arg = Arg('test')
    sync_snapshot = CodeSnapshot(function=code.function,
                                 name=code.get_id(),
                                 arg=arg(2, 4))
    async_snapshot = AsyncCodeSnapshot.from_dict(sync_snapshot.to_dict())
    assert sync_snapshot.to_dict() == async_snapshot.to_dict()
