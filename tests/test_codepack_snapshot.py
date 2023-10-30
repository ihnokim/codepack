from codepack.jobs.codepack_snapshot import CodePackSnapshot
from codepack.jobs.async_codepack_snapshot import AsyncCodePackSnapshot
from codepack.jobs.code_snapshot import CodeSnapshot
from codepack.jobs.async_code_snapshot import AsyncCodeSnapshot
import pytest


test_params = ("code_snapshot_class,codepack_snapshot_class", [(CodeSnapshot, CodePackSnapshot),
                                                               (AsyncCodeSnapshot, AsyncCodePackSnapshot)])


@pytest.mark.parametrize("codepack_snapshot_class", [CodePackSnapshot, AsyncCodePackSnapshot])
def test_initializing_codepack_snapshot_with_empty_list_of_code_snapshots(codepack_snapshot_class):
    with pytest.raises(TypeError):
        _ = codepack_snapshot_class()
    with pytest.raises(ValueError):
        _ = codepack_snapshot_class(code_snapshots=[])
