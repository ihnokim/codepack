from codepack.jobs.result_cache import ResultCache
from codepack.jobs.async_result_cache import AsyncResultCache
import json
import pytest


@pytest.mark.parametrize("result_cache_class", [ResultCache, AsyncResultCache])
def test_serialization_without_compression(result_cache_class):
    result = result_cache_class(data=12345,
                                name='test-result',
                                serial_number='test-sn',
                                encoding='utf-8',
                                compression=None)
    serialized_result = result.to_dict()
    assert serialized_result['data'] == json.dumps(12345).encode('utf-8')
    deserialized_result = result_cache_class.from_dict(serialized_result)
    assert deserialized_result.data == 12345
