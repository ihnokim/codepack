from codepack.interfaces.memory_message_queue import MemoryMessageQueue
from codepack.interfaces.async_memory_message_queue import AsyncMemoryMessageQueue
from codepack.interfaces.file_message_queue import FileMessageQueue
from codepack.interfaces.async_file_message_queue import AsyncFileMessageQueue
from tests import run_function
import pytest


test_params = ("mq_class,mq_args", [(MemoryMessageQueue, {}),
                                    (AsyncMemoryMessageQueue, {}),
                                    (FileMessageQueue, {'path': 'testdir'}),
                                    (AsyncFileMessageQueue, {'path': 'testdir'})])


@pytest.mark.parametrize("mq1_class,mq2_class", [(MemoryMessageQueue, MemoryMessageQueue),
                                                 (AsyncMemoryMessageQueue, AsyncMemoryMessageQueue),
                                                 (MemoryMessageQueue, AsyncMemoryMessageQueue)])
def test_if_two_different_mqs_share_same_inner_queue(mq1_class, mq2_class):
    mq1 = mq1_class()
    mq2 = mq2_class()
    assert mq1 != mq2
    assert mq1.messages == mq2.messages


@pytest.mark.parametrize(*test_params)
def test_close(mq_class, mq_args, testdir):
    mq = mq_class(**mq_args)
    assert not mq.is_closed()
    mq.close()
    assert mq.is_closed()


@pytest.mark.parametrize(*test_params)
def test_initialization_as_interface(mq_class, mq_args, testdir):
    mq = mq_class.get_instance(config=mq_args)
    assert isinstance(mq, mq_class)


@pytest.mark.parametrize(*test_params)
def test_creating_redundant_topics(mq_class, mq_args, testdir):
    mq = mq_class(**mq_args)
    mq.create_topic('test')
    assert mq.topic_exists('test')
    with pytest.raises(ValueError):
        mq.create_topic('test')


@pytest.mark.parametrize(*test_params)
def test_if_session_equals_to_itself(mq_class, mq_args, testdir):
    mq = mq_class(**mq_args)
    assert mq.get_session() == mq


@pytest.mark.parametrize(*test_params)
def test_listing_topics(mq_class, mq_args, testdir):
    mq = mq_class(**mq_args)
    assert mq.list_topics() == []
    mq.create_topic('test1')
    mq.create_topic('test2')
    assert set(mq.list_topics()) == {'test1', 'test2'}


@pytest.mark.parametrize(*test_params)
def test_removing_topics(mq_class, mq_args, testdir):
    mq = mq_class(**mq_args)
    mq.create_topic('test1')
    mq.create_topic('test2')
    mq.create_topic('test3')
    mq.remove_topic('test1')
    assert set(mq.list_topics()) == {'test2', 'test3'}


@pytest.mark.parametrize(*test_params)
def test_parsing_config(mq_class, mq_args, testdir):
    assert {'test_key': 'test_value'} == mq_class.parse_config({'test_key': 'test_value'})


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_if_group_offset_increases_independently(mq_class, mq_args, testdir):
    mq = mq_class(**mq_args)
    mq.create_topic('test')
    await run_function(mq.send, topic='test', message='0')
    await run_function(mq.send, topic='test', message='1')
    await run_function(mq.send, topic='test', message='2')
    await run_function(mq.send, topic='test', message='3')
    assert mq.get_offset(topic='test', group='group1') == 0
    assert mq.get_offset(topic='test', group='group2') == 0
    group1_messages = await run_function(mq.receive, topic='test', group='group1', batch=2)
    group2_messages = await run_function(mq.receive, topic='test', group='group2', batch=1)
    assert mq.get_offset(topic='test', group='group1') == 2
    assert mq.get_offset(topic='test', group='group2') == 1
    assert group1_messages == ['0', '1']
    assert group2_messages == ['0']
    group1_messages = await run_function(mq.receive, topic='test', group='group1', batch=2)
    group2_messages = await run_function(mq.receive, topic='test', group='group2', batch=2)
    assert mq.get_offset(topic='test', group='group1') == 4
    assert mq.get_offset(topic='test', group='group2') == 3
    assert group1_messages == ['2', '3']
    assert group2_messages == ['1', '2']
    group1_messages = await run_function(mq.receive, topic='test', group='group1', batch=2)
    group2_messages = await run_function(mq.receive, topic='test', group='group2', batch=1)
    assert mq.get_offset(topic='test', group='group1') == 4
    assert mq.get_offset(topic='test', group='group2') == 4
    assert group1_messages == []
    assert group2_messages == ['3']


@pytest.mark.asyncio
@pytest.mark.parametrize(*test_params)
async def test_removing_topics_when_group_offset_and_lock_exist(mq_class, mq_args, testdir):
    mq = mq_class(**mq_args)
    mq.create_topic('test1')
    mq.create_topic('test2')
    await run_function(mq.receive, topic='test1', group='group1', batch=2)
    await run_function(mq.receive, topic='test1', group='group2', batch=1)
    await run_function(mq.receive, topic='test2', group='group1', batch=2)
    await run_function(mq.receive, topic='test2', group='group2', batch=1)
    assert mq.get_offset(topic='test1', group='group1') == 0
    assert mq.get_offset(topic='test1', group='group2') == 0
    assert mq.get_offset(topic='test2', group='group1') == 0
    assert mq.get_offset(topic='test2', group='group2') == 0
    assert set(mq.locks.keys()) == {mq.get_group_key(topic='test1', group='group1'),
                                    mq.get_group_key(topic='test1', group='group2'),
                                    mq.get_group_key(topic='test2', group='group1'),
                                    mq.get_group_key(topic='test2', group='group2')}
    mq.remove_topic('test1')
    assert mq.get_offset(topic='test1', group='group1') == 0
    assert mq.get_offset(topic='test1', group='group2') == 0
    assert mq.get_offset(topic='test2', group='group1') == 0
    assert mq.get_offset(topic='test2', group='group2') == 0
    assert set(mq.locks.keys()) == {mq.get_group_key(topic='test2', group='group1'),
                                    mq.get_group_key(topic='test2', group='group2')}
