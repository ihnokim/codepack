from codepack.messengers.receiver import Receiver
from codepack.messengers.async_receiver import AsyncReceiver
from codepack.messengers.sender import Sender
from codepack.messengers.async_sender import AsyncSender
from codepack.interfaces.memory_message_queue import MemoryMessageQueue
from codepack.interfaces.async_memory_message_queue import AsyncMemoryMessageQueue
import pytest


async def raise_keyboard_interrupt(x):
    raise KeyboardInterrupt()


test_params_memory_messengers = ("messenger_class,messenger_args,default_inner_class",
                                 [(Receiver, (['test-topic'], 'test-group'), MemoryMessageQueue),
                                  (AsyncReceiver, (['test-topic'], 'test-group'), AsyncMemoryMessageQueue),
                                  (Sender, (), MemoryMessageQueue),
                                  (AsyncSender, (), AsyncMemoryMessageQueue)])


@pytest.mark.parametrize(*test_params_memory_messengers)
def test_memory_messenger_initialization_without_anything(messenger_class, messenger_args, default_inner_class):
    messenger_instance = messenger_class(*messenger_args)
    assert isinstance(messenger_instance.mq, default_inner_class)


@pytest.mark.parametrize(*test_params_memory_messengers)
def test_memory_messenger_initialization_with_message_queue(messenger_class, messenger_args, default_inner_class):
    queue_instance = default_inner_class()
    messenger_instance = messenger_class(*messenger_args, message_queue=queue_instance)
    assert isinstance(messenger_instance.mq, default_inner_class)
    assert messenger_instance.mq == queue_instance


def test_closing_sync_memory_receiver():
    mr = Receiver(topics=['test-topic'], group='test-group')
    assert mr.looper is None
    assert mr.mq is not None
    mr.poll(lambda x: x, background=True, interval=0.1)
    assert mr.looper.is_running()
    mr.close()
    assert not mr.looper.is_running()
    assert mr.mq is None


@pytest.mark.asyncio
async def test_closing_async_memory_receiver(event_loop):
    mr = AsyncReceiver(topics=['test-topic'], group='test-group')
    assert mr.mq is not None
    assert not mr.stop_event.is_set()
    polling_task = event_loop.create_task(mr.poll(lambda x: x, interval=0.1))
    await mr.close()
    assert mr.stop_event.is_set()
    assert mr.mq is None
    await polling_task


@pytest.mark.asyncio
async def test_sending_keyboard_interrupt_to_async_memory_receiver():
    mr = AsyncReceiver(topics=['test-topic'], group='test-group')
    assert mr.mq is not None
    await mr.mq.send('test-topic', 'test-message')
    assert not mr.stop_event.is_set()
    await mr.poll(raise_keyboard_interrupt, interval=0.1)
    assert mr.stop_event.is_set()
    assert mr.mq is not None
