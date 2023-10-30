from typing import Any, Optional
from tests import add2, mul2, add3, print_x, do_nothing, combination, linear, run_function, sleep
from collections import Counter
import json
import pytest
from codepack.code import Code
from codepack.codepack import CodePack
from codepack.jobs.code_snapshot import CodeSnapshot
from codepack.jobs.async_code_snapshot import AsyncCodeSnapshot
from codepack.jobs.codepack_snapshot import CodePackSnapshot
from codepack.jobs.async_codepack_snapshot import AsyncCodePackSnapshot
from codepack.jobs.result_cache import ResultCache
from codepack.jobs.async_result_cache import AsyncResultCache
from codepack.jobs.job import Job
from codepack.jobs.async_job import AsyncJob
from codepack.arg import Arg
from codepack.argpack import ArgPack
from codepack.jobs.states import States, AsyncStates
from codepack.utils.observable import Observable
from codepack.utils.observer import Observer


polling_interval = 0.05


class CustomObserver(Observer):
    def __init__(self):
        self.messages = set()

    def update(self, observable: Optional[Observable] = None, **kwargs: Any) -> None:
        for v in kwargs.values():
            self.messages.add(v)


@pytest.mark.asyncio
@pytest.mark.parametrize("job_class", [Job, AsyncJob])
async def test_dependency_check_if_codepack_snapshot_does_not_exist(job_class):
    job = job_class('test')
    assert await run_function(job.check_if_dependencies_resolved) == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("job_managers_fixture", ["memory_job_managers",
                                                  "async_memory_job_managers",
                                                  "file_job_managers",
                                                  "async_file_job_managers"])
async def test_if_job_is_submitted_correctly(init_storages, job_managers_fixture, request):
    job_managers = request.getfixturevalue(job_managers_fixture)
    supervisor, worker = job_managers
    code = Code(add2)
    serial_number = await run_function(supervisor.submit_code, code=code)
    messages = await run_function(worker.receiver.receive)
    if isinstance(messages, list):
        assert len(messages) == 1
        d = json.loads(messages[0])
    else:
        _message_chunks = [x async for x in messages]
        assert len(_message_chunks) == 1
        assert len(_message_chunks[0]) == 1
        d = json.loads(_message_chunks[0][0])
    assert 'code_snapshot' in d.keys()
    assert d['code_snapshot'] == serial_number


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_managers_fixture,code_snapshot_class,job_class,result_cache_class",
    [
     ("memory_job_managers", CodeSnapshot, Job, ResultCache),
     ("async_memory_job_managers", AsyncCodeSnapshot, AsyncJob, AsyncResultCache),
     ("file_job_managers", CodeSnapshot, Job, ResultCache),
     ("async_file_job_managers", AsyncCodeSnapshot, AsyncJob, AsyncResultCache)
     ])
async def test_if_one_submitted_job_is_executed(init_storages,
                                                job_managers_fixture,
                                                code_snapshot_class,
                                                job_class,
                                                result_cache_class,
                                                request,
                                                event_loop):
    job_managers = request.getfixturevalue(job_managers_fixture)
    supervisor, worker = job_managers
    if 'async' not in job_managers_fixture:
        worker.start(interval=polling_interval)
        polling_task = None
    else:
        polling_task = event_loop.create_task(worker.start(interval=polling_interval))
    try:
        code = Code(add2)
        arg = Arg(name='test')
        serial_number = await run_function(supervisor.submit_code, code=code, arg=arg(3, b=5))
        await sleep(key=job_managers_fixture)
        code_snapshot_storage = code_snapshot_class.get_storage()
        job_storage = job_class.get_storage()
        result_cache_storage = result_cache_class.get_storage()
        assert await run_function(code_snapshot_storage.count) == 1
        assert await run_function(job_storage.count) == 5
        sorted_list = sorted([d for d in await run_function(job_storage.load_many)], key=lambda x: x['timestamp'])
        sorted_states = [x['state'] for x in sorted_list]
        assert sorted_states == ['NEW', 'CHECKING', 'READY', 'RUNNING', 'TERMINATED']
        assert await run_function(result_cache_storage.count) == 1
        assert await run_function(result_cache_storage.exists, id=serial_number)
        assert await run_function(supervisor.get_code_result, serial_number=serial_number) == 8
    finally:
        await run_function(worker.stop)
        if polling_task is not None:
            await polling_task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_managers_fixture,code_snapshot_class,job_class,result_cache_class",
    [
     ("memory_job_managers", CodeSnapshot, Job, ResultCache),
     ("async_memory_job_managers", AsyncCodeSnapshot, AsyncJob, AsyncResultCache),
     ("file_job_managers", CodeSnapshot, Job, ResultCache),
     ("async_file_job_managers", AsyncCodeSnapshot, AsyncJob, AsyncResultCache)
     ])
async def test_if_one_submitted_job_without_arg_is_executed(init_storages,
                                                            job_managers_fixture,
                                                            code_snapshot_class,
                                                            job_class,
                                                            result_cache_class,
                                                            request,
                                                            event_loop):
    job_managers = request.getfixturevalue(job_managers_fixture)
    supervisor, worker = job_managers
    if 'async' not in job_managers_fixture:
        worker.start(interval=polling_interval)
        polling_task = None
    else:
        polling_task = event_loop.create_task(worker.start(interval=polling_interval))
    try:
        code = Code(do_nothing)
        serial_number = await run_function(supervisor.submit_code, code=code)
        await sleep(key=job_managers_fixture)
        code_snapshot_storage = code_snapshot_class.get_storage()
        job_storage = job_class.get_storage()
        result_cache_storage = result_cache_class.get_storage()
        assert await run_function(code_snapshot_storage.count) == 1
        assert await run_function(job_storage.count) == 5
        sorted_list = sorted([d for d in await run_function(job_storage.load_many)], key=lambda x: x['timestamp'])
        sorted_states = [x['state'] for x in sorted_list]
        assert sorted_states == ['NEW', 'CHECKING', 'READY', 'RUNNING', 'TERMINATED']
        assert await run_function(result_cache_storage.count) == 1
        assert await run_function(result_cache_storage.exists, id=serial_number)
        assert await run_function(supervisor.get_code_result, serial_number=serial_number) is None
    finally:
        await run_function(worker.stop)
        if polling_task is not None:
            await polling_task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_managers_fixture,code_snapshot_class,codepack_snapshot_class,job_class,states_class",
    [
     ("memory_job_managers", CodeSnapshot, CodePackSnapshot, Job, States),
     ("async_memory_job_managers", AsyncCodeSnapshot, AsyncCodePackSnapshot, AsyncJob, AsyncStates),
     ("file_job_managers", CodeSnapshot, CodePackSnapshot, Job, States),
     ("async_file_job_managers", AsyncCodeSnapshot, AsyncCodePackSnapshot, AsyncJob, AsyncStates)
     ])
async def test_if_dependent_jobs_are_executed(init_storages,
                                              job_managers_fixture,
                                              code_snapshot_class,
                                              codepack_snapshot_class,
                                              job_class,
                                              states_class,
                                              request,
                                              event_loop):
    job_managers = request.getfixturevalue(job_managers_fixture)
    code1 = Code(add2)
    code2 = Code(mul2)
    code1 >> code2 | 'a'
    arg1 = Arg('arg1')(2, 3)
    arg2 = Arg('arg2')(b=2)
    argpack = ArgPack(args=[arg1, arg2])
    argpack.map_code(code_id=code1.get_id(), arg_id=arg1.get_id())
    argpack.map_code(code_id=code2.get_id(), arg_id=arg2.get_id())
    codepack = CodePack(codes=[code1])
    supervisor, worker = job_managers
    if 'async' not in job_managers_fixture:
        worker.start(interval=polling_interval)
        polling_task = None
    else:
        polling_task = event_loop.create_task(worker.start(interval=polling_interval))
    try:
        serial_number = await run_function(supervisor.submit_codepack, codepack=codepack, argpack=argpack)
        await sleep(key=job_managers_fixture)
        code_snapshot_storage = code_snapshot_class.get_storage()
        codepack_snapshot_storage = codepack_snapshot_class.get_storage()
        job_storage = job_class.get_storage()
        assert await run_function(codepack_snapshot_storage.count) == 1
        assert await run_function(code_snapshot_storage.count) == 2
        assert await run_function(job_storage.count) > 2
        code_snapshot_serial_numbers = list(await run_function(code_snapshot_storage.list_all))
        for serial_number in code_snapshot_serial_numbers:
            await run_function(supervisor.get_code_state, serial_number=serial_number) == states_class.TERMINATED
    finally:
        await run_function(worker.stop)
        if polling_task is not None:
            await polling_task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_managers_fixture,code_snapshot_class,codepack_snapshot_class,job_class,states_class",
    [
     ("memory_job_managers", CodeSnapshot, CodePackSnapshot, Job, States),
     ("async_memory_job_managers", AsyncCodeSnapshot, AsyncCodePackSnapshot, AsyncJob, AsyncStates),
     ("file_job_managers", CodeSnapshot, CodePackSnapshot, Job, States),
     ("async_file_job_managers", AsyncCodeSnapshot, AsyncCodePackSnapshot, AsyncJob, AsyncStates)
     ])
async def test_if_multiple_upstream_jobs_trigger_multiple_state_propagation_but_one_execution(init_storages,
                                                                                              job_managers_fixture,
                                                                                              code_snapshot_class,
                                                                                              codepack_snapshot_class,
                                                                                              job_class,
                                                                                              states_class,
                                                                                              request,
                                                                                              event_loop):
    job_managers = request.getfixturevalue(job_managers_fixture)
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    code4 = Code(combination)
    code1 >> code4 | 'a'
    code2 >> code4 | 'b'
    code3 > code4
    arg1 = Arg('arg1')(2, 3)
    arg2 = Arg('arg2')(a=4, b=5)
    arg3 = Arg('arg3')(2, 3, c=2)
    arg4 = Arg('arg4')(c=3, d=6)
    argpack = ArgPack(args=[arg1, arg2, arg3, arg4])
    argpack.map_code(code_id=code1.get_id(), arg_id=arg1.get_id())
    argpack.map_code(code_id=code2.get_id(), arg_id=arg2.get_id())
    argpack.map_code(code_id=code3.get_id(), arg_id=arg3.get_id())
    argpack.map_code(code_id=code4.get_id(), arg_id=arg4.get_id())
    codepack = CodePack(codes=[code1])
    supervisor, worker = job_managers
    if 'async' not in job_managers_fixture:
        worker.start(interval=0.05)
        polling_task = None
    else:
        polling_task = event_loop.create_task(worker.start(interval=0.05))
    try:
        serial_number = await run_function(supervisor.submit_codepack, codepack=codepack, argpack=argpack)
        await sleep(key=job_managers_fixture)
        code_snapshot_storage = code_snapshot_class.get_storage()
        codepack_snapshot_storage = codepack_snapshot_class.get_storage()
        job_storage = job_class.get_storage()
        assert await run_function(codepack_snapshot_storage.count) == 1
        assert await run_function(code_snapshot_storage.count) == 4
        assert await run_function(job_storage.count) > 4
        code_snapshot_serial_numbers = list(await run_function(code_snapshot_storage.list_all))
        for serial_number in code_snapshot_serial_numbers:
            await run_function(job_class.check_state, code_snapshot=serial_number) == states_class.TERMINATED
        search_result = await run_function(code_snapshot_storage.search, key='name', value=code4.get_id())
        assert len(search_result) == 1
        job_history = await run_function(job_class.history, code_snapshot=search_result[0])
        states = Counter([x[0] for x in job_history])
        assert states['CHECKING'] > 3
        assert states['READY'] == 3
        assert states['RUNNING'] == 1
        assert states['TERMINATED'] == 1
    finally:
        await run_function(worker.stop)
        if polling_task is not None:
            await polling_task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_managers_fixture,code_snapshot_class,job_class,states_class",
    [
     ("memory_job_managers", CodeSnapshot, Job, States),
     ("async_memory_job_managers", AsyncCodeSnapshot, AsyncJob, AsyncStates),
     ("file_job_managers", CodeSnapshot, Job, States),
     ("async_file_job_managers", AsyncCodeSnapshot, AsyncJob, AsyncStates)
     ])
async def test_error_propagation(init_storages,
                                 job_managers_fixture,
                                 code_snapshot_class,
                                 job_class,
                                 states_class,
                                 request,
                                 event_loop):
    job_managers = request.getfixturevalue(job_managers_fixture)
    code1 = Code(add2)
    code2 = Code(mul2)
    code3 = Code(add3)
    code4 = Code(combination)
    code5 = Code(linear)
    code1 >> code3 | 'a'
    code2 >> code3 | 'b'
    code3 > [code4, code5]
    code3 >> code4 | 'c'
    arg1 = Arg(name='arg1')(2, 3)
    arg2 = Arg(name='arg2')(a=4, b=None)
    arg3 = Arg(name='arg3')(c=2)
    arg4 = Arg(name='arg4')(a=2, b=4, d=6)
    arg5 = Arg(name='arg5')(a=2, b=3, c=4)
    argpack = ArgPack(args=[arg1, arg2, arg3, arg4, arg5])
    argpack.map_code(code_id=code1.get_id(), arg_id=arg1.get_id())
    argpack.map_code(code_id=code2.get_id(), arg_id=arg2.get_id())
    argpack.map_code(code_id=code3.get_id(), arg_id=arg3.get_id())
    argpack.map_code(code_id=code4.get_id(), arg_id=arg4.get_id())
    argpack.map_code(code_id=code5.get_id(), arg_id=arg5.get_id())
    codepack = CodePack(codes=[code1])
    supervisor, worker = job_managers
    if 'async' not in job_managers_fixture:
        worker.start(interval=polling_interval)
        polling_task = None
    else:
        polling_task = event_loop.create_task(worker.start(interval=polling_interval))
    try:
        _ = await run_function(supervisor.submit_codepack, codepack=codepack, argpack=argpack)
        await sleep(key=job_managers_fixture)
        code_snapshot_storage = code_snapshot_class.get_storage()
        code_snapshot_serial_numbers = list(await run_function(code_snapshot_storage.list_all))
        code_snapshots = {x['name']: x for x in await run_function(code_snapshot_storage.load_many,
                                                                   id=code_snapshot_serial_numbers)}
        sn = 'serial_number'
        assert await run_function(supervisor.get_code_state,
                                  serial_number=code_snapshots[code1.get_id()][sn]) == states_class.TERMINATED
        assert await run_function(supervisor.get_code_state,
                                  serial_number=code_snapshots[code2.get_id()][sn]) == states_class.ERROR
        assert await run_function(supervisor.get_code_state,
                                  serial_number=code_snapshots[code3.get_id()][sn]) == states_class.ERROR
        assert await run_function(supervisor.get_code_state,
                                  serial_number=code_snapshots[code4.get_id()][sn]) == states_class.ERROR
        assert await run_function(supervisor.get_code_state,
                                  serial_number=code_snapshots[code5.get_id()][sn]) == states_class.ERROR
        search_result = await run_function(code_snapshot_storage.search, key='name', value=code5.get_id())
        job_history = await run_function(job_class.history, code_snapshot=search_result[0])
        states = Counter([x[0] for x in job_history])
        assert states['CHECKING'] == 1
        assert states['WAITING'] == 1
        assert states['ERROR'] >= 1
    finally:
        await run_function(worker.stop)
        if polling_task is not None:
            await polling_task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_managers_fixture,code_snapshot_class,job_class,states_class",
    [
     ("memory_job_managers", CodeSnapshot, Job, States),
     ("async_memory_job_managers", AsyncCodeSnapshot, AsyncJob, AsyncStates),
     ("file_job_managers", CodeSnapshot, Job, States),
     ("async_file_job_managers", AsyncCodeSnapshot, AsyncJob, AsyncStates)
     ])
async def test_getting_result_when_subscription_is_enabled(init_storages,
                                                           job_managers_fixture,
                                                           code_snapshot_class,
                                                           job_class,
                                                           states_class,
                                                           request,
                                                           event_loop):
    job_managers = request.getfixturevalue(job_managers_fixture)
    code1 = Code(add3)
    code2 = Code(mul2)
    code3 = Code(combination)
    code4 = Code(linear)
    code5 = Code(print_x)
    code1 >> code3 | 'c'
    code2 >> code3 | 'd'
    code3 > [code4, code5]
    code3 >> code4 | 'c'
    code3 >> code5 | 'x'
    arg1 = Arg(name='arg1')(1, 2, c=3)
    arg2 = Arg(name='arg2')(a=1, b=2)
    arg3 = Arg(name='arg3')(a=2, b=5)
    arg4 = Arg(name='arg4')(a=5, b=7)
    arg5 = Arg(name='arg5')()
    argpack = ArgPack(args=[arg1, arg2, arg3, arg4, arg5])
    argpack.map_code(code_id=code1.get_id(), arg_id=arg1.get_id())
    argpack.map_code(code_id=code2.get_id(), arg_id=arg2.get_id())
    argpack.map_code(code_id=code3.get_id(), arg_id=arg3.get_id())
    argpack.map_code(code_id=code4.get_id(), arg_id=arg4.get_id())
    argpack.map_code(code_id=code5.get_id(), arg_id=arg5.get_id())
    codepack = CodePack(codes=[code1], subscription=code4)
    supervisor, worker = job_managers
    if 'async' not in job_managers_fixture:
        worker.start(interval=polling_interval)
        polling_task = None
    else:
        polling_task = event_loop.create_task(worker.start(interval=polling_interval))
    try:
        codepack_serial_number = await run_function(supervisor.submit_codepack, codepack=codepack, argpack=argpack)
        await sleep(key=job_managers_fixture)
        code_snapshot_storage = code_snapshot_class.get_storage()
        code_snapshot_serial_numbers = list(await run_function(code_snapshot_storage.list_all))
        for serial_number in code_snapshot_serial_numbers:
            await run_function(job_class.check_state, code_snapshot=serial_number) == states_class.TERMINATED
        result = await run_function(supervisor.get_codepack_result, codepack_serial_number)
        assert result == 57
    finally:
        await run_function(worker.stop)
        if polling_task is not None:
            await polling_task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_managers_fixture,code_snapshot_class,job_class,states_class",
    [
     ("memory_job_managers", CodeSnapshot, Job, States),
     ("async_memory_job_managers", AsyncCodeSnapshot, AsyncJob, AsyncStates),
     ("file_job_managers", CodeSnapshot, Job, States),
     ("async_file_job_managers", AsyncCodeSnapshot, AsyncJob, AsyncStates)
     ])
async def test_getting_result_when_subscription_is_disabled(init_storages,
                                                            job_managers_fixture,
                                                            code_snapshot_class,
                                                            job_class,
                                                            states_class,
                                                            request,
                                                            event_loop):
    job_managers = request.getfixturevalue(job_managers_fixture)
    code1 = Code(add3)
    code2 = Code(mul2)
    code3 = Code(combination)
    code4 = Code(linear)
    code5 = Code(print_x)
    code1 >> code3 | 'c'
    code2 >> code3 | 'd'
    code3 > [code4, code5]
    code3 >> code4 | 'c'
    code3 >> code5 | 'x'
    arg1 = Arg(name='arg1')(1, 2, c=3)
    arg2 = Arg(name='arg2')(a=1, b=2)
    arg3 = Arg(name='arg3')(a=2, b=5)
    arg4 = Arg(name='arg4')(a=5, b=7)
    arg5 = Arg(name='arg5')()
    argpack = ArgPack(args=[arg1, arg2, arg3, arg4, arg5])
    argpack.map_code(code_id=code1.get_id(), arg_id=arg1.get_id())
    argpack.map_code(code_id=code2.get_id(), arg_id=arg2.get_id())
    argpack.map_code(code_id=code3.get_id(), arg_id=arg3.get_id())
    argpack.map_code(code_id=code4.get_id(), arg_id=arg4.get_id())
    argpack.map_code(code_id=code5.get_id(), arg_id=arg5.get_id())
    codepack = CodePack(codes=[code1])
    supervisor, worker = job_managers
    if 'async' not in job_managers_fixture:
        worker.start(interval=polling_interval)
        polling_task = None
    else:
        polling_task = event_loop.create_task(worker.start(interval=polling_interval))
    try:
        codepack_serial_number = await run_function(supervisor.submit_codepack, codepack=codepack, argpack=argpack)
        await sleep(key=job_managers_fixture)
        code_snapshot_storage = code_snapshot_class.get_storage()
        code_snapshot_serial_numbers = list(await run_function(code_snapshot_storage.list_all))
        for serial_number in code_snapshot_serial_numbers:
            await run_function(job_class.check_state, code_snapshot=serial_number) == states_class.TERMINATED
        result = await run_function(supervisor.get_codepack_result, codepack_serial_number)
        assert result is None
    finally:
        await run_function(worker.stop)
        if polling_task is not None:
            await polling_task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_managers_fixture,states_class",
    [
     ("memory_job_managers", States),
     ("async_memory_job_managers", AsyncStates),
     ("file_job_managers", States),
     ("async_file_job_managers", AsyncStates)
     ])
async def test_state_monitoring(init_storages,
                                job_managers_fixture,
                                states_class,
                                request,
                                event_loop):
    job_managers = request.getfixturevalue(job_managers_fixture)
    code1 = Code(add2)
    code2 = Code(mul2)
    code1 >> code2 | 'a'
    arg1 = Arg('arg1')(2, 3)
    arg2 = Arg('arg2')(b=2)
    argpack = ArgPack(args=[arg1, arg2])
    argpack.map_code(code_id=code1.get_id(), arg_id=arg1.get_id())
    argpack.map_code(code_id=code2.get_id(), arg_id=arg2.get_id())
    codepack = CodePack(codes=[code1])
    supervisor, worker = job_managers
    if 'async' not in job_managers_fixture:
        worker.start(interval=polling_interval)
        supervisor.start(interval=polling_interval)
        polling_task1 = None
        polling_task2 = None
    else:
        polling_task1 = event_loop.create_task(worker.start(interval=polling_interval))
        polling_task2 = event_loop.create_task(supervisor.start(interval=polling_interval))
    observer = CustomObserver()
    supervisor.subscribe(id='test', observer=observer)
    try:
        assert len(supervisor.observable.observers) == 1
        assert supervisor.observable.get_observer(id='test') is not None
        serial_number = await run_function(supervisor.submit_codepack, codepack=codepack, argpack=argpack)
        await sleep(key=job_managers_fixture)
        assert len(observer.messages) > 0
        code_snapshot_storage = CodeSnapshot.get_storage()
        code_snapshot_serial_numbers = list(await run_function(code_snapshot_storage.list_all))
        for serial_number in code_snapshot_serial_numbers:
            assert await run_function(supervisor.get_code_state, serial_number=serial_number) == states_class.TERMINATED
    finally:
        await run_function(worker.stop)
        await run_function(supervisor.stop)
        if polling_task1 is not None:
            await polling_task1
        if polling_task2 is not None:
            await polling_task2


@pytest.mark.asyncio
@pytest.mark.parametrize("job_managers_fixture", ["memory_job_managers",
                                                  "async_memory_job_managers",
                                                  "file_job_managers",
                                                  "async_file_job_managers"])
async def test_state_monitoring_with_duplicate_ids(init_storages, job_managers_fixture, request, event_loop):
    job_managers = request.getfixturevalue(job_managers_fixture)
    supervisor, worker = job_managers
    if 'async' not in job_managers_fixture:
        worker.start(interval=polling_interval)
        supervisor.start(interval=polling_interval)
        polling_task1 = None
        polling_task2 = None
    else:
        polling_task1 = event_loop.create_task(worker.start(interval=polling_interval))
        polling_task2 = event_loop.create_task(supervisor.start(interval=polling_interval))
    try:
        observer1 = CustomObserver()
        observer2 = CustomObserver()
        supervisor.subscribe(id='test1', observer=observer1)
        with pytest.raises(ValueError):
            supervisor.subscribe(id='test1', observer=observer2)
    finally:
        await run_function(worker.stop)
        await run_function(supervisor.stop)
        if polling_task1 is not None:
            await polling_task1
        if polling_task2 is not None:
            await polling_task2


@pytest.mark.asyncio
@pytest.mark.parametrize("job_managers_fixture", ["memory_job_managers",
                                                  "async_memory_job_managers",
                                                  "file_job_managers",
                                                  "async_file_job_managers"])
async def test_state_monitoring_after_removing_duplicate_ids(init_storages, job_managers_fixture, request, event_loop):
    job_managers = request.getfixturevalue(job_managers_fixture)
    supervisor, worker = job_managers
    if 'async' not in job_managers_fixture:
        worker.start(interval=polling_interval)
        supervisor.start(interval=polling_interval)
        polling_task1 = None
        polling_task2 = None
    else:
        polling_task1 = event_loop.create_task(worker.start(interval=polling_interval))
        polling_task2 = event_loop.create_task(supervisor.start(interval=polling_interval))
    try:
        observer1 = CustomObserver()
        observer2 = CustomObserver()
        supervisor.subscribe(id='test1', observer=observer1)
        supervisor.unsubscribe(id='test1')
        supervisor.subscribe(id='test1', observer=observer2)
    finally:
        await run_function(worker.stop)
        await run_function(supervisor.stop)
        if polling_task1 is not None:
            await polling_task1
        if polling_task2 is not None:
            await polling_task2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_managers_fixture,code_snapshot_class,states_class,result_cache_class",
    [
     ("memory_job_managers", CodeSnapshot, States, ResultCache),
     ("async_memory_job_managers", AsyncCodeSnapshot, AsyncStates, AsyncResultCache),
     ("file_job_managers", CodeSnapshot, States, ResultCache),
     ("async_file_job_managers", AsyncCodeSnapshot, AsyncStates, AsyncResultCache)
     ])
async def test_execution_when_saving_result_cache_is_failed(init_storages,
                                                            job_managers_fixture,
                                                            code_snapshot_class,
                                                            states_class,
                                                            result_cache_class,
                                                            request,
                                                            event_loop):
    def sync_dummy_function(id, item):
        return False

    async def async_dummy_function(id, item):
        return False

    if 'async' not in job_managers_fixture:
        result_cache_class.get_storage().save = sync_dummy_function
    else:
        result_cache_class.get_storage().save = async_dummy_function
    job_managers = request.getfixturevalue(job_managers_fixture)
    code1 = Code(add2)
    code2 = Code(mul2)
    code1 >> code2 | 'a'
    arg1 = Arg('arg1')(2, 3)
    arg2 = Arg('arg2')(b=3)
    argpack = ArgPack(args=[arg1, arg2])
    argpack.map_code(code_id=code1.get_id(), arg_id=arg1.get_id())
    argpack.map_code(code_id=code2.get_id(), arg_id=arg2.get_id())
    codepack = CodePack(codes=[code1])
    supervisor, worker = job_managers
    if 'async' not in job_managers_fixture:
        worker.start(interval=polling_interval)
        polling_task = None
    else:
        polling_task = event_loop.create_task(worker.start(interval=polling_interval))
    try:
        serial_number = await run_function(supervisor.submit_codepack, codepack=codepack, argpack=argpack)
        await sleep(key=job_managers_fixture)
        code_snapshot_storage = code_snapshot_class.get_storage()
        code_snapshot_serial_numbers = list(await run_function(code_snapshot_storage.list_all))
        result_states = set()
        for serial_number in code_snapshot_serial_numbers:
            result_states.add(await run_function(supervisor.get_code_state, serial_number=serial_number))
        assert result_states == {states_class.ERROR, states_class.ERROR}
    finally:
        await run_function(worker.stop)
        await run_function(supervisor.stop)
        if polling_task is not None:
            await polling_task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_managers_fixture,code_snapshot_class,states_class,result_cache_class",
    [
     ("memory_job_managers", CodeSnapshot, States, ResultCache),
     ("async_memory_job_managers", AsyncCodeSnapshot, AsyncStates, AsyncResultCache),
     ("file_job_managers", CodeSnapshot, States, ResultCache),
     ("async_file_job_managers", AsyncCodeSnapshot, AsyncStates, AsyncResultCache)
     ])
async def test_execution_when_result_cache_missing(init_storages,
                                                   job_managers_fixture,
                                                   code_snapshot_class,
                                                   states_class,
                                                   result_cache_class,
                                                   request,
                                                   event_loop):
    def sync_dummy_function(id, item):
        return True

    async def async_dummy_function(id, item):
        return True

    if 'async' not in job_managers_fixture:
        result_cache_class.get_storage().save = sync_dummy_function
    else:
        result_cache_class.get_storage().save = async_dummy_function
    job_managers = request.getfixturevalue(job_managers_fixture)
    code1 = Code(add2)
    code2 = Code(mul2)
    code1 >> code2 | 'a'
    arg1 = Arg('arg1')(2, 3)
    arg2 = Arg('arg2')(b=3)
    argpack = ArgPack(args=[arg1, arg2])
    argpack.map_code(code_id=code1.get_id(), arg_id=arg1.get_id())
    argpack.map_code(code_id=code2.get_id(), arg_id=arg2.get_id())
    codepack = CodePack(codes=[code1])
    supervisor, worker = job_managers
    if 'async' not in job_managers_fixture:
        worker.start(interval=polling_interval)
        polling_task = None
    else:
        polling_task = event_loop.create_task(worker.start(interval=polling_interval))
    try:
        serial_number = await run_function(supervisor.submit_codepack, codepack=codepack, argpack=argpack)
        await sleep(key=job_managers_fixture)
        code_snapshot_storage = code_snapshot_class.get_storage()
        code_snapshot_serial_numbers = list(await run_function(code_snapshot_storage.list_all))
        result_states = set()
        for serial_number in code_snapshot_serial_numbers:
            result_states.add(await run_function(supervisor.get_code_state, serial_number=serial_number))
        assert result_states == {states_class.TERMINATED, states_class.ERROR}
    finally:
        await run_function(worker.stop)
        await run_function(supervisor.stop)
        if polling_task is not None:
            await polling_task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_managers_fixture,"
    "code_snapshot_class,"
    "codepack_snapshot_class,"
    "job_class,"
    "states_class,"
    "code_states,"
    "codepack_state",
    [
     ("memory_job_managers",
      CodeSnapshot,
      CodePackSnapshot,
      Job,
      States,
      ['TERMINATED', 'TERMINATED', 'TERMINATED', 'TERMINATED', 'TERMINATED'],
      'TERMINATED'),
     ("async_memory_job_managers",
      AsyncCodeSnapshot,
      AsyncCodePackSnapshot,
      AsyncJob,
      AsyncStates,
      ['TERMINATED', 'TERMINATED', 'TERMINATED', 'TERMINATED', 'TERMINATED'],
      'TERMINATED'),
     ("memory_job_managers",
      CodeSnapshot,
      CodePackSnapshot,
      Job,
      States,
      ['TERMINATED', 'ERROR', 'UNKNOWN', 'RUNNING', 'READY'],
      'ERROR'),
     ("async_memory_job_managers",
      AsyncCodeSnapshot,
      AsyncCodePackSnapshot,
      AsyncJob,
      AsyncStates,
      ['TERMINATED', 'ERROR', 'UNKNOWN', 'RUNNING', 'READY'],
      'ERROR'),
     ("memory_job_managers",
      CodeSnapshot,
      CodePackSnapshot,
      Job,
      States,
      ['TERMINATED', 'WAITING', 'UNKNOWN', 'RUNNING', 'READY'],
      'RUNNING'),
     ("async_memory_job_managers",
      AsyncCodeSnapshot,
      AsyncCodePackSnapshot,
      AsyncJob,
      AsyncStates,
      ['TERMINATED', 'WAITING', 'UNKNOWN', 'RUNNING', 'READY'],
      'RUNNING'),
     ("memory_job_managers",
      CodeSnapshot,
      CodePackSnapshot,
      Job,
      States,
      ['TERMINATED', 'WAITING', 'UNKNOWN', 'TERMINATED', 'CHECKING'],
      'WAITING'),
     ("async_memory_job_managers",
      AsyncCodeSnapshot,
      AsyncCodePackSnapshot,
      AsyncJob,
      AsyncStates,
      ['TERMINATED', 'WAITING', 'UNKNOWN', 'TERMINATED', 'CHECKING'],
      'WAITING'),
     ("memory_job_managers",
      CodeSnapshot,
      CodePackSnapshot,
      Job,
      States,
      ['TERMINATED', 'TERMINATED', 'TERMINATED', 'TERMINATED', 'NEW'],
      'NEW'),
     ("async_memory_job_managers",
      AsyncCodeSnapshot,
      AsyncCodePackSnapshot,
      AsyncJob,
      AsyncStates,
      ['TERMINATED', 'TERMINATED', 'TERMINATED', 'TERMINATED', 'NEW'],
      'NEW'),
     ])
async def test_getting_codepack_state(init_storages,
                                      job_managers_fixture,
                                      code_snapshot_class,
                                      codepack_snapshot_class,
                                      job_class,
                                      states_class,
                                      code_states,
                                      codepack_state,
                                      request):
    job_managers = request.getfixturevalue(job_managers_fixture)
    codes = [Code(add3), Code(mul2), Code(combination), Code(linear), Code(print_x)]
    state_names = code_states
    supervisor, _ = job_managers
    code_snapshots = list()
    for i, code in enumerate(codes):
        code_snapshot = code_snapshot_class(function=code.function)
        code_snapshots.append(code_snapshot)
        await run_function(code_snapshot.save)
    code_snapshot_serial_numbers = [cs.serial_number for cs in code_snapshots]
    codepack_snapshot = codepack_snapshot_class(code_snapshots=code_snapshot_serial_numbers)
    await run_function(codepack_snapshot.save)
    for i, code_snapshot in enumerate(code_snapshots):
        state = getattr(states_class, state_names[i])
        job = job_class(code_snapshot=code_snapshot.serial_number,
                        codepack_snapshot=codepack_snapshot.serial_number,
                        state=state)
        await run_function(job.save)
    final_state = await run_function(supervisor.get_codepack_state,
                                     serial_number=codepack_snapshot.serial_number)
    assert final_state.get_name() == codepack_state
