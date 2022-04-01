from codepack import Scheduler, Default
from codepack.storage.storage import Storage
from codepack.storage.messenger import Messenger
from codepack.plugin.service import Service
from codepack.plugin.employee import Employee
from fastapi import FastAPI, Request
from .routers import code, codepack, argpack, scheduler
from .dependencies import common


app = FastAPI()
app.include_router(code.router)
app.include_router(codepack.router)
app.include_router(argpack.router)
app.include_router(scheduler.router)


@app.middleware('http')
async def add_process_time_header(request: Request, call_next):
    response = await call_next(request)
    response.headers['Access-Control-Allow_Origin'] = '*'
    response.headers['Access-Control-Allow_Headers'] = 'Content-Type,Authorization'
    response.headers['Access-Control-Allow-Methods'] = 'GET,PUT,POST,DELETE'
    return response


@app.on_event('startup')
async def startup():
    Default.init()
    config = Default.config.get_config(section='apiserver')
    _scheduler = config.get('scheduler', 'self')
    common.add(key='supervisor', value=Default.get_employee('supervisor'), destroy=destroy_supervisor)
    if isinstance(_scheduler, Scheduler):
        common.add(key='scheduler', value=_scheduler, destroy=destroy_scheduler)
        common.scheduler.start()
    elif isinstance(_scheduler, str):
        if _scheduler == 'self':
            common.add(key='scheduler', value=Default.get_scheduler(), destroy=destroy_scheduler)
            common.scheduler.start()
        else:
            common.add(key='scheduler', value=_scheduler)


@app.on_event('shutdown')
async def shutdown():
    for instance in Default.instances.values():
        if isinstance(instance, Service):
            instance.storage.close()
    common.clear()


@app.get('/organize')
async def organize():
    common.supervisor.organize()
    return None


@app.get('/organize/{serial_number}')
async def organize(serial_number: str):
    common.supervisor.organize(serial_number=serial_number)
    return None


def destroy_supervisor(x):
    x.close()


def destroy_scheduler(x):
    if isinstance(x, Scheduler):
        if x.is_running():
            x.stop()
