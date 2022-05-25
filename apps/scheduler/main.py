from codepack import __version__
from codepack import Scheduler, Default
from codepack.plugins.service import Service
from fastapi import FastAPI, Request
from .routers import scheduler
from .dependencies import common


app = FastAPI(title='CodePack Scheduler', version=__version__)
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
    common.add(key='scheduler', value=Default.get_scheduler(), destroy=destroy_scheduler)
    common.scheduler.start()


@app.on_event('shutdown')
async def shutdown():
    for instance in Default.instances.values():
        if isinstance(instance, Service):
            instance.storage.close()
    common.clear()


def destroy_scheduler(x):
    if isinstance(x, Scheduler):
        if x.is_running():
            x.stop()
