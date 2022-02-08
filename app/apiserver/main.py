from codepack.scheduler import Scheduler
from codepack.config import Default
from fastapi import FastAPI, Request
from codepack.employee import Supervisor
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
    supervisor = config.get('supervisor', 'self')
    _scheduler = config.get('scheduler', 'self')
    if isinstance(supervisor, Supervisor):
        common['supervisor'] = supervisor
    elif isinstance(supervisor, str):
        if supervisor == 'self':
            common['supervisor'] = Default.get_employee('supervisor')
        else:
            raise NotImplementedError("supervisor should be 'self', not '%s'")
    if isinstance(_scheduler, Scheduler):
        common['scheduler'] = _scheduler
        common.scheduler.start()
    elif isinstance(_scheduler, str):
        if _scheduler == 'self':
            common['scheduler'] = Default.get_scheduler()
            common.scheduler.start()
        else:
            common['scheduler'] = _scheduler


@app.on_event('shutdown')
async def shutdown():
    common.supervisor.producer.close()
    for service in Default.instances.values():
        if hasattr(service, 'mongodb'):
            service.mongodb.close()
    if isinstance(common.scheduler, Scheduler):
        if common.scheduler.is_running():
            common.scheduler.stop()
        if hasattr(common.scheduler, 'mongodb'):
            common.scheduler.mongodb.close()


@app.get('/organize')
async def organize():
    common.supervisor.organize()
    return None


@app.get('/organize/{serial_number}')
async def organize(serial_number: str):
    common.supervisor.organize(serial_number=serial_number)
    return None
