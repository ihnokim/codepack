from codepack.scheduler import Scheduler, get_default_scheduler
from codepack.config import Config
from fastapi import FastAPI, Request
from codepack.service import DefaultService
from codepack.employee import Supervisor
from .routers import code, codepack, argpack
from .dependencies import common


app = FastAPI()
app.include_router(code.router)
app.include_router(codepack.router)
app.include_router(argpack.router)


@app.middleware('http')
async def add_process_time_header(request: Request, call_next):
    response = await call_next(request)
    response.headers['Access-Control-Allow_Origin'] = '*'
    response.headers['Access-Control-Allow_Headers'] = 'Content-Type,Authorization'
    response.headers['Access-Control-Allow-Methods'] = 'GET,PUT,POST,DELETE'
    return response


@app.on_event('startup')
async def startup():
    common['config'] = Config()
    config = common['config'].get_config(section='apiservier')
    supervisor = config.get('supervisor', 'self')
    scheduler = config.get('scheduler', 'self')
    if isinstance(supervisor, Supervisor):
        common['supervisor'] = supervisor
    elif isinstance(supervisor, str):
        if supervisor == 'self':
            common['supervisor'] = Supervisor()
        else:
            raise NotImplementedError("supervisor should be 'self', not '%s'")
    if isinstance(scheduler, Scheduler):
        common['scheduler'] = scheduler
        common.scheduler.start()
    elif isinstance(scheduler, str):
        if scheduler == 'self':
            common['scheduler'] = get_default_scheduler()
            common.scheduler.start()
        else:
            common['scheduler'] = scheduler


@app.on_event('shutdown')
async def shutdown():
    common.supervisor.producer.close()
    services = [DefaultService.delivery_service,
                DefaultService.code_storage_service,
                DefaultService.code_snapshot_service,
                DefaultService.codepack_storage_service,
                DefaultService.codepack_snapshot_service,
                DefaultService.argpack_storage_service]
    for service in services:
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
