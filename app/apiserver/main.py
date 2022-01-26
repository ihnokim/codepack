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
    common['supervisor'] = Supervisor()


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


@app.get('/organize')
async def organize():
    common.supervisor.organize()
    return None


@app.get('/organize/{serial_number}')
async def organize(serial_number: str):
    common.supervisor.organize(serial_number=serial_number)
    return None
