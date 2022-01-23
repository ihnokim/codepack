from fastapi import FastAPI, Request
from codepack import Code, CodePack
from typing import Tuple
import os
from codepack.service import DefaultService
from codepack.utils.config import get_default_service_config
from codepack.employee import Supervisor
from .routers import code
from .dependencies import common


# os.environ['CODEPACK_CONFIG_PATH'] = 'config/test.ini'
app = FastAPI()
app.include_router(code.router)


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
    code_storage_service = DefaultService.get_default_code_storage_service(obj=Code)
    if hasattr(code_storage_service, 'mongodb'):
        code_storage_service.mongodb.close()


@app.get('/organize')
async def organize():
    common.supervisor.organize()
    return None
