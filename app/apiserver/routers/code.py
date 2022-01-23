from fastapi import APIRouter, Depends, HTTPException
from ..models.code import NewCodeFromStorageToRun
from codepack.service import DefaultService
from codepack import Code
from ..dependencies import common


router = APIRouter(
    prefix="/code",
    tags=['code'],
    response={404: {'description': 'Not found'}},
)


@router.post('/run')
async def run(params: NewCodeFromStorageToRun):
    storage_service = DefaultService.get_default_code_storage_service(obj=Code)
    code = storage_service.load(params.id)
    common.supervisor.order(code=code, args=params.args, kwargs=params.kwargs)
    return {'serial_number': code.serial_number}


@router.get('/state/{serial_number}')
async def state(serial_number: str):
    snapshot_service = DefaultService.get_default_code_snapshot_service()
    ret = snapshot_service.load(serial_number=serial_number, projection={'state'})
    if ret:
        _state = ret['state']
    else:
        _state = 'UNKNOWN'
    return {'seriual_number': serial_number, 'state': _state}


@router.get('/result/{serial_number}')
async def result(serial_number: str):
    delivery_service = DefaultService.get_default_delivery_service()
    _result = delivery_service.receive(serial_number=serial_number)
    return {'serial_number': serial_number, 'result': _result}
