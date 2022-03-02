from fastapi import APIRouter
from ..models.code import CodeID, CodeJSON, SnapshotJSON
from codepack.config import Default
from codepack import Code
from codepack.snapshot import CodeSnapshot
from ..dependencies import common


router = APIRouter(
    prefix='/code',
    tags=['code'],
    responses={404: {'description': 'Not found'}},
)


@router.post('/run')
async def run(params: CodeJSON):
    code = Code.from_json(params.code)
    common.supervisor.run_code(code=code, args=params.args, kwargs=params.kwargs)
    return {'serial_number': code.serial_number}


@router.post('/run/id')
async def run_by_id(params: CodeID):
    storage_service = Default.get_service('code', 'storage_service')
    code = storage_service.load(params.id)
    common.supervisor.run_code(code=code, args=params.args, kwargs=params.kwargs)
    return {'serial_number': code.serial_number}


@router.post('/run/snapshot')
async def run_by_snapshot(params: SnapshotJSON):
    snapshot = CodeSnapshot.from_json(params.snapshot)
    code = Code.from_snapshot(snapshot)
    common.supervisor.run_code(code=code, args=snapshot.args, kwargs=snapshot.kwargs)
    return {'serial_number': code.serial_number}


@router.post('/save')
async def save(code: CodeJSON):
    tmp = Code.from_json(code.code)
    tmp.save()
    return {'id': tmp.id}


@router.post('/update')
async def update(code: CodeJSON):
    tmp = Code.from_json(code.code)
    tmp.save(update=True)
    return {'id': tmp.id}


@router.get('/remove/{id}')
async def remove(id: str):
    storage_service = Default.get_service('code', 'storage_service')
    storage_service.remove(id)
    return {'id': id}


@router.get('/load/{id}')
async def load(id: str):
    code = Code(id=id)
    return {'code': code.to_json()}


@router.get('/state/{serial_number}')
async def state(serial_number: str):
    snapshot_service = Default.get_service('code_snapshot', 'snapshot_service')
    ret = snapshot_service.load(serial_number=serial_number, projection={'state'})
    if ret:
        _state = ret['state']
    else:
        _state = 'UNKNOWN'
    return {'seriual_number': serial_number, 'state': _state}


@router.get('/result/{serial_number}')
async def result(serial_number: str):
    delivery_service = Default.get_service('delivery', 'delivery_service')
    tmp = delivery_service.check(serial_number=serial_number)
    if tmp:
        _result = delivery_service.receive(serial_number=serial_number)
    else:
        _result = None
    return {'serial_number': serial_number, 'result': _result}
