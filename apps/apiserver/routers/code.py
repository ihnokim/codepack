from codepack import Default, Code, CodeSnapshot
from fastapi import APIRouter, HTTPException
from ..models.code import Args, JsonCode, JsonCodeAndArgs
from ..models import SearchQuery, JsonSnapshot
from ..dependencies import common


router = APIRouter(
    prefix='/code',
    tags=['code'],
    responses={404: {'description': 'Not Found'},
               409: {'description': 'Conflict'}},
)


@router.post('/run')
async def run(params: JsonCodeAndArgs):
    code = Code.from_dict(params.code)
    common.supervisor.run_code(code=code, args=params.args, kwargs=params.kwargs)
    return {'serial_number': code.get_serial_number()}


@router.post('/run/{name}')
async def run_by_name(name: str, params: Args):
    code = Code.load(name=name)
    if code is None:
        raise HTTPException(status_code=404, detail='%s not found' % name)
    common.supervisor.run_code(code=code, args=params.args, kwargs=params.kwargs)
    return {'serial_number': code.get_serial_number()}


@router.post('/run/snapshot')
async def run_by_snapshot(params: JsonSnapshot):
    snapshot = CodeSnapshot.from_dict(params.snapshot)
    code = Code.from_snapshot(snapshot)
    common.supervisor.run_code(code=code, args=snapshot.args, kwargs=snapshot.kwargs)
    return {'serial_number': code.get_serial_number()}


@router.post('/save')
async def save(params: JsonCode):
    tmp = Code.from_dict(params.code)
    try:
        tmp.save()
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {'name': tmp.get_name()}


@router.patch('/update')
async def update(params: JsonCode):
    tmp = Code.from_dict(params.code)
    tmp.save(update=True)
    return {'name': tmp.get_name()}


@router.delete('/remove/{name}')
async def remove(name: str):
    storage_service = Default.get_service('code', 'storage_service')
    if storage_service.check(name=name):
        Code.remove(name=name)
    else:
        raise HTTPException(status_code=404, detail='%s not found' % name)
    return {'name': name}


@router.get('/load/{name}')
async def load(name: str):
    code = Code.load(name=name)
    if code is None:
        raise HTTPException(status_code=404, detail='%s not found' % name)
    return code.to_dict()


@router.get('/search')
async def search(params: SearchQuery):
    storage_service = Default.get_service('code', 'storage_service')
    return storage_service.search(query=params.query, projection=params.projection)


@router.get('/state/{serial_number}')
async def state(serial_number: str):
    snapshot_service = Default.get_service('code_snapshot', 'snapshot_service')
    snapshot = snapshot_service.load(serial_number=serial_number, projection={'state', 'message'})
    ret = {'serial_number': serial_number}
    if snapshot:
        _state = snapshot['state']
        if snapshot['message']:
            ret['message'] = snapshot['message']
    else:
        _state = 'UNKNOWN'
    ret['state'] = _state
    return ret


@router.get('/result/{serial_number}')
async def result(serial_number: str):
    delivery_service = Default.get_service('delivery', 'delivery_service')
    tmp = delivery_service.check(serial_number=serial_number)
    if tmp:
        _result = delivery_service.receive(serial_number=serial_number)
    else:
        _result = None
    return {'serial_number': serial_number, 'result': _result}
