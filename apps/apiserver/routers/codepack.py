from codepack import Default, CodePack, CodePackSnapshot, ArgPack
from fastapi import APIRouter, HTTPException
from ..models.codepack import JsonCodePack, JsonCodePackAndJsonArgPack
from ..models.argpack import JsonArgPack
from ..models import SearchQuery, JsonSnapshot
from ..dependencies import common


router = APIRouter(
    prefix='/codepack',
    tags=['codepack'],
    responses={404: {'description': 'Not Found'},
               409: {'description': 'Conflict'}},
)


@router.post('/run')
async def run(params: JsonCodePackAndJsonArgPack):
    codepack = CodePack.from_dict(params.codepack)
    argpack = ArgPack.from_dict(params.argpack)
    common.supervisor.run_codepack(codepack=codepack, argpack=argpack)
    return {'serial_number': codepack.get_serial_number()}


@router.post('/run/{name}')
async def run_by_name(name: str, params: JsonArgPack):
    codepack = CodePack.load(name=name)
    if codepack is None:
        raise HTTPException(status_code=404, detail='%s not found' % name)
    argpack = ArgPack.from_dict(params.argpack)
    common.supervisor.run_codepack(codepack=codepack, argpack=argpack)
    return {'serial_number': codepack.get_serial_number()}


@router.post('/run/{codepack_name}/{argpack_name}')
async def run_by_name_pair(codepack_name: str, argpack_name: str):
    codepack = CodePack.load(codepack_name)
    if codepack is None:
        raise HTTPException(status_code=404, detail='%s not found' % codepack_name)
    argpack = ArgPack.load(argpack_name)
    if argpack is None:
        raise HTTPException(status_code=404, detail='%s not found' % argpack_name)
    common.supervisor.run_codepack(codepack=codepack, argpack=argpack)
    return {'serial_number': codepack.get_serial_number()}


@router.post('/run/snapshot')
async def run_by_snapshot(params: JsonSnapshot):
    snapshot = CodePackSnapshot.from_dict(params.snapshot)
    codepack = CodePack.from_snapshot(snapshot)
    argpack = ArgPack.from_dict(snapshot.argpack)
    common.supervisor.run_codepack(codepack=codepack, argpack=argpack)
    return {'serial_number': codepack.get_serial_number()}


@router.post('/save')
async def save(params: JsonCodePack):
    tmp = CodePack.from_dict(params.codepack)
    try:
        tmp.save()
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {'name': tmp.get_name()}


@router.patch('/update')
async def update(params: JsonCodePack):
    tmp = CodePack.from_dict(params.codepack)
    tmp.save(update=True)
    return {'name': tmp.get_name()}


@router.delete('/remove/{name}')
async def remove(name: str):
    storage_service = Default.get_service('codepack', 'storage_service')
    if storage_service.check(name=name):
        CodePack.remove(name=name)
    else:
        raise HTTPException(status_code=404, detail='%s not found' % name)
    return {'name': name}


@router.get('/load/{name}')
async def load(name: str):
    codepack = CodePack.load(name=name)
    if codepack is None:
        raise HTTPException(status_code=404, detail='%s not found' % name)
    return codepack.to_dict()


@router.get('/search')
async def search(params: SearchQuery):
    storage_service = Default.get_service('code', 'storage_service')
    return storage_service.search(query=params.query, projection=params.projection)


@router.get('/state/{serial_number}')
async def state(serial_number: str):
    snapshot_service = Default.get_service('codepack_snapshot', 'snapshot_service')
    tmp = snapshot_service.load(serial_number=serial_number)
    ret = {'serial_number': serial_number}
    if tmp:
        codepack_snapshot = CodePackSnapshot.from_dict(tmp)
        codepack = CodePack.from_snapshot(codepack_snapshot)
        _state = codepack.get_state()
        if _state == 'ERROR':
            ret['message'] = codepack.get_message()
    else:
        _state = 'UNKNOWN'
    ret['state'] = _state
    return ret


@router.get('/result/{serial_number}')
async def result(serial_number: str):
    snapshot_service = Default.get_service('codepack_snapshot', 'snapshot_service')
    snapshot = snapshot_service.load(serial_number=serial_number)
    _result = None
    if snapshot and snapshot['subscribe']:
        codepack_snapshot = CodePackSnapshot.from_dict(snapshot)
        codepack = CodePack.from_snapshot(codepack_snapshot)
        _result = codepack.get_result()
    return {'serial_number': serial_number, 'result': _result}
