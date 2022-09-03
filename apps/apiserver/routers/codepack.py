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
    return {'serial_number': codepack.serial_number}


@router.post('/run/{id}')
async def run_by_id(id: str, params: JsonArgPack):
    codepack = CodePack.load(id)
    if codepack is None:
        raise HTTPException(status_code=404, detail='%s not found' % id)
    argpack = ArgPack.from_dict(params.argpack)
    common.supervisor.run_codepack(codepack=codepack, argpack=argpack)
    return {'serial_number': codepack.serial_number}


@router.post('/run/{codepack_id}/{argpack_id}')
async def run_by_id_pair(codepack_id: str, argpack_id: str):
    codepack = CodePack.load(codepack_id)
    if codepack is None:
        raise HTTPException(status_code=404, detail='%s not found' % codepack_id)
    argpack = ArgPack.load(argpack_id)
    if argpack is None:
        raise HTTPException(status_code=404, detail='%s not found' % argpack_id)
    common.supervisor.run_codepack(codepack=codepack, argpack=argpack)
    return {'serial_number': codepack.serial_number}


@router.post('/run/snapshot')
async def run_by_snapshot(params: JsonSnapshot):
    snapshot = CodePackSnapshot.from_dict(params.snapshot)
    codepack = CodePack.from_snapshot(snapshot)
    argpack = ArgPack.from_dict(snapshot.argpack)
    common.supervisor.run_codepack(codepack=codepack, argpack=argpack)
    return {'serial_number': codepack.serial_number}


@router.post('/save')
async def save(params: JsonCodePack):
    tmp = CodePack.from_dict(params.codepack)
    try:
        tmp.save()
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {'id': tmp.get_id()}


@router.patch('/update')
async def update(params: JsonCodePack):
    tmp = CodePack.from_dict(params.codepack)
    tmp.save(update=True)
    return {'id': tmp.get_id()}


@router.delete('/remove/{id}')
async def remove(id: str):
    storage_service = Default.get_service('codepack', 'storage_service')
    if storage_service.check(id=id):
        CodePack.remove(id=id)
    else:
        raise HTTPException(status_code=404, detail='%s not found' % id)
    return {'id': id}


@router.get('/load/{id}')
async def load(id: str):
    codepack = CodePack.load(id=id)
    if codepack is None:
        raise HTTPException(status_code=404, detail='%s not found' % id)
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
