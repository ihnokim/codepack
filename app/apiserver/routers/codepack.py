from fastapi import APIRouter
from ..models.codepack import CodePackID, CodePackJSON, SnapshotJSON, IDPair
from codepack.config import Default
from codepack import CodePack
from codepack.snapshot import CodePackSnapshot
from codepack.argpack import ArgPack
from ..dependencies import common


router = APIRouter(
    prefix='/codepack',
    tags=['codepack'],
    responses={404: {'description': 'Not found'}},
)


@router.post('/run')
async def run(params: CodePackJSON):
    codepack = CodePack.from_json(params.codepack)
    argpack = ArgPack.from_json(params.argpack)
    common.supervisor.run_codepack(codepack=codepack, argpack=argpack)
    return {'serial_number': codepack.serial_number}


@router.post('/run/id')
async def run_by_id(params: CodePackID):
    storage_service = Default.get_service('codepack', 'storage_service')
    codepack = storage_service.load(params.id)
    argpack = ArgPack.from_json(params.argpack)
    common.supervisor.run_codepack(codepack=codepack, argpack=argpack)
    return {'serial_number': codepack.serial_number}


@router.post('/run/id-pair')
async def run_by_id_pair(params: IDPair):
    codepack_storage_service = Default.get_service('codepack', 'storage_service')
    codepack = codepack_storage_service.load(params.codepack_id)
    argpack_storage_service = Default.get_service('argpack', 'storage_service')
    argpack = argpack_storage_service.load(params.argpack_id)
    common.supervisor.run_codepack(codepack=codepack, argpack=argpack)
    return {'serial_number': codepack.serial_number}


@router.post('/run/snapshot')
async def run_by_snapshot(params: SnapshotJSON):
    snapshot = CodePackSnapshot.from_json(params.snapshot)
    codepack = CodePack.from_snapshot(snapshot)
    argpack = ArgPack.from_dict(snapshot.argpack)
    common.supervisor.run_codepack(codepack=codepack, argpack=argpack)
    return {'serial_number': codepack.serial_number}


@router.post('/save')
async def save(codepack: CodePackJSON):
    tmp = CodePack.from_json(codepack.codepack)
    tmp.save()
    return {'id': tmp.id}


@router.post('/update')
async def update(codepack: CodePackJSON):
    tmp = CodePack.from_json(codepack.codepack)
    tmp.save(update=True)
    return {'id': tmp.id}


@router.get('/remove/{id}')
async def remove(id: str):
    storage_service = Default.get_service('codepack', 'storage_service')
    storage_service.remove(id)
    return {'id': id}


@router.get('/load/{id}')
async def load(id: str):
    storage_service = Default.get_service('codepack', 'storage_service')
    codepack = storage_service.load(id)
    return {'codepack': codepack.to_json()}


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
