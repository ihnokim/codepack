from fastapi import APIRouter
from ..models.scheduler import CodePackIDJob, CodePackJSONJob, SnapshotJSONJob, IDPairJob
from codepack.config import Default
from codepack import CodePack
from codepack.snapshot import CodePackSnapshot
from codepack.argpack import ArgPack
from ..dependencies import common


router = APIRouter(
    prefix='/scheduler',
    tags=['scheduler'],
    responses={404: {'description': 'Not found'}},
)


@router.post('/register')
async def register(params: CodePackJSONJob):
    codepack = CodePack.from_json(params.codepack)
    argpack = ArgPack.from_json(params.argpack)
    common.scheduler.add_codepack(codepack=codepack, argpack=argpack, job_id=params.job_id,
                                  trigger=params.trigger, **params.trigger_config)
    return {'serial_number': codepack.serial_number}


@router.post('/register/id')
async def register_by_id(params: CodePackIDJob):
    storage_service = Default.get_service('codepack', 'storage_service')
    codepack = storage_service.load(params.id)
    argpack = ArgPack.from_json(params.argpack)
    common.scheduler.add_codepack(codepack=codepack, argpack=argpack, job_id=params.job_id,
                                  trigger=params.trigger, **params.trigger_config)
    return {'serial_number': codepack.serial_number}


@router.post('/register/id-pair')
async def register_by_id_pair(params: IDPairJob):
    codepack_storage_service = Default.get_service('codepack', 'storage_service')
    codepack = codepack_storage_service.load(params.codepack_id)
    argpack_storage_service = Default.get_service('argpack', 'storage_service')
    argpack = argpack_storage_service.load(params.argpack_id)
    common.scheduler.add_codepack(codepack=codepack, argpack=argpack, job_id=params.job_id,
                                  trigger=params.trigger, **params.trigger_config)
    return {'serial_number': codepack.serial_number}


@router.post('/register/snapshot')
async def register_by_snapshot(params: SnapshotJSONJob):
    snapshot = CodePackSnapshot.from_json(params.snapshot)
    common.scheduler.add_codepack(snapshot=snapshot, job_id=params.job_id,
                                  trigger=params.trigger, **params.trigger_config)
    return {'serial_number': snapshot.serial_number}


@router.get('/unregister/{id}')
async def unregister(id: str):
    common.scheduler.remove_job(job_id=id)
    return {'id': id}


@router.get('/alive')
async def alive():
    is_alive = common.scheduler.is_running()
    return {'alive': is_alive}
