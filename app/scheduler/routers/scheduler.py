from codepack import CodePack, CodePackSnapshot, ArgPack
from fastapi import APIRouter
from ..models.scheduler import CodePackIDJob, CodePackJSONJob, SnapshotJSONJob, IDPairJob
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
    codepack = CodePack.load(params.id)
    argpack = ArgPack.from_json(params.argpack)
    common.scheduler.add_codepack(codepack=codepack, argpack=argpack, job_id=params.job_id,
                                  trigger=params.trigger, **params.trigger_config)
    return {'serial_number': codepack.serial_number}


@router.post('/register/id-pair')
async def register_by_id_pair(params: IDPairJob):
    codepack = CodePack.load(params.codepack_id)
    argpack = ArgPack.load(params.argpack_id)
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
