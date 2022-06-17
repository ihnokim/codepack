from codepack import CodePack, CodePackSnapshot, ArgPack
from fastapi import APIRouter, HTTPException
from ..models.scheduler import CodePackIDJob, CodePackJSONJob, SnapshotJSONJob, IDPairJob
from apscheduler.jobstores.base import JobLookupError, ConflictingIdError
from ..dependencies import common


router = APIRouter(
    prefix='/scheduler',
    tags=['scheduler'],
    responses={404: {'description': 'Not Found'},
               409: {'description': 'Conflict'}},
)


@router.post('/register')
async def register(params: CodePackJSONJob):
    codepack = CodePack.from_dict(params.codepack)
    argpack = ArgPack.from_dict(params.argpack)
    try:
        common.scheduler.add_codepack(codepack=codepack, argpack=argpack, job_id=params.job_id,
                                      trigger=params.trigger, **params.trigger_config)
    except ConflictingIdError:
        _job_id = params.job_id if params.job_id else codepack.id
        raise HTTPException(status_code=409, detail='%s already exists' % _job_id)
    return {'serial_number': codepack.serial_number}


@router.post('/register/id')
async def register_by_id(params: CodePackIDJob):
    codepack = CodePack.load(params.id)
    if codepack is None:
        raise HTTPException(status_code=404, detail="'%s' not found" % params.id)
    argpack = ArgPack.from_dict(params.argpack)
    try:
        common.scheduler.add_codepack(codepack=codepack, argpack=argpack, job_id=params.job_id,
                                      trigger=params.trigger, **params.trigger_config)
    except ConflictingIdError:
        _job_id = params.job_id if params.job_id else codepack.id
        raise HTTPException(status_code=409, detail='%s already exists' % _job_id)
    return {'serial_number': codepack.serial_number}


@router.post('/register/id-pair')
async def register_by_id_pair(params: IDPairJob):
    codepack = CodePack.load(params.codepack_id)
    if codepack is None:
        raise HTTPException(status_code=404, detail="'%s' not found" % params.codepack_id)
    argpack = ArgPack.load(params.argpack_id)
    if argpack is None:
        raise HTTPException(status_code=404, detail="'%s' not found" % params.argpack_id)
    try:
        common.scheduler.add_codepack(codepack=codepack, argpack=argpack, job_id=params.job_id,
                                      trigger=params.trigger, **params.trigger_config)
    except ConflictingIdError:
        _job_id = params.job_id if params.job_id else codepack.id
        raise HTTPException(status_code=409, detail='%s already exists' % _job_id)
    return {'serial_number': codepack.serial_number}


@router.post('/register/snapshot')
async def register_by_snapshot(params: SnapshotJSONJob):
    snapshot = CodePackSnapshot.from_dict(params.snapshot)
    try:
        common.scheduler.add_codepack(snapshot=snapshot, job_id=params.job_id,
                                      trigger=params.trigger, **params.trigger_config)
    except ConflictingIdError:
        _job_id = params.job_id if params.job_id else snapshot.id
        raise HTTPException(status_code=409, detail='%s already exists' % _job_id)
    return {'serial_number': snapshot.serial_number}


@router.delete('/unregister/{id}')
async def unregister(id: str):
    try:
        common.scheduler.remove_job(job_id=id)
    except JobLookupError:
        raise HTTPException(status_code=404, detail='%s not found' % id)
    return {'id': id}


@router.get('/alive')
async def alive():
    is_alive = common.scheduler.is_running()
    return {'alive': is_alive}
