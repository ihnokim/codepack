from codepack import CodePack, CodePackSnapshot, ArgPack, Scheduler
from fastapi import APIRouter, HTTPException
from apscheduler.jobstores.base import JobLookupError, ConflictingIdError
from ..models.scheduler import Job, JobWithJsonArgPack, JobWithJsonCodePackAndJsonArgPack, JobWithJsonSnapshot
from ..dependencies import common
import requests
import os


router = APIRouter(
    prefix='/scheduler',
    tags=['scheduler'],
    responses={404: {'description': 'Not Found'},
               409: {'description': 'Conflict'}},
)


@router.post('/register')
async def register(params: JobWithJsonCodePackAndJsonArgPack):
    if isinstance(common.scheduler, str):
        return redirect_to_remote_scheduler(requests.post, 'scheduler/register', json=params.dict())
    elif isinstance(common.scheduler, Scheduler):
        codepack = CodePack.from_dict(params.codepack)
        argpack = ArgPack.from_dict(params.argpack)
        try:
            common.scheduler.add_codepack(codepack=codepack, argpack=argpack, job_id=params.job_id,
                                          trigger=params.trigger, **params.trigger_config)
        except ConflictingIdError:
            _job_id = params.job_id if params.job_id else codepack.id
            raise HTTPException(status_code=409, detail='%s already exists' % _job_id)
        return {'serial_number': codepack.serial_number}
    else:
        raise TypeError(common.scheduler)


@router.post('/register/{id}')
async def register_by_id(id: str, params: JobWithJsonArgPack):
    if isinstance(common.scheduler, str):
        return redirect_to_remote_scheduler(requests.post, 'scheduler/register/id', json=params.dict())
    elif isinstance(common.scheduler, Scheduler):
        codepack = CodePack.load(id)
        if codepack is None:
            raise HTTPException(status_code=404, detail='%s not found' % id)
        argpack = ArgPack.from_dict(params.argpack)
        try:
            common.scheduler.add_codepack(codepack=codepack, argpack=argpack, job_id=params.job_id,
                                          trigger=params.trigger, **params.trigger_config)
        except ConflictingIdError:
            _job_id = params.job_id if params.job_id else codepack.id
            raise HTTPException(status_code=409, detail='%s already exists' % _job_id)
        return {'serial_number': codepack.serial_number}
    else:
        raise TypeError(common.scheduler)


@router.post('/register/{codepack_id}/{argpack_id}')
async def register_by_id_pair(codepack_id: str, argpack_id: str, params: Job):
    if isinstance(common.scheduler, str):
        return redirect_to_remote_scheduler(requests.post, 'scheduler/register/id-pair', json=params.dict())
    elif isinstance(common.scheduler, Scheduler):
        codepack = CodePack.load(codepack_id)
        if codepack is None:
            raise HTTPException(status_code=404, detail='%s not found' % codepack_id)
        argpack = ArgPack.load(argpack_id)
        if argpack is None:
            raise HTTPException(status_code=404, detail='%s not found' % argpack_id)
        try:
            common.scheduler.add_codepack(codepack=codepack, argpack=argpack, job_id=params.job_id,
                                          trigger=params.trigger, **params.trigger_config)
        except ConflictingIdError:
            _job_id = params.job_id if params.job_id else codepack.id
            raise HTTPException(status_code=409, detail='%s already exists' % _job_id)
        return {'serial_number': codepack.serial_number}
    else:
        raise TypeError(common.scheduler)


@router.post('/register/snapshot')
async def register_by_snapshot(params: JobWithJsonSnapshot):
    if isinstance(common.scheduler, str):
        return redirect_to_remote_scheduler(requests.post, 'scheduler/register/snapshot', json=params.dict())
    elif isinstance(common.scheduler, Scheduler):
        snapshot = CodePackSnapshot.from_dict(params.snapshot)
        try:
            common.scheduler.add_codepack(snapshot=snapshot, job_id=params.job_id,
                                          trigger=params.trigger, **params.trigger_config)
        except ConflictingIdError:
            _job_id = params.job_id if params.job_id else snapshot.id
            raise HTTPException(status_code=409, detail='%s already exists' % _job_id)
        return {'serial_number': snapshot.serial_number}
    else:
        raise TypeError(common.scheduler)


@router.delete('/unregister/{id}')
async def unregister(id: str):
    if isinstance(common.scheduler, str):
        return redirect_to_remote_scheduler(requests.delete, 'scheduler/unregister/%s' % id)
    elif isinstance(common.scheduler, Scheduler):
        try:
            common.scheduler.remove_job(job_id=id)
        except JobLookupError:
            raise HTTPException(status_code=404, detail='%s not found' % id)
        return {'id': id}
    else:
        raise TypeError(common.scheduler)


@router.get('/alive')
async def alive():
    if isinstance(common.scheduler, str):
        return redirect_to_remote_scheduler(requests.get, 'scheduler/alive')
    elif isinstance(common.scheduler, Scheduler):
        is_alive = common.scheduler.is_running()
        return {'alive': is_alive}
    else:
        raise TypeError(common.scheduler)


def redirect_to_remote_scheduler(method, url, **kwargs):
    return method(os.path.join(common.scheduler, url), **kwargs).json()
