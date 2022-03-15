from fastapi import APIRouter
from ..models.scheduler import CodePackIDJob, CodePackJSONJob, SnapshotJSONJob, IDPairJob
from codepack.config import Default
from codepack import CodePack
from codepack.snapshot import CodePackSnapshot
from codepack.argpack import ArgPack
from codepack.scheduler import Scheduler
from ..dependencies import common
import requests
import json
import os


router = APIRouter(
    prefix='/scheduler',
    tags=['scheduler'],
    responses={404: {'description': 'Not found'}},
)


@router.post('/register')
async def register(params: CodePackJSONJob):
    if isinstance(common.scheduler, str):
        return redirect_to_remote_scheduler(requests.post, 'scheduler/register', data=json.dumps(params.dict()))
    elif isinstance(common.scheduler, Scheduler):
        codepack = CodePack.from_json(params.codepack)
        argpack = ArgPack.from_json(params.argpack)
        common.scheduler.add_codepack(codepack=codepack, argpack=argpack, job_id=params.job_id,
                                      trigger=params.trigger, **params.trigger_config)
        return {'serial_number': codepack.serial_number}
    else:
        raise TypeError(common.scheduler)


@router.post('/register/id')
async def register_by_id(params: CodePackIDJob):
    if isinstance(common.scheduler, str):
        return redirect_to_remote_scheduler(requests.post, 'scheduler/register/id', data=json.dumps(params.dict()))
    elif isinstance(common.scheduler, Scheduler):
        storage_service = Default.get_service('codepack', 'storage_service')
        codepack = storage_service.load(params.id)
        argpack = ArgPack.from_json(params.argpack)
        common.scheduler.add_codepack(codepack=codepack, argpack=argpack, job_id=params.job_id,
                                      trigger=params.trigger, **params.trigger_config)
        return {'serial_number': codepack.serial_number}
    else:
        raise TypeError(common.scheduler)


@router.post('/register/id-pair')
async def register_by_id_pair(params: IDPairJob):
    if isinstance(common.scheduler, str):
        return redirect_to_remote_scheduler(requests.post, 'scheduler/register/id-pair', data=json.dumps(params.dict()))
    elif isinstance(common.scheduler, Scheduler):
        codepack_storage_service = Default.get_service('codepack', 'storage_service')
        codepack = codepack_storage_service.load(params.codepack_id)
        argpack_storage_service = Default.get_service('argpack', 'storage_service')
        argpack = argpack_storage_service.load(params.argpack_id)
        common.scheduler.add_codepack(codepack=codepack, argpack=argpack, job_id=params.job_id,
                                      trigger=params.trigger, **params.trigger_config)
        return {'serial_number': codepack.serial_number}
    else:
        raise TypeError(common.scheduler)


@router.post('/register/snapshot')
async def register_by_snapshot(params: SnapshotJSONJob):
    if isinstance(common.scheduler, str):
        return redirect_to_remote_scheduler(requests.post, 'scheduler/register/snapshot', data=json.dumps(params.dict()))
    elif isinstance(common.scheduler, Scheduler):
        snapshot = CodePackSnapshot.from_json(params.snapshot)
        common.scheduler.add_codepack(snapshot=snapshot, job_id=params.job_id,
                                      trigger=params.trigger, **params.trigger_config)
        return {'serial_number': snapshot.serial_number}
    else:
        raise TypeError(common.scheduler)


@router.get('/unregister/{id}')
async def unregister(id: str):
    if isinstance(common.scheduler, str):
        return redirect_to_remote_scheduler(requests.get, 'scheduler/unregister/%s' % id)
    elif isinstance(common.scheduler, Scheduler):
        common.scheduler.remove_job(job_id=id)
        return {'id': id}
    else:
        raise TypeError(common.scheduler)


def redirect_to_remote_scheduler(method, url, **kwargs):
    return method(os.path.join(common.scheduler, url), **kwargs).json()
