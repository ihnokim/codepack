from fastapi import APIRouter
from ..models.argpack import ArgPackJSON
from codepack.config import Default
from codepack.argpack import ArgPack


router = APIRouter(
    prefix='/argpack',
    tags=['argpack'],
    responses={404: {'description': 'Not found'}}
)


@router.post('/save')
async def save(argpack: ArgPackJSON):
    storage_service = Default.get_service('argpack', 'storage_service')
    tmp = ArgPack.from_json(argpack.argpack)
    storage_service.save(item=tmp)
    return {'id': tmp.id}


@router.post('/update')
async def update(argpack: ArgPackJSON):
    storage_service = Default.get_service('argpack', 'storage_service')
    tmp = ArgPack.from_json(argpack.argpack)
    storage_service.save(item=tmp, update=True)
    return {'id': tmp.id}


@router.get('/remove/{id}')
async def remove(id: str):
    storage_service = Default.get_service('argpack', 'storage_service')
    storage_service.remove(id)
    return {'id': id}


@router.get('/load/{id}')
async def load(id: str):
    storage_service = Default.get_service('argpack', 'storage_service')
    argpack = storage_service.load(id)
    return {'argpack': argpack.to_json()}
