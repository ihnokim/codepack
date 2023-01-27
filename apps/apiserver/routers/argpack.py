from codepack import ArgPack, Default
from fastapi import APIRouter, HTTPException
from ..models.argpack import JsonArgPack
from ..models import SearchQuery


router = APIRouter(
    prefix='/argpack',
    tags=['argpack'],
    responses={404: {'description': 'Not Found'},
               409: {'description': 'Conflict'}}
)


@router.post('/save')
async def save(params: JsonArgPack):
    tmp = ArgPack.from_dict(params.argpack)
    if tmp.get_name() is None:
        raise HTTPException(status_code=422, detail='name should not be null')
    try:
        tmp.save()
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {'name': tmp.get_name()}


@router.patch('/update')
async def update(params: JsonArgPack):
    tmp = ArgPack.from_dict(params.argpack)
    tmp.save(update=True)
    return {'name': tmp.get_name()}


@router.delete('/remove/{name}')
async def remove(name: str):
    storage_service = Default.get_service('argpack', 'storage_service')
    if storage_service.check(name=name):
        ArgPack.remove(name=name)
    else:
        raise HTTPException(status_code=404, detail="%s not found" % name)
    return {'name': name}


@router.get('/load/{name}')
async def load(name: str):
    argpack = ArgPack.load(name=name)
    if argpack is None:
        raise HTTPException(status_code=404, detail="%s not found" % name)
    return argpack.to_dict()


@router.get('/search')
async def search(params: SearchQuery):
    storage_service = Default.get_service('argpack', 'storage_service')
    return storage_service.search(query=params.query, projection=params.projection)
