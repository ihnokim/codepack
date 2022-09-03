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
    try:
        tmp.save()
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {'id': tmp.get_id()}


@router.patch('/update')
async def update(params: JsonArgPack):
    tmp = ArgPack.from_dict(params.argpack)
    tmp.save(update=True)
    return {'id': tmp.get_id()}


@router.delete('/remove/{id}')
async def remove(id: str):
    storage_service = Default.get_service('argpack', 'storage_service')
    if storage_service.check(id=id):
        ArgPack.remove(id=id)
    else:
        raise HTTPException(status_code=404, detail="%s not found" % id)
    return {'id': id}


@router.get('/load/{id}')
async def load(id: str):
    argpack = ArgPack.load(id=id)
    if argpack is None:
        raise HTTPException(status_code=404, detail="%s not found" % id)
    return argpack.to_dict()


@router.get('/search')
async def search(params: SearchQuery):
    storage_service = Default.get_service('code', 'storage_service')
    return storage_service.search(query=params.query, projection=params.projection)
