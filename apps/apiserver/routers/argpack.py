from codepack import ArgPack, Default
from fastapi import APIRouter, HTTPException
from ..models.argpack import ArgPackJSON
from ..models import SearchQuery


router = APIRouter(
    prefix='/argpack',
    tags=['argpack'],
    responses={404: {'description': 'Not Found'},
               409: {'description': 'Conflict'}}
)


@router.post('/save')
async def save(argpack: ArgPackJSON):
    tmp = ArgPack.from_dict(argpack.argpack)
    try:
        tmp.save()
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {'id': tmp.id}


@router.patch('/update')
async def update(argpack: ArgPackJSON):
    tmp = ArgPack.from_dict(argpack.argpack)
    tmp.save(update=True)
    return {'id': tmp.id}


@router.delete('/remove/{id}')
async def remove(id: str):
    storage_service = Default.get_service('argpack', 'storage_service')
    if storage_service.check(id=id):
        ArgPack.remove(id)
    else:
        raise HTTPException(status_code=404, detail="%s not found" % id)
    return {'id': id}


@router.get('/load/{id}')
async def load(id: str):
    argpack = ArgPack.load(id)
    if argpack is None:
        raise HTTPException(status_code=404, detail="%s not found" % id)
    return argpack.to_dict()


@router.get('/search')
async def search(search_query: SearchQuery):
    storage_service = Default.get_service('code', 'storage_service')
    return storage_service.search(query=search_query.query, projection=search_query.projection)
