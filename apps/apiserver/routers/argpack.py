from codepack import ArgPack
from fastapi import APIRouter, HTTPException
from ..models.argpack import ArgPackJSON


router = APIRouter(
    prefix='/argpack',
    tags=['argpack'],
    responses={404: {'description': 'Not Found'},
               409: {'description': 'Conflict'}}
)


@router.post('/save')
async def save(argpack: ArgPackJSON):
    tmp = ArgPack.from_json(argpack.argpack)
    try:
        tmp.save()
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return {'id': tmp.id}


@router.patch('/update')
async def update(argpack: ArgPackJSON):
    tmp = ArgPack.from_json(argpack.argpack)
    tmp.save(update=True)
    return {'id': tmp.id}


@router.delete('/remove/{id}')
async def remove(id: str):
    ArgPack.remove(id)
    return {'id': id}


@router.get('/load/{id}')
async def load(id: str):
    argpack = ArgPack.load(id)
    if argpack is None:
        raise HTTPException(status_code=404, detail="%s not found" % id)
    return {'argpack': argpack.to_json()}
