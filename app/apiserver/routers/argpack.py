from codepack import ArgPack
from fastapi import APIRouter, HTTPException
from ..models.argpack import ArgPackJSON


router = APIRouter(
    prefix='/argpack',
    tags=['argpack'],
    responses={404: {'description': 'Not found'}}
)


@router.post('/save')
async def save(argpack: ArgPackJSON):
    tmp = ArgPack.from_json(argpack.argpack)
    tmp.save()
    return {'id': tmp.id}


@router.post('/update')
async def update(argpack: ArgPackJSON):
    tmp = ArgPack.from_json(argpack.argpack)
    tmp.save(update=True)
    return {'id': tmp.id}


@router.get('/remove/{id}')
async def remove(id: str):
    ArgPack.remove(id)
    return {'id': id}


@router.get('/load/{id}')
async def load(id: str):
    argpack = ArgPack.load(id)
    if argpack is None:
        raise HTTPException(status_code=404, detail="'%s' not found" % id)
    return {'argpack': argpack.to_json()}
