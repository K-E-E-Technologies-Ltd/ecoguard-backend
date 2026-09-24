from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import hub

router = APIRouter(prefix='/stream', tags=['Realtime updates'])


@router.get('')
async def stream(request: Request):
    async def events():
        try:
            async for chunk in hub.subscribe():
                if await request.is_disconnected():
                    break
                yield chunk
        except Exception:
            pass

    return StreamingResponse(
        events(),
        media_type='text/event-stream',
        headers={
            'Cache-Control': 'no-cache, no-transform',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',
        },
    )