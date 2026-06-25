from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
import httpx

app = FastAPI()

VPS_IP = "45.84.88.133"
VPS_PORT = "8080" # پورتی که در ۳اکس‌یو‌آی ست کردی
TARGET_URL = f"http://{VPS_IP}:{VPS_PORT}"

@app.all("/{path:path}")
async def gateway(path: str, request: Request):
    headers = dict(request.headers)
    method = request.method
    headers["host"] = f"{VPS_IP}:{VPS_PORT}"
    body = await request.body()
    
    async def stream_request():
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream(method, f"{TARGET_URL}/{path}", headers=headers, content=body) as response:
                async for chunk in response.aiter_bytes():
                    yield chunk

    return StreamingResponse(stream_request(), status_code=200)
