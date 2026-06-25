from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import StreamingResponse
from starlette.websockets import WebSocketDisconnect
import httpx
import websockets
import asyncio

app = FastAPI()

VPS_IP = "45.84.88.133"
VPS_PORT = "8080"

# ۱. مدیریت کانکشن‌های وب‌ساکت فیلترشکن (حیاتی برای اینباند شما)
@app.websocket("/{path:path}")
async def websocket_gateway(websocket: WebSocket, path: str):
    await websocket.accept()
    
    # ساختن آدرس وب‌ساکت سرور اصلی
    target_ws_url = f"ws://{VPS_IP}:{VPS_PORT}/{path}"
    
    try:
        # اتصال مستقیم به Xray سرور
        async with websockets.connect(target_ws_url) as vps_ws:
            
            # لوله‌کشی دوجانبه دیتا بین کلاینت ایران و سرور اصلی
            async def forward_to_vps():
                try:
                    while True:
                        data = await websocket.receive_bytes()
                        await vps_ws.send(data)
                except Exception:
                    pass

            async def forward_to_client():
                try:
                    while True:
                        data = await vps_ws.recv()
                        await websocket.send_bytes(data)
                except Exception:
                    pass

            await asyncio.gather(forward_to_vps(), forward_to_client())
            
    except WebSocketDisconnect:
        await websocket.close()
    except Exception:
        await websocket.close()

# ۲. مدیریت درخواست‌های عادی HTTP (برای زنده ماندن ظاهر سایت)
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def http_gateway(path: str, request: Request):
    headers = dict(request.headers)
    method = request.method
    headers["host"] = f"{VPS_IP}:{VPS_PORT}"
    body = await request.body()
    
    async def stream_request():
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream(method, f"http://{VPS_IP}:{VPS_PORT}/{path}", headers=headers, content=body) as response:
                async for chunk in response.aiter_bytes():
                    yield chunk

    return StreamingResponse(stream_request(), status_code=200)
