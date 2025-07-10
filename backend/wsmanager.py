from fastapi import WebSocket
from typing import Set

connectedClients: Set[WebSocket] = set()

async def pushTick(tick: dict):
    for ws in list(connectedClients):
        try:
            await ws.send_json(tick)
            print(f"✅ Pushed tick to {ws.client.host}", flush=True)
        except Exception:
            connectedClients.remove(ws)