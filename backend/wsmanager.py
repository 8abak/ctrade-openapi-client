from fastapi import WebSocket
from typing import Set

connectedClients: Set[WebSocket] = set()

async def pushTick(tick: dict):
    print(f"📡 Total clients: {len(connectedClients)}", flush=True)
    for ws in list(connectedClients):
        try:
            await ws.send_json(tick)
            print(f"✅ Pushing tick to all clients: {tick}", flush=True)
        except Exception:
            connectedClients.remove(ws)