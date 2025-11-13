import os
import json
from typing import Dict, List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI Backend!"}

@app.get("/api/hello")
def hello():
    return {"message": "Hello from the backend API!"}

@app.get("/test")
def test_database():
    """Test endpoint to check if database is available and accessible"""
    response = {
        "backend": "✅ Running",
        "database": "❌ Not Available",
        "database_url": None,
        "database_name": None,
        "connection_status": "Not Connected",
        "collections": []
    }
    
    try:
        # Try to import database module
        from database import db
        
        if db is not None:
            response["database"] = "✅ Available"
            response["database_url"] = "✅ Configured"
            response["database_name"] = db.name if hasattr(db, 'name') else "✅ Connected"
            response["connection_status"] = "Connected"
            
            # Try to list collections to verify connectivity
            try:
                collections = db.list_collection_names()
                response["collections"] = collections[:10]  # Show first 10 collections
                response["database"] = "✅ Connected & Working"
            except Exception as e:
                response["database"] = f"⚠️  Connected but Error: {str(e)[:50]}"
        else:
            response["database"] = "⚠️  Available but not initialized"
            
    except ImportError:
        response["database"] = "❌ Database module not found (run enable-database first)"
    except Exception as e:
        response["database"] = f"❌ Error: {str(e)[:50]}"
    
    # Check environment variables
    import os
    response["database_url"] = "✅ Set" if os.getenv("DATABASE_URL") else "❌ Not Set"
    response["database_name"] = "✅ Set" if os.getenv("DATABASE_NAME") else "❌ Not Set"
    
    return response

# ---- Simple in-memory room manager for realtime gameplay (ephemeral state) ----
class ConnectionManager:
    def __init__(self):
        # rooms: room_id -> list of WebSocket connections
        self.rooms: Dict[str, List[WebSocket]] = {}
        # player names per connection id
        self.players: Dict[int, str] = {}

    async def connect(self, room_id: str, websocket: WebSocket, player: str):
        await websocket.accept()
        if room_id not in self.rooms:
            self.rooms[room_id] = []
        self.rooms[room_id].append(websocket)
        self.players[id(websocket)] = player
        # notify others
        await self.broadcast(room_id, {
            "type": "system",
            "event": "join",
            "player": player
        })

    def disconnect(self, room_id: str, websocket: WebSocket):
        if room_id in self.rooms and websocket in self.rooms[room_id]:
            self.rooms[room_id].remove(websocket)
        player = self.players.pop(id(websocket), None)
        return player

    async def send_personal(self, websocket: WebSocket, data: dict):
        await websocket.send_text(json.dumps(data))

    async def broadcast(self, room_id: str, data: dict, exclude: WebSocket | None = None):
        if room_id not in self.rooms:
            return
        message = json.dumps(data)
        for conn in list(self.rooms[room_id]):
            if exclude is not None and conn is exclude:
                continue
            try:
                await conn.send_text(message)
            except Exception:
                # best-effort: drop broken sockets
                try:
                    self.rooms[room_id].remove(conn)
                except Exception:
                    pass

manager = ConnectionManager()

@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    # Expect the client to send an initial join message with player name
    player_name = None
    try:
        await websocket.accept()
        # first message should be join payload
        raw = await websocket.receive_text()
        msg = json.loads(raw)
        if msg.get("type") != "join":
            await websocket.send_text(json.dumps({"type": "error", "message": "First message must be type 'join'"}))
            await websocket.close()
            return
        player_name = (msg.get("player") or "Player").strip() or "Player"
        # re-accept under manager to broadcast join
        # note: we've already accepted once, so call manager.connect but skip accept
        # Adjust: use manager.connect but handle double-accept by not calling accept there if already
        # For simplicity keep manager.connect as is by not double-accept: we already accepted here, so patch:
        if room_id not in manager.rooms:
            manager.rooms[room_id] = []
        manager.rooms[room_id].append(websocket)
        manager.players[id(websocket)] = player_name
        await manager.broadcast(room_id, {"type": "system", "event": "join", "player": player_name}, exclude=None)
        # Acknowledge join
        await manager.send_personal(websocket, {"type": "joined", "room": room_id, "player": player_name})

        # Main loop: relay updates to everyone in room
        while True:
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
            except Exception:
                await manager.send_personal(websocket, {"type": "error", "message": "Invalid JSON"})
                continue

            mtype = payload.get("type")
            # Supported:
            # - update: {type:'update', player, x,y, angle, vx, vy}
            # - powerup: {type:'powerup', player, kind}
            # - chat: {type:'chat', player, text}
            if mtype in ("update", "powerup", "chat"):
                await manager.broadcast(room_id, payload, exclude=websocket if mtype == "update" else None)
            else:
                await manager.send_personal(websocket, {"type": "error", "message": f"Unknown type: {mtype}"})

    except WebSocketDisconnect:
        left_player = manager.disconnect(room_id, websocket)
        if left_player:
            await manager.broadcast(room_id, {"type": "system", "event": "leave", "player": left_player})
    except Exception:
        # On any other error, close socket politely
        try:
            await websocket.close()
        except Exception:
            pass


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
