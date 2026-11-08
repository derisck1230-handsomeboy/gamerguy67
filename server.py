#!/usr/bin/env python3
"""
大航海期貨所 - 聯機伺服器（Render / 本機皆可用）
- HTTP：提供靜態網頁（lobby / game）
- WebSocket：房間、同步、重連
"""
from __future__ import annotations

import asyncio
import json
import mimetypes
import os
import random
from pathlib import Path
from typing import Any, Dict, Optional, Set

import websockets
from websockets.asyncio.server import ServerConnection

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8766"))

def _find_static_dir():
    here = Path(__file__).resolve().parent
    for c in (here, here.parent):
        if (c / "lobby.html").is_file():
            return c
    return here

STATIC_DIR = _find_static_dir()

rooms: Dict[str, Dict[str, Any]] = {}
clients: Dict[ServerConnection, Dict[str, Any]] = {}


def gen_code() -> str:
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    for _ in range(50):
        code = "SAIL-" + "".join(random.choice(chars) for _ in range(4))
        if code not in rooms:
            return code
    return "SAIL-" + "".join(random.choice(chars) for _ in range(6))


def public_room(room: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "code": room["code"],
        "maxPlayers": room["maxPlayers"],
        "status": room["status"],
        "players": room["players"],
        "hostId": room["hostId"],
    }


async def send(ws: ServerConnection, payload: Dict[str, Any]) -> None:
    try:
        await ws.send(json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass


async def broadcast_room(
    code: str,
    payload: Dict[str, Any],
    except_ws: Optional[ServerConnection] = None,
) -> None:
    room = rooms.get(code)
    if not room:
        return
    dead: Set[ServerConnection] = set()
    raw = json.dumps(payload, ensure_ascii=False)
    for ws in list(room["clients"]):
        if except_ws is not None and ws is except_ws:
            continue
        try:
            await ws.send(raw)
        except Exception:
            dead.add(ws)
    for ws in dead:
        room["clients"].discard(ws)
        clients.pop(ws, None)


async def handle_create(ws: ServerConnection, data: Dict[str, Any]) -> None:
    name = (data.get("playerName") or "船長").strip()[:12] or "船長"
    try:
        max_players = int(data.get("maxPlayers") or 4)
    except Exception:
        max_players = 4
    max_players = max(2, min(5, max_players))

    code = gen_code()
    player = {
        "id": 1,
        "name": name,
        "isHost": True,
        "cash": 1000,
        "debt": 0,
        "holdings": {},
        "job": None,
        "jobTitle": "",
        "jobs": [],
    }
    room = {
        "code": code,
        "maxPlayers": max_players,
        "status": "waiting",
        "hostId": 1,
        "nextId": 2,
        "players": [player],
        "clients": {ws},
        "gameState": None,
    }
    rooms[code] = room
    clients[ws] = {"code": code, "playerId": 1, "name": name}

    await send(ws, {"type": "created", "playerId": 1, "room": public_room(room)})


async def handle_join(ws: ServerConnection, data: Dict[str, Any]) -> None:
    code = (data.get("code") or "").strip().upper()
    name = (data.get("playerName") or "航海士").strip()[:12] or "航海士"
    room = rooms.get(code)
    if not room:
        await send(ws, {"type": "error", "message": "房間不存在"})
        return
    if room["status"] != "waiting":
        await send(ws, {"type": "error", "message": "遊戲已開始，無法加入（可用重連）"})
        return
    if len(room["players"]) >= room["maxPlayers"]:
        await send(ws, {"type": "error", "message": "房間已滿"})
        return
    if any(p["name"] == name for p in room["players"]):
        name = f"{name}{random.randint(1, 99)}"

    pid = room["nextId"]
    room["nextId"] += 1
    player = {
        "id": pid,
        "name": name,
        "isHost": False,
        "cash": 1000,
        "debt": 0,
        "holdings": {},
        "job": None,
        "jobTitle": "",
        "jobs": [],
    }
    room["players"].append(player)
    room["clients"].add(ws)
    clients[ws] = {"code": code, "playerId": pid, "name": name}

    await send(ws, {"type": "joined", "playerId": pid, "room": public_room(room)})
    await broadcast_room(code, {"type": "room_update", "room": public_room(room)})


async def handle_reconnect(ws: ServerConnection, data: Dict[str, Any]) -> None:
    code = (data.get("code") or "").strip().upper()
    try:
        pid = int(data.get("playerId"))
    except Exception:
        await send(ws, {"type": "error", "message": "重連參數錯誤"})
        return
    room = rooms.get(code)
    if not room:
        await send(ws, {"type": "error", "message": "房間不存在或已解散"})
        return
    player = next((p for p in room["players"] if p["id"] == pid), None)
    if not player:
        await send(ws, {"type": "error", "message": "玩家不在此房間"})
        return

    room["clients"].add(ws)
    clients[ws] = {"code": code, "playerId": pid, "name": player["name"]}
    await send(
        ws,
        {
            "type": "reconnected",
            "playerId": pid,
            "room": public_room(room),
            "gameState": room.get("gameState"),
            "status": room["status"],
        },
    )
    await broadcast_room(
        code,
        {"type": "player_online", "playerId": pid, "name": player["name"]},
        except_ws=ws,
    )


async def handle_start(ws: ServerConnection, data: Dict[str, Any]) -> None:
    meta = clients.get(ws)
    if not meta:
        return
    room = rooms.get(meta["code"])
    if not room:
        return
    if meta["playerId"] != room["hostId"]:
        await send(ws, {"type": "error", "message": "只有房主可以開始遊戲"})
        return
    if len(room["players"]) < 2:
        await send(ws, {"type": "error", "message": "至少需要 2 位玩家"})
        return
    room["status"] = "playing"
    await broadcast_room(meta["code"], {"type": "game_start", "room": public_room(room)})


async def handle_game_state(ws: ServerConnection, data: Dict[str, Any]) -> None:
    meta = clients.get(ws)
    if not meta:
        return
    room = rooms.get(meta["code"])
    if not room:
        return
    state = data.get("state")
    if state is None:
        return
    room["gameState"] = state
    await broadcast_room(
        meta["code"],
        {"type": "game_state", "fromPlayerId": meta["playerId"], "state": state},
        except_ws=ws,
    )


async def handle_game_event(ws: ServerConnection, data: Dict[str, Any]) -> None:
    meta = clients.get(ws)
    if not meta:
        return
    room = rooms.get(meta["code"])
    if not room:
        return
    await broadcast_room(
        meta["code"],
        {
            "type": "game_event",
            "fromPlayerId": meta["playerId"],
            "event": data.get("event"),
            "payload": data.get("payload"),
        },
        except_ws=ws,
    )


async def handle_chat(ws: ServerConnection, data: Dict[str, Any]) -> None:
    meta = clients.get(ws)
    if not meta:
        return
    text = (data.get("text") or "").strip()[:200]
    if not text:
        return
    await broadcast_room(
        meta["code"],
        {
            "type": "chat",
            "playerId": meta["playerId"],
            "name": meta["name"],
            "text": text,
        },
    )


async def on_message(ws: ServerConnection, raw: str) -> None:
    try:
        data = json.loads(raw)
    except Exception:
        await send(ws, {"type": "error", "message": "訊息格式錯誤"})
        return
    t = data.get("type")
    if t == "create":
        await handle_create(ws, data)
    elif t == "join":
        await handle_join(ws, data)
    elif t == "reconnect":
        await handle_reconnect(ws, data)
    elif t == "start":
        await handle_start(ws, data)
    elif t == "game_state":
        await handle_game_state(ws, data)
    elif t == "game_event":
        await handle_game_event(ws, data)
    elif t == "chat":
        await handle_chat(ws, data)
    elif t == "ping":
        await send(ws, {"type": "pong"})
    else:
        await send(ws, {"type": "error", "message": f"未知指令: {t}"})


def safe_static_path(url_path: str) -> Optional[Path]:
    """將 URL 路徑對應到 STATIC_DIR 內檔案，防止 path traversal。"""
    path = url_path.split("?", 1)[0]
    if path in ("", "/"):
        path = "/lobby.html"
    # 去掉開頭 /
    rel = path.lstrip("/")
    if ".." in rel.split("/"):
        return None
    candidate = (STATIC_DIR / rel).resolve()
    try:
        candidate.relative_to(STATIC_DIR.resolve())
    except ValueError:
        return None
    if candidate.is_file():
        return candidate
    return None


async def process_request(connection, request):
    """
    HTTP 請求：回靜態檔或健康檢查；WebSocket 升級則回 None 繼續握手。
    """
    from websockets.http11 import Response, Headers

    path = request.path or "/"
    upgrade = request.headers.get("Upgrade", "").lower()
    if upgrade == "websocket":
        return None  # 走 WebSocket

    if path in ("/health", "/healthz"):
        return Response(200, "OK", Headers([("Content-Type", "text/plain; charset=utf-8")]), b"ok")

    file_path = safe_static_path(path)
    if not file_path:
        return Response(404, "Not Found", Headers([("Content-Type", "text/plain; charset=utf-8")]), b"Not Found")

    data = file_path.read_bytes()
    ctype, _ = mimetypes.guess_type(str(file_path))
    if not ctype:
        ctype = "application/octet-stream"
    if ctype.startswith("text/") or ctype in ("application/javascript", "application/json"):
        ctype = ctype + "; charset=utf-8"
    headers = Headers([
        ("Content-Type", ctype),
        ("Content-Length", str(len(data))),
        ("Cache-Control", "no-cache"),
    ])
    return Response(200, "OK", headers, data)


async def on_connect(ws: ServerConnection) -> None:
    clients[ws] = {}
    await send(
        ws,
        {
            "type": "hello",
            "message": "大航海期貨所聯機伺服器",
            "port": PORT,
        },
    )
    try:
        async for raw in ws:
            await on_message(ws, raw)
    finally:
        meta = clients.pop(ws, None) or {}
        code = meta.get("code")
        pid = meta.get("playerId")
        if not code or code not in rooms:
            return
        room = rooms[code]
        room["clients"].discard(ws)
        await broadcast_room(
            code,
            {
                "type": "player_offline",
                "playerId": pid,
                "name": meta.get("name"),
            },
        )

async def main() -> None:
    print(f"[期貨所] STATIC={STATIC_DIR}")
    print(f"[期貨所] http://0.0.0.0:{PORT}/lobby.html")
    print(f"[期貨所] ws://0.0.0.0:{PORT}/")
    async with websockets.serve(
        on_connect,
        HOST,
        PORT,
        process_request=process_request,
        ping_interval=20,
        ping_timeout=20,
    ):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
