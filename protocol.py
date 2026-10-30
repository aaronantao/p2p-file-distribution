"""Small newline-delimited JSON protocol shared by the tracker and peers."""

from __future__ import annotations

import json
import socket
from typing import Any


def send_json(conn: socket.socket, message: dict[str, Any]) -> None:
    conn.sendall((json.dumps(message, separators=(",", ":")) + "\n").encode())


def recv_json(conn: socket.socket) -> dict[str, Any]:
    data = b""
    while not data.endswith(b"\n"):
        block = conn.recv(64 * 1024)
        if not block:
            raise ConnectionError("connection closed before a full message arrived")
        data += block
        if len(data) > 2 * 1024 * 1024:
            raise ValueError("protocol message is too large")
    return json.loads(data.decode())
