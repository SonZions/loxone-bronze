from __future__ import annotations

import base64
import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def json_safe(value: Any) -> Any:
    """Convert loxwebsocket parser output into JSON-safe values."""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return {"__bytes_b64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            safe_key = json_safe(key)
            if not isinstance(safe_key, str):
                safe_key = json.dumps(safe_key, sort_keys=True, ensure_ascii=False)
            out[safe_key] = json_safe(item)
        return out
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def raw_envelope(message: Any, parsed: Any = None, captured_at: str | None = None) -> dict[str, Any]:
    """Preserve the transport payload and, where available, the library's parsed form."""
    if captured_at is None:
        captured_at = utc_now_iso()

    if isinstance(message, bytes):
        transport = {
            "encoding": "base64",
            "data": base64.b64encode(message).decode("ascii"),
        }
    else:
        transport = {
            "encoding": "utf8",
            "data": str(message),
        }

    return {
        "captured_at": captured_at,
        "transport": transport,
        "parsed": json_safe(parsed),
    }


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
