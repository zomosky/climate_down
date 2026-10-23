"""Atomic receipts and redaction shared by the bounded CAMS worker."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re


def now() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


def save(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            h.update(block)
    return h.hexdigest()


def signature(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def safe_error(error: Exception, secret: str = "") -> dict:
    message = str(error)
    if secret:
        message = message.replace(secret, "[REDACTED]")
    message = re.sub(r"https?://[^\s\"'<>]+", "[URL_REDACTED]", message)
    message = re.sub(r"[\w.+-]+@[\w.-]+", "[EMAIL_REDACTED]", message)
    # Also cover common auth fields if a transport exception embeds headers.
    message = re.sub(r"(?i)(authorization|api[-_]?key|token)([\s\"':=]+)[^\s,}]+",
                     r"\1\2[REDACTED]", message)
    return {"type": type(error).__name__, "message": message[:1200]}
