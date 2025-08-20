from __future__ import annotations
import hmac, hashlib, time, os
from pathlib import Path
from typing import Tuple
from fastapi import UploadFile
from app.core.config import settings


def _safe_name(name: str) -> str:
    keep = (c if c.isalnum() or c in ("-", "_", ".", "+") else "_" for c in name)
    out = "".join(keep)
    return out or "file"


def artifact_dir_for(run_id: int, block_run_id: int | None) -> Path:
    base = Path(settings.ARTIFACTS_DIR).expanduser().resolve()
    sub = (
        base / str(run_id) / (str(block_run_id) if block_run_id is not None else "run")
    )
    sub.mkdir(parents=True, exist_ok=True)
    return sub


def save_upload(
    run_id: int, block_run_id: int | None, file: UploadFile, filename: str | None = None
) -> Tuple[str, int]:
    dirp = artifact_dir_for(run_id, block_run_id)
    name = _safe_name(filename or file.filename or "file.bin")
    path = dirp / name
    # write stream
    size = 0
    with open(path, "wb") as f:
        while True:
            chunk = file.file.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            f.write(chunk)
    # return URI (local:// relative path from ARTIFACTS_DIR root)
    rel = path.relative_to(Path(settings.ARTIFACTS_DIR).expanduser().resolve())
    return f"local://{rel.as_posix()}", size


def open_local_uri(uri: str) -> Path:
    assert uri.startswith("local://")
    rel = uri[len("local://") :]
    return Path(settings.ARTIFACTS_DIR).expanduser().resolve() / rel


def sign_for_download(artifact_id: int, exp_ts: int | None = None) -> str:
    if exp_ts is None:
        exp_ts = int(time.time()) + int(settings.SIGNED_URL_TTL_SECONDS)
    msg = f"{artifact_id}.{exp_ts}".encode("utf-8")
    sig = hmac.new(settings.SECRET_KEY.encode("utf-8"), msg, hashlib.sha256).hexdigest()
    return f"/artifacts/{artifact_id}/download?exp={exp_ts}&sig={sig}"


def verify_signature(artifact_id: int, exp: int, sig: str) -> bool:
    if int(exp) < int(time.time()):
        return False
    msg = f"{artifact_id}.{int(exp)}".encode("utf-8")
    good = hmac.new(
        settings.SECRET_KEY.encode("utf-8"), msg, hashlib.sha256
    ).hexdigest()
    # Timing-safe compare
    return hmac.compare_digest(good, sig)
