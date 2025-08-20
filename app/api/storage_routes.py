from __future__ import annotations
from typing import Optional, Dict, Any
from fastapi import (
    APIRouter,
    Depends,
    UploadFile,
    File,
    Form,
    HTTPException,
    Response,
    Query,
)
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.dependencies import get_db
from app import models
from app.core.storage import (
    save_upload,
    open_local_uri,
    sign_for_download,
    verify_signature,
)
from app.core.config import settings

router = APIRouter()


@router.post("/runs/{run_id}/artifacts/upload")
async def upload_artifact(
    run_id: int,
    db: Session = Depends(get_db),
    block_run_id: Optional[int] = Form(default=None),
    kind: Optional[str] = Form(default=None),
    file: UploadFile = File(...),
    filename: Optional[str] = Form(default=None),
):
    run = db.get(models.PipelineRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    uri, size = save_upload(run_id, block_run_id, file, filename=filename)
    art = models.Artifact(
        pipeline_run_id=run_id,
        block_run_id=block_run_id,
        uri=uri,
        kind=models.ArtifactKind[kind] if kind else models.ArtifactKind.GENERIC,
        preview_json={"size": size, "filename": filename or file.filename},
    )
    db.add(art)
    db.commit()
    db.refresh(art)
    return {"id": art.id, "uri": art.uri, "size": size}


@router.get("/runs/{run_id}/artifacts")
def list_artifacts(run_id: int, db: Session = Depends(get_db)):
    rows = (
        db.execute(
            select(models.Artifact).where(models.Artifact.pipeline_run_id == run_id)
        )
        .scalars()
        .all()
    )
    return [
        {
            "id": a.id,
            "uri": a.uri,
            "kind": a.kind.value if hasattr(a.kind, "value") else str(a.kind),
            "block_run_id": a.block_run_id,
            "preview": a.preview_json or {},
        }
        for a in rows
    ]


@router.get("/artifacts/{artifact_id}/sign")
def sign_artifact_download(artifact_id: int, ttl: int = Query(default=None)):
    exp = None
    if ttl is not None:
        import time

        exp = int(time.time()) + int(ttl)
    return {"url": sign_for_download(artifact_id, exp_ts=exp)}


@router.get("/artifacts/{artifact_id}/download")
def download_artifact(
    artifact_id: int,
    db: Session = Depends(get_db),
    exp: Optional[int] = None,
    sig: Optional[str] = None,
):
    art = db.get(models.Artifact, artifact_id)
    if not art:
        raise HTTPException(status_code=404, detail="Artifact not found")

    if settings.SIGNED_URLS_REQUIRED:
        if not (exp and sig and verify_signature(artifact_id, int(exp), sig)):
            raise HTTPException(status_code=401, detail="Invalid or expired signature")
    # Only support local:// scheme in this step
    if not art.uri.startswith("local://"):
        raise HTTPException(status_code=400, detail="Unsupported URI scheme")
    path = open_local_uri(art.uri)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File missing")
    data = path.read_bytes()
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={path.name}"},
    )
