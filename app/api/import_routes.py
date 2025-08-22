from __future__ import annotations
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Body, Query
from pydantic import BaseModel, Field, validator, computed_field
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.dependencies import get_db
from app import models
from app.core.scheduler import Scheduler
from app.core.serialization import export_pipeline_spec

router = APIRouter()


class CsvReaderCfg(BaseModel):
    input_path: str = Field(..., description="Path to input CSV")
    delimiter: Optional[str] = Field(default=",", min_length=1, max_length=1)


class LlmSentimentCfg(BaseModel):
    model: Optional[str] = Field(default=None)
    temperature: Optional[float] = Field(default=0.0, ge=0.0, le=2.0)


class LlmToxicityCfg(BaseModel):
    model: Optional[str] = Field(default=None)
    threshold: Optional[float] = Field(default=0.5, ge=0.0, le=1.0)


class FileWriterCfg(BaseModel):
    output_path: str = Field(...)


class CsvWriterCfg(BaseModel):
    output_path: str = Field(...)


BLOCK_CFG_MODELS = {
    "CSV_READER": CsvReaderCfg,
    "LLM_SENTIMENT": LlmSentimentCfg,
    "LLM_TOXICITY": LlmToxicityCfg,
    "FILE_WRITER": FileWriterCfg,
    "CSV_WRITER": CsvWriterCfg,
}


class ImportBlock(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    type: str = Field(...)
    config: Optional[Dict[str, Any]] = Field(default=None)

    @validator("type")
    def type_upper(cls, v: str) -> str:
        return v.upper()


class ImportEdge(BaseModel):
    from_: str = Field(..., alias="from")
    to: str


class PipelineImportIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    replace_if_exists: bool = Field(default=False)
    blocks: List[ImportBlock]
    edges: List[ImportEdge]


class PipelineImportOut(BaseModel):
    pipeline_id: int
    created_blocks: int
    created_edges: int
    replaced: bool = False
    version: int

    @computed_field(alias="id", return_type=int)
    def id_value(self) -> int:
        return self.pipeline_id


def _validate_spec(spec: PipelineImportIn) -> List[str]:
    errors: List[str] = []
    names = [b.name for b in spec.blocks]
    if len(names) != len(set(names)):
        errors.append("Duplicate block names are not allowed.")
    for b in spec.blocks:
        t = b.type.upper()
        if t not in BLOCK_CFG_MODELS:
            errors.append(f"Unknown block type: {t}")
            continue
        if b.config is not None:
            try:
                BLOCK_CFG_MODELS[t](**b.config)
            except Exception as e:
                errors.append(f"Invalid config for block '{b.name}' ({t}): {e}")
    name_set = set(names)
    for e in spec.edges:
        if e.from_ not in name_set:
            errors.append(f"Edge.from '{e.from_}' does not match any block.")
        if e.to not in name_set:
            errors.append(f"Edge.to '{e.to}' does not match any block.")
    # cycle detection
    adj = {n: [] for n in names}
    for e in spec.edges:
        adj[e.from_].append(e.to)
    state = {n: 0 for n in names}
    cycle = False

    def dfs(u: str):
        nonlocal cycle
        state[u] = 1
        for v in adj.get(u, []):
            if state[v] == 0:
                dfs(v)
            elif state[v] == 1:
                cycle = True
                return
        state[u] = 2

    for n in names:
        if state[n] == 0:
            dfs(n)
        if cycle:
            break
    if cycle:
        errors.append("Graph contains a cycle.")
    return errors


def _parse_yaml_or_json(body: Any) -> PipelineImportIn:
    if isinstance(body, dict):
        return PipelineImportIn(**body)
    if isinstance(body, str):
        try:
            import yaml  # type: ignore
        except Exception:
            raise HTTPException(
                status_code=400, detail="YAML input provided but PyYAML not installed."
            )
        try:
            data = yaml.safe_load(body)
            if not isinstance(data, dict):
                raise ValueError("YAML did not decode to a dict object.")
            return PipelineImportIn(**data)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to parse YAML: {e}")
    raise HTTPException(
        status_code=400,
        detail="Unsupported import body; provide a JSON object or YAML string.",
    )


@router.post("/pipelines/import", response_model=PipelineImportOut)
def import_pipeline(
    spec: Any = Body(...),
    dry_run: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    parsed = _parse_yaml_or_json(spec)
    errors = _validate_spec(parsed)
    if errors:
        if dry_run:
            raise HTTPException(
                status_code=400, detail={"valid": False, "errors": errors}
            )
        raise HTTPException(status_code=400, detail="; ".join(errors))

    if dry_run:
        last = (
            db.execute(
                select(models.PipelineHistory)
                .where(models.PipelineHistory.pipeline_name == parsed.name)
                .order_by(models.PipelineHistory.version.desc())
            )
            .scalars()
            .first()
        )
        next_ver = (last.version + 1) if last else 1
        return PipelineImportOut(
            pipeline_id=0,
            created_blocks=len(parsed.blocks),
            created_edges=len(parsed.edges),
            replaced=False,
            version=next_ver,
        )

    replaced = False
    existing = db.execute(
        select(models.Pipeline).where(models.Pipeline.name == parsed.name)
    ).scalar_one_or_none()
    next_version = 1

    if existing:
        next_version = existing.version + 1
        spec_old = export_pipeline_spec(db, existing.id)
        db.add(
            models.PipelineHistory(
                pipeline_name=existing.name,
                version=existing.version,
                spec_json=spec_old,
            )
        )
        db.commit()

        if parsed.replace_if_exists:
            db.delete(existing)
            db.commit()
            replaced = True
        else:
            raise HTTPException(
                status_code=409,
                detail=f"Pipeline with name '{parsed.name}' already exists.",
            )

    p = models.Pipeline(name=parsed.name, version=next_version)
    db.add(p)
    db.flush()

    name_to_id = {}
    for b in parsed.blocks:
        cfg = BLOCK_CFG_MODELS[b.type](**(b.config or {})).dict()
        blk = models.Block(
            pipeline_id=p.id,
            type=models.BlockType[b.type],
            name=b.name,
            config_json=cfg,
        )
        db.add(blk)
        db.flush()
        name_to_id[b.name] = blk.id

    for e in parsed.edges:
        db.add(
            models.Edge(
                pipeline_id=p.id,
                from_block_id=name_to_id[e.from_],
                to_block_id=name_to_id[e.to],
            )
        )

    db.commit()

    Scheduler(db).validate_dag(p.id)

    spec_new = export_pipeline_spec(db, p.id)
    db.add(
        models.PipelineHistory(
            pipeline_name=p.name, version=p.version, spec_json=spec_new
        )
    )
    db.commit()

    return PipelineImportOut(
        pipeline_id=p.id,
        created_blocks=len(parsed.blocks),
        created_edges=len(parsed.edges),
        replaced=replaced,
        version=p.version,
    )
