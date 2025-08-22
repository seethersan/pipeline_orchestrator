from __future__ import annotations
from typing import Optional
from fastapi import APIRouter, Query, HTTPException
from pathlib import Path
import csv
from app.core.config import settings
from app.llm import langchain_client as llm

router = APIRouter()


@router.post("/datasets/synthesize")
def synthesize_dataset(
    count: int = Query(default=40, ge=10, le=200),
    output_path: Optional[str] = Query(default=None),
):
    """
    Generate a CSV with columns: id, text.
    Prompts the configured LLM provider for social-style messages or uses heuristics.
    """
    positives = [
        "Love this product, exceeded expectations",
        "Great service and friendly support team",
        "Amazing results, would recommend to friends",
        "Excellent quality and super fast delivery",
        "Happy with the purchase, five stars",
        "Enjoy using this every single day",
        "Nice user experience, very intuitive design",
        "Awesome update, fixed my issues quickly",
        "This works perfectly for my needs",
        "Solid value and dependable performance",
    ]
    negatives = [
        "Bad experience, support never replied",
        "Terrible delays ruined my day",
        "Awful interface and confusing menus",
        "Worst purchase I made this year",
        "Poor quality, broke after one week",
        "Hate how slow this app is",
        "Very buggy and crashes often",
        "Not worth the price at all",
        "Shipping delays and missing items",
        "Returning this, really disappointed",
    ]
    neutrals = [
        "It works as expected for now",
        "Received the package this morning",
        "Using it occasionally during work",
        "Updates installed without any issues",
        "Just another ordinary day at the office",
        "Considering alternatives but undecided",
        "Reading the docs before trying features",
        "Nothing special, basic functionality",
        "Scheduled a demo for next week",
        "Tested briefly, no strong opinion yet",
    ]
    toxic = [
        "This app is trash, shut up already",
        "Who designed this, you idiot",
        "Support was dumb and unhelpful",
        "Stupid update broke everything again",
        "Moron-level mistakes in the UI",
        "Loser product, waste of time",
        "Dumb feature nobody asked for",
        "Awful docs, you fools",
        "What a dumb decision, seriously",
        "Trash tier quality, embarrassing",
    ]

    pool = positives + negatives + neutrals + toxic
    # Ensure diversity by cycling
    rows = []
    for i in range(1, count + 1):
        text = pool[(i - 1) % len(pool)]
        rows.append({"id": i, "text": text})

    # Decide output path
    outp = Path(output_path or Path(settings.ARTIFACTS_DIR).parent / "sample_messages.csv")
    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id", "text"])
        w.writeheader()
        w.writerows(rows)

    return {"count": len(rows), "path": str(outp)}
