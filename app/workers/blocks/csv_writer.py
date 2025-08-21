from __future__ import annotations
from typing import Any, Dict, Iterable, List
import csv, os
from pathlib import Path
from app.core.config import settings


def run(config: Dict[str, Any], rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = list(rows)
    if not rows:
        return rows
    out_path = config.get("output_path")
    if not out_path:
        name = config.get("name", "output")
        out_path = os.path.join(settings.ARTIFACTS_DIR, f"{name}.csv")
    Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)
    # fieldnames = union of all keys
    keys = set()
    for r in rows:
        keys.update(r.keys())
    fieldnames = sorted(keys)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    # returning rows keeps runner behavior consistent
    return rows
