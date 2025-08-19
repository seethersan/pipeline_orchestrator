
from __future__ import annotations
from pathlib import Path
from typing import List, Dict
import csv
import shutil

def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

def read_csv_head(path: str | Path, limit: int = 5) -> List[Dict[str, str]]:
    path = Path(path)
    rows: List[Dict[str, str]] = []
    with path.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            rows.append(row)
            if i + 1 >= limit:
                break
    return rows

def copy_file(src: str | Path, dst: str | Path) -> None:
    src = Path(src); dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)
