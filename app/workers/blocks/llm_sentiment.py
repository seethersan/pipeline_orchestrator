from __future__ import annotations
from typing import Any, Dict, Iterable, List
from app.llm.langchain_client import llm_predict
from app import models

PROMPT_TEMPLATE = (
    "You are a strict sentiment classifier.\n\n"
    "Return exactly one of: POSITIVE, NEGATIVE, or NEUTRAL.\n"
    "Text: {text}\n"
    "Answer:\n"
)


def run(config: Dict[str, Any], rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    text_field = config.get("text_field", "text")
    out: List[Dict[str, Any]] = []
    for r in rows:
        text = str(r.get(text_field, ""))
        prompt = PROMPT_TEMPLATE.format(text=text)
        label = llm_predict(prompt, system="Classify sentiment").strip().upper()
        if label not in {"POSITIVE", "NEGATIVE", "NEUTRAL"}:
            label = "NEUTRAL"
        out.append({**r, "sentiment": label})
    return out
