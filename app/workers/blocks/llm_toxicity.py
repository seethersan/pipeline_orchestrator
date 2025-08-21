from __future__ import annotations
from typing import Any, Dict, Iterable, List
from app.llm.langchain_client import llm_predict

PROMPT = (
    "You are a strict toxicity classifier.\n\n"
    "Return exactly one of: TOXIC or NON_TOXIC.\n"
    "Text: {text}\n"
    "Answer:\n"
)


def run(config: Dict[str, Any], rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    text_field = config.get("text_field", "text")
    out: List[Dict[str, Any]] = []
    for r in rows:
        text = str(r.get(text_field, ""))
        label = (
            llm_predict(PROMPT.format(text=text), system="Detect toxicity")
            .strip()
            .upper()
        )
        if label not in {"TOXIC", "NON_TOXIC"}:
            label = "NON_TOXIC"
        out.append({**r, "toxicity": label})
    return out
