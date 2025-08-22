from __future__ import annotations

import re
from typing import Optional
from app.core.config import settings

# --- Tiny heuristics (deterministic, no deps) --------------------------------

_POS = {"good", "love", "great", "awesome", "amazing", "excellent", "nice", "happy", "enjoy"}
_NEG = {"bad", "hate", "terrible", "awful", "worst", "poor", "sad", "angry", "delay", "delays"}
_INSULTS = {"idiot", "stupid", "dumb", "moron", "trash", "shut up", "loser", "fool"}

def _extract_text(prompt: str) -> str:
    # Our prompts are like: "Text: {text}\nAnswer:" — try to pull text after "Text:"
    m = re.search(r"Text:\s*(.*)\n", prompt, flags=re.IGNORECASE | re.DOTALL)
    return m.group(1).strip() if m else prompt

def _heuristic_sentiment(text: str) -> str:
    t = text.lower()
    pos = sum(w in t for w in _POS)
    neg = sum(w in t for w in _NEG)
    if pos > neg:
        return "POSITIVE"
    if neg > pos:
        return "NEGATIVE"
    return "NEUTRAL"

def _heuristic_toxic(text: str) -> str:
    t = text.lower()
    return "TOXIC" if any(w in t for w in _INSULTS) else "NON_TOXIC"

# --- Public API used by steps & tests ----------------------------------------

def llm_predict(prompt: str, system: Optional[str] = None) -> str:
    """
    Unified LLM entrypoint.
    - If settings.LLM_PROVIDER is "gemini": try LangChain Google Generative AI
    - Otherwise (default "mock"): fast heuristic classification
    """
    provider = (getattr(settings, "LLM_PROVIDER", None) or "mock").lower()
    sys = (system or "").lower()
    text = _extract_text(prompt)

    if provider == "gemini":
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI  # optional dependency
            model = getattr(settings, "GEMINI_MODEL", None) or "gemini-1.5-flash"
            api_key = getattr(settings, "GEMINI_API_KEY", None)
            if not api_key:
                # No key → fall back to heuristic to avoid hard failures in tests
                return _heuristic_sentiment(text) if "sentiment" in sys else _heuristic_toxic(text)
            llm = ChatGoogleGenerativeAI(model=model, api_key=api_key)
            # Keep it simple; prompt already contains instruction + text
            out = llm.predict(prompt).strip().upper()
            if "sentiment" in sys:
                return out if out in {"POSITIVE", "NEGATIVE", "NEUTRAL"} else _heuristic_sentiment(text)
            if "tox" in sys:
                return out if out in {"TOXIC", "NON_TOXIC"} else _heuristic_toxic(text)
            return out
        except Exception:
            # Any runtime/import error → safe heuristic
            return _heuristic_sentiment(text) if "sentiment" in sys else _heuristic_toxic(text)

    # Default: mock/heuristic (CI/static)
    if "sentiment" in sys:
        return _heuristic_sentiment(text)
    if "tox" in sys:
        return _heuristic_toxic(text)
    # generic fallback
    return "NEUTRAL"
