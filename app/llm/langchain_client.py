from __future__ import annotations
from typing import Optional
from app.core.config import settings

def llm_available() -> bool:
    return (settings.LLM_PROVIDER or 'mock').lower() != 'mock'

def _gemini():
    from langchain_google_genai import ChatGoogleGenerativeAI
    api_key = settings.GEMINI_API_KEY
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")
    model = settings.GEMINI_MODEL or "gemini-1.5-flash"
    return ChatGoogleGenerativeAI(
        model=model,
        api_key=api_key,
        temperature=float(settings.LLM_TEMPERATURE or 0.0),
    )

def llm_predict(prompt: str, system: Optional[str] = None) -> str:
    provider = (settings.LLM_PROVIDER or "mock").lower()
    if provider == "gemini":
        llm = _gemini()
        messages = []
        if system:
            messages.append(("system", system))
        messages.append(("human", prompt))
        out = llm.invoke(messages)
        try:
            return out.content if hasattr(out, "content") else str(out)
        except Exception:
            return str(out)
    else:
        text = prompt.lower()
        if "sentiment" in text or "clasifica" in text:
            if any(k in text for k in ["good","excelente","great","positivo","awesome","love"]):
                return "POSITIVE"
            if any(k in text for k in ["bad","malo","terrible","negativo","hate","awful"]):
                return "NEGATIVE"
            return "NEUTRAL"
        return "OK"
