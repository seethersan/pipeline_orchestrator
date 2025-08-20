from __future__ import annotations
from typing import List, Literal

SentimentLabel = Literal["NEGATIVE", "NEUTRAL", "POSITIVE"]
ToxicLabel = Literal["NON_TOXIC", "TOXIC"]


class LLMClientProtocol:
    def classify_sentiment(self, texts: List[str]) -> List[SentimentLabel]:
        raise NotImplementedError

    def detect_toxicity(self, texts: List[str]) -> List[ToxicLabel]:
        raise NotImplementedError


class LocalHeuristicClient(LLMClientProtocol):
    POS = {
        "good",
        "great",
        "excellent",
        "love",
        "happy",
        "amazing",
        "awesome",
        "nice",
        "wonderful",
        "like",
    }
    NEG = {
        "bad",
        "terrible",
        "hate",
        "sad",
        "angry",
        "awful",
        "horrible",
        "worst",
        "dislike",
    }
    TOX = {"idiot", "stupid", "dumb", "trash", "suck", "moron", "shut up"}

    def classify_sentiment(self, texts: List[str]) -> List[SentimentLabel]:
        out: List[SentimentLabel] = []
        for t in texts:
            tl = t.lower()
            pos = any(w in tl for w in self.POS)
            neg = any(w in tl for w in self.NEG)
            if pos and not neg:
                out.append("POSITIVE")
            elif neg and not pos:
                out.append("NEGATIVE")
            else:
                out.append("NEUTRAL")
        return out

    def detect_toxicity(self, texts: List[str]) -> List[ToxicLabel]:
        out: List[ToxicLabel] = []
        for t in texts:
            tl = t.lower()
            tox = any(w in tl for w in self.TOX)
            out.append("TOXIC" if tox else "NON_TOXIC")
        return out
