from __future__ import annotations
from typing import Callable, Dict
from sqlalchemy.orm import Session
from app import models
from app.steps import csv_reader, llm_sentiment, llm_toxicity, file_writer

StepFn = Callable[[Session, int], None]

REGISTRY: Dict[models.BlockType, StepFn] = {
    models.BlockType.CSV_READER: csv_reader.run,
    models.BlockType.LLM_SENTIMENT: llm_sentiment.run,
    models.BlockType.LLM_TOXICITY: llm_toxicity.run,
    models.BlockType.FILE_WRITER: file_writer.run,
}
