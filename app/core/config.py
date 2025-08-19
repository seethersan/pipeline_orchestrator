
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from pathlib import Path

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)
    APP_NAME: str = Field(default="pipeline-orchestrator")
    LOG_LEVEL: str = Field(default="INFO")
    SQLITE_PATH: str = Field(default="./data/db.sqlite3")

    @property
    def sqlite_uri(self) -> str:
        path = Path(self.SQLITE_PATH).expanduser().resolve()
        return f"sqlite+pysqlite:///{path}"

settings = Settings()
