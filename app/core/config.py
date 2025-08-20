from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False
    )

    # App
    APP_NAME: str = Field(default="pipeline-orchestrator")
    LOG_LEVEL: str = Field(default="INFO")
    SQLITE_PATH: str = Field(default="./data/db.sqlite3")

    # Retry defaults (Step 6)
    MAX_ATTEMPTS_DEFAULT: int = Field(default=3)
    BACKOFF_BASE_SECONDS: int = Field(default=3)

    @property
    def sqlite_uri(self) -> str:
        path = Path(self.SQLITE_PATH).expanduser().resolve()
        return f"sqlite+pysqlite:///{path}"


settings = Settings()
