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

    # Retry defaults (Step 11 fix: make terminal by default)
    MAX_ATTEMPTS_DEFAULT: int = Field(default=1)
    BACKOFF_BASE_SECONDS: int = Field(default=0)

    # Notifications
    NOTIFY_WEBHOOK_URL: str | None = Field(default=None)
    NOTIFY_EVENTS: list[str] = Field(default_factory=lambda: ["SUCCEEDED", "FAILED"])

    @property
    def sqlite_uri(self) -> str:
        path = Path(self.SQLITE_PATH).expanduser().resolve()
        return f"sqlite+pysqlite:///{path}"


settings = Settings()
