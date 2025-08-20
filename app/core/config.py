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

    # Retry defaults
    MAX_ATTEMPTS_DEFAULT: int = Field(default=1)
    BACKOFF_BASE_SECONDS: int = Field(default=0)

    # Notifications
    NOTIFY_WEBHOOK_URL: str | None = Field(default=None)
    NOTIFY_EVENTS: list[str] = Field(default_factory=lambda: ["SUCCEEDED", "FAILED"])

    # Auth & Rate limiting
    API_KEY: str | None = Field(default=None)
    RATE_LIMIT_PER_MINUTE: int = Field(default=120)
    RATE_LIMIT_WINDOW_SECONDS: int = Field(default=2)  # test-friendly small window
    RATE_LIMIT_PATHS: list[str] = Field(default_factory=lambda: ["/health"])

    # Artifact storage (Step 13)
    ARTIFACTS_DIR: str = Field(default="./data/artifacts")
    SECRET_KEY: str = Field(
        default="dev-secret"
    )  # used for signed URLs (HMAC). Change in production.
    SIGNED_URL_TTL_SECONDS: int = Field(default=300)
    SIGNED_URLS_REQUIRED: bool = Field(default=False)

    @property
    def sqlite_uri(self) -> str:
        path = Path(self.SQLITE_PATH).expanduser().resolve()
        return f"sqlite+pysqlite:///{path}"


settings = Settings()
