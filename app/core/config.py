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
    RATE_LIMIT_WINDOW_SECONDS: int = Field(default=2)
    RATE_LIMIT_PATHS: list[str] = Field(default_factory=lambda: ["/health"])

    # Artifacts
    ARTIFACTS_DIR: str = Field(default="./data/artifacts")
    SECRET_KEY: str = Field(default="dev-secret")
    SIGNED_URL_TTL_SECONDS: int = Field(default=300)
    SIGNED_URLS_REQUIRED: bool = Field(default=False)

    # CORS
    CORS_ALLOW_ORIGINS: list[str] = Field(
        default_factory=lambda: ["http://localhost:5173", "http://localhost:8080"]
    )

    # Streams
    STREAM_BACKEND: str = Field(default="none")  # none|kafka|qstash|eventhubs|kinesis
    KAFKA_BOOTSTRAP: str = Field(default="redpanda:9092")
    KAFKA_TOPIC_DEFAULT: str = Field(default="pipeline_events")
    # Optional cloud placeholders
    QSTASH_URL: str | None = Field(default=None)
    QSTASH_TOKEN: str | None = Field(default=None)
    EVENTHUBS_CONN_STR: str | None = Field(default=None)
    KINESIS_STREAM_NAME: str | None = Field(default=None)

    @property
    def sqlite_uri(self) -> str:
        path = Path(self.SQLITE_PATH).expanduser().resolve()
        return f"sqlite+pysqlite:///{path}"


settings = Settings()
