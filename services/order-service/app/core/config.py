from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def find_project_root(start: Path, marker: str = "pyproject.toml") -> Path:
    """Walk upward from `start` until a directory containing `marker` is found.

    Falls back to `start` itself if no marker is found (e.g. filesystem root reached),
    so this never raises even if the layout changes again.
    """
    for parent in [start, *start.parents]:
        if (parent / marker).exists():
            return parent
    return start


BASE_DIR = Path(__file__).resolve().parent  # .../app/core
PROJECT_ROOT = find_project_root(BASE_DIR)   # .../order-service (has pyproject.toml)
ENV_FILE = PROJECT_ROOT / ".env"


class Settings(BaseSettings):
    app_name: str = "Order Service"
    app_version: str = "0.1.0"

    environment: str = Field(
        default="development",
        validation_alias="APP_ENV",
    )

    database_url: str = (
        "postgresql+psycopg://user:Aminadia%40123@localhost:5432/commerce"
    )

    test_database_url: str = (
        "postgresql+psycopg://postgres:Aminadia%40123@localhost:5432/commerce_test"
    )

    redis_url: str = "redis://localhost:6379/0"

    kafka_bootstrap_servers: str = "localhost:9092"

    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()