from functools import lru_cache

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_ignore_empty=True,  # a blank value in .env falls back to the default
    )

    database_url: str = "postgresql+psycopg://cardpulse:cardpulse@localhost:5432/cardpulse"
    pokemon_tcg_api_key: str | None = None
    pokemon_tcg_base_url: str = "https://api.pokemontcg.io/v2"
    log_level: str = "INFO"

    # --- price ingestion ---
    price_source_name: str = "pokemontcg"
    price_api_base_url: str = ""  # blank = the adapter's own default URL
    price_api_key: str | None = None
    price_request_timeout_seconds: float = Field(default=30.0, gt=0)
    price_max_pages: int = Field(default=1, ge=0)  # 0 = all pages

    # --- entity resolution ---
    match_auto_threshold: float = Field(default=90.0, ge=0, le=100)
    match_review_min_score: float = Field(default=50.0, ge=0, le=100)

    @field_validator("log_level")
    @classmethod
    def _normalize_log_level(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def _review_min_not_above_auto(self) -> "Settings":
        if self.match_review_min_score > self.match_auto_threshold:
            raise ValueError("MATCH_REVIEW_MIN_SCORE must not exceed MATCH_AUTO_THRESHOLD")
        return self

    @property
    def psycopg_dsn(self) -> str:
        """Plain libpq-style DSN for psycopg (strips the SQLAlchemy driver suffix)."""
        return self.database_url.replace("postgresql+psycopg://", "postgresql://", 1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
