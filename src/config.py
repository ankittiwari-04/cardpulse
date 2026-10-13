from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql+psycopg://cardpulse:cardpulse@localhost:5432/cardpulse"
    pokemon_tcg_api_key: str | None = None
    pokemon_tcg_base_url: str = "https://api.pokemontcg.io/v2"
    price_api_key: str | None = None
    price_api_base_url: str = ""
    log_level: str = "INFO"

    @field_validator("log_level")
    @classmethod
    def _normalize_log_level(cls, value: str) -> str:
        return value.upper()

    @property
    def psycopg_dsn(self) -> str:
        """Plain libpq-style DSN for psycopg (strips the SQLAlchemy driver suffix)."""
        return self.database_url.replace("postgresql+psycopg://", "postgresql://", 1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
