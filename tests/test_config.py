from src.config import Settings


def test_settings_load_from_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@db:5432/x")
    monkeypatch.setenv("LOG_LEVEL", "debug")
    monkeypatch.setenv("POKEMON_TCG_API_KEY", "abc")

    settings = Settings(_env_file=None)

    assert settings.database_url == "postgresql+psycopg://u:p@db:5432/x"
    assert settings.psycopg_dsn == "postgresql://u:p@db:5432/x"
    assert settings.log_level == "DEBUG"
    assert settings.pokemon_tcg_api_key == "abc"


def test_settings_defaults(monkeypatch):
    for key in ("DATABASE_URL", "LOG_LEVEL", "POKEMON_TCG_API_KEY", "PRICE_API_KEY"):
        monkeypatch.delenv(key, raising=False)

    settings = Settings(_env_file=None)

    assert settings.log_level == "INFO"
    assert settings.pokemon_tcg_api_key is None
