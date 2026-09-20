from pathlib import Path

from finstream.config import load_settings


def test_loads_environment_values(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TWELVE_DATA_API_KEY", "market-key")
    monkeypatch.setenv("FRED_API_KEY", "fred-key")
    monkeypatch.setenv("SEC_USER_AGENT", "FinStream test@example.com")
    monkeypatch.setenv(
        "POSTGRES_DSN",
        "postgresql://finstream:fake-password@localhost:5432/finstream",
    )

    settings = load_settings()

    assert settings.twelve_data_api_key == "market-key"
    assert settings.fred_api_key == "fred-key"
    assert settings.sec_user_agent == "FinStream test@example.com"
    assert settings.postgres_dsn == (
        "postgresql://finstream:fake-password@localhost:5432/finstream"
    )


def test_missing_and_blank_values_are_none(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    monkeypatch.setenv("TWELVE_DATA_API_KEY", "   ")
    monkeypatch.setenv("SEC_USER_AGENT", "\t")
    monkeypatch.setenv("POSTGRES_DSN", "   ")

    settings = load_settings()

    assert settings.twelve_data_api_key is None
    assert settings.fred_api_key is None
    assert settings.sec_user_agent is None
    assert settings.postgres_dsn is None


def test_environment_values_take_precedence_over_dotenv(
    monkeypatch, tmp_path: Path
) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "TWELVE_DATA_API_KEY=dotenv-market-key\n"
        "FRED_API_KEY= dotenv-fred-key \n"
        "SEC_USER_AGENT= FinStream dotenv@example.com \n"
        "POSTGRES_DSN= postgresql://dotenv:fake@localhost:5432/finstream \n",
        encoding="utf-8",
    )

    monkeypatch.setenv("TWELVE_DATA_API_KEY", "environment-market-key")
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    monkeypatch.setenv(
        "POSTGRES_DSN",
        "postgresql://environment:fake@localhost:5432/finstream",
    )

    settings = load_settings(dotenv_path=dotenv_path)

    assert settings.twelve_data_api_key == "environment-market-key"
    assert settings.fred_api_key == "dotenv-fred-key"
    assert settings.sec_user_agent == "FinStream dotenv@example.com"
    assert settings.postgres_dsn == (
        "postgresql://environment:fake@localhost:5432/finstream"
    )


def test_postgres_dsn_is_excluded_from_settings_repr() -> None:
    fake_dsn = (
        "postgresql://finstream:obvious-fake-password@localhost:5432/finstream"
    )

    settings = load_settings(
        dotenv_path=None,
        environment={"POSTGRES_DSN": fake_dsn},
    )

    assert fake_dsn not in repr(settings)
    assert "obvious-fake-password" not in repr(settings)
