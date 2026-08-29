from pathlib import Path

from finstream.config import load_settings


def test_loads_environment_values(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TWELVE_DATA_API_KEY", "market-key")
    monkeypatch.setenv("FRED_API_KEY", "fred-key")
    monkeypatch.setenv("SEC_USER_AGENT", "FinStream test@example.com")

    settings = load_settings()

    assert settings.twelve_data_api_key == "market-key"
    assert settings.fred_api_key == "fred-key"
    assert settings.sec_user_agent == "FinStream test@example.com"


def test_missing_and_blank_values_are_none(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    monkeypatch.setenv("TWELVE_DATA_API_KEY", "   ")
    monkeypatch.setenv("SEC_USER_AGENT", "\t")

    settings = load_settings()

    assert settings.twelve_data_api_key is None
    assert settings.fred_api_key is None
    assert settings.sec_user_agent is None


def test_environment_values_take_precedence_over_dotenv(
    monkeypatch, tmp_path: Path
) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "TWELVE_DATA_API_KEY=dotenv-market-key\n"
        "FRED_API_KEY= dotenv-fred-key \n"
        "SEC_USER_AGENT= FinStream dotenv@example.com \n",
        encoding="utf-8",
    )

    monkeypatch.setenv("TWELVE_DATA_API_KEY", "environment-market-key")
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)

    settings = load_settings(dotenv_path=dotenv_path)

    assert settings.twelve_data_api_key == "environment-market-key"
    assert settings.fred_api_key == "dotenv-fred-key"
    assert settings.sec_user_agent == "FinStream dotenv@example.com"
