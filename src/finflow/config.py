"""Environment-driven configuration for FinFlow."""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values


@dataclass(frozen=True)
class Settings:
    """Configuration values used by FinFlow data sources."""

    twelve_data_api_key: str | None
    fred_api_key: str | None
    sec_user_agent: str | None


def _clean_value(value: str | None) -> str | None:
    if value is None:
        return None

    cleaned_value = value.strip()
    return cleaned_value or None


def load_settings(
    dotenv_path: str | Path | None = ".env",
    environment: Mapping[str, str | None] | None = None,
) -> Settings:
    """Load settings from an optional dotenv file and environment variables."""
    dotenv_settings = dotenv_values(dotenv_path) if dotenv_path is not None else {}
    environment_settings = os.environ if environment is None else environment

    def value_for(name: str) -> str | None:
        if name in environment_settings:
            return _clean_value(environment_settings[name])
        return _clean_value(dotenv_settings.get(name))

    return Settings(
        twelve_data_api_key=value_for("TWELVE_DATA_API_KEY"),
        fred_api_key=value_for("FRED_API_KEY"),
        sec_user_agent=value_for("SEC_USER_AGENT"),
    )
