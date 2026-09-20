"""Emit the Airflow metadata SQLAlchemy URL from required environment values."""

import os
import sys
from urllib.parse import quote


_REQUIRED_VARIABLES = (
    "AIRFLOW_POSTGRES_DB",
    "AIRFLOW_POSTGRES_USER",
    "AIRFLOW_POSTGRES_PASSWORD",
)


def _required_value(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"{name} must be set for the Airflow metadata database")
    return value


def main() -> None:
    database, user, password = (
        _required_value(name) for name in _REQUIRED_VARIABLES
    )
    sys.stdout.write(
        "postgresql+psycopg2://"
        f"{quote(user, safe='')}:{quote(password, safe='')}"
        f"@postgres:5432/{quote(database, safe='')}"
    )


if __name__ == "__main__":
    main()
