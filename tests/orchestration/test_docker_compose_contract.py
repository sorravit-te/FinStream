"""Semantic checks for the local Docker Compose runtime contract."""

from pathlib import Path

import yaml


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = REPOSITORY_ROOT / "compose.yaml"
LONG_RUNNING_SERVICES = {
    "postgres",
    "airflow-api-server",
    "airflow-scheduler",
    "airflow-dag-processor",
}
NAMED_VOLUMES = {"postgres_data", "bronze_data", "airflow_logs"}


def _compose_config() -> dict[str, object]:
    config = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    assert isinstance(config, dict)
    return config


def test_long_running_services_restart_unless_stopped() -> None:
    services = _compose_config()["services"]

    for service_name in LONG_RUNNING_SERVICES:
        assert services[service_name]["restart"] == "unless-stopped"


def test_airflow_init_remains_a_non_restarting_one_shot_service() -> None:
    airflow_init = _compose_config()["services"]["airflow-init"]

    assert "restart" not in airflow_init
    assert airflow_init["depends_on"]["postgres"]["condition"] == "service_healthy"
    assert "airflow db migrate" in airflow_init["command"][-1]


def test_named_persistent_volumes_remain_wired_to_services() -> None:
    config = _compose_config()
    services = config["services"]

    assert set(config["volumes"]) == NAMED_VOLUMES
    assert "postgres_data:/var/lib/postgresql/data" in services["postgres"]["volumes"]
    for service_name in (
        "airflow-init",
        "airflow-api-server",
        "airflow-scheduler",
        "airflow-dag-processor",
    ):
        assert "airflow_logs:/opt/airflow/logs" in services[service_name]["volumes"]
    for service_name in ("airflow-init", "airflow-scheduler"):
        assert (
            "bronze_data:/opt/airflow/finstream/data/bronze"
            in services[service_name]["volumes"]
        )
