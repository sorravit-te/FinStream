"""Static checks for the optional Airflow Step 11.1 foundation."""

from pathlib import Path
import tomllib


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_airflow_is_pinned_optional_dependency() -> None:
    """Core installation remains independent of the Airflow runtime."""
    with (REPOSITORY_ROOT / "pyproject.toml").open("rb") as pyproject_file:
        project = tomllib.load(pyproject_file)["project"]

    assert project["optional-dependencies"]["airflow"] == [
        "apache-airflow==3.3.2"
    ]
    assert not any(
        dependency.startswith("apache-airflow")
        for dependency in project["dependencies"]
    )


def test_future_dag_directory_is_not_a_python_package() -> None:
    """Future DAG modules cannot shadow the installed ``airflow`` package."""
    dags_directory = REPOSITORY_ROOT / "orchestration" / "dags"

    assert dags_directory.is_dir()
    assert not (dags_directory / "__init__.py").exists()
    assert not (REPOSITORY_ROOT / "airflow").exists()
