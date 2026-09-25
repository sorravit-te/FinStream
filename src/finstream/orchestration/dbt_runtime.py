"""Airflow-independent dbt CLI runtime boundaries for FinStream."""

import os
from pathlib import Path
import shutil
import subprocess
import sys


DbtRuntimeSummary = dict[str, str | int]

_DEFAULT_DBT_DIRECTORY = Path(__file__).resolve().parents[3] / "dbt"


def run_dbt_seed(
    *,
    project_dir: str | Path | None = None,
    profiles_dir: str | Path | None = None,
) -> DbtRuntimeSummary:
    """Recreate the FinStream dbt seed relations through the installed dbt CLI."""
    return _run_dbt_command(
        "seed",
        additional_args=("--full-refresh",),
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )


def run_dbt_models(
    *,
    project_dir: str | Path | None = None,
    profiles_dir: str | Path | None = None,
) -> DbtRuntimeSummary:
    """Run the FinStream dbt models through the installed dbt CLI."""
    return _run_dbt_command(
        "run", project_dir=project_dir, profiles_dir=profiles_dir
    )


def run_dbt_tests(
    *,
    project_dir: str | Path | None = None,
    profiles_dir: str | Path | None = None,
) -> DbtRuntimeSummary:
    """Run the FinStream dbt analytical tests through the installed dbt CLI."""
    return _run_dbt_command(
        "test", project_dir=project_dir, profiles_dir=profiles_dir
    )


def _run_dbt_command(
    command: str,
    *,
    additional_args: tuple[str, ...] = (),
    project_dir: str | Path | None,
    profiles_dir: str | Path | None,
) -> DbtRuntimeSummary:
    resolved_project_dir = _resolve_dbt_directory(project_dir)
    resolved_profiles_dir = _resolve_dbt_directory(profiles_dir)
    dbt_executable = _resolve_dbt_executable()
    subprocess.run(
        [
            dbt_executable,
            command,
            *additional_args,
            "--project-dir",
            str(resolved_project_dir),
            "--profiles-dir",
            str(resolved_profiles_dir),
        ],
        check=True,
    )
    return {"command": command, "return_code": 0}


def _resolve_dbt_directory(directory: str | Path | None) -> Path:
    """Resolve an explicit directory or the repository dbt directory deterministically."""
    if directory is None:
        return _DEFAULT_DBT_DIRECTORY
    return Path(directory)


def _resolve_dbt_executable() -> str:
    """Prefer dbt installed beside the current Python executable over PATH."""
    executable_name = "dbt.exe" if sys.platform == "win32" else "dbt"
    environment_executable = Path(sys.executable).parent / executable_name
    environment_executable_is_usable = environment_executable.is_file() and (
        sys.platform == "win32" or os.access(environment_executable, os.X_OK)
    )
    if environment_executable_is_usable:
        return str(environment_executable)

    path_executable = shutil.which("dbt")
    if path_executable is not None:
        return path_executable

    raise FileNotFoundError(
        "Could not find a dbt executable beside the current Python interpreter "
        "or on PATH"
    )
