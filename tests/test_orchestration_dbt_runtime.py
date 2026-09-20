"""Unit tests for Airflow-independent dbt CLI runtime adapters."""

from pathlib import Path
import subprocess
from unittest.mock import MagicMock

import pytest

from finstream.orchestration import dbt_runtime


def _default_dbt_directory() -> Path:
    return Path(dbt_runtime.__file__).resolve().parents[3] / "dbt"


@pytest.mark.parametrize(
    ("runtime_function", "command"),
    [
        (dbt_runtime.run_dbt_seed, "seed"),
        (dbt_runtime.run_dbt_models, "run"),
        (dbt_runtime.run_dbt_tests, "test"),
    ],
)
def test_dbt_runtime_uses_repository_dbt_directory_without_cwd_dependency(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    runtime_function: object,
    command: str,
) -> None:
    runner = MagicMock(name="dbt_runner")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dbt_runtime.subprocess, "run", runner)
    monkeypatch.setattr(dbt_runtime, "_resolve_dbt_executable", lambda: "environment-dbt")

    result = runtime_function()

    dbt_directory = _default_dbt_directory()
    runner.assert_called_once_with(
        [
            "environment-dbt",
            command,
            "--project-dir",
            str(dbt_directory),
            "--profiles-dir",
            str(dbt_directory),
        ],
        check=True,
    )
    assert result == {"command": command, "return_code": 0}


def test_dbt_runtime_allows_explicit_project_and_profiles_directories(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = MagicMock(name="dbt_runner")
    project_directory = tmp_path / "project"
    profiles_directory = tmp_path / "profiles"
    monkeypatch.setattr(dbt_runtime.subprocess, "run", runner)
    monkeypatch.setattr(dbt_runtime, "_resolve_dbt_executable", lambda: "environment-dbt")

    result = dbt_runtime.run_dbt_models(
        project_dir=project_directory,
        profiles_dir=profiles_directory,
    )

    runner.assert_called_once_with(
        [
            "environment-dbt",
            "run",
            "--project-dir",
            str(project_directory),
            "--profiles-dir",
            str(profiles_directory),
        ],
        check=True,
    )
    assert result == {"command": "run", "return_code": 0}


def test_dbt_runtime_propagates_cli_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = subprocess.CalledProcessError(1, ["dbt", "test"])
    monkeypatch.setattr(
        dbt_runtime.subprocess,
        "run",
        MagicMock(side_effect=failure),
    )
    monkeypatch.setattr(dbt_runtime, "_resolve_dbt_executable", lambda: "environment-dbt")

    with pytest.raises(subprocess.CalledProcessError) as raised:
        dbt_runtime.run_dbt_tests()

    assert raised.value is failure


def test_dbt_runtime_prefers_windows_environment_executable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    python_executable = tmp_path / "python.exe"
    dbt_executable = tmp_path / "dbt.exe"
    python_executable.touch()
    dbt_executable.touch()
    monkeypatch.setattr(dbt_runtime.sys, "platform", "win32")
    monkeypatch.setattr(dbt_runtime.sys, "executable", str(python_executable))
    path_lookup = MagicMock(return_value="global-dbt")
    monkeypatch.setattr(dbt_runtime.shutil, "which", path_lookup)
    permission_check = MagicMock(return_value=False)
    monkeypatch.setattr(dbt_runtime.os, "access", permission_check)

    result = dbt_runtime._resolve_dbt_executable()

    assert result == str(dbt_executable)
    path_lookup.assert_not_called()
    permission_check.assert_not_called()


def test_dbt_runtime_prefers_executable_posix_environment_executable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    python_executable = tmp_path / "python"
    dbt_executable = tmp_path / "dbt"
    python_executable.touch()
    dbt_executable.touch()
    monkeypatch.setattr(dbt_runtime.sys, "platform", "linux")
    monkeypatch.setattr(dbt_runtime.sys, "executable", str(python_executable))
    permission_check = MagicMock(return_value=True)
    monkeypatch.setattr(dbt_runtime.os, "access", permission_check)
    path_lookup = MagicMock(return_value="global-dbt")
    monkeypatch.setattr(dbt_runtime.shutil, "which", path_lookup)

    result = dbt_runtime._resolve_dbt_executable()

    assert result == str(dbt_executable)
    permission_check.assert_called_once_with(dbt_executable, dbt_runtime.os.X_OK)
    path_lookup.assert_not_called()


def test_dbt_runtime_uses_path_when_posix_environment_file_is_not_executable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    python_executable = tmp_path / "python"
    dbt_executable = tmp_path / "dbt"
    python_executable.touch()
    dbt_executable.touch()
    monkeypatch.setattr(dbt_runtime.sys, "platform", "linux")
    monkeypatch.setattr(dbt_runtime.sys, "executable", str(python_executable))
    permission_check = MagicMock(return_value=False)
    monkeypatch.setattr(dbt_runtime.os, "access", permission_check)
    path_lookup = MagicMock(return_value="global-dbt")
    monkeypatch.setattr(dbt_runtime.shutil, "which", path_lookup)

    assert dbt_runtime._resolve_dbt_executable() == "global-dbt"
    permission_check.assert_called_once_with(dbt_executable, dbt_runtime.os.X_OK)
    path_lookup.assert_called_once_with("dbt")


def test_dbt_runtime_fails_when_posix_environment_file_is_not_executable_and_no_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    python_executable = tmp_path / "python"
    dbt_executable = tmp_path / "dbt"
    python_executable.touch()
    dbt_executable.touch()
    monkeypatch.setattr(dbt_runtime.sys, "platform", "linux")
    monkeypatch.setattr(dbt_runtime.sys, "executable", str(python_executable))
    permission_check = MagicMock(return_value=False)
    monkeypatch.setattr(dbt_runtime.os, "access", permission_check)
    monkeypatch.setattr(dbt_runtime.shutil, "which", MagicMock(return_value=None))

    with pytest.raises(FileNotFoundError, match="current Python interpreter"):
        dbt_runtime._resolve_dbt_executable()

    permission_check.assert_called_once_with(dbt_executable, dbt_runtime.os.X_OK)


def test_dbt_runtime_uses_path_fallback_only_without_environment_executable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(dbt_runtime.sys, "platform", "linux")
    monkeypatch.setattr(dbt_runtime.sys, "executable", str(tmp_path / "python"))
    path_lookup = MagicMock(return_value="global-dbt")
    monkeypatch.setattr(dbt_runtime.shutil, "which", path_lookup)

    assert dbt_runtime._resolve_dbt_executable() == "global-dbt"
    path_lookup.assert_called_once_with("dbt")


def test_dbt_runtime_fails_clearly_when_no_executable_is_available(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(dbt_runtime.sys, "platform", "linux")
    monkeypatch.setattr(dbt_runtime.sys, "executable", str(tmp_path / "python"))
    monkeypatch.setattr(dbt_runtime.shutil, "which", MagicMock(return_value=None))

    with pytest.raises(FileNotFoundError, match="current Python interpreter"):
        dbt_runtime._resolve_dbt_executable()


def test_dbt_runtime_module_does_not_import_airflow() -> None:
    source = Path(dbt_runtime.__file__).read_text(encoding="utf-8").lower()

    assert "import airflow" not in source
    assert "from airflow" not in source
    assert "shell=true" not in source
