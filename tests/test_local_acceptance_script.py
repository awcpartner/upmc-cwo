from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "scripts" / "run_local_acceptance.py"
PASS_SUMMARY = (
    "PASS: on-time feed no incident; delayed feed incident and successful DAG; "
    "idempotency one POST/one work note; sanitized 401"
)


@pytest.fixture(scope="module")
def acceptance_module():
    spec = importlib.util.spec_from_file_location("run_local_acceptance", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_airflow_environment_is_allowlisted(
    acceptance_module, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    poisoned = {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://external/database",
        "AIRFLOW__CORE__EXECUTOR": "CeleryExecutor",
        "AIRFLOW__SECRETS__BACKEND": "external.secrets.Backend",
        "AIRFLOW_CONN_UNRELATED": "https://credential@example.invalid",
        "AIRFLOW_CONFIG": "/external/airflow.cfg",
        "HTTP_PROXY": "http://external.proxy.invalid",
        "HTTPS_PROXY": "https://external.proxy.invalid",
        "ALL_PROXY": "socks5://external.proxy.invalid",
        "SERVICENOW_PASSWORD": "CALLER_SECRET",
    }
    for key, value in poisoned.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(acceptance_module.sys, "executable", "/external/python")

    airflow_home = tmp_path / "airflow"
    temporary_home = tmp_path / "home"
    environment = acceptance_module._build_airflow_environment(
        airflow_home=airflow_home,
        temporary_home=temporary_home,
        base_url="http://127.0.0.1:12345",
        username="local.acceptance",
        credential="generated-local-credential",
    )

    assert not any(value in environment.values() for value in poisoned.values())
    assert environment["HOME"] == str(temporary_home)
    assert environment["AIRFLOW_HOME"] == str(airflow_home)
    assert environment["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] == (
        f"sqlite:///{airflow_home / 'airflow.db'}"
    )
    assert environment["AIRFLOW__CORE__EXECUTOR"] == "SequentialExecutor"
    assert environment["AIRFLOW__CORE__LOAD_EXAMPLES"] == "False"
    assert environment["AIRFLOW__CORE__DAGS_FOLDER"] == str(PROJECT_ROOT / "dags")
    assert environment["AIRFLOW__SECRETS__BACKEND"] == ""
    assert environment["AIRFLOW_CONFIG"] == str(airflow_home / "airflow.cfg")
    assert "PYTHONPATH" not in environment
    assert "PYTHONUSERBASE" not in environment
    assert environment["NO_PROXY"] == "127.0.0.1,localhost"
    assert environment["no_proxy"] == "127.0.0.1,localhost"
    unexpected_proxy_keys = {
        key
        for key in environment
        if "PROXY" in key.upper() and key not in {"NO_PROXY", "no_proxy"}
    }
    assert unexpected_proxy_keys == set()
    assert Path(environment["PATH"].split(os.pathsep)[0]) == (
        PROJECT_ROOT / ".venv-airflow3" / "bin"
    )
    assert set(environment) == acceptance_module.AIRFLOW_ENVIRONMENT_KEYS


def test_on_time_path_guard_rejects_any_servicenow_request(
    acceptance_module,
) -> None:
    state = acceptance_module.MockServiceNowState(
        "local.acceptance", "generated-local-credential", "sensitive-body"
    )

    acceptance_module._assert_on_time_path_no_requests(state)
    for method in ("GET", "POST", "PATCH"):
        state.reset()
        state.request_history.append(method)
        with pytest.raises(AssertionError, match="contacted the ServiceNow API"):
            acceptance_module._assert_on_time_path_no_requests(state)


def test_mock_s3_client_models_present_and_missing_marker(acceptance_module) -> None:
    state = acceptance_module.MockS3State(marker_exists=True)
    client = acceptance_module.MockS3Client(state)

    assert client.head_object(Bucket="demo-bucket", Key="feed.ready") == {
        "ContentLength": 0
    }
    assert state.request_history == [{"Bucket": "demo-bucket", "Key": "feed.ready"}]

    state.marker_exists = False
    with pytest.raises(acceptance_module.MockS3Error) as error:
        client.head_object(Bucket="demo-bucket", Key="feed.ready")

    assert error.value.response["Error"]["Code"] == "404"


def test_local_acceptance_script_ignores_poisoned_python_environment(
    tmp_path: Path,
) -> None:
    poison_path = tmp_path / "pythonpath"
    user_site = (
        tmp_path
        / "userbase"
        / "lib"
        / f"python{sys.version_info.major}.{sys.version_info.minor}"
        / "site-packages"
    )
    for shadow_root in (poison_path, user_site):
        shadow_root.mkdir(parents=True)
        (shadow_root / "airflow.py").write_text(
            "raise RuntimeError('POISONED AIRFLOW MODULE LOADED')\n"
        )
        provider = shadow_root / "upmc_provider_servicenow"
        provider.mkdir()
        (provider / "__init__.py").write_text(
            "raise RuntimeError('POISONED PROVIDER MODULE LOADED')\n"
        )

    poisoned_environment = os.environ.copy()
    poisoned_environment.update(
        {
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://external/database",
            "AIRFLOW__CORE__EXECUTOR": "CeleryExecutor",
            "AIRFLOW__SECRETS__BACKEND": "external.secrets.Backend",
            "AIRFLOW_CONN_SERVICENOW_DEFAULT": (
                "https://caller-credential@example.invalid"
            ),
            "AIRFLOW_CONFIG": "/external/airflow.cfg",
            "HTTP_PROXY": "http://external.proxy.invalid",
            "HTTPS_PROXY": "https://external.proxy.invalid",
            "ALL_PROXY": "socks5://external.proxy.invalid",
            "PYTHONNOUSERSITE": "0",
            "PYTHONPATH": str(poison_path),
            "PYTHONUSERBASE": str(tmp_path / "userbase"),
        }
    )
    caller_python = Path(sys._base_executable)
    assert caller_python != PROJECT_ROOT / ".venv-airflow3" / "bin" / "python"
    result = subprocess.run(
        [str(caller_python), str(SCRIPT)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env=poisoned_environment,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == PASS_SUMMARY
    assert result.stderr == ""
