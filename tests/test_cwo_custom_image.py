from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_dockerfile_preserves_the_cwo_runtime_contract() -> None:
    dockerfile = (PROJECT_ROOT / "Dockerfile").read_text()

    assert "ARG CWO_BASE_IMAGE" in dockerfile
    assert "FROM ${CWO_BASE_IMAGE}" in dockerfile
    assert "ARG AIRFLOW_VENV_DIR=/opt/airflow_venv" in dockerfile
    assert 'uv pip install --python "${AIRFLOW_VENV_DIR}/bin/python"' in dockerfile
    assert "--constraint /usr/local/airflow/constraints.txt" in dockerfile
    assert dockerfile.count("USER root") == 1
    assert dockerfile.rstrip().endswith("USER 50000")
    assert "/entrypoint" not in dockerfile
    assert "istio_and_db_check_wrapper.sh" not in dockerfile
    assert "airflow_local_settings.py" not in dockerfile


def test_custom_requirements_installs_only_the_provider_wheel() -> None:
    requirements = [
        line.strip()
        for line in (PROJECT_ROOT / "custom-requirements.txt").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]

    assert requirements == [
        "/tmp/upmc_airflow_provider_servicenow-0.1.0-py3-none-any.whl"
    ]


def test_docker_context_excludes_local_state_and_credentials() -> None:
    patterns = set((PROJECT_ROOT / ".dockerignore").read_text().splitlines())

    assert {
        ".git",
        ".venv/",
        ".venv-airflow3/",
        ".airflow/",
        ".airflow3/",
        ".env",
        "*.pem",
        "*.key",
        "airflow.db",
        "logs/",
    } <= patterns
