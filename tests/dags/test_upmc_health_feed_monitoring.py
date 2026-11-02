from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from airflow.models import DagBag
from airflow.sdk import TriggerRule
from airflow.sdk.exceptions import AirflowException

from upmc_provider_servicenow.operators.incident import (
    ServiceNowUpsertIncidentOperator,
)

DAG_ID = "upmc_health_data_incident_demo"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DAG_FILE = PROJECT_ROOT / "dags" / f"{DAG_ID}.py"
S3_BUCKET = "<s3-bucket-name>"
S3_KEY = "demo-health-feed/inbound/feed.ready"


class FakeS3Error(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(f"unsafe AWS detail for {code}")
        self.response = {"Error": {"Code": code}}


class FakeS3Client:
    def __init__(self, error_code: str | None = None) -> None:
        self.error_code = error_code
        self.requests: list[dict[str, str]] = []

    def head_object(self, **kwargs: str) -> dict[str, object]:
        self.requests.append(kwargs)
        if self.error_code is not None:
            raise FakeS3Error(self.error_code)
        return {"ContentLength": 0, "ETag": '"demo-marker"'}


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    return DagBag(
        dag_folder=str(DAG_FILE.parent),
        include_examples=False,
        safe_mode=False,
    )


@pytest.fixture(scope="module")
def dag(dag_bag: DagBag):
    return dag_bag.dags.get(DAG_ID)


def test_dag_imports_and_preserves_manual_production_controls(
    dag_bag: DagBag, dag
) -> None:
    assert dag_bag.import_errors == {}
    assert dag is not None
    assert dag.schedule is None
    assert dag.catchup is False
    assert dag.dagrun_timeout == timedelta(minutes=30)
    assert dag.is_paused_upon_creation is True
    assert set(dag.tags) == {"upmc", "demo", "servicenow", "s3"}


def test_task_graph_routes_both_handled_outcomes_to_successful_join(dag) -> None:
    assert set(dag.task_ids) == {
        "check_s3_feed_marker",
        "route_by_feed_status",
        "report_feed_on_time",
        "create_or_update_servicenow_incident",
        "record_monitoring_outcome",
    }

    check = dag.get_task("check_s3_feed_marker")
    route = dag.get_task("route_by_feed_status")
    healthy = dag.get_task("report_feed_on_time")
    incident = dag.get_task("create_or_update_servicenow_incident")
    outcome = dag.get_task("record_monitoring_outcome")

    assert check.downstream_task_ids == {route.task_id}
    assert route.downstream_task_ids == {healthy.task_id, incident.task_id}
    assert healthy.downstream_task_ids == {outcome.task_id}
    assert incident.downstream_task_ids == {outcome.task_id}
    assert outcome.upstream_task_ids == {healthy.task_id, incident.task_id}
    assert incident.trigger_rule == TriggerRule.ALL_SUCCESS
    assert outcome.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS


def test_present_s3_marker_returns_on_time_without_reading_object_body(dag) -> None:
    client = FakeS3Client()
    check = dag.get_task("check_s3_feed_marker")

    result = check.python_callable(s3_client_factory=lambda: client)

    assert result == {
        "status": "ON_TIME",
        "s3_uri": f"s3://{S3_BUCKET}/{S3_KEY}",
    }
    assert client.requests == [{"Bucket": S3_BUCKET, "Key": S3_KEY}]


@pytest.mark.parametrize("error_code", ["404", "NoSuchKey", "NotFound"])
def test_missing_s3_marker_returns_delayed(dag, error_code: str) -> None:
    client = FakeS3Client(error_code)
    check = dag.get_task("check_s3_feed_marker")

    result = check.python_callable(s3_client_factory=lambda: client)

    assert result["status"] == "DELAYED"
    assert result["s3_uri"] == f"s3://{S3_BUCKET}/{S3_KEY}"


def test_s3_access_error_fails_with_sanitized_message(dag) -> None:
    client = FakeS3Client("AccessDenied")
    check = dag.get_task("check_s3_feed_marker")

    with pytest.raises(
        AirflowException, match="Unable to check S3 feed marker"
    ) as error:
        check.python_callable(s3_client_factory=lambda: client)

    assert "unsafe AWS detail" not in str(error.value)


@pytest.mark.parametrize(
    ("status", "expected_task"),
    [
        ("ON_TIME", "report_feed_on_time"),
        ("DELAYED", "create_or_update_servicenow_incident"),
    ],
)
def test_route_selects_the_expected_response(
    dag, status: str, expected_task: str
) -> None:
    route = dag.get_task("route_by_feed_status")
    ti = SimpleNamespace(
        xcom_pull=lambda task_ids: {
            "status": status,
            "s3_uri": f"s3://{S3_BUCKET}/{S3_KEY}",
        }
    )

    assert route.python_callable(ti=ti) == expected_task


def test_route_rejects_unknown_status(dag) -> None:
    route = dag.get_task("route_by_feed_status")
    ti = SimpleNamespace(xcom_pull=lambda task_ids: {"status": "UNKNOWN"})

    with pytest.raises(AirflowException, match="unexpected feed status"):
        route.python_callable(ti=ti)


def test_incident_is_idempotent_per_feed_date_and_contains_s3_context(dag) -> None:
    task = dag.get_task("create_or_update_servicenow_incident")

    assert isinstance(task, ServiceNowUpsertIncidentOperator)
    assert task.correlation_id == "{{ dag.dag_id }}:demo:{{ ds_nodash }}:feed-delay"
    combined = "\n".join(
        str(getattr(task, field))
        for field in ServiceNowUpsertIncidentOperator.template_fields
    )
    assert f"s3://{S3_BUCKET}/{S3_KEY}" in combined
    assert "{{ run_id }}" in combined
    assert "DELAYED" in combined
    assert task.servicenow_conn_id == "servicenow_default"
    assert 0 <= task.retries <= 3


def test_dag_source_contains_no_phi_or_credentials() -> None:
    source = DAG_FILE.read_text().lower()
    forbidden = {
        "patient_name",
        "medical_record",
        "date_of_birth",
        "social_security",
        "mrn",
        "password",
        "passwd",
        "api_key",
        "client_secret",
        "access_token",
    }

    assert not any(value in source for value in forbidden)
