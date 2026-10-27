from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.sdk import DAG, TriggerRule
from airflow.sdk.exceptions import AirflowException

from upmc_provider_servicenow.operators.incident import (
    ServiceNowUpsertIncidentOperator,
)

DAG_ID = "upmc_health_data_incident_demo"
ENVIRONMENT = "demo"
S3_BUCKET = "upmc-la-s3-backup-bucket"
S3_KEY = "demo-health-feed/inbound/feed.ready"
S3_URI = f"s3://{S3_BUCKET}/{S3_KEY}"
CHECK_TASK_ID = "check_s3_feed_marker"
HEALTHY_TASK_ID = "report_feed_on_time"
INCIDENT_TASK_ID = "create_or_update_servicenow_incident"
CHECK_LOG_URL_TEMPLATE = (
    "{{ ti.log_url | "
    "replace('task_id=create_or_update_servicenow_incident', "
    "'task_id=check_s3_feed_marker') "
    "if ti is defined else 'unavailable' }}"
)

logger = logging.getLogger(__name__)


def _create_s3_client() -> Any:
    import boto3

    return boto3.client("s3")


def _aws_error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if not isinstance(response, Mapping):
        return None
    details = response.get("Error")
    if not isinstance(details, Mapping):
        return None
    code = details.get("Code")
    return str(code) if code is not None else None


def check_s3_feed_marker(
    *,
    s3_client_factory: Callable[[], Any] | None = None,
) -> dict[str, str]:
    client = (s3_client_factory or _create_s3_client)()
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except Exception as error:
        if _aws_error_code(error) in {"404", "NoSuchKey", "NotFound"}:
            logger.warning("Expected healthcare feed marker is delayed: %s", S3_URI)
            return {"status": "DELAYED", "s3_uri": S3_URI}
        raise AirflowException("Unable to check S3 feed marker") from None
    logger.info("Expected healthcare feed marker arrived on time: %s", S3_URI)
    return {"status": "ON_TIME", "s3_uri": S3_URI}


def route_by_feed_status(**context: object) -> str:
    task_instance = context.get("ti")
    if task_instance is None or not hasattr(task_instance, "xcom_pull"):
        raise AirflowException("S3 feed check result was not available")
    result = task_instance.xcom_pull(task_ids=CHECK_TASK_ID)
    if not isinstance(result, Mapping):
        raise AirflowException("S3 feed check returned an invalid result")
    status = result.get("status")
    if status == "ON_TIME":
        return HEALTHY_TASK_ID
    if status == "DELAYED":
        return INCIDENT_TASK_ID
    raise AirflowException("S3 feed check returned an unexpected feed status")


def report_feed_on_time() -> None:
    logger.info("Healthcare feed arrived on time; no ServiceNow incident is required")


def record_monitoring_outcome() -> None:
    logger.info("Healthcare feed monitoring response completed successfully")


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    is_paused_upon_creation=True,
    tags=["upmc", "demo", "servicenow", "s3"],
) as dag:
    check = PythonOperator(
        task_id=CHECK_TASK_ID,
        python_callable=check_s3_feed_marker,
    )

    route = BranchPythonOperator(
        task_id="route_by_feed_status",
        python_callable=route_by_feed_status,
    )

    healthy = PythonOperator(
        task_id=HEALTHY_TASK_ID,
        python_callable=report_feed_on_time,
    )

    incident = ServiceNowUpsertIncidentOperator(
        task_id=INCIDENT_TASK_ID,
        retries=2,
        retry_delay=timedelta(minutes=1),
        servicenow_conn_id="servicenow_default",
        short_description="Healthcare feed delayed: {{ dag.dag_id }}",
        description=(
            "DAG: {{ dag.dag_id }}\n"
            "Run: {{ run_id }}\n"
            f"Environment: {ENVIRONMENT}\n"
            "Feed status: DELAYED\n"
            f"Expected marker: {S3_URI}\n"
            f"S3 check log: {CHECK_LOG_URL_TEMPLATE}"
        ),
        correlation_id="{{ dag.dag_id }}:demo:{{ ds_nodash }}:feed-delay",
        impact="2",
        urgency="2",
        work_notes=(
            "Airflow detected that the expected healthcare feed marker was missing "
            f"and completed escalation. Review {CHECK_LOG_URL_TEMPLATE}."
        ),
    )

    outcome = PythonOperator(
        task_id="record_monitoring_outcome",
        python_callable=record_monitoring_outcome,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    check >> route >> [healthy, incident]
    healthy >> outcome
    incident >> outcome
