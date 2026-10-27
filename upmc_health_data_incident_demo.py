from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param, TriggerRule
from airflow.sdk.exceptions import AirflowException, AirflowFailException

from upmc_provider_servicenow.operators.incident import (
    ServiceNowUpsertIncidentOperator,
)

DAG_ID = "upmc_health_data_incident_demo"
ENVIRONMENT = "demo"
SIMULATOR_TASK_ID = "simulate_healthcare_file_ingestion"
FAILURE_MESSAGE = (
    "Inbound healthcare file was not available before the processing cutoff"
)
SIMULATOR_LOG_URL_TEMPLATE = (
    "{{ ti.log_url | "
    "replace('task_id=create_or_update_servicenow_incident', "
    "'task_id=simulate_healthcare_file_ingestion') "
    "if ti is defined else 'unavailable' }}"
)

logger = logging.getLogger(__name__)


def simulate_healthcare_file_ingestion(**context: object) -> None:
    params = context["params"]
    if not isinstance(params, dict):
        raise AirflowException("DAG parameters were not available")
    if params["simulate_failure"]:
        raise AirflowException(FAILURE_MESSAGE)
    logger.info("Healthcare file ingestion simulation completed successfully")


def mark_pipeline_failed() -> None:
    raise AirflowFailException(
        "The demo pipeline failed and its ServiceNow incident was handled"
    )


def report_success() -> None:
    logger.info("No ServiceNow incident is required for the successful demo run")


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    is_paused_upon_creation=True,
    params={"simulate_failure": Param(True, type="boolean")},
    tags=["upmc", "demo", "servicenow"],
) as dag:
    simulator = PythonOperator(
        task_id=SIMULATOR_TASK_ID,
        python_callable=simulate_healthcare_file_ingestion,
    )

    incident = ServiceNowUpsertIncidentOperator(
        task_id="create_or_update_servicenow_incident",
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=2,
        retry_delay=timedelta(minutes=1),
        servicenow_conn_id="servicenow_default",
        short_description="Airflow demo workload failure: {{ dag.dag_id }}",
        description=(
            "DAG: {{ dag.dag_id }}\n"
            "Run: {{ run_id }}\n"
            f"Environment: {ENVIRONMENT}\n"
            f"Failed task: {SIMULATOR_TASK_ID}\n"
            f"Error: {FAILURE_MESSAGE}\n"
            f"Airflow log URL: {SIMULATOR_LOG_URL_TEMPLATE}"
        ),
        correlation_id="{{ dag.dag_id }}:demo:{{ run_id }}",
        impact="2",
        urgency="2",
        work_notes=(
            "The Airflow demo failure handler processed this run. "
            f"Review the task log at {SIMULATOR_LOG_URL_TEMPLATE}."
        ),
    )

    terminal_failure = PythonOperator(
        task_id="mark_pipeline_failed",
        python_callable=mark_pipeline_failed,
    )

    success = PythonOperator(
        task_id="report_success",
        python_callable=report_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    simulator >> [incident, success]
    incident >> terminal_failure
