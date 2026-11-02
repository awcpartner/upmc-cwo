from __future__ import annotations

from typing import Any

from airflow.sdk import BaseOperator
from airflow.sdk.exceptions import AirflowException

from upmc_provider_servicenow.hooks.servicenow import ServiceNowHook


class ServiceNowUpsertIncidentOperator(BaseOperator):
    """Create or update a ServiceNow incident by correlation ID."""

    template_fields = (
        "short_description",
        "description",
        "correlation_id",
        "impact",
        "urgency",
        "work_notes",
    )

    def __init__(
        self,
        *,
        short_description: str,
        description: str,
        correlation_id: str,
        impact: str,
        urgency: str,
        work_notes: str,
        servicenow_conn_id: str = "servicenow_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.short_description = short_description
        self.description = description
        self.correlation_id = correlation_id
        self.impact = impact
        self.urgency = urgency
        self.work_notes = work_notes
        self.servicenow_conn_id = servicenow_conn_id

    def execute(self, context: dict[str, Any]) -> dict[str, str]:
        for field in (
            "short_description",
            "description",
            "correlation_id",
            "work_notes",
        ):
            self._validate_required_text(field, getattr(self, field))
        self._validate_priority("impact", self.impact)
        self._validate_priority("urgency", self.urgency)
        hook = ServiceNowHook(servicenow_conn_id=self.servicenow_conn_id)
        return hook.upsert_incident(
            short_description=self.short_description,
            description=self.description,
            correlation_id=self.correlation_id,
            impact=self.impact,
            urgency=self.urgency,
            work_notes=self.work_notes,
        )

    @staticmethod
    def _validate_priority(field: str, value: object) -> None:
        if not isinstance(value, str) or value not in {"1", "2", "3"}:
            raise AirflowException(
                f"ServiceNow {field} must be one of the strings 1, 2, or 3"
            )

    @staticmethod
    def _validate_required_text(field: str, value: object) -> None:
        if not isinstance(value, str) or not value.strip():
            raise AirflowException(f"ServiceNow {field} must be a non-empty string")
