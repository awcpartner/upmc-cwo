from __future__ import annotations

import hashlib
from unittest.mock import Mock

import pytest
from airflow.sdk.exceptions import AirflowException

from upmc_provider_servicenow.hooks.servicenow import ServiceNowHook
from upmc_provider_servicenow.operators import incident as incident_module
from upmc_provider_servicenow.operators.incident import (
    ServiceNowUpsertIncidentOperator,
)

SYS_ID = "c" * 32


def marked(note: str) -> str:
    digest = hashlib.sha256(note.encode()).hexdigest()
    return f"airflow_idempotency_{digest}\n{note}"


def response(status_code: int, payload: object) -> Mock:
    result = Mock()
    result.status_code = status_code
    result.json.return_value = payload
    result.text = "SENSITIVE RESPONSE BODY"
    return result


def operator(**overrides: object) -> ServiceNowUpsertIncidentOperator:
    arguments = {
        "task_id": "upsert_incident",
        "short_description": "Synthetic alert",
        "description": "Demo description",
        "correlation_id": "dag-run-1",
        "impact": "2",
        "urgency": "3",
        "work_notes": "Synthetic observation",
    }
    arguments.update(overrides)
    return ServiceNowUpsertIncidentOperator(**arguments)


def test_operator_declares_all_incident_fields_as_templated() -> None:
    assert set(ServiceNowUpsertIncidentOperator.template_fields) == {
        "short_description",
        "description",
        "correlation_id",
        "impact",
        "urgency",
        "work_notes",
    }


def test_operator_renders_fields_and_returns_only_minimal_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        response(200, {"result": []}),
        response(
            201,
            {
                "result": {
                    "sys_id": SYS_ID,
                    "number": "INC008",
                    "description": "must not escape",
                }
            },
        ),
    ]
    calls: list[dict[str, object]] = []

    def fake_run(self: ServiceNowHook, endpoint: str, **kwargs: object) -> Mock:
        calls.append({"endpoint": endpoint, **kwargs})
        return responses.pop(0)

    monkeypatch.setattr(ServiceNowHook, "run", fake_run)
    task = operator(
        short_description="{{ params.summary }}",
        description="{{ params.description }}",
        correlation_id="{{ params.correlation_id }}",
        impact="{{ params.impact }}",
        urgency="{{ params.urgency }}",
        work_notes="{{ params.work_notes }}",
    )
    context = {
        "params": {
            "summary": "Rendered summary",
            "description": "Rendered description",
            "correlation_id": "rendered-key",
            "impact": "1",
            "urgency": "2",
            "work_notes": "Rendered note",
        }
    }

    task.render_template_fields(context)
    result = task.execute(context)

    assert result == {
        "sys_id": SYS_ID,
        "number": "INC008",
        "action": "created",
    }
    assert calls[1]["json"] == {
        "short_description": "Rendered summary",
        "description": "Rendered description",
        "correlation_id": "rendered-key",
        "impact": "1",
        "urgency": "2",
        "work_notes": marked("Rendered note"),
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("impact", "0"),
        ("impact", "4"),
        ("impact", 1),
        ("urgency", "high"),
        ("urgency", 2),
    ],
)
def test_operator_rejects_invalid_impact_and_urgency(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    called = False

    def fail_if_called(self: ServiceNowHook, endpoint: str, **kwargs: object) -> Mock:
        nonlocal called
        called = True
        raise AssertionError("HTTP must not be called for invalid input")

    monkeypatch.setattr(ServiceNowHook, "run", fail_if_called)
    task = operator(**{field: value})  # type: ignore[arg-type]

    with pytest.raises(AirflowException, match=field):
        task.execute({})

    assert called is False


@pytest.mark.parametrize(
    "field",
    [
        "short_description",
        "description",
        "correlation_id",
        "work_notes",
    ],
)
@pytest.mark.parametrize("value", [None, "", "   \t", 123])
def test_operator_rejects_invalid_required_templated_fields_before_hook(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    hook_factory = Mock(side_effect=AssertionError("hook must not be constructed"))
    monkeypatch.setattr(incident_module, "ServiceNowHook", hook_factory)
    task = operator(**{field: value})

    with pytest.raises(AirflowException) as error:
        task.execute({})

    message = str(error.value)
    assert field in message
    assert repr(value) not in message
    hook_factory.assert_not_called()


def test_operator_strictly_validates_correlation_before_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def fail_if_called(self: ServiceNowHook, endpoint: str, **kwargs: object) -> Mock:
        nonlocal called
        called = True
        raise AssertionError("HTTP must not be called for invalid input")

    monkeypatch.setattr(ServiceNowHook, "run", fail_if_called)

    with pytest.raises(AirflowException, match="correlation_id"):
        operator(correlation_id="run^ORactive=false").execute({})

    assert called is False


def test_operator_does_not_log_descriptions_or_credentials(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def fake_run(self: ServiceNowHook, endpoint: str, **kwargs: object) -> Mock:
        return response(401, {"error": "SECRET RESPONSE"})

    monkeypatch.setattr(ServiceNowHook, "run", fake_run)
    task = operator(
        short_description="SECRET SUMMARY",
        description="SECRET DESCRIPTION",
        work_notes="SECRET NOTES",
    )

    with pytest.raises(AirflowException):
        task.execute({})

    assert "SECRET" not in caplog.text
