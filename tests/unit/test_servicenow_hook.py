from __future__ import annotations

import hashlib
from collections.abc import Mapping
from unittest.mock import Mock

import pytest
from airflow.sdk.exceptions import AirflowException
from requests import ConnectionError

from upmc_provider_servicenow.hooks.servicenow import ServiceNowHook

SYS_ID = "a" * 32
OTHER_SYS_ID = "b" * 32


def marked(note: str) -> str:
    digest = hashlib.sha256(note.encode()).hexdigest()
    return f"airflow_idempotency_{digest}\n{note}"


def response(status_code: int, payload: object) -> Mock:
    result = Mock()
    result.status_code = status_code
    result.json.return_value = payload
    result.text = "SENSITIVE RESPONSE BODY"
    return result


def malformed_json_response(status_code: int = 200) -> Mock:
    result = response(status_code, {})
    result.json.side_effect = ValueError("SENSITIVE INVALID JSON")
    return result


def install_http_responses(
    monkeypatch: pytest.MonkeyPatch,
    responses: list[Mock | BaseException],
) -> list[dict[str, object]]:
    calls: list[dict[str, object]] = []

    def fake_run(self: ServiceNowHook, endpoint: str, **kwargs: object) -> Mock:
        calls.append({"endpoint": endpoint, **kwargs})
        result = responses.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    monkeypatch.setattr(ServiceNowHook, "run", fake_run)
    return calls


def test_find_active_incident_finds_correlation_regardless_of_active_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_http_responses(
        monkeypatch,
        [
            response(
                200,
                {
                    "result": [
                        {
                            "sys_id": SYS_ID,
                            "number": "INC001",
                            "description": "omit",
                        }
                    ]
                },
            )
        ],
    )

    result = ServiceNowHook().find_active_incident("airflow-run-7")

    assert result == {"sys_id": SYS_ID, "number": "INC001"}
    assert calls == [
        {
            "endpoint": "/api/now/table/incident",
            "data": {
                "sysparm_query": "correlation_id=airflow-run-7",
                "sysparm_limit": 1,
                "sysparm_fields": "sys_id,number",
            },
            "headers": {"Accept": "application/json"},
            "extra_options": {"check_response": False},
        }
    ]


def test_find_active_incident_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_http_responses(monkeypatch, [response(200, {"result": []})])

    assert ServiceNowHook().find_active_incident("missing") is None


@pytest.mark.parametrize(
    "correlation_id",
    [
        "",
        "run^ORactive=false",
        "run=value",
        "run\nactive=false",
        "run/value",
        "run%5Eactive=false",
        "a" * 251,
    ],
)
def test_find_active_incident_rejects_query_injection_before_http(
    monkeypatch: pytest.MonkeyPatch, correlation_id: str
) -> None:
    calls = install_http_responses(monkeypatch, [])

    with pytest.raises(AirflowException, match="correlation_id"):
        ServiceNowHook().find_active_incident(correlation_id)

    assert calls == []


def test_lookup_rejects_malformed_json_without_leaking_response(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    install_http_responses(monkeypatch, [malformed_json_response()])

    with pytest.raises(AirflowException, match="malformed response") as error:
        ServiceNowHook().find_active_incident("manual__safe-run")

    emitted = f"{error.value}\n{caplog.text}"
    assert "SENSITIVE" not in emitted
    assert error.value.__suppress_context__ is True
    assert error.value.__cause__ is None


def test_create_incident_posts_json_and_returns_minimal_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_http_responses(
        monkeypatch,
        [
            response(
                201,
                {
                    "result": {
                        "sys_id": SYS_ID,
                        "number": "INC002",
                        "description": "omit",
                    }
                },
            )
        ],
    )
    payload = {
        "short_description": "Synthetic alert",
        "description": "Demo only",
        "correlation_id": "run-8",
        "impact": "2",
        "urgency": "3",
    }

    result = ServiceNowHook().create_incident(payload)

    assert result == {"sys_id": SYS_ID, "number": "INC002"}
    assert calls[0]["endpoint"] == "/api/now/table/incident"
    assert calls[0]["json"] == payload
    assert calls[0]["headers"] == {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def test_update_incident_patches_marked_note_when_marker_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_http_responses(
        monkeypatch,
        [
            response(200, {"result": []}),
            response(
                200,
                {"result": {"sys_id": SYS_ID, "number": "INC003"}},
            ),
        ],
    )

    result = ServiceNowHook().update_incident(SYS_ID, "Synthetic retry")

    assert result == {"sys_id": SYS_ID, "number": "INC003"}
    assert calls[0]["endpoint"] == "/api/now/table/sys_journal_field"
    assert calls[0]["data"] == {
        "sysparm_query": (
            f"element_id={SYS_ID}^element=work_notes"
            f"^valueLIKE{marked('Synthetic retry').splitlines()[0]}"
        ),
        "sysparm_limit": 1,
        "sysparm_fields": "sys_id",
    }
    assert calls[1]["endpoint"] == f"/api/now/table/incident/{SYS_ID}"
    assert calls[1]["json"] == {"work_notes": marked("Synthetic retry")}


def test_update_incident_skips_patch_when_marker_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_http_responses(
        monkeypatch,
        [
            response(200, {"result": [{"sys_id": OTHER_SYS_ID}]}),
            response(
                200,
                {"result": {"sys_id": SYS_ID, "number": "INC003"}},
            ),
        ],
    )

    result = ServiceNowHook().update_incident(SYS_ID, "Synthetic retry")

    assert result == {"sys_id": SYS_ID, "number": "INC003"}
    assert [call["endpoint"] for call in calls] == [
        "/api/now/table/sys_journal_field",
        f"/api/now/table/incident/{SYS_ID}",
    ]
    assert "json" not in calls[1]


def test_update_rejects_malformed_json_without_leaking_note(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    install_http_responses(
        monkeypatch,
        [response(200, {"result": []}), malformed_json_response()],
    )

    with pytest.raises(AirflowException, match="malformed response") as error:
        ServiceNowHook().update_incident(SYS_ID, "SENSITIVE WORK NOTE")

    emitted = f"{error.value}\n{caplog.text}"
    assert "SENSITIVE" not in emitted
    assert error.value.__suppress_context__ is True
    assert error.value.__cause__ is None


def test_ambiguous_patch_reconciles_marker_found_during_polling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    calls = install_http_responses(
        monkeypatch,
        [
            response(200, {"result": []}),
            response(503, {"error": "ambiguous accepted response"}),
            response(200, {"result": []}),
            response(200, {"result": [{"sys_id": OTHER_SYS_ID}]}),
            response(
                200,
                {"result": {"sys_id": SYS_ID, "number": "INC003"}},
            ),
        ],
    )
    hook = ServiceNowHook()

    result = hook.update_incident(SYS_ID, "Accepted before response failed")

    assert result == {"sys_id": SYS_ID, "number": "INC003"}
    patch_calls = [
        call
        for call in calls
        if call["endpoint"] == f"/api/now/table/incident/{SYS_ID}" and "json" in call
    ]
    assert len(patch_calls) == 1
    assert sleeps == [ServiceNowHook.RECONCILIATION_DELAY_SECONDS]


def test_ambiguous_patch_re_raises_after_bounded_polling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    calls = install_http_responses(
        monkeypatch,
        [
            response(200, {"result": []}),
            response(503, {"error": "ambiguous accepted response"}),
            *[
                response(200, {"result": []})
                for _ in range(ServiceNowHook.RECONCILIATION_ATTEMPTS)
            ],
        ],
    )

    with pytest.raises(AirflowException, match="server error"):
        ServiceNowHook().update_incident(
            SYS_ID, "Accepted status could not be confirmed"
        )

    journal_calls = [
        call for call in calls if call["endpoint"] == "/api/now/table/sys_journal_field"
    ]
    assert len(journal_calls) == ServiceNowHook.RECONCILIATION_ATTEMPTS + 1
    assert len(sleeps) == ServiceNowHook.RECONCILIATION_ATTEMPTS - 1


@pytest.mark.parametrize("status_code", [401, 403])
def test_patch_auth_failure_does_not_poll(
    monkeypatch: pytest.MonkeyPatch, status_code: int
) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    calls = install_http_responses(
        monkeypatch,
        [
            response(200, {"result": []}),
            response(status_code, {"error": "SECRET AUTH RESPONSE"}),
        ],
    )

    with pytest.raises(AirflowException, match="authentication|authorization"):
        ServiceNowHook().update_incident(SYS_ID, "SENSITIVE WORK NOTE")

    assert len(calls) == 2
    assert sleeps == []


@pytest.mark.parametrize("sys_id", ["", "abc123", "g" * 32, "a" * 31, "a" * 33])
def test_update_incident_rejects_invalid_sys_id_before_http(
    monkeypatch: pytest.MonkeyPatch, sys_id: str
) -> None:
    calls = install_http_responses(monkeypatch, [])

    with pytest.raises(AirflowException, match="sys_id"):
        ServiceNowHook().update_incident(sys_id, "Synthetic retry")

    assert calls == []


def test_upsert_creates_when_no_active_incident(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_http_responses(
        monkeypatch,
        [
            response(200, {"result": []}),
            response(201, {"result": {"sys_id": SYS_ID, "number": "INC004"}}),
        ],
    )

    result = ServiceNowHook().upsert_incident(
        short_description="Synthetic alert",
        description="No PHI",
        correlation_id="stable-key",
        impact="2",
        urgency="1",
        work_notes="First observation",
    )

    assert result == {"sys_id": SYS_ID, "number": "INC004", "action": "created"}
    assert [call["endpoint"] for call in calls] == [
        "/api/now/table/incident",
        "/api/now/table/incident",
    ]
    assert calls[1]["json"]["work_notes"] == marked("First observation")


def test_upsert_updates_existing_incident_without_posting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_http_responses(
        monkeypatch,
        [
            response(200, {"result": [{"sys_id": SYS_ID, "number": "INC005"}]}),
            response(200, {"result": []}),
            response(200, {"result": {"sys_id": SYS_ID, "number": "INC005"}}),
        ],
    )

    result = ServiceNowHook().upsert_incident(
        short_description="Synthetic alert",
        description="No PHI",
        correlation_id="stable-key",
        impact="2",
        urgency="1",
        work_notes="Retry observation",
    )

    assert result == {
        "sys_id": SYS_ID,
        "number": "INC005",
        "action": "updated",
    }
    assert len(calls) == 3
    assert calls[2]["endpoint"].endswith(f"/{SYS_ID}")
    assert calls[2]["json"] == {"work_notes": marked("Retry observation")}


def test_task_retry_does_not_create_duplicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_http_responses(
        monkeypatch,
        [
            response(200, {"result": []}),
            response(201, {"result": {"sys_id": SYS_ID, "number": "INC006"}}),
            response(200, {"result": [{"sys_id": SYS_ID, "number": "INC006"}]}),
            response(200, {"result": [{"sys_id": OTHER_SYS_ID}]}),
            response(200, {"result": {"sys_id": SYS_ID, "number": "INC006"}}),
        ],
    )
    hook = ServiceNowHook()
    arguments = {
        "short_description": "Synthetic alert",
        "description": "No PHI",
        "correlation_id": "same-retry-key",
        "impact": "1",
        "urgency": "1",
        "work_notes": "Retry-safe note",
    }

    first = hook.upsert_incident(**arguments)
    second = hook.upsert_incident(**arguments)

    assert first["action"] == "created"
    assert second["action"] == "updated"
    create_calls = [
        call for call in calls if "json" in call and "correlation_id" in call["json"]
    ]
    assert len(create_calls) == 1
    assert create_calls[0]["json"]["work_notes"] == marked("Retry-safe note")


def test_retry_finds_closed_incident_and_never_posts_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_http_responses(
        monkeypatch,
        [
            response(200, {"result": []}),
            response(201, {"result": {"sys_id": SYS_ID, "number": "INC006"}}),
            response(200, {"result": [{"sys_id": SYS_ID, "number": "INC006"}]}),
            response(200, {"result": []}),
            response(409, {"error": "closed incidents cannot be updated"}),
        ],
    )
    hook = ServiceNowHook()
    arguments = {
        "short_description": "Synthetic alert",
        "description": "No PHI",
        "correlation_id": "same-closed-retry-key",
        "impact": "1",
        "urgency": "1",
        "work_notes": "Retry-safe note",
    }

    first = hook.upsert_incident(**arguments)
    with pytest.raises(AirflowException, match="HTTP status 409"):
        hook.upsert_incident(**arguments)

    assert first["action"] == "created"
    post_calls = [
        call for call in calls if "json" in call and "correlation_id" in call["json"]
    ]
    assert len(post_calls) == 1
    lookup_calls = [
        call
        for call in calls
        if call["endpoint"] == "/api/now/table/incident" and "data" in call
    ]
    assert all(
        "^active=true" not in call["data"]["sysparm_query"] for call in lookup_calls
    )


def test_ambiguous_create_reconciles_incident_found_during_polling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    calls = install_http_responses(
        monkeypatch,
        [
            response(200, {"result": []}),
            response(503, {"error": "ambiguous accepted response"}),
            response(200, {"result": []}),
            response(200, {"result": [{"sys_id": SYS_ID, "number": "INC009"}]}),
        ],
    )
    hook = ServiceNowHook()
    arguments = {
        "short_description": "Synthetic alert",
        "description": "No PHI",
        "correlation_id": "scheduled__2026-10-26T12:52:00+00:00",
        "impact": "1",
        "urgency": "1",
        "work_notes": "Accepted before response failed",
    }

    result = hook.upsert_incident(**arguments)

    assert result == {
        "sys_id": SYS_ID,
        "number": "INC009",
        "action": "created",
    }
    post_calls = [call for call in calls if "json" in call]
    assert len(post_calls) == 1
    assert sleeps == [ServiceNowHook.RECONCILIATION_DELAY_SECONDS]


def test_transport_error_create_reconciles_without_leaking_exception(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr("time.sleep", lambda _: None)
    calls = install_http_responses(
        monkeypatch,
        [
            response(200, {"result": []}),
            ConnectionError("SECRET TRANSPORT CREDENTIAL"),
            response(200, {"result": [{"sys_id": SYS_ID, "number": "INC010"}]}),
        ],
    )

    result = ServiceNowHook().upsert_incident(
        short_description="SECRET DESCRIPTION",
        description="SECRET LONG DESCRIPTION",
        correlation_id="manual__safe-run",
        impact="2",
        urgency="2",
        work_notes="SECRET WORK NOTE",
    )

    assert result == {
        "sys_id": SYS_ID,
        "number": "INC010",
        "action": "created",
    }
    assert len([call for call in calls if "json" in call]) == 1
    assert "SECRET" not in caplog.text


def test_ambiguous_create_re_raises_after_bounded_polling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    calls = install_http_responses(
        monkeypatch,
        [
            response(200, {"result": []}),
            response(503, {"error": "ambiguous accepted response"}),
            *[
                response(200, {"result": []})
                for _ in range(ServiceNowHook.RECONCILIATION_ATTEMPTS)
            ],
        ],
    )

    with pytest.raises(AirflowException, match="server error"):
        ServiceNowHook().upsert_incident(
            short_description="Synthetic alert",
            description="No PHI",
            correlation_id="manual__safe-run",
            impact="2",
            urgency="2",
            work_notes="Could not reconcile",
        )

    assert len([call for call in calls if "json" in call]) == 1
    assert len(sleeps) == ServiceNowHook.RECONCILIATION_ATTEMPTS - 1


@pytest.mark.parametrize("status_code", [401, 403])
def test_create_auth_failure_does_not_poll(
    monkeypatch: pytest.MonkeyPatch, status_code: int
) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    calls = install_http_responses(
        monkeypatch,
        [
            response(200, {"result": []}),
            response(status_code, {"error": "SECRET AUTH RESPONSE"}),
        ],
    )

    with pytest.raises(AirflowException, match="authentication|authorization"):
        ServiceNowHook().upsert_incident(
            short_description="SECRET DESCRIPTION",
            description="SECRET LONG DESCRIPTION",
            correlation_id="manual__safe-run",
            impact="2",
            urgency="2",
            work_notes="SECRET WORK NOTE",
        )

    assert len(calls) == 2
    assert sleeps == []


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"result": {}},
        {"result": {"sys_id": "", "number": "INC007"}},
        {"result": {"sys_id": "id-only"}},
        {"result": []},
    ],
)
def test_create_rejects_malformed_responses(
    monkeypatch: pytest.MonkeyPatch, payload: Mapping[str, object]
) -> None:
    install_http_responses(monkeypatch, [response(201, payload)])

    with pytest.raises(AirflowException, match="malformed response"):
        ServiceNowHook().create_incident({"short_description": "SECRET DESCRIPTION"})


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (401, "authentication failed"),
        (429, "rate limited"),
        (503, "server error"),
    ],
)
def test_http_failures_are_sanitized_and_retryable(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    status_code: int,
    expected: str,
) -> None:
    install_http_responses(
        monkeypatch,
        [response(status_code, {"error": "SECRET RESPONSE CREDENTIAL"})],
    )

    with pytest.raises(AirflowException, match=expected) as error:
        ServiceNowHook().create_incident(
            {
                "short_description": "SECRET DESCRIPTION",
                "password": "SECRET PASSWORD",
            }
        )

    emitted = f"{error.value}\n{caplog.text}"
    assert "SECRET" not in emitted
    assert "SENSITIVE RESPONSE BODY" not in emitted


def test_hook_uses_default_connection_id() -> None:
    hook = ServiceNowHook()

    assert hook.http_conn_id == "servicenow_default"


def test_provider_metadata_registers_hook_and_operator() -> None:
    from upmc_provider_servicenow.get_provider_info import get_provider_info

    info = get_provider_info()

    assert info["package-name"] == "upmc-airflow-provider-servicenow"
    assert info["versions"] == ["0.1.0"]
    assert info["hooks"] == [
        {
            "integration-name": "ServiceNow",
            "python-modules": ["upmc_provider_servicenow.hooks.servicenow"],
        }
    ]
    assert info["operators"] == [
        {
            "integration-name": "ServiceNow",
            "python-modules": ["upmc_provider_servicenow.operators.incident"],
        }
    ]
