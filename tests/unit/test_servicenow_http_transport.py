from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs

import pytest
import requests
import requests_mock
from airflow.sdk.exceptions import AirflowException

from upmc_provider_servicenow.hooks.servicenow import ServiceNowHook

BASE_URL = "https://instance.example"
INCIDENT_URL = f"{BASE_URL}/api/now/table/incident"
SYS_ID = "d" * 32
CONNECTION_URI = "http://integration-user:SECRET_PASSWORD@instance.example/https"


@pytest.fixture(autouse=True)
def servicenow_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "AIRFLOW_CONN_SERVICENOW_DEFAULT",
        CONNECTION_URI,
    )


def test_create_uses_real_http_post_shape(
    requests_mock: requests_mock.Mocker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    requests_mock.post(
        INCIDENT_URL,
        status_code=201,
        json={"result": {"sys_id": SYS_ID, "number": "INC100"}},
    )
    payload = {
        "short_description": "SENSITIVE SUMMARY",
        "description": "SENSITIVE DESCRIPTION",
        "correlation_id": "manual__transport-test",
        "impact": "2",
        "urgency": "3",
        "work_notes": "SENSITIVE WORK NOTE",
    }

    result = ServiceNowHook().create_incident(payload)

    request = requests_mock.request_history[0]
    assert result == {"sys_id": SYS_ID, "number": "INC100"}
    assert request.method == "POST"
    assert request.path == "/api/now/table/incident"
    assert request.json() == payload
    assert request.headers["Accept"] == "application/json"
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["Authorization"].startswith("Basic ")
    assert "SENSITIVE" not in caplog.text
    assert "SECRET_PASSWORD" not in caplog.text


def test_update_uses_real_http_get_and_patch_shapes(
    requests_mock: requests_mock.Mocker,
) -> None:
    requests_mock.get(
        f"{BASE_URL}/api/now/table/sys_journal_field",
        json={"result": []},
    )
    requests_mock.patch(
        f"{INCIDENT_URL}/{SYS_ID}",
        json={"result": {"sys_id": SYS_ID, "number": "INC101"}},
    )

    result = ServiceNowHook().update_incident(SYS_ID, "SENSITIVE TRANSPORT NOTE")

    journal_request, patch_request = requests_mock.request_history
    assert result == {"sys_id": SYS_ID, "number": "INC101"}
    assert journal_request.method == "GET"
    assert journal_request.path == "/api/now/table/sys_journal_field"
    query = parse_qs(journal_request.query)
    assert query["sysparm_query"][0].startswith(f"element_id={SYS_ID}")
    assert patch_request.method == "PATCH"
    assert patch_request.path == f"/api/now/table/incident/{SYS_ID}"
    assert patch_request.json()["work_notes"].endswith("\nSENSITIVE TRANSPORT NOTE")
    assert patch_request.headers["Content-Type"] == "application/json"


def test_http_error_redacts_real_request_and_response(
    requests_mock: requests_mock.Mocker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    requests_mock.post(
        INCIDENT_URL,
        status_code=503,
        text="SENSITIVE RESPONSE BODY",
    )

    with pytest.raises(AirflowException, match="server error") as error:
        ServiceNowHook().create_incident(
            {
                "short_description": "SENSITIVE SUMMARY",
                "description": "SENSITIVE DESCRIPTION",
                "work_notes": "SENSITIVE WORK NOTE",
            }
        )

    emitted = f"{error.value}\n{caplog.text}"
    assert "SENSITIVE" not in emitted
    assert "SECRET_PASSWORD" not in emitted


def test_transport_error_is_sanitized_at_real_http_boundary(
    requests_mock: requests_mock.Mocker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    requests_mock.post(
        INCIDENT_URL,
        exc=requests.ConnectTimeout("SENSITIVE RESPONSE BODY SECRET_PASSWORD"),
    )

    with pytest.raises(AirflowException, match="transport error") as error:
        ServiceNowHook().create_incident(
            {
                "description": "SENSITIVE DESCRIPTION",
                "work_notes": "SENSITIVE WORK NOTE",
            }
        )

    emitted = f"{error.value}\n{caplog.text}"
    assert "SENSITIVE" not in emitted
    assert "SECRET_PASSWORD" not in emitted
    assert error.value.__cause__ is None


def test_external_http_is_rejected_before_request_is_sent(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv(
        "AIRFLOW_CONN_SERVICENOW_DEFAULT",
        "http://integration-user:SECRET_PASSWORD@instance.example",
    )
    sent: list[requests.PreparedRequest] = []

    def fail_if_sent(
        self: requests.Session, request: requests.PreparedRequest, **kwargs: Any
    ) -> requests.Response:
        sent.append(request)
        raise AssertionError("unsafe request was sent")

    monkeypatch.setattr(requests.Session, "send", fail_if_sent)

    with pytest.raises(AirflowException, match="secure transport") as error:
        ServiceNowHook().create_incident({"description": "SENSITIVE PAYLOAD"})

    assert sent == []
    emitted = f"{error.value}\n{caplog.text}"
    assert "SECRET_PASSWORD" not in emitted
    assert "SENSITIVE PAYLOAD" not in emitted
    assert "instance.example" not in emitted


def test_loopback_http_uses_finite_timeouts_and_disables_redirects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "AIRFLOW_CONN_SERVICENOW_DEFAULT",
        "http://integration-user:SECRET_PASSWORD@127.0.0.1:8080",
    )
    sends: list[dict[str, Any]] = []

    def fake_send(
        self: requests.Session, request: requests.PreparedRequest, **kwargs: Any
    ) -> requests.Response:
        sends.append(kwargs)
        result = requests.Response()
        result.status_code = 201
        result.url = request.url
        result._content = (
            f'{{"result": {{"sys_id": "{SYS_ID}", "number": "INC102"}}}}'
        ).encode()
        result.headers["Content-Type"] = "application/json"
        return result

    monkeypatch.setattr(requests.Session, "send", fake_send)

    result = ServiceNowHook().create_incident({"description": "acceptance test"})

    assert result == {"sys_id": SYS_ID, "number": "INC102"}
    assert len(sends) == 1
    assert sends[0]["allow_redirects"] is False
    connect_timeout, read_timeout = sends[0]["timeout"]
    assert 0 < connect_timeout < float("inf")
    assert 0 < read_timeout < float("inf")


@pytest.mark.parametrize(
    "connection_uri",
    [
        "http://integration-user:SECRET_PASSWORD@instance.example/ftp",
        "http://integration-user:SECRET_PASSWORD@192.0.2.1",
        "http://integration-user:SECRET_PASSWORD@127.0.0.2",
    ],
)
def test_unsafe_transport_errors_never_leak_connection_secrets(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    connection_uri: str,
) -> None:
    monkeypatch.setenv("AIRFLOW_CONN_SERVICENOW_DEFAULT", connection_uri)

    with pytest.raises(AirflowException, match="secure transport") as error:
        ServiceNowHook().create_incident({"description": "SENSITIVE PAYLOAD"})

    emitted = f"{error.value}\n{caplog.text}"
    assert "SECRET_PASSWORD" not in emitted
    assert "SENSITIVE PAYLOAD" not in emitted
    assert "instance.example" not in emitted
