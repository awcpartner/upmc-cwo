from __future__ import annotations

import hashlib
import math
import re
import time
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

import requests
from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk.exceptions import AirflowException


class _AmbiguousServiceNowWrite(AirflowException):
    """A write might have reached ServiceNow before its response failed."""


class ServiceNowHook(HttpHook):
    """Interact with the ServiceNow incident Table API.

    Bounded reconciliation protects sequential Airflow task retries after
    ambiguous responses. It cannot prevent simultaneous writers from racing;
    absolute concurrency control requires a ServiceNow uniqueness rule, which
    is outside the no-customization PDI scope.
    """

    _INCIDENT_ENDPOINT = "/api/now/table/incident"
    _JOURNAL_ENDPOINT = "/api/now/table/sys_journal_field"
    _CORRELATION_ID_PATTERN = re.compile(r"[A-Za-z0-9_.~:+-]{1,250}")
    _SYS_ID_PATTERN = re.compile(r"[0-9a-fA-F]{32}")
    _MARKER_PREFIX = "airflow_idempotency_"
    _READ_HEADERS = {"Accept": "application/json"}
    _WRITE_HEADERS = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    _NO_RESPONSE_LOGGING = {"check_response": False}
    _DEFAULT_TIMEOUT = (5.0, 30.0)
    RECONCILIATION_ATTEMPTS = 3
    RECONCILIATION_DELAY_SECONDS = 0.1

    def __init__(self, servicenow_conn_id: str = "servicenow_default") -> None:
        super().__init__(method="GET", http_conn_id=servicenow_conn_id)

    def find_active_incident(self, correlation_id: str) -> dict[str, str] | None:
        """Find an incident by correlation, including closed/inactive records.

        The historical method name remains for provider API compatibility.
        """
        self._validate_correlation_id(correlation_id)
        payload = self._request(
            "GET",
            self._INCIDENT_ENDPOINT,
            data={
                "sysparm_query": f"correlation_id={correlation_id}",
                "sysparm_limit": 1,
                "sysparm_fields": "sys_id,number",
            },
            headers=self._READ_HEADERS,
        )
        records = payload.get("result")
        if not isinstance(records, list):
            raise AirflowException("ServiceNow returned a malformed response")
        if not records:
            return None
        return self._minimal_result(records[0])

    def create_incident(self, payload: dict[str, Any]) -> dict[str, str]:
        response = self._request(
            "POST",
            self._INCIDENT_ENDPOINT,
            json=payload,
            headers=self._WRITE_HEADERS,
        )
        return self._minimal_result(response.get("result"))

    def update_incident(self, sys_id: str, work_notes: str) -> dict[str, str]:
        self._validate_sys_id(sys_id)
        marker = self._work_note_marker(work_notes)
        if self._journal_contains_marker(sys_id, marker):
            return self._get_incident(sys_id)

        try:
            response = self._request(
                "PATCH",
                f"{self._INCIDENT_ENDPOINT}/{sys_id}",
                json={"work_notes": f"{marker}\n{work_notes}"},
                headers=self._WRITE_HEADERS,
            )
        except _AmbiguousServiceNowWrite:
            if self._poll_for_work_note_marker(sys_id, marker):
                return self._get_incident(sys_id)
            raise
        return self._minimal_result(response.get("result"))

    def upsert_incident(
        self,
        *,
        short_description: str,
        description: str,
        correlation_id: str,
        impact: str,
        urgency: str,
        work_notes: str,
    ) -> dict[str, str]:
        existing = self.find_active_incident(correlation_id)
        if existing is not None:
            incident = self.update_incident(existing["sys_id"], work_notes)
            return {**incident, "action": "updated"}

        try:
            incident = self.create_incident(
                {
                    "short_description": short_description,
                    "description": description,
                    "correlation_id": correlation_id,
                    "impact": impact,
                    "urgency": urgency,
                    "work_notes": (
                        f"{self._work_note_marker(work_notes)}\n{work_notes}"
                    ),
                }
            )
        except _AmbiguousServiceNowWrite:
            incident = self._poll_for_incident(correlation_id)
            if incident is None:
                raise
        return {**incident, "action": "created"}

    def _poll_for_incident(self, correlation_id: str) -> dict[str, str] | None:
        for attempt in range(self.RECONCILIATION_ATTEMPTS):
            try:
                incident = self.find_active_incident(correlation_id)
            except _AmbiguousServiceNowWrite:
                incident = None
            if incident is not None:
                return incident
            self._sleep_before_next_attempt(attempt)
        return None

    def _poll_for_work_note_marker(self, sys_id: str, marker: str) -> bool:
        for attempt in range(self.RECONCILIATION_ATTEMPTS):
            try:
                if self._journal_contains_marker(sys_id, marker):
                    return True
            except _AmbiguousServiceNowWrite:
                pass
            self._sleep_before_next_attempt(attempt)
        return False

    def _sleep_before_next_attempt(self, attempt: int) -> None:
        if attempt < self.RECONCILIATION_ATTEMPTS - 1:
            time.sleep(self.RECONCILIATION_DELAY_SECONDS)

    def _journal_contains_marker(self, sys_id: str, marker: str) -> bool:
        payload = self._request(
            "GET",
            self._JOURNAL_ENDPOINT,
            data={
                "sysparm_query": (
                    f"element_id={sys_id}^element=work_notes^valueLIKE{marker}"
                ),
                "sysparm_limit": 1,
                "sysparm_fields": "sys_id",
            },
            headers=self._READ_HEADERS,
        )
        records = payload.get("result")
        if not isinstance(records, list):
            raise AirflowException("ServiceNow returned a malformed response")
        if not records:
            return False
        record = records[0]
        if not isinstance(record, Mapping):
            raise AirflowException("ServiceNow returned a malformed response")
        self._validate_response_sys_id(record.get("sys_id"))
        return True

    def _get_incident(self, sys_id: str) -> dict[str, str]:
        payload = self._request(
            "GET",
            f"{self._INCIDENT_ENDPOINT}/{sys_id}",
            data={"sysparm_fields": "sys_id,number"},
            headers=self._READ_HEADERS,
        )
        return self._minimal_result(payload.get("result"))

    @classmethod
    def _work_note_marker(cls, work_notes: str) -> str:
        if not isinstance(work_notes, str):
            raise AirflowException("ServiceNow work_notes must be a string")
        digest = hashlib.sha256(work_notes.encode()).hexdigest()
        return f"{cls._MARKER_PREFIX}{digest}"

    @classmethod
    def _validate_correlation_id(cls, correlation_id: object) -> None:
        if not isinstance(
            correlation_id, str
        ) or not cls._CORRELATION_ID_PATTERN.fullmatch(correlation_id):
            raise AirflowException(
                "ServiceNow correlation_id contains unsupported characters"
            )

    @classmethod
    def _validate_sys_id(cls, sys_id: object) -> None:
        if not isinstance(sys_id, str) or not cls._SYS_ID_PATTERN.fullmatch(sys_id):
            raise AirflowException(
                "ServiceNow sys_id must contain exactly 32 hexadecimal characters"
            )

    @classmethod
    def _validate_response_sys_id(cls, sys_id: object) -> None:
        try:
            cls._validate_sys_id(sys_id)
        except AirflowException:
            raise AirflowException("ServiceNow returned a malformed response") from None

    def run_and_check(
        self,
        session: requests.Session,
        prepped_request: requests.PreparedRequest,
        extra_options: dict[Any, Any],
    ) -> requests.Response:
        """Send without allowing HttpHook to log transport exception details."""
        options = extra_options or {}
        self._validate_transport_url(prepped_request.url)
        settings = session.merge_environment_settings(
            prepped_request.url,
            proxies=options.get("proxies", {}),
            stream=options.get("stream", False),
            verify=options.get("verify"),
            cert=options.get("cert"),
        )
        send_kwargs: dict[str, Any] = {
            "timeout": self._finite_timeout(options.get("timeout")),
            "allow_redirects": False,
        }
        send_kwargs.update(settings)
        response = session.send(prepped_request, **send_kwargs)
        if options.get("check_response", True):
            self._raise_for_status(response.status_code)
        return response

    @staticmethod
    def _validate_transport_url(url: str | None) -> None:
        try:
            parsed = urlsplit(url or "")
            hostname = parsed.hostname
        except ValueError:
            hostname = None
            parsed = urlsplit("")

        if parsed.scheme == "https" and hostname:
            return
        if parsed.scheme == "http" and ServiceNowHook._is_loopback(hostname):
            return
        raise AirflowException("ServiceNow requires secure transport")

    @staticmethod
    def _is_loopback(hostname: str | None) -> bool:
        if not hostname:
            return False
        return hostname.casefold() in {"localhost", "127.0.0.1", "::1"}

    @classmethod
    def _finite_timeout(cls, timeout: object) -> float | tuple[float, float]:
        if timeout is None:
            return cls._DEFAULT_TIMEOUT
        if isinstance(timeout, bool):
            raise AirflowException("ServiceNow timeout configuration is invalid")
        if isinstance(timeout, int | float):
            if timeout > 0 and math.isfinite(timeout):
                return float(timeout)
            raise AirflowException("ServiceNow timeout configuration is invalid")
        if (
            isinstance(timeout, tuple)
            and len(timeout) == 2
            and all(
                isinstance(value, int | float)
                and not isinstance(value, bool)
                and value > 0
                and math.isfinite(value)
                for value in timeout
            )
        ):
            return float(timeout[0]), float(timeout[1])
        raise AirflowException("ServiceNow timeout configuration is invalid")

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> Mapping[str, Any]:
        self.method = method
        try:
            response = self.run(
                endpoint,
                **kwargs,
                extra_options=self._NO_RESPONSE_LOGGING,
            )
        except requests.RequestException:
            raise _AmbiguousServiceNowWrite("ServiceNow transport error") from None
        self._raise_for_status(response.status_code)
        try:
            payload = response.json()
        except (TypeError, ValueError):
            raise AirflowException("ServiceNow returned a malformed response") from None
        if not isinstance(payload, Mapping):
            raise AirflowException("ServiceNow returned a malformed response")
        return payload

    @staticmethod
    def _raise_for_status(status_code: int) -> None:
        if 200 <= status_code < 300:
            return
        if status_code == 401:
            message = "ServiceNow authentication failed"
        elif status_code == 403:
            message = "ServiceNow authorization failed"
        elif status_code == 429:
            message = "ServiceNow request was rate limited"
        elif 500 <= status_code < 600:
            raise _AmbiguousServiceNowWrite("ServiceNow server error")
        else:
            message = f"ServiceNow request failed with HTTP status {status_code}"
        raise AirflowException(message)

    @staticmethod
    def _minimal_result(value: object) -> dict[str, str]:
        if not isinstance(value, Mapping):
            raise AirflowException("ServiceNow returned a malformed response")
        sys_id = value.get("sys_id")
        number = value.get("number")
        if not isinstance(sys_id, str) or not isinstance(number, str) or not number:
            raise AirflowException("ServiceNow returned a malformed response")
        ServiceNowHook._validate_response_sys_id(sys_id)
        return {"sys_id": sys_id, "number": number}
