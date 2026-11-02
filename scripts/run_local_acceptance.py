#!/usr/bin/env python3
"""Run the Airflow demo against an in-process, local-only ServiceNow API."""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import secrets
import subprocess
import sys
import tempfile
import threading
from contextlib import redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType
from urllib.parse import parse_qs, urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = PROJECT_ROOT / "dags" / "upmc_health_data_incident_demo.py"
VENV_DIRECTORY = PROJECT_ROOT / ".venv-airflow3"
VENV_BIN_DIRECTORY = VENV_DIRECTORY / "bin"
VENV_PYTHON = VENV_BIN_DIRECTORY / "python"
PROVIDER_SOURCE_DIRECTORY = PROJECT_ROOT / "src"
_ISOLATED_SENTINEL = "_UPMC_ACCEPTANCE_ISOLATED"
SYS_ID = "a" * 32
PASS_SUMMARY = (
    "PASS: on-time feed no incident; delayed feed incident and successful DAG; "
    "idempotency one POST/one work note; sanitized 401"
)
AIRFLOW_ENVIRONMENT_KEYS = frozenset(
    {
        "AIRFLOW_CONFIG",
        "AIRFLOW_CONN_SERVICENOW_DEFAULT",
        "AIRFLOW_HOME",
        "AIRFLOW__CORE__DAGS_FOLDER",
        "AIRFLOW__CORE__EXECUTOR",
        "AIRFLOW__CORE__LOAD_EXAMPLES",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        "AIRFLOW__SECRETS__BACKEND",
        "AIRFLOW__SECRETS__BACKEND_KWARGS",
        "HOME",
        "NO_PROXY",
        "PATH",
        "PYTHONNOUSERSITE",
        "TMPDIR",
        "no_proxy",
    }
)


def _ensure_isolated_runtime() -> None:
    trusted_path = os.pathsep.join(
        (
            str(VENV_BIN_DIRECTORY),
            "/usr/bin",
            "/bin",
            "/usr/sbin",
            "/sbin",
        )
    )
    if os.environ.get(_ISOLATED_SENTINEL) != "1":
        if not VENV_PYTHON.is_file():
            raise AssertionError(
                "the repository .venv-airflow3 Python executable was not found"
            )
        isolated_environment = {
            _ISOLATED_SENTINEL: "1",
            "PATH": trusted_path,
            "PYTHONNOUSERSITE": "1",
        }
        os.execve(
            str(VENV_PYTHON),
            [str(VENV_PYTHON), "-I", str(Path(__file__).resolve())],
            isolated_environment,
        )

    if not sys.flags.isolated:
        raise AssertionError("the acceptance child is not running in isolated mode")
    if Path(sys.executable).resolve() != VENV_PYTHON.resolve():
        raise AssertionError("the acceptance child is not using the repository venv")
    sys.path.insert(0, str(PROVIDER_SOURCE_DIRECTORY))


class MockServiceNowState:
    def __init__(self, username: str, credential: str, sensitive_body: str) -> None:
        token = base64.b64encode(f"{username}:{credential}".encode()).decode()
        self.authorization = f"Basic {token}"
        self.sensitive_body = sensitive_body
        self.incidents: list[dict[str, object]] = []
        self.request_history: list[str] = []
        self.post_count = 0
        self.patch_count = 0
        self.lock = threading.Lock()

    def reset(self) -> None:
        with self.lock:
            self.incidents.clear()
            self.request_history.clear()
            self.post_count = 0
            self.patch_count = 0

    def record_request(self, method: str) -> None:
        with self.lock:
            self.request_history.append(method)


class MockS3Error(Exception):
    def __init__(self, code: str) -> None:
        super().__init__("local-only S3 response")
        self.response = {"Error": {"Code": code}}


class MockS3State:
    def __init__(self, marker_exists: bool) -> None:
        self.marker_exists = marker_exists
        self.request_history: list[dict[str, str]] = []


class MockS3Client:
    def __init__(self, state: MockS3State) -> None:
        self.state = state

    def head_object(self, **kwargs: str) -> dict[str, int]:
        self.state.request_history.append(kwargs)
        if not self.state.marker_exists:
            raise MockS3Error("404")
        return {"ContentLength": 0}


def _mock_boto3_module(state: MockS3State) -> ModuleType:
    module = ModuleType("boto3")

    def client(service_name: str) -> MockS3Client:
        if service_name != "s3":
            raise AssertionError("the demo requested an unexpected AWS service")
        return MockS3Client(state)

    module.client = client
    return module


class MockServiceNowHandler(BaseHTTPRequestHandler):
    server: MockServiceNowServer

    def log_message(self, format: str, *args: object) -> None:
        return

    def _send(self, status: int, payload: object) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        return self.headers.get("Authorization") == self.server.state.authorization

    def _require_authorization(self) -> bool:
        if self._authorized():
            return True
        self._send(401, {"error": self.server.state.sensitive_body})
        return False

    def _json_body(self) -> dict[str, object]:
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length))
        if not isinstance(payload, dict):
            raise ValueError("expected a JSON object")
        return payload

    def do_GET(self) -> None:
        self.server.state.record_request("GET")
        if not self._require_authorization():
            return
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        with self.server.state.lock:
            if parsed.path == "/api/now/table/incident":
                expression = query.get("sysparm_query", [""])[0]
                correlation_id = expression.removeprefix("correlation_id=").split(
                    "^", 1
                )[0]
                matches = [
                    {
                        "sys_id": incident["sys_id"],
                        "number": incident["number"],
                    }
                    for incident in self.server.state.incidents
                    if incident["correlation_id"] == correlation_id
                ]
                self._send(200, {"result": matches[:1]})
                return
            if parsed.path == "/api/now/table/sys_journal_field":
                expression = query.get("sysparm_query", [""])[0]
                marker = expression.split("^valueLIKE", 1)[-1]
                matches = [
                    {"sys_id": "b" * 32}
                    for incident in self.server.state.incidents
                    if any(
                        marker in str(note) for note in incident.get("work_notes", [])
                    )
                ]
                self._send(200, {"result": matches[:1]})
                return
            prefix = "/api/now/table/incident/"
            if parsed.path.startswith(prefix):
                sys_id = parsed.path.removeprefix(prefix)
                incident = next(
                    (
                        item
                        for item in self.server.state.incidents
                        if item["sys_id"] == sys_id
                    ),
                    None,
                )
                if incident is not None:
                    self._send(
                        200,
                        {
                            "result": {
                                "sys_id": incident["sys_id"],
                                "number": incident["number"],
                            }
                        },
                    )
                    return
        self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        self.server.state.record_request("POST")
        if not self._require_authorization():
            return
        if urlparse(self.path).path != "/api/now/table/incident":
            self._send(404, {"error": "not found"})
            return
        payload = self._json_body()
        with self.server.state.lock:
            self.server.state.post_count += 1
            incident = {
                **payload,
                "sys_id": SYS_ID,
                "number": "INC0001",
                "work_notes": [payload.get("work_notes", "")],
            }
            self.server.state.incidents.append(incident)
        self._send(
            201,
            {"result": {"sys_id": SYS_ID, "number": "INC0001"}},
        )

    def do_PATCH(self) -> None:
        self.server.state.record_request("PATCH")
        if not self._require_authorization():
            return
        sys_id = urlparse(self.path).path.removeprefix("/api/now/table/incident/")
        payload = self._json_body()
        with self.server.state.lock:
            incident = next(
                (
                    item
                    for item in self.server.state.incidents
                    if item["sys_id"] == sys_id
                ),
                None,
            )
            if incident is None:
                self._send(404, {"error": "not found"})
                return
            self.server.state.patch_count += 1
            work_notes = incident["work_notes"]
            if isinstance(work_notes, list):
                work_notes.append(payload.get("work_notes", ""))
            number = incident["number"]
        self._send(200, {"result": {"sys_id": sys_id, "number": number}})


class MockServiceNowServer(ThreadingHTTPServer):
    def __init__(self, state: MockServiceNowState) -> None:
        super().__init__(("127.0.0.1", 0), MockServiceNowHandler)
        self.state = state


def _connection_json(base_url: str, username: str, credential: str) -> str:
    return json.dumps(
        {
            "conn_type": "http",
            "host": base_url,
            "login": username,
            "password": credential,
        }
    )


def _build_airflow_environment(
    *,
    airflow_home: Path,
    temporary_home: Path,
    base_url: str,
    username: str,
    credential: str,
) -> dict[str, str]:
    if not VENV_PYTHON.is_file():
        raise AssertionError(
            "the repository .venv-airflow3 Python executable was not found"
        )
    airflow_home.mkdir(mode=0o700)
    temporary_home.mkdir(mode=0o700)
    trusted_path = os.pathsep.join(
        (
            str(VENV_BIN_DIRECTORY),
            "/usr/bin",
            "/bin",
            "/usr/sbin",
            "/sbin",
        )
    )
    environment = {
        "AIRFLOW_CONFIG": str(airflow_home / "airflow.cfg"),
        "AIRFLOW_CONN_SERVICENOW_DEFAULT": _connection_json(
            base_url, username, credential
        ),
        "AIRFLOW_HOME": str(airflow_home),
        "AIRFLOW__CORE__DAGS_FOLDER": str(PROJECT_ROOT / "dags"),
        "AIRFLOW__CORE__EXECUTOR": "SequentialExecutor",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": (
            f"sqlite:///{airflow_home / 'airflow.db'}"
        ),
        "AIRFLOW__SECRETS__BACKEND": "",
        "AIRFLOW__SECRETS__BACKEND_KWARGS": "{}",
        "HOME": str(temporary_home),
        "NO_PROXY": "127.0.0.1,localhost",
        "PATH": trusted_path,
        "PYTHONNOUSERSITE": "1",
        "TMPDIR": str(temporary_home),
        "no_proxy": "127.0.0.1,localhost",
    }
    if set(environment) != AIRFLOW_ENVIRONMENT_KEYS:
        raise AssertionError("the Airflow environment allowlist is inconsistent")
    return environment


def _assert_on_time_path_no_requests(state: MockServiceNowState) -> None:
    if state.request_history:
        raise AssertionError("the on-time feed path contacted the ServiceNow API")


def _task_states(dag_run: object) -> dict[str, str | None]:
    return {
        task_instance.task_id: task_instance.state
        for task_instance in dag_run.get_task_instances()
    }


def _upsert_arguments(correlation_id: str) -> dict[str, str]:
    return {
        "short_description": "Local-only Airflow acceptance failure",
        "description": "Fake operational data; no health record and no PHI",
        "correlation_id": correlation_id,
        "impact": "2",
        "urgency": "2",
        "work_notes": "Local-only retry-safe acceptance note",
    }


def _run_acceptance(credential: str, sensitive_body: str) -> None:
    caller_environment = os.environ.copy()
    username = "local.acceptance"
    state = MockServiceNowState(username, credential, sensitive_body)
    server = MockServiceNowServer(state)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    s3_state = MockS3State(marker_exists=True)
    previous_boto3 = sys.modules.get("boto3")
    sys.modules["boto3"] = _mock_boto3_module(s3_state)

    airflow_parent = PROJECT_ROOT / ".airflow"
    airflow_parent.mkdir(exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(
            prefix="acceptance-", dir=airflow_parent
        ) as acceptance_directory:
            acceptance_root = Path(acceptance_directory)
            airflow_environment = _build_airflow_environment(
                airflow_home=acceptance_root / "airflow",
                temporary_home=acceptance_root / "home",
                base_url=base_url,
                username=username,
                credential=credential,
            )
            os.environ.clear()
            os.environ.update(airflow_environment)

            migration = subprocess.run(
                [str(VENV_PYTHON), "-I", "-m", "airflow", "db", "migrate"],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                check=False,
                env=airflow_environment,
            )
            if migration.returncode != 0:
                raise AssertionError("local Airflow database migration failed")

            import pendulum
            from airflow.models import DagBag
            from airflow.sdk.exceptions import AirflowException
            from airflow.utils.state import DagRunState, TaskInstanceState

            import upmc_provider_servicenow
            from upmc_provider_servicenow.hooks.servicenow import ServiceNowHook

            provider_file = Path(upmc_provider_servicenow.__file__).resolve()
            if not provider_file.is_relative_to(PROVIDER_SOURCE_DIRECTORY.resolve()):
                raise AssertionError("the repository provider source was not imported")

            dag_bag = DagBag(
                dag_folder=str(DAG_FILE),
                include_examples=False,
                safe_mode=False,
            )
            if dag_bag.import_errors:
                raise AssertionError("the demo DAG did not import cleanly")
            dag = dag_bag.get_dag("upmc_health_data_incident_demo")
            if dag is None:
                raise AssertionError("the demo DAG was not found")

            state.reset()
            s3_state.marker_exists = True
            on_time_run = dag.test(
                logical_date=pendulum.datetime(2026, 10, 26, 10, tz="UTC"),
            )
            if on_time_run.state != DagRunState.SUCCESS:
                raise AssertionError("the on-time feed path did not succeed")
            _assert_on_time_path_no_requests(state)

            state.reset()
            s3_state.marker_exists = False
            delayed_run = dag.test(
                logical_date=pendulum.datetime(2026, 10, 26, 10, 5, tz="UTC"),
            )
            delayed_states = _task_states(delayed_run)
            if delayed_run.state != DagRunState.SUCCESS:
                raise AssertionError("the delayed feed path did not succeed")
            if state.post_count != 1 or len(state.incidents) != 1:
                raise AssertionError(
                    "the delayed feed path did not create one incident"
                )
            if delayed_states.get("create_or_update_servicenow_incident") != (
                TaskInstanceState.SUCCESS
            ):
                raise AssertionError("incident handling did not succeed")
            if delayed_states.get("report_feed_on_time") != TaskInstanceState.SKIPPED:
                raise AssertionError("the on-time branch was not skipped")
            if delayed_states.get("record_monitoring_outcome") != (
                TaskInstanceState.SUCCESS
            ):
                raise AssertionError("the monitoring outcome did not succeed")

            state.reset()
            hook = ServiceNowHook()
            arguments = _upsert_arguments("local-acceptance-retry")
            first = hook.upsert_incident(**arguments)
            second = hook.upsert_incident(**arguments)
            if first["action"] != "created" or second["action"] != "updated":
                raise AssertionError("the hook did not reconcile the repeated upsert")
            if state.post_count != 1 or state.patch_count != 0:
                raise AssertionError("the repeated upsert issued a duplicate write")
            notes = state.incidents[0]["work_notes"]
            if not isinstance(notes, list) or len(notes) != 1:
                raise AssertionError("the repeated upsert duplicated the work note")

            os.environ["AIRFLOW_CONN_SERVICENOW_UNAUTHORIZED"] = _connection_json(
                base_url, username, f"wrong-{credential}"
            )
            captured_logs = io.StringIO()
            handler = logging.StreamHandler(captured_logs)
            root_logger = logging.getLogger()
            root_logger.addHandler(handler)
            try:
                try:
                    ServiceNowHook(
                        servicenow_conn_id="servicenow_unauthorized"
                    ).create_incident({"short_description": "Local-only auth check"})
                except AirflowException as error:
                    surfaced = f"{error}\n{captured_logs.getvalue()}"
                else:
                    raise AssertionError("the mock API did not return a 401 response")
            finally:
                root_logger.removeHandler(handler)
            if credential in surfaced or sensitive_body in surfaced:
                raise AssertionError("the 401 path exposed sensitive content")
            if "ServiceNow authentication failed" not in surfaced:
                raise AssertionError("the 401 path was not sanitized")
    finally:
        if previous_boto3 is None:
            sys.modules.pop("boto3", None)
        else:
            sys.modules["boto3"] = previous_boto3
        os.environ.clear()
        os.environ.update(caller_environment)
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        if server_thread.is_alive():
            raise AssertionError("the local mock server did not stop")


def main() -> int:
    credential = f"local-only-{secrets.token_hex(12)}"
    sensitive_body = f"mock-sensitive-{secrets.token_hex(12)}"
    captured = io.StringIO()
    try:
        with redirect_stdout(captured), redirect_stderr(captured):
            _run_acceptance(credential, sensitive_body)
        emitted = captured.getvalue()
        if credential in emitted or sensitive_body in emitted:
            raise AssertionError("captured output exposed sensitive content")
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    print(PASS_SUMMARY)
    return 0


if __name__ == "__main__":
    _ensure_isolated_runtime()
    raise SystemExit(main())
