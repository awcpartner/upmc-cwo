# UPMC Airflow ServiceNow demo

This repository is a one-DAG demonstration of production-style S3 feed
monitoring and ServiceNow incident handling in Apache Airflow. The DAG checks
only for an empty completion marker; it does not create, read, transform, or
transmit real or synthetic health records. Its messages contain no PHI, patient
identifiers, or credentials.

## Architecture and task flow

The manual, paused-by-default DAG `upmc_health_data_incident_demo` has two
handled outcomes:

```text
check_s3_feed_marker -> route_by_feed_status
├─ ON_TIME -> report_feed_on_time --------------------┐
└─ DELAYED -> create_or_update_servicenow_incident ---┤
                                                       └─ record_monitoring_outcome
```

The marker is `s3://<s3-bucket-name>/demo-health-feed/inbound/feed.ready`.
If it exists, no ServiceNow request is made. If it is missing, the provider
finds an incident by feed date, then creates it or appends an
idempotency-marked work note. Both outcomes finish successfully when the
monitoring response completes; S3 access errors and ServiceNow failures still
fail the DAG.

## Project layout and compatibility

- `dags/upmc_health_data_incident_demo.py` — the single demo DAG
- `src/upmc_provider_servicenow/` — provider hook, operator, and metadata
- `tests/` — provider and DAG tests
- `docs/architecture.md` — customer and engineering architecture overview
- `docs/deployment-runbook.md` — end-to-end CWO deployment and validation
- `docs/servicenow-pdi-setup.md` — safe PDI preparation and API checks
- `docs/cwo-custom-image.md` — CWO 1.6 custom-image build and deployment

Local validation targets the CWO 1.6.0 runtime: Python 3.12 and Airflow 3.2.1.
Package metadata supports Airflow `>=3.2.1,<3.3`, HTTP provider `>=6,<7`, and
standard provider `>=1.12,<2`.

## Safe local setup

From the repository root, use an isolated Python 3.12 environment and the
official constraints matching the exact Airflow and Python versions:

```bash
python3.12 -m venv .venv-airflow3
source .venv-airflow3/bin/activate
python -m pip install --upgrade pip

export AIRFLOW_VERSION=3.2.1
export PYTHON_VERSION=3.12
export AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
python -m pip install --constraint "$AIRFLOW_CONSTRAINTS_URL" -e '.[dev]'

export AIRFLOW_HOME="$PWD/.airflow"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db migrate
```

Keep `AIRFLOW_HOME` inside the repository; `.airflow/`, local databases, logs,
virtual environments, and build output are ignored by Git.

## Validate

Run these exact commands from the activated environment:

```bash
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m build --wheel
python -m pip install --no-deps --force-reinstall \
  dist/upmc_airflow_provider_servicenow-0.1.0-py3-none-any.whl
python -m pip show upmc-airflow-provider-servicenow |
  grep -Fx 'Version: 0.1.0'
airflow providers list | grep -F 'upmc-airflow-provider-servicenow'
airflow dags list-import-errors --local
python scripts/run_local_acceptance.py
```

The acceptance command uses only generated fake credentials and operational
demo text. It starts an in-process API bound to localhost, runs both DAG paths,
replaces the caller environment with a local-only Airflow allowlist, checks
retry/idempotency and sanitized 401 handling, stops the server, and never
contacts a PDI or any other external service. Its final output is:

```text
PASS: on-time feed no incident; delayed feed incident and successful DAG; idempotency one POST/one work note; sanitized 401
```

## Configure ServiceNow

Prepare and wake a PDI by following
[the PDI setup guide](docs/servicenow-pdi-setup.md). In Airflow, create an HTTP
connection with:

- connection ID `servicenow_default`
- connection type `HTTP`
- host set only to the HTTPS PDI base URL, with no `/api` path or credentials
- a dedicated integration login and its password/token supplied through the
  connection UI or an approved secrets backend

Do not put credentials in this README, the DAG, command arguments, shell
history, source control, or logs. Basic authentication is demo-only; production
must use OAuth with a secrets backend.

## Run the demo

Wake the PDI and confirm CWO worker connectivity to S3 and ServiceNow.

For the on-time path, create the empty marker through the AWS console or an
approved AWS CLI identity:

```bash
aws s3api put-object \
  --bucket <s3-bucket-name> \
  --key demo-health-feed/inbound/feed.ready
```

Trigger the DAG without parameters. Expected result: the check and on-time
reporter succeed, the incident task is skipped, no incident is created, and the
DAG succeeds.

For the delayed path, delete only the empty demo marker:

```bash
aws s3api delete-object \
  --bucket <s3-bucket-name> \
  --key demo-health-feed/inbound/feed.ready
```

Trigger the DAG again. Expected result: the check reports `DELAYED`, the
provider creates or updates one correlated ServiceNow incident, the on-time
task is skipped, and the DAG succeeds because escalation completed.

The incident contains:

- a short description naming the demo DAG
- description fields for DAG ID, run ID, `demo` environment, delayed status,
  expected S3 marker, and Airflow check log URL
- `correlation_id` in the form `<dag_id>:demo:<feed-date>:feed-delay`
- impact `2`, urgency `2`, and a work note with an idempotency marker

Airflow retries the incident task at most twice. Re-running the same task/run
date uses the same correlation ID; an existing incident, including a closed
one, is never recreated, and the hashed work-note marker prevents a duplicate
note.

## Troubleshooting

- **S3 marker returns 403:** grant the CWO worker `s3:GetObject` for the marker
  and `s3:ListBucket` for the bucket/demo prefix. Without list permission, S3
  can return access denied instead of not found for an absent marker.
- **Timeout, DNS, or connection failure:** wake a sleeping PDI, then verify DNS,
  proxy/TLS trust, and HTTPS egress from the actual Airflow worker. Allowlist
  only the required ServiceNow hostname and port 443.
- **401 / 403:** verify the connection secret and the dedicated user's active
  status, `itil` role, Table API access, and instance ACLs. Never log the secret.
- **429 / 5xx:** treat as retryable/ambiguous. Respect rate limits and inspect
  the bounded reconciliation result before manually retrying.
- **`sys_journal_field` authorization failure:** grant the least read access
  required to locate this integration's work-note marker; do not grant `admin`.
- **Provider import or version mismatch:** activate the venv, reinstall the
  wheel under matching Airflow constraints, confirm supported package versions,
  then rerun `airflow providers list` and `airflow dags list-import-errors`.


For the image build, CWO form values, health checks, and rollback procedure,
follow the [CWO custom-image guide](docs/cwo-custom-image.md). Rollback means
selecting the prior immutable image and rerunning provider, DAG-import,
component, and worker-task checks. External image publishing, Cloudera
deployment, and live PDI execution were not performed locally.

## Verified local evidence

Local validation produced the following evidence without a real Cloudera
cluster or ServiceNow PDI:

- `python -m pytest` passed all 95 included tests under Airflow 3.2.1.
- `python -m build --wheel` built the wheel; the documented
  `pip install --no-deps --force-reinstall` command installed it;
  `pip show ... | grep -Fx 'Version: 0.1.0'` verified its exact version; and
  `airflow providers list` discovered the provider.
- `airflow dags list-import-errors --local` reported no errors.
- `python scripts/run_local_acceptance.py` ran on-time and delayed feed paths
  against fake S3 and its localhost ServiceNow API, verified incident creation
  and successful monitoring completion, and printed the pass summary shown
  above.
- The same acceptance command verified one POST and one work note across two
  identical hook invocations, plus sanitized exception/log output for a mock
  401 response containing sensitive text.

These results validate local behavior only; they do not imply a real
Cloudera or PDI deployment.
