# UPMC CWO deployment runbook

## Purpose

This runbook deploys the custom ServiceNow provider and S3 feed-monitoring DAG
to Cloudera Workflow Orchestrator. It covers image reproduction, CWO
configuration, external-service setup, validation, troubleshooting, and
rollback.

Never commit or paste Docker, ServiceNow, AWS, or Git credentials into this
repository.

## Deployment artifacts

- CWO base image:
  `container.repository.cloudera.com/cloudera/cloudera-airflow-3.2.1-py3.12:1.6.0-b85`
- Custom image: `docker.io/<registry-namespace>/cwo-servicenow:0.1.0`
- Provider wheel:
  `dist/upmc_airflow_provider_servicenow-0.1.0-py3-none-any.whl`
- DAG: `dags/upmc_health_data_incident_demo.py`
- S3 marker:
  `s3://<s3-bucket-name>/demo-health-feed/inbound/feed.ready`
- Airflow connection: `servicenow_default`

## 1. Confirm prerequisites

Confirm all of the following before deployment:

- CWO 1.6.0 with custom-image support.
- Apache Airflow 3.2.1 and Python 3.12.
- Docker Desktop or an approved CI builder.
- Network access to the Cloudera image registry and Docker Hub.
- A Docker Hub repository named `<registry-namespace>/cwo-servicenow` that is accessible to CWO.
- A Docker Hub token that can push during build and a token that can pull from
  CWO.
- CWO worker access to the S3 bucket and ServiceNow hostname.
- A ServiceNow integration user with incident Table API access.
- A Git repository configured as the CWO DAG bundle.

Confirm the workload cluster architecture before selecting the Docker build
platform. The commands below target `linux/amd64`.

## 2. Validate the source

From the repository root:

```bash
cd /path/to/UPMC
source .venv-airflow3/bin/activate

export AIRFLOW_HOME="$PWD/.airflow3"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

python -m pytest -q
python -m ruff check .
python -m ruff format --check .
python scripts/run_local_acceptance.py
```

Expected acceptance output:

```text
PASS: on-time feed no incident; delayed feed incident and successful DAG; idempotency one POST/one work note; sanitized 401
```

## 3. Build the provider wheel

```bash
python -m build --wheel
test -f dist/upmc_airflow_provider_servicenow-0.1.0-py3-none-any.whl
shasum -a 256 dist/upmc_airflow_provider_servicenow-0.1.0-py3-none-any.whl
```

The wheel contains the custom ServiceNow hook, operator, and provider metadata.

## 4. Pull and validate the CWO base image

```bash
export CWO_BASE_IMAGE='container.repository.cloudera.com/cloudera/cloudera-airflow-3.2.1-py3.12:1.6.0-b85'

docker pull "$CWO_BASE_IMAGE"
docker run --rm "$CWO_BASE_IMAGE" airflow version
docker run --rm "$CWO_BASE_IMAGE" python --version
```

Required results:

```text
Airflow 3.2.1
Python 3.12.x
```

Confirm the runtime contract:

```bash
docker run --rm --entrypoint /bin/sh "$CWO_BASE_IMAGE" -c '
test "$(id -u)" = "50000"
test -x /opt/airflow_venv/bin/python
test -f /usr/local/airflow/constraints.txt
echo "CWO runtime contract: OK"
'
```

## 5. Build and validate the custom image

```bash
export CUSTOM_IMAGE='docker.io/<registry-namespace>/cwo-servicenow:0.1.0'

docker buildx build \
  --load \
  --platform=linux/amd64 \
  --build-arg CWO_BASE_IMAGE="${CWO_BASE_IMAGE}" \
  --tag "${CUSTOM_IMAGE}" \
  --file Dockerfile \
  .
```

Inspect and test:

```bash
docker image inspect "$CUSTOM_IMAGE" \
  --format 'Architecture={{.Architecture}} User={{.Config.User}} Size={{.Size}}'

docker run --rm --platform=linux/amd64 "$CUSTOM_IMAGE" airflow version
docker run --rm --platform=linux/amd64 "$CUSTOM_IMAGE" python --version

docker run --rm --platform=linux/amd64 "$CUSTOM_IMAGE" python -c \
  "from importlib.metadata import version; from upmc_provider_servicenow.hooks.servicenow import ServiceNowHook; print(version('upmc-airflow-provider-servicenow'))"
```

Required results are architecture `amd64`, user `50000`, Airflow `3.2.1`,
Python `3.12.x`, and provider version `0.1.0`.

## 6. Push the image

Create or confirm a Docker Hub repository that CWO can access. If registry
authentication is enabled, authenticate with an access token:

```bash
docker login docker.io --username <registry-username>
docker push "$CUSTOM_IMAGE"
docker buildx imagetools inspect "$CUSTOM_IMAGE"
```

Record the resulting immutable digest in the release evidence. Do not store the
token in shell scripts or Git.

## 7. Configure the CWO deployment

Enable the custom-image option and enter:

```text
Docker Registry: docker.io
Docker Registry Username: <registry-username>
Docker Registry Password: Docker Hub access token
Docker Registry Email: optional
Image Name: <registry-namespace>/cwo-servicenow:0.1.0
```

The image name excludes `docker.io`. Wait until the DAG processor, triggerer,
and worker pods are healthy. Investigate `ImagePullBackOff`, authentication
errors, or restart loops before proceeding.


## 9. Configure ServiceNow

Prepare the integration user:

- User ID: `airflow.integration`
- Active: selected
- Locked out: cleared
- Incident access: `itil`
- Basic Auth API access: convert the dedicated identity to Web Service Access
  Only or grant `snc_basic_auth_api_access`

Create the standard Airflow HTTP connection:

```text
Connection ID: servicenow_default
Connection Type: HTTP
Host: https://<servicenow-instance>.service-now.com
Login: airflow.integration
Password: integration-user password
```

Do not include an `/api` path in the host. Basic Authentication is for the
developer-instance demo only; use OAuth and a secrets backend in production.

## 10. Deploy the DAG through Git

Copy `dags/upmc_health_data_incident_demo.py` into the path synchronized by the
CWO DAG bundle. Commit and push only the intended source changes through the
normal repository workflow.

Wait for CWO to load the new Git revision. Confirm that the DAG has no import
errors and contains:

```text
check_s3_feed_marker
route_by_feed_status
report_feed_on_time
create_or_update_servicenow_incident
record_monitoring_outcome
```

Keep the DAG paused until S3 and ServiceNow configuration is complete.

## 11. Validate the on-time path

Create the empty marker using an approved AWS identity:

```bash
aws s3api put-object \
  --bucket <s3-bucket-name> \
  --key demo-health-feed/inbound/feed.ready
```

Trigger the DAG without parameters.

Expected task states:

- `check_s3_feed_marker`: success
- `route_by_feed_status`: success
- `report_feed_on_time`: success
- `create_or_update_servicenow_incident`: skipped
- `record_monitoring_outcome`: success
- DAG run: success

Confirm that no ServiceNow incident was created.

## 12. Validate the delayed path

Delete only the empty demo marker:

```bash
aws s3api delete-object \
  --bucket <s3-bucket-name> \
  --key demo-health-feed/inbound/feed.ready
```

Trigger a new DAG run.

Expected task states:

- `check_s3_feed_marker`: success with status `DELAYED`
- `route_by_feed_status`: success
- `report_feed_on_time`: skipped
- `create_or_update_servicenow_incident`: success
- `record_monitoring_outcome`: success
- DAG run: success

In ServiceNow, open the incident list and search for:

```text
Healthcare feed delayed
```

Rerun while the marker remains absent. Confirm that the same feed-date incident
is reused rather than creating a duplicate.

## 13. Troubleshooting

### Image cannot be pulled

- Verify registry hostname `docker.io`.
- Verify image name `<registry-namespace>/cwo-servicenow:0.1.0`.
- If registry authentication is enabled, verify the Docker Hub token can read
  the repository.
- Verify workload-cluster egress to Docker Hub.

### DAG import error

- Confirm the custom image is enabled on the DAG processor.
- Confirm provider version `0.1.0` is installed.
- Confirm the Git bundle contains the intended DAG revision.

### S3 returns access denied

- Check both the IAM identity policy and permissions boundary.
- Ensure `s3:ListBucket` uses the bucket ARN without `/*`.
- Ensure `s3:GetObject` uses the object or prefix ARN with `/*`.
- Check bucket and KMS key policies where applicable.

### ServiceNow returns 401

- Confirm the exact user ID and password.
- Confirm the user is active and unlocked.
- Configure Basic Auth Restriction for Web Service Access Only or grant
  `snc_basic_auth_api_access`.

### ServiceNow returns 403

- Confirm the user has `itil`.
- Review incident and journal-table ACLs.

### Logs are unavailable

- Confirm the worker IAM role and permissions boundary allow S3 log access.
- Verify `s3:ListBucket` on the bucket and object operations on
  `airflow-dags/*`.

## 14. Rollback

The S3-monitoring change is DAG-only. To roll it back:

1. Revert the DAG Git commit.
2. Push the prior approved DAG revision.
3. Wait for CWO to load the prior DAG version.
4. Confirm DAG parsing and task graph.
5. Keep the DAG paused if validation fails.

If the custom-image deployment itself fails, select the previously approved
image tag or digest in CWO and repeat pod-health, provider-discovery, DAG-import,
and worker-task checks.

## 15. Production hardening

- Replace Basic Authentication with OAuth.
- Store secrets in the approved Airflow secrets backend.
- Replace the static marker with a date-partitioned completion marker.
- Add a production schedule and SLA ownership.
- Add incident recovery, resolution, or closure behavior.
- Reduce IAM permissions to the exact marker and log prefixes.
- Scan, sign, and promote immutable images by digest.
- Monitor CWO component health, DAG failures, ServiceNow rate limits, and stale
  incidents.
