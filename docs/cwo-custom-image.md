# Build and deploy the CWO custom image

This demo builds one custom image:

```text
docker.io/<registry-namespace>/cwo-servicenow:0.1.0
```

It is the official CWO 1.6.0 Airflow 3.2.1/Python 3.12 image plus the
`upmc-airflow-provider-servicenow` wheel. ServiceNow does not need a container.
The DAG remains in Git, and credentials remain in the Airflow HTTP connection.

## 1. Obtain the required base image

Ask the CWO platform owner for the complete pullable image reference and any
required registry credentials. It must follow this shape:

```text
<cloudera-registry>/cloudera/cloudera-airflow-3.2.1-py3.12:1.6.0
```

Do not guess this value and do not substitute an upstream Apache Airflow or
generic Python image. Log in to the Cloudera registry if it is private, then
confirm that Docker can pull the exact image.

```bash
export CWO_BASE_IMAGE='<complete-reference-from-cloudera>'
docker pull "$CWO_BASE_IMAGE"
```

## 2. Build and test the provider wheel

```bash
source .venv-airflow3/bin/activate
export AIRFLOW_HOME="$PWD/.airflow3"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

python -m pytest -q
python -m ruff check .
python -m ruff format --check .
rm -rf build dist
python -m build --wheel
test -f dist/upmc_airflow_provider_servicenow-0.1.0-py3-none-any.whl
```

The Dockerfile installs only that wheel into `/opt/airflow_venv` with the
base image's `/usr/local/airflow/constraints.txt`. It temporarily uses root and
returns to user `50000`.

## 3. Build the Linux image

Create a Docker Hub repository named `<registry-namespace>/cwo-servicenow` that is accessible to CWO. Use a
Docker Hub personal access token for login; do not put it in this repository or
chat.

```bash
docker login docker.io --username <registry-username>

export CUSTOM_IMAGE='docker.io/<registry-namespace>/cwo-servicenow:0.1.0'
docker buildx build \
  --platform linux/amd64 \
  --build-arg CWO_BASE_IMAGE="$CWO_BASE_IMAGE" \
  --tag "$CUSTOM_IMAGE" \
  --file Dockerfile \
  --load \
  .
```

Use `linux/amd64` only after confirming that it matches the workload cluster.
If the cluster uses another architecture, use the platform value supplied by
the CWO owner.

## 4. Inspect and validate the image

```bash
test "$(docker image inspect "$CUSTOM_IMAGE" --format '{{.Config.User}}')" = "50000"
docker run --rm "$CUSTOM_IMAGE" airflow version
docker run --rm "$CUSTOM_IMAGE" python --version
docker run --rm "$CUSTOM_IMAGE" \
  python -c "from upmc_provider_servicenow.hooks.servicenow import ServiceNowHook; print('provider import OK')"
docker run --rm "$CUSTOM_IMAGE" \
  airflow providers list
```

Required results are Airflow `3.2.1`, Python `3.12.x`, final user `50000`, a
successful provider import, and provider version `0.1.0` in the provider list.

## 5. Push the image

```bash
docker push "$CUSTOM_IMAGE"
docker buildx imagetools inspect "$CUSTOM_IMAGE"
```

Record the pushed digest. Use the immutable digest for production promotion and
rollback even though the CWO form asks for the tag.

## 6. Configure the CWO deployment

Enter these values:

- **Enable Custom Image:** selected
- **Docker Registry:** `docker.io`
- **Docker Registry Username:** `<registry-username>`
- **Docker Registry Password:** a read-only Docker Hub access token
- **Docker Registry Email:** optional Docker Hub account email
- **Image Name:** `<registry-namespace>/cwo-servicenow:0.1.0`

The image name excludes `docker.io`. CWO uses this image for the DAG processor,
triggerer, and worker. The API server and scheduler continue using Cloudera's
standard image.

## 7. Configure and deploy the demo

Create the standard HTTP connection `servicenow_default` in Airflow:

- host: `https://<servicenow-instance>.service-now.com`
- login: the dedicated ServiceNow integration user
- password: supplied only through the connection or approved secret backend

Because this is a standard HTTP connection, it can be created in the UI even
though the custom provider is absent from the API-server image. Configure the
DAG Git source, confirm that `upmc_health_data_incident_demo.py` imports, and
leave it paused until the ServiceNow PDI is awake.

Allow the worker identity to read object metadata for:

```text
s3://<s3-bucket-name>/demo-health-feed/inbound/feed.ready
```

Grant `s3:GetObject` on that object and `s3:ListBucket` on the bucket, restricted
to the demo prefix where the IAM policy supports it. The list permission lets
S3 return a distinguishable not-found response instead of access denied.

Create that empty marker and trigger the DAG. The on-time branch succeeds and
does not contact ServiceNow. Delete only the marker and trigger again. The
delayed branch creates or updates one correlated incident and the DAG succeeds
because escalation completed. An S3 authorization/network error or ServiceNow
failure still fails the DAG.

This DAG-only change does not require rebuilding the custom image; update the
Git source and wait for CWO to parse the new DAG version.

## Rollback

Keep the previous approved image tag and digest. If pods fail to start, the
provider is missing, DAG parsing fails, or the worker test fails, restore the
previous image in the CWO deployment and repeat the health and DAG-import
checks. Never copy this provider into the Airflow plugins directory.
