# ServiceNow PDI setup for the Airflow demo

## Status and scope

This repository contains local preparation only. No ServiceNow Personal
Developer Instance (PDI) URL or credentials are available here, so the PDI
activation, user creation, connectivity check, and API tests below must be
completed manually in the target environment. Do not treat this document as
evidence that a live PDI has been configured.

> **No PHI:** Never send protected health information (PHI), production
> records, patient identifiers, or realistic patient data to a PDI, Airflow
> logs, shell history, or test incidents. Use clearly synthetic demo data only.

## 1. Wake the PDI

1. Sign in to the [ServiceNow Developer Site](https://developer.servicenow.com/).
2. Open **My Instance** and select **Start Building** or **Wake Instance**.
3. Wait until the instance status is online, then open it.
4. Record its base URL as `SERVICENOW_INSTANCE`, including `https://` and
   excluding `/api/...` (for example, `https://dev12345.service-now.com`).

PDIs hibernate after inactivity. A timeout, DNS failure, or unavailable
instance after an idle period may mean the PDI must be woken again in the
Developer Site. Do not build automation that assumes a PDI is continuously
available.

## 2. Create a dedicated integration user

Perform these steps while signed in as the PDI administrator:

1. Navigate to **All > User Administration > Users** and select **New**.
2. Create a user with:
   - **User ID:** `airflow.integration`
   - **Active:** selected
   - **Web service access only:** selected, if this field is available
   - A generated, unique password stored in an approved secret manager
3. Save the user, open it again, and add the `itil` role from the **Roles**
   related list.
4. Confirm the user can read, create, and update records in the `incident`
   table. The standard `itil` role is suitable for this PDI demo; instance
   customizations and ACLs can change its effective access.

Use the `admin` role only temporarily to troubleshoot permissions. Do not run
the Airflow integration as an administrator. Remove any temporary elevated
role as soon as troubleshooting is complete.

## 3. Set local environment variables

Set variables in the shell used for testing. Do not commit values, place them
in DAG source, or paste them into tickets or logs.

```bash
export SERVICENOW_INSTANCE='https://devXXXXX.service-now.com'
export SERVICENOW_USERNAME='airflow.integration'
read -s -p 'ServiceNow password: ' SERVICENOW_PASSWORD
export SERVICENOW_PASSWORD
printf '\n'

umask 077
export SERVICENOW_CURL_DIR
SERVICENOW_CURL_DIR="$(mktemp -d "${TMPDIR:-/tmp}/servicenow-curl.XXXXXX")"
export SERVICENOW_CURL_CONFIG="$SERVICENOW_CURL_DIR/config"
python - <<'PY'
import json
import os
from pathlib import Path

credential = f"{os.environ['SERVICENOW_USERNAME']}:{os.environ['SERVICENOW_PASSWORD']}"
Path(os.environ["SERVICENOW_CURL_CONFIG"]).write_text(
    f"user = {json.dumps(credential)}\n"
)
PY
chmod 600 "$SERVICENOW_CURL_CONFIG"
unset SERVICENOW_PASSWORD

cleanup_servicenow_curl_config() {
  local curl_config="${SERVICENOW_CURL_CONFIG:-}"
  local curl_dir="${SERVICENOW_CURL_DIR:-}"
  if [ -n "$curl_config" ]; then
    rm -f -- "$curl_config"
  fi
  if [ -n "$curl_dir" ]; then
    rmdir -- "$curl_dir" 2>/dev/null || true
  fi
  unset SERVICENOW_PASSWORD SERVICENOW_USERNAME SERVICENOW_INSTANCE
  unset SERVICENOW_CURL_CONFIG SERVICENOW_CURL_DIR
}

abort_servicenow_curl_session() {
  trap - HUP INT TERM
  cleanup_servicenow_curl_config
  exit 1
}

trap cleanup_servicenow_curl_config EXIT
trap abort_servicenow_curl_session HUP INT TERM
```

The commands below use Basic Authentication only because this is a short-lived
demo. For production, use OAuth and retrieve tokens/client secrets from an
Airflow-supported secret backend. Do not store production secrets directly in
Airflow Variables, DAG files, images, or source control. The temporary curl
configuration is readable only by its owner, keeps the password out of process
arguments, and is removed by the cleanup trap. The password environment
variable is unset immediately after the protected file is created. An interrupt
or termination signal removes the file, unsets the remaining connection values,
and exits with failure; normal shell exit performs the same idempotent cleanup.

## 4. Verify outbound connectivity from the Airflow worker

Run the preflight inside the same worker container, pod, VM, or execution
environment that will run the task:

```bash
curl --silent --show-error --fail-with-body \
  --config "$SERVICENOW_CURL_CONFIG" \
  --connect-timeout 10 --max-time 30 \
  --header 'Accept: application/json' \
  --get "${SERVICENOW_INSTANCE%/}/api/now/table/incident" \
  --data-urlencode 'sysparm_limit=1' \
  --data-urlencode 'sysparm_fields=sys_id,number'
```

A successful request proves that the worker can resolve the PDI hostname,
establish TLS, reach the ServiceNow endpoint through its firewall/proxy path,
authenticate, and read incidents. A test from a laptop or the Airflow webserver
is not sufficient: workers can have different DNS, proxy, routing, firewall,
certificate, and network-policy settings.

If this fails, first confirm that the PDI is awake, then check worker DNS,
egress rules, proxy configuration, trusted certificate authorities, and the
integration user's role and password.

## 5. Test in REST API Explorer

In ServiceNow, navigate to **All > System Web Services > REST > REST API
Explorer**. Select **Table API**, the current API version, and the `incident`
table. REST API Explorer executes requests as the user signed in to the
ServiceNow UI. Use it to verify the request shapes and responses. If
`airflow.integration` is restricted to web-service access, it cannot sign in to
the UI; use the curl tests in the next section to validate that user's actual
permissions. Do not infer the integration user's access from an Explorer
request made by an administrator.

Run these requests using synthetic values only:

1. **GET** `/api/now/table/incident`
   - Add `sysparm_limit=1`.
   - Add `sysparm_fields=sys_id,number`.
   - Expect HTTP `200` and a `result` array. Each returned record should expose
     `sys_id` and `number`.
2. **POST** `/api/now/table/incident`
   - Use a JSON body such as
     `{"short_description":"Synthetic Airflow integration test","description":"Demo data only; contains no PHI","urgency":"3","impact":"3"}`.
   - Expect HTTP `201`. Save `result.sys_id` and `result.number`; `sys_id` is
     required for the PATCH request.
3. **PATCH** `/api/now/table/incident/{sys_id}`
   - Replace `{sys_id}` with the value returned by POST.
   - Use a JSON body such as
     `{"short_description":"Synthetic Airflow integration test - updated"}`.
   - Expect HTTP `200` and confirm `result.sys_id` and `result.number` identify
     the same incident.

Response formatting can vary with ServiceNow settings, so validate values under
the top-level `result` property rather than relying on field order.

## 6. Repeat the API test with curl

The following commands are copyable and read authentication only from the
protected curl configuration. [`jq`](https://jqlang.github.io/jq/) is used to
inspect responses and carry the new incident's `sys_id` into the PATCH.

### GET

```bash
curl --silent --show-error --fail-with-body \
  --config "$SERVICENOW_CURL_CONFIG" \
  --header 'Accept: application/json' \
  --get "${SERVICENOW_INSTANCE%/}/api/now/table/incident" \
  --data-urlencode 'sysparm_limit=1' \
  --data-urlencode 'sysparm_fields=sys_id,number' |
  jq '.result[] | {sys_id, number}'
```

### POST

```bash
POST_RESPONSE="$(
  curl --silent --show-error --fail-with-body \
    --config "$SERVICENOW_CURL_CONFIG" \
    --header 'Accept: application/json' \
    --header 'Content-Type: application/json' \
    --request POST \
    --data '{"short_description":"Synthetic Airflow integration test","description":"Demo data only; contains no PHI","urgency":"3","impact":"3"}' \
    "${SERVICENOW_INSTANCE%/}/api/now/table/incident"
)"
printf '%s\n' "$POST_RESPONSE" | jq '.result | {sys_id, number}'
INCIDENT_SYS_ID="$(printf '%s\n' "$POST_RESPONSE" | jq -er '.result.sys_id')"
```

### PATCH

Run this in the same shell as POST so `INCIDENT_SYS_ID` is available:

```bash
curl --silent --show-error --fail-with-body \
  --config "$SERVICENOW_CURL_CONFIG" \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json' \
  --request PATCH \
  --data '{"short_description":"Synthetic Airflow integration test - updated"}' \
  "${SERVICENOW_INSTANCE%/}/api/now/table/incident/${INCIDENT_SYS_ID}" |
  jq '.result | {sys_id, number}'
```

## 7. Credential lifecycle and cleanup

- Rotate the `airflow.integration` password after setup, whenever exposure is
  suspected, and according to the owning team's credential-rotation policy.
- Update the approved secret store atomically with rotation and rerun the
  worker-side preflight.
- Revoke old credentials and remove temporary `admin` access immediately.
- Delete or close synthetic incidents when the demo is complete.
- Remove the protected file and unset remaining connection values:

  ```bash
  cleanup_servicenow_curl_config
  trap - EXIT HUP INT TERM
  unset POST_RESPONSE INCIDENT_SYS_ID
  ```

- Expect the PDI to hibernate when inactive. Wake it manually before later demo
  runs and repeat the worker-side preflight.
