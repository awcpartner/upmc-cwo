ARG CWO_BASE_IMAGE
FROM ${CWO_BASE_IMAGE}

ARG UV_VERSION=0.7.3
ARG AIRFLOW_VENV_DIR=/opt/airflow_venv

USER root

COPY custom-requirements.txt /tmp/custom-requirements.txt
COPY dist/upmc_airflow_provider_servicenow-0.1.0-py3-none-any.whl /tmp/upmc_airflow_provider_servicenow-0.1.0-py3-none-any.whl

RUN pip install --disable-pip-version-check --no-cache-dir "uv==${UV_VERSION}" && \
    uv pip install --python "${AIRFLOW_VENV_DIR}/bin/python" \
      --no-cache-dir \
      -r /tmp/custom-requirements.txt \
      --constraint /usr/local/airflow/constraints.txt && \
    rm /tmp/custom-requirements.txt \
       /tmp/upmc_airflow_provider_servicenow-0.1.0-py3-none-any.whl

USER 50000
