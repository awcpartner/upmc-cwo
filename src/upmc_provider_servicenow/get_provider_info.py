from __future__ import annotations

from typing import Any


def get_provider_info() -> dict[str, Any]:
    """Return metadata consumed by Airflow's provider manager."""
    return {
        "package-name": "upmc-airflow-provider-servicenow",
        "name": "UPMC ServiceNow",
        "description": "ServiceNow incident integration for Apache Airflow.",
        "state": "ready",
        "versions": ["0.1.0"],
        "dependencies": [
            "apache-airflow>=3.2.1,<3.3",
            "apache-airflow-providers-http>=6,<7",
            "apache-airflow-providers-standard>=1.12,<2",
        ],
        "integrations": [
            {
                "integration-name": "ServiceNow",
                "tags": ["service"],
            }
        ],
        "hooks": [
            {
                "integration-name": "ServiceNow",
                "python-modules": ["upmc_provider_servicenow.hooks.servicenow"],
            }
        ],
        "operators": [
            {
                "integration-name": "ServiceNow",
                "python-modules": ["upmc_provider_servicenow.operators.incident"],
            }
        ],
    }
