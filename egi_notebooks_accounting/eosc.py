"""EOSC EU Node Accounting implementation

EOSC EU Node expects aggregated accounting information for the number of hours
a given flavor of jupyter server has been running over the last day, following
this definition:

{
    "metric_name": "small-environment-2-vcpu-4-gb-ram",
    "metric_description": "total runtime per day (in hours)",
    "metric_type": "aggregated",
    "unit_type": "Hours/day"
}

The report is done by sending records with a POST API call to:
/accounting-system/installations/{installation_id}/metrics

with a JSON like:
{
  "metric_definition_id": "<metric id (depends on the flavor)>”,
  "time_period_start": "2023-01-05T09:13:07Z",
  "time_period_end": "2024-01-05T09:13:07Z",
  "value": 10.2,
  "group_id": "group id", # personal or group project
  "user_id": "user id" # user aai
}

This code goes to the accounting db and aggregates the information for the last 24 hours
and pushes it to the EOSC Accounting

Configuration:
[prometheus]
notebooks_db=<notebooks db file>

[eosc]
token_url=https://aai-demo.eosc-portal.eu/auth/realms/core/protocol/openid-connect/token
accounting_url=https://accounting.devel.argo.grnet.gr
refresh_token=
client_secret=
client_id=
installaion_id=<id of the installation to report accounting for>

[eosc.flavors]
# contains a list of flavors and metrics they are mapped to
<name of the flavor>=<metric id>
# example:
small-environment-2-vcpu-4-gb-ram=668bdd5988e1d617b217ecb9
"""

import argparse
import json
import logging
import os
import time
from configparser import ConfigParser
from datetime import datetime, timedelta

import requests
from requests.auth import HTTPBasicAuth

from .model import db_init, VM

PROM_CONFIG = "prometheus"
EOSC_CONFIG = "eosc"
FLAVOR_CONFIG = "eosc.flavors"
DEFAULT_CONFIG_FILE = "config.ini"
DEFAULT_TOKEN_URL = (
    "https://aai-demo.eosc-portal.eu/auth/realms/core/protocol/openid-connect/token"
)
DEFAULT_ACCOUNTING_URL = "https://accounting.devel.argo.grnet.gr"


def get_access_token(token_url, client_id, client_secret):
    response = requests.post(
        token_url,
        auth=HTTPBasicAuth(client_id, client_secret),
        data={
            "grant_type": "client_credentials",
            "scope": "openid email profile voperson_id entitlements",
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    return response.json()["access_token"]


def push_metric(
    accounting_url,
    token,
    installation,
    metric,
    period_start,
    period_end,
    user,
    group,
    value,
):
    data = {
        "metric_definition_id": metric,
        "time_period_start": period_start,
        "time_period_end": period_end,
        "user": user,
        "group": group,
        "value": value,
    }
    response = requests.post(
        f"{accounting_url}/accounting-system/installations/{installation}/metrics",
        headers={"Authorization": f"Bearer {token}"},
        data=json.dumps(data),
    )
    response.raise_for_status()


def update_pod_metric(pod, metrics, flavor_config):
    if not pod.flavor or pod.flavor not in flavor_config:
        # cannot report
        logging.debug(f"Flavor {pod.flavor} does not have a configured metric")
        return
    user, group = (pod.global_user_name, pod.fqan)
    user_metrics = metrics.get((user, group), {})
    flavor_metric = flavor_config[pod.flavor]
    flavor_metric_value = user_metrics.get(flavor_metric, 0)
    user_metrics[flavor_metric] = flavor_metric_value + pod.wall
    metrics[(user, group)] = user_metrics


def main():
    parser = argparse.ArgumentParser(description="EOSC Accounting metric pusher")
    parser.add_argument(
        "-c", "--config", help="config file", default=DEFAULT_CONFIG_FILE
    )
    args = parser.parse_args()

    parser = ConfigParser()
    parser.read(args.config)
    prom_config = parser[PROM_CONFIG] if PROM_CONFIG in parser else {}
    eosc_config = parser[EOSC_CONFIG] if EOSC_CONFIG in parser else {}
    flavor_config = parser[FLAVOR_CONFIG] if FLAVOR_CONFIG in parser else {}
    db_file = os.environ.get("NOTEBOOKS_DB", prom_config.get("notebooks_db", None))
    db_init(db_file)

    verbose = os.environ.get("VERBOSE", prom_config.get("verbose", 0))
    verbose = logging.DEBUG if verbose == "1" else logging.INFO
    logging.basicConfig(level=verbose)

    # EOSC accounting config
    # AAI
    token_url = os.environ.get(
        "TOKEN_URL", eosc_config.get("token_url", DEFAULT_TOKEN_URL)
    )
    refresh_token = os.environ.get(
        "REFRESH_TOKEN", eosc_config.get("refresh_token", "")
    )
    client_id = os.environ.get("CLIENT_ID", eosc_config.get("client_id", ""))
    client_secret = os.environ.get(
        "CLIENT_SECRET", eosc_config.get("client_secret", "")
    )
    accounting_url = os.environ.get(
        "ACCOUNTING_URL", eosc_config.get("accounting_url", DEFAULT_ACCOUNTING_URL)
    )
    installation = eosc_config.get("installation_id", "")

    # ==== queries ====
    to_date = datetime.now()
    from_date = to_date - timedelta(days=1)
    metrics = {}
    # pods ending in between the reporting times
    for pod in VM.select().where((VM.end_time >= from_date) | (VM.end_time <= to_date)):
        update_pod_metric(pod, metrics, flavor_config)
    # pods starting but not finished between the reporting times
    for pod in VM.select().where((VM.start_time >= from_date) | (VM.end_time == None)):
        update_pod_metric(pod, metrics, flavor_config)

    # ==== push this to EOSC accounting ====
    token = get_access_token(token_url, client_id, client_secret)
    period_start = (from_date.strftime("%Y-%m-%dT%H:%M:%SZ"),)
    period_end = (to_date.strftime("%Y-%m-%dT%H:%M:%SZ"),)
    for (user, group), flavors in metrics.items():
        for metric_key, value in flavors.items():
            print(metric_key, value)
            push_metric(
                accounting_url,
                token,
                installation,
                metric_key,
                period_start,
                period_end,
                user,
                group,
                value,
            )


if __name__ == "__main__":
    main()
