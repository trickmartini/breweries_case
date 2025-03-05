#!/bin/bash

GRAFANA_URL="grafana:3000"
GRAFANA_USER="admin"
GRAFANA_PASSWORD="admin"


DATA_SOURCE_JSON=$(cat /opt/airflow/grafana/data_sources/airflow_posrtgres.json)


curl -X POST "$GRAFANA_URL/api/datasources" \
     -u "$GRAFANA_USER:$GRAFANA_PASSWORD" \
     -H "Content-Type: application/json" \
     -d "$DATA_SOURCE_JSON"

DASHBOARD_JSON=$(cat /opt/airflow/grafana/dashboards/etl_monitoring.json)

curl -X POST "$GRAFANA_URL/api/dashboards/db" \
     -u "$GRAFANA_USER:$GRAFANA_PASSWORD" \
     -H "Content-Type: application/json" \
     -d "{ \"dashboard\": $DASHBOARD_JSON, \"overwrite\": true }"
