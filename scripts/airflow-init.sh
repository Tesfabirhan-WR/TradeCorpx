#!/usr/bin/env bash
set -euo pipefail

airflow db migrate

if airflow users list --output json |
  python -c 'import json, sys; sys.exit(not any(user.get("username") == "admin" for user in json.load(sys.stdin)))'
then
  echo "Airflow admin user already exists."
else
  airflow users create \
    --username admin \
    --password admin \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com
fi
