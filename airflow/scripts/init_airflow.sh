set -euo pipefail

mkdir -p /opt/airflow/data

airflow db migrate

airflow users create \
  --username "${AIRFLOW_ADMIN_USER:-airflow}" \
  --password "${AIRFLOW_ADMIN_PASSWORD:-airflow}" \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com || true

airflow connections delete mongo_default >/dev/null 2>&1 || true
airflow connections add mongo_default \
  --conn-type mongo \
  --conn-host mongo \
  --conn-port 27017 \
  --conn-login "${MONGO_ROOT_USER:-root}" \
  --conn-password "${MONGO_ROOT_PASSWORD:-example}" \
  --conn-schema "${MONGO_DB:-reviews_db}" \
  --conn-extra '{"authSource": "admin"}'

airflow connections delete fs_default >/dev/null 2>&1 || true
airflow connections add fs_default \
  --conn-type fs \
  --conn-extra '{"path": "/"}'

echo "Airflow init done."
