#!/bin/bash
# Creates one database AND one database user per service-owned schema.
# Each user can only touch its own database, so a service physically cannot
# modify another service's tables (the "service boundary" requirement).
#
# Runs automatically on first start of the MySQL container
# (/docker-entrypoint-initdb.d) and can be run by hand against any MySQL 8 server.
set -euo pipefail

declare -A PASSWORDS=(
  [upload]="${UPLOAD_DB_PASSWORD:-upload_pw}"
  [metadata]="${METADATA_DB_PASSWORD:-metadata_pw}"
  [orchestrator]="${ORCHESTRATOR_DB_PASSWORD:-orchestrator_pw}"
  [transcoder]="${TRANSCODER_DB_PASSWORD:-transcoder_pw}"
  [thumbnail]="${THUMBNAIL_DB_PASSWORD:-thumbnail_pw}"
)

SQL=""
for svc in "${!PASSWORDS[@]}"; do
  SQL+="CREATE DATABASE IF NOT EXISTS vp_${svc} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'vp_${svc}'@'%' IDENTIFIED BY '${PASSWORDS[$svc]}';
ALTER USER 'vp_${svc}'@'%' IDENTIFIED BY '${PASSWORDS[$svc]}';
GRANT ALL PRIVILEGES ON vp_${svc}.* TO 'vp_${svc}'@'%';
"
done
SQL+="FLUSH PRIVILEGES;"

if [[ -n "${MYSQL_ROOT_PASSWORD:-}" ]]; then
  mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" -e "${SQL}"
else
  mysql -uroot -e "${SQL}"
fi
echo "service databases and users created"
