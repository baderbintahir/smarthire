#!/bin/bash
# Runs ONCE, the first time the Postgres volume is empty.
# Creates one schema + one login user per service; each user can only use its own schema.
# To re-run after changing this file: docker compose down -v
set -euo pipefail

SERVICES=(identity job candidate application notification analytics)

for svc in "${SERVICES[@]}"; do
  role="${svc}_user"
  pw_var="$(echo "$svc" | tr '[:lower:]' '[:upper:]')_DB_PASSWORD"
  pw="${!pw_var:?$pw_var is not set}"   # indirect lookup of e.g. $IDENTITY_DB_PASSWORD

  # :"x" quotes an identifier, :'x' quotes a string literal (psql variables, no manual escaping)
  psql -v ON_ERROR_STOP=1 -v svc="$svc" -v role="$role" -v pw="$pw" \
       --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE ROLE :"role" LOGIN PASSWORD :'pw';
CREATE SCHEMA :"svc" AUTHORIZATION :"role";
-- Unqualified table names resolve to the service's own schema (avoids tables landing in public)
ALTER ROLE :"role" SET search_path = :"svc";
SQL
done

# Nobody gets to use or create objects in the shared public schema
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -c "REVOKE ALL ON SCHEMA public FROM PUBLIC;"
