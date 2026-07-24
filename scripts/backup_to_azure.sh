#!/bin/bash
set -euo pipefail
source /home/lab/quantLab/.env

DAY=$(date -u -d "yesterday" +%F)
SRC="/home/lab/quantLab/data/raw/$DAY"
OUT="/home/lab/quantLab/backups/raw_$DAY.tar.gz"

[ -d "$SRC" ] || { echo "no data dir for $DAY"; exit 1; }

tar -czf "$OUT" -C /home/lab/quantLab/data/raw "$DAY"

az storage blob upload \
  --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
  --container-name quantlab-backups \
  --name "raw_$DAY.tar.gz" \
  --file "$OUT" \
  --overwrite

rm "$OUT"

# --- postgres dump ---------------------------------------------------------
PGOUT="/home/lab/quantLab/backups/pg_$DAY.sql.gz"
docker exec quantlab-pg pg_dump -U quantlab quantlab | gzip > "$PGOUT"

az storage blob upload \
  --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
  --container-name quantlab-backups \
  --name "pg_$DAY.sql.gz" \
  --file "$PGOUT" \
  --overwrite

rm "$PGOUT"
# ---------------------------------------------------------------------------

echo "backed up $DAY OK"

[ -n "${BACKUP_HEALTHCHECK_URL:-}" ] && curl -fsS "$BACKUP_HEALTHCHECK_URL" > /dev/null || true
