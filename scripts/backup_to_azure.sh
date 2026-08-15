#!/usr/bin/env bash

set -Eeuo pipefail
umask 077

BASE="/home/lab/quantLab"
RAW_ROOT="$BASE/data/raw"
BACKUP_DIR="$BASE/backups"
CONTAINER="quantlab-backups"

source "$BASE/.env"

: "${AZURE_STORAGE_CONNECTION_STRING:?AZURE_STORAGE_CONNECTION_STRING is required}"

DAY=$(date -u -d "yesterday" +%F)

SRC="$RAW_ROOT/$DAY"

RAW_BLOB="raw_${DAY}.tar.gz"
PG_BLOB="pg_${DAY}.sql.gz"

RAW_TMP="$BACKUP_DIR/${RAW_BLOB}.part"
PG_TMP="$BACKUP_DIR/${PG_BLOB}.part"

# Keep today + previous 6 calendar days locally.
RETENTION_DAYS=7

# Refuse to create large temporary backups unless enough
# working space remains after retention cleanup.
MIN_FREE_KB=$((6 * 1024 * 1024))

HC="${BACKUP_HEALTHCHECK_URL:-}"
HC="${HC%/}"

mkdir -p "$BACKUP_DIR"

# Prevent overlapping cron/manual backup runs.
exec 9>/tmp/quantlab-azure-backup.lock

if ! flock -n 9; then
    echo "ERROR: another Azure backup is already running"
    exit 1
fi


hc_start() {
    [ -n "$HC" ] || return 0

    curl \
        -m 10 \
        --retry 3 \
        -fsS \
        "$HC/start" \
        >/dev/null \
        || true
}


hc_success() {
    [ -n "$HC" ] || return 0

    curl \
        -m 10 \
        --retry 3 \
        -fsS \
        "$HC" \
        >/dev/null \
        || true
}


hc_fail() {
    [ -n "$HC" ] || return 0

    curl \
        -m 10 \
        --retry 3 \
        -fsS \
        "$HC/fail" \
        >/dev/null \
        || true
}


cleanup() {
    rc=$?

    trap - EXIT

    rm -f \
        "$RAW_TMP" \
        "$PG_TMP"

    if [ "$rc" -ne 0 ]; then
        echo "BACKUP FAILED rc=$rc"
        hc_fail
    fi

    exit "$rc"
}

trap cleanup EXIT


free_kb() {
    df -Pk "$BASE" \
        | awk 'NR == 2 {print $4}'
}


check_free_space() {
    local free

    free=$(free_kb)

    echo "free disk before backup step: ${free} KB"

    if [ "$free" -lt "$MIN_FREE_KB" ]; then
        echo \
          "ERROR: insufficient free disk space; " \
          "need at least ${MIN_FREE_KB} KB"
        return 1
    fi
}


verify_remote_raw() {
    local blob="$1"

    set -o pipefail

    az storage blob download \
        --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
        --container-name "$CONTAINER" \
        --name "$blob" \
        --only-show-errors \
        2>/dev/null \
      | tar -tzf - >/dev/null
}


verify_remote_pg() {
    local blob="$1"

    set -o pipefail

    az storage blob download \
        --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
        --container-name "$CONTAINER" \
        --name "$blob" \
        --only-show-errors \
        2>/dev/null \
      | gzip -t
}


prune_old_raw() {
    local cutoff
    local dir
    local day
    local blob

    cutoff=$(
        date -u \
          -d "${RETENTION_DAYS} days ago" \
          +%F
    )

    echo "local raw retention cutoff: $cutoff"

    for dir in "$RAW_ROOT"/2026-??-??; do
        [ -d "$dir" ] || continue

        day=$(basename "$dir")

        [[ "$day" =~ ^2026-[0-9]{2}-[0-9]{2}$ ]] \
            || continue

        # Remove cutoff day and anything older.
        if [[ "$day" > "$cutoff" ]]; then
            continue
        fi

        blob="raw_${day}.tar.gz"

        echo \
          "retention candidate: $day " \
          "(verifying Azure first)"

        if verify_remote_raw "$blob"; then

            echo \
              "Azure verified: $blob; " \
              "removing local $dir"

            rm -rf -- "$dir"

        else

            echo \
              "WARNING: could not verify $blob; " \
              "keeping local $dir"

        fi
    done
}


echo "========================================"
echo "QUANTLAB AZURE BACKUP"
echo "========================================"
echo "run UTC: $(date -u --iso-8601=seconds)"
echo "backup day: $DAY"

hc_start

# Remove partial files from a previous failed run for this day.
rm -f \
    "$RAW_TMP" \
    "$PG_TMP"

# First reclaim any safely archived old raw data.
prune_old_raw

check_free_space

[ -d "$SRC" ] || {
    echo "ERROR: no raw data directory for $DAY"
    exit 1
}


echo
echo "========== RAW BACKUP =========="

tar -czf "$RAW_TMP" \
    -C "$RAW_ROOT" \
    "$DAY"

echo "testing local raw archive"

tar -tzf "$RAW_TMP" >/dev/null

echo "uploading $RAW_BLOB"

az storage blob upload \
    --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
    --container-name "$CONTAINER" \
    --name "$RAW_BLOB" \
    --file "$RAW_TMP" \
    --overwrite true \
    --only-show-errors

echo "verifying remote raw archive"

verify_remote_raw "$RAW_BLOB"

echo "remote raw verification PASS"

rm -f "$RAW_TMP"


# Check again before creating the much larger PostgreSQL dump.
check_free_space


echo
echo "========== POSTGRES BACKUP =========="

set -o pipefail

docker exec quantlab-pg \
    pg_dump \
      -U quantlab \
      quantlab \
  | gzip -c \
  > "$PG_TMP"

echo "testing local PostgreSQL gzip"

gzip -t "$PG_TMP"

echo "local PostgreSQL gzip PASS"

echo "uploading $PG_BLOB"

az storage blob upload \
    --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
    --container-name "$CONTAINER" \
    --name "$PG_BLOB" \
    --file "$PG_TMP" \
    --overwrite true \
    --only-show-errors

LOCAL_SIZE=$(stat -c '%s' "$PG_TMP")

REMOTE_SIZE=$(
    az storage blob show \
        --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
        --container-name "$CONTAINER" \
        --name "$PG_BLOB" \
        --query 'properties.contentLength' \
        -o tsv \
        --only-show-errors
)

echo "PostgreSQL local bytes : $LOCAL_SIZE"
echo "PostgreSQL remote bytes: $REMOTE_SIZE"

if [ "$LOCAL_SIZE" != "$REMOTE_SIZE" ]; then
    echo "ERROR: PostgreSQL remote size mismatch"
    exit 1
fi

echo "verifying remote PostgreSQL gzip"

verify_remote_pg "$PG_BLOB"

echo "remote PostgreSQL verification PASS"

rm -f "$PG_TMP"


echo
echo "========== FINAL RETENTION =========="

prune_old_raw

echo
echo "========== DISK =========="

df -h /

echo
echo "backed up $DAY OK"

# We reached the end successfully.
trap - EXIT

rm -f \
    "$RAW_TMP" \
    "$PG_TMP"

hc_success

exit 0
