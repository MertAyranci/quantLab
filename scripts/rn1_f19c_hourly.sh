#!/usr/bin/env bash

set -Eeuo pipefail

cd /home/lab/quantLab

exec 9>/tmp/rn1_f19c_operate.lock

if ! flock -n 9; then
    echo "$(date -u --iso-8601=seconds) F19c already running; skip"
    exit 0
fi

exec \
  /home/lab/quantLab/.venv/bin/python \
  /home/lab/quantLab/collectors/polymarket/rn1_f19c_operate.py
