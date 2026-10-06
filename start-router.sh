#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
umask 077
cd -- "$script_dir"

nohup python3 -u router.py --database "$script_dir/router.sqlite" "$@" >> "$script_dir/router.log" 2>&1 < /dev/null &
pid=$!
sleep 0.3
if ! kill -0 "$pid" 2>/dev/null; then
    printf 'Router konnte nicht starten; siehe %s/router.log\n' "$script_dir" >&2
    exit 1
fi
printf 'Router läuft im Hintergrund (PID %s). Log: %s/router.log\n' "$pid" "$script_dir"
