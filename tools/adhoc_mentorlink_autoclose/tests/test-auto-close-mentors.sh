#!/usr/bin/env bash
set -euo pipefail

SCRIPT="tools/adhoc_mentorlink_autoclose/auto_close_mentors.py"
FIXDIR="tools/adhoc_mentorlink_autoclose/tests/fixtures"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

cp "$FIXDIR/mentors.input.yml" "$TMPDIR/mentors.yml"

TIMEZONE="Europe/London" \
LOCAL_CSV="$FIXDIR/responses.csv" \
MENTORS_YML_PATH="$TMPDIR/mentors.yml" \
DRY_RUN="0" \
python "$SCRIPT"

if diff -u "$FIXDIR/mentors.expected.yml" "$TMPDIR/mentors.yml"; then
  echo "✅ Test passed"
else
  echo "❌ Test failed"
  exit 1
fi