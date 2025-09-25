#!/usr/bin/env python3
"""
Women Coding Community - Mentor capacity automation

Runs on every new Google Form response (or manually/cron).
Logic:
  1) Load current month’s responses.
  2) Keep only the FIRST application per mentee (earliest timestamp) for the month.
  3) Count applications per mentor.
  4) Load `_data/mentors.yml`. For any mentor where count >= hours AND current month is in `availability`:
       - remove current month number from `availability`
       - set `sort: 100`
  5) Save file; GitHub Action will open a PR.

Environment variables:
  - GCP_SA_KEY_FILE: path to service account JSON (created by workflow or local)
  - SHEET_ID: Google Sheet ID
  - SHEET_WORKSHEET_TITLE: worksheet/tab (default: "Form Responses 1")
  - MENTORS_YML_PATH: path to mentors.yml (default: "_data/mentors.yml")
  - TIMEZONE: IANA timezone (default: "Europe/London")
  - DRY_RUN: "1" to log only, no writes (default "0")
  - LOCAL_CSV: path to local CSV fixture for offline tests (skips Google)
    (columns required: timestamp, mentee_name, mentor_name, email)
"""

import os, io, sys, logging
from datetime import datetime
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta
from dateutil import parser as dateparser
import pandas as pd
from ruamel.yaml import YAML
from unidecode import unidecode

LOCAL_CSV = os.getenv("LOCAL_CSV")
if not LOCAL_CSV:
    import gspread

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ---------- Config ----------
TZ = os.getenv("TIMEZONE", "Europe/London")
MENTORS_YML_PATH = os.getenv("MENTORS_YML_PATH", "_data/mentors.yml")
SHEET_ID = os.getenv("SHEET_ID")
SHEET_TITLE = os.getenv("SHEET_WORKSHEET_TITLE", "Form Responses 1")
SA_FILE = os.getenv("GCP_SA_KEY_FILE")
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

if not LOCAL_CSV:
    for var in ("SHEET_ID", "GCP_SA_KEY_FILE"):
        if not os.getenv(var):
            logging.error(f"Missing env var: {var}")
            sys.exit(1)

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = unidecode(str(s)).strip().lower()
    return " ".join(s.split())

def normalize_name(s: str) -> str:
    return normalize_text(s)

def parse_timestamp(ts_raw: str, tz: ZoneInfo):
    if not ts_raw:
        return None
    try:
        dt = dateparser.parse(ts_raw)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        else:
            dt = dt.astimezone(tz)
        return dt
    except Exception:
        return None

def current_month_bounds(tz: ZoneInfo):
    now = datetime.now(tz)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = start + relativedelta(months=1)  # exclusive
    return start, end, now.month

def ensure_int(x):
    try:
        return int(str(x).strip())
    except Exception:
        return None

def as_int_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        out = []
        for v in value:
            iv = ensure_int(v)
            if iv is not None:
                out.append(iv)
        return out
    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",")]
        out = []
        for p in parts:
            iv = ensure_int(p)
            if iv is not None:
                out.append(iv)
        return out
    iv = ensure_int(value)
    return [iv] if iv is not None else []

def find_header(header_row, candidates):
    normalized_headers = [normalize_text(h) for h in header_row]
    for i, h in enumerate(normalized_headers):
        if any(c in h for c in candidates):
            return header_row[i]
    return None

# ---------- Load responses into DataFrame ----------
tz = ZoneInfo(TZ)
start, end, current_month_num = current_month_bounds(tz)

if LOCAL_CSV:
    logging.info(f"Loading local CSV fixture: {LOCAL_CSV}")
    df = pd.read_csv(LOCAL_CSV)
    df.columns = [c.strip().lower() for c in df.columns]
    required = {"timestamp", "mentee_name", "mentor_name", "email"}
    if not required.issubset(set(df.columns)):
        logging.error(f"Fixture CSV must contain columns: {sorted(required)}")
        sys.exit(1)
    df = df[["timestamp","mentee_name","mentor_name","email"]]
else:
    logging.info("Authorizing gspread...")
    gc = gspread.service_account(filename=SA_FILE)
    logging.info(f"Opening spreadsheet {SHEET_ID!r} ...")
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(SHEET_TITLE)
    except Exception:
        logging.warning(f"Worksheet {SHEET_TITLE!r} not found, falling back to first sheet.")
        ws = sh.sheet1
    header = ws.row_values(1)
    if not header:
        logging.info("No header row found. Exiting.")
        sys.exit(0)
    col_ts = find_header(header, ["timestamp"])
    col_mentee = find_header(header, ["what is your full name", "mentee name"])
    col_mentor = find_header(header, ["mentor's name", "mentor name"])
    col_email = find_header(header, ["what is your email address", "email"])
    needed = [("Timestamp", col_ts), ("Mentee Name", col_mentee), ("Mentor Name", col_mentor), ("Email", col_email)]
    missing = [n for n, c in needed if c is None]
    if missing:
        logging.error(f"Missing required columns in sheet: {missing}")
        sys.exit(1)
    rows = ws.get_all_records()
    if not rows:
        logging.info("No responses found.")
        sys.exit(0)
    df = pd.DataFrame(rows)[[col_ts, col_mentee, col_mentor, col_email]]
    df.columns = ["timestamp", "mentee_name", "mentor_name", "email"]

# Parse timestamps, filter to current month
df["ts_parsed"] = df["timestamp"].apply(lambda x: parse_timestamp(x, tz))
df = df[df["ts_parsed"].notna()].copy()
df = df[(df["ts_parsed"] >= start) & (df["ts_parsed"] < end)].copy()
if df.empty:
    logging.info("No responses for the current month. Nothing to do.")
    sys.exit(0)

# Dedupe: only first application per mentee in the month
df["email_norm"] = df["email"].apply(normalize_text)
df["mentee_norm"] = df["mentee_name"].apply(normalize_name)
df["dedupe_key"] = df.apply(
    lambda r: r["email_norm"] if r["email_norm"] else f"name::{r['mentee_norm']}", axis=1
)
df = df.sort_values("ts_parsed", ascending=True).drop_duplicates(subset=["dedupe_key"], keep="first")

# Count applications per mentor
df["mentor_norm"] = df["mentor_name"].apply(normalize_name)
counts = df.groupby("mentor_norm").size().to_dict()
if not counts:
    logging.info("After dedupe, there are no valid applications. Nothing to do.")
    sys.exit(0)

logging.info("Counts per mentor (first applications only):")
for k, v in counts.items():
    logging.info(f"  {k!r}: {v}")

# ---------- Load & update mentors.yml ----------
yaml = YAML()
yaml.preserve_quotes = True  # keep original quoting
yaml.width = 4096
yaml.indent(mapping=2, sequence=2, offset=2)

with io.open(MENTORS_YML_PATH, "r", encoding="utf-8") as f:
    data = yaml.load(f)

# Handle top-level list or mapping-with-list
mentors = data
if isinstance(data, dict):
    mentors = data.get("mentors") or data.get("items") or []

def mentor_display_name(item):
    for key in ("name", "full_name", "mentor", "title"):
        if key in item and item[key]:
            return str(item[key])
    first = str(item.get("first_name", "")).strip()
    last = str(item.get("last_name", "")).strip()
    if first or last:
        return f"{first} {last}".strip()
    return ""

mentor_by_norm = {}
for m in mentors:
    nm = normalize_name(mentor_display_name(m))
    if nm:
        mentor_by_norm[nm] = m

modified = False
changed = []

for mentor_norm, applied_count in counts.items():
    if mentor_norm not in mentor_by_norm:
        logging.warning(f"Mentor from sheet not found in mentors.yml: {mentor_norm!r}")
        continue
    mitem = mentor_by_norm[mentor_norm]
    hours = ensure_int(mitem.get("hours"))
    if hours is None:
        logging.warning(f"Mentor {mentor_norm!r} has non-integer 'hours' ({mitem.get('hours')!r}); skipping.")
        continue

    avail_list = as_int_list(mitem.get("availability"))
    is_current_available = current_month_num in avail_list

    if applied_count >= hours and is_current_available:
        logging.info(f"Capacity reached for {mentor_norm!r} (count={applied_count}, hours={hours}); updating YAML.")
        mitem["availability"] = [x for x in avail_list if x != current_month_num]
        mitem["sort"] = 100
        modified = True
        changed.append(mentor_display_name(mitem))
    else:
        logging.info(f"No change for {mentor_norm!r} (count={applied_count}, hours={hours}, month_available={is_current_available})")

if not modified:
    logging.info("No changes required. Exiting.")
    sys.exit(0)

if DRY_RUN:
    logging.info("[DRY_RUN] Would write changes, but DRY_RUN=1; exiting.")
    sys.exit(0)

with io.open(MENTORS_YML_PATH, "w", encoding="utf-8") as f:
    yaml.dump(data, f)

logging.info("Updated mentors.yml. Changed mentors: %s", ", ".join(changed))