#!/usr/bin/env python3
"""
adhoc_mentorlink_autoclose: close a mentor's ad-hoc link when monthly capacity is reached.

Process (each run):
  1) Load current month responses (Google Sheet or LOCAL_CSV fixture).
  2) Keep only FIRST application per mentee (earliest timestamp) for the month.
  3) Count applications per mentor (by normalized name).
  4) For any mentor with count >= hours AND current month present in `availability`:
       - remove the current month from availability
       - set `sort: 100`
     Write those changes back to mentors.yml using SURGICAL, TEXT-ONLY PATCHES
     to preserve all other formatting and indentation exactly.

ENV:
  - GCP_SA_KEY_FILE: path to SA JSON (required unless LOCAL_CSV set)
  - SHEET_ID: Google Sheet ID (required unless LOCAL_CSV set)
  - SHEET_WORKSHEET_TITLE: sheet tab (default: "Form Responses 1")
  - LOCAL_CSV: path to fixture CSV (columns: timestamp, mentee_name, mentor_name, email)
  - MENTORS_YML_PATH: path to mentors.yml (default: "_data/mentors.yml")
  - TIMEZONE: IANA TZ (default: "Europe/London")
  - DRY_RUN: "1" = log only, no write
"""

import io
import os
import re
import sys
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta
from dateutil import parser as dateparser
import pandas as pd
from unidecode import unidecode
from ruamel.yaml import YAML  # only used for READ (to find mentors safely)

LOCAL_CSV = os.getenv("LOCAL_CSV")
if not LOCAL_CSV:
    import gspread  # only needed for live Sheet access

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

# ---------- Helpers ----------
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
    """Return the original header text from header_row that contains any of `candidates` (normalized, substring)."""
    normalized_headers = [normalize_text(h) for h in header_row]
    for i, h in enumerate(normalized_headers):
        if any(c in h for c in candidates):
            return header_row[i]
    return None

# ---------- Load responses ----------
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
    df = df[["timestamp", "mentee_name", "mentor_name", "email"]]
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
    col_ts     = find_header(header, ["timestamp"])
    col_mentee = find_header(header, ["what is your full name", "mentee name"])
    col_mentor = find_header(header, ["mentor's name", "mentor name"])
    col_email  = find_header(header, ["what is your email address", "email"])
    needed = [("Timestamp", col_ts), ("Mentee Name", col_mentee), ("Mentor Name", col_mentor), ("Email", col_email)]
    missing = [n for n, c in needed if c is None]
    if missing:
        logging.error(f"Missing required columns in sheet: {missing}")
        sys.exit(1)
    rows = ws.get_all_records()
    if not rows:
        logging.info("No responses found.")
        sys.exit(0)
    # Keep only the needed columns; rename to normalized names
    df = pd.DataFrame(rows)[[col_ts, col_mentee, col_mentor, col_email]]
    df.columns = ["timestamp", "mentee_name", "mentor_name", "email"]

# timestamps & month filter
df["ts_parsed"] = df["timestamp"].apply(lambda x: parse_timestamp(x, tz))
df = df[df["ts_parsed"].notna()].copy()
df = df[(df["ts_parsed"] >= start) & (df["ts_parsed"] < end)].copy()
if df.empty:
    logging.info("No responses for the current month. Nothing to do.")
    sys.exit(0)

# first application per mentee (by earliest)
df["email_norm"]  = df["email"].apply(normalize_text)
df["mentee_norm"] = df["mentee_name"].apply(normalize_name)
df["dedupe_key"]  = df.apply(lambda r: r["email_norm"] if r["email_norm"] else f"name::{r['mentee_norm']}", axis=1)
df = df.sort_values("ts_parsed", ascending=True).drop_duplicates(subset=["dedupe_key"], keep="first")

# counts per mentor
df["mentor_norm"] = df["mentor_name"].apply(normalize_name)
counts = df.groupby("mentor_norm").size().to_dict()
if not counts:
    logging.info("After dedupe, there are no valid applications. Nothing to do.")
    sys.exit(0)

logging.info("Counts per mentor (first apps only):")
for k, v in counts.items():
    logging.info(f"  {k!r}: {v}")

# ---------- Text-patch utilities (preserve formatting) ----------
def line_ending(s: str) -> str:
    if s.endswith("\r\n"):
        return "\r\n"
    if s.endswith("\r"):
        return "\r"
    return "\n"

def build_mentor_blocks(lines):
    """
    Return {normalized_name: (start_idx, end_idx, indent_str, original_name)} for each mentor block.
    A mentor block starts at a line like:    <indent>- name: Some Name
    and ends right before the next "- name:" or EOF.
    """
    pat = re.compile(r'^(\s*)-\s*name:\s*(.+?)\s*$', re.UNICODE)
    starts = []
    for i, line in enumerate(lines):
        m = pat.match(line)
        if m:
            starts.append((i, m.group(1), m.group(2).strip()))
    blocks = {}
    for idx, (start, indent, name) in enumerate(starts):
        end = starts[idx + 1][0] if idx + 1 < len(starts) else len(lines)
        blocks[normalize_name(name)] = (start, end, indent, name)
    return blocks

def patch_availability_and_sort_in_block(lines, start, end, current_month):
    """
    Within lines[start:end], update:
      - availability: remove current_month (handles inline [..] or block list below)
      - sort: set to 100 (insert if missing)
    Returns True if any change was made.
    """
    changed = False

    # availability header
    avail_hdr_re = re.compile(r'^(\s*)availability:\s*(.*?)(\s*(#.*))?$', re.IGNORECASE)
    avail_hdr_idx = None
    avail_hdr_indent = ""
    avail_rest = ""
    avail_comment = ""
    avail_hdr_nl = "\n"

    for i in range(start, end):
        m = avail_hdr_re.match(lines[i])
        if m:
            avail_hdr_idx = i
            avail_hdr_indent = m.group(1) or ""
            avail_rest = (m.group(2) or "").strip()
            avail_comment = (m.group(3) or "")
            avail_hdr_nl = line_ending(lines[i])
            break

    if avail_hdr_idx is not None:
        # case A: inline flow style, e.g. "availability: [9, 10]"
        if "[" in avail_rest and "]" in avail_rest:
            nums = [int(x) for x in re.findall(r'\d+', avail_rest)]
            new_nums = [n for n in nums if n != current_month]
            new_rest = "[]"
            if new_nums:
                new_rest = "[" + ", ".join(str(n) for n in new_nums) + "]"
            new_line = f"{avail_hdr_indent}availability: {new_rest}{avail_comment}{avail_hdr_nl}"
            if lines[avail_hdr_idx] != new_line:
                lines[avail_hdr_idx] = new_line
                changed = True
        else:
            # case B: block style list after header
            item_indent = avail_hdr_indent + "  "
            j = avail_hdr_idx + 1
            items_idx = []
            item_nl = "\n"
            while j < end:
                m = re.match(rf'^({re.escape(item_indent)})-\s*(\d+)\s*([#].*)?$', lines[j])
                if not m:
                    break
                items_idx.append(j)
                item_nl = line_ending(lines[j])
                j += 1
            if items_idx:
                nums = []
                for k in items_idx:
                    m = re.match(rf'^{re.escape(item_indent)}-\s*(\d+)\s*([#].*)?$', lines[k])
                    if m:
                        nums.append(int(m.group(1)))
                new_nums = [n for n in nums if n != current_month]

                if len(new_nums) != len(nums):
                    changed = True
                    if not new_nums:
                        # collapse to inline empty list
                        lines[avail_hdr_idx] = f"{avail_hdr_indent}availability: []{avail_comment}{avail_hdr_nl}"
                        # remove the old list lines
                        for k in reversed(items_idx):
                            del lines[k]
                            end -= 1
                    else:
                        # remove old items
                        for k in reversed(items_idx):
                            del lines[k]
                            end -= 1
                        # insert new items
                        insert_at = avail_hdr_idx + 1
                        for n in new_nums:
                            lines.insert(insert_at, f"{item_indent}- {n}{item_nl}")
                            insert_at += 1

    # sort: set to 100 (insert if missing)
    sort_re = re.compile(r'^(\s*)sort:\s*(\d+)\s*(#.*)?$')
    sort_idx = None
    sort_indent = None
    sort_nl = "\n"
    for i in range(start, end):
        m = sort_re.match(lines[i])
        if m:
            sort_idx = i
            sort_indent = m.group(1) or ""
            sort_nl = line_ending(lines[i])
            current = int(m.group(2))
            if current != 100:
                lines[i] = f"{sort_indent}sort: 100{sort_nl}"
                changed = True
            break

    if sort_idx is None:
        # insert sort after availability (if present), else at end of block
        target_nl = avail_hdr_nl if avail_hdr_idx is not None else "\n"
        insert_indent = (avail_hdr_indent if avail_hdr_idx is not None else "  ")
        insert_at = end
        if avail_hdr_idx is not None:
            insert_at = avail_hdr_idx + 1
            # skip any availability item lines to place sort after them
            while insert_at < len(lines) and re.match(r'^\s*-\s*\d+\s*$', lines[insert_at].strip()):
                insert_at += 1
        lines.insert(insert_at, f"{insert_indent}sort: 100{target_nl}")
        changed = True

    return changed

def apply_text_patches(path, names_to_update, current_month):
    """
    names_to_update: iterable of *display* names (exact as in file, any case/accents).
    Returns list of names actually changed.
    """
    # Read preserving original per-line newline endings
    with io.open(path, "r", encoding="utf-8") as f:
        text = f.read()
    lines = text.splitlines(True)  # keep line endings

    blocks = build_mentor_blocks(lines)
    changed_names = []

    for display_name in names_to_update:
        key = normalize_name(display_name)
        if key not in blocks:
            logging.warning(f"Mentor not found in YAML (text mode): {display_name!r}")
            continue
        start, end, indent, orig_name = blocks[key]
        if patch_availability_and_sort_in_block(lines, start, end, current_month):
            changed_names.append(orig_name)

    if changed_names and not DRY_RUN:
        with io.open(path, "w", encoding="utf-8") as f:
            f.write("".join(lines))

    return changed_names

# ---------- Decide who to update (YAML read only) ----------
yaml = YAML()
with io.open(MENTORS_YML_PATH, "r", encoding="utf-8") as f:
    data = yaml.load(f)

mentors = data if isinstance(data, list) else (data.get("mentors") or data.get("items") or [])
def mentor_display_name(item):
    for key in ("name", "full_name", "mentor", "title"):
        if key in item and item[key]:
            return str(item[key])
    first = str(item.get("first_name", "")).strip()
    last = str(item.get("last_name", "")).strip()
    if first or last:
        return f"{first} {last}".strip()
    return ""

# Map normalized -> display name as it appears in file
display_by_norm = {}
for m in mentors:
    nm = normalize_name(mentor_display_name(m))
    if nm:
        display_by_norm[nm] = mentor_display_name(m)

to_update = []
for mentor_norm, applied_count in counts.items():
    disp = display_by_norm.get(mentor_norm)
    if not disp:
        logging.warning(f"Mentor from sheet not found in mentors.yml: {mentor_norm!r}")
        continue
    # find that mentor item to read hours & availability
    mitem = next((m for m in mentors if normalize_name(mentor_display_name(m)) == mentor_norm), None)
    if mitem is None:
        continue
    hours = ensure_int(mitem.get("hours"))
    if hours is None:
        logging.warning(f"Mentor {disp!r} has non-integer 'hours' ({mitem.get('hours')!r}); skipping.")
        continue
    avail_list = as_int_list(mitem.get("availability"))
    is_current_available = current_month_num in avail_list
    if applied_count >= hours and is_current_available:
        to_update.append(disp)
        logging.info(f"Capacity reached for {disp!r} (count={applied_count}, hours={hours}); will patch.")

if not to_update:
    logging.info("No changes required. Exiting.")
    sys.exit(0)

if DRY_RUN:
    logging.info("[DRY_RUN] Would patch mentors.yml for: %s", ", ".join(to_update))
    sys.exit(0)

changed_names = apply_text_patches(MENTORS_YML_PATH, to_update, current_month_num)
if changed_names:
    logging.info("Patched mentors.yml (text mode). Changed mentors: %s", ", ".join(changed_names))
else:
    logging.info("No text changes were applied (unexpected).")