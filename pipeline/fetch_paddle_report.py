#!/usr/bin/env python3
"""
fetch_paddle_report.py  —  v1

Downloads the complete Paddle Billing transaction line-item dataset using the
Paddle Reports API (/reports).  This replaces the old transactions-list approach
which required 1000+ paginated API pages (~20 min) and had a silently-ignored
billed_at filter.

With the Reports API:
  • Paddle generates the CSV server-side  (~30 s total)
  • No pagination at all
  • Same CSV schema that compute_mrr.py already expects
  • No filter-ignore issues

Flow
─────
  1. POST /reports            → create a "transaction_line_items" report
  2. Poll GET /reports/{id}   → wait for status = "ready"
  3. GET {download_url}       → download the CSV
  4. Normalise column names   → map to the names compute_mrr.py expects
  5. Write transaction_line_items.csv
"""

import csv, io, os, sys, time
import requests
from pathlib import Path

# ── Config ───────────────────────────────────────────────────────────────────
API_KEY      = os.environ['PADDLE_API_KEY']
ENV          = os.environ.get('PADDLE_ENV', 'production')
BASE_URL     = ('https://sandbox-api.paddle.com' if ENV == 'sandbox'
                else 'https://api.paddle.com')
OUTPUT_CSV   = Path(__file__).parent / 'transaction_line_items.csv'

# Fetch everything from 2024-01-01 onwards (pre-launch rows are skipped later
# by compute_mrr.py which only reports from REPORT_START = 2025-01).
FETCH_FROM   = '2024-01-01T00:00:00Z'

POLL_INTERVAL = 5     # seconds between status checks
POLL_TIMEOUT  = 600   # 10 minutes max

HEADERS = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json',
}

# Column name mapping: Paddle Reports CSV name -> what compute_mrr.py expects.
# Only entries that differ from the Paddle native name are listed here.
# (Most column names already match the existing transaction_line_items.csv schema.)
COL_REMAP = {
    # Paddle Reports uses bare names; our CSV uses transaction_-prefixed versions
    'status':      'transaction_status',
    'billed_at':   'transaction_billed_at',
    'created_at':  'transaction_created_at',
    # add more here if needed after first live run
}

# ── Step 1: Create report ────────────────────────────────────────────────────
print('[1/3] Creating Paddle transaction_line_items report …', flush=True)

payload = {
    'type': 'transaction_line_items',
    'filters': [
        {'name': 'updated_at', 'operator': 'gte', 'value': FETCH_FROM}
    ],
}

r = requests.post(f'{BASE_URL}/reports', json=payload, headers=HEADERS, timeout=30)
if not r.ok:
    print(f'  ERROR {r.status_code}: {r.text}', file=sys.stderr)
    sys.exit(1)

report   = r.json()['data']
rep_id   = report['id']
print(f'  Report ID : {rep_id}', flush=True)
print(f'  Status    : {report["status"]}', flush=True)

# ── Step 2: Poll until ready ─────────────────────────────────────────────────
print('[2/3] Waiting for report to be ready …', flush=True)
elapsed = 0
download_url = None

while elapsed < POLL_TIMEOUT:
    time.sleep(POLL_INTERVAL)
    elapsed += POLL_INTERVAL

    r = requests.get(f'{BASE_URL}/reports/{rep_id}', headers=HEADERS, timeout=30)
    r.raise_for_status()
    data   = r.json()['data']
    status = data['status']
    print(f'  {elapsed:>4}s  status={status}', flush=True)

    if status == 'ready':
        # Paddle uses either "url" or "download_url" depending on API version
        download_url = data.get('download_url') or data.get('url')
        break
    if status in ('failed', 'invalid'):
        print(f'  Report generation failed: {data}', file=sys.stderr)
        sys.exit(1)
else:
    print('  Timed out waiting for report.', file=sys.stderr)
    sys.exit(1)

# ── Step 3: Download & normalise CSV ─────────────────────────────────────────
print(f'[3/3] Downloading CSV …', flush=True)

dl = requests.get(download_url, timeout=120)
dl.raise_for_status()

raw = dl.content.decode('utf-8-sig')   # strip UTF-8 BOM if present
reader = csv.DictReader(io.StringIO(raw))
orig_fields = list(reader.fieldnames or [])

# Apply column remapping
new_fields = [COL_REMAP.get(f, f) for f in orig_fields]
rows = []
skipped_pre_launch = 0

for row in reader:
    new_row = {COL_REMAP.get(k, k): v for k, v in row.items()}

    # Safety filter: skip rows billed before Paddle go-live (2024-12-01)
    billed = new_row.get('transaction_billed_at', '') or ''
    if billed and billed[:10] < '2024-12-01':
        skipped_pre_launch += 1
        continue

    rows.append(new_row)

print(f'  Total line items   : {len(rows):,}', flush=True)
print(f'  Pre-launch skipped : {skipped_pre_launch:,}', flush=True)
print(f'  Original columns   : {orig_fields}', flush=True)
if set(orig_fields) != set(new_fields):
    print(f'  Remapped columns   : {[c for c in new_fields if c not in orig_fields]}',
          flush=True)

with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=new_fields)
    writer.writeheader()
    writer.writerows(rows)

print(f'  Saved to: {OUTPUT_CSV.name}', flush=True)
print('Done.', flush=True)
