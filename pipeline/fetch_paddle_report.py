#!/usr/bin/env python3
"""
fetch_paddle_report.py  —  Pull transaction_line_items report from Paddle Reports API.
Writes pipeline/transaction_line_items.csv (excluded from git via .gitignore).

API flow:
  1. POST /reports               → create report
  2. GET  /reports/{id}          → poll until status=ready
  3. GET  /reports/{id}/download-url → get signed S3 URL
  4. GET  {signed_url}           → download CSV
"""
import os, sys, time, requests
from pathlib import Path

BASE_URL = 'https://api.paddle.com'
API_KEY  = os.environ.get('PADDLE_API_KEY', '')
OUT_PATH = Path(__file__).parent / 'transaction_line_items.csv'

if not API_KEY:
    sys.exit('ERROR: PADDLE_API_KEY environment variable not set')

HEADERS = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json',
}

def api(method, path, **kw):
    r = requests.request(method, BASE_URL + path, headers=HEADERS, timeout=60, **kw)
    try:
        return r.json()
    except Exception:
        print(f'  Non-JSON response ({r.status_code}): {r.text[:200]}')
        return {}

# ── Step 1: Create report ────────────────────────────────────────────────
print('[1/3] Creating Paddle transaction_line_items report …', flush=True)
body = {'type': 'transaction_line_items', 'filters': []}
resp = api('POST', '/reports', json=body)
data = resp.get('data', resp)
rep_id = data.get('id')
if not rep_id:
    sys.exit(f'ERROR: could not create report. Response: {resp}')
print(f'  Report ID: {rep_id}', flush=True)

# ── Step 2: Poll until ready ──────────────────────────────────────────────
print('[2/3] Polling for completion …', flush=True)
elapsed = 0
while True:
    time.sleep(10); elapsed += 10
    resp = api('GET', f'/reports/{rep_id}')
    data = resp.get('data', resp)
    status = data.get('status', 'unknown')
    rows   = data.get('rows', '?')
    print(f'  [{elapsed}s] status={status} rows={rows}', flush=True)
    if status == 'ready':
        break
    if status in ('failed', 'error'):
        sys.exit(f'ERROR: report failed. Response: {resp}')
    if elapsed >= 1800:
        sys.exit('ERROR: timed out after 30 minutes waiting for report')

# ── Step 3: Get download URL ──────────────────────────────────────────────
print('[3/3] Fetching download URL …', flush=True)
dl_resp = api('GET', f'/reports/{rep_id}/download-url')
dl_data = dl_resp.get('data', dl_resp)
url     = dl_data.get('url') or dl_data.get('download_url')
if not url:
    sys.exit(f'ERROR: no download URL in response: {dl_resp}')

print(f'  Downloading CSV …', flush=True)
with requests.get(url, stream=True, timeout=300) as r:
    r.raise_for_status()
    with open(OUT_PATH, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024*1024):
            f.write(chunk)

size_mb = OUT_PATH.stat().st_size / 1024 / 1024
print(f'  Saved → {OUT_PATH}  ({size_mb:.1f} MB)', flush=True)
