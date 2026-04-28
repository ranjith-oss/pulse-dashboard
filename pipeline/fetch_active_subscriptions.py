#!/usr/bin/env python3
"""
fetch_active_subscriptions.py  —  Pull current active subscriptions from Paddle REST API.
Writes pipeline/active_subscriptions.json (current-month MRR snapshot).

MRR calculation:
  - Fetches all active subscription IDs + plan metadata from Paddle API
  - Joins each subscription against its most recent transaction in the CSV
  - Uses (subtotal - discount) × transaction_to_balance_currency_exchange_rate
    to get the USD amount — the same rate Paddle uses, same as ChartMogul

API: GET /subscriptions?status=active&per_page=200  (paginated)
"""
import os, sys, json, csv, requests
from pathlib import Path

BASE_URL  = 'https://api.paddle.com'
API_KEY   = os.environ.get('PADDLE_API_KEY', '')
OUT_PATH  = Path(__file__).parent / 'active_subscriptions.json'
PLAN_MAP  = {  # same normalisation as compute_mrr.py
    'growth annual':     'Growth Annual',    'growth monthly':   'Growth Monthly',
    'pro+ annual':       'PRO+ Annual',      'pro+ monthly':     'PRO+ Monthly',
    'pro annual':        'PRO Annual',       'pro monthly':      'PRO Monthly',
    'essential annual':  'Essential Annual', 'essential monthly':'Essential Monthly',
    'lite annual':       'Lite Annual',      'lite monthly':     'Lite Monthly',
    'plus annual':       'Plus Annual',      'plus monthly':     'Plus Monthly',
    'starter annual':    'Starter Annual',   'starter monthly':  'Starter Monthly',
    'select':            'Select',
    'team member':       'Add-on: Team Member',
    'storage':           'Add-on: Storage',
    'social sets':       'Add-on: Social Sets',
    'credit':            'Add-on: Credits',
}

if not API_KEY:
    sys.exit('ERROR: PADDLE_API_KEY environment variable not set')

HEADERS = {'Authorization': f'Bearer {API_KEY}'}

def norm_plan(name):
    n = (name or '').lower()
    for key, grp in PLAN_MAP.items():
        if key in n: return grp
    return 'Other'

def get_paginated(path, params=None):
    """Fetch all pages from a Paddle paginated endpoint."""
    items = []
    # Start with the base URL + initial params
    url = BASE_URL + path
    p   = {**(params or {}), 'per_page': 200}

    while True:
        r = requests.get(url, headers=HEADERS, params=p, timeout=30)

        if r.status_code == 403:
            print('', flush=True)
            print('WARNING: Paddle API returned 403 Forbidden.', flush=True)
            print('  The API key is missing "Subscriptions Read" permission.', flush=True)
            print('  To fix: Paddle Dashboard -> Developer Tools -> Authentication', flush=True)
            print('          -> Edit your API key -> enable "Subscriptions: Read"', flush=True)
            print('', flush=True)
            print('Skipping active_subscriptions.json — pipeline will use transaction data for current month.', flush=True)
            sys.exit(0)

        r.raise_for_status()
        data = r.json()
        page = data.get('data', [])
        items.extend(page)

        meta       = data.get('meta', {})
        pagination = meta.get('pagination', {})
        has_more   = pagination.get('has_more', False)
        next_url   = pagination.get('next', '')

        if not has_more or not next_url or not page:
            break

        # Paddle returns a full URL in 'next' — request it directly.
        # Set p={} so requests does NOT append params again (they are already in the URL).
        url = next_url
        p   = {}

    return items

print('[1/3] Fetching active subscriptions from Paddle …', flush=True)
subs = get_paginated('/subscriptions', {'status': 'active'})
print(f'  Found {len(subs):,} active subscriptions', flush=True)

# ── Step 2: Load transaction CSV — use Paddle's own USD exchange rates ────────
CSV_PATH = Path(__file__).parent / 'transaction_line_items.csv'
print('[2/3] Loading transaction CSV for Paddle USD exchange rates …', flush=True)
# Build map: subscription_id → most recent completed transaction row
sub_latest_txn = {}
if CSV_PATH.exists():
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row.get('transaction_status') != 'completed':
                continue
            sid = row.get('subscription_id', '').strip()
            if not sid:
                continue
            billed_at = row.get('transaction_billed_at', '') or row.get('completed_at', '')
            existing  = sub_latest_txn.get(sid)
            if existing is None or billed_at > existing.get('_billed_at', ''):
                row['_billed_at'] = billed_at
                sub_latest_txn[sid] = row
    print(f'  Loaded exchange rates for {len(sub_latest_txn):,} subscriptions from CSV', flush=True)
else:
    print('  WARNING: transaction_line_items.csv not found — MRR will use unit_price (less accurate)', flush=True)

# ── Step 3: Compute MRR ───────────────────────────────────────────────────────
print('[3/3] Computing MRR …', flush=True)
total_mrr   = 0.0
plan_mrr    = {}
sub_details = []
skipped     = 0
csv_matched = 0

for sub in subs:
    sub_id  = sub.get('id', '')
    cust_id = sub.get('customer_id', '')
    currency= sub.get('currency_code', 'USD')

    # Get plan metadata from subscription items
    items = sub.get('items', [])
    for item in items:
        price    = item.get('price', {})
        qty      = item.get('quantity', 1) or 1
        plan_name  = price.get('description', '') or price.get('name', '')
        plan_group = norm_plan(plan_name)

        # Billing cycle
        billing   = price.get('billing_cycle', {})
        interval  = (billing.get('interval', '') or '').lower()
        frequency = int(billing.get('frequency', 1) or 1)
        if interval in ('year', 'yearly', 'annual'):
            cycle_months = 12 * frequency
        elif interval in ('month', 'monthly'):
            cycle_months = frequency
        else:
            cycle_months = 1

        # ── Prefer CSV rate (Paddle's own USD conversion) ──────────────────
        txn = sub_latest_txn.get(sub_id)
        if txn:
            try:
                subtotal  = float(txn.get('subtotal') or 0)
                discount  = float(txn.get('discount') or 0)
                fx_rate   = float(txn.get('transaction_to_balance_currency_exchange_rate') or 1)
                net_usd   = (subtotal - discount) * fx_rate
                mrr       = net_usd / max(cycle_months, 1)
                csv_matched += 1
            except (ValueError, TypeError):
                txn = None  # fall through to unit_price fallback

        if not txn:
            # Fallback: unit_price treated as USD (accurate for USD-only subs)
            unit_price_data = price.get('unit_price', {})
            unit_amount = int(unit_price_data.get('amount', 0))
            unit_usd    = unit_amount / 100.0
            if unit_usd <= 0:
                skipped += 1
                continue
            mrr = (unit_usd * qty) / max(cycle_months, 1)

        if mrr <= 0:
            skipped += 1
            continue

        total_mrr += mrr
        plan_mrr[plan_group] = plan_mrr.get(plan_group, 0) + mrr
        sub_details.append({
            'sub_id':      sub_id,
            'customer_id': cust_id,
            'plan':        plan_group,
            'mrr':         round(mrr, 4),
            'interval':    interval,
            'currency':    currency,
        })

plan_mrr = {k: round(v, 2) for k, v in sorted(plan_mrr.items(), key=lambda x: -x[1])}

output = {
    'total_mrr':   round(total_mrr, 2),
    'active_subs': len(subs),
    'plan_mrr':    plan_mrr,
    'subs':        sub_details,
}

with open(OUT_PATH, 'w') as f:
    json.dump(output, f, indent=2)

print(f'  Total MRR  : ${total_mrr:,.2f}', flush=True)
print(f'  Active subs: {len(subs):,}  |  CSV-matched: {csv_matched:,}  |  skipped: {skipped}', flush=True)
print(f'  Plan breakdown:', flush=True)
for plan, mrr in list(plan_mrr.items())[:10]:
    print(f'    {plan:<35} ${mrr:>9,.2f}', flush=True)
print(f'  Saved → {OUT_PATH}', flush=True)
