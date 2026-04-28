#!/usr/bin/env python3
"""
fetch_active_subscriptions.py  —  Pull current active subscriptions from Paddle REST API.
Writes pipeline/active_subscriptions.json (current-month MRR snapshot).

This is the authoritative source for CURRENT MONTH MRR.
It mirrors what ChartMogul does: subscription state × recurring price.

API: GET /subscriptions?status=active&per_page=200  (paginated)
"""
import os, sys, json, requests
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
    cursor = None
    while True:
        p = {**(params or {}), 'per_page': 200}
        if cursor: p['after'] = cursor
        r = requests.get(BASE_URL + path, headers=HEADERS, params=p, timeout=30)
        r.raise_for_status()
        data = r.json()
        page = data.get('data', [])
        items.extend(page)
        meta = data.get('meta', {})
        pagination = meta.get('pagination', {})
        cursor = pagination.get('next') or pagination.get('cursor_after')
        if not cursor or not page:
            break
    return items

print('[1/2] Fetching active subscriptions from Paddle …', flush=True)
subs = get_paginated('/subscriptions', {'status': 'active'})
print(f'  Found {len(subs):,} active subscriptions', flush=True)

print('[2/2] Computing MRR from subscription prices …', flush=True)
total_mrr   = 0.0
plan_mrr    = {}
sub_details = []
skipped     = 0

for sub in subs:
    sub_id  = sub.get('id', '')
    cust_id = sub.get('customer_id', '')
    status  = sub.get('status', '')
    currency= sub.get('currency_code', 'USD')

    # Items on the subscription
    items = sub.get('items', [])
    for item in items:
        price = item.get('price', {})
        qty   = item.get('quantity', 1) or 1

        # Unit price in smallest currency unit (cents)
        unit_price_data = price.get('unit_price', {})
        unit_amount = int(unit_price_data.get('amount', 0))  # in cents
        unit_usd = unit_amount / 100.0

        if unit_usd <= 0:
            skipped += 1
            continue

        # Billing cycle
        billing = price.get('billing_cycle', {})
        interval = (billing.get('interval', '') or '').lower()
        frequency = int(billing.get('frequency', 1) or 1)

        if interval in ('year', 'yearly', 'annual'):
            cycle_months = 12 * frequency
        elif interval in ('month', 'monthly'):
            cycle_months = frequency
        else:
            cycle_months = 1

        # Exchange rate: use scheduled_change or just assume USD for now
        # TODO: handle multi-currency subs via exchange rate
        mrr = (unit_usd * qty) / cycle_months

        plan_name = price.get('description', '') or price.get('name', '')
        plan_group = norm_plan(plan_name)

        total_mrr += mrr
        plan_mrr[plan_group] = plan_mrr.get(plan_group, 0) + mrr

        sub_details.append({
            'sub_id':     sub_id,
            'customer_id': cust_id,
            'plan':       plan_group,
            'mrr':        round(mrr, 4),
            'interval':   interval,
            'currency':   currency,
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

print(f'  Total MRR : ${total_mrr:,.2f}', flush=True)
print(f'  Active subs: {len(subs):,}  (skipped {skipped} zero-price items)', flush=True)
print(f'  Plan breakdown:', flush=True)
for plan, mrr in list(plan_mrr.items())[:10]:
    print(f'    {plan:<35} ${mrr:>9,.2f}', flush=True)
print(f'  Saved → {OUT_PATH}', flush=True)
