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

# ── FX rates: fetch live USD-base rates, fall back to hardcoded ──────────────
def get_fx_rates():
    try:
        r = requests.get('https://open.er-api.com/v6/latest/USD', timeout=10)
        data = r.json()
        if data.get('result') == 'success':
            return data['rates']
    except Exception:
        pass
    # Fallback hardcoded rates (approximate, update periodically)
    return {
        'USD': 1.0,   'INR': 0.01195, 'EUR': 1.08,  'GBP': 1.27,
        'CAD': 0.73,  'AUD': 0.63,    'BRL': 0.176,  'MXN': 0.049,
        'SGD': 0.74,  'AED': 0.272,   'IDR': 0.000062,'JPY': 0.0067,
        'MYR': 0.225, 'PHP': 0.0175,  'THB': 0.028,  'VND': 0.000040,
        'NGN': 0.00063,'KES': 0.0077, 'ZAR': 0.054,  'TRY': 0.028,
        'SAR': 0.267, 'QAR': 0.275,   'KWD': 3.26,   'BDT': 0.0091,
        'PKR': 0.0036,'LKR': 0.0034,  'NZD': 0.58,   'CHF': 1.12,
        'SEK': 0.094, 'NOK': 0.093,   'DKK': 0.145,  'PLN': 0.25,
        'CZK': 0.044, 'HUF': 0.0028,  'RON': 0.22,   'CLP': 0.00105,
        'COP': 0.000245,'PEN': 0.266, 'ARS': 0.00099,'TWD': 0.031,
        'KRW': 0.00071,'HKD': 0.128,
    }

FX = get_fx_rates()

def to_usd(amount_minor, currency):
    """Convert from Paddle minor unit (cents/paise/etc.) to USD."""
    rate = FX.get(currency.upper(), 1.0)
    # Paddle uses 100 subunits for virtually all currencies
    return (amount_minor / 100.0) * rate

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

        # Unit price in smallest currency unit — convert to USD
        unit_price_data = price.get('unit_price', {})
        unit_amount  = int(unit_price_data.get('amount', 0))
        price_currency = (unit_price_data.get('currency_code') or currency).upper()
        unit_usd = to_usd(unit_amount, price_currency)

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
