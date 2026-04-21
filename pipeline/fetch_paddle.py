#!/usr/bin/env python3
"""
Paddle Billing API → transaction_line_items CSV
Fetches all completed transactions from Paddle API and saves them in the
same CSV format that compute_mrr.py expects.

Env vars required:
  PADDLE_API_KEY   — your Paddle secret key (pdl_live_...)
  PADDLE_ENV       — 'production' (default) or 'sandbox'
"""

import csv, os, time, json
from pathlib import Path
from datetime import datetime, timezone, timedelta

try:
    import requests
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'requests', '-q'])
    import requests

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY        = os.environ['PADDLE_API_KEY']
ENV            = os.environ.get('PADDLE_ENV', 'production')
BASE_URL       = ('https://sandbox-api.paddle.com'
                  if ENV == 'sandbox' else 'https://api.paddle.com')
FULL_FETCH_FROM = '2024-10-01T00:00:00Z'   # seed date: Paddle Billing launch month
PER_PAGE        = 200                        # Paddle max

# Output goes into the same directory as the script (pipeline/)
OUT_DIR  = Path(__file__).parent
TLI_OUT  = OUT_DIR / 'transaction_line_items.csv'

# ── Incremental fetch: determine start date ───────────────────────────────────
def get_incremental_start():
    if not TLI_OUT.exists():
        print(f"  No existing CSV — full fetch from {FULL_FETCH_FROM}", flush=True)
        return FULL_FETCH_FROM, False
    latest_date = None
    with open(TLI_OUT, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            for col in ('transaction_billed_at', 'completed_at', 'transaction_created_at'):
                val = row.get(col, '').strip()
                if val:
                    try:
                        d = datetime.strptime(val[:19], '%Y-%m-%dT%H:%M:%S')
                        if latest_date is None or d > latest_date:
                            latest_date = d
                    except ValueError:
                        pass
    if latest_date is None:
        print(f"  Existing CSV has no dates — full fetch from {FULL_FETCH_FROM}", flush=True)
        return FULL_FETCH_FROM, False
    cutoff = (latest_date - timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"  Existing CSV latest date: {latest_date.date()} — incremental fetch from {cutoff}", flush=True)
    return cutoff, True

FETCH_FROM, IS_INCREMENTAL = get_incremental_start()

ZERO_DECIMAL = {
    'BIF','CLP','DJF','GNF','JPY','KMF','KRW','MGA','PYG',
    'RWF','UGX','VND','VUV','XAF','XOF','XPF'
}

def to_major(amount_str, currency):
    try:
        v = float(amount_str)
    except (TypeError, ValueError):
        return 0.0
    if currency.upper() in ZERO_DECIMAL:
        return v
    return v / 100.0

def paddle_get(path, params=None):
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Content-Type':  'application/json',
    }
    for attempt in range(8):
        try:
            r = requests.get(BASE_URL + path, headers=headers, params=params, timeout=60)
            if r.status_code == 429:
                wait = int(r.headers.get('Retry-After', 15))
                print(f"    Rate-limited — waiting {wait}s …", flush=True)
                time.sleep(wait)
                continue
            if r.status_code in (500, 502, 503, 504):
                wait = min(10 * (attempt + 1), 60)
                print(f"    Paddle {r.status_code} — retrying in {wait}s (attempt {attempt+1}/8) …", flush=True)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ConnectionError as e:
            wait = min(10 * (attempt + 1), 60)
            print(f"    Connection error — retrying in {wait}s (attempt {attempt+1}/8): {e}", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"Failed after 8 retries: {path}")

# ── Fetch price catalog for authoritative billing_cycle lookup ────────────────
print("[0/2] Fetching price catalog …", flush=True)
price_bc = {}
_after = None
while True:
    _params = {'per_page': 200}
    if _after:
        _params['after'] = _after
    _resp  = paddle_get('/prices', _params)
    _prices = _resp.get('data', [])
    for _p in _prices:
        _pid = _p.get('id', '')
        if _pid:
            _bc = _p.get('billing_cycle', {}) or {}
            if _bc:
                price_bc[_pid] = _bc
    _pag = _resp.get('meta', {}).get('pagination', {})
    if not _pag.get('has_more', False) or not _prices:
        break
    _next = _pag.get('next', '')
    _after = None
    if _next:
        from urllib.parse import urlparse, parse_qs
        _qs    = parse_qs(urlparse(_next).query)
        _after = _qs.get('after', [None])[0]
    if not _after:
        break
print(f"  {len(price_bc)} prices with billing_cycle loaded", flush=True)

# ── Fetch transactions ────────────────────────────────────────────────────────
mode_label = "INCREMENTAL" if IS_INCREMENTAL else "FULL"
print(f"[1/2] Fetching completed transactions from Paddle API … [{mode_label}]", flush=True)
print(f"      Base URL   : {BASE_URL}", flush=True)
print(f"      Fetch from : {FETCH_FROM}", flush=True)

rows_written = 0
txn_count    = 0
page         = 0
after_cursor = None

fieldnames = [
    'transaction_id', 'transaction_status', 'customer_id', 'customer_email',
    'subscription_id', 'product_name', 'price_id', 'price_description',
    'billing_cycle_frequency', 'billing_cycle_interval',
    'transaction_currency_code', 'unit_price', 'quantity',
    'subtotal', 'discount', 'tax', 'total', 'proration_rate',
    'billing_period_starts_at', 'billing_period_ends_at',
    'transaction_billed_at', 'transaction_created_at', 'completed_at',
    'origin', 'collection_mode',
    'balance_currency_code',
    'transaction_to_balance_currency_exchange_rate',
]

file_mode = 'a' if IS_INCREMENTAL else 'w'
with open(TLI_OUT, file_mode, newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    if not IS_INCREMENTAL:
        writer.writeheader()

    while True:
        page += 1
        params = {
            'status':              'completed',
            'billed_at[gte]':      FETCH_FROM,
            'per_page':            PER_PAGE,
        }
        if after_cursor:
            params['after'] = after_cursor

        resp     = paddle_get('/transactions', params)
        txns     = resp.get('data', [])
        meta     = resp.get('meta', {})
        pag      = meta.get('pagination', {})

        if not txns:
            break

        for txn in txns:
            txn_id    = txn.get('id', '')
            status    = txn.get('status', '')
            cust_id   = txn.get('customer_id', '')
            sub_id    = txn.get('subscription_id', '') or ''
            origin    = txn.get('origin', '')
            mode      = txn.get('collection_mode', '')
            currency  = txn.get('currency_code', 'USD')
            billed_at = txn.get('billed_at', '') or ''
            created_at = txn.get('created_at', '') or ''
            email     = ''

            completed_at = ''
            for attempt in (txn.get('payments') or []):
                if attempt.get('status') == 'captured':
                    completed_at = attempt.get('captured_at', '')
                    break

            details       = txn.get('details', {})
            totals        = details.get('totals', {})
            bal_currency  = totals.get('balance_currency_code', 'USD') or 'USD'

            raw_sub     = totals.get('subtotal', '0') or '0'
            raw_bal_sub = totals.get('balance_subtotal', None)

            if raw_bal_sub is not None:
                try:
                    local_sub = float(raw_sub)
                    bal_sub   = float(raw_bal_sub)
                    if local_sub != 0:
                        rate = (bal_sub / local_sub)
                    else:
                        rate = 1.0
                except (ValueError, ZeroDivisionError):
                    rate = 1.0
            else:
                rate = 1.0

            line_items = details.get('line_items', [])

            if not line_items:
                line_items_raw = txn.get('items', [])
                for item in line_items_raw:
                    price     = item.get('price', {}) or {}
                    product   = price.get('product', {}) or {}
                    _pid_fb   = price.get('id', '')
                    bc        = price.get('billing_cycle', {}) or price_bc.get(_pid_fb, {})
                    bp        = item.get('billing_period', {}) or {}
                    itotals   = item.get('totals', {}) or {}

                    subtotal  = to_major(itotals.get('subtotal',  '0'), currency)
                    discount  = to_major(itotals.get('discount',  '0'), currency)
                    tax       = to_major(itotals.get('tax',       '0'), currency)
                    total     = to_major(itotals.get('total',     '0'), currency)
                    unit_p    = to_major(price.get('unit_price', {}).get('amount', '0'), currency)
                    qty       = item.get('quantity', 1)
                    pror      = item.get('proration', {}).get('rate', '') if item.get('proration') else ''

                    writer.writerow({
                        'transaction_id':    txn_id,
                        'transaction_status': status,
                        'customer_id':       cust_id,
                        'customer_email':    email,
                        'subscription_id':   sub_id,
                        'product_name':      product.get('name', ''),
                        'price_id':          price.get('id', ''),
                        'price_description': price.get('description', ''),
                        'billing_cycle_frequency': bc.get('frequency', 1),
                        'billing_cycle_interval':  bc.get('interval', 'month'),
                        'transaction_currency_code': currency,
                        'unit_price':        unit_p,
                        'quantity':          qty,
                        'subtotal':          subtotal,
                        'discount':          discount,
                        'tax':               tax,
                        'total':             total,
                        'proration_rate':    pror,
                        'billing_period_starts_at': bp.get('starts_at', ''),
                        'billing_period_ends_at':   bp.get('ends_at', ''),
                        'transaction_billed_at':    billed_at,
                        'transaction_created_at':   created_at,
                        'completed_at':      completed_at,
                        'origin':            origin,
                        'collection_mode':   mode,
                        'balance_currency_code': bal_currency,
                        'transaction_to_balance_currency_exchange_rate': rate,
                    })
                    rows_written += 1
            else:
                items_lookup = {}
                for itm in (txn.get('items') or []):
                    p       = itm.get('price', {}) or {}
                    pid     = p.get('id', '')
                    if pid:
                        items_lookup[pid] = {
                            'bc':      p.get('billing_cycle', {}) or {},
                            'bp':      itm.get('billing_period', {}) or {},
                            'product': (p.get('product', {}) or {}).get('name', ''),
                            'desc':    p.get('description', ''),
                        }

                for li in line_items:
                    price_id  = li.get('price_id', '')
                    li_totals = li.get('totals', {}) or {}
                    proration = li.get('proration', {}) or {}
                    qty       = li.get('quantity', 1)

                    itm_info  = items_lookup.get(price_id, {})
                    bc        = itm_info.get('bc') or price_bc.get(price_id, {})
                    bc_freq   = bc.get('frequency', 1)
                    bc_intv   = bc.get('interval', 'month')

                    bp_itm    = itm_info.get('bp', {})
                    bp_li     = li.get('billing_period', {}) or {}
                    bp        = bp_itm if bp_itm.get('starts_at') else bp_li

                    prod_name = itm_info.get('product', '') or \
                                (li.get('product', {}) or {}).get('name', '')

                    subtotal  = to_major(li_totals.get('subtotal',  '0'), currency)
                    discount  = to_major(li_totals.get('discount',  '0'), currency)
                    tax       = to_major(li_totals.get('tax',       '0'), currency)
                    total     = to_major(li_totals.get('total',     '0'), currency)
                    pror_rate = proration.get('rate', '') if proration else ''
                    unit_p    = to_major(li_totals.get('subtotal',  '0'), currency)

                    writer.writerow({
                        'transaction_id':    txn_id,
                        'transaction_status': status,
                        'customer_id':       cust_id,
                        'customer_email':    email,
                        'subscription_id':   sub_id,
                        'product_name':      prod_name,
                        'price_id':          price_id,
                        'price_description': itm_info.get('desc', ''),
                        'billing_cycle_frequency': bc_freq,
                        'billing_cycle_interval':  bc_intv,
                        'transaction_currency_code': currency,
                        'unit_price':        unit_p,
                        'quantity':          qty,
                        'subtotal':          subtotal,
                        'discount':          discount,
                        'tax':               tax,
                        'total':             total,
                        'proration_rate':    pror_rate,
                        'billing_period_starts_at': bp.get('starts_at', ''),
                        'billing_period_ends_at':   bp.get('ends_at', ''),
                        'transaction_billed_at':    billed_at,
                        'transaction_created_at':   created_at,
                        'completed_at':      completed_at,
                        'origin':            origin,
                        'collection_mode':   mode,
                        'balance_currency_code': bal_currency,
                        'transaction_to_balance_currency_exchange_rate': rate,
                    })
                    rows_written += 1

            txn_count += 1

        print(f"    Page {page:>3}: {len(txns)} txns  |  total so far: {txn_count:,}  "
              f"({rows_written:,} line items)", flush=True)

        if not pag.get('has_more', False):
            break
        next_url = pag.get('next', '')
        if next_url:
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(next_url).query)
            after_cursor = qs.get('after', [None])[0]
        if not after_cursor:
            break

print(f"\n  ✓ {txn_count:,} transactions  →  {rows_written:,} line item rows", flush=True)
print(f"  ✓ Saved: {TLI_OUT}", flush=True)
