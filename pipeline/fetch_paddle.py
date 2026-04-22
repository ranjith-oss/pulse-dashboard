#!/usr/bin/env python3
"""
Paddle Billing API → transaction_line_items CSV
Fetches all completed transactions from Paddle API and saves them in the
same CSV format that compute_mrr.py expects.

Env vars required:
  PADDLE_API_KEY  – your Paddle secret key (pdl_live_...)
  PADDLE_ENV      – 'production' (default) or 'sandbox'
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

API_KEY  = os.environ['PADDLE_API_KEY']
ENV      = os.environ.get('PADDLE_ENV', 'production')
BASE_URL = ('https://sandbox-api.paddle.com'
            if ENV == 'sandbox' else
            'https://api.paddle.com')

FULL_FETCH_FROM = '2024-10-01T00:00:00Z'   # seed date: Paddle Billing launch month
PER_PAGE        = 200                        # Paddle max

# Output goes into the same directory as the script (pipeline/)
OUT_DIR  = Path(__file__).parent
TLI_OUT  = OUT_DIR / 'transaction_line_items.csv'

# ── Always do a full fetch ────────────────────────────────────────────────────
# NOTE: Paddle's /transactions endpoint does NOT support billed_at[gte] filtering
# (the parameter is silently ignored). Every run returns ALL transactions regardless,
# so there is no benefit to incremental mode. We always do a full overwrite so
# the CSV stays clean and exchange rates are always freshly computed.
FETCH_FROM     = FULL_FETCH_FROM
IS_INCREMENTAL = False
print(f"  Full fetch from {FULL_FETCH_FROM} (Paddle billed_at filter not supported)", flush=True)

# Zero-decimal currencies (don't divide by 100)
ZERO_DECIMAL = {
    'BIF','CLP','DJF','GNF','JPY','KMF','KRW','MGA','PYG',
    'RWF','UGX','VND','VUV','XAF','XOF','XPF'
}

def to_major(amount_str, currency):
    """Convert Paddle minor-unit string to float major units."""
    try:
        v = float(amount_str)
    except (TypeError, ValueError):
        return 0.0
    if currency.upper() in ZERO_DECIMAL:
        return v
    return v / 100.0

def paddle_get(path, params=None):
    """Single authenticated GET to Paddle API with retry on 429 and 5xx errors."""
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
                wait = min(10 * (attempt + 1), 60)  # 10s, 20s, 30s … up to 60s
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
# Paddle's list-transactions API does NOT reliably return billing_cycle inside
# items[].price — it's often absent. Fetching /prices first gives us a small,
# stable lookup table (usually < 200 prices) that is always populated.
print("[0/2] Fetching price catalog …", flush=True)
price_bc = {}   # price_id → {'interval': 'month'|'year', 'frequency': N}
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
mode_label = "FULL"
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

# Always full fetch — overwrite CSV with fresh data and correct exchange rates
file_mode = 'w'
with open(TLI_OUT, file_mode, newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    while True:
        page += 1
        params = {
            'status':              'completed',
            'billed_at[gte]':      FETCH_FROM,
            'per_page':            PER_PAGE,
            # NOTE: do NOT add 'include=customer' — Paddle silently caps per_page
            # at 30 when that param is used, making full fetches 6x slower.
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
            currency  = txn.get('currency_code', 'USD') or 'USD'
            billed_at = txn.get('billed_at', '') or ''
            created_at = txn.get('created_at', '') or ''

            # customer email not fetched (include=customer removed for speed)
            email     = ''

            # Completed_at from payment_attempts
            completed_at = ''
            for attempt in (txn.get('payments') or []):
                if attempt.get('status') == 'captured':
                    completed_at = attempt.get('captured_at', '')
                    break

            # Exchange rate: use details.payouts_totals (payout/USD amounts) vs
            # details.totals (local currency amounts). Both are in minor units.
            # IMPORTANT: details.totals.balance_subtotal does NOT exist in Paddle's
            # API — the correct source is details.payouts_totals.subtotal.
            details       = txn.get('details', {})
            totals        = details.get('totals', {})
            payouts_totals = details.get('payouts_totals', {}) or {}
            bal_currency  = totals.get('currency_code', 'USD') or 'USD'

            raw_sub     = totals.get('subtotal', '0') or '0'
            raw_bal_sub = payouts_totals.get('subtotal', None)

            if raw_bal_sub is not None:
                try:
                    local_sub = float(raw_sub)
                    bal_sub   = float(raw_bal_sub)
                    if local_sub != 0:
                        rate = (bal_sub / local_sub)
                        # both are in minor units so rate is correct
                    else:
                        rate = 1.0
                except (ValueError, ZeroDivisionError):
                    rate = 1.0
            else:
                rate = 1.0    # same currency or unknown (fallback)

            # Line items live in details.line_items
            line_items = details.get('line_items', [])

            if not line_items:
                # Fallback: use items array directly
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
                # Build lookup from items[] for billing_period + billing_cycle
                # details.line_items does NOT reliably carry these fields —
                # they must be sourced from items[] keyed by price_id.
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

                    # Billing cycle: items[] first, then price catalog, then default
                    itm_info  = items_lookup.get(price_id, {})
                    bc        = itm_info.get('bc') or price_bc.get(price_id, {})
                    bc_freq   = bc.get('frequency', 1)
                    bc_intv   = bc.get('interval', 'month')

                    # billing_period: prefer items[], fall back to line_item field
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

        txn_count += len(txns)
        print(f"    Page {page:4d}: {len(txns):2d} txns  |  total so far: {txn_count:,}  ({rows_written:,} line items)", flush=True)

        after_cursor = pag.get('next', None)
        if not pag.get('has_more', False) or not after_cursor:
            break

print(f"  ✓ {txn_count:,} transactions → {rows_written:,} line item rows", flush=True)
print(f"  ✓ Saved: {TLI_OUT}", flush=True)
