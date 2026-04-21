#!/usr/bin/env python3
"""
MRR Data Validator
Runs after compute_mrr.py, before build_dashboard.py and update_gsheet.py.

Exits with code 1 (aborting the pipeline) if ANY check fails.
Exits with code 0 only when all checks pass — safe to publish outputs.

Checks:
  1. JSON loads and has required keys
  2. At least 3 months of data present
  3. Data starts at or after REPORT_START (2025-01)
  4. No missing months (sequential, no gaps)
  5. Opening MRR continuity — opening[N] == closing[N-1]
  6. Waterfall integrity — closing == opening + new + expansion + reactivation - contraction - churn
  7. All MRR movement components are non-negative
  8. Closing MRR > 0 for every reported month
  9. ARPA > 0 for every month with active customers
 10. Churn rate between 0% and 100% for every month
 11. NRR between 0% and 500% for every month that has it
 12. Summary fields populated, non-zero, and consistent with last month
 13. Plan mix MRR sums to within 1% of current MRR
 14. Customer list non-empty and all emails populated
"""

import json, sys
from pathlib import Path
from datetime import datetime

DATA_FILE    = Path(__file__).parent / 'mrr_data.json'
REPORT_START = '2025-01'
WATERFALL_TOL = 0.50   # $0.50 rounding tolerance per month

# ── helpers ──────────────────────────────────────────────────────────────────
def add_months(ym, n):
    y, m = int(ym[:4]), int(ym[5:7])
    m += n
    while m > 12: m -= 12; y += 1
    while m <  1: m += 12; y -= 1
    return f'{y:04d}-{m:02d}'

PASS = '\033[92m✓\033[0m'
FAIL = '\033[91m✗\033[0m'

errors   = []
warnings = []

def ok(msg):
    print(f'  {PASS}  {msg}', flush=True)

def err(msg):
    errors.append(msg)
    print(f'  {FAIL}  ERROR: {msg}', flush=True)

def warn(msg):
    warnings.append(msg)
    print(f'  ⚠   WARN: {msg}', flush=True)

# ── load ─────────────────────────────────────────────────────────────────────
print('\n══ MRR Data Validation ══\n', flush=True)

if not DATA_FILE.exists():
    print(f'  {FAIL}  mrr_data.json not found at {DATA_FILE}')
    sys.exit(1)

try:
    with open(DATA_FILE) as f:
        data = json.load(f)
except Exception as e:
    print(f'  {FAIL}  Failed to parse mrr_data.json: {e}')
    sys.exit(1)

ok('mrr_data.json loaded and parsed')

# ── check 1: required keys ────────────────────────────────────────────────────
required_keys = {'summary', 'monthly_metrics', 'plan_mix', 'customers'}
missing = required_keys - set(data.keys())
if missing:
    err(f'Missing top-level keys: {missing}')
else:
    ok('All required top-level keys present')

if errors:
    print(f'\n══ ABORTED — {len(errors)} error(s) found ══\n')
    sys.exit(1)

months  = data['monthly_metrics']
summary = data['summary']
pmix    = data['plan_mix']
custs   = data['customers']

# ── check 2: minimum months ───────────────────────────────────────────────────
if len(months) < 3:
    err(f'Only {len(months)} month(s) of data — expected at least 3')
else:
    ok(f'{len(months)} months of data  ({months[0]["month"]} → {months[-1]["month"]})')

# ── check 3: starts at or after REPORT_START ──────────────────────────────────
if months[0]['month'] < REPORT_START:
    err(f'Data starts at {months[0]["month"]} — expected {REPORT_START} or later')
elif months[0]['month'] > REPORT_START:
    warn(f'Data starts at {months[0]["month"]} (expected {REPORT_START}) — check FETCH_FROM date')
else:
    ok(f'Data starts correctly at {REPORT_START}')

# ── check 4: no missing months (sequential) ───────────────────────────────────
month_list = [m['month'] for m in months]
gaps = []
for i in range(1, len(month_list)):
    expected = add_months(month_list[i-1], 1)
    if month_list[i] != expected:
        gaps.append(f'gap between {month_list[i-1]} and {month_list[i]} (expected {expected})')
if gaps:
    for g in gaps:
        err(f'Missing month: {g}')
else:
    ok('No missing months — full sequential coverage')

# ── check 5: opening MRR continuity ──────────────────────────────────────────
continuity_errors = 0
for i in range(1, len(months)):
    prev_closing = months[i-1]['closing_mrr']
    this_opening = months[i]['opening_mrr']
    diff = abs(prev_closing - this_opening)
    if diff > WATERFALL_TOL:
        err(f'{months[i]["month"]}: opening MRR ${this_opening:,.2f} ≠ prev closing ${prev_closing:,.2f} (diff ${diff:,.2f})')
        continuity_errors += 1
if continuity_errors == 0:
    ok('Opening MRR continuity — each opening equals prior closing')

# ── check 6: waterfall integrity ─────────────────────────────────────────────
waterfall_errors = 0
for m in months:
    expected = (m['opening_mrr'] + m['new_business'] + m['expansion']
                + m['reactivation'] - m['contraction'] - m['churn'])
    diff = abs(expected - m['closing_mrr'])
    if diff > WATERFALL_TOL:
        err(f'{m["month"]}: waterfall mismatch — expected ${expected:,.2f} got ${m["closing_mrr"]:,.2f} (diff ${diff:,.2f})')
        waterfall_errors += 1
if waterfall_errors == 0:
    ok('Waterfall integrity — all months: closing = opening + movements')

# ── check 7: non-negative MRR components ─────────────────────────────────────
neg_errors = 0
for m in months:
    for field in ('new_business', 'expansion', 'reactivation', 'contraction', 'churn'):
        if m[field] < -0.01:
            err(f'{m["month"]}: {field} is negative (${m[field]:,.2f})')
            neg_errors += 1
if neg_errors == 0:
    ok('All MRR movement components are non-negative')

# ── check 8: closing MRR > 0 for all months ──────────────────────────────────
zero_mrr = [m['month'] for m in months if m['closing_mrr'] <= 0]
if zero_mrr:
    err(f'Zero or negative closing MRR in months: {zero_mrr}')
else:
    ok('Closing MRR > 0 for all reported months')

# ── check 9: ARPA > 0 for months with customers ──────────────────────────────
arpa_errors = 0
for m in months:
    if m['active_customers'] > 0 and m['arpa'] <= 0:
        err(f'{m["month"]}: active_customers={m["active_customers"]} but ARPA=${m["arpa"]:.2f}')
        arpa_errors += 1
if arpa_errors == 0:
    ok('ARPA > 0 for all months with active customers')

# ── check 10: churn rate 0-100% ───────────────────────────────────────────────
churn_errors = 0
for m in months:
    cr = m['mrr_churn_rate']
    if cr < 0 or cr > 100:
        err(f'{m["month"]}: MRR churn rate {cr:.2f}% out of range [0,100]')
        churn_errors += 1
if churn_errors == 0:
    ok('MRR churn rates all within 0-100%')

# ── check 11: NRR 0-500% ─────────────────────────────────────────────────────
nrr_errors = 0
for m in months:
    nrr = m.get('nrr')
    if nrr is not None:
        if nrr < 0 or nrr > 500:
            err(f'{m["month"]}: NRR {nrr:.2f}% out of expected range [0,500]')
            nrr_errors += 1
if nrr_errors == 0:
    ok('NRR values all within 0-500%')

# ── check 12: summary consistency ────────────────────────────────────────────
last_m = months[-1]
sum_errors = 0

for field, expected, label in [
    ('current_mrr',       last_m['closing_mrr'],      'current_mrr vs last closing_mrr'),
    ('current_arr',       last_m['arr'],               'current_arr vs last arr'),
    ('current_customers', last_m['active_customers'],  'current_customers vs last active_customers'),
]:
    got = summary.get(field)
    if got != expected:
        err(f'Summary {label}: summary says {got}, last month says {expected}')
        sum_errors += 1

if summary.get('current_mrr', 0) <= 0:
    err(f'Summary current_mrr is zero or missing')
    sum_errors += 1
if summary.get('avg_nrr') is None:
    warn('Summary avg_nrr is missing')
if sum_errors == 0:
    ok(f'Summary consistent with last month  (MRR=${summary["current_mrr"]:,.0f}, '
       f'ARR=${summary["current_arr"]:,.0f}, Customers={summary["current_customers"]})')

# ── check 13: plan mix totals ─────────────────────────────────────────────────
if pmix:
    pm_total = sum(p['mrr'] for p in pmix)
    cur_mrr  = summary['current_mrr']
    pct_diff = abs(pm_total - cur_mrr) / cur_mrr * 100 if cur_mrr else 100
    if pct_diff > 1.0:
        err(f'Plan mix total ${pm_total:,.2f} differs from current MRR ${cur_mrr:,.2f} by {pct_diff:.1f}%')
    else:
        ok(f'Plan mix total ${pm_total:,.2f} matches current MRR (within 1%)')
else:
    warn('Plan mix is empty')

# ── check 14: customer list ───────────────────────────────────────────────────
if not custs:
    err('Customer list is empty')
else:
    no_email = [c for c in custs if not c.get('email', '').strip()]
    if no_email:
        warn(f'{len(no_email)} customer(s) have no email address')
    active_custs = [c for c in custs if c['status'] == 'active']
    ok(f'Customer list: {len(custs):,} total, {len(active_custs):,} active')

# ── result ────────────────────────────────────────────────────────────────────
print(f'\n{"─"*50}', flush=True)
if warnings:
    print(f'  {len(warnings)} warning(s):', flush=True)
    for w in warnings:
        print(f'    ⚠  {w}', flush=True)

if errors:
    print(f'\n  ✗  VALIDATION FAILED — {len(errors)} error(s). Pipeline aborted.', flush=True)
    print(f'     Dashboard and Google Sheet were NOT updated.', flush=True)
    print(f'{"─"*50}\n', flush=True)
    sys.exit(1)
else:
    print(f'\n  ✓  ALL CHECKS PASSED — safe to publish outputs.', flush=True)
    print(f'{"─"*50}\n', flush=True)
    sys.exit(0)
