#!/usr/bin/env python3
"""
MRR Analytics Engine — v4
Source : Paddle Billing — transaction_line_items CSV (single file, all fields present)
Window : Jan 2025 → latest complete billing month

KEY METHODOLOGY (unchanged from v3, simplified input)
────────────────────────────────────────────────────────────────────────────
1. Only completed transactions (transaction_status = 'completed')
2. Only rows with a subscription_id (skips one-off purchases)
3. Exchange rate from line-item field transaction_to_balance_currency_exchange_rate
   (100% populated in new export format — verified)
4. Discount deducted before conversion:
     net_local = subtotal − discount;  net_usd = net_local × rate
5. Proration credits (subtotal ≤ 0) and fully-discounted items (net_local ≤ 0) skipped
6. Annual plans divided by 12 to get monthly MRR contribution
7. Active period from billing_period_starts_at / billing_period_ends_at
8. MRR overwrite: newer transaction for same sub replaces prior value for that month
9. MRR Movements (ChartMogul):
     New Business  : active now, never before
     Reactivation  : active now, was before, gap in between
     Expansion     : MRR rose vs prior month
     Contraction   : MRR fell vs prior month
     Churn         : active last month, not this month
10. Customer dedup by email; ARPA = Closing MRR / Active Customers
────────────────────────────────────────────────────────────────────────────
"""

import csv, json, sys, glob, os
from collections import defaultdict
from datetime import datetime, date, timezone, timedelta
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────
UPLOADS_DIR  = Path(__file__).parent
OUTPUT_JSON  = Path(__file__).parent / 'mrr_data.json'
REPORT_START = '2025-01'

def find_latest(pattern):
    """Return the most recently modified file matching a glob pattern."""
    matches = sorted(UPLOADS_DIR.glob(pattern), key=os.path.getmtime, reverse=True)
    if not matches:
        raise FileNotFoundError(f"No file matching {pattern} in {UPLOADS_DIR}")
    return matches[0]

TLI_FILE = UPLOADS_DIR / 'transaction_line_items.csv'
ADJ_FILE = None
print(f"  Transaction line items : {TLI_FILE.name}")

# ── Helpers ──────────────────────────────────────────────────────────────────
def parse_date(s):
    if not s: return None
    s = s.strip()
    for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
        try: return datetime.strptime(s, fmt).date()
        except: pass
    return None

def month_str(d):
    return d.strftime('%Y-%m') if d else None

def add_months(ym, n):
    y, m = int(ym[:4]), int(ym[5:7])
    m += n
    while m > 12: m -= 12; y += 1
    while m < 1:  m += 12; y -= 1
    return f'{y:04d}-{m:02d}'

def months_range(ym_start, ym_end):
    months, ym = [], ym_start
    while ym <= ym_end:
        months.append(ym)
        ym = add_months(ym, 1)
    return months

# ── Step 1: Load & aggregate line items ──────────────────────────────────────
print("[1/4] Loading transaction line items …", flush=True)

sub_txn_agg      = defaultdict(lambda: defaultdict(dict))
seen_dedup        = set()
true_dups         = 0
skipped_neg       = 0
skipped_discount  = 0
skipped_no_sub    = 0
total_gross_usd   = 0.0
total_disc_usd    = 0.0

with open(TLI_FILE, encoding='utf-8') as f:
    for row in csv.DictReader(f):

        # Filter: completed + subscription only
        if row['transaction_status'] != 'completed':
            continue
        sub_id = row.get('subscription_id', '').strip()
        if not sub_id:
            skipped_no_sub += 1
            continue

        txn_id = row['transaction_id']

        # Dedup by (txn_id, price_id, subtotal, period_start)
        dedup_key = (txn_id, row.get('price_id',''),
                     row.get('subtotal',''), row.get('billing_period_starts_at',''))
        if dedup_key in seen_dedup:
            true_dups += 1
            continue
        seen_dedup.add(dedup_key)

        # Amounts
        try:
            subtotal = float(row['subtotal']) if row.get('subtotal') else 0.0
            discount = float(row['discount']) if row.get('discount') else 0.0
        except (ValueError, TypeError):
            subtotal = discount = 0.0

        if subtotal <= 0:
            skipped_neg += 1
            continue

        net_local = subtotal - discount
        if net_local <= 0:
            skipped_discount += 1
            continue

        # Exchange rate (100% populated in new format)
        try:
            rate = float(row['transaction_to_balance_currency_exchange_rate'])
            if rate <= 0: rate = 1.0
        except (ValueError, TypeError):
            rate = 1.0

        net_usd   = net_local * rate
        gross_usd = subtotal  * rate
        disc_usd  = discount  * rate
        total_gross_usd += gross_usd
        total_disc_usd  += disc_usd

        # Dates (parse early so we can use period dates for billing inference)
        billed_date  = (parse_date(row.get('transaction_billed_at'))
                        or parse_date(row.get('completed_at'))
                        or parse_date(row.get('transaction_created_at')))
        period_start = parse_date(row.get('billing_period_starts_at', ''))
        period_end   = parse_date(row.get('billing_period_ends_at', ''))

        if not billed_date:
            continue

        # Billing cycle — prefer date-based inference over billing_cycle field.
        # Paddle's list API often omits billing_cycle in price objects, so we
        # infer from how long the billing period actually spans.
        if period_start and period_end:
            _days = (period_end - period_start).days
            billing_months = max(1, round(_days / 30.44))
        else:
            # Fallback: use explicit billing_cycle columns
            try:
                freq = int(row['billing_cycle_frequency']) if row.get('billing_cycle_frequency') else 1
            except:
                freq = 1
            interval = row.get('billing_cycle_interval', 'month')
            if interval == 'year':
                billing_months = 12 * max(1, freq)
            else:
                billing_months = max(1, freq)

        # Customer identifier — use email when available, fall back to customer_id.
        # (email is blank when include=customer is omitted from the Paddle API call)
        email = row.get('customer_email', '').strip().lower()
        if not email:
            email = row.get('customer_id', '').strip()
        product = row.get('product_name', '').strip()
        origin  = row.get('origin', '').strip()

        # Aggregate per (sub_id, txn_id)
        entry = sub_txn_agg[sub_id][txn_id]
        if not entry:
            entry.update({
                'date':           billed_date,
                'email':          email,
                'origin':         origin,
                'net_usd':        0.0,
                'billing_months': billing_months,
                'period_start':   period_start,
                'period_end':     period_end,
                'product':        product,
            })
        entry['net_usd'] += net_usd
        if billing_months > entry['billing_months']:
            entry['billing_months'] = billing_months
        if period_start and (not entry['period_start'] or period_start < entry['period_start']):
            entry['period_start'] = period_start
        if period_end and (not entry['period_end'] or period_end > entry['period_end']):
            entry['period_end'] = period_end

print(f"  True duplicates removed      : {true_dups:,}", flush=True)
print(f"  Proration credits skipped    : {skipped_neg:,}", flush=True)
print(f"  100%-discounted skipped      : {skipped_discount:,}", flush=True)
print(f"  No sub_id skipped            : {skipped_no_sub:,}", flush=True)
print(f"  Gross USD                    : ${total_gross_usd:,.2f}", flush=True)
print(f"  Discount USD                 : ${total_disc_usd:,.2f}", flush=True)
print(f"  Net USD (actual revenue)     : ${total_gross_usd - total_disc_usd:,.2f}", flush=True)
print(f"  Discount %                   : {total_disc_usd/total_gross_usd*100:.2f}%", flush=True)
print(f"  Unique subscriptions         : {len(sub_txn_agg):,}", flush=True)

# ── Diagnostic: billing_months distribution (helps verify annual plan handling)
_bm_counts = {}
for _sub, _txns in sub_txn_agg.items():
    for _txn in _txns.values():
        _bm = _txn['billing_months']
        _bm_counts[_bm] = _bm_counts.get(_bm, 0) + 1
print(f"  Billing months distribution  : { {k: v for k, v in sorted(_bm_counts.items())} }", flush=True)
# If annual plans are handled correctly you should see a large count at bm=12

# ── Step 2: Build subscription MRR timeline ──────────────────────────────────
print("[2/4] Building subscription MRR timelines …", flush=True)

sub_monthly_mrr = defaultdict(dict)
sub_info        = {}

for sub_id, txn_dict in sub_txn_agg.items():
    txn_list = sorted(txn_dict.values(), key=lambda x: x['date'])
    sub_info[sub_id] = {
        'email':   txn_list[0]['email'],
        'product': txn_list[0]['product'],
    }
    for t in txn_list:
        bm  = max(1, t['billing_months'])
        mrr = t['net_usd'] / bm

        if t['period_start'] and t['period_end']:
            start_m = month_str(t['period_start'])
            # Paddle's ends_at is EXCLUSIVE (= start of next billing period).
            # Subtract 1 day so a monthly Oct 1→Nov 1 maps to Oct only, not Oct+Nov.
            end_m   = month_str(t['period_end'] - timedelta(days=1))
        else:
            start_m = month_str(t['date'])
            end_m   = add_months(start_m, bm - 1)

        for m in months_range(start_m, end_m):
            sub_monthly_mrr[sub_id][m] = mrr          # overwrite = last txn wins

print(f"  MRR map built for {len(sub_monthly_mrr):,} subscriptions", flush=True)

# ── Step 3: Monthly metrics ──────────────────────────────────────────────────
print("[3/4] Computing monthly metrics …", flush=True)

all_months = sorted(set(m for sub in sub_monthly_mrr.values() for m in sub))
monthly_metrics = {}

for i, month in enumerate(all_months):
    prev_month = all_months[i - 1] if i > 0 else None

    active_this = {k: v[month]      for k, v in sub_monthly_mrr.items() if month in v}
    active_prev = ({k: v[prev_month] for k, v in sub_monthly_mrr.items()
                    if prev_month in v} if prev_month else {})

    closing_mrr = sum(active_this.values())
    opening_mrr = sum(active_prev.values())

    new_biz = exp = contr = churn = reactiv = 0.0
    new_sub_cnt = churned_sub_cnt = 0

    for sub_id, mrr in active_this.items():
        if sub_id not in active_prev:
            ever_active = any(m < month for m in sub_monthly_mrr[sub_id])
            if ever_active:
                reactiv += mrr
            else:
                new_biz     += mrr
                new_sub_cnt += 1
        else:
            diff = mrr - active_prev[sub_id]
            if   diff >  0.005: exp   += diff
            elif diff < -0.005: contr += abs(diff)

    for sub_id, mrr in active_prev.items():
        if sub_id not in active_this:
            churn         += mrr
            churned_sub_cnt += 1

    net_new = new_biz + exp + reactiv - contr - churn

    # Customer counts (email-deduplicated)
    active_emails = {sub_info[s]['email'] for s in active_this if s in sub_info}
    prev_emails   = ({sub_info[s]['email'] for s in active_prev if s in sub_info}
                     if prev_month else set())

    new_cust = sum(
        1 for s in active_this
        if sub_info.get(s, {}).get('email', '') and
        min(sub_monthly_mrr[s].keys()) == month
    )

    monthly_metrics[month] = {
        'month':                 month,
        'opening_mrr':           round(opening_mrr, 2),
        'closing_mrr':           round(closing_mrr, 2),
        'new_business':          round(new_biz,     2),
        'expansion':             round(exp,         2),
        'contraction':           round(contr,       2),
        'churn':                 round(churn,       2),
        'reactivation':          round(reactiv,     2),
        'net_new_mrr':           round(net_new,     2),
        'arr':                   round(closing_mrr * 12, 2),
        'active_customers':      len(active_emails),
        'new_customers':         new_cust,
        'churned_customers':     len(prev_emails - active_emails),
        'active_subscriptions':  len(active_this),
        'new_subscriptions':     new_sub_cnt,
        'churned_subscriptions': churned_sub_cnt,
        'arpa':                  round(closing_mrr / len(active_emails), 2) if active_emails else 0,
        'mrr_churn_rate':        0.0,
        'net_mrr_churn_rate':    0.0,
        'customer_churn_rate':   0.0,
        'nrr':                   None,
    }

# Derived churn rates + NRR
for i, month in enumerate(all_months[1:], 1):
    prev = all_months[i - 1]
    m, p = monthly_metrics[month], monthly_metrics[prev]
    d_mrr  = p['closing_mrr']
    d_cust = p['active_customers']
    m['mrr_churn_rate']      = round(m['churn'] / d_mrr  * 100, 2) if d_mrr  > 0 else 0
    m['net_mrr_churn_rate']  = round((m['churn'] - m['expansion'] - m['reactivation']) / d_mrr * 100, 2) if d_mrr > 0 else 0
    m['customer_churn_rate'] = round(m['churned_customers'] / d_cust * 100, 2) if d_cust > 0 else 0
    # NRR = (Opening + Expansion + Reactivation - Contraction - Churn) / Opening * 100
    if m['opening_mrr'] > 0:
        retained = m['opening_mrr'] + m['expansion'] + m['reactivation'] - m['contraction'] - m['churn']
        m['nrr'] = round(retained / m['opening_mrr'] * 100, 2)

# Cap to last COMPLETE month (exclude current partial month)
today = date.today()
if today.month == 1:
    last_complete_month = f'{today.year - 1:04d}-12'
else:
    last_complete_month = f'{today.year:04d}-{today.month - 1:02d}'

months_with_data = [m for m in monthly_metrics if monthly_metrics[m]['closing_mrr'] > 0]
last_actual = max((m for m in months_with_data if m <= last_complete_month), default=last_complete_month)
output_months = {m: v for m, v in monthly_metrics.items()
                 if REPORT_START <= m <= last_actual}
out_list = sorted(output_months.keys())

print(f"  Output range: {out_list[0]} → {out_list[-1]}  ({len(out_list)} months)", flush=True)

# ── Step 4: Plan mix, customers, summary ─────────────────────────────────────
print("[4/4] Building plan mix, customers, summary …", flush=True)

last_month = out_list[-1]

# Plan mix for latest month
plan_mrr   = defaultdict(float)
plan_custs = defaultdict(set)
for sub_id, mrr_map in sub_monthly_mrr.items():
    if last_month in mrr_map:
        prod  = sub_info.get(sub_id, {}).get('product', 'Unknown') or 'Unknown'
        email = sub_info.get(sub_id, {}).get('email', '')
        plan_mrr[prod]   += mrr_map[last_month]
        plan_custs[prod].add(email)

plan_mix = sorted(
    [{'plan': k, 'mrr': round(v, 2), 'customers': len(plan_custs[k])}
     for k, v in plan_mrr.items()],
    key=lambda x: -x['mrr']
)[:15]

# Customer list
email_to_subs = defaultdict(list)
for sub_id, info in sub_info.items():
    ident = info.get('email') or sub_id   # fall back to sub_id if no email
    email_to_subs[ident].append(sub_id)

customer_list = []
for email, sub_ids in email_to_subs.items():
    all_mrr     = {}
    plan_by_m   = {}
    for sub_id in sub_ids:
        prod = (sub_info[sub_id].get('product') or 'Unknown').strip() or 'Unknown'
        for m, mrr in sub_monthly_mrr[sub_id].items():
            if REPORT_START <= m <= last_month:
                all_mrr[m] = round(all_mrr.get(m, 0.0) + mrr, 4)
                if m not in plan_by_m:
                    plan_by_m[m] = prod
    if not all_mrr:
        continue
    sorted_m = sorted(all_mrr.keys())
    customer_list.append({
        'email':         email,
        'plan':          plan_by_m.get(sorted_m[-1], 'Unknown'),
        'status':        'active' if last_month in all_mrr else 'churned',
        'first_month':   sorted_m[0],
        'last_active':   sorted_m[-1],
        'current_mrr':   round(all_mrr.get(last_month, 0.0), 2),
        'peak_mrr':      round(max(all_mrr.values()), 2),
        'total_paid':    round(sum(all_mrr.values()), 2),
        'months_active': len(sorted_m),
        'mrr_history':   {m: round(v, 2) for m, v in all_mrr.items()},
    })

customer_list.sort(key=lambda x: (0 if x['status'] == 'active' else 1,
                                   -x['current_mrr'], -x['total_paid']))
print(f"  Customers with in-range activity: {len(customer_list):,}", flush=True)

# Summary
lm          = output_months[last_month]
non_zero_cr = [v['mrr_churn_rate'] for v in output_months.values() if v['mrr_churn_rate'] > 0]
avg_churn   = sum(non_zero_cr) / len(non_zero_cr) if non_zero_cr else 1
ltv         = lm['arpa'] / (avg_churn / 100) if avg_churn > 0 else 0
nrr_values  = [v['nrr'] for v in output_months.values() if v.get('nrr') is not None]
avg_nrr     = round(sum(nrr_values) / len(nrr_values), 2) if nrr_values else 0

summary = {
    'current_mrr':           lm['closing_mrr'],
    'current_arr':           lm['arr'],
    'current_arpa':          lm['arpa'],
    'current_customers':     lm['active_customers'],
    'current_subscriptions': lm['active_subscriptions'],
    'avg_mrr_churn_rate':    round(avg_churn, 2),
    'avg_nrr':               avg_nrr,
    'ltv':                   round(ltv, 2),
    'data_start':            out_list[0],
    'data_end':              last_month,
    'total_months':          len(out_list),
}

output = {
    'summary':         summary,
    'monthly_metrics': list(output_months.values()),
    'plan_mix':        plan_mix,
    'customers':       customer_list,
}

with open(OUTPUT_JSON, 'w') as f:
    json.dump(output, f, indent=2)

# ── Print results ─────────────────────────────────────────────────────────────
print(f"\n{'='*60}", flush=True)
print(f"  OUTPUT: {OUTPUT_JSON}", flush=True)
print(f"  Period: {out_list[0]} → {last_month}", flush=True)
print(f"  Current MRR  : ${lm['closing_mrr']:>10,.2f}", flush=True)
print(f"  Current ARR  : ${lm['arr']:>10,.2f}", flush=True)
print(f"  Active Cust  : {lm['active_customers']:>10,}", flush=True)
print(f"  ARPA         : ${lm['arpa']:>10.2f}", flush=True)
print(f"  Avg Churn    : {avg_churn:>10.2f}%", flush=True)
print(f"  Avg NRR      : {avg_nrr:>10.2f}%", flush=True)
print(f"  LTV          : ${ltv:>10,.2f}", flush=True)
print(f"{'='*60}", flush=True)

# ── Waterfall integrity check ─────────────────────────────────────────────────
print("\n── Waterfall integrity ──", flush=True)
errors = 0
for m in output_months.values():
    expected = (m['opening_mrr'] + m['new_business'] + m['expansion']
                + m['reactivation'] - m['contraction'] - m['churn'])
    if abs(expected - m['closing_mrr']) > 0.20:
        print(f"  ⚠  {m['month']}: expected {expected:.2f}  got {m['closing_mrr']:.2f}  diff={expected-m['closing_mrr']:.2f}")
        errors += 1
if errors == 0:
    print("  ✓ ALL months pass (opening + movements = closing)", flush=True)

print("\n── Monthly summary ──", flush=True)
print(f"  {'Month':<8} {'Closing MRR':>12} {'Cust':>6} {'New $':>9} {'Churn $':>9} {'Churn%':>7} {'Net New':>9}")
print(f"  {'-'*70}")
for m in output_months.values():
    print(f"  {m['month']:<8} ${m['closing_mrr']:>11,.0f} {m['active_customers']:>6,} "
          f"${m['new_business']:>8,.0f} ${m['churn']:>8,.0f} {m['mrr_churn_rate']:>6.1f}% "
          f"${m['net_new_mrr']:>8,.0f}")
