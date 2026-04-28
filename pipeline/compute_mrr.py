#!/usr/bin/env python3
"""
compute_mrr.py  —  Unified MRR engine  v3
═══════════════════════════════════════════════════════════════════════════
SOURCE OF TRUTH:
  ≤ 2026-02  →  ChartMogul (data/chartmogul_data.json)
               Pre-processed from ChartMogul CSV exports.
               Handles legacy billing, migration artefacts, annual
               proration — all the things raw Paddle transactions miss.

  ≥ 2026-03  →  Paddle Reports API  (pipeline/transaction_line_items.csv)
               Fetched daily by fetch_paddle_report.py.
               Uses subtotal − discount (pre-tax, pre-commission).

WHY PADDLE-ONLY WAS WRONG
  ─────────────────────────────────────────────────────────────────────
  Period          Paddle    ChartMogul   Gap    Root cause
  ─────────────────────────────────────────────────────────────────────
  Jan-25          $88,998  $132,047  -$43k  Legacy billing missing
  Aug-25         $109,056  $108,924   +$0   Crossover (all migrated)
  Feb-26         $113,397   $95,693  +$18k  Annual period double-count
  ─────────────────────────────────────────────────────────────────────
  1. Jan-Aug 2025: Paddle only sees transactions from Dec-2024 onwards.
     Customers on the legacy billing system (pre-Paddle) were not
     represented until their subscriptions renewed on Paddle.
  2. Sep 2025+: All customers now on Paddle, but annual plan billing
     causes a different overcounting problem — when an annual renewal
     falls near a month boundary, the billing-period detection assigns
     the same subscription to two overlapping months, doubling ~$15k.
  ChartMogul ingests both legacy + Paddle events and resolves conflicts
  using subscription state (not transaction events), giving correct MRR.

FIX in v3:
  The key bug was using billing_period_starts_at as the ONLY month anchor.
  This missed all active annual subscriptions that started before March 2026
  but were still active in March/April 2026 (e.g., an annual sub renewed
  in Nov 2025 covers Nov 2025 – Nov 2026, contributing MRR to every month
  in that range including March–November 2026).

  Correct approach: distribute MRR across ALL months covered by billing_period
  (from p_start to p_end), filtered to ≥ PADDLE_START.
═══════════════════════════════════════════════════════════════════════════
"""
import csv, json, sys, re, os
from pathlib import Path
from datetime import date, datetime
from collections import defaultdict
from datetime import date as _date

# Today and current month — defined early so available throughout all parts
today = _date.today()
current_month = f"{today.year:04d}-{today.month:02d}"

ROOT   = Path(__file__).parent.parent
DATA   = ROOT / "data"
PIPE   = Path(__file__).parent
# CM_CUT auto-detected from chartmogul_data.json (last available month)
# PADDLE_START auto-set to the month AFTER last ChartMogul month
REPORT_START = '2022-10'   # earliest month to include in output

# ════════════════════════════════════════════════════════════════════════
# PART 1 — ChartMogul historical data (≤ CM_CUT)
# ════════════════════════════════════════════════════════════════════════
print("[1/3] Loading ChartMogul data …", flush=True)
cm_path = DATA / "chartmogul_data.json"
if not cm_path.exists():
    sys.exit(f"ERROR: {cm_path} not found. Run pipeline/preprocess_chartmogul.py first.")

with open(cm_path) as f:
    cm = json.load(f)

# SOURCE OF TRUTH BOUNDARY
# ChartMogul CSV  → up to and including Feb 2026 (clean, authoritative)
# Paddle API      → March 2026 onwards
#   Current month : Paddle Subscriptions API (GET /subscriptions?status=active)
#   Prior complete: Paddle transaction CSV with billing-period fixes
CM_CUT       = '2026-02'
PADDLE_START = '2026-03'
def next_month(mk):
    y, m = int(mk[:4]), int(mk[5:7])
    m += 1
    if m > 12: y += 1; m = 1
    return f'{y:04d}-{m:02d}'
print(f"  ChartMogul cutoff: {CM_CUT}  |  Paddle starts: {PADDLE_START}", flush=True)

# Build monthly_metrics dict from ChartMogul
monthly_metrics = {}
for mk, rec in cm.items():
    if mk > CM_CUT: continue
    monthly_metrics[mk] = {
        'closing_mrr':   rec['mrr'],
        'customers':     rec['cust'],
        'arpa':          rec['arpa'],
        'new_biz':       rec['moves'].get('new_biz', 0),
        'expansion':     rec['moves'].get('expansion', 0),
        'reactivation':  rec['moves'].get('reactivation', 0),
        'contraction':   rec['moves'].get('contraction', 0),
        'churn':         rec['moves'].get('churn', 0),
        'plan_mix':      rec.get('plans', {}),
        'top_movers':    rec.get('movers', {}),
        'source':        'chartmogul',
    }

print(f"  ChartMogul months loaded: {len(monthly_metrics)}  ({min(monthly_metrics)} → {max(monthly_metrics)})", flush=True)

# ════════════════════════════════════════════════════════════════════════
# PART 2 — Paddle data  (≥ PADDLE_START)
# ════════════════════════════════════════════════════════════════════════
print("[2/3] Loading Paddle transaction data …", flush=True)

csv_path = PIPE / "transaction_line_items.csv"
if not csv_path.exists():
    sys.exit(f"ERROR: {csv_path} not found. Run fetch_paddle_report.py first.")

def parse_date(s):
    if not s or not s.strip(): return None
    s = s.strip()[:19]
    for fmt in ('%Y-%m-%dT%H:%M:%S','%Y-%m-%d %H:%M:%S','%Y-%m-%d'):
        try: return datetime.strptime(s[:len(fmt)], fmt[:len(s)]).date()
        except: pass
    try: return datetime.fromisoformat(s.replace('Z','+00:00')).date()
    except: return None

def add_months(d, n):
    """Add n months to a date object."""
    m = d.month - 1 + n
    y = d.year + m // 12
    m = m % 12 + 1
    import calendar
    day = min(d.day, calendar.monthrange(y, m)[1])
    return date(y, m, day)

def months_in_range(p_start, p_end):
    """Yield YYYY-MM strings for all months that overlap [p_start, p_end)."""
    curr = date(p_start.year, p_start.month, 1)
    # end is exclusive: if p_end is exactly a month boundary, don't include that month
    # but if p_end falls mid-month, include that month
    while True:
        mk = f"{curr.year:04d}-{curr.month:02d}"
        # Check overlap: curr_month starts at curr, ends at start of next month
        if curr.month == 12:
            next_month = date(curr.year + 1, 1, 1)
        else:
            next_month = date(curr.year, curr.month + 1, 1)
        # Overlap condition: curr < p_end AND next_month > p_start
        if curr >= p_end:
            break
        yield mk
        curr = next_month

with open(csv_path, encoding='utf-8') as f:
    raw = list(csv.DictReader(f))

print(f"  Loaded {len(raw):,} rows", flush=True)

# Pre-build all billing periods per subscription (needed for churn detection)
# For annual subs expiring in a given month with NO subsequent renewal,
# that sub has churned — we must not count it.
sub_all_periods = defaultdict(list)
for _row in raw:
    if _row.get('transaction_status','').lower() != 'completed': continue
    if not _row.get('subscription_id'): continue
    _ps = parse_date(_row.get('billing_period_starts_at',''))
    _pe = parse_date(_row.get('billing_period_ends_at',''))
    if not _ps or not _pe: continue
    _email = _row.get('customer_email','').lower().strip()
    _sid   = _row.get('subscription_id','')
    sub_all_periods[f"{_email}|{_sid}"].append((_ps, _pe))

def has_renewal_after(key2, period_end):
    """True if the sub has any billing period starting on/after period_end."""
    return any(s >= period_end for s, e in sub_all_periods[key2])

# Per-subscription-per-month MRR accumulator
# sub_month_mrr[email|sub_id][YYYY-MM] = mrr_contribution
sub_month_mrr  = defaultdict(lambda: defaultdict(float))
sub_month_plan = defaultdict(lambda: defaultdict(str))   # plan per sub per month
seen_dedup = set()
skipped = 0

for row in raw:
    if row.get('transaction_status','').lower() != 'completed': continue
    if not row.get('subscription_id'):                          continue

    # Dedup by transaction_id + price_id + billing_start (same line item)
    key = (row['transaction_id'], row.get('price_id',''),
           row.get('subtotal',''), row.get('billing_period_starts_at',''))
    if key in seen_dedup: skipped += 1; continue
    seen_dedup.add(key)

    try:
        subtotal = float(row['subtotal'] or 0)
        discount = float(row.get('discount') or 0)
    except: continue
    if subtotal <= 0: continue
    net_local = subtotal - discount
    if net_local <= 0: continue

    try:
        rate = float(row.get('transaction_to_balance_currency_exchange_rate') or 1)
        if rate <= 0: rate = 1.0
    except: rate = 1.0

    net_usd = net_local * rate

    billed  = (parse_date(row.get('transaction_billed_at',''))
               or parse_date(row.get('completed_at',''))
               or parse_date(row.get('transaction_created_at','')))
    p_start = parse_date(row.get('billing_period_starts_at',''))
    p_end   = parse_date(row.get('billing_period_ends_at',''))

    # Determine billing cycle (months)
    freq  = row.get('billing_cycle_frequency','1').strip() or '1'
    intvl = row.get('billing_cycle_interval','month').strip().lower() or 'month'
    try: freq_n = int(float(freq))
    except: freq_n = 1
    if intvl in ('year','yearly','annual'): cycle_months = 12 * freq_n
    elif intvl in ('month','monthly'):      cycle_months = freq_n
    elif intvl in ('week','weekly'):        cycle_months = 1
    else:                                   cycle_months = 1

    # Auto-detect annual from billing period length if cycle_months==1
    if p_start and p_end and cycle_months == 1:
        days = (p_end - p_start).days
        if days > 60:
            cycle_months = max(1, round(days / 30.44))

    mrr_contribution = net_usd / max(cycle_months, 1)

    email  = row.get('customer_email','unknown').lower().strip()
    sub_id = row.get('subscription_id','')
    key2   = f"{email}|{sub_id}"
    plan   = row.get('price_description','') or row.get('product_name','')

    # ── Assign MRR to the correct month(s) ────────────────────────────────
    # RULE:
    #   Monthly subs  → anchor to p_start month ONLY.
    #     If the subscription renews, a new transaction exists for the next month.
    #     If it doesn't renew, the customer churned — we must not carry MRR forward
    #     just because the billing period technically crosses the month boundary.
    #     (This was the root cause of the ~$15k Paddle overcount vs ChartMogul.)
    #
    #   Annual subs   → distribute MRR across ALL months in the billing period.
    #     An annual charge paid in Nov 2025 covers Nov 2025 – Oct 2026 and must
    #     show its prorated MRR ($X/12) in each of those 12 months.
    #
    # cycle_months == 1 → monthly; cycle_months > 1 → annual/multi-month

    if cycle_months == 1:
        # Monthly: anchor to p_start (or billed date if no period)
        anchor_date = p_start or billed
        if anchor_date:
            mk = f"{anchor_date.year:04d}-{anchor_date.month:02d}"
            if mk >= PADDLE_START:
                if mrr_contribution > sub_month_mrr[key2][mk]:
                    sub_month_mrr[key2][mk] = mrr_contribution
                    sub_month_plan[key2][mk] = plan
    else:
        # Annual/multi-month: distribute across all months in billing period
        if p_start and p_end and p_end > p_start:
            for mk in months_in_range(p_start, p_end):
                if mk < PADDLE_START: continue
                # For COMPLETED months: skip if this annual sub's billing period
                # expires THIS month and there is no renewal. That means the
                # customer churned — ChartMogul stops counting them at churn.
                # (For the current month, skip this check — Subscriptions API
                # will override anyway, and mid-month renewals may not exist yet.)
                if mk < current_month:
                    period_end_month = f"{p_end.year:04d}-{p_end.month:02d}"
                    if period_end_month == mk and not has_renewal_after(key2, p_end):
                        continue  # churned annual sub — exclude
                if mrr_contribution > sub_month_mrr[key2][mk]:
                    sub_month_mrr[key2][mk] = mrr_contribution
                    sub_month_plan[key2][mk] = plan
        elif billed:
            mk = f"{billed.year:04d}-{billed.month:02d}"
            if mk >= PADDLE_START:
                if mrr_contribution > sub_month_mrr[key2][mk]:
                    sub_month_mrr[key2][mk] = mrr_contribution
                    sub_month_plan[key2][mk] = plan

print(f"  Dedup skipped: {skipped}. Active sub-month entries computed.", flush=True)

# ── Plan normalisation ────────────────────────────────────────────────────
PLAN_MAP_RE = [
    (re.compile(r'growth.*annual',     re.I), 'Growth Annual'),
    (re.compile(r'growth.*monthly',    re.I), 'Growth Monthly'),
    (re.compile(r'pro\+.*annual',      re.I), 'PRO+ Annual'),
    (re.compile(r'pro\+.*monthly',     re.I), 'PRO+ Monthly'),
    (re.compile(r'pro.*annual',        re.I), 'PRO Annual'),
    (re.compile(r'pro.*monthly',       re.I), 'PRO Monthly'),
    (re.compile(r'essential.*annual',  re.I), 'Essential Annual'),
    (re.compile(r'essential.*monthly', re.I), 'Essential Monthly'),
    (re.compile(r'lite.*annual',       re.I), 'Lite Annual'),
    (re.compile(r'lite.*monthly',      re.I), 'Lite Monthly'),
    (re.compile(r'plus.*annual',       re.I), 'Plus Annual'),
    (re.compile(r'plus.*monthly',      re.I), 'Plus Monthly'),
    (re.compile(r'starter.*annual',    re.I), 'Starter Annual'),
    (re.compile(r'starter.*monthly',   re.I), 'Starter Monthly'),
    (re.compile(r'select',             re.I), 'Select'),
    (re.compile(r'team member',        re.I), 'Add-on: Team Member'),
    (re.compile(r'storage',            re.I), 'Add-on: Storage'),
    (re.compile(r'social sets',        re.I), 'Add-on: Social Sets'),
    (re.compile(r'credit',             re.I), 'Add-on: Credits'),
    (re.compile(r'growth',             re.I), 'Growth Monthly'),
]
def norm_plan(name):
    for pat, grp in PLAN_MAP_RE:
        if pat.search(name): return grp
    return 'Other'

# ── Aggregate Paddle months ───────────────────────────────────────────────

paddle_months = defaultdict(lambda: {'mrr':0.0,'subs':set(),'plans':defaultdict(float)})

for key2, month_data in sub_month_mrr.items():
    for mk, mrr in month_data.items():
        if mk < PADDLE_START or mk > current_month: continue
        plan_raw = sub_month_plan[key2].get(mk, '')
        plan     = norm_plan(plan_raw)
        paddle_months[mk]['mrr']   += mrr
        paddle_months[mk]['subs'].add(key2)
        paddle_months[mk]['plans'][plan] += mrr

sorted_paddle_months = sorted(paddle_months)
print(f"  Paddle months: {sorted_paddle_months[0] if sorted_paddle_months else 'none'} → {sorted_paddle_months[-1] if sorted_paddle_months else 'none'}", flush=True)

# ── Movements for Paddle months (per-subscription waterfall) ─────────────
for i, mk in enumerate(sorted_paddle_months):
    curr_subs = {k2: sub_month_mrr[k2][mk] for k2 in paddle_months[mk]['subs']}

    new_biz = expansion = reactivation = contraction = churn = 0.0

    if i == 0:
        # First Paddle month: all active subs treated as new (can't match to ChartMogul by email)
        new_biz = sum(curr_subs.values())
    else:
        prev_mk   = sorted_paddle_months[i-1]
        prev_subs = {k2: sub_month_mrr[k2][prev_mk] for k2 in paddle_months[prev_mk]['subs']}

        for k2, curr_v in curr_subs.items():
            prev_v = prev_subs.get(k2, 0)
            if prev_v == 0:
                new_biz += curr_v
            elif curr_v > prev_v + 0.01:
                expansion += curr_v - prev_v
            elif curr_v < prev_v - 0.01:
                contraction += prev_v - curr_v
        for k2, prev_v in prev_subs.items():
            if k2 not in curr_subs:
                churn += prev_v

    total_mrr = round(paddle_months[mk]['mrr'], 2)
    subs_count = len(paddle_months[mk]['subs'])
    plans = {k: round(v,2) for k,v in
             sorted(paddle_months[mk]['plans'].items(), key=lambda x:-x[1])}

    monthly_metrics[mk] = {
        'closing_mrr':  total_mrr,
        'customers':    subs_count,
        'arpa':         round(total_mrr / subs_count, 2) if subs_count else 0,
        'new_biz':      round(new_biz, 2),
        'expansion':    round(expansion, 2),
        'reactivation': round(reactivation, 2),
        'contraction':  round(-contraction, 2),
        'churn':        round(-churn, 2),
        'plan_mix':     plans,
        'top_movers':   {},
        'source':       'paddle',
    }

print(f"  Paddle months computed: {len(sorted_paddle_months)}  ({sorted_paddle_months[0] if sorted_paddle_months else 'n/a'} → {sorted_paddle_months[-1] if sorted_paddle_months else 'n/a'})", flush=True)

# ════════════════════════════════════════════════════════════════════════
# PART 2b — Override CURRENT MONTH with active subscriptions snapshot
# ════════════════════════════════════════════════════════════════════════
# For the current (live) month, use the Paddle Subscriptions API snapshot.
# This is the authoritative source — same approach as ChartMogul:
#   active subscription × recurring price = MRR
# It avoids the "billing period crossing" overcount that plagues
# the transaction-data approach for the current in-progress month.
active_subs_path = PIPE / 'active_subscriptions.json'
if active_subs_path.exists():
    with open(active_subs_path) as f:
        active_data = json.load(f)
    live_mrr   = active_data.get('total_mrr', 0)
    live_subs  = active_data.get('active_subs', 0)
    live_plans = active_data.get('plan_mrr', {})
    # Override current month entry (or create it if no Paddle transaction yet)
    if live_mrr > 0:
        prev_mk = sorted_paddle_months[-1] if sorted_paddle_months else CM_CUT
        prev    = monthly_metrics.get(prev_mk, {})
        prev_mrr= prev.get('closing_mrr', 0)
        # Compute approximate movements (will be refined when month closes)
        delta = live_mrr - prev_mrr
        monthly_metrics[current_month] = {
            'closing_mrr':  round(live_mrr, 2),
            'customers':    live_subs,
            'arpa':         round(live_mrr / live_subs, 2) if live_subs else 0,
            'new_biz':      round(max(0, delta), 2),
            'expansion':    0,
            'reactivation': 0,
            'contraction':  0,
            'churn':        round(min(0, delta), 2),
            'plan_mix':     {k: round(v,2) for k,v in
                            sorted(live_plans.items(), key=lambda x:-x[1])},
            'top_movers':   {},
            'source':       'paddle_live',
        }
        print(f"  Active subs snapshot: {live_subs:,} subs, MRR=${live_mrr:,.0f}  (overrides {current_month})", flush=True)
else:
    print("  No active_subscriptions.json found — using transaction data for current month", flush=True)

# ════════════════════════════════════════════════════════════════════════
# PART 3 — Assemble output JSON
# ════════════════════════════════════════════════════════════════════════
print("[3/3] Building mrr_data.json …", flush=True)

output_months = {mk: v for mk, v in monthly_metrics.items()
                 if mk >= REPORT_START and mk <= current_month and v['closing_mrr'] > 0}

sorted_months = sorted(output_months)
if not sorted_months:
    sys.exit("ERROR: no months with data")

# ── Summary stats ──────────────────────────────────────────────────────
last_mk = sorted_months[-1]
last    = output_months[last_mk]

# Average NRR (trailing 12 months) — expansion + contraction / opening MRR
trailing = sorted_months[-12:]
nrr_vals = []
for mk in trailing[1:]:
    rec = output_months[mk]
    idx = sorted_months.index(mk)
    p   = output_months.get(sorted_months[idx-1], {})
    o_mrr = p.get('closing_mrr', 0)
    if o_mrr > 0:
        retained = o_mrr + rec.get('expansion',0) + rec.get('contraction',0)
        nrr_vals.append(retained / o_mrr)
avg_nrr = round(sum(nrr_vals)/len(nrr_vals)*100, 1) if nrr_vals else 0

# Quick Ratio (trailing 12m)
qr_num = qr_den = 0.0
for mk in trailing:
    rec = output_months[mk]
    qr_num += rec.get('new_biz',0) + rec.get('expansion',0) + rec.get('reactivation',0)
    qr_den += abs(rec.get('churn',0)) + abs(rec.get('contraction',0))
quick_ratio = round(qr_num / qr_den, 2) if qr_den else 0

# Peak MRR
peak_mk  = max(sorted_months, key=lambda m: output_months[m]['closing_mrr'])
peak_mrr = output_months[peak_mk]['closing_mrr']

# MRR growth first→last
first_mrr = output_months[sorted_months[0]]['closing_mrr']
mrr_growth_pct = round((last['closing_mrr']/first_mrr - 1)*100, 1) if first_mrr else 0

# ── Monthly waterfall list ─────────────────────────────────────────────
monthly_list = []
for mk in sorted_months:
    rec = output_months[mk]
    monthly_list.append({
        'month':        mk,
        'mrr':          rec['closing_mrr'],
        'customers':    rec['customers'],
        'arpa':         rec['arpa'],
        'new_biz':      round(rec.get('new_biz',0), 2),
        'expansion':    round(rec.get('expansion',0), 2),
        'reactivation': round(rec.get('reactivation',0), 2),
        'contraction':  round(rec.get('contraction',0), 2),
        'churn':        round(rec.get('churn',0), 2),
        'net_new':      round(
            rec.get('new_biz',0) + rec.get('expansion',0) + rec.get('reactivation',0) +
            rec.get('contraction',0) + rec.get('churn',0), 2),
        'plan_mix':     rec.get('plan_mix', {}),
        'top_movers':   rec.get('top_movers', {}),
        'source':       rec.get('source','chartmogul'),
    })

# Plan trend
all_plans = sorted({p for m in monthly_list for p in m['plan_mix']},
                   key=lambda p: -output_months[last_mk]['plan_mix'].get(p,0))

plan_trend = {}
for plan in all_plans:
    plan_trend[plan] = {m['month']: m['plan_mix'].get(plan, 0) for m in monthly_list}

output = {
    'summary': {
        'current_mrr':      last['closing_mrr'],
        'current_arr':      round(last['closing_mrr'] * 12, 2),
        'current_arpa':     last['arpa'],
        'active_customers': last['customers'],
        'avg_nrr':          avg_nrr,
        'quick_ratio':      quick_ratio,
        'peak_mrr':         peak_mrr,
        'peak_month':       peak_mk,
        'mrr_growth_pct':   mrr_growth_pct,
        'data_start':       sorted_months[0],
        'data_end':         sorted_months[-1],
        'total_months':     len(sorted_months),
        'last_updated':     today.isoformat(),
        'source_note':      f'ChartMogul ≤{CM_CUT}, Paddle ≥{PADDLE_START} (auto-detected)',
    },
    'monthly':    monthly_list,
    'plan_trend': plan_trend,
    'all_plans':  all_plans,
}

out_path = PIPE / "mrr_data.json"
with open(out_path, 'w') as f:
    json.dump(output, f, separators=(',',':'))

print(f"\n{'='*60}", flush=True)
print(f"  Output : {out_path}", flush=True)
print(f"  Period : {sorted_months[0]} → {sorted_months[-1]}  ({len(sorted_months)} months)", flush=True)
print(f"  Current MRR    : ${last['closing_mrr']:,.0f}", flush=True)
print(f"  Current ARR    : ${last['closing_mrr']*12:,.0f}", flush=True)
print(f"  Active Cust    : {last['customers']:,}", flush=True)
print(f"  ARPA           : ${last['arpa']:,.2f}", flush=True)
print(f"  Avg NRR (12m)  : {avg_nrr:.1f}%", flush=True)
print(f"  Quick Ratio    : {quick_ratio:.2f}", flush=True)
print(f"  Peak MRR       : ${peak_mrr:,.0f}  ({peak_mk})", flush=True)
print(f"{'='*60}", flush=True)

print("\n--- Monthly summary ---", flush=True)
print(f"{'Month':<10} {'MRR':>10} {'Cust':>6} {'New$':>9} {'Exp$':>9} {'Churn$':>9} {'Net$':>9} {'Src':>5}", flush=True)
print("-"*72, flush=True)
for rec in monthly_list:
    print(f"{rec['month']:<10} ${rec['mrr']:>9,.0f} {rec['customers']:>6,} "
          f"${rec['new_biz']:>8,.0f} ${rec['expansion']:>8,.0f} "
          f"${rec['churn']:>8,.0f} ${rec['net_new']:>8,.0f} "
          f"{rec['source'][:4]:>5}", flush=True)

print("\n--- Plan mix (latest month) ---", flush=True)
for plan, mrr in sorted(last['plan_mix'].items(), key=lambda x:-x[1])[:15]:
    print(f"  {plan:<35} ${mrr:>8,.0f}", flush=True)
