#!/usr/bin/env python3
"""
preprocess_chartmogul.py  —  run once after new ChartMogul exports arrive.
Converts the three raw CSVs into a compact JSON the daily pipeline reads.

Inputs  (data/):
  chartmogul_mrr_per_month.csv
  chartmogul_activities.csv
  chartmogul_mrr_per_plan.csv

Output (data/):
  chartmogul_data.json
"""
import csv, json, collections, re
from pathlib import Path

ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"

# ── Plan name normaliser ──────────────────────────────────────────────────
PLAN_MAP = [
    (re.compile(r'growth.*annual',     re.I), 'Growth Annual'),
    (re.compile(r'growth.*monthly',    re.I), 'Growth Monthly'),
    (re.compile(r'growth.*semi',       re.I), 'Growth Semi-Annual'),
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
    (re.compile(r'credits|addon',      re.I), 'Add-on: Credits'),
    (re.compile(r'unlimited',          re.I), 'Unlimited'),
    (re.compile(r'custom',             re.I), 'Custom'),
    (re.compile(r'captions',           re.I), 'Captions'),
    (re.compile(r'ai inbox',           re.I), 'Add-on: AI Inbox'),
    (re.compile(r'growth',             re.I), 'Growth Monthly'),
]

def norm_plan(name):
    for pat, grp in PLAN_MAP:
        if pat.search(name): return grp
    return 'Other'

# ── Load CSVs ─────────────────────────────────────────────────────────────
print("Loading CSVs...", flush=True)
with open(DATA/"chartmogul_mrr_per_month.csv", encoding='utf-8-sig') as f:
    mrr_rows = list(csv.DictReader(f))
with open(DATA/"chartmogul_activities.csv", encoding='utf-8-sig') as f:
    act_rows = list(csv.DictReader(f))
with open(DATA/"chartmogul_mrr_per_plan.csv", encoding='utf-8-sig') as f:
    plan_rows = list(csv.DictReader(f))
print(f"  MRR/month: {len(mrr_rows):,} customers | Activities: {len(act_rows):,} | Plans: {len(plan_rows):,}")

# ── 1. Closing MRR + customer list per month ──────────────────────────────
date_cols  = [c for c in mrr_rows[0] if c.startswith('20')]
month_keys = [c[:7] for c in date_cols]

monthly = {}
for col, mk in zip(date_cols, month_keys):
    active = []
    for r in mrr_rows:
        try: v = float(r.get(col) or 0)
        except: v = 0.0
        if v > 0:
            active.append({'n': (r.get('customer_name') or r.get('company_name') or r.get('customer_external_ids','')).strip()[:60],
                           'mrr': round(v, 2)})
    total = round(sum(x['mrr'] for x in active), 2)
    monthly[mk] = {
        'mrr':  total,
        'cust': len(active),
        'arpa': round(total / len(active), 2) if active else 0,
        'src':  'chartmogul',
        'top':  sorted(active, key=lambda x: -x['mrr'])[:30]
    }

# ── 2. Movements per month ────────────────────────────────────────────────
TYPES = ['new_biz','expansion','reactivation','contraction','churn']
moves  = collections.defaultdict(lambda: {t: 0.0 for t in TYPES})
movers = collections.defaultdict(lambda: {t: [] for t in TYPES})

for r in act_rows:
    d = r.get('date',''); mk = d[:7]; mt = r.get('movement_type','')
    if not d or mt not in TYPES: continue
    try: amt = float(r.get('mrr_movement_in_account_currency') or 0)
    except: amt = 0.0
    moves[mk][mt] += amt
    movers[mk][mt].append({
        'n':    (r.get('customer_name') or r.get('customer_uuid',''))[:60],
        'desc': r.get('description','')[:80],
        'mrr':  round(abs(amt), 2),
        'dt':   d[:10]
    })

for mk in movers:
    for mt in TYPES:
        movers[mk][mt] = sorted(movers[mk][mt], key=lambda x: -x['mrr'])[:20]

# ── 3. Plan mix per month ─────────────────────────────────────────────────
plan_dcols = [c for c in plan_rows[0] if c.startswith('20')]
plan_mkeys = [c[:7] for c in plan_dcols]
plan_mix   = collections.defaultdict(lambda: collections.defaultdict(float))

for r in plan_rows:
    grp = norm_plan(r.get('plan_name',''))
    for col, mk in zip(plan_dcols, plan_mkeys):
        try: v = float(r.get(col) or 0)
        except: v = 0.0
        if v > 0: plan_mix[mk][grp] += v

# ── 4. Merge & write ─────────────────────────────────────────────────────
all_months = sorted(set(list(monthly) + list(moves)))
out = {}
for mk in all_months:
    base = monthly.get(mk, {'mrr':0,'cust':0,'arpa':0,'src':'chartmogul','top':[]})
    out[mk] = {
        **base,
        'moves':  {t: round(moves[mk][t], 2) for t in TYPES} if mk in moves else {t:0 for t in TYPES},
        'movers': movers.get(mk, {t:[] for t in TYPES}),
        'plans':  {k: round(v, 2) for k, v in sorted(plan_mix[mk].items(), key=lambda x:-x[1])},
    }

outpath = DATA / "chartmogul_data.json"
with open(outpath, 'w') as f:
    json.dump(out, f, separators=(',',':'))

print(f"\n✓ {outpath}  ({outpath.stat().st_size/1024:.0f} KB)  —  {min(out)} → {max(out)}")
