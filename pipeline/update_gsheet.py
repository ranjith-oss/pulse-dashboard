#!/usr/bin/env python3
"""
Writes mrr_data.json → Google Sheet
Tabs: Summary | Monthly Breakdown | MRR Waterfall | Plan Mix | Churn Analysis | Growth Metrics

Env vars required:
  GOOGLE_SERVICE_ACCOUNT_JSON  — full JSON string of the service account key
  GOOGLE_SHEET_ID              — spreadsheet ID (from the URL)
"""

import json, os, sys, time
from pathlib import Path

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install',
                           'gspread', 'google-auth', '-q'])
    import gspread
    from google.oauth2.service_account import Credentials

BASE     = Path(__file__).parent
DATA     = BASE / 'mrr_data.json'
SHEET_ID = os.environ['GOOGLE_SHEET_ID']
SA_JSON  = os.environ['GOOGLE_SERVICE_ACCOUNT_JSON']

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

# ── Auth ──────────────────────────────────────────────────────────────────────
print("[1/3] Authenticating with Google Sheets …", flush=True)
sa_info = json.loads(SA_JSON)
creds   = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
gc      = gspread.authorize(creds)
sh      = gc.open_by_key(SHEET_ID)
print(f"      Opened: {sh.title}", flush=True)

# ── Load data ─────────────────────────────────────────────────────────────────
with open(DATA) as f:
    raw = json.load(f)

months  = raw['monthly_metrics']
summary = raw['summary']
pmix    = raw['plan_mix']

def fmt_usd(v):
    return round(float(v), 2) if v else 0.0

def get_or_create(name):
    try:
        return sh.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=name, rows=200, cols=30)

def write_sheet(ws, rows, bold_header=True):
    """Clear and batch-write all rows. rows[0] = header."""
    ws.clear()
    time.sleep(0.5)   # avoid quota
    ws.update('A1', rows, value_input_option='USER_ENTERED')
    if bold_header and rows:
        ws.format('1:1', {'textFormat': {'bold': True},
                          'backgroundColor': {'red': 0.388, 'green': 0.400, 'blue': 0.945}})
    print(f"      ✓ {ws.title}: {len(rows)-1} data rows", flush=True)

print("[2/3] Writing tabs …", flush=True)

# ── Tab 1: Summary ────────────────────────────────────────────────────────────
ws0 = get_or_create('Summary')
sum_rows = [['Metric', 'Value'], [
    'Current MRR',          fmt_usd(summary['current_mrr'])], [
    'Current ARR',          fmt_usd(summary['current_arr'])], [
    'Current ARPA',         fmt_usd(summary['current_arpa'])], [
    'Active Customers',     summary['current_customers']], [
    'Active Subscriptions', summary['current_subscriptions']], [
    'Avg MRR Churn Rate %', summary['avg_mrr_churn_rate']], [
    'Avg NRR %',            summary.get('avg_nrr', 0)], [
    'Est. LTV',             fmt_usd(summary['ltv'])], [
    'Data Period',          f"{summary['data_start']} → {summary['data_end']}"], [
    'Total Months',         summary['total_months']],
]
write_sheet(ws0, sum_rows)

# ── Tab 2: Monthly Breakdown ──────────────────────────────────────────────────
ws1 = get_or_create('Monthly Breakdown')
mb_header = ['Month', 'Opening MRR', 'New Business', 'Expansion', 'Reactivation',
             'Contraction', 'Churn', 'Net New MRR', 'Closing MRR', 'ARR',
             'Active Customers', 'New Customers', 'Churned Customers', 'ARPA',
             'MoM Growth %', 'MRR Churn %', 'NRR %']
mb_rows = [mb_header]
for i, m in enumerate(months):
    prev = months[i-1] if i > 0 else None
    mom  = round((m['closing_mrr'] - prev['closing_mrr']) / prev['closing_mrr'] * 100, 2) \
           if prev and prev['closing_mrr'] else 0
    mb_rows.append([
        m['month'],
        fmt_usd(m['opening_mrr']),    fmt_usd(m['new_business']),
        fmt_usd(m['expansion']),       fmt_usd(m['reactivation']),
        fmt_usd(m['contraction']),     fmt_usd(m['churn']),
        fmt_usd(m['net_new_mrr']),     fmt_usd(m['closing_mrr']),
        fmt_usd(m['arr']),
        m['active_customers'],         m['new_customers'],
        m['churned_customers'],        fmt_usd(m['arpa']),
        mom,                           round(m['mrr_churn_rate'], 2),
        round(m['nrr'], 2) if m.get('nrr') is not None else '',
    ])
write_sheet(ws1, mb_rows)

# ── Tab 3: MRR Waterfall ──────────────────────────────────────────────────────
ws2 = get_or_create('MRR Waterfall')
wf_header = ['Month', 'Opening MRR', 'New Business', 'Expansion', 'Reactivation',
             'Contraction', 'Churn', 'Net New MRR', 'Closing MRR']
wf_rows = [wf_header]
for m in months:
    wf_rows.append([
        m['month'],
        fmt_usd(m['opening_mrr']),  fmt_usd(m['new_business']),
        fmt_usd(m['expansion']),     fmt_usd(m['reactivation']),
        fmt_usd(m['contraction']),   fmt_usd(m['churn']),
        fmt_usd(m['net_new_mrr']),   fmt_usd(m['closing_mrr']),
    ])
write_sheet(ws2, wf_rows)

# ── Tab 4: Plan Mix ───────────────────────────────────────────────────────────
ws3 = get_or_create('Plan Mix')
pm_header = ['Plan', 'MRR', 'Customers', 'MRR per Customer', '% of Total MRR']
pm_rows   = [pm_header]
total_pm  = sum(p['mrr'] for p in pmix)
for p in pmix:
    mrr_pc = round(p['mrr'] / p['customers'], 2) if p['customers'] else 0
    share  = round(p['mrr'] / total_pm * 100, 2) if total_pm else 0
    pm_rows.append([p['plan'], fmt_usd(p['mrr']), p['customers'], mrr_pc, share])
write_sheet(ws3, pm_rows)

# ── Tab 5: Churn Analysis ─────────────────────────────────────────────────────
ws4 = get_or_create('Churn Analysis')
ca_header = ['Month', 'Active Customers', 'New Customers', 'Churned Customers',
             'MRR Churn %', 'Net MRR Churn %', 'Customer Churn %',
             'Churn MRR', 'New Biz MRR']
ca_rows = [ca_header]
for m in months:
    ca_rows.append([
        m['month'],
        m['active_customers'],     m['new_customers'],
        m['churned_customers'],
        round(m['mrr_churn_rate'], 2),
        round(m['net_mrr_churn_rate'], 2),
        round(m['customer_churn_rate'], 2),
        fmt_usd(m['churn']),       fmt_usd(m['new_business']),
    ])
write_sheet(ws4, ca_rows)

# ── Tab 6: Growth Metrics ─────────────────────────────────────────────────────
ws5 = get_or_create('Growth Metrics')
gm_header = ['Month', 'Opening MRR', 'Closing MRR', 'MoM Growth %',
             'ARR', 'ARPA', 'NRR %', 'Active Customers', 'New Customers', 'Active Subscriptions']
gm_rows = [gm_header]
for i, m in enumerate(months):
    prev = months[i-1] if i > 0 else None
    mom  = round((m['closing_mrr'] - prev['closing_mrr']) / prev['closing_mrr'] * 100, 2) \
           if prev and prev['closing_mrr'] else 0
    gm_rows.append([
        m['month'],
        fmt_usd(m['opening_mrr']),  fmt_usd(m['closing_mrr']),
        mom,                         fmt_usd(m['arr']),
        fmt_usd(m['arpa']),
        round(m['nrr'], 2) if m.get('nrr') is not None else '',
        m['active_customers'],
        m['new_customers'],          m['active_subscriptions'],
    ])
write_sheet(ws5, gm_rows)

print(f"\n[3/3] All tabs updated ✓", flush=True)
print(f"      Sheet URL: https://docs.google.com/spreadsheets/d/{SHEET_ID}", flush=True)
