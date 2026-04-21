#!/usr/bin/env python3
"""
Pulse - Revenue Intelligence Dashboard by quso.ai
Run: python3 build_pulse_dashboard.py
     python3 build_pulse_dashboard.py --recompute
"""

import json, sys, subprocess
from pathlib import Path
from datetime import datetime

DATA_FILE = Path(__file__).parent / 'mrr_data.json'
OUTPUT    = Path(__file__).parent.parent / 'index.html'

if '--recompute' in sys.argv:
    print("  Recomputing MRR data...")
    r = subprocess.run([sys.executable, str(Path(__file__).parent / 'compute_mrr.py')])
    if r.returncode != 0: sys.exit(1)

with open(DATA_FILE) as f:
    raw = json.load(f)

# Convert mrr_history dict -> compact array indexed by month position
months_order = [m['month'] for m in raw['monthly_metrics']]
for cust in raw.get('customers', []):
    h = cust.pop('mrr_history', {})
    cust['h'] = [round(h.get(m, 0), 2) for m in months_order]

data_json    = json.dumps(raw, separators=(',', ':'))
data_json    = data_json.replace('</', '<\\/')
generated_at = datetime.now().strftime('%Y-%m-%d %H:%M')
data_start   = raw['summary']['data_start']
data_end     = raw['summary']['data_end']
cur_mrr      = '${:,.0f}'.format(raw['summary']['current_mrr'])
cur_arr      = '${:,.0f}'.format(raw['summary']['current_arr'])
n_customers  = len(raw.get('customers', []))

# Unique plans for filter dropdown
plans_set = sorted(set(c['plan'] for c in raw.get('customers', [])))
plans_json = json.dumps(plans_set)

# ---- CSS -----------------------------------------------------------------------
CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#07090f;--bg2:#0c1220;
  --sf:rgba(255,255,255,0.03);--sf2:rgba(255,255,255,0.06);--sf3:rgba(255,255,255,0.09);
  --bd:rgba(255,255,255,0.08);--bd2:rgba(255,255,255,0.05);
  --tx:#e2e8f0;--mu:#64748b;--fa:#334155;
  --ac:#6366f1;--ac2:rgba(99,102,241,0.12);--ac3:rgba(99,102,241,0.06);
  --gn:#10b981;--rd:#f43f5e;--am:#f59e0b;--pu:#8b5cf6;--tl:#06b6d4;
}
html{scroll-behavior:smooth}
body{background:var(--bg);color:var(--tx);
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;
  font-size:14px;line-height:1.5;-webkit-font-smoothing:antialiased;min-height:100vh;
  background-image:
    radial-gradient(ellipse 90% 50% at 50% -10%,rgba(99,102,241,0.09),transparent 60%),
    radial-gradient(ellipse 50% 40% at 90% 90%,rgba(16,185,129,0.04),transparent 60%)}

/* LOGIN */
.lp-wrap{position:fixed;inset:0;z-index:1000;display:flex;align-items:center;justify-content:center;
  background:linear-gradient(135deg,#060912 0%,#0a0f1e 100%);transition:opacity .6s ease,visibility .6s}
.lp-wrap.hidden{opacity:0;visibility:hidden;pointer-events:none}
.lp-orbs{position:absolute;inset:0;overflow:hidden;pointer-events:none}
.lp-orb{position:absolute;border-radius:50%;filter:blur(90px);opacity:.3;animation:orbFloat 10s ease-in-out infinite}
.lp-orb1{width:500px;height:500px;background:rgba(99,102,241,0.6);top:-150px;left:-150px;animation-delay:0s}
.lp-orb2{width:350px;height:350px;background:rgba(139,92,246,0.5);bottom:-100px;right:-100px;animation-delay:-4s}
.lp-orb3{width:250px;height:250px;background:rgba(6,182,212,0.4);top:40%;right:15%;animation-delay:-7s}
@keyframes orbFloat{0%,100%{transform:translate(0,0) scale(1)}50%{transform:translate(25px,20px) scale(1.08)}}
.lp-content{position:relative;z-index:1;display:flex;flex-direction:column;align-items:center;gap:32px}
.lp-hero{text-align:center}
.lp-logo-row{display:flex;align-items:center;justify-content:center;gap:12px;margin-bottom:20px}
.lp-co-name{font-size:18px;font-weight:700;color:rgba(255,255,255,0.9);letter-spacing:-.3px}
.lp-title{font-size:56px;font-weight:800;letter-spacing:-2px;line-height:1;
  background:linear-gradient(135deg,#fff 0%,#a5b4fc 50%,#c4b5fd 100%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;margin-bottom:10px}
.lp-sub{font-size:15px;color:rgba(255,255,255,0.35);letter-spacing:.02em}
.lp-card{width:380px;padding:36px 36px 28px;
  background:rgba(255,255,255,0.04);backdrop-filter:blur(28px);
  border:1px solid rgba(255,255,255,0.1);border-radius:20px;
  box-shadow:0 32px 100px rgba(0,0,0,0.5),inset 0 1px 0 rgba(255,255,255,0.06)}
.lp-field{margin-bottom:16px}
.lp-lbl{display:block;font-size:11px;font-weight:600;color:var(--mu);letter-spacing:.08em;text-transform:uppercase;margin-bottom:7px}
.lp-inp{width:100%;background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.09);
  color:var(--tx);border-radius:10px;padding:11px 14px;font-size:14px;outline:none;
  transition:border-color .2s,box-shadow .2s;font-family:inherit}
.lp-inp:focus{border-color:rgba(99,102,241,0.6);box-shadow:0 0 0 3px rgba(99,102,241,0.1)}
.lp-inp::placeholder{color:var(--fa)}
.lp-err{color:#f43f5e;font-size:12px;margin-bottom:12px;display:none;
  background:rgba(244,63,94,0.08);border:1px solid rgba(244,63,94,0.2);
  border-radius:8px;padding:9px 12px;text-align:center}
.lp-btn{width:100%;background:linear-gradient(135deg,#6366f1 0%,#8b5cf6 100%);
  color:#fff;border:none;border-radius:10px;padding:13px;margin-top:8px;
  font-size:14px;font-weight:700;cursor:pointer;transition:opacity .2s,transform .15s,box-shadow .2s;
  box-shadow:0 6px 24px rgba(99,102,241,0.4);font-family:inherit}
.lp-btn:hover{opacity:.92;transform:translateY(-1px)}
.lp-btn:active{transform:translateY(0)}
.lp-foot{font-size:11px;color:var(--fa);text-align:center}

/* TOPBAR */
.topbar{position:sticky;top:0;z-index:200;
  background:rgba(7,9,15,0.9);backdrop-filter:blur(24px);
  border-bottom:1px solid var(--bd);padding:0 20px;height:54px;
  display:flex;align-items:center;gap:12px;overflow-x:auto}
.brand{display:flex;align-items:center;gap:10px;flex-shrink:0;margin-right:4px}
.bname{font-size:17px;font-weight:800;letter-spacing:-.5px;color:#fff;line-height:1}
.bsub{font-size:9.5px;font-weight:600;color:var(--mu);letter-spacing:.1em;text-transform:uppercase;line-height:1;margin-top:2px}
.vd{width:1px;min-width:1px;height:22px;background:var(--bd);flex-shrink:0}
/* Tab navigation */
.tabnav{display:flex;gap:2px;flex-shrink:0}
.tabn{background:transparent;border:1px solid transparent;color:var(--mu);
  padding:5px 14px;border-radius:8px;font-size:12.5px;font-weight:600;cursor:pointer;
  transition:all .18s;white-space:nowrap;font-family:inherit;letter-spacing:.01em}
.tabn:hover{color:var(--tx);background:rgba(255,255,255,0.04)}
.tabn.active{background:var(--ac2);border-color:var(--ac);color:#a5b4fc}
/* Filter zone */
.filterzone{display:flex;align-items:center;gap:10px;flex:1;min-width:0;overflow-x:auto}
.qp{display:flex;gap:4px;flex-shrink:0}
.qpb{background:transparent;border:1px solid var(--bd);color:var(--mu);
  padding:3px 9px;border-radius:20px;font-size:11px;font-weight:500;cursor:pointer;
  transition:all .18s;white-space:nowrap;font-family:inherit}
.qpb:hover{border-color:var(--ac);color:var(--tx)}
.qpb.active{background:var(--ac2);border-color:var(--ac);color:#a5b4fc;font-weight:700}
.fg{display:flex;align-items:center;gap:6px;flex-shrink:0}
.fl{color:var(--mu);font-size:11px;white-space:nowrap;font-weight:500}
select{background:rgba(255,255,255,0.05);color:var(--tx);border:1px solid var(--bd);
  border-radius:8px;padding:4px 9px;font-size:12px;cursor:pointer;outline:none;
  transition:border-color .15s;font-family:inherit}
select:focus,select:hover{border-color:var(--ac)}
.tr{display:flex;gap:5px;align-items:center;flex-shrink:0}
.tog{padding:3px 9px;border-radius:20px;font-size:11px;font-weight:600;
  cursor:pointer;border:1.5px solid;transition:all .18s;user-select:none;white-space:nowrap}
.tog.off{background:transparent!important;border-color:var(--bd)!important;color:var(--fa)!important}
.tbr{margin-left:auto;display:flex;align-items:center;gap:7px;flex-shrink:0}
.upd{color:var(--mu);font-size:11px;white-space:nowrap}
.mbtn{background:rgba(255,255,255,0.04);border:1px solid var(--bd);color:var(--mu);
  padding:4px 10px;border-radius:7px;font-size:11px;cursor:pointer;transition:all .18s;
  font-family:inherit;white-space:nowrap}
.mbtn:hover{border-color:var(--ac);color:var(--tx)}

/* TAB PAGES */
.tab-page{display:none}
.tab-page.active{display:block}
.main{padding:22px 24px 56px;max-width:1800px;margin:0 auto}

/* PAGE HEADER */
.ph{margin-bottom:20px;display:flex;align-items:flex-end;justify-content:space-between;flex-wrap:wrap;gap:12px}
.pt{font-size:22px;font-weight:800;letter-spacing:-.6px;color:#fff}
.ps{color:var(--mu);font-size:12px;margin-top:4px}
.pill{display:inline-flex;align-items:center;gap:4px;
  background:rgba(99,102,241,0.08);border:1px solid rgba(99,102,241,0.18);
  border-radius:20px;padding:3px 12px;font-size:11px;color:#a5b4fc;font-weight:500}

/* INSIGHTS */
.ibar{display:flex;gap:10px;margin-bottom:20px;overflow-x:auto;padding-bottom:2px}
.icard{background:rgba(255,255,255,0.03);border:1px solid var(--bd);border-radius:10px;
  padding:9px 16px;white-space:nowrap;display:flex;align-items:center;gap:9px;flex-shrink:0;
  transition:border-color .18s}
.icard:hover{border-color:rgba(255,255,255,0.14)}
.iico{font-size:14px;flex-shrink:0}
.itx{font-size:12px;color:var(--mu)}
.itx b{color:var(--tx);font-weight:600}

/* KPI GRID */
.kgrid{display:grid;grid-template-columns:repeat(8,1fr);gap:10px;margin-bottom:14px}
.kcard{background:rgba(255,255,255,0.03);border:1px solid var(--bd);border-radius:12px;
  padding:16px 18px;border-left:3px solid;transition:all .2s;cursor:default}
.kcard:hover{background:rgba(255,255,255,0.05);transform:translateY(-1px)}
.klbl{color:var(--mu);font-size:10px;text-transform:uppercase;letter-spacing:.09em;
  margin-bottom:6px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.kval{font-size:20px;font-weight:800;line-height:1.1;margin-bottom:4px;
  font-variant-numeric:tabular-nums;letter-spacing:-.5px;color:#fff}
.kdlt{font-size:11px;display:flex;align-items:center;gap:3px;min-height:16px}
.up{color:var(--gn)}.dn{color:var(--rd)}.nu{color:var(--mu)}
.cp{color:var(--gn);font-weight:600}.cn{color:var(--rd);font-weight:600}

/* STATS BAR */
.sbar{display:flex;gap:18px;padding:12px 20px;
  background:rgba(255,255,255,0.03);border:1px solid var(--bd);
  border-radius:12px;margin-bottom:18px;flex-wrap:wrap;align-items:center}
.si{display:flex;align-items:center;gap:7px}
.sd{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.sl{color:var(--mu);font-size:11px}
.sv{font-weight:700;font-size:12.5px;color:var(--tx)}

/* CHART CARDS */
.grid{display:grid;gap:16px;margin-bottom:16px}
.g1{grid-template-columns:1fr}
.g11{grid-template-columns:1fr 1fr}
.g21{grid-template-columns:2fr 1fr}
.g12{grid-template-columns:1fr 2fr}
.g211{grid-template-columns:2fr 1fr 1fr}
.g31{grid-template-columns:3fr 1fr}
.g111{grid-template-columns:1fr 1fr 1fr}
.card{background:rgba(255,255,255,0.03);border:1px solid var(--bd);border-radius:14px;
  padding:22px 24px 18px;display:flex;flex-direction:column;transition:border-color .2s}
.card:hover{border-color:rgba(255,255,255,0.12)}
.ch{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:18px}
.ct{font-size:13.5px;font-weight:700;color:#fff;letter-spacing:-.1px}
.cs{font-size:11px;color:var(--mu);margin-top:3px}
.cw{position:relative;flex:1;min-height:240px}
.cleg{display:flex;flex-wrap:wrap;gap:8px 14px;margin-top:12px}
.cleg-item{display:flex;align-items:center;gap:5px;font-size:11px;color:var(--mu)}
.cleg-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}

/* DATA TABLE (main) */
.tcard{background:rgba(255,255,255,0.03);border:1px solid var(--bd);border-radius:14px;overflow:hidden;margin-bottom:20px}
.thead{padding:14px 20px;border-bottom:1px solid var(--bd);
  display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px}
.ttl{font-size:13px;font-weight:700;color:#fff}
.thead-right{display:flex;align-items:center;gap:10px}
.tsearch{background:rgba(255,255,255,0.05);border:1px solid var(--bd);color:var(--tx);
  padding:5px 12px;border-radius:8px;font-size:12px;outline:none;width:140px;
  transition:border-color .15s,width .2s,box-shadow .2s;font-family:inherit}
.tsearch:focus{border-color:var(--ac);width:190px;box-shadow:0 0 0 3px rgba(99,102,241,0.08)}
.tsearch::placeholder{color:var(--fa)}
.bsm{background:rgba(99,102,241,0.09);border:1px solid rgba(99,102,241,0.22);color:#a5b4fc;
  padding:5px 14px;border-radius:8px;font-size:12px;cursor:pointer;transition:all .18s;
  font-family:inherit;font-weight:600}
.bsm:hover{background:rgba(99,102,241,0.18)}
.twrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:12px}
th{background:rgba(255,255,255,0.03);color:var(--mu);font-size:9.5px;font-weight:700;
  text-transform:uppercase;letter-spacing:.08em;padding:9px 14px;text-align:right;
  border-bottom:1px solid var(--bd);white-space:nowrap;cursor:pointer;user-select:none;transition:color .15s}
th:first-child{text-align:left}
th:hover{color:var(--tx)}
th.sort-asc{color:var(--ac)}th.sort-asc::after{content:' \2191'}
th.sort-desc{color:var(--ac)}th.sort-desc::after{content:' \2193'}
td{padding:9px 14px;text-align:right;border-bottom:1px solid rgba(255,255,255,0.04);
  font-variant-numeric:tabular-nums;transition:background .1s}
td:first-child{text-align:left;color:var(--mu);font-weight:600}
tr:hover td{background:rgba(255,255,255,0.025)}
tr:last-child td{border-bottom:none}
.tfoot{padding:10px 20px;border-top:1px solid var(--bd);
  display:flex;justify-content:space-between;align-items:center}
.tfoot-info{font-size:11px;color:var(--mu)}

/* CUSTOMER PAGE */
.cust-summary{display:flex;gap:20px;padding:14px 20px;
  background:rgba(255,255,255,0.03);border:1px solid var(--bd);
  border-radius:12px;margin-bottom:16px;flex-wrap:wrap;align-items:center}
.cust-filters{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:16px}
.cust-search{background:rgba(255,255,255,0.05);border:1px solid var(--bd);color:var(--tx);
  padding:8px 14px;border-radius:10px;font-size:13px;outline:none;width:280px;
  transition:border-color .2s,box-shadow .2s;font-family:inherit}
.cust-search:focus{border-color:var(--ac);box-shadow:0 0 0 3px rgba(99,102,241,0.08)}
.cust-search::placeholder{color:var(--fa)}
.cust-fsel{background:rgba(255,255,255,0.05);color:var(--tx);border:1px solid var(--bd);
  border-radius:8px;padding:7px 12px;font-size:12px;cursor:pointer;outline:none;
  transition:border-color .15s;font-family:inherit}
.cust-fsel:hover,.cust-fsel:focus{border-color:var(--ac)}
.stog{padding:5px 14px;border-radius:8px;font-size:12px;font-weight:600;
  cursor:pointer;border:1px solid var(--bd);background:transparent;color:var(--mu);
  transition:all .18s;font-family:inherit}
.stog.active{background:var(--ac2);border-color:var(--ac);color:#a5b4fc}
.stog:hover{border-color:var(--ac);color:var(--tx)}
.cust-count{font-size:12px;color:var(--mu);margin-left:auto;white-space:nowrap}
.badge{display:inline-flex;align-items:center;font-size:10px;font-weight:700;
  padding:2px 8px;border-radius:20px;letter-spacing:.04em;text-transform:uppercase}
.badge-active{background:rgba(16,185,129,0.12);color:#10b981;border:1px solid rgba(16,185,129,0.25)}
.badge-churned{background:rgba(244,63,94,0.1);color:#f43f5e;border:1px solid rgba(244,63,94,0.2)}
.cust-row{cursor:pointer;transition:background .12s}
.cust-row:hover td{background:rgba(255,255,255,0.055)!important;border-bottom-color:rgba(255,255,255,0.08)!important}
.cust-row:active td{background:rgba(99,102,241,0.1)!important}
.email-cell{max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:monospace;font-size:11px}
/* Pagination */
.cpag{display:flex;align-items:center;justify-content:center;gap:8px;padding:14px 20px;border-top:1px solid var(--bd)}
.cpag-btn{background:rgba(255,255,255,0.04);border:1px solid var(--bd);color:var(--mu);
  padding:5px 14px;border-radius:7px;font-size:12px;cursor:pointer;transition:all .18s;font-family:inherit}
.cpag-btn:hover:not(:disabled){border-color:var(--ac);color:var(--tx)}
.cpag-btn:disabled{opacity:.35;cursor:default}
.cpag-info{font-size:12px;color:var(--mu)}

/* CUSTOMER DETAIL */
.back-btn{display:inline-flex;align-items:center;gap:6px;background:rgba(255,255,255,0.04);
  border:1px solid var(--bd);color:var(--mu);padding:7px 14px;border-radius:9px;
  font-size:12px;font-weight:600;cursor:pointer;margin-bottom:20px;
  transition:all .18s;font-family:inherit}
.back-btn:hover{border-color:var(--ac);color:var(--tx)}
.cd-header{display:flex;align-items:flex-start;gap:20px;margin-bottom:24px;flex-wrap:wrap}
.cd-email{font-size:20px;font-weight:800;color:#fff;letter-spacing:-.3px;
  word-break:break-all;font-family:monospace}
.cd-meta{display:flex;gap:10px;align-items:center;margin-top:8px;flex-wrap:wrap}
.cd-plan{font-size:12px;color:var(--mu);background:rgba(255,255,255,0.05);
  border:1px solid var(--bd);border-radius:20px;padding:3px 12px}
.cd-kgrid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}
.cd-hist{background:rgba(255,255,255,0.03);border:1px solid var(--bd);border-radius:14px;
  padding:22px 24px 18px;margin-bottom:20px}
.cd-hist-title{font-size:13.5px;font-weight:700;color:#fff;margin-bottom:16px}
.cd-cw{position:relative;height:220px}

/* MODAL */
.mbg{position:fixed;inset:0;background:rgba(0,0,0,0.72);backdrop-filter:blur(6px);
  z-index:500;display:none;align-items:center;justify-content:center;padding:20px}
.mbg.open{display:flex}
.modal{background:#0c1220;border:1px solid var(--bd);border-radius:18px;
  padding:32px;max-width:740px;width:100%;max-height:80vh;overflow-y:auto;position:relative;
  box-shadow:0 40px 120px rgba(0,0,0,0.7)}
.mcl{position:absolute;top:14px;right:16px;background:rgba(255,255,255,0.06);
  border:1px solid var(--bd);color:var(--mu);width:30px;height:30px;border-radius:8px;
  cursor:pointer;font-size:14px;display:flex;align-items:center;justify-content:center;
  transition:all .15s;font-family:inherit}
.mcl:hover{color:var(--tx);background:rgba(255,255,255,0.1);border-color:var(--ac)}
.modal h2{font-size:18px;font-weight:700;margin-bottom:20px;color:#fff;letter-spacing:-.3px}
.modal table{font-size:12px}.modal th,.modal td{border:1px solid var(--bd);padding:9px 12px;text-align:left}
.modal th{color:var(--mu);background:rgba(255,255,255,0.03);font-weight:700;font-size:10px;text-transform:uppercase;letter-spacing:.07em}
.modal td{color:var(--tx)}
.modal p{font-size:12px;color:var(--mu);margin-top:16px;line-height:1.6}

@media(max-width:1400px){.kgrid{grid-template-columns:repeat(4,1fr)}.cd-kgrid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:1100px){.g21,.g31,.g211,.g111{grid-template-columns:1fr}}
@media(max-width:900px){.kgrid{grid-template-columns:repeat(2,1fr)}.g11,.g12{grid-template-columns:1fr}}
"""

# ---- HTML -------------------------------------------------------------------
HEAD = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Pulse - Revenue Intelligence - quso.ai</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>{CSS}</style>
</head>"""

LOGIN = f"""
<div id="loginPage" class="lp-wrap">
  <div class="lp-orbs">
    <div class="lp-orb lp-orb1"></div>
    <div class="lp-orb lp-orb2"></div>
    <div class="lp-orb lp-orb3"></div>
  </div>
  <div class="lp-content">
    <div class="lp-hero">
      <div class="lp-logo-row"><span class="lp-co-name">quso.ai</span></div>
      <div class="lp-title">Pulse</div>
      <div class="lp-sub">Revenue Intelligence Dashboard</div>
    </div>
    <div class="lp-card">
      <div class="lp-field">
        <label class="lp-lbl">Username</label>
        <input id="lpUser" class="lp-inp" type="text" placeholder="Enter username"
               autocomplete="username" onkeydown="if(event.key==='Enter')doLogin()"/>
      </div>
      <div class="lp-field">
        <label class="lp-lbl">Password</label>
        <input id="lpPass" class="lp-inp" type="password" placeholder="Enter password"
               autocomplete="current-password" onkeydown="if(event.key==='Enter')doLogin()"/>
      </div>
      <div id="lpErr" class="lp-err">Invalid credentials - please try again.</div>
      <button class="lp-btn" onclick="doLogin()">Sign In &rarr;</button>
    </div>
    <div class="lp-foot">Pulse by quso.ai &middot; Confidential &middot; {generated_at[:7]}</div>
  </div>
</div>"""

DASH_SHELL = f"""
<div id="dashPage" style="display:none">
<nav class="topbar">
  <div class="brand">
    <div><div class="bname">Pulse</div><div class="bsub">quso.ai</div></div>
  </div>
  <div class="vd"></div>
  <div class="tabnav">
    <button class="tabn active" data-tab="overview" onclick="switchPage('overview',this)">Overview</button>
    <button class="tabn" data-tab="revenue" onclick="switchPage('revenue',this)">Revenue</button>
    <button class="tabn" data-tab="customers" onclick="switchPage('customers',this)">Customers</button>
  </div>
  <div class="vd"></div>
  <div class="filterzone" id="filterZone">
    <div class="qp">
      <button class="qpb" onclick="setQuickPeriod(3,this)">3M</button>
      <button class="qpb" onclick="setQuickPeriod(6,this)">6M</button>
      <button class="qpb" onclick="setQuickPeriod(12,this)">12M</button>
      <button class="qpb" onclick="setQuickPeriod('ytd',this)">YTD</button>
      <button class="qpb active" onclick="setQuickPeriod('all',this)">All</button>
    </div>
    <div class="vd"></div>
    <div class="fg"><span class="fl">From</span><select id="selS" onchange="applyFilters()"></select></div>
    <div class="fg"><span class="fl">To</span><select id="selE" onchange="applyFilters()"></select></div>
  </div>
  <div class="tbr">
    <span class="upd">Updated {generated_at}</span>
    <button class="mbtn" onclick="document.getElementById('mmodal').classList.toggle('open')">Methodology</button>
    <button class="mbtn" onclick="doLogout()">Sign Out</button>
  </div>
</nav>

<!-- METHODOLOGY MODAL -->
<div class="mbg" id="mmodal" onclick="if(event.target===this)this.classList.remove('open')">
  <div class="modal">
    <button class="mcl" onclick="document.getElementById('mmodal').classList.remove('open')">&#x2715;</button>
    <h2>MRR Calculation Methodology</h2>
    <table><thead><tr><th>Decision</th><th>Rule</th></tr></thead><tbody>
      <tr><td>Data Source</td><td>Paddle Billing only (Oct 2024+). Classic Paddle excluded.</td></tr>
      <tr><td>Txn Filter</td><td>Only status = completed (34,649 of 190,750 transactions).</td></tr>
      <tr><td>Currency (v3)</td><td>FX rate from transactions CSV. 31% non-USD transactions.</td></tr>
      <tr><td>Discounts (v3)</td><td>net_usd = (subtotal - discount) x rate. MRR = actual payments.</td></tr>
      <tr><td>Exclusions</td><td>Proration credits and 100% discounted items excluded (1,714 items).</td></tr>
      <tr><td>MRR Formula</td><td>Annual: net_usd / 12. Monthly: net_usd / freq.</td></tr>
      <tr><td>Movements</td><td>New Biz: first-ever. Reactivation: returned after gap. Expansion/Contraction: diff.</td></tr>
      <tr><td>NRR</td><td>(Opening + Expansion + Reactivation - Contraction - Churn) / Opening. Period avg.</td></tr>
      <tr><td>Customers</td><td>Deduplicated by email. ARPA = Closing MRR / Customers. LTV = ARPA / Avg Churn.</td></tr>
      <tr><td>Waterfall</td><td>Closing = Opening + New + Expansion + Reactivation - Contraction - Churn.</td></tr>
    </tbody></table>
    <p>Generated: {generated_at} &middot; {data_start} to {data_end} &middot; {n_customers:,} customers tracked</p>
  </div>
</div>

<!-- PAGE: OVERVIEW -->
<div id="page-overview" class="tab-page active">
<div class="main">
  <div class="ph">
    <div><div class="pt">Revenue Overview</div>
      <div class="ps">Paddle Billing &middot; USD &middot; {data_start} to {data_end}</div></div>
    <div style="display:flex;gap:8px;flex-wrap:wrap">
      <span class="pill">MRR {cur_mrr}</span><span class="pill">ARR {cur_arr}</span>
    </div>
  </div>
  <div class="ibar" id="ibar"></div>
  <div class="kgrid" id="kgrid"></div>
  <div class="sbar" id="sbar"></div>
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;flex-wrap:wrap;padding:10px 16px;background:rgba(255,255,255,0.02);border:1px solid var(--bd);border-radius:10px">
    <span style="font-size:11px;color:var(--mu);font-weight:600;letter-spacing:.06em;text-transform:uppercase;white-space:nowrap;flex-shrink:0">Movement Filters</span>
    <div class="tr" id="movTogs" style="flex-wrap:wrap"></div>
  </div>
  <div class="grid g21">
    <div class="card" style="min-height:360px">
      <div class="ch"><div>
        <div class="ct">MRR Growth &amp; Movement Breakdown</div>
        <div class="cs">Closing MRR (line) over movement categories (stacked bars)</div>
      </div></div>
      <div class="cw"><canvas id="cT"></canvas></div>
    </div>
    <div class="card" style="min-height:360px">
      <div class="ch"><div>
        <div class="ct">Net New MRR</div>
        <div class="cs">Month-over-month net change</div>
      </div></div>
      <div class="cw"><canvas id="cN"></canvas></div>
    </div>
  </div>
  <div class="grid g11">
    <div class="card" style="min-height:300px">
      <div class="ch"><div>
        <div class="ct">Churn Rate Trends</div>
        <div class="cs">MRR churn %, net MRR churn %, customer churn %</div>
      </div></div>
      <div class="cw"><canvas id="cCh"></canvas></div>
    </div>
    <div class="card" style="min-height:300px">
      <div class="ch"><div>
        <div class="ct">New Business vs Churn</div>
        <div class="cs">Monthly acquisition vs revenue lost</div>
      </div></div>
      <div class="cw"><canvas id="cNvC"></canvas></div>
    </div>
  </div>
  <div class="tcard">
    <div class="thead">
      <div class="ttl">Monthly Data - Full Breakdown</div>
      <div class="thead-right">
        <input id="tableSearch" class="tsearch" type="text" placeholder="Search month..."
               oninput="S.tableSearch=this.value;renderTable(filtered())"/>
        <button class="bsm" onclick="downloadCSV()">&#8595; Export CSV</button>
      </div>
    </div>
    <div class="twrap"><table id="dtbl"></table></div>
    <div class="tfoot">
      <span class="tfoot-info" id="tFootInfo"></span>
      <span class="tfoot-info">Click column headers to sort</span>
    </div>
  </div>
</div>
</div>

<!-- PAGE: REVENUE -->
<div id="page-revenue" class="tab-page">
<div class="main">
  <div class="ph">
    <div><div class="pt">Revenue Deep Dive</div>
      <div class="ps">All revenue charts &middot; Use filters above to adjust period</div></div>
  </div>
  <div class="grid g1">
    <div class="card" style="min-height:300px">
      <div class="ch"><div>
        <div class="ct">ARPA Trend</div>
        <div class="cs">Average Revenue Per Account by month &mdash; key signal for pricing &amp; expansion health</div>
      </div></div>
      <div class="cw"><canvas id="cAR"></canvas></div>
    </div>
  </div>
  <div class="grid g11">
    <div class="card" style="min-height:360px">
      <div class="ch"><div>
        <div class="ct">MRR Growth Rate (MoM %)</div>
        <div class="cs">Month-over-month percentage change in closing MRR</div>
      </div></div>
      <div class="cw"><canvas id="cGR"></canvas></div>
    </div>
    <div class="card" style="min-height:360px">
      <div class="ch"><div>
        <div class="ct">Expansion vs Contraction vs Reactivation</div>
        <div class="cs">Revenue changes from existing customer base</div>
      </div></div>
      <div class="cw"><canvas id="cExCo"></canvas></div>
    </div>
  </div>
  <div class="grid g1">
    <div class="card" style="min-height:380px">
      <div class="ch"><div>
        <div class="ct">MRR Waterfall - Full Breakdown</div>
        <div class="cs">All movement categories stacked with Closing MRR line</div>
      </div></div>
      <div class="cw"><canvas id="cT2"></canvas></div>
    </div>
  </div>
  <div class="grid g11">
    <div class="card" style="min-height:320px">
      <div class="ch"><div>
        <div class="ct">Customer &amp; Subscription Metrics</div>
        <div class="cs">Active customers and subscriptions over time</div>
      </div></div>
      <div class="cw"><canvas id="cCu"></canvas></div>
    </div>
    <div class="card" style="min-height:320px">
      <div class="ch"><div>
        <div class="ct">Plan Mix</div>
        <div class="cs">MRR by plan (current period)</div>
      </div></div>
      <div class="cw" style="min-height:200px"><canvas id="cPM"></canvas></div>
      <div id="planLegend" class="cleg" style="margin-top:10px"></div>
    </div>
  </div>
</div>
</div>

<!-- PAGE: CUSTOMERS -->
<div id="page-customers" class="tab-page">
<div class="main">
  <div class="ph">
    <div><div class="pt">Customer Intelligence</div>
      <div class="ps">{n_customers:,} customers tracked &middot; Search, filter and explore all accounts</div></div>
  </div>
  <div class="cust-summary" id="custSummary"></div>
  <div class="cust-filters">
    <input id="custSearch" class="cust-search" type="text" placeholder="Search by email..."
           oninput="CS.search=this.value;CS.page=0;renderCustomerList()"/>
    <select id="custPlan" class="cust-fsel" onchange="CS.plan=this.value;CS.page=0;renderCustomerList()">
      <option value="">All Plans</option>
    </select>
    <button class="stog active" id="sTogAll" onclick="setStatusFilter('all',this)">All</button>
    <button class="stog" id="sTogActive" onclick="setStatusFilter('active',this)">Active</button>
    <button class="stog" id="sTogChurned" onclick="setStatusFilter('churned',this)">Churned</button>
    <span class="cust-count" id="custCount"></span>
  </div>
  <div class="tcard">
    <div class="twrap"><table id="custTbl"></table></div>
    <div class="cpag" id="custPag"></div>
  </div>
</div>
</div>

<!-- PAGE: CUSTOMER DETAIL -->
<div id="page-customer-detail" class="tab-page">
<div class="main">
  <button class="back-btn" onclick="switchPage('customers',null)">&#8592; Back to Customers</button>
  <div class="cd-header">
    <div>
      <div class="cd-email" id="cdEmail"></div>
      <div class="cd-meta">
        <span id="cdBadge"></span>
        <span class="cd-plan" id="cdPlan"></span>
        <span class="cd-plan" id="cdSince"></span>
      </div>
    </div>
  </div>
  <div class="cd-kgrid" id="cdKgrid"></div>
  <div class="cd-hist">
    <div class="cd-hist-title">MRR History</div>
    <div class="cd-cw"><canvas id="cCustHist"></canvas></div>
  </div>
  <div class="tcard">
    <div class="thead"><div class="ttl">Month-by-Month Activity</div></div>
    <div class="twrap"><table id="cdTable"></table></div>
  </div>
</div>
</div>

</div>"""  # end dashPage

# ---- JavaScript (must stay 100% ASCII) ------------------------------------
JS_LINES = [
    "const DATA = " + data_json + ";",
    "const PLAN_OPTIONS = " + plans_json + ";",
    "",
    "const CREDS = {u:'quso', p:'Revenue2025'};",
    "const MONTHS = DATA.monthly_metrics.map(function(m){return m.month;});",
    "const CUSTOMERS = DATA.customers || [];",
    "",
    "var S = {",
    "  start: 0, end: MONTHS.length-1,",
    "  show: {new_business:true,expansion:true,reactivation:true,contraction:true,churn:true},",
    "  sortCol:'month', sortDir:1, tableSearch:'',",
    "  page: 'overview'",
    "};",
    "var CS = {search:'',plan:'',status:'all',sortCol:'current_mrr',sortDir:-1,page:0,perPage:50};",
    "",
    "// -- Colors",
    "const CLR = {",
    "  nb:'#6366f1',ex:'#10b981',re:'#8b5cf6',co:'#f59e0b',ch:'#f43f5e',",
    "  wh:'#e2e8f0',gn:'#10b981',rd:'#f43f5e',gd:'rgba(255,255,255,0.05)',tk:'#64748b'",
    "};",
    "const PLAN_PAL=['#6366f1','#10b981','#f59e0b','#8b5cf6','#f43f5e','#06b6d4',",
    "  '#f97316','#ec4899','#84cc16','#a78bfa','#fb923c','#34d399','#60a5fa','#fbbf24'];",
    "const MOVS=[",
    "  {key:'new_business',lbl:'New Business',clr:'#6366f1'},",
    "  {key:'expansion',   lbl:'Expansion',   clr:'#10b981'},",
    "  {key:'reactivation',lbl:'Reactivation',clr:'#8b5cf6'},",
    "  {key:'contraction', lbl:'Contraction', clr:'#f59e0b'},",
    "  {key:'churn',       lbl:'Churn',       clr:'#f43f5e'},",
    "];",
    "",
    "Chart.defaults.color=CLR.tk;",
    "Chart.defaults.borderColor=CLR.gd;",
    "Chart.defaults.font.family=\"-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,sans-serif\";",
    "Chart.defaults.font.size=11;",
    "",
    "// -- Formatters",
    "function f0(v){return '$'+Math.abs(v).toLocaleString('en-US',{maximumFractionDigits:0});}",
    "function f2(v){return '$'+v.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});}",
    "function fP(v){return v.toFixed(1)+'%';}",
    "function fN(v){return Math.round(v).toLocaleString('en-US');}",
    "function filtered(){return DATA.monthly_metrics.slice(S.start,S.end+1);}",
    "",
    "var AX={grid:{color:CLR.gd},ticks:{color:CLR.tk}};",
    "function baseOpts(extra){",
    "  var o={responsive:true,maintainAspectRatio:false,animation:{duration:280},",
    "    plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false,",
    "      backgroundColor:'rgba(7,9,15,0.95)',borderColor:'rgba(255,255,255,0.12)',",
    "      borderWidth:1,padding:12,titleColor:'#fff',bodyColor:'#94a3b8'}}};",
    "  return Object.assign(o,extra||{});",
    "}",
    "var CH={};",
    "function upsert(id,cfg){",
    "  var el=document.getElementById(id);",
    "  if(!el) return;",
    "  if(CH[id]){CH[id].data=cfg.data;CH[id].options=cfg.options;CH[id].resize();CH[id].update('none');}",
    "  else{CH[id]=new Chart(el.getContext('2d'),cfg);}",
    "}",
    "",
    "// -- Login/Logout",
    "function doLogin(){",
    "  var u=document.getElementById('lpUser').value.trim();",
    "  var p=document.getElementById('lpPass').value;",
    "  if(u===CREDS.u&&p===CREDS.p){",
    "    document.getElementById('lpErr').style.display='none';",
    "    document.getElementById('loginPage').classList.add('hidden');",
    "    document.getElementById('dashPage').style.display='block';",
    "    setTimeout(initDash,80);",
    "  }else{",
    "    document.getElementById('lpErr').style.display='block';",
    "    var ui=document.getElementById('lpUser');",
    "    var pi=document.getElementById('lpPass');",
    "    ui.style.borderColor='#f43f5e';pi.style.borderColor='#f43f5e';",
    "    setTimeout(function(){ui.style.borderColor='';pi.style.borderColor='';},2500);",
    "  }",
    "}",
    "function doLogout(){",
    "  document.getElementById('dashPage').style.display='none';",
    "  document.getElementById('loginPage').classList.remove('hidden');",
    "  document.getElementById('lpPass').value='';",
    "  document.getElementById('lpErr').style.display='none';",
    "}",
    "",
    "// -- Page switching",
    "function switchPage(name,btn){",
    "  document.querySelectorAll('.tab-page').forEach(function(p){p.classList.remove('active');});",
    "  var pg=document.getElementById('page-'+name);",
    "  if(pg) pg.classList.add('active');",
    "  document.querySelectorAll('.tabn').forEach(function(b){b.classList.remove('active');});",
    "  if(btn) btn.classList.add('active');",
    "  else{",
    "    var tb=document.querySelector('.tabn[data-tab=\"'+name+'\"]');",
    "    if(tb) tb.classList.add('active');",
    "  }",
    "  var showFilter=(name==='overview'||name==='revenue');",
    "  document.getElementById('filterZone').style.display=showFilter?'':'none';",
    "  S.page=name;",
    "  if(name==='overview'){updateOverview();}",
    "  else if(name==='revenue'){",
    "    updateRevenue();",
    "    setTimeout(function(){Object.values(CH).forEach(function(c){try{c.resize();}catch(e){}});},60);",
    "  }",
    "  else if(name==='customers'){renderCustomerList();}",
    "}",
    "",
    "// -- Period / Filter",
    "function setQuickPeriod(p,btn){",
    "  var n=MONTHS.length;",
    "  if(p==='all'){S.start=0;S.end=n-1;}",
    "  else if(p==='ytd'){",
    "    var yr=MONTHS[n-1].substring(0,4);",
    "    var ys=0;",
    "    for(var i=0;i<n;i++){if(MONTHS[i].substring(0,4)===yr){ys=i;break;}}",
    "    S.start=ys;S.end=n-1;",
    "  }else{S.start=Math.max(0,n-parseInt(p));S.end=n-1;}",
    "  document.getElementById('selS').value=S.start;",
    "  document.getElementById('selE').value=S.end;",
    "  document.querySelectorAll('.qpb').forEach(function(b){b.classList.remove('active');});",
    "  if(btn) btn.classList.add('active');",
    "  updateAll();",
    "}",
    "function applyFilters(){",
    "  S.start=parseInt(document.getElementById('selS').value);",
    "  S.end=Math.max(S.start,parseInt(document.getElementById('selE').value));",
    "  document.querySelectorAll('.qpb').forEach(function(b){b.classList.remove('active');});",
    "  updateAll();",
    "}",
    "function toggleMov(key){",
    "  S.show[key]=!S.show[key];",
    "  var tog=document.getElementById('tog_'+key);",
    "  if(tog) tog.classList.toggle('off',!S.show[key]);",
    "  updateAll();",
    "}",
    "",
    "// -- NRR",
    "function calcNRR(f){",
    "  var tO=0,tR=0;",
    "  f.forEach(function(m){",
    "    if(m.opening_mrr<=0) return;",
    "    tO+=m.opening_mrr;",
    "    tR+=m.opening_mrr+m.expansion+m.reactivation-m.contraction-m.churn;",
    "  });",
    "  return tO>0?(tR/tO)*100:100;",
    "}",
    "",
    "// -- KPIs",
    "function renderKPIs(f){",
    "  if(!f||!f.length){document.getElementById('kgrid').innerHTML='';return;}",
    "  var last=f[f.length-1],prev=f.length>1?f[f.length-2]:null;",
    "  var nrr=calcNRR(f);",
    "  var mom=prev&&prev.closing_mrr>0?((last.closing_mrr-prev.closing_mrr)/prev.closing_mrr*100):0;",
    "  function dp(cur,pr){",
    "    if(!pr||pr<=0) return '<span class=\"nu\">--</span>';",
    "    var d=((cur-pr)/pr*100);",
    "    return '<span class=\"'+(d>=0?'up':'dn')+'\">'+(d>=0?'+':'')+d.toFixed(1)+'% MoM</span>';",
    "  }",
    "  function dd(cur,pr,fmt){",
    "    if(!pr) return '<span class=\"nu\">--</span>';",
    "    var d=cur-pr;var cls=d>=0?'up':'dn';",
    "    return '<span class=\"'+cls+'\">'+(d>=0?'+':'-')+fmt(Math.abs(d))+' MoM</span>';",
    "  }",
    "  var kpis=[",
    "    {l:'Closing MRR',   v:f0(last.closing_mrr),  d:dp(last.closing_mrr,prev?prev.closing_mrr:null), b:'#6366f1'},",
    "    {l:'ARR',           v:f0(last.arr),           d:dp(last.arr,prev?prev.arr:null),                 b:'#8b5cf6'},",
    "    {l:'ARPA',          v:f2(last.arpa),          d:dd(last.arpa,prev?prev.arpa:null,function(x){return '$'+x.toFixed(2);}), b:'#06b6d4'},",
    "    {l:'Active Customers',v:fN(last.active_customers),d:dd(last.active_customers,prev?prev.active_customers:null,function(x){return Math.round(x).toLocaleString();}), b:'#10b981'},",
    "    {l:'MRR Churn Rate',v:fP(last.mrr_churn_rate),d:prev?'<span class=\"'+(last.mrr_churn_rate<=prev.mrr_churn_rate?'up':'dn')+'\">'+(last.mrr_churn_rate<=prev.mrr_churn_rate?'&#9660;':'&#9650;')+Math.abs(last.mrr_churn_rate-prev.mrr_churn_rate).toFixed(2)+'pp MoM</span>':'<span class=\"nu\">--</span>',b:'#f43f5e'},",
    "    {l:'LTV',           v:f0(DATA.summary.ltv),   d:'<span class=\"nu\">ARPA / Avg Churn</span>',    b:'#f59e0b'},",
    "    {l:'Net Rev Retention',v:nrr.toFixed(0)+'%',  d:'<span class=\"'+(nrr>=100?'up':'dn')+'\">Period avg NRR</span>',b:'#10b981'},",
    "    {l:'MoM MRR Growth',v:(mom>=0?'+':'')+mom.toFixed(1)+'%',d:'<span class=\"nu\">vs last month</span>',b:mom>=0?'#10b981':'#f43f5e'},",
    "  ];",
    "  document.getElementById('kgrid').innerHTML=kpis.map(function(k){",
    "    return '<div class=\"kcard\" style=\"border-left-color:'+k.b+'\">'+",
    "      '<div class=\"klbl\">'+k.l+'</div>'+",
    "      '<div class=\"kval\">'+k.v+'</div>'+",
    "      '<div class=\"kdlt\">'+k.d+'</div></div>';",
    "  }).join('');",
    "}",
    "",
    "// -- Insights",
    "function renderInsights(f){",
    "  if(!f||f.length<2){document.getElementById('ibar').innerHTML='';return;}",
    "  var first=f[0],last=f[f.length-1];",
    "  var mg=last.closing_mrr-first.opening_mrr;",
    "  var mgp=(mg/first.opening_mrr*100).toFixed(1);",
    "  var bestM=f.reduce(function(a,b){return b.closing_mrr>a.closing_mrr?b:a;});",
    "  var totNB=f.reduce(function(s,m){return s+m.new_business;},0);",
    "  var totCh=f.reduce(function(s,m){return s+m.churn;},0);",
    "  var nrr=calcNRR(f);",
    "  var avgA=f.reduce(function(s,m){return s+m.arpa;},0)/f.length;",
    "  var its=[",
    "    {ico:'&#128200;',txt:'MRR <b>'+(mg>=0?'+':'')+mgp+'%</b> in selected period'},",
    "    {ico:'&#127942;',txt:'Peak MRR: <b>'+bestM.month+'</b> at <b>$'+Math.round(bestM.closing_mrr).toLocaleString()+'</b>'},",
    "    {ico:'&#128640;',txt:'New Biz: <b>$'+Math.round(totNB).toLocaleString()+'</b> vs Churn: <b>$'+Math.round(totCh).toLocaleString()+'</b>'},",
    "    {ico:'&#128273;',txt:'Net Revenue Retention: <b>'+nrr.toFixed(0)+'%</b>'},",
    "    {ico:'&#128181;',txt:'Avg ARPA: <b>$'+avgA.toFixed(2)+'</b> over '+f.length+' months'},",
    "  ];",
    "  document.getElementById('ibar').innerHTML=its.map(function(it){",
    "    return '<div class=\"icard\"><span class=\"iico\">'+it.ico+'</span><span class=\"itx\">'+it.txt+'</span></div>';",
    "  }).join('');",
    "}",
    "",
    "// -- Stats bar",
    "function renderStats(f){",
    "  if(!f||!f.length){document.getElementById('sbar').innerHTML='';return;}",
    "  var tNB=f.reduce(function(s,m){return s+m.new_business;},0);",
    "  var tEx=f.reduce(function(s,m){return s+m.expansion;},0);",
    "  var tRe=f.reduce(function(s,m){return s+m.reactivation;},0);",
    "  var tCo=f.reduce(function(s,m){return s+m.contraction;},0);",
    "  var tCh=f.reduce(function(s,m){return s+m.churn;},0);",
    "  var aS=f.reduce(function(s,m){return s+m.active_subscriptions;},0)/f.length;",
    "  var aA=f.reduce(function(s,m){return s+m.arpa;},0)/f.length;",
    "  var aC=f.reduce(function(s,m){return s+m.mrr_churn_rate;},0)/f.length;",
    "  var st=[",
    "    {d:'#6366f1',l:'Total New Biz',v:f0(tNB)},",
    "    {d:'#10b981',l:'Total Expansion',v:f0(tEx)},",
    "    {d:'#8b5cf6',l:'Total Reactivation',v:f0(tRe)},",
    "    {d:'#f59e0b',l:'Total Contraction',v:'-'+f0(tCo)},",
    "    {d:'#f43f5e',l:'Total Churn',v:'-'+f0(tCh)},",
    "    {d:'#06b6d4',l:'Avg Subscriptions',v:Math.round(aS).toLocaleString()},",
    "    {d:'#e2e8f0',l:'Avg ARPA',v:'$'+aA.toFixed(2)},",
    "    {d:'#f43f5e',l:'Avg MRR Churn',v:aC.toFixed(2)+'%'},",
    "  ];",
    "  document.getElementById('sbar').innerHTML=st.map(function(s){",
    "    return '<div class=\"si\"><div class=\"sd\" style=\"background:'+s.d+'\"></div>'+",
    "      '<span class=\"sl\">'+s.l+'</span><span class=\"sv\">'+s.v+'</span></div>';",
    "  }).join('');",
    "}",
    "",
    "// -- Chart helpers",
    "function lsDS(data,clr,lbl){",
    "  return{label:lbl,data:data,borderColor:clr,backgroundColor:'transparent',",
    "    borderWidth:2,pointRadius:3,pointHoverRadius:5,tension:.4,fill:false};",
    "}",
    "",
    "// -- Overview charts",
    "function renderMrrTrend(id,f){",
    "  var labels=f.map(function(m){return m.month;});",
    "  var dsets=[];",
    "  MOVS.forEach(function(mv){",
    "    if(!S.show[mv.key]) return;",
    "    var vals=f.map(function(m){return mv.key==='churn'?-m[mv.key]:m[mv.key];});",
    "    dsets.push({type:'bar',label:mv.lbl,data:vals,",
    "      backgroundColor:mv.clr+'bb',borderColor:mv.clr,borderWidth:1,borderRadius:3,",
    "      yAxisID:'y',stack:'s'});",
    "  });",
    "  dsets.push({type:'line',label:'Closing MRR',data:f.map(function(m){return m.closing_mrr;}),",
    "    borderColor:'#e2e8f0',backgroundColor:'rgba(226,232,240,0.06)',",
    "    borderWidth:2.5,pointRadius:3,pointHoverRadius:5,tension:.35,fill:false,yAxisID:'y2',order:0});",
    "  upsert(id,{type:'bar',data:{labels:labels,datasets:dsets},",
    "    options:baseOpts({scales:{",
    "      y:{...AX,stacked:true,title:{display:true,text:'Movement ($)',color:CLR.tk,font:{size:10}}},",
    "      y2:{...AX,position:'right',grid:{drawOnChartArea:false},",
    "        title:{display:true,text:'Closing MRR ($)',color:CLR.tk,font:{size:10}}}},",
    "    plugins:{...baseOpts().plugins,legend:{display:true,position:'bottom',",
    "      labels:{boxWidth:10,padding:14,font:{size:10},color:CLR.tk}}}})});",
    "}",
    "function renderNetNew(f){",
    "  var labels=f.map(function(m){return m.month;});",
    "  var vals=f.map(function(m){return m.net_new_mrr;});",
    "  upsert('cN',{type:'bar',data:{labels:labels,datasets:[{label:'Net New MRR',data:vals,",
    "    backgroundColor:vals.map(function(v){return v>=0?'rgba(99,102,241,0.7)':'rgba(244,63,94,0.7)';}),",
    "    borderColor:vals.map(function(v){return v>=0?'#6366f1':'#f43f5e';}),",
    "    borderWidth:1,borderRadius:4}]},options:baseOpts({scales:{x:AX,y:AX}})});",
    "}",
    "function renderChurn(f){",
    "  var labels=f.map(function(m){return m.month;});",
    "  upsert('cCh',{type:'line',data:{labels:labels,datasets:[",
    "    lsDS(f.map(function(m){return m.mrr_churn_rate;}),'#f43f5e','MRR Churn %'),",
    "    lsDS(f.map(function(m){return m.net_mrr_churn_rate;}),'#f59e0b','Net MRR Churn %'),",
    "    lsDS(f.map(function(m){return m.customer_churn_rate;}),'#8b5cf6','Customer Churn %'),",
    "  ]},options:baseOpts({scales:{x:AX,y:{...AX,ticks:{callback:function(v){return v+'%';}}}},",
    "    plugins:{...baseOpts().plugins,legend:{display:true,position:'bottom',",
    "      labels:{boxWidth:10,padding:14,font:{size:10},color:CLR.tk}}}})});",
    "}",
    "function renderNvC(f){",
    "  var labels=f.map(function(m){return m.month;});",
    "  upsert('cNvC',{type:'bar',data:{labels:labels,datasets:[",
    "    {label:'New Business',data:f.map(function(m){return m.new_business;}),",
    "      backgroundColor:'rgba(99,102,241,0.75)',borderColor:'#6366f1',borderWidth:1,borderRadius:3},",
    "    {label:'Churn',data:f.map(function(m){return -m.churn;}),",
    "      backgroundColor:'rgba(244,63,94,0.75)',borderColor:'#f43f5e',borderWidth:1,borderRadius:3},",
    "  ]},options:baseOpts({scales:{x:AX,y:AX},",
    "    plugins:{...baseOpts().plugins,legend:{display:true,position:'bottom',",
    "      labels:{boxWidth:10,padding:14,font:{size:10},color:CLR.tk}}}})});",
    "}",
    "",
    "// -- Revenue tab charts",
    "function renderGrowthRate(f){",
    "  var labels=f.map(function(m){return m.month;});",
    "  var rates=f.map(function(m,i){",
    "    if(i===0) return 0;",
    "    var p=f[i-1].closing_mrr;",
    "    return p>0?((m.closing_mrr-p)/p)*100:0;",
    "  });",
    "  upsert('cGR',{type:'bar',data:{labels:labels,datasets:[{label:'MoM Growth %',data:rates,",
    "    backgroundColor:rates.map(function(v){return v>=0?'rgba(99,102,241,0.7)':'rgba(244,63,94,0.7)';}),",
    "    borderColor:rates.map(function(v){return v>=0?'#6366f1':'#f43f5e';}),",
    "    borderWidth:1,borderRadius:4}]},",
    "    options:baseOpts({scales:{x:AX,y:{...AX,ticks:{callback:function(v){return v.toFixed(1)+'%';}}}}})});",
    "}",
    "function renderExCo(f){",
    "  var labels=f.map(function(m){return m.month;});",
    "  upsert('cExCo',{type:'bar',data:{labels:labels,datasets:[",
    "    {label:'Expansion',data:f.map(function(m){return m.expansion;}),",
    "      backgroundColor:'rgba(16,185,129,0.75)',borderColor:'#10b981',borderWidth:1,borderRadius:3},",
    "    {label:'Reactivation',data:f.map(function(m){return m.reactivation;}),",
    "      backgroundColor:'rgba(139,92,246,0.75)',borderColor:'#8b5cf6',borderWidth:1,borderRadius:3},",
    "    {label:'Contraction',data:f.map(function(m){return -m.contraction;}),",
    "      backgroundColor:'rgba(245,158,11,0.75)',borderColor:'#f59e0b',borderWidth:1,borderRadius:3},",
    "  ]},options:baseOpts({scales:{x:AX,y:AX},",
    "    plugins:{...baseOpts().plugins,legend:{display:true,position:'bottom',",
    "      labels:{boxWidth:10,padding:14,font:{size:10},color:CLR.tk}}}})});",
    "}",
    "function renderCust(f){",
    "  var labels=f.map(function(m){return m.month;});",
    "  upsert('cCu',{type:'bar',data:{labels:labels,datasets:[",
    "    {type:'bar',label:'Active Customers',data:f.map(function(m){return m.active_customers;}),",
    "      backgroundColor:'rgba(99,102,241,0.5)',borderColor:'#6366f1',borderWidth:1,borderRadius:3,yAxisID:'y'},",
    "    {type:'line',label:'Active Subscriptions',data:f.map(function(m){return m.active_subscriptions;}),",
    "      borderColor:'#10b981',backgroundColor:'rgba(16,185,129,0.08)',",
    "      borderWidth:2,pointRadius:3,tension:.35,fill:false,yAxisID:'y2'},",
    "  ]},options:baseOpts({scales:{",
    "    y:{...AX,title:{display:true,text:'Customers',color:CLR.tk,font:{size:10}}},",
    "    y2:{...AX,position:'right',grid:{drawOnChartArea:false},",
    "      title:{display:true,text:'Subscriptions',color:CLR.tk,font:{size:10}}}},",
    "    plugins:{...baseOpts().plugins,legend:{display:true,position:'bottom',",
    "      labels:{boxWidth:10,padding:14,font:{size:10},color:CLR.tk}}}})});",
    "}",
    "function renderArpa(f){",
    "  var labels=f.map(function(m){return m.month;});",
    "  upsert('cAR',{type:'line',data:{labels:labels,datasets:[{label:'ARPA',",
    "    data:f.map(function(m){return m.arpa;}),",
    "    borderColor:'#06b6d4',backgroundColor:'rgba(6,182,212,0.08)',",
    "    borderWidth:2.5,pointRadius:3,pointHoverRadius:5,tension:.4,fill:true}]},",
    "    options:baseOpts({scales:{x:AX,y:{...AX,ticks:{callback:function(v){return '$'+v.toFixed(0);}}}}})});",
    "}",
    "function renderPlanMix(){",
    "  var pm=DATA.plan_mix.slice().sort(function(a,b){return b.mrr-a.mrr;}).slice(0,10);",
    "  var labels=pm.map(function(p){return p.plan;});",
    "  var vals=pm.map(function(p){return p.mrr;});",
    "  var tot=vals.reduce(function(s,v){return s+v;},0);",
    "  var bgs=PLAN_PAL.slice(0,pm.length);",
    "  upsert('cPM',{type:'doughnut',data:{labels:labels,datasets:[{data:vals,",
    "    backgroundColor:bgs,borderColor:'rgba(7,9,15,0.8)',borderWidth:2,hoverOffset:6}]},",
    "    options:{responsive:true,maintainAspectRatio:false,animation:{duration:400},cutout:'68%',",
    "      plugins:{legend:{display:false},",
    "        tooltip:{backgroundColor:'rgba(7,9,15,0.95)',borderColor:'rgba(255,255,255,0.12)',",
    "          borderWidth:1,padding:12,titleColor:'#fff',bodyColor:'#94a3b8',callbacks:{",
    "            label:function(c){return ' '+c.label+': $'+Math.round(c.raw).toLocaleString()+",
    "              ' ('+((c.raw/tot)*100).toFixed(1)+'%)';}}}}}});",
    "  document.getElementById('planLegend').innerHTML=pm.map(function(p,i){",
    "    var pct=((p.mrr/tot)*100).toFixed(1);",
    "    return '<div class=\"cleg-item\"><div class=\"cleg-dot\" style=\"background:'+bgs[i]+'\"></div>'+",
    "      '<span>'+p.plan+' <b style=\"color:#e2e8f0\">'+pct+'%</b></span></div>';",
    "  }).join('');",
    "}",
    "",
    "// -- Main table (sortable, fixed onclick using data-col)",
    "var TABLE_COLS=[",
    "  {k:'month',              lbl:'Month',         fmt:function(v){return v;},           cls:''},",
    "  {k:'opening_mrr',        lbl:'Opening MRR',   fmt:f0,                               cls:''},",
    "  {k:'new_business',       lbl:'New Biz',       fmt:function(v){return v>0?'+'+f0(v):f0(v);},cls:'cp'},",
    "  {k:'expansion',          lbl:'Expansion',     fmt:function(v){return v>0?'+'+f0(v):f0(v);},cls:'cp'},",
    "  {k:'reactivation',       lbl:'Reactivation',  fmt:function(v){return v>0?'+'+f0(v):f0(v);},cls:'cp'},",
    "  {k:'contraction',        lbl:'Contraction',   fmt:function(v){return v>0?'-'+f0(v):f0(v);},cls:'cn'},",
    "  {k:'churn',              lbl:'Churn',         fmt:function(v){return v>0?'-'+f0(v):f0(v);},cls:'cn'},",
    "  {k:'net_new_mrr',        lbl:'Net New MRR',   fmt:function(v){return (v>=0?'+':'')+f0(Math.abs(v));},cls:''},",
    "  {k:'closing_mrr',        lbl:'Closing MRR',   fmt:f0,                               cls:''},",
    "  {k:'arr',                lbl:'ARR',           fmt:f0,                               cls:''},",
    "  {k:'active_customers',   lbl:'Customers',     fmt:fN,                               cls:''},",
    "  {k:'new_customers',      lbl:'New Cust',      fmt:fN,                               cls:'cp'},",
    "  {k:'churned_customers',  lbl:'Churned',       fmt:fN,                               cls:'cn'},",
    "  {k:'active_subscriptions',lbl:'Subs',         fmt:fN,                               cls:''},",
    "  {k:'arpa',               lbl:'ARPA',          fmt:f2,                               cls:''},",
    "  {k:'mrr_churn_rate',     lbl:'MRR Churn%',    fmt:fP,                               cls:''},",
    "  {k:'net_mrr_churn_rate', lbl:'Net Churn%',    fmt:fP,                               cls:''},",
    "  {k:'customer_churn_rate',lbl:'Cust Churn%',   fmt:fP,                               cls:''},",
    "];",
    "function sortTable(col){",
    "  S.sortCol===col?S.sortDir*=-1:(S.sortCol=col,S.sortDir=1);",
    "  renderTable(filtered());",
    "}",
    "function renderTable(f){",
    "  var rows=f.slice();",
    "  rows.sort(function(a,b){",
    "    var av=a[S.sortCol],bv=b[S.sortCol];",
    "    return typeof av==='string'?av.localeCompare(bv)*S.sortDir:(av-bv)*S.sortDir;",
    "  });",
    "  var q=S.tableSearch.toLowerCase();",
    "  if(q) rows=rows.filter(function(r){return r.month.toLowerCase().indexOf(q)>=0;});",
    "  var hdr='<thead><tr>'+TABLE_COLS.map(function(c){",
    "    var sc=S.sortCol===c.k?(S.sortDir===1?' sort-asc':' sort-desc'):'';",
    "    return '<th class=\"'+sc+'\" data-col=\"'+c.k+'\" onclick=\"sortTable(this.dataset.col)\">'+c.lbl+'</th>';",
    "  }).join('')+'</tr></thead>';",
    "  var body='<tbody>'+rows.map(function(row){",
    "    return '<tr>'+TABLE_COLS.map(function(c,ci){",
    "      var cls=c.cls;",
    "      if(c.k==='net_new_mrr') cls=row.net_new_mrr>=0?'cp':'cn';",
    "      return '<td class=\"'+cls+'\">'+(ci===0?'<b>'+c.fmt(row[c.k])+'</b>':c.fmt(row[c.k]))+'</td>';",
    "    }).join('')+'</tr>';",
    "  }).join('')+'</tbody>';",
    "  var sumK=['new_business','expansion','reactivation','contraction','churn','net_new_mrr','new_customers','churned_customers'];",
    "  var foot='<tfoot><tr style=\"font-weight:700;background:rgba(255,255,255,0.04)\">'+",
    "    TABLE_COLS.map(function(c,i){",
    "      if(i===0) return '<td>Total / Last</td>';",
    "      if(sumK.indexOf(c.k)>=0){",
    "        var tot=rows.reduce(function(s,r){return s+r[c.k];},0);",
    "        var cls=c.cls;if(c.k==='net_new_mrr') cls=tot>=0?'cp':'cn';",
    "        return '<td class=\"'+cls+'\">'+c.fmt(tot)+'</td>';",
    "      }",
    "      return rows.length?'<td>'+c.fmt(rows[rows.length-1][c.k])+'</td>':'<td>--</td>';",
    "    }).join('')+'</tr></tfoot>';",
    "  document.getElementById('dtbl').innerHTML=hdr+body+foot;",
    "  document.getElementById('tFootInfo').textContent=rows.length+' months shown';",
    "}",
    "function downloadCSV(){",
    "  var f=filtered();",
    "  var keys=TABLE_COLS.map(function(c){return c.k;});",
    "  var rows=f.map(function(r){return keys.map(function(k){return r[k];}).join(',');});",
    "  var csv=[keys.join(',')].concat(rows).join('\\n');",
    "  var a=document.createElement('a');",
    "  a.href='data:text/csv;charset=utf-8,'+encodeURIComponent(csv);",
    "  a.download='pulse_mrr_'+f[f.length-1].month+'.csv';",
    "  a.click();",
    "}",
    "",
    "// -- Customer page",
    "var CUST_COLS=[",
    "  {k:'email',       lbl:'Email',         fmt:function(v){return v;},     cls:'email-cell'},",
    "  {k:'plan',        lbl:'Plan',          fmt:function(v){return v;},     cls:''},",
    "  {k:'status',      lbl:'Status',        fmt:function(v){return v==='active'?'<span class=\"badge badge-active\">Active</span>':'<span class=\"badge badge-churned\">Churned</span>';},cls:''},",
    "  {k:'current_mrr', lbl:'Current MRR',   fmt:f0,                         cls:''},",
    "  {k:'peak_mrr',    lbl:'Peak MRR',      fmt:f0,                         cls:''},",
    "  {k:'total_paid',  lbl:'Total Paid',    fmt:f0,                         cls:'cp'},",
    "  {k:'months_active',lbl:'Months Active',fmt:function(v){return v;},     cls:''},",
    "  {k:'first_month', lbl:'First Seen',    fmt:function(v){return v;},     cls:'nu'},",
    "  {k:'last_active', lbl:'Last Active',   fmt:function(v){return v;},     cls:'nu'},",
    "];",
    "function setStatusFilter(status,btn){",
    "  CS.status=status;CS.page=0;",
    "  document.querySelectorAll('.stog').forEach(function(b){b.classList.remove('active');});",
    "  if(btn) btn.classList.add('active');",
    "  renderCustomerList();",
    "}",
    "function filteredCustomers(){",
    "  var q=CS.search.toLowerCase().trim();",
    "  return CUSTOMERS.filter(function(c){",
    "    if(q&&c.email.toLowerCase().indexOf(q)<0) return false;",
    "    if(CS.status!=='all'&&c.status!==CS.status) return false;",
    "    if(CS.plan&&c.plan!==CS.plan) return false;",
    "    return true;",
    "  }).sort(function(a,b){",
    "    var av=a[CS.sortCol],bv=b[CS.sortCol];",
    "    return typeof av==='string'?av.localeCompare(bv)*CS.sortDir:(av-bv)*CS.sortDir;",
    "  });",
    "}",
    "function sortCustomers(col){",
    "  CS.sortCol===col?CS.sortDir*=-1:(CS.sortCol=col,CS.sortDir=1);",
    "  CS.page=0;renderCustomerList();",
    "}",
    "function renderCustomerList(){",
    "  var all=filteredCustomers();",
    "  var total=all.length;",
    "  var pp=CS.perPage;",
    "  var maxPage=Math.ceil(total/pp)-1;",
    "  if(CS.page>maxPage) CS.page=Math.max(0,maxPage);",
    "  var rows=all.slice(CS.page*pp,(CS.page+1)*pp);",
    "  document.getElementById('custCount').textContent='Showing '+(CS.page*pp+1)+'-'+Math.min((CS.page+1)*pp,total)+' of '+total.toLocaleString()+' customers';",
    "  // Summary bar",
    "  var actN=CUSTOMERS.filter(function(c){return c.status==='active';}).length;",
    "  var chN=CUSTOMERS.filter(function(c){return c.status==='churned';}).length;",
    "  var totMRR=CUSTOMERS.filter(function(c){return c.status==='active';}).reduce(function(s,c){return s+c.current_mrr;},0);",
    "  document.getElementById('custSummary').innerHTML=",
    "    '<div class=\"si\"><div class=\"sd\" style=\"background:#10b981\"></div><span class=\"sl\">Active</span><span class=\"sv\">'+actN.toLocaleString()+'</span></div>'+",
    f"    '<div class=\"si\"><div class=\"sd\" style=\"background:#f43f5e\"></div><span class=\"sl\">Churned</span><span class=\"sv\">'+chN.toLocaleString()+'</span><span style=\"font-size:10px;color:var(--fa);margin-left:5px\">since {data_start}</span></div>'+",
    "    '<div class=\"si\"><div class=\"sd\" style=\"background:#6366f1\"></div><span class=\"sl\">Active MRR</span><span class=\"sv\">'+f0(totMRR)+'</span></div>'+",
    "    '<div class=\"si\"><div class=\"sd\" style=\"background:#f59e0b\"></div><span class=\"sl\">Total Tracked</span><span class=\"sv\">'+CUSTOMERS.length.toLocaleString()+'</span></div>';",
    "  // Table header",
    "  var hdr='<thead><tr>'+CUST_COLS.map(function(c){",
    "    var sc=CS.sortCol===c.k?(CS.sortDir===1?' sort-asc':' sort-desc'):'';",
    "    return '<th class=\"'+sc+'\" data-col=\"'+c.k+'\" onclick=\"sortCustomers(this.dataset.col)\">'+c.lbl+'</th>';",
    "  }).join('')+'</tr></thead>';",
    "  var body='<tbody>'+rows.map(function(row){",
    "    var email=row.email;",
    "    return '<tr class=\"cust-row\" onclick=\"openCustomer('+JSON.stringify(email)+')\">'",
    "      +CUST_COLS.map(function(c){",
    "        return '<td class=\"'+c.cls+'\">'+c.fmt(row[c.k])+'</td>';",
    "      }).join('')+'</tr>';",
    "  }).join('')+'</tbody>';",
    "  document.getElementById('custTbl').innerHTML=hdr+body;",
    "  // Pagination",
    "  var pi=CS.page,pm=Math.max(0,maxPage);",
    "  document.getElementById('custPag').innerHTML=",
    "    '<button class=\"cpag-btn\" onclick=\"CS.page=0;renderCustomerList()\" '+(pi===0?'disabled':'')+'>First</button>'+",
    "    '<button class=\"cpag-btn\" onclick=\"CS.page--;renderCustomerList()\" '+(pi===0?'disabled':'')+'>Prev</button>'+",
    "    '<span class=\"cpag-info\">Page '+(pi+1)+' of '+(pm+1)+'</span>'+",
    "    '<button class=\"cpag-btn\" onclick=\"CS.page++;renderCustomerList()\" '+(pi>=pm?'disabled':'')+'>Next</button>'+",
    "    '<button class=\"cpag-btn\" onclick=\"CS.page='+pm+';renderCustomerList()\" '+(pi>=pm?'disabled':'')+'>Last</button>';",
    "}",
    "",
    "// -- Customer detail",
    "function openCustomer(email){",
    "  var cust=null;",
    "  for(var i=0;i<CUSTOMERS.length;i++){if(CUSTOMERS[i].email===email){cust=CUSTOMERS[i];break;}}",
    "  if(!cust) return;",
    "  document.getElementById('cdEmail').textContent=cust.email;",
    "  document.getElementById('cdBadge').innerHTML=cust.status==='active'?",
    "    '<span class=\"badge badge-active\">Active</span>':'<span class=\"badge badge-churned\">Churned</span>';",
    "  document.getElementById('cdPlan').textContent=cust.plan;",
    "  document.getElementById('cdSince').textContent='Since '+cust.first_month;",
    "  var kpis=[",
    "    {l:'Current MRR',v:f0(cust.current_mrr),b:'#6366f1'},",
    "    {l:'Total Paid',v:f0(cust.total_paid),b:'#10b981'},",
    "    {l:'Months Active',v:cust.months_active,b:'#06b6d4'},",
    "    {l:'Peak MRR',v:f0(cust.peak_mrr),b:'#f59e0b'},",
    "  ];",
    "  document.getElementById('cdKgrid').innerHTML=kpis.map(function(k){",
    "    return '<div class=\"kcard\" style=\"border-left-color:'+k.b+'\">'+",
    "      '<div class=\"klbl\">'+k.l+'</div><div class=\"kval\">'+k.v+'</div></div>';",
    "  }).join('');",
    "  // MRR history chart",
    "  var h=cust.h;",
    "  var bgs=h.map(function(v){return v>0?'rgba(99,102,241,0.75)':'rgba(255,255,255,0.04)';});",
    "  var bds=h.map(function(v){return v>0?'#6366f1':'rgba(255,255,255,0.08)';});",
    "  upsert('cCustHist',{type:'bar',data:{labels:MONTHS,datasets:[{label:'MRR',data:h,",
    "    backgroundColor:bgs,borderColor:bds,borderWidth:1,borderRadius:4}]},",
    "    options:baseOpts({scales:{x:AX,y:{...AX,ticks:{callback:function(v){return v>0?'$'+v.toFixed(0):'';}}}}})}); ",
    "  // Month-by-month table",
    "  var trows=MONTHS.map(function(m,i){",
    "    var mrr=h[i];",
    "    var cls=mrr>0?'':'color:var(--fa)';",
    "    return '<tr style=\"'+cls+'\"><td><b>'+m+'</b></td><td>'+(mrr>0?f0(mrr):'--')+'</td>'+",
    "      '<td>'+(mrr>0?'<span class=\"badge badge-active\">Active</span>':'<span class=\"badge badge-churned\">Inactive</span>')+'</td></tr>';",
    "  }).join('');",
    "  document.getElementById('cdTable').innerHTML=",
    "    '<thead><tr><th style=\"text-align:left\">Month</th><th>MRR</th><th>Status</th></tr></thead>'+",
    "    '<tbody>'+trows+'</tbody>';",
    "  // Navigate to detail page without changing tab button state",
    "  document.querySelectorAll('.tab-page').forEach(function(p){p.classList.remove('active');});",
    "  document.getElementById('page-customer-detail').classList.add('active');",
    "  S.page='customer-detail';",
    "  setTimeout(function(){try{CH['cCustHist'].resize();}catch(e){}},60);",
    "}",
    "",
    "// -- updateAll / updateOverview / updateRevenue",
    "function updateOverview(){",
    "  var f=filtered();",
    "  renderKPIs(f);renderInsights(f);renderStats(f);",
    "  renderMrrTrend('cT',f);renderNetNew(f);",
    "  renderChurn(f);renderNvC(f);",
    "  renderTable(f);",
    "}",
    "function updateRevenue(){",
    "  var f=filtered();",
    "  renderGrowthRate(f);renderExCo(f);",
    "  renderMrrTrend('cT2',f);",
    "  renderCust(f);renderArpa(f);renderPlanMix();",
    "}",
    "function updateAll(){",
    "  if(S.page==='overview') updateOverview();",
    "  else if(S.page==='revenue') updateRevenue();",
    "}",
    "",
    "// -- Init",
    "function initSelects(){",
    "  var ss=document.getElementById('selS'),se=document.getElementById('selE');",
    "  ss.innerHTML='';se.innerHTML='';",
    "  MONTHS.forEach(function(m,i){",
    "    var o1=document.createElement('option');o1.value=i;o1.textContent=m;ss.appendChild(o1);",
    "    var o2=document.createElement('option');o2.value=i;o2.textContent=m;se.appendChild(o2);",
    "  });",
    "  ss.value=S.start;se.value=S.end;",
    "}",
    "function initToggles(){",
    "  var c=document.getElementById('movTogs');c.innerHTML='';",
    "  MOVS.forEach(function(mv){",
    "    var b=document.createElement('button');",
    "    b.className='tog';b.id='tog_'+mv.key;b.textContent=mv.lbl;",
    "    b.style.borderColor=mv.clr;b.style.color=mv.clr;b.style.background=mv.clr+'18';",
    "    b.onclick=function(){toggleMov(mv.key);};",
    "    c.appendChild(b);",
    "  });",
    "}",
    "function initPlanFilter(){",
    "  var sel=document.getElementById('custPlan');",
    "  PLAN_OPTIONS.forEach(function(p){",
    "    var o=document.createElement('option');o.value=p;o.textContent=p;sel.appendChild(o);",
    "  });",
    "}",
    "function initDash(){",
    "  initSelects();initToggles();initPlanFilter();",
    "  renderPlanMix();updateOverview();",
    "}",
    "",
    "document.addEventListener('DOMContentLoaded',function(){});",
]

JS = "\n".join(JS_LINES)

# Verify 100% ASCII in JS
bad = [(i,c) for i,c in enumerate(JS) if ord(c) > 127]
if bad:
    for pos,ch in bad[:10]:
        print(f"  WARNING non-ASCII at {pos}: U+{ord(ch):04X} '{ch}' context: {repr(JS[max(0,pos-20):pos+20])}")
    sys.exit(1)
else:
    print("  JS block: 100% ASCII - no encoding issues")

HTML = (HEAD + "\n<body>\n" + LOGIN + "\n" + DASH_SHELL +
        "\n<script>\n" + JS + "\n</script>\n</body>\n</html>\n")

with open(OUTPUT, 'w', encoding='utf-8') as f:
    f.write(HTML)

print(f"  Pulse dashboard -> {OUTPUT}")
print(f"  Range: {data_start} to {data_end} | MRR: {cur_mrr} | ARR: {cur_arr}")
print(f"  Customers: {n_customers:,} | Size: {len(HTML)//1024} KB")
print(f"  Login: quso / Revenue2025")
