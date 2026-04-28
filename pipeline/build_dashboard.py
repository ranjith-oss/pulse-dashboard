#!/usr/bin/env python3
"""
build_dashboard.py  —  Generate interactive single-file HTML dashboard
Reads pipeline/mrr_data.json → writes index.html in repo root.
"""
import json, sys
from pathlib import Path
from datetime import date

ROOT     = Path(__file__).parent.parent
PIPE     = Path(__file__).parent
OUT_PATH = ROOT / "index.html"
DATA_PATH= PIPE / "mrr_data.json"

if not DATA_PATH.exists():
    sys.exit(f"ERROR: {DATA_PATH} not found. Run compute_mrr.py first.")

with open(DATA_PATH) as f:
    data = json.load(f)

# Embed data as JS constant
data_js = json.dumps(data, separators=(',', ':'))

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Vidyo.ai — MRR Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root {{
    --bg: #0f1117; --surface: #1a1d27; --surface2: #22263a;
    --border: #2e3250; --text: #e2e8f0; --muted: #8b9cc8;
    --green: #10b981; --red: #ef4444; --blue: #3b82f6;
    --purple: #a78bfa; --orange: #f59e0b; --teal: #14b8a6;
    --accent: #6366f1;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: 'Inter', system-ui, sans-serif; min-height:100vh; }}
  a {{ color: var(--accent); text-decoration:none; }}

  /* Header */
  .header {{ background: var(--surface); border-bottom: 1px solid var(--border); padding: 16px 24px; display:flex; align-items:center; justify-content:space-between; }}
  .header h1 {{ font-size: 1.25rem; font-weight: 700; letter-spacing: -0.02em; }}
  .header h1 span {{ color: var(--accent); }}
  .header .meta {{ font-size: 0.75rem; color: var(--muted); text-align:right; }}

  /* Layout */
  .container {{ max-width: 1400px; margin: 0 auto; padding: 24px; }}

  /* Metric cards */
  .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 24px; }}
  .card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 16px; }}
  .card .label {{ font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); margin-bottom: 6px; }}
  .card .value {{ font-size: 1.5rem; font-weight: 700; line-height: 1; }}
  .card .sub {{ font-size: 0.7rem; color: var(--muted); margin-top: 4px; }}
  .card.green .value {{ color: var(--green); }}
  .card.red .value {{ color: var(--red); }}
  .card.blue .value {{ color: var(--blue); }}
  .card.purple .value {{ color: var(--purple); }}
  .card.orange .value {{ color: var(--orange); }}
  .card.teal .value {{ color: var(--teal); }}

  /* Section headers */
  .section-header {{ display:flex; align-items:center; justify-content:space-between; margin-bottom: 12px; }}
  .section-header h2 {{ font-size: 0.85rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: var(--muted); }}
  .hint {{ font-size: 0.7rem; color: var(--muted); font-style: italic; }}

  /* Chart containers */
  .chart-wrap {{ background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 20px; margin-bottom: 20px; }}
  .chart-wrap canvas {{ cursor: pointer; }}

  /* Drill-down panel */
  #drill-panel {{
    display: none; background: var(--surface); border: 1px solid var(--accent);
    border-radius: 10px; padding: 20px; margin-bottom: 20px;
    animation: fadeIn 0.2s ease;
  }}
  @keyframes fadeIn {{ from {{opacity:0;transform:translateY(-8px)}} to {{opacity:1;transform:translateY(0)}} }}
  #drill-panel h3 {{ font-size: 1rem; font-weight: 700; margin-bottom: 16px; color: var(--accent); }}
  .drill-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  @media(max-width:700px) {{ .drill-grid {{ grid-template-columns: 1fr; }} }}
  .drill-box {{ background: var(--surface2); border-radius: 8px; padding: 14px; }}
  .drill-box h4 {{ font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.07em; color: var(--muted); margin-bottom: 10px; }}
  .movement-row {{ display:flex; justify-content:space-between; align-items:center; padding: 5px 0; border-bottom: 1px solid var(--border); font-size: 0.82rem; }}
  .movement-row:last-child {{ border-bottom: none; }}
  .movement-row .amt {{ font-weight: 600; }}
  .amt.pos {{ color: var(--green); }}
  .amt.neg {{ color: var(--red); }}
  .mover-row {{ display:flex; justify-content:space-between; align-items:center; padding: 4px 0; font-size: 0.78rem; color: var(--text); border-bottom: 1px solid var(--border); }}
  .mover-row:last-child {{ border-bottom: none; }}
  .mover-name {{ overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 200px; }}
  .mover-amt {{ font-weight: 600; white-space: nowrap; margin-left: 8px; }}
  .plan-row {{ display:flex; justify-content:space-between; align-items:center; padding: 4px 0; font-size: 0.78rem; border-bottom: 1px solid var(--border); }}
  .plan-row:last-child {{ border-bottom: none; }}
  .plan-bar {{ height: 4px; border-radius: 2px; background: var(--accent); margin-top: 3px; }}

  /* Source pill */
  .source-pill {{ display:inline-block; font-size:0.62rem; padding:2px 7px; border-radius:10px; font-weight:600; text-transform:uppercase; letter-spacing:0.05em; margin-left:8px; }}
  .src-chartmogul {{ background:#1e3a5f; color:#60a5fa; }}
  .src-paddle {{ background:#1e4d2e; color:#34d399; }}

  /* Two-col grid */
  .two-col {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:20px; }}
  @media(max-width:900px) {{ .two-col {{ grid-template-columns:1fr; }} }}

  /* Tabs */
  .tabs {{ display:flex; gap:4px; margin-bottom:14px; }}
  .tab {{ padding:5px 14px; border-radius:6px; font-size:0.75rem; font-weight:600; cursor:pointer; border:1px solid var(--border); color:var(--muted); transition:all 0.15s; }}
  .tab:hover {{ border-color: var(--accent); color: var(--text); }}
  .tab.active {{ background: var(--accent); border-color: var(--accent); color: white; }}

  /* Plan selector */
  .plan-toggles {{ display:flex; flex-wrap:wrap; gap:6px; margin-bottom:12px; }}
  .plan-toggle {{ padding:3px 10px; border-radius:12px; font-size:0.7rem; cursor:pointer; border:1px solid var(--border); transition:all 0.15s; white-space:nowrap; }}
  .plan-toggle.on {{ border-color: transparent; color: white; }}

  /* Footer */
  .footer {{ text-align:center; font-size:0.7rem; color: var(--muted); padding: 24px 0 40px; }}

  /* Close btn */
  .close-btn {{ float:right; cursor:pointer; color:var(--muted); font-size:1rem; line-height:1; padding:4px 8px; border-radius:4px; }}
  .close-btn:hover {{ background: var(--surface2); color: var(--text); }}
</style>
</head>
<body>

<div class="header">
  <h1>Vidyo<span>.ai</span> — MRR Dashboard</h1>
  <div class="meta">
    Last updated: <strong id="last-updated">—</strong><br>
    <span id="source-note" style="font-size:0.65rem;opacity:0.7;"></span>
  </div>
</div>

<div class="container">

  <!-- Metric Cards -->
  <div class="cards" id="metric-cards"></div>

  <!-- Main MRR Chart -->
  <div class="chart-wrap">
    <div class="section-header">
      <h2>Monthly Recurring Revenue</h2>
      <span class="hint">Click any bar to drill down</span>
    </div>
    <canvas id="mrr-chart" height="90"></canvas>
  </div>

  <!-- Drill-down Panel -->
  <div id="drill-panel">
    <span class="close-btn" onclick="closeDrill()">✕</span>
    <h3 id="drill-title">—</h3>
    <div class="drill-grid" id="drill-content"></div>
  </div>

  <!-- Movements Waterfall -->
  <div class="chart-wrap">
    <div class="section-header">
      <h2>MRR Movements Waterfall</h2>
      <div class="tabs" id="waterfall-tabs">
        <div class="tab active" onclick="setWaterfallRange('all',this)">All time</div>
        <div class="tab" onclick="setWaterfallRange('24',this)">24m</div>
        <div class="tab" onclick="setWaterfallRange('12',this)">12m</div>
        <div class="tab" onclick="setWaterfallRange('6',this)">6m</div>
      </div>
    </div>
    <canvas id="waterfall-chart" height="90"></canvas>
  </div>

  <!-- Plan Trend + NRR Side by Side -->
  <div class="two-col">

    <div class="chart-wrap" style="margin-bottom:0">
      <div class="section-header"><h2>Plan-wise MRR Trend</h2></div>
      <div class="plan-toggles" id="plan-toggles"></div>
      <canvas id="plan-chart" height="200"></canvas>
    </div>

    <div class="chart-wrap" style="margin-bottom:0">
      <div class="section-header"><h2>Key Metrics Trend</h2></div>
      <div class="tabs" id="metric-tabs">
        <div class="tab active" onclick="setMetricLine('arpa',this)">ARPA</div>
        <div class="tab" onclick="setMetricLine('customers',this)">Customers</div>
        <div class="tab" onclick="setMetricLine('nrr',this)">NRR %</div>
        <div class="tab" onclick="setMetricLine('qr',this)">Quick Ratio</div>
        <div class="tab" onclick="setMetricLine('churn_rate',this)">Logo Churn</div>
      </div>
      <canvas id="metric-line-chart" height="200"></canvas>
    </div>

  </div>

</div>

<div class="footer">
  Powered by ChartMogul + Paddle Reports API &nbsp;·&nbsp;
  Data: <span id="data-range">—</span> &nbsp;·&nbsp;
  <span id="total-months">—</span>
</div>

<script>
const DATA = {data_js};

// ── Helpers ─────────────────────────────────────────────────────────────
const fmt  = (n, dec=0) => n == null ? '—' : '$' + n.toLocaleString('en-US', {{minimumFractionDigits:dec, maximumFractionDigits:dec}});
const fmtN = (n) => n == null ? '—' : n.toLocaleString('en-US');
const pct  = (n) => n == null ? '—' : n.toFixed(1) + '%';

const PLAN_COLORS = [
  '#6366f1','#10b981','#f59e0b','#ef4444','#3b82f6','#a78bfa',
  '#14b8a6','#f97316','#84cc16','#ec4899','#06b6d4','#8b5cf6',
  '#22c55e','#eab308','#e879f9','#0ea5e9','#d946ef','#fb923c',
];

// ── Render metric cards ──────────────────────────────────────────────────
function renderCards() {{
  const s = DATA.summary;
  const m = DATA.monthly;
  const last = m[m.length-1];
  const prev = m[m.length-2];
  const momChange = prev ? ((last.mrr - prev.mrr) / prev.mrr * 100).toFixed(1) : null;
  const cards = [
    {{ label:'Current MRR',    value: fmt(s.current_mrr), sub: momChange != null ? (momChange>=0?'+':'')+momChange+'% MoM' : '', cls:'blue' }},
    {{ label:'ARR',            value: fmt(s.current_arr), sub:`as of ${{s.data_end}}`, cls:'blue' }},
    {{ label:'Active Customers', value: fmtN(s.active_customers), sub:`ARPA ${{fmt(s.current_arpa,2)}}`, cls:'purple' }},
    {{ label:'Peak MRR',       value: fmt(s.peak_mrr), sub:s.peak_month, cls:'orange' }},
    {{ label:'Avg NRR (12m)',  value: pct(s.avg_nrr), sub:'Net Revenue Retention', cls: s.avg_nrr >= 100 ? 'green' : 'red' }},
    {{ label:'Quick Ratio (12m)', value: fmtN(s.quick_ratio)+'x', sub:'New+Exp / Churn+Cont', cls: s.quick_ratio >= 1 ? 'green' : 'red' }},
    {{ label:'MRR Growth',     value: (s.mrr_growth_pct>=0?'+':'')+s.mrr_growth_pct+'%', sub:`${{s.data_start}} → ${{s.data_end}}`, cls: s.mrr_growth_pct>=0?'green':'red' }},
    {{ label:'Data Period',    value: s.total_months+'m', sub:`${{s.data_start}} → ${{s.data_end}}`, cls:'teal' }},
  ];
  document.getElementById('metric-cards').innerHTML = cards.map(c =>
    `<div class="card ${{c.cls}}">
       <div class="label">${{c.label}}</div>
       <div class="value">${{c.value}}</div>
       ${{c.sub ? `<div class="sub">${{c.sub}}</div>` : ''}}
     </div>`
  ).join('');
  document.getElementById('last-updated').textContent = s.last_updated;
  document.getElementById('source-note').textContent  = s.source_note;
  document.getElementById('data-range').textContent   = s.data_start + ' → ' + s.data_end;
  document.getElementById('total-months').textContent  = s.total_months + ' months of data';
}}

// ── Main MRR chart ───────────────────────────────────────────────────────
let mrrChart;
function renderMRRChart() {{
  const ctx = document.getElementById('mrr-chart').getContext('2d');
  const labels = DATA.monthly.map(m => m.month);
  const vals   = DATA.monthly.map(m => m.mrr);
  const bgColors = DATA.monthly.map(m => m.source === 'paddle' ? 'rgba(99,102,241,0.75)' : 'rgba(59,130,246,0.65)');
  const bdrColors= DATA.monthly.map(m => m.source === 'paddle' ? '#818cf8' : '#60a5fa');

  mrrChart = new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels,
      datasets: [
        {{
          type: 'line',
          label: 'MRR Trend',
          data: vals,
          borderColor: '#a78bfa',
          borderWidth: 2,
          pointRadius: 3,
          pointHoverRadius: 5,
          fill: false,
          tension: 0.35,
          yAxisID: 'y',
          order: 0,
        }},
        {{
          type: 'bar',
          label: 'MRR',
          data: vals,
          backgroundColor: bgColors,
          borderColor: bdrColors,
          borderWidth: 1,
          borderRadius: 3,
          yAxisID: 'y',
          order: 1,
        }}
      ]
    }},
    options: {{
      responsive: true,
      interaction: {{ mode:'index', intersect:false }},
      plugins: {{
        legend: {{ display:false }},
        tooltip: {{
          callbacks: {{
            label: (ctx) => {{
              if (ctx.datasetIndex !== 1) return null;
              const m = DATA.monthly[ctx.dataIndex];
              return [
                ` MRR: ${{fmt(m.mrr)}}`,
                ` Customers: ${{fmtN(m.customers)}}`,
                ` ARPA: ${{fmt(m.arpa,2)}}`,
                ` Source: ${{m.source}}`,
              ];
            }},
            title: (items) => items[0]?.label || '',
          }},
          backgroundColor: '#1a1d27',
          borderColor: '#2e3250',
          borderWidth: 1,
          titleColor: '#e2e8f0',
          bodyColor: '#8b9cc8',
        }},
      }},
      scales: {{
        x: {{ grid:{{color:'#1e2235'}}, ticks:{{color:'#6b7a9f', maxRotation:45, font:{{size:10}}}} }},
        y: {{ grid:{{color:'#1e2235'}}, ticks:{{color:'#6b7a9f', callback: v=>'$'+v.toLocaleString()}} }},
      }},
      onClick: (e, els) => {{
        if (els.length) openDrill(els[0].index);
      }},
    }}
  }});
}}

// ── Drill-down ─────────────────────────────────────────────────────────
function openDrill(idx) {{
  const m = DATA.monthly[idx];
  const panel = document.getElementById('drill-panel');
  const src = m.source === 'paddle'
    ? '<span class="source-pill src-paddle">Paddle</span>'
    : '<span class="source-pill src-chartmogul">ChartMogul</span>';

  document.getElementById('drill-title').innerHTML =
    `${{m.month}} &nbsp; ${{fmt(m.mrr)}} MRR &nbsp; ${{fmtN(m.customers)}} customers &nbsp; ARPA ${{fmt(m.arpa,2)}} ${{src}}`;

  const moves = [
    {{label:'New Business',  val:m.new_biz,      cls:'pos'}},
    {{label:'Expansion',     val:m.expansion,    cls:'pos'}},
    {{label:'Reactivation',  val:m.reactivation, cls:'pos'}},
    {{label:'Contraction',   val:m.contraction,  cls: m.contraction<0?'neg':'pos'}},
    {{label:'Churn',         val:m.churn,        cls:'neg'}},
    {{label:'Net New MRR',   val:m.net_new,      cls: m.net_new>=0?'pos':'neg'}},
  ];

  const movHtml = `<div class="drill-box">
    <h4>MRR Movements</h4>
    ${{moves.map(r=>`<div class="movement-row">
      <span>${{r.label}}</span>
      <span class="amt ${{r.cls}}">${{r.val>=0?'+':''}}${{Math.abs(r.val).toLocaleString('en-US',{{minimumFractionDigits:0,maximumFractionDigits:0}})}}</span>
    </div>`).join('')}}
  </div>`;

  // Plan mix
  const plans = Object.entries(m.plan_mix).sort((a,b)=>b[1]-a[1]);
  const totalPlanMRR = plans.reduce((s,[,v])=>s+v,0);
  const planHtml = `<div class="drill-box">
    <h4>Plan Mix</h4>
    ${{plans.slice(0,10).map(([plan,val])=>`<div class="plan-row">
      <span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:180px">${{plan}}</span>
      <div style="text-align:right">
        <span style="font-weight:600;font-size:0.8rem">${{fmt(val)}}</span>
        <span style="color:var(--muted);font-size:0.7rem;margin-left:4px">${{totalPlanMRR>0?(val/totalPlanMRR*100).toFixed(1)+'%':''}}</span>
      </div>
    </div>`).join('')}}
  </div>`;

  // Top movers (from ChartMogul months)
  const movers = m.top_movers || {{}};
  const mkMoverBox = (type, label, colorCls) => {{
    const list = movers[type] || [];
    if (!list.length) return `<div class="drill-box"><h4>${{label}}</h4><div style="color:var(--muted);font-size:0.75rem;padding:8px 0">No data (Paddle months)</div></div>`;
    return `<div class="drill-box">
      <h4>${{label}}</h4>
      ${{list.slice(0,8).map(r=>`<div class="mover-row">
        <span class="mover-name">${{r.name}}</span>
        <span class="mover-amt" style="color:var(--${{colorCls}})">${{r.amount>=0?'+':''}}${{Math.abs(r.amount).toLocaleString('en-US',{{minimumFractionDigits:0,maximumFractionDigits:0}})}}</span>
      </div>`).join('')}}
    </div>`;
  }};

  document.getElementById('drill-content').innerHTML =
    movHtml + planHtml +
    mkMoverBox('new_biz',    'Top New Customers',   'green') +
    mkMoverBox('churn',      'Top Churned',         'red') +
    mkMoverBox('expansion',  'Top Expanded',        'blue') +
    mkMoverBox('contraction','Top Contracted',      'orange');

  panel.style.display = 'block';
  panel.scrollIntoView({{behavior:'smooth', block:'nearest'}});
}}

function closeDrill() {{
  document.getElementById('drill-panel').style.display = 'none';
}}

// ── Waterfall chart ──────────────────────────────────────────────────────
let waterfallChart;
let wfRange = 'all';
function setWaterfallRange(r, el) {{
  wfRange = r;
  document.querySelectorAll('#waterfall-tabs .tab').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
  renderWaterfall();
}}
function renderWaterfall() {{
  const allM = DATA.monthly;
  const months = wfRange === 'all' ? allM :
                 wfRange === '24'  ? allM.slice(-24) :
                 wfRange === '12'  ? allM.slice(-12) :
                                     allM.slice(-6);
  if (waterfallChart) waterfallChart.destroy();
  const ctx = document.getElementById('waterfall-chart').getContext('2d');
  waterfallChart = new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: months.map(m=>m.month),
      datasets: [
        {{ label:'New Biz',     data: months.map(m=>m.new_biz),      backgroundColor:'#10b981', stack:'s' }},
        {{ label:'Expansion',   data: months.map(m=>m.expansion),    backgroundColor:'#3b82f6', stack:'s' }},
        {{ label:'Reactivation',data: months.map(m=>m.reactivation), backgroundColor:'#a78bfa', stack:'s' }},
        {{ label:'Contraction', data: months.map(m=>m.contraction),  backgroundColor:'#f59e0b', stack:'s' }},
        {{ label:'Churn',       data: months.map(m=>m.churn),        backgroundColor:'#ef4444', stack:'s' }},
      ]
    }},
    options: {{
      responsive: true,
      plugins: {{
        legend: {{ labels:{{ color:'#8b9cc8', font:{{size:11}} }} }},
        tooltip: {{
          callbacks: {{
            label: (ctx) => ` ${{ctx.dataset.label}}: ${{ctx.raw>=0?'+':''}}${{Math.abs(ctx.raw).toLocaleString('en-US',{{maximumFractionDigits:0}})}}`,
          }},
          backgroundColor:'#1a1d27', borderColor:'#2e3250', borderWidth:1,
          titleColor:'#e2e8f0', bodyColor:'#8b9cc8',
        }},
      }},
      scales: {{
        x: {{ stacked:true, grid:{{color:'#1e2235'}}, ticks:{{color:'#6b7a9f', maxRotation:45, font:{{size:10}}}} }},
        y: {{ stacked:true, grid:{{color:'#1e2235'}}, ticks:{{color:'#6b7a9f', callback:v=>'$'+v.toLocaleString()}} }},
      }},
    }}
  }});
}}

// ── Plan trend chart ─────────────────────────────────────────────────────
let planChart;
const activePlans = new Set();
function renderPlanToggles() {{
  const toggles = document.getElementById('plan-toggles');
  DATA.all_plans.forEach((plan, i) => {{
    const color = PLAN_COLORS[i % PLAN_COLORS.length];
    if (i < 8) activePlans.add(plan);  // default: top 8
    const btn = document.createElement('div');
    btn.className = 'plan-toggle' + (i < 8 ? ' on' : '');
    btn.textContent = plan;
    btn.style.borderColor = i < 8 ? color : '#2e3250';
    btn.style.background  = i < 8 ? color+'33' : 'transparent';
    btn.dataset.plan = plan;
    btn.dataset.color = color;
    btn.onclick = () => togglePlan(plan, btn);
    toggles.appendChild(btn);
  }});
}}
function togglePlan(plan, btn) {{
  const color = btn.dataset.color;
  if (activePlans.has(plan)) {{
    activePlans.delete(plan);
    btn.classList.remove('on');
    btn.style.borderColor = '#2e3250';
    btn.style.background  = 'transparent';
  }} else {{
    activePlans.add(plan);
    btn.classList.add('on');
    btn.style.borderColor = color;
    btn.style.background  = color + '33';
  }}
  renderPlanChart();
}}
function renderPlanChart() {{
  if (planChart) planChart.destroy();
  const ctx = document.getElementById('plan-chart').getContext('2d');
  const months = DATA.monthly.map(m=>m.month);
  const datasets = DATA.all_plans
    .filter(p => activePlans.has(p))
    .map((p, i) => {{
      const idx = DATA.all_plans.indexOf(p);
      return {{
        label: p,
        data: months.map(mk => (DATA.plan_trend[p]||{{}})[mk] || 0),
        borderColor: PLAN_COLORS[idx % PLAN_COLORS.length],
        backgroundColor: PLAN_COLORS[idx % PLAN_COLORS.length] + '20',
        borderWidth: 2,
        pointRadius: 0,
        pointHoverRadius: 4,
        fill: false,
        tension: 0.3,
      }};
    }});
  planChart = new Chart(ctx, {{
    type: 'line',
    data: {{ labels: months, datasets }},
    options: {{
      responsive: true,
      interaction: {{ mode:'index', intersect:false }},
      plugins: {{
        legend: {{ display:false }},
        tooltip: {{
          callbacks: {{
            label: ctx => ` ${{ctx.dataset.label}}: ${{fmt(ctx.raw)}}`,
          }},
          backgroundColor:'#1a1d27', borderColor:'#2e3250', borderWidth:1,
          titleColor:'#e2e8f0', bodyColor:'#8b9cc8',
        }},
      }},
      scales: {{
        x: {{ grid:{{color:'#1e2235'}}, ticks:{{color:'#6b7a9f', maxRotation:45, font:{{size:10}}}} }},
        y: {{ grid:{{color:'#1e2235'}}, ticks:{{color:'#6b7a9f', callback:v=>'$'+v.toLocaleString()}} }},
      }},
    }}
  }});
}}

// ── Metric line chart ────────────────────────────────────────────────────
let metricLineChart;
let currentMetric = 'arpa';

function computeMetricData(key) {{
  const m = DATA.monthly;
  if (key === 'arpa') return m.map(r=>r.arpa);
  if (key === 'customers') return m.map(r=>r.customers);
  if (key === 'nrr') {{
    return m.map((r,i) => {{
      if (i===0) return null;
      const prev = m[i-1].mrr;
      if (!prev) return null;
      const retained = prev + r.expansion + r.contraction;
      return +(retained/prev*100).toFixed(1);
    }});
  }}
  if (key === 'qr') {{
    // Trailing 3m quick ratio
    return m.map((r,i) => {{
      const slice = m.slice(Math.max(0,i-2), i+1);
      const num = slice.reduce((s,x)=>s+x.new_biz+x.expansion+x.reactivation,0);
      const den = slice.reduce((s,x)=>s+Math.abs(x.churn)+Math.abs(x.contraction),0);
      return den>0 ? +(num/den).toFixed(2) : null;
    }});
  }}
  if (key === 'churn_rate') {{
    // Logo churn rate = |churn count| / prev customers (approximated from MRR churn / ARPA)
    return m.map((r,i) => {{
      if (i===0) return null;
      const prev = m[i-1];
      if (!prev.mrr || !prev.arpa || prev.arpa===0) return null;
      const churnedCustomers = Math.abs(r.churn) / prev.arpa;
      return +(churnedCustomers/prev.customers*100).toFixed(2);
    }});
  }}
  return [];
}}

function setMetricLine(key, el) {{
  currentMetric = key;
  document.querySelectorAll('#metric-tabs .tab').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
  renderMetricLine();
}}

function renderMetricLine() {{
  if (metricLineChart) metricLineChart.destroy();
  const ctx = document.getElementById('metric-line-chart').getContext('2d');
  const labels = DATA.monthly.map(m=>m.month);
  const vals = computeMetricData(currentMetric);
  const labels_map = {{
    arpa:'ARPA ($)', customers:'Active Customers',
    nrr:'NRR (%)', qr:'Quick Ratio (3m)', churn_rate:'Logo Churn Rate (%)'
  }};
  const colors_map = {{
    arpa:'#f59e0b', customers:'#a78bfa', nrr:'#10b981', qr:'#3b82f6', churn_rate:'#ef4444'
  }};
  const color = colors_map[currentMetric];
  metricLineChart = new Chart(ctx, {{
    type:'line',
    data:{{
      labels,
      datasets:[{{
        label: labels_map[currentMetric],
        data: vals,
        borderColor: color,
        backgroundColor: color+'20',
        borderWidth: 2.5,
        pointRadius: 2,
        pointHoverRadius: 5,
        fill: true,
        tension: 0.35,
        spanGaps: true,
      }}]
    }},
    options:{{
      responsive: true,
      plugins:{{
        legend:{{display:false}},
        tooltip:{{
          callbacks:{{
            label: ctx => {{
              const v = ctx.raw;
              if(v==null) return null;
              if(currentMetric==='arpa') return ` ARPA: ${{fmt(v,2)}}`;
              if(currentMetric==='customers') return ` Customers: ${{fmtN(v)}}`;
              if(currentMetric==='nrr') return ` NRR: ${{v}}%`;
              if(currentMetric==='qr') return ` Quick Ratio: ${{v}}x`;
              if(currentMetric==='churn_rate') return ` Logo Churn: ${{v}}%`;
            }},
          }},
          backgroundColor:'#1a1d27', borderColor:'#2e3250', borderWidth:1,
          titleColor:'#e2e8f0', bodyColor:'#8b9cc8',
        }},
      }},
      scales:{{
        x:{{ grid:{{color:'#1e2235'}}, ticks:{{color:'#6b7a9f', maxRotation:45, font:{{size:10}}}} }},
        y:{{ grid:{{color:'#1e2235'}}, ticks:{{color:'#6b7a9f'}} }},
      }},
    }}
  }});
}}

// ── Init ─────────────────────────────────────────────────────────────────
renderCards();
renderMRRChart();
renderWaterfall();
renderPlanToggles();
renderPlanChart();
renderMetricLine();
</script>
</body>
</html>"""

with open(OUT_PATH, 'w', encoding='utf-8') as f:
    f.write(html)

size_kb = OUT_PATH.stat().st_size / 1024
print(f"  Dashboard written → {OUT_PATH}  ({size_kb:.0f} KB)", flush=True)
print(f"  Months embedded: {len(data['monthly'])}", flush=True)
print(f"  Plans: {data['all_plans'][:5]} …", flush=True)
