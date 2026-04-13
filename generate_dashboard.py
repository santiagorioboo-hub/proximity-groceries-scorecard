#!/usr/bin/env python3
# Proximity Groceries Scorecard - Dashboard Generator
import json, warnings
from datetime import datetime
warnings.filterwarnings('ignore')

today = datetime.now().strftime('%d/%m/%Y')

MONTHS = ["Oct'25","Nov'25","Dic'25","Ene'26","Feb'26","Mar'26"]
MONTHS_NPS = ["Dic'25","Ene'26","Feb'26","Mar'26"]

# ======================================================
# DATA
# ======================================================

growth_data = [
  {'metric':'NMV','fmt':'money','rows':[
    {'s':'Total','v':[10451118,56806992,170234400,175439531,242175980,306868637]},
    {'s':'Caballito','v':[None,None,34055079,44895424,57731083,68029249]},
    {'s':'Scalabrini','v':[10451118,34150904,56456356,50747817,69074828,90765907]},
    {'s':'Vicente Lopez','v':[None,22656087,51330633,46035027,69560421,91646470]},
    {'s':'Villa Urquiza','v':[None,None,28392332,33761265,45809648,56480628]},
  ]},
  {'metric':'NSI','fmt':'num','rows':[
    {'s':'Total','v':[3075,16231,43115,46662,63266,78175]},
    {'s':'Caballito','v':[None,None,9054,12562,16804,19118]},
    {'s':'Scalabrini','v':[3075,9912,14894,13275,17661,23740]},
    {'s':'Vicente Lopez','v':[None,6319,12180,11289,16385,20329]},
    {'s':'Villa Urquiza','v':[None,None,6988,9537,12416,15033]},
  ]},
  {'metric':'Compras','fmt':'num','rows':[
    {'s':'Total','v':[202,987,2927,3098,4244,5262]},
    {'s':'Caballito','v':[None,None,656,837,1122,1309]},
    {'s':'Scalabrini','v':[202,593,946,885,1186,1556]},
    {'s':'Vicente Lopez','v':[None,394,843,794,1151,1438]},
    {'s':'Villa Urquiza','v':[None,None,482,582,785,961]},
  ]},
  {'metric':'Órdenes','fmt':'num','rows':[
    {'s':'Total','v':[1449,6836,17261,19825,26254,33162]},
    {'s':'Caballito','v':[None,None,3652,5241,7075,7866]},
    {'s':'Scalabrini','v':[1449,4144,5688,5588,7063,9899]},
    {'s':'Vicente Lopez','v':[None,2692,4968,4858,6839,8649]},
    {'s':'Villa Urquiza','v':[None,None,2955,4138,5277,6776]},
  ]},
  {'metric':'APV LC','fmt':'dollar','rows':[
    {'s':'Total','v':[51738,57555,58160,56630,57063,58318]},
    {'s':'Caballito','v':[None,None,51913,53638,51454,52059]},
    {'s':'Scalabrini','v':[51738,57590,59679,57342,58242,58391]},
    {'s':'Vicente Lopez','v':[None,57503,60890,57979,60435,63732]},
    {'s':'Villa Urquiza','v':[None,None,58905,58009,58356,58773]},
  ]},
  {'metric':'NASP LC','fmt':'dollar','rows':[
    {'s':'Total','v':[3399,3500,3948,3760,3828,3925]},
    {'s':'Caballito','v':[None,None,3761,3574,3436,3564]},
    {'s':'Scalabrini','v':[3399,3445,3791,3823,3911,3827]},
    {'s':'Vicente Lopez','v':[None,3585,4214,4078,4245,4508]},
    {'s':'Villa Urquiza','v':[None,None,4063,3540,3690,3757]},
  ]},
  {'metric':'CVR','fmt':'pct','isPP':True,'rows':[
    {'s':'Total','v':[None,0.0273,0.0279,0.0275,0.0273,0.0201]},
  ]},
  {'metric':'NSI/Cart','fmt':'dec','rows':[
    {'s':'Total','v':[15.22,16.44,14.73,15.06,14.91,14.86]},
    {'s':'Caballito','v':[None,None,14.0,15.0,15.0,15.0]},
    {'s':'Scalabrini','v':[15.0,17.0,16.0,15.0,15.0,15.0]},
    {'s':'Vicente Lopez','v':[None,16.0,14.0,14.0,14.0,15.0]},
    {'s':'Villa Urquiza','v':[None,None,14.0,16.0,16.0,16.0]},
  ]},
  {'metric':'Stores','fmt':'num','rows':[{'s':'Total','v':[1,2,4,4,4,4]}]},
  {'metric':'Orders/Purchase','fmt':'dec','rows':[{'s':'Total','v':[7.17,6.93,5.90,6.40,6.19,6.30]}]},
]

ops_data = [
  {'metric':'Fill Rate Items','fmt':'pct','isPP':True,'rows':[{'s':'Total','v':[0.8958,0.9191,0.9051,0.9172,0.9123,0.8898]}]},
  {'metric':'Fill Rate Items c/reemplazos','fmt':'pct','isPP':True,'rows':[{'s':'Total','v':[0.9135,0.9246,0.9138,0.9289,0.9225,0.9013]}]},
  {'metric':'Fill Rate Compras','fmt':'pct','isPP':True,'rows':[{'s':'Total','v':[0.6215,0.7217,0.6570,0.6436,0.6469,0.5930]}]},
  {'metric':'Fill Rate Compras c/reemplazos','fmt':'pct','isPP':True,'rows':[{'s':'Total','v':[0.6776,0.7549,0.6876,0.6882,0.6795,0.6254]}]},
  {'metric':'On Time','fmt':'pct','isPP':True,'rows':[{'s':'Total','v':[0.9645,0.9287,0.7897,0.8923,0.8082,0.8558]}]},
  {'metric':'Cancelaciones','fmt':'pct','isPP':True,'isNegGood':True,'rows':[{'s':'Total','v':[0.0789,0.0519,0.0878,0.0510,0.0523,0.0668]}]},
]

demanda_data = [
  {'metric':'Visitas','fmt':'num','rows':[{'s':'Total','v':[5858,23194,72015,87194,107601,153449]}]},
  {'metric':'Buyers Mes Actual','fmt':'num','rows':[
    {'s':'Total','v':[185,873,2501,2563,3539,4226]},
    {'s':'Churns','v':[0,151,743,2197,2088,2881]},
    {'s':'Repeated','v':[0,34,130,304,475,658]},
    {'s':'Buyer Mes Anterior','v':[0,185,873,2501,2563,3539]},
  ]},
]

nps_data = [
  {'metric':'NPS','fmt':'pp','isPP':True,'rows':[
    {'s':'Total','v':[0.26,0.39,0.20,0.15]},
    {'s':'Scalabrini','v':[0.14,0.48,0.25,0.09]},
    {'s':'Vicente Lopez','v':[0.21,0.19,-0.05,0.27]},
    {'s':'Caballito','v':[0.16,0.45,0.20,0.06]},
    {'s':'Villa Urquiza','v':[0.27,0.44,0.37,0.17]},
  ]},
]

nmv_vals = [10528449,57103789,171803278,177394590,244905554,312119068]

pl_lines = [
  {'metric':'NMV','isNmv':True,'children':[]},
  {'metric':'Net Variable Fee','v':[0.0830,0.0831,0.0856,0.0825,0.0838,0.0845],'children':[
    {'metric':'Net Fix Fees','v':[0.0000,0.0000,0.0000,0.0000,0.0000,0.0000]},
    {'metric':'Promotions','v':[0.0000,-0.0002,0.0000,-0.0003,-0.0001,-0.0001]},
  ]},
  {'metric':'Product Monetization','v':[0.0830,0.0829,0.0856,0.0822,0.0838,0.0844],'children':[
    {'metric':'Financing gross','v':[0.0120,0.0057,0.0124,0.0096,0.0081,0.0077]},
    {'metric':'Financing Cost','v':[-0.0159,-0.0143,-0.0150,-0.0128,-0.0110,-0.0124]},
    {'metric':'Buyer Real Revenue','v':[0.0340,0.0288,0.0282,0.0285,0.0293,0.0279]},
    {'metric':'Seller Real Revenue','v':[0.0012,0.0001,0.0004,0.0001,0.0001,0.0001]},
    {'metric':'Advertising','v':[0.0008,0.0031,0.0034,0.0017,0.0015,0.0034]},
    {'metric':'Sales Taxes','v':[-0.0054,-0.0060,-0.0062,-0.0067,-0.0061,-0.0048]},
  ]},
  {'metric':'Net Monetization','v':[0.1097,0.1003,0.1089,0.1027,0.1058,0.1064],'children':[
    {'metric':'Shipping Distribution Cost','v':[-0.0626,-0.0534,-0.0529,-0.0542,-0.0571,-0.0569]},
    {'metric':'Shipping Ops Variable','v':[-0.0001,0.0000,0.0000,0.0000,0.0000,0.0000]},
    {'metric':'Collection Fees','v':[-0.0097,-0.0108,-0.0119,-0.0098,-0.0091,-0.0090]},
    {'metric':'Chargebacks','v':[0.0000,0.0000,-0.0015,-0.0001,-0.0061,-0.0003]},
    {'metric':'Bad Debt','v':[0.0001,-0.0007,0.0005,-0.0003,-0.0012,-0.0002]},
    {'metric':'BPP','v':[-0.0231,-0.0056,-0.0066,-0.0033,-0.0034,-0.0058]},
    {'metric':'Marketing Performance','v':[-0.0032,-0.0033,-0.0031,-0.0032,-0.0031,-0.0036]},
    {'metric':'Coupons Mkt','v':[0.0000,-0.0016,-0.0013,-0.0016,-0.0030,-0.0026]},
    {'metric':'CX + Hosting + Fraud Prev Variable','v':[-0.0138,-0.0100,-0.0100,-0.0107,-0.0110,-0.0161]},
  ]},
  {'metric':'Variable Contribution','v':[-0.0030,0.0146,0.0217,0.0191,0.0113,0.0116],'children':[
    {'metric':'CX + Hosting + Fraud Prev Fix','v':[-0.0039,-0.0033,-0.0040,-0.0035,-0.0037,-0.0038]},
  ]},
  {'metric':'Direct Contribution','v':[-0.0103,0.0081,0.0129,0.0137,0.0054,0.0056],'children':[]},
]

# P&L by Tienda (total, not monthly - shown as cross-section)
tiendas_names = ['Caballito','Palermo\n(Scalabrini)','Vicente Lopez','Villa Urquiza']
tiendas_nmv   = [69362977, 93016605, 92558674, 57180812]
pl_tiendas = [
  {'metric':'NMV','isNmv':True,'children':[]},
  {'metric':'Net Variable Fee','v':[0.0868,0.0849,0.0851,0.0795],'children':[
    {'metric':'Net Fix Fees','v':[0,0,0,0]},
    {'metric':'Promotions','v':[-0.000124,-0.0000948,-0.0000342,-0.0000687]},
  ]},
  {'metric':'Product Monetization','v':[0.0867,0.0848,0.0851,0.0794],'children':[
    {'metric':'Financing gross','v':[0.00629,0.00745,0.00694,0.01105]},
    {'metric':'Financing Cost','v':[-0.00970,-0.01337,-0.01342,-0.01119]},
    {'metric':'Buyer Real Revenue','v':[0.03408,0.02707,0.02502,0.02644]},
    {'metric':'Seller Real Revenue','v':[0.000191,0.0000711,0,0.000116]},
    {'metric':'Advertising','v':[0.003701,0.003304,0.003341,0.003243]},
    {'metric':'Sales Taxes','v':[-0.004887,-0.004830,-0.004818,-0.004738]},
  ]},
  {'metric':'Net Monetization','v':[0.1165,0.1046,0.1023,0.1045],'children':[
    {'metric':'Shipping Distribution Cost','v':[-0.0643,-0.0559,-0.0528,-0.0563]},
    {'metric':'Collection Fees','v':[-0.00796,-0.00947,-0.00984,-0.00808]},
    {'metric':'Chargebacks','v':[0.00432,-0.000135,0,-0.00632]},
    {'metric':'Bad Debt','v':[-0.000246,-0.000245,-0.000246,-0.000245]},
    {'metric':'BPP','v':[-0.00586,-0.00432,-0.00725,-0.00540]},
    {'metric':'Marketing Performance','v':[-0.00350,-0.00357,-0.00371,-0.00347]},
    {'metric':'Coupons Mkt','v':[-0.00358,-0.00214,-0.00233,-0.00248]},
    {'metric':'CX + Hosting + Fraud Prev Variable','v':[-0.01931,-0.01363,-0.01211,-0.02072]},
  ]},
  {'metric':'Variable Contribution','v':[0.01590,0.01489,0.01369,0.00140],'children':[
    {'metric':'CX + Hosting + Fraud Prev Fix','v':[-0.003698,-0.004157,-0.003800,-0.003586]},
  ]},
  {'metric':'Direct Contribution','v':[0.00977,0.00895,0.00795,-0.00487],'children':[]},
]

# P&L Verticales (top categories)
verticals = [
  {'v':'CPG','nmv':300826500,'pm_pct':0.0838,'vc_pct':0.0109,'dc_pct':0.0049},
  {'v':'Beauty','nmv':8830423,'pm_pct':0.1001,'vc_pct':0.0664,'dc_pct':0.0607},
  {'v':'Health','nmv':775434,'pm_pct':0.0999,'vc_pct':0.0479,'dc_pct':0.0410},
  {'v':'Furnishing & Houseware','nmv':816013,'pm_pct':0.1122,'vc_pct':-0.3225,'dc_pct':-0.3287},
  {'v':'Construction & Industry','nmv':587929,'pm_pct':0.0993,'vc_pct':0.0026,'dc_pct':0.0040},
  {'v':'T & B','nmv':143979,'pm_pct':0.1000,'vc_pct':0.0456,'dc_pct':0.0431},
  {'v':'Sports','nmv':97193,'pm_pct':0.1000,'vc_pct':-0.0177,'dc_pct':-0.0261},
  {'v':'Entertainment','nmv':41596,'pm_pct':0.1000,'vc_pct':-0.1472,'dc_pct':-0.1556},
]

# ======================================================
# HTML
# ======================================================

def j(x): return json.dumps(x, ensure_ascii=False)

html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Proximity Groceries Scorecard</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0b1220;padding:24px;color:#d0d8e8}}
.card{{background:#111827;border-radius:14px;padding:24px 28px;box-shadow:0 4px 24px rgba(0,0,0,0.5);max-width:1300px;margin:0 auto;border:1px solid #1c2a3e}}
.hdr{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:1.1rem}}
.title{{font-size:18px;font-weight:700;color:#e8edf5;margin-bottom:3px}}
.subtitle{{font-size:12px;color:#4d6180}}
.upd{{font-size:11px;color:#3a5070;text-align:right;padding-top:2px}}
.tabs{{display:flex;gap:2px;border-bottom:1.5px solid #1c2a3e;margin-bottom:1.2rem;flex-wrap:wrap}}
.tab{{padding:8px 22px;font-size:13px;cursor:pointer;border-bottom:2.5px solid transparent;color:#4d6180;font-weight:500;transition:all .15s;margin-bottom:-1.5px}}
.tab.active{{color:#4d9ef0;border-bottom-color:#4d9ef0;font-weight:700}}
.tab:hover{{color:#a0b4cc}}
.subtabs{{display:flex;gap:6px;margin-bottom:1rem}}
.subtab{{padding:4px 14px;font-size:12px;cursor:pointer;border-radius:20px;color:#4d6180;border:1px solid #1c2a3e;font-weight:500;background:#0b1220}}
.subtab.active{{background:rgba(77,158,240,0.15);color:#4d9ef0;border-color:#4d9ef0;font-weight:600}}
.filter-bar{{display:flex;align-items:center;gap:8px;margin-bottom:14px}}
.filter-lbl{{font-size:11px;color:#4d6180;font-weight:500}}
.fsel{{background:#0b1220;color:#c0cedd;border:1px solid #1c2a3e;border-radius:7px;padding:5px 28px 5px 10px;font-size:12px;cursor:pointer;outline:none;appearance:none;-webkit-appearance:none;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%234d6180'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 8px center;font-family:inherit;transition:border-color .15s}}
.fsel:hover,.fsel:focus{{border-color:#4d9ef0;color:#e8edf5}}
.fsel option{{background:#111827;color:#c0cedd}}
.tbl{{overflow-x:auto}}
table{{width:100%;border-collapse:collapse;font-size:12px;min-width:920px}}
th{{background:#0a1018;color:#8099b8;font-weight:600;padding:0 10px;text-align:right;white-space:nowrap;font-size:11px;letter-spacing:.02em;line-height:1.3}}
th .rel{{font-size:9px;color:#3a5070;font-weight:400;display:block;margin-bottom:2px;letter-spacing:.04em}}
th .dt{{display:block;padding:6px 0}}
th.left{{text-align:left}}
th.hl{{background:#0d1e36}}
th.last{{background:#0f2244;color:#6db3f5}}
td{{padding:6px 10px;text-align:right;border-bottom:1px solid #161f2e;color:#c0cedd;white-space:nowrap}}
td.left{{text-align:left}}
td.metric{{font-weight:700;color:#e8edf5}}
td.apert{{font-size:11px;color:#4d6180}}
td.apert-total{{font-size:11px;color:#4d9ef0;font-weight:600}}
td.store{{color:#4d6180;padding-left:26px;font-size:11px}}
td.hl{{background:rgba(77,158,240,0.06);font-weight:500}}
td.last{{background:rgba(77,158,240,0.10);font-weight:600;color:#d8e8f8}}
td.store.hl{{background:rgba(77,158,240,0.03)}}
td.store.last{{background:rgba(77,158,240,0.06)}}
td.main{{font-weight:700;background:#0d1725;color:#e8edf5}}
td.main.hl{{background:rgba(77,158,240,0.08)}}
td.main.last{{background:rgba(77,158,240,0.14);color:#6db3f5}}
td.child{{color:#4d6180;background:#0f1924}}
td.child.hl{{background:rgba(77,158,240,0.03)}}
td.child.last{{background:rgba(77,158,240,0.06)}}
td.pos{{color:#34c47a;font-weight:700}}
td.neg{{color:#e05252;font-weight:700}}
td.neu{{color:#2c3f5c}}
tr:hover td{{background:#141e2e!important}}
tr:hover td.last{{background:#1a3060!important}}
.tog{{font-size:9px;cursor:pointer;color:#2c3f5c;margin-right:4px;user-select:none}}
.tog:hover{{color:#4d9ef0}}
.charts-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
@media(max-width:800px){{.charts-grid{{grid-template-columns:1fr}}}}
.chart-card{{background:#0d1725;border:1px solid #1c2a3e;border-radius:10px;padding:18px 20px}}
.chart-title{{font-size:12px;font-weight:700;color:#8099b8;text-transform:uppercase;letter-spacing:.06em;margin-bottom:12px}}
.chart-sub{{font-size:11px;color:#3a5070;margin-top:6px}}
.plan-hdr{{font-size:15px;font-weight:700;color:#e8edf5;margin-bottom:4px}}
.plan-sub{{font-size:12px;color:#4d6180;margin-bottom:18px}}
.plan-badge{{display:inline-block;background:rgba(77,158,240,0.18);color:#4d9ef0;border-radius:5px;padding:2px 9px;font-size:11px;font-weight:600;margin-left:8px;vertical-align:middle}}
#tt{{position:fixed;background:#1a2a42;border:1px solid #2d4a6e;border-radius:8px;padding:7px 12px;font-size:12px;color:#e8edf5;pointer-events:none;display:none;z-index:999;box-shadow:0 4px 16px rgba(0,0,0,0.5);white-space:nowrap}}
#tt .tt-lbl{{font-size:10px;color:#4d9ef0;font-weight:600;margin-bottom:2px}}
#tt .tt-val{{font-size:13px;font-weight:700;color:#e8edf5}}
</style>
</head>
<body>
<div id="tt"><div class="tt-lbl" id="tt-lbl"></div><div class="tt-val" id="tt-val"></div></div>
<div class="card">
  <div class="hdr">
    <div>
      <div class="title">Proximity Groceries Scorecard</div>
      <div class="subtitle">Meses cerrados &nbsp;·&nbsp; Oct'25 → Mar'26 &nbsp;·&nbsp; Caballito · Scalabrini · Vicente Lopez · Villa Urquiza</div>
    </div>
    <div class="upd">Actualizado: {today}</div>
  </div>
  <div class="tabs">
    <div class="tab active" onclick="switchTab('growth')">Growth</div>
    <div class="tab" onclick="switchTab('ops')">Ops</div>
    <div class="tab" onclick="switchTab('demanda')">Demanda</div>
    <div class="tab" onclick="switchTab('nps')">NPS</div>
    <div class="tab" onclick="switchTab('pl')">P&amp;L</div>
    <div class="tab" onclick="switchTab('charts')">Gráficos</div>
    <div class="tab" onclick="switchTab('plan')">Plan</div>
  </div>
  <div class="tbl">
    <div id="t-growth">
      <div class="filter-bar">
        <span class="filter-lbl">Apertura:</span>
        <select class="fsel" id="store-filter" onchange="setStore(this.value)">
          <option value="Todas">Todas las tiendas</option>
          <option value="Caballito">Caballito</option>
          <option value="Scalabrini">Scalabrini</option>
          <option value="Vicente Lopez">Vicente Lopez</option>
          <option value="Villa Urquiza">Villa Urquiza</option>
        </select>
      </div>
      <table><thead id="hg"></thead><tbody id="bg"></tbody></table>
    </div>
    <div id="t-ops" style="display:none"><table><thead id="ho"></thead><tbody id="bo"></tbody></table></div>
    <div id="t-demanda" style="display:none"><table><thead id="hd"></thead><tbody id="bd"></tbody></table></div>
    <div id="t-nps" style="display:none"><table><thead id="hn"></thead><tbody id="bn"></tbody></table></div>
    <div id="t-pl" style="display:none">
      <div class="subtabs">
        <div class="subtab active" onclick="switchSub('s-total')">Total</div>
        <div class="subtab" onclick="switchSub('s-tiendas')">Tiendas</div>
        <div class="subtab" onclick="switchSub('s-vert')">Verticales</div>
      </div>
      <div id="s-total"><table><thead id="hpt"></thead><tbody id="bpt"></tbody></table></div>
      <div id="s-tiendas" style="display:none"><table><thead id="hpti"></thead><tbody id="bpti"></tbody></table></div>
      <div id="s-vert" style="display:none"><table><thead id="hpv"></thead><tbody id="bpv"></tbody></table></div>
    </div>
    <div id="t-charts" style="display:none">
      <div class="charts-grid">
        <div class="chart-card"><div class="chart-title">NMV Mensual</div><svg id="c-nmv" width="100%" style="overflow:visible"></svg></div>
        <div class="chart-card"><div class="chart-title">Compras Mensual</div><svg id="c-ord" width="100%" style="overflow:visible"></svg></div>
        <div class="chart-card"><div class="chart-title">Fill Rate Items</div><svg id="c-fr" width="100%" style="overflow:visible"></svg></div>
        <div class="chart-card"><div class="chart-title">Visitas Mensuales</div><svg id="c-vis" width="100%" style="overflow:visible"></svg></div>
      </div>
    </div>
    <div id="t-plan" style="display:none">
      <div class="plan-hdr">Plan V2 <span class="plan-badge">Forecast 2+10 2026</span></div>
      <div class="plan-sub">Proximity Groceries MLA · Plan V2 vs Real &nbsp;·&nbsp; Plan disponible desde Ene'26</div>
      <div class="tbl"><table><thead id="h-plan"></thead><tbody id="b-plan"></tbody></table></div>
    </div>
  </div>
</div>

<script>
const MONTHS={j(MONTHS)};
const MONTHS_NPS={j(MONTHS_NPS)};
const growthData={j(growth_data)};
const opsData={j(ops_data)};
const demandaData={j(demanda_data)};
const npsData={j(nps_data)};
const nmvVals={j(nmv_vals)};
const plLines={j(pl_lines)};
const tiendasNames={j(tiendas_names)};
const tiendasNmv={j(tiendas_nmv)};
const plTiendas={j(pl_tiendas)};
const verticals={j(verticals)};


let activeStore='Todas';

function relLabel(months,i){{
  const offset=months.length-1-i;
  if(offset===0)return'Última mes';
  return'M-'+offset;
}}

window.setStore=function(s){{activeStore=s;buildBody('bg',growthData,MONTHS,4,5);}};

function fv(v,fmt){{
  if(v===null||v===undefined)return'-';
  if(fmt==='money'){{if(v>=1e9)return'$'+(v/1e9).toFixed(1)+'B';if(v>=1e6)return'$'+(v/1e6).toFixed(1)+'M';if(v>=1e3)return'$'+(v/1e3).toFixed(0)+'K';return'$'+v;}}
  if(fmt==='dollar')return'$'+Math.round(v).toLocaleString();
  if(fmt==='pct')return(v*100).toFixed(1)+'%';
  if(fmt==='pp')return(v>=0?'+':'')+Math.round(v*100)+'pp';
  if(fmt==='dec')return v.toFixed(1);
  if(v>=1e6)return(v/1e6).toFixed(1)+'M';
  if(v>=1e3)return(v/1e3).toFixed(1)+'K';
  return v.toLocaleString();
}}

function fpl(v,isNmv){{
  if(v===null||v===undefined)return'-';
  if(isNmv)return fv(v,'money');
  return(v>=0?'':'')+( v*100).toFixed(2)+'%';
}}

function vsLP(vals,isPP,isNegGood,idx){{
  const c=vals[idx],p=vals[idx-1];
  if(c==null||p==null)return null;
  if(isPP)return{{val:(c-p)*100,isPP:true,isNegGood}};
  return{{val:(c-p)/p*100,isPP:false,isNegGood}};
}}

function fmtVs(obj){{
  if(!obj)return{{txt:'-',cls:'last neu'}};
  const{{val,isPP,isNegGood}}=obj;
  const s=val>=0?'+':'';
  const txt=isPP?s+val.toFixed(2)+'pp':s+val.toFixed(1)+'%';
  const good=isNegGood?val<=0:val>=0;
  return{{txt,cls:good?'last pos':'last neg'}};
}}

function spark(vals){{
  const pts=vals.map((v,i)=>{{return{{v,i}}}}).filter(p=>p.v!=null);
  if(pts.length<2)return'<td></td>';
  const xs=pts.map(p=>p.i),ys=pts.map(p=>p.v);
  const mnX=Math.min(...xs),mxX=Math.max(...xs),mnY=Math.min(...ys),mxY=Math.max(...ys);
  const W=64,H=22,pd=2;
  const sx=x=>mxX===mnX?W/2:pd+(x-mnX)/(mxX-mnX)*(W-2*pd);
  const sy=y=>mxY===mnY?H/2:H-pd-(y-mnY)/(mxY-mnY)*(H-2*pd);
  const d=pts.map((p,i)=>(i===0?'M':'L')+sx(p.i).toFixed(1)+','+sy(p.v).toFixed(1)).join(' ');
  const l=pts[pts.length-1],p2=pts[pts.length-2];
  const col=l.v>=p2.v?'#34c47a':'#e05252';
  return`<td style="text-align:center;padding:4px 8px"><svg width="${{W}}" height="${{H}}" viewBox="0 0 ${{W}} ${{H}}"><path d="${{d}}" fill="none" stroke="${{col}}" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/><circle cx="${{sx(l.i).toFixed(1)}}" cy="${{sy(l.v).toFixed(1)}}" r="2.5" fill="${{col}}"/></svg></td>`;
}}

const EXP={{}};

function buildHead(hid,months,hlIdx){{
  const lastIdx=months.length-1;
  document.getElementById(hid).innerHTML=`<tr>
    <th class="left" style="min-width:180px"><span class="dt">Métrica</span></th>
    <th class="left" style="min-width:72px"><span class="rel">&nbsp;</span><span class="dt">Apertura</span></th>
    ${{months.map((m,i)=>{{
      const isLast=i===lastIdx;
      const cls=isLast?'last':(i>=hlIdx?'hl':'');
      const rel=relLabel(months,i);
      return`<th class="${{cls}}"><span class="rel">${{rel}}</span><span class="dt">${{m}}</span></th>`;
    }}).join('')}}
    <th class="last"><span class="rel">cambio</span><span class="dt">vs LP</span></th>
    <th style="min-width:80px"><span class="dt">Tendencia</span></th></tr>`;
}}

function buildBody(bid,data,months,hlIdx,lpIdx){{
  const tb=document.getElementById(bid);tb.innerHTML='';
  const isGrowth=bid==='bg';
  const lastIdx=months.length-1;
  data.forEach((m,mi)=>{{
    if(isGrowth&&activeStore!=='Todas'){{
      const sRow=m.rows.find(r=>r.s===activeStore)||m.rows[0];
      const tr=document.createElement('tr');
      let h=`<td class="metric left"><span class="tog"></span>${{m.metric}}</td>`;
      h+=`<td class="apert-total left">${{sRow.s}}</td>`;
      sRow.v.forEach((v,i)=>{{
        const cls=i===lastIdx?'last':(i>=hlIdx?'hl':'');
        h+=`<td class="${{cls}}">${{fv(v,m.fmt)}}</td>`;
      }});
      const vl=vsLP(sRow.v,m.isPP,m.isNegGood,lpIdx);
      const{{txt,cls}}=fmtVs(vl);
      h+=`<td class="${{cls}}">${{txt}}</td>`;
      h+=spark(sRow.v);
      tr.innerHTML=h;tb.appendChild(tr);
      return;
    }}
    const isExp=EXP[bid+'_'+mi],hasC=m.rows.length>1;
    m.rows.forEach((row,ri)=>{{
      if(ri>0&&!isExp)return;
      const isT=ri===0,tr=document.createElement('tr');
      let h='';
      if(isT){{
        h+=`<td class="metric left"><span class="tog" onclick="tog('${{bid}}',${{mi}})">${{hasC?(isExp?'▼':'▶'):''}}</span>${{m.metric}}</td>`;
        h+=`<td class="apert-total left">● Total</td>`;
      }} else {{
        h+=`<td></td><td class="store left">${{row.s}}</td>`;
      }}
      row.v.forEach((v,i)=>{{
        const isLast=i===lastIdx;
        const cls=isT?(isLast?'last':(i>=hlIdx?'hl':'')):(isLast?'store last':(i>=hlIdx?'store hl':'store'));
        h+=`<td class="${{cls}}">${{fv(v,m.fmt)}}</td>`;
      }});
      const vl=vsLP(row.v,m.isPP,m.isNegGood,lpIdx);
      const{{txt,cls}}=fmtVs(vl);
      h+=`<td class="${{isT?cls:cls+' store'}}">${{txt}}</td>`;
      h+=spark(row.v);
      tr.innerHTML=h;tb.appendChild(tr);
    }});
  }});
}}

function buildPLHead(hid,cols,isTienda){{
  const w=isTienda?240:220;
  const lastIdx=cols.length-1;
  let ths='';
  if(!isTienda){{
    ths=cols.map((c,i)=>{{
      const isLast=i===lastIdx;
      const cls=isLast?'last':(i>=lastIdx-1?'hl':'');
      const rel=relLabel(cols,i);
      return`<th class="${{cls}}"><span class="rel">${{rel}}</span><span class="dt">${{c}}</span></th>`;
    }}).join('');
  }} else {{
    ths=cols.map((c,i)=>`<th class="${{i===lastIdx?'last':'hl'}}"><span class="rel">Tienda</span><span class="dt">${{c}}</span></th>`).join('');
  }}
  document.getElementById(hid).innerHTML=`<tr>
    <th class="left" style="min-width:${{w}}px"><span class="dt">Línea P&L</span></th>
    ${{ths}}
    ${{!isTienda?'<th class="last"><span class="rel">cambio</span><span class="dt">vs LP</span></th><th><span class="dt">Tendencia</span></th>':''}}</tr>`;
}}

function buildPLBody(bid,lines,nmvArr,cols,isTienda){{
  const tb=document.getElementById(bid);tb.innerHTML='';
  const lastIdx=cols.length-1;
  lines.forEach((line,li)=>{{
    const isExp=EXP[bid+'_'+li],hasC=line.children&&line.children.length>0;
    const isNmv=line.isNmv;
    const vals=isNmv?nmvArr:(line.v||[]);
    const tr=document.createElement('tr');
    let h=`<td class="main left"><span class="tog" onclick="togPL('${{bid}}',${{li}})">${{hasC?(isExp?'▼':'▶'):''}}</span>${{line.metric}}</td>`;
    vals.forEach((v,i)=>{{
      const cls=i===lastIdx?'main last':'main'+(i>=lastIdx-1?' hl':'');
      h+=`<td class="${{cls}}">${{fpl(v,isNmv)}}</td>`;
    }});
    if(!isTienda){{
      if(isNmv){{
        const d=vals[4]&&vals[5]?(vals[5]-vals[4])/vals[4]*100:null;
        h+=d!=null?`<td class="main last ${{d>=0?'pos':'neg'}}">${{d>=0?'+':''}}${{d.toFixed(1)}}%</td>`:`<td class="main last neu">-</td>`;
      }} else if(vals.length>=6){{
        const d=(vals[5]-vals[4])*100;
        h+=`<td class="main last ${{d>=0?'pos':'neg'}}">${{d>=0?'+':''}}${{d.toFixed(2)}}pp</td>`;
      }} else h+=`<td class="main last neu">-</td>`;
      h+=spark(vals);
    }}
    tr.innerHTML=h;tb.appendChild(tr);
    if(isExp&&hasC){{
      line.children.forEach(child=>{{
        const cr=document.createElement('tr');
        let ch=`<td class="child left" style="padding-left:28px">${{child.metric}}</td>`;
        child.v.forEach((v,i)=>{{
          const cls=i===lastIdx?'child last':'child'+(i>=lastIdx-1?' hl':'');
          ch+=`<td class="${{cls}}">${{fpl(v,false)}}</td>`;
        }});
        if(!isTienda){{
          const d=(child.v[5]-child.v[4])*100;
          ch+=`<td class="child last ${{d>=0?'pos':'neg'}}">${{d>=0?'+':''}}${{d.toFixed(2)}}pp</td>`;
          ch+=spark(child.v);
        }}
        cr.innerHTML=ch;tb.appendChild(cr);
      }});
    }}
  }});
}}

function buildVertBody(){{
  const tb=document.getElementById('bpv');tb.innerHTML='';
  document.getElementById('hpv').innerHTML=`<tr>
    <th class="left" style="min-width:220px"><span class="dt">Vertical</span></th>
    <th><span class="dt">NMV</span></th>
    <th><span class="dt">Prod. Monetization</span></th>
    <th><span class="dt">Variable Contribution</span></th>
    <th class="last"><span class="dt">Direct Contribution</span></th></tr>`;
  verticals.forEach(vt=>{{
    const tr=document.createElement('tr');
    const dc_cls=vt.dc_pct>=0?'pos':'neg';
    const vc_cls=vt.vc_pct>=0?'pos':'neg';
    tr.innerHTML=`
      <td class="left metric">${{vt.v}}</td>
      <td>${{fv(vt.nmv,'money')}}</td>
      <td>${{(vt.pm_pct*100).toFixed(2)}}%</td>
      <td class="${{vc_cls}}">${{(vt.vc_pct*100).toFixed(2)}}%</td>
      <td class="last ${{dc_cls}}">${{(vt.dc_pct*100).toFixed(2)}}%</td>`;
    tb.appendChild(tr);
  }});
}}

window.tog=function(bid,mi){{EXP[bid+'_'+mi]=!EXP[bid+'_'+mi];buildBody(bid,bid==='bg'?growthData:bid==='bo'?opsData:bid==='bd'?demandaData:npsData,bid==='bn'?MONTHS_NPS:MONTHS,bid==='bn'?3:5,bid==='bn'?3:5);}};
window.togPL=function(bid,li){{EXP[bid+'_'+li]=!EXP[bid+'_'+li];if(bid==='bpt')buildPLBody('bpt',plLines,nmvVals,MONTHS,false);else buildPLBody('bpti',plTiendas,tiendasNmv,tiendasNames,true);}};

window.switchTab=function(tab){{
  ['growth','ops','demanda','nps','pl','charts','plan'].forEach(t=>document.getElementById('t-'+t).style.display=t===tab?'':'none');
  document.querySelectorAll('.tab').forEach((el,i)=>el.classList.toggle('active',['growth','ops','demanda','nps','pl','charts','plan'][i]===tab));
  if(tab==='charts')buildCharts();
  if(tab==='plan')buildPlan();
}};

window.switchSub=function(sub){{
  ['s-total','s-tiendas','s-vert'].forEach(s=>document.getElementById(s).style.display=s===sub?'':'none');
  document.querySelectorAll('.subtab').forEach((el,i)=>el.classList.toggle('active',['s-total','s-tiendas','s-vert'][i]===sub));
}};

// ── CHARTS ────────────────────────────────────────────────────────────────────
function getW(id){{const r=document.getElementById(id).getBoundingClientRect();return Math.max(r.width,300)||380;}}

const tt=document.getElementById('tt');
const ttLbl=document.getElementById('tt-lbl');
const ttVal=document.getElementById('tt-val');
function showTT(e,lbl,val){{tt.style.display='block';ttLbl.textContent=lbl;ttVal.textContent=val;moveTT(e);}}
function moveTT(e){{tt.style.left=(e.clientX+14)+'px';tt.style.top=(e.clientY-36)+'px';}}
function hideTT(){{tt.style.display='none';}}

function fmtChart(v,fmt){{
  if(v==null)return'-';
  if(fmt==='pct')return(v*100).toFixed(1)+'%';
  if(fmt==='money')return v>=1e9?'$'+(v/1e9).toFixed(2)+'B':v>=1e6?'$'+(v/1e6).toFixed(2)+'M':v>=1e3?'$'+(v/1e3).toFixed(0)+'K':'$'+v;
  return v>=1e6?(v/1e6).toFixed(2)+'M':v>=1e3?(v/1e3).toFixed(0)+'K':v.toLocaleString();
}}

function drawLineArea(svgId,vals,labels,color,fmt){{
  const W=getW(svgId),H=200,PL=52,PR=14,PT=28,PB=32;
  const cW=W-PL-PR,cH=H-PT-PB;
  const pts=vals.map((v,i)=>{{return{{v,i,lbl:labels[i]}}}}).filter(p=>p.v!=null);
  if(pts.length<2)return;
  const ys=pts.map(p=>p.v),mnY=Math.min(...ys)*0.92,mxY=Math.max(...ys)*1.04,rng=mxY-mnY||mxY||1;
  const N=vals.length;
  const sx=i=>PL+i/(N-1)*cW;
  const sy=v=>PT+cH-(v-mnY)/rng*cH;
  let svg=`<defs><linearGradient id="ag${{svgId}}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${{color}}" stop-opacity="0.3"/><stop offset="100%" stop-color="${{color}}" stop-opacity="0.02"/></linearGradient></defs>`;
  for(let g=0;g<=3;g++){{
    const yg=PT+cH*g/3;
    const val=mxY-(rng*g/3);
    const lbl=fmtChart(val,fmt);
    svg+=`<line x1="${{PL}}" y1="${{yg.toFixed(1)}}" x2="${{W-PR}}" y2="${{yg.toFixed(1)}}" stroke="#1c2a3e" stroke-width="1"/>`;
    svg+=`<text x="${{PL-5}}" y="${{(yg+4).toFixed(1)}}" text-anchor="end" font-size="9" fill="#3a5070">${{lbl}}</text>`;
  }}
  const path=pts.map((p,i)=>(i===0?'M':'L')+sx(p.i).toFixed(1)+','+sy(p.v).toFixed(1)).join(' ');
  const lastP=pts[pts.length-1],firstP=pts[0];
  svg+=`<path d="${{path}} L${{sx(lastP.i).toFixed(1)}},${{(PT+cH).toFixed(1)}} L${{sx(firstP.i).toFixed(1)}},${{(PT+cH).toFixed(1)}} Z" fill="url(#ag${{svgId}})"/>`;
  svg+=`<path d="${{path}}" fill="none" stroke="${{color}}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>`;
  pts.forEach((p,pi)=>{{
    const isLast=pi===pts.length-1;
    const cx=sx(p.i).toFixed(1),cy=sy(p.v).toFixed(1);
    const valLbl=fmtChart(p.v,fmt);
    // stagger label above/below to avoid overlap
    const above=pi%2===0;
    const ly=(sy(p.v)+(above?-9:14)).toFixed(1);
    svg+=`<text x="${{cx}}" y="${{ly}}" text-anchor="middle" font-size="9" font-weight="${{isLast?700:500}}" fill="${{isLast?color:'#5a7a9a'}}">${{valLbl}}</text>`;
    svg+=`<circle class="hp" cx="${{cx}}" cy="${{cy}}" r="${{isLast?4.5:3}}" fill="${{isLast?color:'#111827'}}" stroke="${{color}}" stroke-width="1.8"
      onmousemove="showTT(event,'${{p.lbl}}','${{valLbl}}')" onmouseleave="hideTT()" style="cursor:pointer"/>`;
  }});
  labels.forEach((lbl,i)=>{{
    svg+=`<text x="${{sx(i).toFixed(1)}}" y="${{(PT+cH+18).toFixed(1)}}" text-anchor="middle" font-size="9" fill="#3a5070">${{lbl}}</text>`;
  }});
  const el=document.getElementById(svgId);
  el.setAttribute('height',H);el.innerHTML=svg;
}}

function drawBars(svgId,vals,labels,color,fmt){{
  const W=getW(svgId),H=200,PL=52,PR=14,PT=28,PB=32;
  const cW=W-PL-PR,cH=H-PT-PB;
  const N=vals.length,gap=5,bW=(cW-gap*(N-1))/N;
  const clean=vals.filter(v=>v!=null),mxY=Math.max(...clean)*1.12||1;
  let svg='';
  for(let g=0;g<=3;g++){{
    const yg=PT+cH*g/3;
    const val=mxY*(1-g/3);
    svg+=`<line x1="${{PL}}" y1="${{yg.toFixed(1)}}" x2="${{W-PR}}" y2="${{yg.toFixed(1)}}" stroke="#1c2a3e" stroke-width="1"/>`;
    svg+=`<text x="${{PL-5}}" y="${{(yg+4).toFixed(1)}}" text-anchor="end" font-size="9" fill="#3a5070">${{fmtChart(val,fmt)}}</text>`;
  }}
  vals.forEach((v,i)=>{{
    if(v==null)return;
    const x=PL+i*(bW+gap);
    const bH=v/mxY*cH;
    const isLast=i===N-1;
    const col=isLast?color:color+'88';
    const cx=(x+bW/2).toFixed(1);
    const valLbl=fmtChart(v,fmt);
    svg+=`<rect class="hp" x="${{x.toFixed(1)}}" y="${{(PT+cH-bH).toFixed(1)}}" width="${{bW.toFixed(1)}}" height="${{bH.toFixed(1)}}" rx="3" fill="${{col}}"
      onmousemove="showTT(event,'${{labels[i]}}','${{valLbl}}')" onmouseleave="hideTT()" style="cursor:pointer"/>`;
    svg+=`<text x="${{cx}}" y="${{(PT+cH-bH-5).toFixed(1)}}" text-anchor="middle" font-size="${{isLast?10:9}}" font-weight="${{isLast?700:400}}" fill="${{isLast?color:'#5a7a9a'}}">${{valLbl}}</text>`;
    svg+=`<text x="${{cx}}" y="${{(PT+cH+18).toFixed(1)}}" text-anchor="middle" font-size="9" fill="#3a5070">${{labels[i]}}</text>`;
  }});
  const el=document.getElementById(svgId);
  el.setAttribute('height',H);el.innerHTML=svg;
}}

function drawMultiLine(svgId,seriesList,labels){{
  const W=getW(svgId),H=200,PL=52,PR=64,PT=28,PB=32;
  const cW=W-PL-PR,cH=H-PT-PB;
  const N=labels.length;
  const allV=seriesList.flatMap(s=>s.vals.filter(v=>v!=null));
  const mnY=Math.min(...allV)*0.95,mxY=Math.max(...allV)*1.05,rng=mxY-mnY||0.01;
  const sx=i=>PL+i/(N-1)*cW;
  const sy=v=>PT+cH-(v-mnY)/rng*cH;
  let svg='';
  for(let g=0;g<=3;g++){{
    const yg=PT+cH*g/3;
    const val=mxY-(rng*g/3);
    svg+=`<line x1="${{PL}}" y1="${{yg.toFixed(1)}}" x2="${{W-PR}}" y2="${{yg.toFixed(1)}}" stroke="#1c2a3e" stroke-width="1"/>`;
    svg+=`<text x="${{PL-5}}" y="${{(yg+4).toFixed(1)}}" text-anchor="end" font-size="9" fill="#3a5070">${{(val*100).toFixed(0)}}%</text>`;
  }}
  seriesList.forEach((s)=>{{
    const pts=s.vals.map((v,i)=>{{return{{v,i}}}}).filter(p=>p.v!=null);
    if(pts.length<2)return;
    const path=pts.map((p,i)=>(i===0?'M':'L')+sx(p.i).toFixed(1)+','+sy(p.v).toFixed(1)).join(' ');
    svg+=`<path d="${{path}}" fill="none" stroke="${{s.color}}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>`;
    pts.forEach((p,pi)=>{{
      const cx=sx(p.i).toFixed(1),cy=sy(p.v).toFixed(1);
      const valLbl=(p.v*100).toFixed(1)+'%';
      const above=pi%2===0;
      const ly=(sy(p.v)+(above?-8:13)).toFixed(1);
      svg+=`<text x="${{cx}}" y="${{ly}}" text-anchor="middle" font-size="9" fill="${{s.color}}" opacity="0.85">${{valLbl}}</text>`;
      svg+=`<circle class="hp" cx="${{cx}}" cy="${{cy}}" r="3" fill="${{s.color}}"
        onmousemove="showTT(event,'${{labels[p.i]}} · ${{s.label}}','${{valLbl}}')" onmouseleave="hideTT()" style="cursor:pointer"/>`;
    }});
    const last=pts[pts.length-1];
    svg+=`<text x="${{(W-PR+6).toFixed(1)}}" y="${{(sy(last.v)+4).toFixed(1)}}" font-size="10" fill="${{s.color}}" font-weight="700">${{s.label}}</text>`;
  }});
  labels.forEach((lbl,i)=>{{
    svg+=`<text x="${{sx(i).toFixed(1)}}" y="${{(PT+cH+18).toFixed(1)}}" text-anchor="middle" font-size="9" fill="#3a5070">${{lbl}}</text>`;
  }});
  const el=document.getElementById(svgId);
  el.setAttribute('height',H);el.innerHTML=svg;
}}

function buildCharts(){{
  const nmvValsGrowth=growthData.find(m=>m.metric==='NMV').rows[0].v;
  const comprasVals=growthData.find(m=>m.metric==='Compras').rows[0].v;
  const visVals=demandaData.find(m=>m.metric==='Visitas').rows[0].v;
  const frItems=opsData.find(m=>m.metric==='Fill Rate Items').rows[0].v;
  const frCompras=opsData.find(m=>m.metric==='Fill Rate Compras').rows[0].v;
  drawLineArea('c-nmv',nmvValsGrowth,MONTHS,'#4d9ef0','money');
  drawBars('c-ord',comprasVals,MONTHS,'#34c47a','num');
  drawMultiLine('c-fr',[
    {{vals:frItems,color:'#4d9ef0',label:'Items'}},
    {{vals:frCompras,color:'#f59e0b',label:'Compras'}}
  ],MONTHS);
  drawBars('c-vis',visVals,MONTHS,'#a78bfa','num');
}}

// ── PLAN V2 ───────────────────────────────────────────────────────────────────
// Source: "Plan V2. Forecast 2+10 2026 | Proximity Groceries MLA.xlsx"
// Tab "Plan V1 & V2" → NMV, Compras, APV | Tab "In Store | Consolidado"
// Plan V2 starts from Ene'26 (hecho en enero con Oct-Dic como real)
// Months: Oct'25, Nov'25, Dic'25, Ene'26, Feb'26, Mar'26
const planData=[
  {{metric:'NMV',fmt:'money',isNegGood:false,
    plan:[null,null,null,175799372,196000000,332708333],
    real:[10451118,56806992,170234400,175439531,242175980,306868637]}},
  {{metric:'Compras',fmt:'num',isNegGood:false,
    plan:[null,null,null,3103,3229,5709],
    real:[202,987,2927,3098,4244,5262]}},
  {{metric:'APV LC',fmt:'dollar',isNegGood:false,
    plan:[null,null,null,56655,60700,58278],
    real:[51738,57555,58160,56630,57063,58318]}},
  {{metric:'Fill Rate Items',sub:'sin reemplazos',fmt:'pct',isNegGood:false,
    plan:[null,null,null,0.92,0.92,0.92],
    real:[0.8958,0.9191,0.9051,0.9172,0.9123,0.8898]}},
  {{metric:'Fill Rate Compras',sub:'sin reemplazos',fmt:'pct',isNegGood:false,
    plan:[null,null,null,0.65,0.65,0.65],
    real:[0.6215,0.7217,0.6570,0.6436,0.6469,0.5930]}},
  {{metric:'On Time',fmt:'pct',isNegGood:false,
    plan:[null,null,null,0.96,0.96,0.96],
    real:[0.9645,0.9287,0.7897,0.8923,0.8082,0.8558]}},
  {{metric:'Cancelaciones',fmt:'pct',isNegGood:true,
    plan:[null,null,null,0.05,0.05,0.05],
    real:[0.0789,0.0519,0.0878,0.0510,0.0523,0.0668]}},
];

function buildPlan(){{
  const lastIdx=MONTHS.length-1;
  document.getElementById('h-plan').innerHTML=`<tr>
    <th class="left" style="min-width:180px"><span class="dt">Métrica</span></th>
    <th class="left" style="min-width:60px"><span class="dt">Apertura</span></th>
    ${{MONTHS.map((m,i)=>{{
      const isLast=i===lastIdx;
      const cls=isLast?'last':(i>=4?'hl':'');
      return`<th class="${{cls}}"><span class="rel">${{relLabel(MONTHS,i)}}</span><span class="dt">${{m}}</span></th>`;
    }}).join('')}}
    <th class="last"><span class="rel">vs plan</span><span class="dt">Mar'26</span></th></tr>`;
  const tb=document.getElementById('b-plan');tb.innerHTML='';
  planData.forEach(row=>{{
    // Plan row
    const trP=document.createElement('tr');
    const subLbl=row.sub?`<div style="font-size:9px;color:#3a5070;font-weight:400;margin-top:1px">${{row.sub}}</div>`:'';
    let hP=`<td class="metric left">${{row.metric}}${{subLbl}}</td>`;
    hP+=`<td class="apert left" style="color:#3a5070;font-size:11px">◌ Plan V2</td>`;
    row.plan.forEach((v,i)=>{{
      const isLast=i===lastIdx;
      const cls=isLast?'last':(i>=4?'hl':'');
      hP+=`<td class="${{cls}}" style="opacity:0.55;font-style:italic">${{fv(v,row.fmt)}}</td>`;
    }});
    hP+=`<td class="last neu">—</td>`;
    trP.innerHTML=hP;tb.appendChild(trP);
    // Real row
    const trR=document.createElement('tr');
    let hR=`<td></td>`;
    hR+=`<td class="apert-total left" style="font-size:11px">● Real</td>`;
    row.real.forEach((v,i)=>{{
      const isLast=i===lastIdx;
      const cls=isLast?'last':(i>=4?'hl':'');
      hR+=`<td class="${{cls}}">${{fv(v,row.fmt)}}</td>`;
    }});
    // vs Plan March
    const pm=row.plan[lastIdx],rm=row.real[lastIdx];
    let vs='<td class="last neu">—</td>';
    if(pm!=null&&rm!=null){{
      const isPct=row.fmt==='pct'||row.fmt==='pp';
      const d=isPct?(rm-pm)*100:(rm-pm)/pm*100;
      const txt=(d>=0?'+':'')+d.toFixed(isPct?2:1)+(isPct?'pp':'%');
      const good=row.isNegGood?d<=0:d>=0;
      vs=`<td class="last ${{good?'pos':'neg'}}">${{txt}}</td>`;
    }}
    hR+=vs;
    trR.innerHTML=hR;tb.appendChild(trR);
  }});
}}

function rebuildAll(){{
  buildHead('hg',MONTHS,4);
  buildBody('bg',growthData,MONTHS,4,5);
  buildHead('ho',MONTHS,4);
  buildBody('bo',opsData,MONTHS,4,5);
  buildHead('hd',MONTHS,4);
  buildBody('bd',demandaData,MONTHS,4,5);
  buildHead('hn',MONTHS_NPS,2);
  buildBody('bn',npsData,MONTHS_NPS,2,3);
  buildPLHead('hpt',MONTHS,false);
  buildPLBody('bpt',plLines,nmvVals,MONTHS,false);
  buildPLHead('hpti',tiendasNames,true);
  buildPLBody('bpti',plTiendas,tiendasNmv,tiendasNames,true);
  buildVertBody();
}}

rebuildAll();
</script>
</body>
</html>"""

out = r"C:\Users\srioboo\dashboard_data\proximity_groceries_scorecard.html"
with open(out,'w',encoding='utf-8') as f:
    f.write(html)
print(f"Dashboard generado: {out}")
print(f"Tamaño: {len(html):,} caracteres")
