#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_dashboard.py  —  Proximity Groceries Scorecard v4
Lee los CSVs generados por los fetch scripts y produce index.html.

Novedades v4:
  - Tab CX mensual: compra perfecta, reclamos por tipo (+ por tienda)
  - Tab P&L mensual: TGMV, NMV, monetización, contribuciones
  - Fechas Móviles 28 días en vista mensual
  - Tab Assortment (placeholder estructurado)
"""
import os, json, math, sys, traceback
from datetime import datetime, date, timedelta
import pandas as pd

# Fix encoding en Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DATA_DIR    = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(DATA_DIR, 'index.html')

STORES       = ["Scalabrini", "Caballito", "Villa Urquiza", "Vicente Lopez"]
STORE_COLORS = {"Scalabrini": "#3b82f6", "Caballito": "#f59e0b",
                "Villa Urquiza": "#10b981", "Vicente Lopez": "#a855f7"}
MONTHS_ES    = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]

# ── HELPERS ───────────────────────────────────────────────────────────────────

def load(fname):
    path = os.path.join(DATA_DIR, fname)
    if not os.path.exists(path):
        print(f"  ⚠  {fname} not found")
        return None
    try:
        df = pd.read_csv(path, encoding='utf-8', encoding_errors='replace')
    except Exception:
        try:
            df = pd.read_csv(path, encoding='latin-1')
        except Exception as e:
            print(f"  ⚠  Error leyendo {fname}: {e}")
            return None
    for col in df.columns:
        if col.lower() in ('fecha', 'semana'):
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            df = df[df[col].notna()]
    return df

def sf(v):
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except:
        return None

def day_str(d):
    m = MONTHS_ES[d.month-1].lower()
    return f"{d.day} {m}"

def week_label(d, all_weeks):
    idx = all_weeks.index(d)
    n   = len(all_weeks) - 1 - idx
    ds  = day_str(d)
    return f"Ult.sem ({ds})" if n == 0 else f"S-{n} ({ds})"

def month_key(d):
    return str(date(d.year, d.month, 1))

def month_label_from_key(mk):
    d = date.fromisoformat(mk)
    return f"{MONTHS_ES[d.month-1]}'{str(d.year)[2:]}"

def month_key_from_ym(ym):
    """Convierte MONTH_FINAL INTEGER (202510) → '2025-10-01'"""
    ym = str(int(float(ym)))
    year, month = int(ym[:4]), int(ym[4:])
    return str(date(year, month, 1))

def day_label(d):
    """'2026-04-27' → '27/abr'"""
    return f"{d.day}/{MONTHS_ES[d.month-1].lower()}"

DAILY_WINDOW = 28   # días a mostrar en vista diaria

# ── LOAD CSVS ─────────────────────────────────────────────────────────────────

print("Loading CSVs...")
wg   = load('weekly_growth.csv')
ws   = load('weekly_by_store.csv')
wb   = load('weekly_buyers.csv')
wo   = load('weekly_ops.csv')
wv   = load('weekly_visits.csv')
wcvr = load('weekly_cvr.csv')
dgf  = load('daily_growth_full.csv')
dvf  = load('daily_visits_full.csv')
dcvr = load('daily_cvr.csv')
dops = load('daily_ops.csv')
mpl_raw       = load('monthly_pl.csv')
mcx_raw       = load('monthly_cx.csv')
mcx_store_raw = load('monthly_cx_by_store.csv')
plan_monthly  = load('plan_monthly.csv')
plan_daily    = load('plan_daily.csv')
plan_weekly   = load('plan_weekly.csv')
nps_monthly   = load('monthly_nps.csv')
q1   = load('q1_clean.csv')
q2   = load('q2_clean.csv')
q3   = load('q3_buyers_ytd.csv')

# ── WEEKLY GROWTH ─────────────────────────────────────────────────────────────

weeks   = sorted(wg['Fecha'].tolist()) if wg is not None else []
wlabels = {str(d): week_label(d, weeks) for d in weeks}

wg_rows = {}
if wg is not None:
    for _, r in wg.iterrows():
        d = str(r['Fecha'])
        c = sf(r.get('Compras')); o = sf(r.get('Ordenes'))
        nmv = sf(r.get('NMV')); nsi = sf(r.get('NSI')); tsie = sf(r.get('TSIE_total'))
        wg_rows[d] = dict(
            Compras  = c, Ordenes = o, NMV = nmv, NSI = nsi,
            APV_LC   = sf(nmv/c)    if (c and nmv)    else None,
            NASP_LC  = sf(nmv/o)    if (o and nmv)    else None,
            FR_Items = sf(nsi/tsie) if (tsie and nsi) else None,
            NSI_Cart = sf(nsi/o)    if (o and nsi)    else None,
            Ord_Comp = sf(o/c)      if c              else None,
        )

if wv is not None:
    for _, r in wv.iterrows():
        d = str(r['Fecha'])
        if d in wg_rows: wg_rows[d]['Visitas'] = sf(r['Visitas'])

if wcvr is not None:
    for _, r in wcvr.iterrows():
        d = str(r['Semana'])
        if d in wg_rows:
            wg_rows[d]['CVR']    = sf(r['CVR'])
            wg_rows[d]['Buyers'] = sf(r['Buyers'])

if ws is not None:
    store_counts = ws.groupby('Semana')['Tienda'].nunique()
    for d, cnt in store_counts.items():
        dk = str(d)
        if dk in wg_rows: wg_rows[dk]['Stores'] = int(cnt)

# ── WEEKLY OPS ────────────────────────────────────────────────────────────────

wo_rows = {}
if wo is not None:
    for _, r in wo.iterrows():
        d = str(r['Semana'])
        wo_rows[d] = dict(
            FR_Items        = sf(r['FR_Items']),
            FR_Items_Remp   = sf(r['FR_Items_Reemplazo']),
            FR_Compras      = sf(r['FR_Compras']),
            FR_Compras_Remp = sf(r['FR_Compras_Reemplazo']),
            Compra_Perfecta = sf(r.get('Compra_Perfecta')),
            On_Time         = sf(r['On_Time']),
            Cancel_Rate     = sf(r['Cancel_Rate']),
            Cancel_Buyer    = sf(r.get('Cancel_Buyer')),
            Cancel_Stockout = sf(r.get('Cancel_Stockout')),
            Cancel_Seller   = sf(r.get('Cancel_Seller')),
        )

# ── WEEKLY BUYERS ─────────────────────────────────────────────────────────────

wb_rows = {}
if wb is not None:
    for _, r in wb.iterrows():
        d = str(r['Semana']); t = r['TIENDA']
        if d not in wb_rows: wb_rows[d] = {}
        wb_rows[d][t] = sf(r['Buyers'])

# ── WEEKLY STORE BREAKDOWN ────────────────────────────────────────────────────

ws_rows = {}
if ws is not None:
    for _, r in ws.iterrows():
        d = str(r['Semana']); t = r['Tienda']
        if d not in ws_rows: ws_rows[d] = {}
        ws_rows[d][t] = dict(
            Compras=sf(r['Compras']), Ordenes=sf(r['Ordenes']),
            NMV=sf(r['NMV']), NSI=sf(r['NSI']), TSIE=sf(r.get('TSIE_total'))
        )

# ── MONTHLY AGGREGATION ──────────────────────────────────────────────────────
# Usa daily_growth_full.csv como fuente primaria para capturar semanas parciales
# (ej: días del mes actual que todavía no completaron una semana entera).
# Para tiendas/buyers/ops se sigue usando el semanal (no hay daily equivalente).

mg_rows = {}
if dgf is not None and len(dgf) > 0:
    dgf2 = dgf.copy(); dgf2['mk'] = dgf2['Fecha'].apply(month_key)
    for mk, grp in dgf2.groupby('mk'):
        c = sf(grp['Compras'].sum()); o = sf(grp['Ordenes'].sum())
        nmv = sf(grp['NMV'].sum()); nsi = sf(grp['NSI'].sum()); tsie = sf(grp['TSIE_total'].sum())
        mg_rows[mk] = dict(
            Compras  = c, Ordenes = o, NMV = nmv, NSI = nsi,
            APV_LC   = sf(nmv/c)    if (c and nmv)    else None,
            NASP_LC  = sf(nmv/o)    if (o and nmv)    else None,
            FR_Items = sf(nsi/tsie) if (tsie and nsi) else None,
            NSI_Cart = sf(nsi/o)    if (o and nsi)    else None,
            Ord_Comp = sf(o/c)      if c              else None,
        )
elif wg is not None:
    # fallback a semanal si no hay daily
    wg2 = wg.copy(); wg2['mk'] = wg2['Fecha'].apply(month_key)
    for mk, grp in wg2.groupby('mk'):
        c = sf(grp['Compras'].sum()); o = sf(grp['Ordenes'].sum())
        nmv = sf(grp['NMV'].sum()); nsi = sf(grp['NSI'].sum()); tsie = sf(grp['TSIE_total'].sum())
        mg_rows[mk] = dict(
            Compras  = c, Ordenes = o, NMV = nmv, NSI = nsi,
            APV_LC   = sf(nmv/c)    if (c and nmv)    else None,
            NASP_LC  = sf(nmv/o)    if (o and nmv)    else None,
            FR_Items = sf(nsi/tsie) if (tsie and nsi) else None,
            NSI_Cart = sf(nsi/o)    if (o and nsi)    else None,
            Ord_Comp = sf(o/c)      if c              else None,
        )

if dvf is not None and len(dvf) > 0:
    dvf2 = dvf.copy(); dvf2['mk'] = dvf2['Fecha'].apply(month_key)
    for mk, grp in dvf2.groupby('mk'):
        if mk in mg_rows: mg_rows[mk]['Visitas'] = sf(grp['Visitas'].sum())
elif wv is not None:
    wv2 = wv.copy(); wv2['mk'] = wv2['Fecha'].apply(month_key)
    for mk, grp in wv2.groupby('mk'):
        if mk in mg_rows: mg_rows[mk]['Visitas'] = sf(grp['Visitas'].sum())

if wcvr is not None:
    wcvr2 = wcvr.copy(); wcvr2['mk'] = wcvr2['Semana'].apply(month_key)
    for mk, grp in wcvr2.groupby('mk'):
        if mk in mg_rows:
            buyers = sf(grp['Buyers'].sum()); visitors = sf(grp['Visitors'].sum())
            mg_rows[mk]['Buyers'] = buyers; mg_rows[mk]['Visitors'] = visitors
            mg_rows[mk]['CVR'] = sf(buyers/visitors) if visitors else None

if ws is not None:
    ws2 = ws.copy(); ws2['mk'] = ws2['Semana'].apply(month_key)
    store_counts_m = ws2.groupby('mk')['Tienda'].nunique()
    for mk, cnt in store_counts_m.items():
        if mk in mg_rows: mg_rows[mk]['Stores'] = int(cnt)

months  = sorted(mg_rows.keys())
_today_mk = month_key(date.today())
mlabels = {
    mk: (month_label_from_key(mk) + ' MTD' if mk == _today_mk else month_label_from_key(mk))
    for mk in months
}

# Monthly ops
mo_rows = {}
if wo is not None:
    wo2 = wo.copy(); wo2['mk'] = wo2['Semana'].apply(month_key)
    for mk, grp in wo2.groupby('mk'):
        def wcol(col): return sf(grp[col].mean()) if col in grp.columns else None
        mo_rows[mk] = dict(
            FR_Items        = wcol('FR_Items'),
            FR_Items_Remp   = wcol('FR_Items_Reemplazo'),
            FR_Compras      = wcol('FR_Compras'),
            FR_Compras_Remp = wcol('FR_Compras_Reemplazo'),
            Compra_Perfecta = wcol('Compra_Perfecta'),
            On_Time         = wcol('On_Time'),
            Cancel_Rate     = wcol('Cancel_Rate'),
            Cancel_Buyer    = wcol('Cancel_Buyer'),
            Cancel_Stockout = wcol('Cancel_Stockout'),
            Cancel_Seller   = wcol('Cancel_Seller'),
        )

# Monthly buyers
mb_rows = {}
if wb is not None:
    wb2 = wb.copy(); wb2['mk'] = wb2['Semana'].apply(month_key)
    for (mk, tienda), grp in wb2.groupby(['mk', 'TIENDA']):
        if mk not in mb_rows: mb_rows[mk] = {}
        mb_rows[mk][tienda] = sf(grp['Buyers'].sum())

# Monthly store breakdown
ms_rows = {}
if ws is not None:
    ws3 = ws.copy(); ws3['mk'] = ws3['Semana'].apply(month_key)
    for (mk, tienda), grp in ws3.groupby(['mk', 'Tienda']):
        if mk not in ms_rows: ms_rows[mk] = {}
        ms_rows[mk][tienda] = dict(
            Compras=sf(grp['Compras'].sum()), Ordenes=sf(grp['Ordenes'].sum()),
            NMV=sf(grp['NMV'].sum()), NSI=sf(grp['NSI'].sum())
        )

# ── DAILY VIEW DATA (últimos 28 días) ────────────────────────────────────────

daily_dates = []   # list of date objects, sorted asc, last 28 days
dg_rows  = {}      # str(date) → growth metrics
dv_rows  = {}      # str(date) → visitas
dcvr_day = {}      # str(date) → buyers, CVR
do_rows  = {}      # str(date) → ops metrics

if dgf is not None and len(dgf) > 0:
    max_d = dgf['Fecha'].max()
    cutoff = max_d - timedelta(days=DAILY_WINDOW - 1)
    dgf_w = dgf[dgf['Fecha'] >= cutoff].copy()
    # build full date range (fill gaps with None)
    all_dates = sorted(set(dgf_w['Fecha'].tolist()))
    daily_dates = all_dates
    for _, r in dgf_w.iterrows():
        dk = str(r['Fecha'])
        c = sf(r.get('Compras')); o = sf(r.get('Ordenes'))
        nmv = sf(r.get('NMV')); nsi = sf(r.get('NSI')); tsie = sf(r.get('TSIE_total'))
        dg_rows[dk] = dict(
            Compras  = c, Ordenes = o, NMV = nmv, NSI = nsi,
            APV_LC   = sf(nmv/c)    if (c and nmv)    else None,
            NASP_LC  = sf(nmv/o)    if (o and nmv)    else None,
            FR_Items = sf(nsi/tsie) if (tsie and nsi) else None,
            NSI_Cart = sf(nsi/o)    if (o and nsi)    else None,
            Ord_Comp = sf(o/c)      if c              else None,
        )

if dvf is not None and len(dvf) > 0 and daily_dates:
    cutoff = daily_dates[0]
    dvf_w = dvf[dvf['Fecha'] >= cutoff]
    for _, r in dvf_w.iterrows():
        dv_rows[str(r['Fecha'])] = sf(r['Visitas'])

if dcvr is not None and daily_dates:
    cutoff = daily_dates[0]
    dcvr_w = dcvr[dcvr['Fecha'] >= cutoff]
    for _, r in dcvr_w.iterrows():
        dk = str(r['Fecha'])
        dcvr_day[dk] = dict(
            Buyers  = sf(r.get('Buyers')),
            CVR     = sf(r.get('CVR')),
            Visitors= sf(r.get('Visitors')),
        )

if dops is not None and daily_dates:
    cutoff = daily_dates[0]
    dops_w = dops[dops['Fecha'] >= cutoff]
    for _, r in dops_w.iterrows():
        dk = str(r['Fecha'])
        do_rows[dk] = dict(
            FR_Items        = sf(r['FR_Items']),
            FR_Items_Remp   = sf(r['FR_Items_Reemplazo']),
            FR_Compras      = sf(r['FR_Compras']),
            FR_Compras_Remp = sf(r['FR_Compras_Reemplazo']),
            Compra_Perfecta = sf(r.get('Compra_Perfecta')),
            On_Time         = sf(r['On_Time']),
            Cancel_Rate     = sf(r['Cancel_Rate']),
            Cancel_Buyer    = sf(r.get('Cancel_Buyer')),
            Cancel_Stockout = sf(r.get('Cancel_Stockout')),
            Cancel_Seller   = sf(r.get('Cancel_Seller')),
        )

# Enrich daily growth with visits and CVR
for dk in dg_rows:
    if dk in dv_rows:  dg_rows[dk]['Visitas'] = dv_rows[dk]
    if dk in dcvr_day:
        dg_rows[dk]['Buyers'] = dcvr_day[dk]['Buyers']
        dg_rows[dk]['CVR']    = dcvr_day[dk]['CVR']

daily_date_strs  = [str(d) for d in daily_dates]
daily_label_map  = {str(d): day_label(d) for d in daily_dates}

# ── P&L MENSUAL ───────────────────────────────────────────────────────────────

mpl_rows = {}
if mpl_raw is not None:
    for _, r in mpl_raw.iterrows():
        try:
            mk = month_key_from_ym(r['Mes'])
            mpl_rows[mk] = dict(
                TGMV                    = sf(r.get('TGMV')),
                NMV                     = sf(r.get('NMV')),
                Net_Variable_Fee        = sf(r.get('Net_Variable_Fee')),
                Net_Monetization        = sf(r.get('Net_Monetization')),
                Product_Net_Monetization= sf(r.get('Product_Net_Monetization')),
                Variable_Contribution   = sf(r.get('Variable_Contribution')),
                Direct_Contribution     = sf(r.get('Direct_Contribution')),
                Shipping_Cost           = sf(r.get('Shipping_Cost')),
                Promotions              = sf(r.get('Promotions')),
                Coupons                 = sf(r.get('Coupons')),
                Take_Rate               = sf(r.get('Take_Rate')),
                VC_Over_NMV             = sf(r.get('VC_Over_NMV')),
                DC_Over_NMV             = sf(r.get('DC_Over_NMV')),
            )
        except Exception as e:
            print(f"  P&L row error: {e}")

# Computed P&L metrics (no están en el CSV, se derivan)
for mk, r in mpl_rows.items():
    nmv = r.get('NMV')
    if nmv and nmv > 0:
        net_mon = r.get('Net_Monetization')
        var_con = r.get('Variable_Contribution')
        dir_con = r.get('Direct_Contribution')
        r['Take_Rate']   = sf(net_mon / nmv) if net_mon is not None else None
        r['VC_Over_NMV'] = sf(var_con / nmv) if var_con is not None else None
        r['DC_Over_NMV'] = sf(dir_con / nmv) if dir_con is not None else None

# ── CX MENSUAL ────────────────────────────────────────────────────────────────

mcx_rows = {}
if mcx_raw is not None:
    # Parse 'Mes' column as date
    for col in mcx_raw.columns:
        if col.lower() == 'mes':
            mcx_raw[col] = pd.to_datetime(mcx_raw[col], errors='coerce').dt.date
    for _, r in mcx_raw.iterrows():
        if pd.isnull(r['Mes']): continue
        mk = str(date(r['Mes'].year, r['Mes'].month, 1))
        mcx_rows[mk] = dict(
            Perfect_Purchases     = sf(r.get('Perfect_Purchases')),
            Total_Purchases       = sf(r.get('Total_Purchases')),
            Perfect_Purchase_Rate = sf(r.get('Perfect_Purchase_Rate')),
            Total_Claims          = sf(r.get('Total_Claims')),
            Repentance            = sf(r.get('Repentance_Claims')),
            Incomplete            = sf(r.get('Incomplete_Claims')),
            Different             = sf(r.get('Different_Claims')),
            Defective             = sf(r.get('Defective_Claims')),
            Claims_Rate           = sf(r.get('Claims_Rate')),
            On_Time_CX            = sf(r.get('On_Time')),
            Cancel_Rate_CX        = sf(r.get('Cancel_Rate')),
        )

# CX por tienda
mcx_store_rows = {}
if mcx_store_raw is not None:
    for col in mcx_store_raw.columns:
        if col.lower() == 'mes':
            mcx_store_raw[col] = pd.to_datetime(mcx_store_raw[col], errors='coerce').dt.date
    for _, r in mcx_store_raw.iterrows():
        if pd.isnull(r['Mes']): continue
        mk = str(date(r['Mes'].year, r['Mes'].month, 1))
        tienda = r.get('Tienda', '')
        if mk not in mcx_store_rows: mcx_store_rows[mk] = {}
        mcx_store_rows[mk][tienda] = dict(
            Perfect_Purchase_Rate = sf(r.get('Perfect_Purchase_Rate')),
            Claims_Rate           = sf(r.get('Claims_Rate')),
            Total_Claims          = sf(r.get('Total_Claims')),
            On_Time               = sf(r.get('On_Time')),
        )

# ── ROLLING 7 DÍAS (para semanal) ────────────────────────────────────────────

rolling = []
if dgf is not None and len(dgf) > 0:
    max_date = dgf['Fecha'].max()
    for i in range(13, -1, -1):
        end   = max_date - timedelta(days=i * 7)
        start = end - timedelta(days=6)
        gg = dgf[(dgf['Fecha'] >= start) & (dgf['Fecha'] <= end)]
        vv = dvf[(dvf['Fecha'] >= start) & (dvf['Fecha'] <= end)] if dvf is not None else pd.DataFrame()
        c   = sf(gg['Compras'].sum())    if len(gg) else None
        o   = sf(gg['Ordenes'].sum())    if len(gg) else None
        nmv = sf(gg['NMV'].sum())        if len(gg) else None
        nsi = sf(gg['NSI'].sum())        if len(gg) else None
        tsie= sf(gg['TSIE_total'].sum()) if len(gg) else None
        vis = sf(vv['Visitas'].sum())    if len(vv) else None
        label = "P" if i == 0 else f"P-{i}"
        p = dict(
            label=label, start=str(start), end=str(end),
            NMV=nmv, Compras=c, Ordenes=o, NSI=nsi,
            APV_LC   = sf(nmv/c)    if (c and nmv)    else None,
            FR_Items = sf(nsi/tsie) if (tsie and nsi) else None,
            Visitas  = vis,
        )
        if dcvr is not None:
            dd = dcvr[(dcvr['Fecha'] >= start) & (dcvr['Fecha'] <= end)]
            if len(dd):
                buyers = sf(dd['Buyers'].sum()); visitors = sf(dd['Visitors'].sum())
                p['CVR'] = sf(buyers/visitors) if visitors else None
        rolling.append(p)

# ── ROLLING 28 DÍAS (para mensual) ───────────────────────────────────────────

rolling28 = []
if dgf is not None and len(dgf) > 0:
    max_date = dgf['Fecha'].max()
    for i in range(6, -1, -1):        # 7 períodos de 28 días
        end   = max_date - timedelta(days=i * 28)
        start = end - timedelta(days=27)
        gg = dgf[(dgf['Fecha'] >= start) & (dgf['Fecha'] <= end)]
        vv = dvf[(dvf['Fecha'] >= start) & (dvf['Fecha'] <= end)] if dvf is not None else pd.DataFrame()
        c   = sf(gg['Compras'].sum())    if len(gg) else None
        o   = sf(gg['Ordenes'].sum())    if len(gg) else None
        nmv = sf(gg['NMV'].sum())        if len(gg) else None
        nsi = sf(gg['NSI'].sum())        if len(gg) else None
        tsie= sf(gg['TSIE_total'].sum()) if len(gg) else None
        vis = sf(vv['Visitas'].sum())    if len(vv) else None
        label = "P28" if i == 0 else f"P28-{i}"
        p = dict(
            label=label, start=str(start), end=str(end),
            NMV=nmv, Compras=c, Ordenes=o, NSI=nsi,
            APV_LC   = sf(nmv/c)    if (c and nmv)    else None,
            FR_Items = sf(nsi/tsie) if (tsie and nsi) else None,
            Visitas  = vis,
        )
        if dcvr is not None:
            dd = dcvr[(dcvr['Fecha'] >= start) & (dcvr['Fecha'] <= end)]
            if len(dd):
                buyers = sf(dd['Buyers'].sum()); visitors = sf(dd['Visitors'].sum())
                p['CVR'] = sf(buyers/visitors) if visitors else None
        rolling28.append(p)

# ── DEMOGRAPHICS ─────────────────────────────────────────────────────────────

def age_group(rango):
    try:
        start = int(str(rango).split('-')[0].strip().split()[0])
        if start < 30:  return '18-29'
        elif start < 45: return '30-44'
        elif start < 60: return '45-59'
        else:            return '60+'
    except: return 'Sin clasif.'

demo_data   = {}
NSE_ORDER   = ['PLATINUM','GOLD','SILVER','BRONZE','Sin clasif.']
AGE_ORDER   = ['18-29','30-44','45-59','60+']
VALID_NSE   = {'BRONZE','SILVER','GOLD','PLATINUM'}

if q1 is not None:
    q1c = q1.copy()
    q1c['Mes'] = pd.to_datetime(q1c['Mes'], errors='coerce').dt.date
    q1c = q1c[q1c['Mes'].notna()].copy()
    q1c['mk']      = q1c['Mes'].apply(month_key)
    q1c['age_grp'] = q1c['Rango_edad'].apply(age_group)
    q1c['nse_grp'] = q1c['Segmento'].apply(lambda x: x if x in VALID_NSE else 'Sin clasif.')

    nse_by_month = {}
    for (mk, nse), grp in q1c.groupby(['mk','nse_grp']):
        if mk not in nse_by_month: nse_by_month[mk] = {}
        nse_by_month[mk][nse] = int(grp['MABs'].sum())

    nse_by_store = {}
    for (tienda, nse), grp in q1c.groupby(['TIENDA','nse_grp']):
        if tienda not in nse_by_store: nse_by_store[tienda] = {}
        nse_by_store[tienda][nse] = int(grp['MABs'].sum())

    age_by_month = {}
    for (mk, age), grp in q1c.groupby(['mk','age_grp']):
        if mk not in age_by_month: age_by_month[mk] = {}
        age_by_month[mk][age] = int(grp['MABs'].sum())

    demo_data['nse_by_month'] = nse_by_month
    demo_data['nse_by_store'] = nse_by_store
    demo_data['age_by_month'] = age_by_month

if q2 is not None:
    q2c = q2.copy()
    q2c['Mes'] = pd.to_datetime(q2c['Mes'], errors='coerce').dt.date
    q2c = q2c[q2c['Mes'].notna()].copy()
    q2c['mk'] = q2c['Mes'].apply(month_key)
    gender_by_month = {}
    for (mk, gen), grp in q2c[q2c['Genero'].isin(['female','male'])].groupby(['mk','Genero']):
        if mk not in gender_by_month: gender_by_month[mk] = {}
        gender_by_month[mk][gen] = int(grp['MABs'].sum())
    demo_data['gender_by_month'] = gender_by_month

demo_data['nse_order'] = NSE_ORDER
demo_data['age_order'] = AGE_ORDER

# Buyers YTD únicos (de q3_buyers_ytd.csv)
buyers_ytd_by_month = {}
buyers_ytd_total    = 0
if q3 is not None:
    q3c = q3.copy()
    q3c['Mes'] = pd.to_datetime(q3c['Mes'], errors='coerce').dt.date
    q3c = q3c[q3c['Mes'].notna()]
    for _, r in q3c.iterrows():
        mk = str(date(r['Mes'].year, r['Mes'].month, 1))
        v  = int(r['Total_Buyers'])
        buyers_ytd_by_month[mk] = v
        buyers_ytd_total        += v
demo_data['buyers_ytd_by_month'] = buyers_ytd_by_month
demo_data['buyers_ytd_total']    = buyers_ytd_total

# ── PLAN DATA ────────────────────────────────────────────────────────────────

plan_monthly_dict = {}   # mk → {NMV_V2, NMV_4+8}
if plan_monthly is not None:
    for _, r in plan_monthly.iterrows():
        mk = str(date.fromisoformat(str(r['Mes'])[:10]))
        plan_monthly_dict[mk] = {
            'NMV_V2':  sf(r.get('NMV_V2')),
            'NMV_4p8': sf(r.get('NMV_4+8')),
        }

plan_daily_dict = {}   # date_str → {NMV_V2, NSI_V2, NMV_4+8, NSI_4+8}
if plan_daily is not None:
    for col in plan_daily.columns:
        if col.lower() == 'fecha':
            plan_daily['Fecha'] = pd.to_datetime(plan_daily['Fecha'], errors='coerce').dt.date
    for _, r in plan_daily.iterrows():
        if pd.isnull(r['Fecha']): continue
        dk = str(r['Fecha'])
        plan_daily_dict[dk] = {
            'NMV_V2':  sf(r.get('NMV_V2')),
            'NSI_V2':  sf(r.get('NSI_V2')),
            'NMV_4p8': sf(r.get('NMV_4+8')),
            'NSI_4p8': sf(r.get('NSI_4+8')),
        }

plan_weekly_dict = {}   # week_str → {NMV_V2, NSI_V2, NMV_4+8, NSI_4+8}
if plan_weekly is not None:
    for col in plan_weekly.columns:
        if col.lower() == 'semana':
            plan_weekly['Semana'] = pd.to_datetime(plan_weekly['Semana'], errors='coerce').dt.date
    for _, r in plan_weekly.iterrows():
        if pd.isnull(r['Semana']): continue
        wk = str(r['Semana'])
        plan_weekly_dict[wk] = {
            'NMV_V2':  sf(r.get('NMV_V2')),
            'NSI_V2':  sf(r.get('NSI_V2')),
            'NMV_4p8': sf(r.get('NMV_4+8')),
            'NSI_4p8': sf(r.get('NSI_4+8')),
        }

# ── NPS DATA ─────────────────────────────────────────────────────────────────

nps_data = {}   # mk → {Tienda → NPS_Score}
if nps_monthly is not None:
    for col in nps_monthly.columns:
        if col.lower() == 'mes':
            nps_monthly[col] = pd.to_datetime(nps_monthly[col], errors='coerce').dt.date
    for _, r in nps_monthly.iterrows():
        if pd.isnull(r['Mes']): continue
        mk     = str(date(r['Mes'].year, r['Mes'].month, 1))
        t = r.get('Tienda')
        tienda = 'Total' if (t is None or str(t).strip() in ('', 'None', 'nan')) else str(t)
        if mk not in nps_data: nps_data[mk] = {}
        nps_data[mk][tienda] = sf(r.get('NPS_Score'))

# ── ASSORTMENT DATA ───────────────────────────────────────────────────────────
# Datos del Google Sheet "Stock Diario" (Ene-Abr 2026, 4 tiendas)
ASSORTMENT = {
    'months': ["Ene'26","Feb'26","Mar'26","Abr'26"],
    'stores': {
        'Caballito':    {'Disponibilidad':[0.710,0.714,0.702,0.708],'Profundidad':[0.5879,0.615,0.600,0.525],'Gap':[12.2, 9.9,10.2,18.3]},
        'Scalabrini':   {'Disponibilidad':[0.690,0.696,0.687,0.711],'Profundidad':[0.5903,0.630,0.616,0.557],'Gap':[10.0, 6.6, 7.1,15.4]},
        'Vicente Lopez':{'Disponibilidad':[0.721,0.724,0.709,0.737],'Profundidad':[0.6010,0.628,0.627,0.570],'Gap':[12.0, 9.6, 8.2,16.7]},
        'Villa Urquiza':{'Disponibilidad':[0.699,0.707,0.699,0.711],'Profundidad':[0.5791,0.602,0.618,0.543],'Gap':[12.0,10.6, 8.2,16.8]},
    }
}

# ── ASSEMBLE DATA BLOB ────────────────────────────────────────────────────────

DATA = dict(
    generated       = datetime.now().strftime('%d/%m/%Y %H:%M'),
    # Semanal
    weeks           = [str(d) for d in weeks],
    wlabels         = wlabels,
    wg              = wg_rows,
    wo              = wo_rows,
    wb              = wb_rows,
    ws              = ws_rows,
    # Mensual
    months          = months,
    mlabels         = mlabels,
    mg              = mg_rows,
    mo              = mo_rows,
    mb              = mb_rows,
    ms              = ms_rows,
    mpl             = mpl_rows,
    mcx             = mcx_rows,
    mcx_store       = mcx_store_rows,
    demo            = demo_data,
    # Diario (últimos 28 días)
    daily_dates     = daily_date_strs,
    dlabels         = daily_label_map,
    dg              = dg_rows,
    do              = do_rows,
    dcvr_day        = dcvr_day,
    # Rolling
    rolling         = rolling,
    rolling28       = rolling28,
    store_colors    = STORE_COLORS,
    stores          = STORES,
    # Plan
    plan_monthly    = plan_monthly_dict,
    plan_weekly     = plan_weekly_dict,
    plan_daily      = plan_daily_dict,
    # NPS dinámico
    nps_data        = nps_data,
    # Assortment
    assortment      = ASSORTMENT,
)

JSON_DATA = json.dumps(DATA, default=str, ensure_ascii=False)

# ── HTML ──────────────────────────────────────────────────────────────────────

HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Proximity Groceries Scorecard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f1f5f9;color:#0f172a;font-size:13px}
/* HEADER */
.header{background:#0f172a;padding:0 20px;display:flex;align-items:center;gap:16px;height:52px;position:sticky;top:0;z-index:100;box-shadow:0 2px 10px rgba(0,0,0,.3)}
.header-logo{width:30px;height:30px;background:linear-gradient(135deg,#3b82f6,#1d4ed8);border-radius:6px;display:flex;align-items:center;justify-content:center;color:#fff;font-weight:800;font-size:12px;flex-shrink:0;letter-spacing:-.5px}
.header-title{font-size:14px;font-weight:700;color:#f1f5f9;flex:1}
.header-sub{font-size:10px;color:#475569;margin-top:1px}
.toggle-group{display:flex;background:rgba(255,255,255,.07);border-radius:7px;padding:3px;gap:2px}
.toggle-btn{padding:5px 16px;border:none;background:transparent;border-radius:5px;cursor:pointer;font-size:11px;font-weight:600;color:#64748b;transition:all .15s;letter-spacing:.2px}
.toggle-btn.active{background:#1d4ed8;color:#fff;box-shadow:0 1px 5px rgba(29,78,216,.45)}
.updated{font-size:11px;color:#475569;white-space:nowrap}
/* TABS */
.tabs-bar{background:#fff;border-bottom:1px solid #e2e8f0;padding:0 20px;display:flex;gap:0;overflow-x:auto;scrollbar-width:none}
.tabs-bar::-webkit-scrollbar{display:none}
.tab-btn{padding:11px 15px;border:none;background:transparent;cursor:pointer;font-size:12px;font-weight:500;color:#64748b;border-bottom:2px solid transparent;margin-bottom:-1px;white-space:nowrap;transition:all .15s}
.tab-btn.active{color:#1d4ed8;border-bottom-color:#1d4ed8;font-weight:700}
.tab-btn:hover:not(.active){color:#0f172a;background:#f8fafc}
/* CONTENT */
.content{padding:16px 20px}
/* TABLE */
.table-wrap{overflow-x:auto;border-radius:10px;box-shadow:0 1px 5px rgba(0,0,0,.09);border:1px solid #e2e8f0}
.sc-table{border-collapse:collapse;width:100%;min-width:600px;background:#fff}
.sc-table thead{background:#0f172a}
.sc-table th{color:#94a3b8;font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.5px;padding:9px 12px;white-space:nowrap;text-align:right}
.sc-table th:first-child{text-align:left;min-width:185px;position:sticky;left:0;background:#0f172a;z-index:2}
.sc-table td{padding:7px 12px;border-bottom:1px solid #f1f5f9;text-align:right;white-space:nowrap;color:#334155;font-variant-numeric:tabular-nums}
.sc-table td:first-child{text-align:left;position:sticky;left:0;background:#fff;z-index:1;font-weight:500;color:#0f172a;border-right:1px solid #e2e8f0}
.sc-table tr:last-child td{border-bottom:none}
.sc-table tr:hover td{background:#f8fafc}
.sc-table tr:hover td:first-child{background:#f8fafc}
.sc-table .last-col{font-weight:700}
.sc-table .good{color:#059669}
.sc-table .bad{color:#dc2626}
.sc-table .neutral{color:#334155}
.sc-table .pct-good{color:#059669;font-size:10px;margin-left:3px}
.sc-table .pct-bad{color:#dc2626;font-size:10px;margin-left:3px}
.section-title{font-size:10.5px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.6px;margin:20px 0 8px;padding-bottom:5px;border-bottom:1px solid #e2e8f0}
/* SPARKLINE */
.spark{display:inline-block;vertical-align:middle}
/* STORE BREAKDOWN */
.store-section{margin-top:16px;border-radius:10px;background:#fff;box-shadow:0 1px 5px rgba(0,0,0,.09);border:1px solid #e2e8f0;overflow:hidden}
.store-section-header{padding:10px 14px;background:#f8fafc;border-bottom:1px solid #e2e8f0;display:flex;align-items:center;justify-content:space-between;cursor:pointer}
.store-section-header h3{font-size:10.5px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:.5px}
.store-toggle-icon{color:#94a3b8;font-size:13px;transition:transform .2s}
.store-section-body{display:none}
.store-section-body.open{display:block}
/* CHARTS */
.charts-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:14px;margin-top:4px}
.chart-card{background:#fff;border-radius:10px;box-shadow:0 1px 5px rgba(0,0,0,.09);border:1px solid #e2e8f0;padding:16px}
.chart-card h3{font-size:10.5px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px}
.chart-card canvas{max-height:200px}
/* PLACEHOLDER */
.placeholder{background:#fff;border-radius:10px;padding:48px;text-align:center;color:#94a3b8;box-shadow:0 1px 5px rgba(0,0,0,.09);border:1px solid #e2e8f0}
.placeholder h3{font-size:14px;font-weight:600;color:#475569;margin-bottom:8px}
/* STORE PILLS */
.store-pill{display:inline-block;padding:2px 9px;border-radius:4px;font-size:10px;font-weight:700;color:#fff;margin-right:4px;letter-spacing:.2px}
/* KPI CARDS */
.kpi-row{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px}
.kpi-card{background:#fff;border-radius:10px;box-shadow:0 1px 5px rgba(0,0,0,.09);border:1px solid #e2e8f0;border-top:3px solid #1d4ed8;padding:12px 16px;min-width:120px;flex:1}
.kpi-label{font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.5px;font-weight:700}
.kpi-value{font-size:22px;font-weight:800;color:#0f172a;margin-top:3px;letter-spacing:-.5px}
.kpi-delta{font-size:11px;margin-top:2px}
.kpi-delta.up{color:#059669}
.kpi-delta.down{color:#dc2626}
/* P&L */
.sc-table .pl-positive{color:#059669;font-weight:600}
.sc-table .pl-negative{color:#dc2626;font-weight:600}
.sc-table .pl-ratio{color:#1d4ed8;font-size:11px}
.sc-table .pl-separator td{background:#1e293b !important;color:#94a3b8 !important;font-size:9.5px !important;font-weight:700 !important;text-transform:uppercase !important;letter-spacing:.5px !important;padding:5px 12px !important}
/* BADGE */
.badge{display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;font-weight:700}
.badge-green{background:#dcfce7;color:#059669}
.badge-red{background:#fee2e2;color:#dc2626}
.badge-blue{background:#dbeafe;color:#1d4ed8}
.badge-yellow{background:#fef3c7;color:#d97706}
/* COMPACT (daily 28-col table) */
.sc-table.compact th{padding:7px 9px;font-size:9px}
.sc-table.compact td{padding:5px 9px;font-size:12px}
.sc-table.compact th:first-child,.sc-table.compact td:first-child{min-width:150px}
</style>
</head>
<body>

<div id="app">
  <div class="header">
    <div class="header-logo">PG</div>
    <div>
      <div class="header-title">Proximity Groceries Scorecard</div>
      <div class="header-sub" id="updated-label"></div>
    </div>
    <div style="flex:1"></div>
    <div class="toggle-group">
      <button class="toggle-btn" id="btn-diario"   onclick="setView('diario')">Diario</button>
      <button class="toggle-btn active" id="btn-semanal" onclick="setView('semanal')">Semanal</button>
      <button class="toggle-btn" id="btn-mensual"  onclick="setView('mensual')">Mensual</button>
      <button class="toggle-btn" id="btn-rolling"  onclick="setView('rolling')">Fechas Móviles</button>
    </div>
  </div>

  <div class="tabs-bar" id="tabs-bar"></div>
  <div class="content" id="content"></div>
</div>

<script>
const D = """ + JSON_DATA + """;
let VIEW = 'semanal';
let CUR_TAB = 'growth';

// ── PLAN DATA (desde CSVs parseados) ─────────────────────────────────────────
// D.plan_monthly[mk] = {NMV_V2, NMV_4p8}
// D.plan_weekly[wk]  = {NMV_V2, NSI_V2, NMV_4p8, NSI_4p8}
// D.plan_daily[dk]   = {NMV_V2, NSI_V2, NMV_4p8, NSI_4p8}  (desde Abril)
const PM = D.plan_monthly || {};
const PW = D.plan_weekly  || {};
const PD = D.plan_daily   || {};

const TABS_SEMANAL = [
  {id:'growth',   label:'Growth'},
  {id:'ops',      label:'Ops'},
  {id:'buyers',   label:'Buyers'},
  {id:'graficos', label:'Gráficos'},
  {id:'plan',     label:'Plan'},
];
const TABS_MENSUAL = [
  {id:'growth',     label:'Growth'},
  {id:'ops',        label:'Ops'},
  {id:'buyers',     label:'Buyers'},
  {id:'graficos',   label:'Gráficos'},
  {id:'cx',         label:'CX'},
  {id:'nps',        label:'NPS'},
  {id:'pl',         label:'P&L'},
  {id:'assortment', label:'Assortment'},
  {id:'demo',       label:'Demográficos'},
  {id:'plan',       label:'Plan'},
];
const TABS_ROLLING = [
  {id:'rolling7d',  label:'Rolling 7D'},
  {id:'rolling28d', label:'Rolling 28D'},
];
const TABS_DIARIO = [
  {id:'growth',   label:'Growth'},
  {id:'ops',      label:'Ops'},
  {id:'buyers',   label:'Buyers'},
  {id:'graficos', label:'Gráficos'},
  {id:'plan',     label:'Plan'},
];

// ── FORMAT HELPERS ────────────────────────────────────────────────────────────

function fmtNMV(v) {
  if (v == null) return '—';
  const s = v >= 0 ? '' : '-', a = Math.abs(v);
  if (a >= 1e12) return s + '$' + (a/1e12).toFixed(2) + 'T';
  if (a >= 1e9)  return s + '$' + (a/1e9).toFixed(1)  + 'B';
  if (a >= 1e6)  return s + '$' + (a/1e6).toFixed(1)  + 'M';
  if (a >= 1e3)  return s + '$' + Math.round(a/1e3)   + 'K';
  return s + '$' + Math.round(a).toLocaleString('es-AR');
}
function fmtCnt(v)   { if (v == null) return '—'; return Math.round(v).toLocaleString('es-AR'); }
function fmtPct(v)   { if (v == null) return '—'; return (v*100).toFixed(1) + '%'; }
function fmtMoney(v) {
  if (v == null) return '—';
  if (Math.abs(v) >= 1e6) return '$' + (v/1e6).toFixed(2) + 'M';
  if (Math.abs(v) >= 1e3) return '$' + Math.round(v/1e3) + 'K';
  return '$' + Math.round(v).toLocaleString('es-AR');
}
function fmtRatio(v, dec) {
  if (v == null) return '—';
  return v.toFixed(dec != null ? dec : 1);
}
function fmt(v, type) {
  switch(type) {
    case 'nmv':    return fmtNMV(v);
    case 'cnt':    return fmtCnt(v);
    case 'pct':    return fmtPct(v);
    case 'money':  return fmtMoney(v);
    case 'ratio':  return fmtRatio(v, 1);
    case 'ratio2': return fmtRatio(v, 2);
    case 'int':    return v == null ? '—' : String(Math.round(v));
    default:       return v == null ? '—' : String(v);
  }
}
function deltaClass(delta, higherBetter) {
  if (delta == null || Math.abs(delta) < 0.001) return 'neutral';
  if (higherBetter) return delta > 0 ? 'good' : 'bad';
  return delta < 0 ? 'good' : 'bad';
}
function deltaTxt(delta) {
  if (delta == null) return '';
  const sign = delta >= 0 ? '+' : '';
  return ` <span class="${delta >= 0 ? 'pct-good':'pct-bad'}">${sign}${(delta*100).toFixed(1)}%</span>`;
}

// ── SPARKLINE SVG ─────────────────────────────────────────────────────────────

function sparkline(vals, w, h, color) {
  w = w || 70; h = h || 22; color = color || '#3b82f6';
  const clean = vals.filter(v => v != null);
  if (clean.length < 2) return '<svg width="'+w+'" height="'+h+'"></svg>';
  const mn = Math.min(...clean), mx = Math.max(...clean);
  const range = mx - mn || 1;
  const pts = clean.map((v, i) => {
    const x = i / (clean.length-1) * (w-4) + 2;
    const y = h - 2 - ((v-mn)/range) * (h-4);
    return x.toFixed(1)+','+y.toFixed(1);
  }).join(' ');
  return `<svg class="spark" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
    <polyline points="${pts}" fill="none" stroke="${color}" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
  </svg>`;
}

// ── TABLE BUILDER ─────────────────────────────────────────────────────────────

function buildTable(periods, labelMap, rowDefs, dataMap, opts) {
  opts = opts || {};
  const showSparkline = opts.showSparkline !== false;
  let h = '<div class="table-wrap"><table class="sc-table"><thead><tr>';
  h += '<th>Métrica</th>';
  for (const p of periods) {
    const lbl = (labelMap && labelMap[p]) || p;
    h += `<th>${lbl}</th>`;
  }
  if (showSparkline) h += '<th style="min-width:80px">Tendencia</th>';
  h += '</tr></thead><tbody>';

  for (const row of rowDefs) {
    // Section separator row
    if (row.separator) {
      h += `<tr class="pl-separator"><td colspan="${periods.length + (showSparkline ? 2 : 1)}" style="padding:6px 12px;font-size:11px;letter-spacing:.4px;text-transform:uppercase">${row.name}</td></tr>`;
      continue;
    }
    const vals = periods.map(p => {
      const rec = dataMap[p];
      return rec ? (rec[row.key] != null ? rec[row.key] : null) : null;
    });
    const lastIdx = vals.length - 1;
    const lastVal = vals[lastIdx];
    const prevVal = vals.slice(0, lastIdx).reverse().find(v => v != null);
    const delta = (lastVal != null && prevVal != null && prevVal !== 0)
                  ? (lastVal - prevVal) / Math.abs(prevVal) : null;
    const extraClass = row.extraClass || '';

    h += '<tr>';
    h += `<td>${row.name}</td>`;
    for (let i = 0; i < periods.length; i++) {
      const v = vals[i];
      const isLast = i === lastIdx;
      let cls = extraClass;
      if (isLast) cls += ' last-col ' + deltaClass(delta, row.hb !== false);
      let txt = fmt(v, row.type);
      if (isLast && delta != null) txt += deltaTxt(delta);
      h += `<td class="${cls}">${txt}</td>`;
    }
    if (showSparkline) {
      const bad = row.hb === false;
      const lastDelta = (lastVal != null && prevVal != null && prevVal !== 0)
                        ? (lastVal - prevVal)/Math.abs(prevVal) : null;
      let sparkColor = '#3b82f6';
      if (lastDelta != null) sparkColor = (!bad ? lastDelta >= 0 : lastDelta <= 0) ? '#059669' : '#ef4444';
      h += `<td>${sparkline(vals, 70, 22, sparkColor)}</td>`;
    }
    h += '</tr>';
  }
  h += '</tbody></table></div>';
  return h;
}

// ── GROWTH TAB ────────────────────────────────────────────────────────────────

const GROWTH_ROWS = [
  {key:'NMV',      name:'NMV',               type:'nmv',   hb:true},
  {key:'Compras',  name:'Compras',            type:'cnt',   hb:true},
  {key:'Ordenes',  name:'Órdenes',            type:'cnt',   hb:true},
  {key:'NSI',      name:'NSI',                type:'cnt',   hb:true},
  {key:'APV_LC',   name:'APV LC',             type:'money', hb:true},
  {key:'NASP_LC',  name:'NASP LC',            type:'money', hb:true},
  {key:'FR_Items', name:'Fill Rate Items',    type:'pct',   hb:true,  color:'#10b981'},
  {key:'NSI_Cart', name:'NSI / Cart',         type:'ratio', hb:true},
  {key:'Ord_Comp', name:'Órdenes / Compra',   type:'ratio', hb:true},
  {key:'Visitas',  name:'Visitas',            type:'cnt',   hb:true,  color:'#f59e0b'},
  {key:'CVR',      name:'CVR',                type:'pct',   hb:true,  color:'#a855f7'},
  {key:'Stores',   name:'Tiendas activas',    type:'int',   hb:true,  color:'#64748b'},
];

function renderGrowth(periods, labelMap, dataMap, storeDataMap) {
  let h = '';
  const lastP = periods[periods.length-1];
  const rec   = dataMap[lastP] || {};
  const lbl   = (labelMap && labelMap[lastP]) || lastP;
  h += `<p style="font-size:11px;color:#94a3b8;margin-bottom:10px">Último período: <strong>${lbl}</strong></p>`;

  h += '<div class="kpi-row">';
  [{key:'NMV',label:'NMV',type:'nmv'},{key:'Compras',label:'Compras',type:'cnt'},
   {key:'Buyers',label:'Buyers',type:'cnt'},{key:'CVR',label:'CVR',type:'pct'},
   {key:'FR_Items',label:'Fill Rate',type:'pct'}].forEach(k => {
    h += `<div class="kpi-card"><div class="kpi-label">${k.label}</div><div class="kpi-value">${fmt(rec[k.key],k.type)}</div></div>`;
  });
  h += '</div>';

  h += buildTable(periods, labelMap, GROWTH_ROWS, dataMap);

  h += `<div class="store-section">
    <div class="store-section-header" onclick="toggleStore('store-body-growth')">
      <h3>Apertura por tienda</h3>
      <span class="store-toggle-icon" id="store-ico-growth">▼</span>
    </div>
    <div class="store-section-body" id="store-body-growth">`;

  const storeRowDefs = [
    {key:'NMV',     name:'NMV',     type:'nmv', hb:true},
    {key:'Compras', name:'Compras', type:'cnt', hb:true},
    {key:'Ordenes', name:'Órdenes', type:'cnt', hb:true},
    {key:'NSI',     name:'NSI',     type:'cnt', hb:true},
  ];
  for (const store of D.stores) {
    const color = D.store_colors[store] || '#667eea';
    h += `<div style="padding:10px 14px 0"><span class="store-pill" style="background:${color}">${store}</span></div>`;
    const sDataMap = {};
    for (const p of periods) {
      sDataMap[p] = (storeDataMap[p] && storeDataMap[p][store]) ? storeDataMap[p][store] : {};
    }
    h += buildTable(periods, labelMap, storeRowDefs, sDataMap, {showSparkline: true});
  }
  h += '</div></div>';
  return h;
}

// ── OPS TAB ───────────────────────────────────────────────────────────────────

const OPS_ROWS_FR = [
  {key:'FR_Items',        name:'Fill Rate Items',               type:'pct', hb:true},
  {key:'FR_Items_Remp',   name:'Fill Rate Items c/Reemplazo',   type:'pct', hb:true},
  {key:'FR_Compras',      name:'Fill Rate Compras',             type:'pct', hb:true},
  {key:'FR_Compras_Remp', name:'Fill Rate Compras c/Reemplazo', type:'pct', hb:true},
];
const OPS_ROWS_ENTREGA = [
  {key:'Compra_Perfecta', name:'Compra Perfecta',              type:'pct', hb:true},
  {key:'On_Time',         name:'On Time',                      type:'pct', hb:true},
];
const OPS_ROWS_CANCEL = [
  {key:'Cancel_Rate',     name:'Cancelaciones Total',          type:'pct', hb:false},
  {key:'Cancel_Buyer',    name:'├ Buyer initiated',            type:'pct', hb:false},
  {key:'Cancel_Stockout', name:'├ Stockout',                   type:'pct', hb:false},
  {key:'Cancel_Seller',   name:'└ Seller',                     type:'pct', hb:false},
];
const OPS_ROWS = [...OPS_ROWS_FR, ...OPS_ROWS_ENTREGA, ...OPS_ROWS_CANCEL];

function renderOps(periods, labelMap, dataMap) {
  const isMensual = VIEW === 'mensual';

  let h = '<div class="section-title">Fill Rate</div>';
  h += buildTable(periods, labelMap, OPS_ROWS_FR, dataMap);

  h += '<div class="section-title" style="margin-top:16px">Compra Perfecta & On Time</div>';
  h += buildTable(periods, labelMap, OPS_ROWS_ENTREGA, dataMap);

  h += '<div class="section-title" style="margin-top:16px">Cancelaciones</div>';
  h += buildTable(periods, labelMap, OPS_ROWS_CANCEL, dataMap);

  // Reclamos CX — solo mensual (viene de monthly_cx.csv)
  if (isMensual && D.mcx && Object.keys(D.mcx).length > 0) {
    const availCX = periods.filter(p => D.mcx[p]);
    if (availCX.length) {
      const CX_ROWS = [
        {key:'Perfect_Purchase_Rate', name:'Compra Perfecta CX (%)', type:'pct', hb:true},
        {key:'Claims_Rate',           name:'Tasa de Reclamos',       type:'pct', hb:false},
        {key:'Total_Claims',          name:'Reclamos Totales',       type:'cnt', hb:false},
        {key:'Repentance',            name:'├ Arrepentimiento',      type:'cnt', hb:false},
        {key:'Incomplete',            name:'├ Incompleto',           type:'cnt', hb:false},
        {key:'Different',             name:'├ Diferente al pedido',  type:'cnt', hb:false},
        {key:'Defective',             name:'└ Defectuoso',           type:'cnt', hb:false},
      ];
      h += '<div class="section-title" style="margin-top:16px">Reclamos (CX)</div>';
      h += buildTable(availCX, labelMap, CX_ROWS, D.mcx);
    }
  }

  // NPS solo en mensual
  if (isMensual) h += renderNPS();

  return h;
}

// ── BUYERS TAB ────────────────────────────────────────────────────────────────

function renderBuyers(periods, labelMap, buyersByStore, growthMap) {
  let h = '';
  const totalDataMap = {};
  for (const p of periods) {
    const stores = buyersByStore[p] || {};
    const total  = Object.values(stores).reduce((s, v) => s + (v||0), 0);
    totalDataMap[p] = { Total: total || null };
    for (const store of D.stores) totalDataMap[p][store] = stores[store] || null;
  }
  const buyerRows = [
    {key:'Total', name:'Total Buyers', type:'cnt', hb:true},
    ...D.stores.map(s => ({key:s, name:s, type:'cnt', hb:true, color:D.store_colors[s]}))
  ];
  h += buildTable(periods, labelMap, buyerRows, totalDataMap);

  const cvrRows = [
    {key:'Visitas', name:'Visitas',          type:'cnt', hb:true, color:'#f59e0b'},
    {key:'Buyers',  name:'Buyers (CVR src)', type:'cnt', hb:true, color:'#a855f7'},
    {key:'CVR',     name:'CVR',              type:'pct', hb:true, color:'#a855f7'},
  ];
  h += '<div class="section-title" style="margin-top:16px">Tráfico &amp; Conversión</div>';
  h += buildTable(periods, labelMap, cvrRows, growthMap);
  return h;
}

// ── GRÁFICOS TAB ──────────────────────────────────────────────────────────────

let chartInstances = [];

function renderGraficos(periods, labelMap, growthMap, opsMap) {
  chartInstances.forEach(c => c.destroy());
  chartInstances = [];

  const labels   = periods.map(p => (labelMap && labelMap[p]) || p);
  const nmvVals  = periods.map(p => { const r=growthMap[p]; return r&&r.NMV?r.NMV/1e6:null; });
  const cmpVals  = periods.map(p => { const r=growthMap[p]; return r?r.Compras:null; });
  const frVals   = periods.map(p => { const r=opsMap[p]; return r&&r.FR_Items?r.FR_Items*100:null; });
  const visVals  = periods.map(p => { const r=growthMap[p]; return r?r.Visitas:null; });

  let h = '<div class="charts-grid">';
  h += '<div class="chart-card"><h3>NMV (ARS M)</h3><canvas id="ch-nmv"></canvas></div>';
  h += '<div class="chart-card"><h3>Compras</h3><canvas id="ch-cmp"></canvas></div>';
  h += '<div class="chart-card"><h3>Fill Rate Items (%)</h3><canvas id="ch-fr"></canvas></div>';
  h += '<div class="chart-card"><h3>Visitas</h3><canvas id="ch-vis"></canvas></div>';
  h += '</div>';

  setTimeout(() => {
    const common = {
      responsive:true, maintainAspectRatio:true,
      plugins:{legend:{display:false}},
      scales:{x:{ticks:{font:{size:9},maxRotation:60}}, y:{ticks:{font:{size:9}}}}
    };
    const lineDs = (data, color) => [{data, borderColor:color, backgroundColor:color+'1a', fill:true, tension:0.35, pointRadius:2, borderWidth:2.5}];
    const barDs  = (data, color) => [{data, backgroundColor:color+'dd', borderRadius:4, borderSkipped:false}];
    const ch1 = new Chart(document.getElementById('ch-nmv'), {type:'line', data:{labels, datasets:lineDs(nmvVals,'#1d4ed8')}, options:common});
    const ch2 = new Chart(document.getElementById('ch-cmp'), {type:'bar',  data:{labels, datasets:barDs(cmpVals,'#3b82f6')},  options:common});
    const ch3 = new Chart(document.getElementById('ch-fr'),  {type:'line', data:{labels, datasets:lineDs(frVals,'#059669')},  options:{...common, scales:{...common.scales, y:{...common.scales.y, min:70, max:100}}}});
    const ch4 = new Chart(document.getElementById('ch-vis'), {type:'bar',  data:{labels, datasets:barDs(visVals,'#f59e0b')},  options:common});
    chartInstances = [ch1, ch2, ch3, ch4];
  }, 50);
  return h;
}

// ── P&L TAB ───────────────────────────────────────────────────────────────────

function renderPL(periods, labelMap) {
  if (!D.mpl || Object.keys(D.mpl).length === 0) {
    return renderPlaceholder('P&L',
      'Datos pendientes — ejecutar <code>fetch_pl.py</code> para cargar métricas de monetización desde BT_UE_OUTPUT_MNG.');
  }

  const availPeriods = periods.filter(p => D.mpl[p]);
  if (availPeriods.length === 0) {
    return renderPlaceholder('P&L', 'Sin datos de P&L para el período mostrado.');
  }

  // KPI cards — último mes disponible
  const lastP = availPeriods[availPeriods.length-1];
  const rec   = D.mpl[lastP] || {};
  const lbl   = (labelMap && labelMap[lastP]) || lastP;

  let h = `<p style="font-size:11px;color:#94a3b8;margin-bottom:10px">Último período con datos P&L: <strong>${lbl}</strong></p>`;

  h += '<div class="kpi-row">';
  [
    {key:'TGMV',               label:'TGMV',       type:'nmv'},
    {key:'NMV',                label:'NMV',         type:'nmv'},
    {key:'Net_Monetization',   label:'Net Monet.',  type:'nmv'},
    {key:'Variable_Contribution', label:'Contrib. Var.', type:'nmv'},
    {key:'Take_Rate',          label:'Take Rate',   type:'pct'},
  ].forEach(k => {
    const v = rec[k.key];
    h += `<div class="kpi-card"><div class="kpi-label">${k.label}</div><div class="kpi-value">${fmt(v,k.type)}</div></div>`;
  });
  h += '</div>';

  const PL_ROWS = [
    {separator:true, name:'Volumen'},
    {key:'TGMV',                    name:'TGMV (GMV Total)',               type:'nmv',  hb:true},
    {key:'NMV',                     name:'NMV (Net Merch. Value)',          type:'nmv',  hb:true},
    {separator:true, name:'Monetización'},
    {key:'Net_Variable_Fee',        name:'Net Variable Fee',                type:'nmv',  hb:true},
    {key:'Net_Monetization',        name:'Net Monetization',                type:'nmv',  hb:true},
    {key:'Product_Net_Monetization',name:'Product Net Monetization',        type:'nmv',  hb:true},
    {key:'Take_Rate',               name:'Take Rate (Net Monet./NMV)',      type:'pct',  hb:true, extraClass:'pl-ratio'},
    {separator:true, name:'Costos'},
    {key:'Shipping_Cost',           name:'Costo de Distribución/Envío',     type:'nmv',  hb:false},
    {key:'Promotions',              name:'Promociones',                     type:'nmv',  hb:false},
    {key:'Coupons',                 name:'Cupones',                         type:'nmv',  hb:false},
    {separator:true, name:'Contribuciones'},
    {key:'Variable_Contribution',   name:'Contribución Variable',           type:'nmv',  hb:true},
    {key:'VC_Over_NMV',             name:'Contrib. Variable / NMV',         type:'pct',  hb:true, extraClass:'pl-ratio'},
    {key:'Direct_Contribution',     name:'Contribución Directa',            type:'nmv',  hb:true},
    {key:'DC_Over_NMV',             name:'Contrib. Directa / NMV',          type:'pct',  hb:true, extraClass:'pl-ratio'},
  ];
  h += buildTable(availPeriods, labelMap, PL_ROWS, D.mpl);
  return h;
}

// ── CX TAB ────────────────────────────────────────────────────────────────────

function renderCX(periods, labelMap, opsMap) {
  const hasCX = D.mcx && Object.keys(D.mcx).length > 0;
  const availCX = hasCX ? periods.filter(p => D.mcx[p]) : [];

  const lastP   = periods[periods.length-1];
  const opsRec  = opsMap[lastP] || {};
  const cxRec   = hasCX && D.mcx[lastP] ? D.mcx[lastP] : {};
  const lbl     = (labelMap && labelMap[lastP]) || lastP;

  let h = `<p style="font-size:11px;color:#94a3b8;margin-bottom:10px">Último período: <strong>${lbl}</strong></p>`;

  // KPI cards
  h += '<div class="kpi-row">';
  const ppr = cxRec.Perfect_Purchase_Rate;
  h += `<div class="kpi-card">
    <div class="kpi-label">Compra Perfecta</div>
    <div class="kpi-value" style="color:${ppr!=null&&ppr>=0.85?'#16a34a':'#dc2626'}">${fmtPct(ppr)}</div>
  </div>`;
  const clr = cxRec.Claims_Rate;
  h += `<div class="kpi-card">
    <div class="kpi-label">Tasa Reclamos</div>
    <div class="kpi-value" style="color:${clr!=null&&clr<=0.03?'#16a34a':'#dc2626'}">${fmtPct(clr)}</div>
  </div>`;
  const cancel = opsRec.Cancel_Rate;
  h += `<div class="kpi-card">
    <div class="kpi-label">Cancelaciones</div>
    <div class="kpi-value" style="color:${cancel!=null&&cancel<=0.10?'#16a34a':'#dc2626'}">${fmtPct(cancel)}</div>
  </div>`;
  const ot = opsRec.On_Time;
  h += `<div class="kpi-card">
    <div class="kpi-label">On Time</div>
    <div class="kpi-value" style="color:${ot!=null&&ot>=0.90?'#16a34a':'#f59e0b'}">${fmtPct(ot)}</div>
  </div>`;
  h += '</div>';

  if (!hasCX) {
    h += renderPlaceholder('Reclamos & Compra Perfecta',
      'Datos pendientes — ejecutar <code>fetch_cx.py</code> para cargar métricas de CX.');
  } else {
    // Compra Perfecta & Claims
    const CX_ROWS = [
      {key:'Perfect_Purchase_Rate', name:'Compra Perfecta (%)',    type:'pct', hb:true,  color:'#10b981'},
      {key:'Perfect_Purchases',     name:'Compras Perfectas (#)',   type:'cnt', hb:true},
      {key:'Total_Purchases',       name:'Compras Totales (#)',     type:'cnt', hb:true},
      {key:'Claims_Rate',           name:'Tasa de Reclamos (%)',    type:'pct', hb:false, color:'#ef4444'},
      {key:'Total_Claims',          name:'Reclamos Totales',        type:'cnt', hb:false},
      {key:'Repentance',            name:'├ Arrepentimiento',       type:'cnt', hb:false},
      {key:'Incomplete',            name:'├ Incompleto',            type:'cnt', hb:false},
      {key:'Different',             name:'├ Diferente al pedido',   type:'cnt', hb:false},
      {key:'Defective',             name:'└ Defectuoso',            type:'cnt', hb:false},
    ];
    h += '<div class="section-title">Compra Perfecta & Reclamos</div>';
    h += buildTable(availCX, labelMap, CX_ROWS, D.mcx);

    // By-store breakdown if available
    if (D.mcx_store && Object.keys(D.mcx_store).length > 0) {
      h += `<div class="store-section" style="margin-top:16px">
        <div class="store-section-header" onclick="toggleStore('store-body-cx')">
          <h3>Apertura por tienda</h3>
          <span class="store-toggle-icon" id="store-ico-cx">▼</span>
        </div>
        <div class="store-section-body" id="store-body-cx">`;

      const cxStoreRows = [
        {key:'Perfect_Purchase_Rate', name:'Compra Perfecta (%)', type:'pct', hb:true},
        {key:'Claims_Rate',           name:'Tasa Reclamos (%)',   type:'pct', hb:false},
        {key:'Total_Claims',          name:'Reclamos Totales',    type:'cnt', hb:false},
        {key:'On_Time',               name:'On Time',             type:'pct', hb:true},
      ];
      for (const store of D.stores) {
        const color = D.store_colors[store] || '#667eea';
        h += `<div style="padding:10px 14px 0"><span class="store-pill" style="background:${color}">${store}</span></div>`;
        const sMap = {};
        for (const p of availCX) {
          sMap[p] = (D.mcx_store[p] && D.mcx_store[p][store]) ? D.mcx_store[p][store] : {};
        }
        h += buildTable(availCX, labelMap, cxStoreRows, sMap, {showSparkline: true});
      }
      h += '</div></div>';
    }
  }

  // Ops (cancel + on time) en la misma pantalla para contexto
  h += '<div class="section-title" style="margin-top:20px">Operaciones (cancelaciones & on time)</div>';
  const OPS_CX_ROWS = [
    {key:'Cancel_Rate', name:'Cancelaciones', type:'pct', hb:false, color:'#ef4444'},
    {key:'On_Time',     name:'On Time',       type:'pct', hb:true,  color:'#f59e0b'},
  ];
  h += buildTable(periods, labelMap, OPS_CX_ROWS, opsMap);

  return h;
}

// ── ROLLING 7D ────────────────────────────────────────────────────────────────

const ROLLING_ROWS = [
  {key:'NMV',      name:'NMV',            type:'nmv',   hb:true},
  {key:'Compras',  name:'Compras',        type:'cnt',   hb:true},
  {key:'Ordenes',  name:'Órdenes',        type:'cnt',   hb:true},
  {key:'APV_LC',   name:'APV LC',         type:'money', hb:true},
  {key:'NSI',      name:'NSI',            type:'cnt',   hb:true},
  {key:'FR_Items', name:'Fill Rate Items',type:'pct',   hb:true, color:'#10b981'},
  {key:'Visitas',  name:'Visitas',        type:'cnt',   hb:true, color:'#f59e0b'},
  {key:'CVR',      name:'CVR',            type:'pct',   hb:true, color:'#a855f7'},
];

function renderRolling() {
  if (!D.rolling || D.rolling.length === 0) {
    return renderPlaceholder('Fechas Móviles 7D',
      'Requiere daily_growth_full.csv y daily_visits_full.csv.');
  }
  const rMap = {}, periods = D.rolling.map(p => p.label), labelMap = {};
  for (const p of D.rolling) {
    rMap[p.label] = p;
    labelMap[p.label] = `${p.label}<br><small style="font-weight:normal;font-size:9px">${p.start.slice(5)}</small>`;
  }
  let h = '<p style="font-size:11px;color:#94a3b8;margin-bottom:10px">Períodos móviles de <strong>7 días</strong> — P = período más reciente</p>';
  h += buildTable(periods, labelMap, ROLLING_ROWS, rMap);
  return h;
}

// ── ROLLING 28D (mensual) ─────────────────────────────────────────────────────

function renderRolling28() {
  if (!D.rolling28 || D.rolling28.length === 0) {
    return renderPlaceholder('Fechas Móviles 28D',
      'Requiere daily_growth_full.csv con suficiente historia.');
  }
  const rMap = {}, periods = D.rolling28.map(p => p.label), labelMap = {};
  for (const p of D.rolling28) {
    rMap[p.label] = p;
    const startFmt = p.start.slice(5).replace('-','/');
    const endFmt   = p.end.slice(5).replace('-','/');
    labelMap[p.label] = `${p.label}<br><small style="font-weight:normal;font-size:9px">${startFmt}→${endFmt}</small>`;
  }
  let h = '<p style="font-size:11px;color:#94a3b8;margin-bottom:10px">Períodos móviles de <strong>28 días</strong> — P28 = período más reciente</p>';
  h += buildTable(periods, labelMap, ROLLING_ROWS, rMap);
  return h;
}

// ── ASSORTMENT ────────────────────────────────────────────────────────────────

function renderAssortment() {
  const A = D.assortment;
  if (!A || !A.stores) return renderPlaceholder('Assortment', 'Sin datos de assortment.');

  const months    = A.months;
  const storeKeys = Object.keys(A.stores);
  const lastMIdx  = months.length - 1;

  // KPI cards — último mes, promedio de tiendas
  const avgDisp = storeKeys.reduce((s,k)=>s+(A.stores[k].Disponibilidad[lastMIdx]||0),0)/storeKeys.length;
  const avgProf = storeKeys.reduce((s,k)=>s+(A.stores[k].Profundidad[lastMIdx]||0),0)/storeKeys.length;
  const avgGap  = storeKeys.reduce((s,k)=>s+(A.stores[k].Gap[lastMIdx]||0),0)/storeKeys.length;

  let h = '<div class="kpi-row">';
  h += '<div class="kpi-card"><div class="kpi-label">Disponibilidad Promedio (' + months[lastMIdx] + ')</div><div class="kpi-value" style="color:' + (avgDisp>=0.70?'#059669':'#dc2626') + '">' + (avgDisp*100).toFixed(1) + '%</div></div>';
  h += '<div class="kpi-card"><div class="kpi-label">Profundidad Promedio (' + months[lastMIdx] + ')</div><div class="kpi-value" style="color:' + (avgProf>=0.60?'#059669':'#f59e0b') + '">' + (avgProf*100).toFixed(1) + '%</div></div>';
  h += '<div class="kpi-card"><div class="kpi-label">Gap Promedio (p.p.)</div><div class="kpi-value" style="color:' + (avgGap<=10?'#059669':'#dc2626') + '">' + avgGap.toFixed(1) + '</div></div>';
  h += '</div>';

  h += '<p style="font-size:11px;color:#94a3b8;margin-bottom:10px">Disponibilidad = % SKUs activos sobre total catálogo &nbsp;|&nbsp; Profundidad = % de SKUs con stock suficiente &nbsp;|&nbsp; Gap = diferencia (p.p.)</p>';

  // Tabla por tienda
  h += '<div class="table-wrap"><table class="sc-table"><thead><tr><th>Tienda / Métrica</th>';
  for (const m of months) h += '<th>' + m + '</th>';
  h += '</tr></thead><tbody>';

  for (const store of storeKeys) {
    const color = D.store_colors[store] || '#94a3b8';
    const sd    = A.stores[store];
    h += '<tr class="pl-separator"><td colspan="' + (months.length+1) + '" style="padding:6px 12px"><span class="store-pill" style="background:' + color + '">' + store + '</span></td></tr>';

    const metrics = [
      {key:'Disponibilidad', lbl:'Disponibilidad', fmt: v => (v*100).toFixed(1)+'%', hb:true,  thresh:0.70},
      {key:'Profundidad',    lbl:'Profundidad',    fmt: v => (v*100).toFixed(1)+'%', hb:true,  thresh:0.60},
      {key:'Gap',            lbl:'Gap (p.p.)',     fmt: v => v.toFixed(1),           hb:false, thresh:10},
    ];
    for (const m of metrics) {
      const vals = sd[m.key] || [];
      h += '<tr><td style="padding-left:24px">' + m.lbl + '</td>';
      for (let i=0; i<months.length; i++) {
        const v   = vals[i];
        const isLast = i === months.length-1;
        let cls = '';
        if (isLast) {
          const good = m.hb ? v >= m.thresh : v <= m.thresh;
          cls = good ? 'good' : 'bad';
        }
        h += '<td class="' + (isLast?'last-col ':'') + cls + '">' + (v!=null?m.fmt(v):'—') + '</td>';
      }
      h += '</tr>';
    }
  }

  h += '</tbody></table></div>';
  return h;
}

// ── DEMOGRÁFICOS TAB ──────────────────────────────────────────────────────────

function renderDemo() {
  const demo = D.demo || {};
  if (!demo.nse_by_month && !demo.age_by_month && !demo.gender_by_month)
    return renderPlaceholder('Demográficos', 'Sin datos — verificar q1_clean.csv y q2_clean.csv.');

  chartInstances.forEach(c => c.destroy()); chartInstances = [];

  const months   = D.months || [];
  const labels   = months.map(mk => (D.mlabels[mk]||mk).replace(' MTD',''));
  const nseOrder = demo.nse_order || ['PLATINUM','GOLD','SILVER','BRONZE','Sin clasif.'];
  const ageOrder = demo.age_order || ['18-29','30-44','45-59','60+'];
  const NSE_C  = {PLATINUM:'#7c3aed',GOLD:'#f59e0b',SILVER:'#94a3b8',BRONZE:'#d97706','Sin clasif.':'#e2e8f0'};
  const AGE_C  = {'18-29':'#1d4ed8','30-44':'#059669','45-59':'#f59e0b','60+':'#a855f7'};
  const GEN_C  = {female:'#ec4899',male:'#3b82f6'};

  function pctStacked(byKey, order, keys) {
    return order.map(seg => ({
      label: seg,
      data: keys.map(k => {
        const row = byKey[k] || {};
        const total = Object.values(row).reduce((s,v) => s+(v||0), 0);
        return total > 0 ? Math.round((row[seg]||0) / total * 1000) / 10 : null;
      })
    }));
  }

  let h = '';
  if (demo.buyers_ytd_total) {
    h += '<div class="kpi-row"><div class="kpi-card"><div class="kpi-label">Buyers Únicos YTD 2026</div><div class="kpi-value">' + fmtCnt(demo.buyers_ytd_total) + '</div></div></div>';
  }

  h += '<div class="charts-grid">';
  if (demo.gender_by_month) h += '<div class="chart-card"><h3>Buyers por Género</h3><canvas id="dch-gen"></canvas></div>';
  if (demo.age_by_month)    h += '<div class="chart-card"><h3>Buyers por Rango Etario</h3><canvas id="dch-age"></canvas></div>';
  if (demo.nse_by_month)    h += '<div class="chart-card"><h3>NSE por Mes</h3><canvas id="dch-nse"></canvas></div>';
  if (demo.nse_by_store)    h += '<div class="chart-card"><h3>NSE por Tienda</h3><canvas id="dch-store"></canvas></div>';
  h += '</div>';

  setTimeout(() => {
    const stackOpts = {
      responsive:true, maintainAspectRatio:true,
      plugins:{legend:{position:'bottom',labels:{font:{size:9},boxWidth:10,padding:5}}},
      scales:{x:{stacked:true,ticks:{font:{size:9},maxRotation:45}},y:{stacked:true,min:0,max:100,ticks:{font:{size:9},callback:v=>v+'%'}}}
    };
    function mkChart(id, xLabels, dsData, colorMap) {
      const el = document.getElementById(id); if (!el) return;
      chartInstances.push(new Chart(el, {
        type:'bar',
        data:{labels:xLabels, datasets: dsData.map(d => ({label:d.label, data:d.data, backgroundColor:colorMap[d.label]||'#94a3b8', borderWidth:0}))},
        options: stackOpts
      }));
    }
    if (demo.gender_by_month) mkChart('dch-gen',   labels, pctStacked(demo.gender_by_month,['female','male'],months), GEN_C);
    if (demo.age_by_month)    mkChart('dch-age',   labels, pctStacked(demo.age_by_month, ageOrder, months), AGE_C);
    if (demo.nse_by_month)    mkChart('dch-nse',   labels, pctStacked(demo.nse_by_month, nseOrder, months), NSE_C);
    if (demo.nse_by_store) {
      const storeData = nseOrder.map(seg => ({
        label: seg,
        data: D.stores.map(s => {
          const row = (demo.nse_by_store[s]||{});
          const tot = Object.values(row).reduce((a,v) => a+(v||0), 0);
          return tot > 0 ? Math.round((row[seg]||0)/tot*1000)/10 : null;
        })
      }));
      mkChart('dch-store', D.stores, storeData, NSE_C);
    }
  }, 50);
  return h;
}

// ── PLAN TAB ──────────────────────────────────────────────────────────────────

function planRow(label, vals, totFn, style) {
  let t = 0, hasAny = false;
  let h = '<tr><td style="' + (style||'') + '">' + label + '</td>';
  for (const v of vals) {
    if (v != null) { t += v; hasAny = true; }
    const cellStyle = style || (v == null ? 'color:#94a3b8' : '');
    h += '<td style="' + cellStyle + '">' + (v != null ? totFn(v) : '—') + '</td>';
  }
  h += '<td style="font-weight:700">' + (hasAny ? totFn(t) : '—') + '</td></tr>';
  return h;
}

function renderPlan() {
  const isSemanal = VIEW === 'semanal';
  const isDiario  = VIEW === 'diario';
  const planMap   = isSemanal ? PW : (isDiario ? PD : PM);
  const growthMap = isSemanal ? D.wg : (isDiario ? D.dg : D.mg);
  const allPeriods= isSemanal ? D.weeks : (isDiario ? D.daily_dates : D.months);
  const labelMap  = isSemanal ? D.wlabels : (isDiario ? D.dlabels : D.mlabels);

  // Períodos que tienen datos de plan o real, desde 2026
  const periods = allPeriods.filter(p => p >= '2026-01-01' && (planMap[p] || growthMap[p]));
  if (!periods.length) return renderPlaceholder('Plan vs Real', 'Sin períodos con datos de plan 2026.');

  // KPI acumulado
  let sumReal=0, sumV2=0, sum48=0;
  for (const p of periods) {
    const rv  = (growthMap[p]||{}).NMV;
    const pv2 = (planMap[p]||{}).NMV_V2;
    const p48 = (planMap[p]||{}).NMV_4p8;
    if (rv)  sumReal += rv;
    if (pv2) sumV2   += pv2;
    if (p48) sum48   += p48;
  }

  let h = '<div class="kpi-row">';
  h += '<div class="kpi-card"><div class="kpi-label">NMV Real (acum.)</div><div class="kpi-value">' + fmtNMV(sumReal||null) + '</div></div>';
  h += '<div class="kpi-card" style="border-top-color:#64748b"><div class="kpi-label">Plan V2 (acum.)</div><div class="kpi-value" style="color:#64748b">' + fmtNMV(sumV2||null) + '</div></div>';
  h += '<div class="kpi-card" style="border-top-color:#94a3b8"><div class="kpi-label">Forecast 4+8 (acum.)</div><div class="kpi-value" style="color:#94a3b8">' + fmtNMV(sum48||null) + '</div></div>';
  if (sumV2 > 0 && sumReal > 0) {
    const pv2 = (sumReal-sumV2)/sumV2;
    h += '<div class="kpi-card" style="border-top-color:' + (pv2>=0?'#059669':'#dc2626') + '"><div class="kpi-label">vs Plan V2</div><div class="kpi-value" style="color:' + (pv2>=0?'#059669':'#dc2626') + '">' + (pv2>=0?'+':'') + (pv2*100).toFixed(1) + '%</div></div>';
  }
  if (sum48 > 0 && sumReal > 0) {
    const p48 = (sumReal-sum48)/sum48;
    h += '<div class="kpi-card" style="border-top-color:' + (p48>=0?'#059669':'#dc2626') + '"><div class="kpi-label">vs Fcst 4+8</div><div class="kpi-value" style="color:' + (p48>=0?'#059669':'#dc2626') + '">' + (p48>=0?'+':'') + (p48*100).toFixed(1) + '%</div></div>';
  }
  h += '</div>';

  const slicedPeriods = isSemanal ? periods : periods.slice(-12);
  h += '<div class="table-wrap"><table class="sc-table' + (isDiario?' compact':'') + '"><thead><tr><th>Métrica</th>';
  for (const p of slicedPeriods) h += '<th>' + ((labelMap&&labelMap[p])||p) + '</th>';
  h += '<th>TOTAL</th></tr></thead><tbody>';

  const v2Vals  = slicedPeriods.map(p => (planMap[p]||{}).NMV_V2  || null);
  const f48Vals = slicedPeriods.map(p => (planMap[p]||{}).NMV_4p8 || null);
  const realVals= slicedPeriods.map(p => (growthMap[p]||{}).NMV   || null);

  h += planRow('Plan V2 (target)', v2Vals,  fmtNMV, 'font-style:italic;color:#64748b');
  h += planRow('Forecast 4+8',     f48Vals, fmtNMV, 'font-style:italic;color:#94a3b8');
  h += planRow('NMV Real',         realVals,fmtNMV, 'font-weight:700');

  h += '<tr><td style="color:#1d4ed8;font-weight:600">% vs Plan V2</td>';
  for (const p of slicedPeriods) {
    const pv=(planMap[p]||{}).NMV_V2, rv=(growthMap[p]||{}).NMV;
    if (pv && rv) { const d=(rv-pv)/pv; h += '<td class="' + (d>=0?'good':'bad') + '">' + (d>=0?'+':'') + (d*100).toFixed(1) + '%</td>'; }
    else h += '<td>—</td>';
  }
  { const d = sumV2>0&&sumReal>0?(sumReal-sumV2)/sumV2:null;
    h += '<td class="' + (d!=null?(d>=0?'good':'bad'):'') + '" style="font-weight:700">' + (d!=null?((d>=0?'+':'')+(d*100).toFixed(1)+'%'):'\u2014') + '</td>'; }
  h += '</tr>';

  h += '<tr><td style="color:#64748b;font-weight:600">% vs Fcst 4+8</td>';
  for (const p of slicedPeriods) {
    const pv=(planMap[p]||{}).NMV_4p8, rv=(growthMap[p]||{}).NMV;
    if (pv && rv) { const d=(rv-pv)/pv; h += '<td class="' + (d>=0?'good':'bad') + '">' + (d>=0?'+':'') + (d*100).toFixed(1) + '%</td>'; }
    else h += '<td>—</td>';
  }
  { const d = sum48>0&&sumReal>0?(sumReal-sum48)/sum48:null;
    h += '<td class="' + (d!=null?(d>=0?'good':'bad'):'') + '" style="font-weight:700">' + (d!=null?((d>=0?'+':'')+(d*100).toFixed(1)+'%'):'\u2014') + '</td>'; }
  h += '</tr>';

  if (isSemanal || isDiario) {
    const nsiV2   = slicedPeriods.map(p => (planMap[p]||{}).NSI_V2  || null);
    const nsi48   = slicedPeriods.map(p => (planMap[p]||{}).NSI_4p8 || null);
    const nsiReal = slicedPeriods.map(p => (growthMap[p]||{}).NSI   || null);
    h += planRow('NSI Plan V2',  nsiV2,  fmtCnt, 'font-style:italic;color:#64748b');
    h += planRow('NSI Fcst 4+8', nsi48,  fmtCnt, 'font-style:italic;color:#94a3b8');
    h += planRow('NSI Real',     nsiReal,fmtCnt, 'font-weight:600');
  }
  h += '</tbody></table></div>';
  return h;
}

const NPS_HARDCODED = {
  months: ["Dic'25","Ene'26","Feb'26","Mar'26","Abr'26"],
  'Total':         [26, 39, 20, 15, 29],
  'Scalabrini':    [14, 48, 25,  9, 31],
  'Caballito':     [16, 45, 20,  6, 20],
  'Vicente Lopez': [29, 19, -5, 27, 34],
  'Villa Urquiza': [27, 44, 37, 17, 20],
};
function npsColor(v){
  if(v==null)return '#94a3b8';
  if(v>=50)return '#15803d'; if(v>=30)return '#16a34a'; if(v>=0)return '#f59e0b'; return '#dc2626';
}
function renderNPS(){
  const hasDynamic=D.nps_data&&Object.keys(D.nps_data).length>0;
  if(hasDynamic)return renderNPSDynamic();
  const nd=NPS_HARDCODED,months=nd.months,storeKeys=['Total',...D.stores],lastIdx=months.length-1;
  let h='<div class="section-title" style="margin-top:20px">NPS por tienda</div>';
  h+='<p style="font-size:11px;color:#94a3b8;margin-bottom:10px">Datos hardcoded — actualización mensual manual</p>';
  h+='<div class="kpi-row">';
  for(const s of storeKeys){
    const v=nd[s]?nd[s][lastIdx]:null;
    h+='<div class="kpi-card"><div class="kpi-label">'+s+'</div><div class="kpi-value" style="color:'+npsColor(v)+'">'+(v!=null?v:'—')+'</div></div>';
  }
  h+='</div>';
  h+='<div class="table-wrap"><table class="sc-table"><thead><tr><th style="text-align:left">Tienda</th>';
  for(const m of months)h+='<th>'+m+'</th>';
  h+='</tr></thead><tbody>';
  for(const s of storeKeys){
    h+='<tr><td style="'+(s==='Total'?'font-weight:700':'font-weight:500')+';text-align:left;padding-left:12px">'+s+'</td>';
    const vals=nd[s]||[];
    for(let i=0;i<months.length;i++){
      const v=vals[i]!=null?vals[i]:null,bg=v!=null?(v>=50?'#dcfce7':v>=30?'#d1fae5':v>=0?'#fef3c7':'#fee2e2'):'';
      h+='<td style="background:'+bg+';color:'+npsColor(v)+';font-weight:600">'+(v!=null?v:'—')+'</td>';
    }
    h+='</tr>';
  }
  h+='</tbody></table></div>'; return h;
}
function renderNPSDynamic(){
  const nd=D.nps_data,mkList=Object.keys(nd).sort();
  const ME=['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];
  const ml=mk=>{const d=new Date(mk+'T00:00:00');return ME[d.getMonth()]+"'"+String(d.getFullYear()).slice(2);};
  const storeKeys=['Total',...D.stores],lastMk=mkList[mkList.length-1];
  let h='<div class="section-title" style="margin-top:20px">NPS por tienda</div>';
  h+='<p style="font-size:11px;color:#94a3b8;margin-bottom:10px">Datos desde BigQuery — último período: <strong>'+ml(lastMk)+'</strong></p>';
  h+='<div class="kpi-row">';
  for(const s of storeKeys){const key=s==='Total'?'':s,v=nd[lastMk]?nd[lastMk][key]:null;h+='<div class="kpi-card"><div class="kpi-label">'+s+'</div><div class="kpi-value" style="color:'+npsColor(v)+'">'+(v!=null?v:'—')+'</div></div>';}
  h+='</div>';
  h+='<div class="table-wrap"><table class="sc-table"><thead><tr><th style="text-align:left">Tienda</th>';
  for(const mk of mkList)h+='<th>'+ml(mk)+'</th>';
  h+='</tr></thead><tbody>';
  for(const s of storeKeys){const key=s==='Total'?'':s;h+='<tr><td style="'+(s==='Total'?'font-weight:700':'font-weight:500')+';text-align:left;padding-left:12px">'+s+'</td>';for(const mk of mkList){const v=nd[mk]?nd[mk][key]:null,bg=v!=null?(v>=50?'#dcfce7':v>=30?'#d1fae5':v>=0?'#fef3c7':'#fee2e2'):'';h+='<td style="background:'+bg+';color:'+npsColor(v)+';font-weight:600">'+(v!=null?v:'—')+'</td>';}h+='</tr>';}
  h+='</tbody></table></div>'; return h;
}
function renderDailyGrowth(){if(!D.daily_dates||!D.daily_dates.length)return renderPlaceholder('Growth Diario','Sin datos.');return renderGrowth(D.daily_dates,D.dlabels||{},D.dg||{},{});}
function renderDailyOps(){if(!D.daily_dates||!D.daily_dates.length)return renderPlaceholder('Ops Diario','Sin datos.');return renderOps(D.daily_dates,D.dlabels||{},D.do||{});}
function renderDailyBuyers(){if(!D.daily_dates||!D.daily_dates.length)return renderPlaceholder('Buyers Diario','Sin datos.');return renderBuyers(D.daily_dates,D.dlabels||{},D.dcvr_day||{},D.dg||{});}
function renderPlaceholder(t,s){return '<div class="placeholder"><h3 style="color:#475569">'+t+'</h3><p style="margin-top:6px;color:#94a3b8;font-size:12px">'+(s||'')+'</p></div>';}
function toggleStore(id){const el=document.getElementById(id),ico=document.getElementById(id.replace('store-body-','store-ico-'));if(!el)return;const open=el.style.display!=='none';el.style.display=open?'none':'';if(ico)ico.textContent=open?'▶':'▼';}
function setView(view){VIEW=view;['semanal','mensual','diario'].forEach(v=>{const b=document.getElementById('btn-'+v);if(b)b.classList.toggle('active',v===view);});const tabs=view==='semanal'?TABS_SEMANAL:view==='mensual'?TABS_MENSUAL:TABS_DIARIO;if(!tabs.find(t=>t.id===CUR_TAB))CUR_TAB=tabs[0].id;renderAll();}
function setTab(id){CUR_TAB=id;renderAll();}
function renderAll(){
  const isSemanal=VIEW==='semanal',isMensual=VIEW==='mensual',isDiario=VIEW==='diario';
  const tabs=isSemanal?TABS_SEMANAL:isMensual?TABS_MENSUAL:TABS_DIARIO;
  const tb=document.getElementById('tabs-bar');
  if(tb)tb.innerHTML=tabs.map(t=>'<button class="tab-btn'+(t.id===CUR_TAB?' active':'')+"\" onclick=\"setTab('"+ t.id +"')\">"+t.label+'</button>').join('');
  let periods,labelMap,growthMap,opsMap,buyersMap,storeMap;
  if(isSemanal){const allW=D.weeks||[];periods=allW.length>14?allW.slice(-14):allW;labelMap=D.wlabels||{};growthMap=D.wg||{};opsMap=D.wo||{};buyersMap=D.wb||{};storeMap=D.ws||{};}
  else if(isMensual){periods=D.months||[];labelMap=D.mlabels||{};growthMap=D.mg||{};opsMap=D.mo||{};buyersMap=D.mb||{};storeMap=D.ms||{};}
  else{periods=D.daily_dates||[];labelMap=D.dlabels||{};growthMap=D.dg||{};opsMap=D.do||{};buyersMap={};storeMap={};}
  const el=document.getElementById('content');if(!el)return;
  let html="";
  if(isDiario){switch(CUR_TAB){case 'growth':html=renderDailyGrowth();break;case 'ops':html=renderDailyOps();break;case 'buyers':html=renderDailyBuyers();break;case 'graficos':html=renderGraficos(periods,labelMap,growthMap,opsMap);break;case 'plan':html=renderPlan();break;default:html=renderPlaceholder(CUR_TAB,'');break;}}
  else{switch(CUR_TAB){case 'growth':html=renderGrowth(periods,labelMap,growthMap,storeMap);break;case 'ops':html=renderOps(periods,labelMap,opsMap);break;case 'buyers':html=renderBuyers(periods,labelMap,buyersMap,growthMap);break;case 'graficos':html=renderGraficos(periods,labelMap,growthMap,opsMap);break;case 'cx':html=renderCX(periods,labelMap,opsMap);break;case 'nps':html=renderNPS();break;case 'pl':html=renderPL(periods,labelMap);break;case 'assortment':html=renderAssortment();break;case 'demo':html=renderDemo();break;case 'plan':html=renderPlan();break;case 'rolling':html=isSemanal?renderRolling():renderRolling28();break;default:html=renderPlaceholder(CUR_TAB,'');break;}}
  el.innerHTML=html;
}
document.getElementById('updated-label').textContent='Actualizado: '+D.generated;
setView('semanal');
</script>
</body>
</html>
"""

with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    f.write(HTML)

print(f'Dashboard escrito \u2192 {OUTPUT_FILE}')
