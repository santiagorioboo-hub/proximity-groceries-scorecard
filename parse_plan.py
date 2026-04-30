#!/usr/bin/env python3
"""parse_plan.py — extrae plan_monthly.csv, plan_daily.csv y plan_weekly.csv de plan_raw.csv"""
import csv, sys, os
from datetime import date, timedelta
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DATA_DIR  = os.path.dirname(os.path.abspath(__file__))
MONTH_MAP = {'ene':1,'feb':2,'mar':3,'abr':4,'may':5,'jun':6,'jul':7,'ago':8,'sep':9,'oct':10,'nov':11,'dic':12}

def parse_num(s):
    if not s or not s.strip(): return None
    s = s.strip().rstrip('\r\n').replace('.','').replace(',','.')
    # si queda como porcentaje, ignorar
    if s.endswith('%'): return None
    try: return float(s)
    except: return None

with open(os.path.join(DATA_DIR, 'plan_raw.csv'), encoding='utf-8') as f:
    rows = list(csv.reader(f))

# ── MONTHLY ──────────────────────────────────────────────────────────────────
month_headers = rows[0]  # ['', 'ene-26', 'feb-26', ..., 'dic-26', 'FY']
v2_row  = rows[4]        # NMV Plan V2
f48_row = rows[6]        # 4+8

MONTHS_LABEL = ['ene','feb','mar','abr','may','jun','jul','ago','sep','oct','nov','dic']
monthly = []
for col_idx, lbl in enumerate(month_headers):
    lbl = lbl.strip().lower()
    m_abbr = lbl.replace('-26','').replace('-2026','')
    if m_abbr not in MONTH_MAP: continue
    mon = MONTH_MAP[m_abbr]
    mk = f'2026-{mon:02d}-01'
    v2  = parse_num(v2_row[col_idx]  if col_idx < len(v2_row)  else None)
    f48 = parse_num(f48_row[col_idx] if col_idx < len(f48_row) else None)
    monthly.append({'Mes': mk, 'NMV_V2': v2, 'NMV_4+8': f48})

with open(os.path.join(DATA_DIR, 'plan_monthly.csv'), 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=['Mes','NMV_V2','NMV_4+8'])
    w.writeheader(); w.writerows(monthly)
print(f"plan_monthly.csv: {len(monthly)} meses")

# ── DAILY ─────────────────────────────────────────────────────────────────────
# Header row is at index 22; data starts at 23
DAILY_HEADER_IDX = 22

def parse_fecha(fecha_str, mes_num):
    """'1-abr' → date(2026, 4, 1)"""
    try:
        parts = fecha_str.strip().split('-')
        day   = int(parts[0])
        mon   = MONTH_MAP.get(parts[1].lower(), mes_num) if len(parts) > 1 else mes_num
        return date(2026, mon, day)
    except:
        return None

daily = []
for row in rows[DAILY_HEADER_IDX + 1:]:
    if not row or not row[0].strip(): continue
    # skip subtotal / pivot rows (col 0 no es un día de semana)
    days_es = {'lunes','martes','miércoles','jueves','viernes','sábado','domingo',
               'miercoles','sabado'}
    if row[0].strip().lower() not in days_es: continue

    try:
        mes_num  = int(row[1].strip()) if row[1].strip().lstrip('-').isdigit() else None
        fecha_d  = parse_fecha(row[2], mes_num)
        if not fecha_d: continue

        # Plan V2: cols 12=Total NMV, 13=Total NSI
        nmv_v2  = parse_num(row[12] if len(row) > 12 else None)
        nsi_v2  = parse_num(row[13] if len(row) > 13 else None)

        # Plan 4+8: cols 47=Total NMV, 48=Total NSI, 49=Total Purchases
        nmv_48  = parse_num(row[47] if len(row) > 47 else None)
        nsi_48  = parse_num(row[48] if len(row) > 48 else None)
        pur_48  = parse_num(row[49] if len(row) > 49 else None)

        daily.append({
            'Fecha': str(fecha_d), 'Dia': row[0].strip(),
            'NMV_V2': nmv_v2, 'NSI_V2': nsi_v2,
            'NMV_4+8': nmv_48, 'NSI_4+8': nsi_48, 'Purchases_4+8': pur_48,
        })
    except Exception as e:
        continue

with open(os.path.join(DATA_DIR, 'plan_daily.csv'), 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=['Fecha','Dia','NMV_V2','NSI_V2','NMV_4+8','NSI_4+8','Purchases_4+8'])
    w.writeheader(); w.writerows(daily)
print(f"plan_daily.csv: {len(daily)} días ({daily[0]['Fecha'] if daily else '?'} → {daily[-1]['Fecha'] if daily else '?'})")

# ── WEEKLY (agrega diario por semana DOM-SAB) ─────────────────────────────────
from collections import defaultdict

def week_start(d):
    """Domingo de la semana de d"""
    return d - timedelta(days=(d.weekday() + 1) % 7)

weekly_acc = defaultdict(lambda: {'NMV_V2':0,'NSI_V2':0,'NMV_4+8':0,'NSI_4+8':0,'Purchases_4+8':0,'days':0})
for r in daily:
    d = date.fromisoformat(r['Fecha'])
    ws = str(week_start(d))
    for k in ('NMV_V2','NSI_V2','NMV_4+8','NSI_4+8','Purchases_4+8'):
        if r[k]: weekly_acc[ws][k] += r[k]
    weekly_acc[ws]['days'] += 1

weekly = [{'Semana': ws, **{k: round(v) if isinstance(v, float) else v
                            for k,v in vals.items() if k != 'days'}}
          for ws, vals in sorted(weekly_acc.items())]

with open(os.path.join(DATA_DIR, 'plan_weekly.csv'), 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=['Semana','NMV_V2','NSI_V2','NMV_4+8','NSI_4+8','Purchases_4+8'])
    w.writeheader(); w.writerows(weekly)
print(f"plan_weekly.csv: {len(weekly)} semanas")

# Quick sanity check
print("\nMensual V2 vs 4+8:")
for m in monthly:
    print(f"  {m['Mes']}  V2={m['NMV_V2']:,.0f}  4+8={m['NMV_4+8']:,.0f}")
