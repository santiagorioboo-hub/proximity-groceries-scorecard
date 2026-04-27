#!/usr/bin/env python3
"""Fetch weekly Ops (scorecard table) and CVR (traffic + buyers)."""
import subprocess, urllib.request, json, time, sys, csv, os, datetime
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DATA_DIR = r'C:\Users\srioboo\dashboard_data'
PROJECT  = 'meli-bi-data'

gcloud = r'C:\Users\srioboo\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
res = subprocess.run(['cmd.exe','/c',gcloud,'auth','print-access-token'],
    capture_output=True, text=True, timeout=30)
TOKEN = res.stdout.strip()
print(f'Token OK (len={len(TOKEN)})')

def bq_post(url, payload):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
        headers={'Authorization':'Bearer '+TOKEN,'Content-Type':'application/json'}, method='POST')
    return json.loads(urllib.request.urlopen(req).read())

def bq_get(url):
    req = urllib.request.Request(url, headers={'Authorization':'Bearer '+TOKEN})
    return json.loads(urllib.request.urlopen(req).read())

def submit(sql, jid):
    r = bq_post(f'https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/jobs',
        {'configuration':{'query':{'query':sql,'useLegacySql':False}},
         'jobReference':{'projectId':PROJECT,'jobId':jid}})
    print(f'  Submitted {jid}: {r["status"]["state"]}')

def wait_and_get(jid, timeout=360):
    url = f'https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/jobs/{jid}'
    for _ in range(timeout//5):
        r = bq_get(url)
        if r['status']['state'] == 'DONE':
            if 'errorResult' in r['status']:
                raise RuntimeError(r['status']['errorResult']['message'])
            break
        time.sleep(5)
    r2 = bq_get(f'https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries/{jid}?maxResults=10000&timeoutMs=5000')
    schema = [f['name'] for f in r2['schema']['fields']]
    rows   = [[c.get('v') for c in row['f']] for row in r2.get('rows',[])]
    return schema, rows

def save(fname, schema, rows):
    p = os.path.join(DATA_DIR, fname)
    with open(p,'w',encoding='utf-8',newline='') as f:
        w = csv.writer(f); w.writerow(schema); w.writerows(rows)
    print(f'  Saved {fname} ({len(rows)} rows)')

ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

# Carrefour store IDs (from exploration)
STORE_IDS = "74170418, 74168488, 73211008, 74610774"  # VU, Cab, Scala, VL

# ── WEEKLY OPS ────────────────────────────────────────────────────────────────
SQL_OPS_WEEKLY = f"""
SELECT
  DATE_TRUNC(DATE, WEEK(SUNDAY)) AS Semana,
  -- Fill Rate Items
  ROUND(SAFE_DIVIDE(SUM(FI_TSI_PICKED_QT_FILL_RATE), SUM(FI_TSI)), 4) AS FR_Items,
  ROUND(SAFE_DIVIDE(SUM(FI_TSI_PICKED_QT_FILL_RATE_REPLACEMENT), SUM(FI_TSI)), 4) AS FR_Items_Reemplazo,
  -- Fill Rate Compras
  ROUND(SAFE_DIVIDE(SUM(FC_COMPLETE_PURCHASE), SUM(FC_TOTAL_PURCHASES)), 4) AS FR_Compras,
  ROUND(SAFE_DIVIDE(SUM(FC_COMPLETE_PURCHASE_REPLACEMENT), SUM(FC_TOTAL_PURCHASES)), 4) AS FR_Compras_Reemplazo,
  -- On Time
  ROUND(SAFE_DIVIDE(SUM(OT_ON_TIME), SUM(OT_PACKS)), 4) AS On_Time,
  -- Cancelaciones
  ROUND(SAFE_DIVIDE(SUM(CR_TOTAL_CANCELS_PURCHASES), SUM(CR_TOTAL_PURCHASES)), 4) AS Cancel_Rate,
  -- Totales para verificación
  SUM(FI_TSI) AS TSI_total,
  SUM(FC_TOTAL_PURCHASES) AS Compras_total,
  SUM(OT_PACKS) AS Packs_total,
  SUM(CR_TOTAL_PURCHASES) AS CR_Purchases_total
FROM `meli-bi-data.WHOWNER.DM_OPS_FH_TB_DASH_SCORECARD_2025_GLOBAL`
WHERE SIT_SITE_ID = 'MLA'
  AND CAST(STORE_ID AS STRING) IN ({','.join(["'"+s.strip()+"'" for s in STORE_IDS.split(",")])})
  AND DATE >= '2025-10-21'
GROUP BY Semana
ORDER BY Semana
"""

# ── WEEKLY CVR ────────────────────────────────────────────────────────────────
SQL_CVR_WEEKLY = """
WITH buyers AS (
  SELECT
    DATE_TRUNC(ORD_DATE, WEEK(SUNDAY)) AS Semana,
    COUNT(DISTINCT BUYER_ID) AS Buyers
  FROM `meli-bi-data.WHOWNER.DM_ORD_DATA_SPM_AGRUP`
  WHERE ORD_DATE >= '2025-10-21'
    AND BUSINESS_MODEL = 'INSTORE'
    AND SIT_SITE_ID = 'MLA'
  GROUP BY 1
),
visitors AS (
  SELECT
    DATE_TRUNC(TIM_DAY, WEEK(SUNDAY)) AS Semana,
    COUNT(DISTINCT USER_ID) AS Visitors
  FROM `meli-bi-data.WHOWNER.DM_TRAFIC_SPM`, UNNEST(PAGE_EVENTS) AS PE
  WHERE TIM_DAY >= '2025-10-21'
    AND PE.NAME IN ('ITEM_PAGE','HOME_INSTORE','ADD_TO_CART','HOME_STORE_BUTTON',
                    'MAIN_AISLE','WIDGET','AISLE','BANNER','CART_INTERVENTION',
                    'CAROUSEL','PUSH_OPEN')
    AND BUSINESS_MODEL = 'INSTORE'
    AND SIT_SITE_ID = 'MLA'
  GROUP BY 1
)
SELECT
  COALESCE(b.Semana, v.Semana) AS Semana,
  IFNULL(b.Buyers, 0)   AS Buyers,
  IFNULL(v.Visitors, 0) AS Visitors,
  ROUND(SAFE_DIVIDE(IFNULL(b.Buyers,0), NULLIF(IFNULL(v.Visitors,0),0)), 4) AS CVR
FROM buyers b
FULL JOIN visitors v USING(Semana)
ORDER BY Semana
"""

# ── DAILY CVR (for rolling windows) ──────────────────────────────────────────
SQL_CVR_DAILY = """
WITH buyers AS (
  SELECT
    ORD_DATE AS Fecha,
    COUNT(DISTINCT BUYER_ID) AS Buyers
  FROM `meli-bi-data.WHOWNER.DM_ORD_DATA_SPM_AGRUP`
  WHERE ORD_DATE >= '2025-10-21'
    AND BUSINESS_MODEL = 'INSTORE'
    AND SIT_SITE_ID = 'MLA'
  GROUP BY 1
),
visitors AS (
  SELECT
    TIM_DAY AS Fecha,
    COUNT(DISTINCT USER_ID) AS Visitors
  FROM `meli-bi-data.WHOWNER.DM_TRAFIC_SPM`, UNNEST(PAGE_EVENTS) AS PE
  WHERE TIM_DAY >= '2025-10-21'
    AND PE.NAME IN ('ITEM_PAGE','HOME_INSTORE','ADD_TO_CART','HOME_STORE_BUTTON',
                    'MAIN_AISLE','WIDGET','AISLE','BANNER','CART_INTERVENTION',
                    'CAROUSEL','PUSH_OPEN')
    AND BUSINESS_MODEL = 'INSTORE'
    AND SIT_SITE_ID = 'MLA'
  GROUP BY 1
)
SELECT
  COALESCE(b.Fecha, v.Fecha) AS Fecha,
  IFNULL(b.Buyers, 0)   AS Buyers,
  IFNULL(v.Visitors, 0) AS Visitors,
  ROUND(SAFE_DIVIDE(IFNULL(b.Buyers,0), NULLIF(IFNULL(v.Visitors,0),0)), 4) AS CVR
FROM buyers b
FULL JOIN visitors v USING(Fecha)
ORDER BY Fecha
"""

print('\nSubmitting jobs...')
jobs = {
    'weekly_ops.csv':   (SQL_OPS_WEEKLY,  f'ops-w-{ts}'),
    'weekly_cvr.csv':   (SQL_CVR_WEEKLY,  f'cvr-w-{ts}'),
    'daily_cvr.csv':    (SQL_CVR_DAILY,   f'cvr-d-{ts}'),
}
for fname, (sql, jid) in jobs.items():
    submit(sql, jid)

print('\nWaiting for results...')
for fname, (sql, jid) in jobs.items():
    print(f'  {fname}...')
    try:
        schema, rows = wait_and_get(jid)
        save(fname, schema, rows)
        # Print sample
        if rows:
            print(f'    First row: {dict(zip(schema, rows[0]))}')
            print(f'    Last row:  {dict(zip(schema, rows[-1]))}')
    except Exception as e:
        print(f'  ERROR {fname}: {e}')

print('\nDone.')
