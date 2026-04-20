#!/usr/bin/env python3
"""Fetch additional weekly data: by store, buyers, cancellations, daily full history."""
import subprocess, urllib.request, json, time, sys, csv, os, datetime
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DATA_DIR = r'C:\Users\srioboo\dashboard_data'
PROJECT  = 'meli-bi-data'

# ── AUTH ──────────────────────────────────────────────────────────────────────
gcloud = r'C:\Users\srioboo\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
res = subprocess.run(['cmd.exe','/c',gcloud,'auth','print-access-token'],
    capture_output=True, text=True, timeout=30)
if res.returncode != 0: raise RuntimeError(res.stderr[:200])
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
    print(f'  {jid}: {r["status"]["state"]}'); return jid

def wait(jid, timeout=360):
    url = f'https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/jobs/{jid}'
    for _ in range(timeout//5):
        r = bq_get(url); s = r['status']['state']
        if s == 'DONE':
            if 'errorResult' in r['status']: raise RuntimeError(r['status']['errorResult']['message'])
            return r
        time.sleep(5)
    raise TimeoutError(jid)

def get_rows(jid):
    r = bq_get(f'https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries/{jid}?maxResults=10000&timeoutMs=5000')
    schema = [f['name'] for f in r['schema']['fields']]
    rows   = [[c.get('v') for c in row['f']] for row in r.get('rows',[])]
    return schema, rows

def save(fname, schema, rows):
    p = os.path.join(DATA_DIR, fname)
    with open(p,'w',encoding='utf-8',newline='') as f:
        w = csv.writer(f); w.writerow(schema); w.writerows(rows)
    print(f'  Saved {fname} ({len(rows)} rows)')

ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

# ── QUERIES ───────────────────────────────────────────────────────────────────
SQL_FR = """
  SELECT ORD_ORDER_ID,
    CASE WHEN ORD_REQUESTED_QUANTITY_MEASURE!='unit'
         THEN CAST(ORD_REQUESTED_QUANTITY_VALUE AS NUMERIC)/CAST(NET_WEIGHT AS NUMERIC)
         ELSE CAST(ORD_REQUESTED_QUANTITY_VALUE AS NUMERIC) END AS TSIE,
    CASE WHEN ORD_REQUESTED_QUANTITY_MEASURE!='unit'
         THEN CAST(ORD_PICKED_QUANTITY AS NUMERIC)/CAST(NET_WEIGHT AS NUMERIC)
         ELSE ORD_PICKED_QUANTITY END AS TSI_PICKED
  FROM `meli-bi-data.WHOWNER.DM_OPS_FH_TB_CANCELS_GLOBAL`
"""

SQL_WEEKLY_STORE = f"""
WITH FR AS ({SQL_FR}),
ORD AS (
  SELECT DATE_TRUNC(b.ORD_CLOSED_DT, WEEK(SUNDAY)) AS Semana,
    CASE WHEN b.ORD_ITEM.NODE_ID='ARP25161987351' THEN 'Scalabrini'
         WHEN b.ORD_ITEM.NODE_ID='ARP25161987354' THEN 'Caballito'
         WHEN b.ORD_ITEM.NODE_ID='ARP25161987353' THEN 'Villa Urquiza'
         WHEN b.ORD_ITEM.NODE_ID='ARP25161987355' THEN 'Vicente Lopez'
    END AS Tienda,
    CAST(b.CRT_PURCHASE_ID AS STRING) AS Compra,
    CAST(b.ORD_ORDER_ID AS STRING) AS Order_ID,
    b.ORD_STATUS AS Status,
    COALESCE(b.cc_usd_ratio,0) AS usd_ratio,
    FR.TSIE,
    CASE WHEN FR.TSI_PICKED>FR.TSIE THEN FR.TSIE ELSE FR.TSI_PICKED END AS TSI,
    b.ORD_ITEM.UNIT_PRICE*FR.TSIE AS TGMV_LC,
    b.ORD_ITEM.UNIT_PRICE*(CASE WHEN FR.TSI_PICKED>FR.TSIE THEN FR.TSIE ELSE FR.TSI_PICKED END) AS TGMV_FR_LC
  FROM `meli-bi-data.WHOWNER.BT_ORD_ORDERS` AS b
  LEFT JOIN FR ON b.ORD_ORDER_ID=FR.ORD_ORDER_ID
  WHERE SIT_SITE_ID='MLA' AND ord_gmv_flg IS TRUE AND ord_category.marketplace_id='TM'
    AND ord_auto_offer_flg IS NOT TRUE AND ORD_ITEM.SUPERMARKET_FLG IS TRUE
    AND ARRAY_TO_STRING(ORD_ITEM_TAGS,',') LIKE '%supermarket_partnership%'
    AND ORD_CLOSED_DT>='2025-10-19' AND b.ord_status NOT IN ('cancelled')
  GROUP BY ALL
)
SELECT Semana, Tienda,
  COUNT(DISTINCT Compra) AS Compras,
  COUNT(DISTINCT Order_ID) AS Ordenes,
  ROUND(SUM(CASE WHEN Status='partially_refunded' THEN TGMV_FR_LC ELSE TGMV_LC END),0) AS NMV,
  ROUND(SUM(CASE WHEN Status='partially_refunded' THEN TGMV_FR_LC ELSE TGMV_LC END)*AVG(NULLIF(usd_ratio,0)),0) AS NMV_USD,
  ROUND(SUM(COALESCE(TSI,TSIE)),0) AS NSI,
  ROUND(SUM(TSIE),0) AS TSIE_total
FROM ORD WHERE Tienda IS NOT NULL
GROUP BY Semana, Tienda ORDER BY Semana, Tienda
"""

SQL_WEEKLY_BUYERS = """
SELECT DATE_TRUNC(FECHA, WEEK(SUNDAY)) AS Semana,
  TIENDA,
  COUNT(DISTINCT BUYER_ID) AS Buyers
FROM `meli-bi-data.WHOWNER.DM_VENTAS_MLA_INSTORE`
WHERE FECHA >= '2025-10-19'
GROUP BY 1, 2 ORDER BY 1, 2
"""

SQL_WEEKLY_CANCELS = f"""
WITH ORD AS (
  SELECT DATE_TRUNC(b.ORD_CLOSED_DT, WEEK(SUNDAY)) AS Semana,
    CAST(b.ORD_ORDER_ID AS STRING) AS Order_ID, b.ORD_STATUS
  FROM `meli-bi-data.WHOWNER.BT_ORD_ORDERS` AS b
  WHERE SIT_SITE_ID='MLA' AND ord_gmv_flg IS TRUE AND ord_category.marketplace_id='TM'
    AND ord_auto_offer_flg IS NOT TRUE AND ORD_ITEM.SUPERMARKET_FLG IS TRUE
    AND ARRAY_TO_STRING(ORD_ITEM_TAGS,',') LIKE '%supermarket_partnership%'
    AND ORD_CLOSED_DT>='2025-10-19'
    AND ORD_ITEM.NODE_ID IN ('ARP25161987351','ARP25161987354','ARP25161987353','ARP25161987355')
  GROUP BY ALL
)
SELECT Semana,
  COUNTIF(ORD_STATUS='cancelled') AS Cancelled,
  COUNT(*) AS Total_Orders,
  ROUND(COUNTIF(ORD_STATUS='cancelled')/NULLIF(COUNT(*),0),4) AS Cancel_Rate
FROM ORD GROUP BY Semana ORDER BY Semana
"""

# Daily from Oct 2025 for longer rolling 28d history
SQL_DAILY_FULL = f"""
WITH FR AS ({SQL_FR}),
ORD AS (
  SELECT DATE(b.ORD_CLOSED_DT) AS Fecha,
    CAST(b.CRT_PURCHASE_ID AS STRING) AS Compra,
    CAST(b.ORD_ORDER_ID AS STRING) AS Order_ID,
    b.ORD_STATUS AS Status, COALESCE(b.cc_usd_ratio,0) AS usd_ratio,
    FR.TSIE,
    CASE WHEN FR.TSI_PICKED>FR.TSIE THEN FR.TSIE ELSE FR.TSI_PICKED END AS TSI,
    b.ORD_ITEM.UNIT_PRICE*FR.TSIE AS TGMV_LC,
    b.ORD_ITEM.UNIT_PRICE*(CASE WHEN FR.TSI_PICKED>FR.TSIE THEN FR.TSIE ELSE FR.TSI_PICKED END) AS TGMV_FR_LC
  FROM `meli-bi-data.WHOWNER.BT_ORD_ORDERS` AS b
  LEFT JOIN FR ON b.ORD_ORDER_ID=FR.ORD_ORDER_ID
  WHERE SIT_SITE_ID='MLA' AND ord_gmv_flg IS TRUE AND ord_category.marketplace_id='TM'
    AND ord_auto_offer_flg IS NOT TRUE AND ORD_ITEM.SUPERMARKET_FLG IS TRUE
    AND ARRAY_TO_STRING(ORD_ITEM_TAGS,',') LIKE '%supermarket_partnership%'
    AND ORD_CLOSED_DT>='2025-10-20' AND b.ord_status NOT IN ('cancelled')
  GROUP BY ALL
)
SELECT Fecha,
  COUNT(DISTINCT Compra) AS Compras, COUNT(DISTINCT Order_ID) AS Ordenes,
  ROUND(SUM(CASE WHEN Status='partially_refunded' THEN TGMV_FR_LC ELSE TGMV_LC END),0) AS NMV,
  ROUND(SUM(CASE WHEN Status='partially_refunded' THEN TGMV_FR_LC ELSE TGMV_LC END)*AVG(NULLIF(usd_ratio,0)),0) AS NMV_USD,
  ROUND(SUM(COALESCE(TSI,TSIE)),0) AS NSI, ROUND(SUM(TSIE),0) AS TSIE_total
FROM ORD GROUP BY Fecha ORDER BY Fecha
"""

SQL_VISITS_DAILY_FULL = """
SELECT TIM_DAY AS Fecha, SUM(QTY_VISITS) AS Visitas
FROM `meli-bi-data.WHOWNER.DM_VISITS_SPM_CPG`
WHERE SIT_SITE_ID='MLA' AND BUSINESS_MODEL='INSTORE' AND TIM_DAY>='2025-10-20'
GROUP BY 1 ORDER BY 1
"""

QUERIES = {
    'weekly_by_store.csv':    (SQL_WEEKLY_STORE,    f'ws-{ts}'),
    'weekly_buyers.csv':      (SQL_WEEKLY_BUYERS,   f'wb-{ts}'),
    'weekly_cancels.csv':     (SQL_WEEKLY_CANCELS,  f'wc-{ts}'),
    'daily_growth_full.csv':  (SQL_DAILY_FULL,      f'dgf-{ts}'),
    'daily_visits_full.csv':  (SQL_VISITS_DAILY_FULL, f'dvf-{ts}'),
}

print('\nSubmitting...')
job_ids = {}
for fname, (sql, jid) in QUERIES.items():
    try:
        submit(sql, jid); job_ids[fname] = jid
    except Exception as e:
        print(f'  ERROR {fname}: {e}')

print('\nWaiting...')
for fname, jid in job_ids.items():
    print(f'  {fname}...')
    try:
        wait(jid); schema, rows = get_rows(jid); save(fname, schema, rows)
    except Exception as e:
        print(f'  ERROR {fname}: {e}')

print('\nDone.')
