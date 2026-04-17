#!/usr/bin/env python3
"""Fetch weekly and daily growth data from BigQuery for the scorecard."""
import urllib.request, urllib.parse, json, time, sys, datetime, csv, os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DATA_DIR = r'C:\Users\srioboo\dashboard_data'

# ── AUTH ──────────────────────────────────────────────────────────────────────
import subprocess
gcloud = r'C:\Users\srioboo\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
result = subprocess.run(['cmd.exe', '/c', gcloud, 'auth', 'print-access-token'],
    capture_output=True, text=True, timeout=30)
if result.returncode != 0:
    raise RuntimeError(f'gcloud auth failed: {result.stderr[:200]}')
token = result.stdout.strip()
print(f'Token OK (len={len(token)})')

PROJECT = 'meli-bi-data'
ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

# ── BQ HELPERS ────────────────────────────────────────────────────────────────
def bq_post(url, payload):
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(),
        headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'},
        method='POST')
    return json.loads(urllib.request.urlopen(req).read())

def bq_get(url):
    req = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + token})
    return json.loads(urllib.request.urlopen(req).read())

def submit_job(sql, job_id):
    resp = bq_post(
        f'https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/jobs',
        {'configuration': {'query': {'query': sql, 'useLegacySql': False}},
         'jobReference': {'projectId': PROJECT, 'jobId': job_id}})
    print(f'  Submitted {job_id}: {resp["status"]["state"]}')
    return job_id

def wait_job(job_id, timeout=360):
    url = f'https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/jobs/{job_id}'
    for _ in range(timeout // 5):
        resp = bq_get(url)
        state = resp['status']['state']
        if state == 'DONE':
            if 'errorResult' in resp['status']:
                raise RuntimeError(resp['status']['errorResult']['message'])
            return resp
        time.sleep(5)
    raise TimeoutError(f'Job {job_id} timed out')

def get_rows(job_id):
    url = f'https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries/{job_id}?maxResults=10000&timeoutMs=5000'
    resp = bq_get(url)
    schema = [f['name'] for f in resp['schema']['fields']]
    rows = [[c.get('v') for c in r['f']] for r in resp.get('rows', [])]
    return schema, rows

def save_csv(path, schema, rows):
    with open(path, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(schema)
        w.writerows(rows)
    print(f'  Saved {os.path.basename(path)} ({len(rows)} rows)')

# ── QUERIES ───────────────────────────────────────────────────────────────────
SQL_FILLRATE_CTE = """
WITH FR AS (
  SELECT ORD_ORDER_ID,
    CASE WHEN ORD_REQUESTED_QUANTITY_MEASURE != 'unit'
         THEN CAST(ORD_REQUESTED_QUANTITY_VALUE AS NUMERIC)/CAST(NET_WEIGHT AS NUMERIC)
         ELSE CAST(ORD_REQUESTED_QUANTITY_VALUE AS NUMERIC) END AS TSIE,
    CASE WHEN ORD_REQUESTED_QUANTITY_MEASURE != 'unit'
         THEN CAST(ORD_PICKED_QUANTITY AS NUMERIC)/CAST(NET_WEIGHT AS NUMERIC)
         ELSE ORD_PICKED_QUANTITY END AS TSI_PICKED
  FROM `meli-bi-data.WHOWNER.DM_OPS_FH_TB_CANCELS_GLOBAL`
),
ORD AS (
  SELECT
    {date_expr} AS Fecha,
    CAST(b.CRT_PURCHASE_ID AS STRING) AS Compra,
    CAST(b.ORD_ORDER_ID AS STRING) AS Order_ID,
    b.ORD_STATUS AS Status,
    COALESCE(b.cc_usd_ratio, 0) AS usd_ratio,
    FR.TSIE,
    CASE WHEN FR.TSI_PICKED > FR.TSIE THEN FR.TSIE ELSE FR.TSI_PICKED END AS TSI,
    b.ORD_ITEM.UNIT_PRICE * FR.TSIE AS TGMV_LC,
    b.ORD_ITEM.UNIT_PRICE * (CASE WHEN FR.TSI_PICKED > FR.TSIE THEN FR.TSIE ELSE FR.TSI_PICKED END) AS TGMV_FR_LC
  FROM `meli-bi-data.WHOWNER.BT_ORD_ORDERS` AS b
  LEFT JOIN FR ON b.ORD_ORDER_ID = FR.ORD_ORDER_ID
  WHERE SIT_SITE_ID = 'MLA' AND ord_gmv_flg IS TRUE AND ord_category.marketplace_id = 'TM'
    AND ord_auto_offer_flg IS NOT TRUE AND ORD_ITEM.SUPERMARKET_FLG IS TRUE
    AND ARRAY_TO_STRING(ORD_ITEM_TAGS, ',') LIKE '%supermarket_partnership%'
    AND ORD_CLOSED_DT >= '{start_date}'
    AND b.ord_status NOT IN ('cancelled')
  GROUP BY ALL
)
SELECT
  Fecha,
  COUNT(DISTINCT Compra) AS Compras,
  COUNT(DISTINCT Order_ID) AS Ordenes,
  ROUND(SUM(CASE WHEN Status = 'partially_refunded' THEN TGMV_FR_LC ELSE TGMV_LC END), 0) AS NMV,
  ROUND(SUM(CASE WHEN Status = 'partially_refunded' THEN TGMV_FR_LC ELSE TGMV_LC END) * AVG(NULLIF(usd_ratio, 0)), 0) AS NMV_USD,
  ROUND(SUM(COALESCE(TSI, TSIE)), 0) AS NSI,
  ROUND(SUM(TSIE), 0) AS TSIE_total
FROM ORD GROUP BY Fecha ORDER BY Fecha
"""

SQL_WEEKLY = SQL_FILLRATE_CTE.format(
    date_expr="DATE_TRUNC(b.ORD_CLOSED_DT, WEEK(SUNDAY))",
    start_date="2025-10-19"
)

SQL_DAILY = SQL_FILLRATE_CTE.format(
    date_expr="DATE(b.ORD_CLOSED_DT)",
    start_date="2026-01-01"
)

SQL_VISITS_WEEKLY = """
SELECT DATE_TRUNC(TIM_DAY, WEEK(SUNDAY)) AS Fecha, SUM(QTY_VISITS) AS Visitas
FROM `meli-bi-data.WHOWNER.DM_VISITS_SPM_CPG`
WHERE SIT_SITE_ID = 'MLA' AND BUSINESS_MODEL = 'INSTORE' AND TIM_DAY >= '2025-10-19'
GROUP BY 1 ORDER BY 1
"""

SQL_VISITS_DAILY = """
SELECT TIM_DAY AS Fecha, SUM(QTY_VISITS) AS Visitas
FROM `meli-bi-data.WHOWNER.DM_VISITS_SPM_CPG`
WHERE SIT_SITE_ID = 'MLA' AND BUSINESS_MODEL = 'INSTORE' AND TIM_DAY >= '2026-01-01'
GROUP BY 1 ORDER BY 1
"""

# ── RUN ───────────────────────────────────────────────────────────────────────
jobs = {
    'weekly_growth':  (SQL_WEEKLY,        f'wg-{ts}'),
    'daily_growth':   (SQL_DAILY,         f'dg-{ts}'),
    'weekly_visits':  (SQL_VISITS_WEEKLY, f'wv-{ts}'),
    'daily_visits':   (SQL_VISITS_DAILY,  f'dv-{ts}'),
}

print('\nSubmitting jobs...')
job_ids = {}
for name, (sql, jid) in jobs.items():
    try:
        submit_job(sql, jid)
        job_ids[name] = jid
    except Exception as e:
        print(f'  ERROR submitting {name}: {e}')

print('\nWaiting for jobs to complete...')
for name, jid in job_ids.items():
    print(f'  Waiting for {name}...')
    try:
        wait_job(jid)
        schema, rows = get_rows(jid)
        save_csv(os.path.join(DATA_DIR, f'{name}.csv'), schema, rows)
    except Exception as e:
        print(f'  ERROR on {name}: {e}')

print('\nDone.')
