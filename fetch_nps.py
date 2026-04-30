#!/usr/bin/env python3
"""fetch_nps.py — NPS mensual por tienda desde BT_CX_NPS_TX_SURVEY_RESPONSES"""
import subprocess, urllib.request, json, time, sys, csv, os, datetime
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DATA_DIR = r'C:\Users\srioboo\dashboard_data'
PROJECT  = 'meli-bi-data'

gcloud = r'C:\Users\srioboo\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
res = subprocess.run(['cmd.exe','/c',gcloud,'auth','print-access-token'], capture_output=True, text=True, timeout=30)
TOKEN = res.stdout.strip()
print(f'Token OK (len={len(TOKEN)})')

def bq_post(url, payload):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
        headers={'Authorization':'Bearer '+TOKEN,'Content-Type':'application/json'}, method='POST')
    return json.loads(urllib.request.urlopen(req).read())

def bq_get(url):
    req = urllib.request.Request(url, headers={'Authorization':'Bearer '+TOKEN})
    return json.loads(urllib.request.urlopen(req).read())

def run_query(sql, jid, timeout=360):
    bq_post(f'https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/jobs',
        {'configuration':{'query':{'query':sql,'useLegacySql':False}},
         'jobReference':{'projectId':PROJECT,'jobId':jid}})
    url = f'https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/jobs/{jid}'
    for _ in range(timeout//5):
        r = bq_get(url)
        if r['status']['state'] == 'DONE':
            if 'errorResult' in r['status']: raise RuntimeError(r['status']['errorResult']['message'])
            break
        time.sleep(5)
    r2 = bq_get(f'https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries/{jid}?maxResults=5000&timeoutMs=5000')
    schema = [f['name'] for f in r2['schema']['fields']]
    rows   = [[c.get('v') for c in row['f']] for row in r2.get('rows',[])]
    return schema, rows

ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

SQL_NPS = """
WITH
NPS_Data AS (
  SELECT
    JSON_VALUE(NPS_TX_PARTICULAR_QUESTIONS, '$[0].NPS_TX_SIT_SITE_ID') AS NPS_TX_SIT_SITE_ID,
    NPS_TX_E_CODE, NPS_TX_CUS_CUST_ID, NPS_TX_RES_END_DT,
    CAST(NPS_TX_ORD_ORDER_ID AS STRING) AS Order_ID,
    CASE
      WHEN SAFE_CAST(SAFE_CAST(NPS_TX_NOTA_NPS AS FLOAT64) AS INT64) < 7  THEN -1
      WHEN SAFE_CAST(SAFE_CAST(NPS_TX_NOTA_NPS AS FLOAT64) AS INT64) < 9  THEN  0
      WHEN SAFE_CAST(SAFE_CAST(NPS_TX_NOTA_NPS AS FLOAT64) AS INT64) IN (9,10) THEN 1
      ELSE NULL
    END AS NPS
  FROM `meli-bi-data.WHOWNER.BT_CX_NPS_TX_SURVEY_RESPONSES`
  WHERE NPS_TX_E_CODE LIKE '%CARREFOUR%'
),
Order_Store_Mapping AS (
  SELECT DISTINCT
    CAST(ORD_ORDER_ID AS STRING) AS Order_ID,
    CASE
      WHEN ORD_ITEM.NODE_ID = 'ARP25161987351' THEN 'Scalabrini'
      WHEN ORD_ITEM.NODE_ID = 'ARP25161987354' THEN 'Caballito'
      WHEN ORD_ITEM.NODE_ID = 'ARP25161987353' THEN 'Villa Urquiza'
      WHEN ORD_ITEM.NODE_ID = 'ARP25161987355' THEN 'Vicente Lopez'
      ELSE NULL
    END AS Tienda
  FROM `meli-bi-data.WHOWNER.BT_ORD_ORDERS`
  WHERE SIT_SITE_ID = 'MLA' AND ORD_CLOSED_DT >= '2025-01-01'
),
Joined AS (
  SELECT n.*, m.Tienda
  FROM NPS_Data n
  LEFT JOIN Order_Store_Mapping m ON n.Order_ID = m.Order_ID
  WHERE n.NPS IS NOT NULL
)
SELECT
  DATE_TRUNC(NPS_TX_RES_END_DT, MONTH)                               AS Mes,
  COALESCE(Tienda, 'Total')                                           AS Tienda,
  ROUND((COUNTIF(NPS=1) - COUNTIF(NPS=-1)) / NULLIF(COUNT(*),0) * 100, 0) AS NPS_Score,
  COUNT(*)                                                             AS Total_Responses,
  COUNTIF(NPS=1)                                                       AS Promotores,
  COUNTIF(NPS=0)                                                       AS Neutros,
  COUNTIF(NPS=-1)                                                      AS Detractores
FROM Joined
GROUP BY GROUPING SETS ((DATE_TRUNC(NPS_TX_RES_END_DT, MONTH), Tienda),
                        (DATE_TRUNC(NPS_TX_RES_END_DT, MONTH)))
ORDER BY Mes, Tienda
"""

print('\nRunning NPS query...')
schema, rows = run_query(SQL_NPS, f'nps-{ts}')
p = os.path.join(DATA_DIR, 'monthly_nps.csv')
with open(p, 'w', encoding='utf-8', newline='') as f:
    w = csv.writer(f); w.writerow(schema); w.writerows(rows)
print(f'Saved monthly_nps.csv ({len(rows)} rows)')
if rows:
    print(f'  First: {dict(zip(schema, rows[0]))}')
    print(f'  Last:  {dict(zip(schema, rows[-1]))}')
