#!/usr/bin/env python3
"""
fetch_data.py — Proximity Groceries Scorecard data fetcher
=============================================================
Cross-platform: runs locally AND inside GitHub Actions.

Auth strategy
-------------
  GitHub Actions : set GOOGLE_CREDENTIALS secret (full service account JSON string)
  Local dev      : run `gcloud auth application-default login` — no secret needed

Outputs: data/*.csv  (one file per query)
"""

import os
import json
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ── Constants ─────────────────────────────────────────────────────────────────
STORES = {
    "ARP25161987351": "Scalabrini",
    "ARP25161987354": "Caballito",
    "ARP25161987353": "Villa Urquiza",
    "ARP25161987355": "Vicente Lopez",
}
STORE_IDS        = list(STORES.keys())
STORE_IDS_SQL    = ", ".join(f"'{s}'" for s in STORE_IDS)
CARREFOUR_ID     = 2516198735
DS               = "meli-bi-data.WHOWNER"
DATA_DIR         = "data"
LOOKBACK_DAYS    = 395      # 13 months so we can always compute vs LY

SP_FILTER = (
    "ORD_ITEM.SUPERMARKET_FLG IS TRUE "
    "AND ARRAY_TO_STRING(ORD_ITEM_TAGS, ',') LIKE '%supermarket_partnership%'"
)

# ── BigQuery client ────────────────────────────────────────────────────────────
def get_client() -> bigquery.Client:
    """
    Returns a BigQuery client.
    Prefers GOOGLE_CREDENTIALS env var (JSON string) used in GitHub Actions.
    Falls back to Application Default Credentials (local gcloud auth).
    NEVER hardcode credentials here — use GitHub Secrets or gcloud ADC.
    """
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_json:
        info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/bigquery.readonly"],
        )
        return bigquery.Client(credentials=creds, project=info["project_id"])
    # Local: gcloud auth application-default login
    return bigquery.Client()


def run(client: bigquery.Client, sql: str, label: str = "") -> pd.DataFrame:
    print(f"  → querying {label} ({len(sql):,} chars)...")
    df = client.query(sql).result().to_dataframe()
    print(f"  ✓ {len(df):,} rows")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — GROWTH
#  Orders, GMV, buyers, fresh product penetration, visits/sessions
# ══════════════════════════════════════════════════════════════════════════════

GROWTH_SQL = f"""
-- Growth: orders, GMV, buyers, fresh orders — one row per (date, store)
WITH base AS (
  SELECT
    DATE(ORD.ORD_CREATED_DT)                                                AS order_date,
    ORD_ITEM.NODE_ID                                                         AS store_id,
    ORD.ORD_ID,
    ORD.ORD_BUYER_ID,
    CAST(ORD.ORD_ITEM_GROSS_VALUE_USD AS FLOAT64)                            AS gmv_usd,
    -- fresh flag: order contains at least one item tagged 'fresh'
    MAX(
      CASE WHEN LOWER(ARRAY_TO_STRING(ORD_ITEM_TAGS, ',')) LIKE '%fresh%'
           THEN 1 ELSE 0 END
    ) OVER (PARTITION BY ORD.ORD_ID)                                         AS has_fresh
  FROM `{DS}.BT_ORD_ORDERS` AS ORD,
       UNNEST(ORD.ORD_ITEMS) AS ORD_ITEM
  WHERE {SP_FILTER}
    AND ORD_ITEM.NODE_ID IN ({STORE_IDS_SQL})
    AND DATE(ORD.ORD_CREATED_DT) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
    AND ORD.ORD_STATUS NOT IN ('cancelled', 'invalid')
)
SELECT
  order_date,
  store_id,
  COUNT(DISTINCT ORD_ID)                                    AS orders,
  COUNT(DISTINCT ORD_BUYER_ID)                              AS buyers,
  ROUND(SUM(gmv_usd), 2)                                    AS gmv_usd,
  COUNT(DISTINCT CASE WHEN has_fresh = 1 THEN ORD_ID END)   AS fresh_orders,
  COUNT(DISTINCT CASE WHEN has_fresh = 1 THEN ORD_BUYER_ID END) AS fresh_buyers
FROM base
GROUP BY 1, 2
ORDER BY 1, 2
"""

VISITS_SQL = f"""
-- Visits / sessions by store, daily
SELECT
  DATE(SESSION_DATE)          AS visit_date,
  NODE_ID                     AS store_id,
  SUM(TOTAL_SESSIONS)         AS sessions,
  SUM(UNIQUE_VISITORS)        AS unique_visitors
FROM `{DS}.DM_VISITS_SPM_CPG`
WHERE NODE_ID IN ({STORE_IDS_SQL})
  AND DATE(SESSION_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
GROUP BY 1, 2
ORDER BY 1, 2
"""


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — OPS
#  Strategy: try the pre-built scorecard table first (single query), then fall
#  back to the component tables if fields are missing.
# ══════════════════════════════════════════════════════════════════════════════

# Primary: master scorecard (most efficient — already joined)
OPS_SCORECARD_SQL = f"""
-- OPS master scorecard from DM_OPS_FH_TB_DASH_SCORECARD_2025_GLOBAL
-- NOTE: verify exact column names against table schema and adjust if needed
SELECT
  CAST(DT_ORDER        AS DATE)            AS order_date,
  NODE_ID                                  AS store_id,
  -- Cancellations
  COALESCE(CANCEL_RATE,            0)      AS cancel_rate,
  COALESCE(CANCELS_TOTAL,          0)      AS cancels_total,
  COALESCE(CANCELS_SELLER,         0)      AS cancels_seller,
  COALESCE(CANCELS_BUYER,          0)      AS cancels_buyer,
  COALESCE(CANCELS_STOCKOUT,       0)      AS cancels_stockout,
  -- Fill rate
  COALESCE(FILL_RATE,              0)      AS fill_rate,
  -- On-time delivery
  COALESCE(OT_RATE,                0)      AS ot_rate,
  COALESCE(OT_EARLY_PCT,           0)      AS ot_early_pct,
  COALESCE(OT_DELAY_PCT,           0)      AS ot_delay_pct,
  COALESCE(OT_ONTIME_PCT,          0)      AS ot_ontime_pct,
  -- Perfect purchase
  COALESCE(PERFECT_PURCHASE_RATE,  0)      AS perfect_purchase_rate,
  -- Volume reference
  COALESCE(TOTAL_ORDERS,           0)      AS total_orders
FROM `{DS}.DM_OPS_FH_TB_DASH_SCORECARD_2025_GLOBAL`
WHERE NODE_ID IN ({STORE_IDS_SQL})
  AND CAST(DT_ORDER AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
ORDER BY order_date, store_id
"""

# Fallback: cancellations from component table (granular cancel types)
CANCELS_SQL = f"""
-- Cancellations by type (SELLER / BUYER / STOCKOUT) and store
-- NOTE: verify column names CANCEL_DATE, NODE_ID, CANCEL_REASON_TYPE
SELECT
  DATE(CANCEL_DATE)           AS cancel_date,
  NODE_ID                     AS store_id,
  UPPER(CANCEL_REASON_TYPE)   AS cancel_type,
  COUNT(*)                    AS cancels
FROM `{DS}.DM_OPS_FH_TB_CANCELS_GLOBAL`
WHERE NODE_ID IN ({STORE_IDS_SQL})
  AND DATE(CANCEL_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""

# Fallback: on-time packs from component table
ONTIME_SQL = f"""
-- On-time delivery status (EARLY / ON_TIME / DELAY) by store and pack
-- NOTE: verify column names PACK_DATE, NODE_ID, OT_STATUS, PACK_ID
SELECT
  DATE(PACK_DATE)             AS pack_date,
  NODE_ID                     AS store_id,
  UPPER(OT_STATUS)            AS ot_status,
  COUNT(DISTINCT PACK_ID)     AS packs
FROM `{DS}.DM_OPS_FH_TB_ON_TIME_NEW_PACKS_GLOBAL`
WHERE NODE_ID IN ({STORE_IDS_SQL})
  AND DATE(PACK_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""

# Fallback: perfect purchase from component table
PERFECT_SQL = f"""
-- Perfect purchase by store
-- NOTE: verify column names ORDER_DATE, NODE_ID, ORDER_ID, PERFECT_PURCHASE_FLAG
SELECT
  DATE(ORDER_DATE)                    AS order_date,
  NODE_ID                             AS store_id,
  COUNT(DISTINCT ORDER_ID)            AS total_orders,
  SUM(CAST(PERFECT_PURCHASE_FLAG AS INT64)) AS perfect_orders
FROM `{DS}.DM_OPS_FH_TB_PERFECT_PURCHASE_GLOBAL`
WHERE NODE_ID IN ({STORE_IDS_SQL})
  AND DATE(ORDER_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
GROUP BY 1, 2
ORDER BY 1, 2
"""

# Fallback: fill rate from component table
FILLRATE_SQL = f"""
-- Fill rate (items picked / items ordered) by store
-- NOTE: verify column names ORDER_DATE, NODE_ID, ITEMS_ORDERED, ITEMS_PICKED
SELECT
  DATE(ORDER_DATE)                                                    AS order_date,
  NODE_ID                                                             AS store_id,
  SUM(ITEMS_ORDERED)                                                  AS items_ordered,
  SUM(ITEMS_PICKED)                                                   AS items_picked,
  ROUND(SAFE_DIVIDE(SUM(ITEMS_PICKED), SUM(ITEMS_ORDERED)), 4)       AS fill_rate
FROM `{DS}.DM_OPS_FH_TB_NEW_FILLRATE_ITEMS_GLOBAL`
WHERE NODE_ID IN ({STORE_IDS_SQL})
  AND DATE(ORDER_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
GROUP BY 1, 2
ORDER BY 1, 2
"""


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — CX
#  Claims filtered by Carrefour respondent ID; BPP from proximity table
# ══════════════════════════════════════════════════════════════════════════════

CLAIMS_SQL = f"""
-- CX Claims for Carrefour (CLA_RESPONDENT_ID = {CARREFOUR_ID})
-- Joined to orders so we can break down by store
WITH scoped_orders AS (
  SELECT DISTINCT
    ORD.ORD_ID,
    DATE(ORD.ORD_CREATED_DT) AS order_date,
    ORD_ITEM.NODE_ID          AS store_id
  FROM `{DS}.BT_ORD_ORDERS` ORD,
       UNNEST(ORD.ORD_ITEMS) AS ORD_ITEM
  WHERE {SP_FILTER}
    AND ORD_ITEM.NODE_ID IN ({STORE_IDS_SQL})
    AND DATE(ORD.ORD_CREATED_DT) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
),
carrefour_claims AS (
  SELECT
    PP.ORD_ID,
    DATE(PP.CLA_CREATED_DT)   AS claim_date,
    PP.CLA_TYPE                AS claim_type,
    PP.CLA_RESULT              AS claim_result
  FROM `{DS}.DM_CX_POST_PURCHASE` PP
  WHERE PP.CLA_RESPONDENT_ID = {CARREFOUR_ID}
    AND DATE(PP.CLA_CREATED_DT) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
)
SELECT
  o.order_date,
  o.store_id,
  c.claim_date,
  c.claim_type,
  c.claim_result,
  COUNT(DISTINCT c.ORD_ID)  AS claims
FROM carrefour_claims c
JOIN scoped_orders o ON c.ORD_ID = o.ORD_ID
GROUP BY 1, 2, 3, 4, 5
ORDER BY o.order_date, o.store_id
"""

BPP_SQL = f"""
-- BPP (Bad Purchase Protection) amounts for Proximity/Carrefour
-- Fields: BPP_DATE, CLAIM_TYPE (PDD/PNR/...), BPP_CO_TOTAL_USD, BPP_RECOVERY_USD
SELECT
  DATE(BPP_DATE)                          AS bpp_date,
  UPPER(CLAIM_TYPE)                       AS claim_type,
  ROUND(SUM(BPP_CO_TOTAL_USD), 2)         AS bpp_total_usd,
  ROUND(SUM(BPP_RECOVERY_USD), 2)         AS bpp_recovery_usd,
  COUNT(*)                                AS cases
FROM `{DS}.DM_CX_BPP_PROXIMITY_CO`
WHERE DATE(BPP_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
GROUP BY 1, 2
ORDER BY 1, 2
"""


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — P&L
#  GMV, NMV, take rate, delivery fee, service fee by store
# ══════════════════════════════════════════════════════════════════════════════

PNL_SQL = f"""
-- P&L financials by store and date
SELECT
  DATE(ORD.ORD_CREATED_DT)                                              AS order_date,
  ORD_ITEM.NODE_ID                                                       AS store_id,
  COUNT(DISTINCT ORD.ORD_ID)                                            AS orders,
  ROUND(SUM(CAST(ORD.ORD_ITEM_GROSS_VALUE_USD  AS FLOAT64)), 2)         AS gmv_usd,
  ROUND(SUM(CAST(ORD.ORD_ITEM_NET_VALUE_USD    AS FLOAT64)), 2)         AS nmv_usd,
  ROUND(SUM(CAST(ORD.DELIVERY_FEE_USD          AS FLOAT64)), 2)         AS delivery_fee_usd,
  ROUND(SUM(CAST(ORD.SERVICE_FEE_USD           AS FLOAT64)), 2)         AS service_fee_usd,
  ROUND(
    SAFE_DIVIDE(
      SUM(CAST(ORD.ORD_ITEM_NET_VALUE_USD   AS FLOAT64)),
      NULLIF(SUM(CAST(ORD.ORD_ITEM_GROSS_VALUE_USD AS FLOAT64)), 0)
    ), 4
  )                                                                      AS take_rate
FROM `{DS}.BT_ORD_ORDERS` ORD,
     UNNEST(ORD.ORD_ITEMS) AS ORD_ITEM
WHERE {SP_FILTER}
  AND ORD_ITEM.NODE_ID IN ({STORE_IDS_SQL})
  AND DATE(ORD.ORD_CREATED_DT) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
  AND ORD.ORD_STATUS NOT IN ('cancelled', 'invalid')
GROUP BY 1, 2
ORDER BY 1, 2
"""


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

QUERIES = {
    # Growth
    "growth_daily":      GROWTH_SQL,
    "visits_daily":      VISITS_SQL,
    # Ops (scorecard first; component tables as fallback)
    "ops_scorecard":     OPS_SCORECARD_SQL,
    "ops_cancels":       CANCELS_SQL,
    "ops_ontime":        ONTIME_SQL,
    "ops_perfect":       PERFECT_SQL,
    "ops_fillrate":      FILLRATE_SQL,
    # CX
    "cx_claims":         CLAIMS_SQL,
    "cx_bpp":            BPP_SQL,
    # P&L
    "pnl_daily":         PNL_SQL,
}


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting data fetch…\n")

    client = get_client()
    errors = []

    for name, sql in QUERIES.items():
        print(f"[{name}]")
        try:
            df = run(client, sql, name)
            path = os.path.join(DATA_DIR, f"{name}.csv")
            df.to_csv(path, index=False)
            print(f"  → saved to {path}\n")
        except Exception as exc:
            print(f"  ✗ FAILED: {exc}\n")
            errors.append((name, str(exc)))

    # Write fetch metadata
    meta = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "errors": errors,
        "queries_ok": len(QUERIES) - len(errors),
        "queries_failed": len(errors),
    }
    with open(os.path.join(DATA_DIR, "meta.json"), "w") as f:
        json.dump(meta, f, indent=2)

    if errors:
        print(f"\n⚠  {len(errors)} query(ies) failed:")
        for n, e in errors:
            print(f"   • {n}: {e}")
        raise SystemExit(1)
    else:
        print(f"\n✓ All {len(QUERIES)} queries succeeded.")


if __name__ == "__main__":
    main()
