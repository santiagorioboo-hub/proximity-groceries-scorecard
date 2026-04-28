#!/usr/bin/env python3
"""
generate_dashboard.py — Proximity Groceries Scorecard HTML generator
======================================================================
Reads data/*.csv produced by fetch_data.py and writes index.html.

Dashboard features
------------------
  • Dark theme inspired by Proximity Foods Scorecard
  • Period selector: Daily | Weekly | Monthly
  • Section tabs: Growth | Ops | CX | P&L
  • Hero cards: key KPIs with Δ vs Last Period
  • Tables with expandable per-store rows
  • SVG sparklines (last 28 days / 12 weeks / 12 months)
  • Color-coded deltas: green = good, red = bad
    (direction-aware: for cancel rate / BPP lower = better)
"""

import os
import json
import math
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np

DATA_DIR    = "data"
OUTPUT_FILE = "index.html"

STORES = {
    "ARP25161987351": "Scalabrini",
    "ARP25161987354": "Caballito",
    "ARP25161987353": "Villa Urquiza",
    "ARP25161987355": "Vicente Lopez",
}
STORE_ORDER = ["Scalabrini", "Caballito", "Villa Urquiza", "Vicente Lopez"]
STORE_COLORS = {
    "Scalabrini":    "#3b82f6",
    "Caballito":     "#f59e0b",
    "Villa Urquiza": "#10b981",
    "Vicente Lopez": "#a855f7",
}

# ── Data loading helpers ───────────────────────────────────────────────────────

def load(filename: str) -> pd.DataFrame | None:
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"  ⚠  {path} not found — section will be empty")
        return None
    df = pd.read_csv(path)
    # normalize date columns
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df


def add_store_name(df: pd.DataFrame, id_col: str = "store_id") -> pd.DataFrame:
    df = df.copy()
    df["store_name"] = df[id_col].map(STORES).fillna(df[id_col])
    return df


# ── Aggregation helpers ────────────────────────────────────────────────────────

def iso_week_start(d: date) -> date:
    """Return the Monday of the ISO week containing d."""
    return d - timedelta(days=d.weekday())


def month_key(d: date) -> str:
    return d.strftime("%Y-%m")


def agg_numeric(df: pd.DataFrame, group_cols: list, sum_cols: list) -> pd.DataFrame:
    """Sum numeric columns, keeping group_cols as index."""
    return df.groupby(group_cols)[sum_cols].sum().reset_index()


# ── Spark line generator ───────────────────────────────────────────────────────

def sparkline_svg(values: list, width: int = 80, height: int = 28,
                  color: str = "#3b82f6", lower_is_better: bool = False) -> str:
    """Return an inline SVG sparkline path."""
    clean = [v for v in values if v is not None and not math.isnan(float(v))]
    if len(clean) < 2:
        return ""
    mn, mx = min(clean), max(clean)
    rng = mx - mn or 1
    n = len(clean)
    step = width / (n - 1)
    pts = []
    for i, v in enumerate(clean):
        x = round(i * step, 1)
        y = round(height - ((float(v) - mn) / rng) * (height - 4) - 2, 1)
        pts.append(f"{x},{y}")
    path = " ".join(pts)
    last = clean[-1]
    prev = clean[-2] if len(clean) >= 2 else last
    if lower_is_better:
        stroke = "#ef4444" if last > prev else "#22c55e"
    else:
        stroke = "#22c55e" if last >= prev else "#ef4444"
    return (
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
        f'fill="none" xmlns="http://www.w3.org/2000/svg">'
        f'<polyline points="{path}" stroke="{stroke}" stroke-width="1.8" '
        f'stroke-linecap="round" stroke-linejoin="round"/>'
        f"</svg>"
    )


# ── Delta computation ──────────────────────────────────────────────────────────

def pct_delta(current, previous) -> float | None:
    if previous is None or previous == 0 or current is None:
        return None
    return (current - previous) / abs(previous)


def fmt_delta(delta: float | None, lower_is_better: bool = False,
              as_pct: bool = True) -> str:
    """Return an HTML badge for a delta value."""
    if delta is None:
        return '<span class="delta neutral">—</span>'
    if as_pct:
        val_str = f"{delta:+.1%}"
    else:
        val_str = f"{delta:+,.0f}"
    good = delta <= 0 if lower_is_better else delta >= 0
    cls = "positive" if good else "negative"
    arrow = "▲" if delta > 0 else "▼"
    return f'<span class="delta {cls}">{arrow} {val_str}</span>'


def fmt_num(v, fmt=",.0f", suffix="") -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    if fmt.endswith("%"):
        return f"{v:.1%}{suffix}"
    return f"{v:{fmt}}{suffix}"


def fmt_usd(v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:,.0f}"


# ══════════════════════════════════════════════════════════════════════════════
#  DATA PROCESSING — GROWTH
# ══════════════════════════════════════════════════════════════════════════════

def process_growth(period: str = "weekly") -> dict:
    """
    Returns {total: {...}, stores: {name: {...}}}
    Each entry: {current, vs_lp, sparkline_html}
    Metrics: orders, buyers, gmv_usd, fresh_orders, fresh_rate
    """
    df = load("growth_daily.csv")
    vis = load("visits_daily.csv")
    result = {"total": {}, "stores": {}}
    if df is None:
        return result

    df = add_store_name(df)
    NUM_COLS = ["orders", "buyers", "gmv_usd", "fresh_orders", "fresh_buyers"]

    # ── aggregate by period ────────────────────────────────────────────────
    if period == "daily":
        latest = df["order_date"].max()
        prev   = latest - timedelta(days=1)
        lpy    = latest - timedelta(days=365)
        curr_df = df[df["order_date"] == latest]
        prev_df = df[df["order_date"] == prev]
        lpy_df  = df[df["order_date"] == lpy]
        spark_dates = sorted([latest - timedelta(days=i) for i in range(27, -1, -1)])
        spark_period_col = "order_date"
    elif period == "weekly":
        df["period"] = df["order_date"].apply(iso_week_start)
        latest_week = df["period"].max()
        prev_week   = latest_week - timedelta(weeks=1)
        lpy_week    = latest_week - timedelta(weeks=52)
        curr_df = df[df["period"] == latest_week]
        prev_df = df[df["period"] == prev_week]
        lpy_df  = df[df["period"] == lpy_week]
        spark_weeks = sorted([latest_week - timedelta(weeks=i) for i in range(11, -1, -1)])
        spark_period_col = "period"
    else:  # monthly
        df["period"] = df["order_date"].apply(month_key)
        latest_month = df["period"].max()
        months = sorted(df["period"].unique())
        idx = months.index(latest_month)
        prev_month   = months[idx - 1] if idx > 0 else None
        lpy_idx      = idx - 12
        lpy_month    = months[lpy_idx] if lpy_idx >= 0 else None
        curr_df = df[df["period"] == latest_month]
        prev_df = df[df["period"] == prev_month] if prev_month else pd.DataFrame()
        lpy_df  = df[df["period"] == lpy_month]  if lpy_month  else pd.DataFrame()
        spark_months = months[max(0, idx-11): idx+1]
        spark_period_col = "period"

    def totals(sub: pd.DataFrame) -> dict | None:
        if sub.empty:
            return None
        t = {c: sub[c].sum() for c in NUM_COLS if c in sub.columns}
        t["fresh_rate"] = t.get("fresh_orders", 0) / t.get("orders", 1)
        return t

    curr_t = totals(curr_df)
    prev_t = totals(prev_df)
    lpy_t  = totals(lpy_df)

    # sparkline (orders, all stores combined)
    if period == "daily":
        spark_vals = [
            df[df["order_date"] == d]["orders"].sum() if d in df["order_date"].values else None
            for d in spark_dates
        ]
    elif period == "weekly":
        spark_vals = [
            df[df["period"] == w]["orders"].sum() if w in df["period"].values else None
            for w in spark_weeks
        ]
    else:
        spark_vals = [
            df[df["period"] == m]["orders"].sum() if m in df["period"].values else None
            for m in spark_months
        ]

    def build_entry(curr, prev, lpy, spark_series):
        if curr is None:
            return {}
        return {
            "orders":      fmt_num(curr.get("orders")),
            "buyers":      fmt_num(curr.get("buyers")),
            "gmv":         fmt_usd(curr.get("gmv_usd")),
            "fresh_orders":fmt_num(curr.get("fresh_orders")),
            "fresh_rate":  fmt_num(curr.get("fresh_rate"), fmt="%"),
            "orders_dlp":  fmt_delta(pct_delta(curr.get("orders"),  prev.get("orders")  if prev else None)),
            "buyers_dlp":  fmt_delta(pct_delta(curr.get("buyers"),  prev.get("buyers")  if prev else None)),
            "gmv_dlp":     fmt_delta(pct_delta(curr.get("gmv_usd"), prev.get("gmv_usd") if prev else None)),
            "fresh_dlp":   fmt_delta(pct_delta(curr.get("fresh_rate"), prev.get("fresh_rate") if prev else None)),
            "orders_dly":  fmt_delta(pct_delta(curr.get("orders"),  lpy.get("orders")   if lpy  else None)),
            "sparkline":   sparkline_svg(spark_series),
        }

    result["total"] = build_entry(curr_t, prev_t, lpy_t, spark_vals)

    # Per-store entries
    for store_id, store_name in STORES.items():
        sc = curr_df[curr_df["store_id"] == store_id]
        sp = prev_df[prev_df["store_id"] == store_id] if not prev_df.empty else pd.DataFrame()
        sl = lpy_df[lpy_df["store_id"] == store_id]  if not lpy_df.empty  else pd.DataFrame()
        sc_t = totals(sc); sp_t = totals(sp); sl_t = totals(sl)

        if period == "daily":
            sv = [
                df[(df["order_date"]==d) & (df["store_id"]==store_id)]["orders"].sum()
                if d in df["order_date"].values else None for d in spark_dates
            ]
        elif period == "weekly":
            sv = [
                df[(df["period"]==w) & (df["store_id"]==store_id)]["orders"].sum()
                if w in df["period"].values else None for w in spark_weeks
            ]
        else:
            sv = [
                df[(df["period"]==m) & (df["store_id"]==store_id)]["orders"].sum()
                if m in df["period"].values else None for m in spark_months
            ]

        result["stores"][store_name] = build_entry(sc_t, sp_t, sl_t, sv)

    return result


# ══════════════════════════════════════════════════════════════════════════════
#  DATA PROCESSING — OPS
# ══════════════════════════════════════════════════════════════════════════════

def process_ops(period: str = "weekly") -> dict:
    """
    Tries scorecard CSV first; falls back to component tables.
    Metrics: cancel_rate, cancel breakdown (seller/buyer/stockout),
             fill_rate, ot_rate (early/delay/ontime), perfect_purchase_rate
    """
    scorecard = load("ops_scorecard.csv")
    cancels   = load("ops_cancels.csv")
    ontime    = load("ops_ontime.csv")
    perfect   = load("ops_perfect.csv")
    fillrate  = load("ops_fillrate.csv")

    result = {"total": {}, "stores": {}}

    # ── use scorecard if available ─────────────────────────────────────────
    if scorecard is not None and not scorecard.empty:
        df = add_store_name(scorecard)
        RATE_COLS  = ["cancel_rate", "fill_rate", "ot_rate", "perfect_purchase_rate",
                      "ot_early_pct", "ot_delay_pct", "ot_ontime_pct"]
        COUNT_COLS = ["cancels_total", "cancels_seller", "cancels_buyer",
                      "cancels_stockout", "total_orders"]
        # keep only cols that actually exist
        RATE_COLS  = [c for c in RATE_COLS  if c in df.columns]
        COUNT_COLS = [c for c in COUNT_COLS if c in df.columns]

        if period == "daily":
            latest = df["order_date"].max()
            prev   = latest - timedelta(days=1)
            curr_df = df[df["order_date"] == latest]
            prev_df = df[df["order_date"] == prev]
            spark_dates = sorted([latest - timedelta(days=i) for i in range(27, -1, -1)])
        elif period == "weekly":
            df["period"] = df["order_date"].apply(iso_week_start)
            latest_w = df["period"].max(); prev_w = latest_w - timedelta(weeks=1)
            curr_df = df[df["period"] == latest_w]
            prev_df = df[df["period"] == prev_w]
            spark_weeks = sorted([latest_w - timedelta(weeks=i) for i in range(11, -1, -1)])
        else:
            df["period"] = df["order_date"].apply(month_key)
            months = sorted(df["period"].unique())
            latest_m = months[-1]; prev_m = months[-2] if len(months) > 1 else None
            curr_df = df[df["period"] == latest_m]
            prev_df = df[df["period"] == prev_m] if prev_m else pd.DataFrame()
            spark_months = months[max(0,len(months)-12):]

        def wtd_avg(sub, rate_col):
            """Weight rate by total_orders if available, else simple mean."""
            if sub.empty or rate_col not in sub.columns:
                return None
            if "total_orders" in sub.columns:
                orders = sub["total_orders"].sum()
                if orders == 0:
                    return None
                return (sub[rate_col] * sub["total_orders"]).sum() / orders
            return sub[rate_col].mean()

        def totals(sub):
            if sub.empty:
                return None
            t = {c: sub[c].sum() for c in COUNT_COLS}
            for r in RATE_COLS:
                t[r] = wtd_avg(sub, r)
            return t

        curr_t = totals(curr_df); prev_t = totals(prev_df)

        def spark_for(sub_df, rate_col):
            if period == "daily":
                return [
                    wtd_avg(sub_df[sub_df["order_date"]==d], rate_col) for d in spark_dates
                ]
            elif period == "weekly":
                return [
                    wtd_avg(sub_df[sub_df["period"]==w], rate_col) for w in spark_weeks
                ]
            else:
                return [
                    wtd_avg(sub_df[sub_df["period"]==m], rate_col) for m in spark_months
                ]

        def build_entry(curr, prev, sub_df):
            if curr is None:
                return {}
            e = {}
            for key, col, lib, fmt in [
                ("cancel_rate",    "cancel_rate",           True,  "%"),
                ("fill_rate",      "fill_rate",             False, "%"),
                ("ot_rate",        "ot_rate",               False, "%"),
                ("perfect_rate",   "perfect_purchase_rate", False, "%"),
                ("ot_early",       "ot_early_pct",          False, "%"),
                ("ot_delay",       "ot_delay_pct",          True,  "%"),
                ("ot_ontime",      "ot_ontime_pct",         False, "%"),
                ("cancels_seller", "cancels_seller",        True,  ",.0f"),
                ("cancels_buyer",  "cancels_buyer",         True,  ",.0f"),
                ("cancels_stockout","cancels_stockout",     True,  ",.0f"),
            ]:
                v = curr.get(col)
                pv= prev.get(col) if prev else None
                e[key]          = fmt_num(v, fmt=fmt) if fmt == "%" else fmt_num(v, fmt=fmt)
                e[f"{key}_dlp"] = fmt_delta(pct_delta(v, pv), lower_is_better=lib)
            if "cancel_rate" in (RATE_COLS or []):
                e["cancel_sparkline"] = sparkline_svg(spark_for(sub_df, "cancel_rate"),
                                                       lower_is_better=True)
                e["fr_sparkline"]     = sparkline_svg(spark_for(sub_df, "fill_rate"))
                e["ot_sparkline"]     = sparkline_svg(spark_for(sub_df, "ot_rate"))
                e["pp_sparkline"]     = sparkline_svg(spark_for(sub_df, "perfect_purchase_rate"))
            return e

        result["total"] = build_entry(curr_t, prev_t, df)
        for store_id, store_name in STORES.items():
            sc = curr_df[curr_df["store_id"] == store_id]
            sp = prev_df[prev_df["store_id"] == store_id] if not prev_df.empty else pd.DataFrame()
            result["stores"][store_name] = build_entry(totals(sc), totals(sp),
                                                        df[df["store_id"] == store_id])
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  DATA PROCESSING — CX
# ══════════════════════════════════════════════════════════════════════════════

def process_cx(period: str = "weekly") -> dict:
    claims_df = load("cx_claims.csv")
    bpp_df    = load("cx_bpp.csv")
    result    = {"total": {}, "stores": {}, "bpp": {}}

    if claims_df is not None and not claims_df.empty:
        claims_df = add_store_name(claims_df)
        # pivot claim_type into columns
        claims_df["claim_date"] = pd.to_datetime(claims_df["claim_date"], errors="coerce").dt.date

        if period == "daily":
            latest  = claims_df["claim_date"].max()
            prev    = latest - timedelta(days=1)
            curr_c  = claims_df[claims_df["claim_date"] == latest]
            prev_c  = claims_df[claims_df["claim_date"] == prev]
        elif period == "weekly":
            claims_df["period"] = claims_df["claim_date"].apply(iso_week_start)
            latest_w = claims_df["period"].max(); prev_w = latest_w - timedelta(weeks=1)
            curr_c = claims_df[claims_df["period"] == latest_w]
            prev_c = claims_df[claims_df["period"] == prev_w]
        else:
            claims_df["period"] = claims_df["claim_date"].apply(month_key)
            months_c = sorted(claims_df["period"].unique())
            latest_m = months_c[-1]; prev_m = months_c[-2] if len(months_c) > 1 else None
            curr_c = claims_df[claims_df["period"] == latest_m]
            prev_c = claims_df[claims_df["period"] == prev_m] if prev_m else pd.DataFrame()

        def cx_totals(sub):
            if sub.empty:
                return None
            return {
                "claims_total": sub["claims"].sum(),
                "claims_by_type": sub.groupby("claim_type")["claims"].sum().to_dict(),
            }

        curr_cx = cx_totals(curr_c)
        prev_cx = cx_totals(prev_c)

        if curr_cx:
            ct = curr_cx["claims_total"]
            pt = prev_cx["claims_total"] if prev_cx else None
            result["total"] = {
                "claims_total":     fmt_num(ct),
                "claims_total_dlp": fmt_delta(pct_delta(ct, pt), lower_is_better=True),
                "by_type":          {k: fmt_num(v) for k, v in curr_cx["claims_by_type"].items()},
            }

        for store_id, store_name in STORES.items():
            sc = curr_c[curr_c["store_id"] == store_id]
            sp = prev_c[prev_c["store_id"] == store_id] if not prev_c.empty else pd.DataFrame()
            sc_t = cx_totals(sc); sp_t = cx_totals(sp)
            if sc_t:
                ct = sc_t["claims_total"]
                pt = sp_t["claims_total"] if sp_t else None
                result["stores"][store_name] = {
                    "claims_total":     fmt_num(ct),
                    "claims_total_dlp": fmt_delta(pct_delta(ct, pt), lower_is_better=True),
                    "by_type":          {k: fmt_num(v) for k, v in sc_t["claims_by_type"].items()},
                }

    # BPP summary
    if bpp_df is not None and not bpp_df.empty:
        bpp_df["bpp_date"] = pd.to_datetime(bpp_df["bpp_date"], errors="coerce").dt.date
        if period == "daily":
            latest_b = bpp_df["bpp_date"].max()
            prev_b   = latest_b - timedelta(days=1)
            curr_b = bpp_df[bpp_df["bpp_date"] == latest_b]
            prev_b = bpp_df[bpp_df["bpp_date"] == prev_b]
        elif period == "weekly":
            bpp_df["period"] = bpp_df["bpp_date"].apply(iso_week_start)
            bw = bpp_df["period"].max()
            curr_b = bpp_df[bpp_df["period"] == bw]
            prev_b = bpp_df[bpp_df["period"] == bw - timedelta(weeks=1)]
        else:
            bpp_df["period"] = bpp_df["bpp_date"].apply(month_key)
            bm = sorted(bpp_df["period"].unique())
            curr_b = bpp_df[bpp_df["period"] == bm[-1]]
            prev_b = bpp_df[bpp_df["period"] == bm[-2]] if len(bm) > 1 else pd.DataFrame()

        ct = curr_b["bpp_total_usd"].sum() if not curr_b.empty else None
        pt = prev_b["bpp_total_usd"].sum() if not prev_b.empty else None
        cr = curr_b["bpp_recovery_usd"].sum() if not curr_b.empty else None
        result["bpp"] = {
            "total":      fmt_usd(ct),
            "recovery":   fmt_usd(cr),
            "total_dlp":  fmt_delta(pct_delta(ct, pt), lower_is_better=True),
            "by_type":    {k: fmt_usd(v) for k, v in
                           curr_b.groupby("claim_type")["bpp_total_usd"].sum().items()},
        }

    return result


# ══════════════════════════════════════════════════════════════════════════════
#  DATA PROCESSING — P&L
# ══════════════════════════════════════════════════════════════════════════════

def process_pnl(period: str = "weekly") -> dict:
    df = load("pnl_daily.csv")
    result = {"total": {}, "stores": {}}
    if df is None or df.empty:
        return result

    df = add_store_name(df)
    NUM_COLS = ["orders", "gmv_usd", "nmv_usd", "delivery_fee_usd", "service_fee_usd"]

    if period == "daily":
        latest = df["order_date"].max()
        prev   = latest - timedelta(days=1)
        lpy    = latest - timedelta(days=365)
        curr_df = df[df["order_date"] == latest]
        prev_df = df[df["order_date"] == prev]
        lpy_df  = df[df["order_date"] == lpy]
        spark_dates = sorted([latest - timedelta(days=i) for i in range(27, -1, -1)])
        spark_period_col = "order_date"
    elif period == "weekly":
        df["period"] = df["order_date"].apply(iso_week_start)
        lw = df["period"].max(); pw = lw - timedelta(weeks=1)
        lyw = lw - timedelta(weeks=52)
        curr_df = df[df["period"] == lw]; prev_df = df[df["period"] == pw]
        lpy_df  = df[df["period"] == lyw]
        spark_weeks = sorted([lw - timedelta(weeks=i) for i in range(11, -1, -1)])
        spark_period_col = "period"
    else:
        df["period"] = df["order_date"].apply(month_key)
        months = sorted(df["period"].unique())
        lm = months[-1]; pm = months[-2] if len(months) > 1 else None
        lym = months[-13] if len(months) >= 13 else None
        curr_df = df[df["period"] == lm]
        prev_df = df[df["period"] == pm]  if pm  else pd.DataFrame()
        lpy_df  = df[df["period"] == lym] if lym else pd.DataFrame()
        spark_months = months[max(0, len(months)-12):]
        spark_period_col = "period"

    def totals(sub):
        if sub.empty:
            return None
        t = {c: sub[c].sum() for c in NUM_COLS if c in sub.columns}
        g = t.get("gmv_usd", 0); n = t.get("nmv_usd", 0)
        t["take_rate"]    = n / g if g else None
        t["contribution"] = n - t.get("delivery_fee_usd", 0) - t.get("service_fee_usd", 0)
        return t

    def spark_gmv(sub_df):
        if period == "daily":
            return [sub_df[sub_df["order_date"]==d]["gmv_usd"].sum() or None for d in spark_dates]
        elif period == "weekly":
            return [sub_df[sub_df["period"]==w]["gmv_usd"].sum() or None for w in spark_weeks]
        else:
            return [sub_df[sub_df["period"]==m]["gmv_usd"].sum() or None for m in spark_months]

    curr_t = totals(curr_df); prev_t = totals(prev_df); lpy_t = totals(lpy_df)

    def build_entry(curr, prev, lpy, sub_df):
        if curr is None:
            return {}
        return {
            "gmv":           fmt_usd(curr.get("gmv_usd")),
            "nmv":           fmt_usd(curr.get("nmv_usd")),
            "take_rate":     fmt_num(curr.get("take_rate"), fmt="%"),
            "delivery_fee":  fmt_usd(curr.get("delivery_fee_usd")),
            "service_fee":   fmt_usd(curr.get("service_fee_usd")),
            "contribution":  fmt_usd(curr.get("contribution")),
            "gmv_dlp":       fmt_delta(pct_delta(curr.get("gmv_usd"),   prev.get("gmv_usd")  if prev else None)),
            "nmv_dlp":       fmt_delta(pct_delta(curr.get("nmv_usd"),   prev.get("nmv_usd")  if prev else None)),
            "gmv_dly":       fmt_delta(pct_delta(curr.get("gmv_usd"),   lpy.get("gmv_usd")   if lpy  else None)),
            "take_rate_dlp": fmt_delta(pct_delta(curr.get("take_rate"), prev.get("take_rate") if prev else None)),
            "sparkline":     sparkline_svg(spark_gmv(sub_df)),
        }

    result["total"] = build_entry(curr_t, prev_t, lpy_t, df)
    for store_id, store_name in STORES.items():
        sc = curr_df[curr_df["store_id"] == store_id]
        sp = prev_df[prev_df["store_id"] == store_id] if not prev_df.empty else pd.DataFrame()
        sl = lpy_df[lpy_df["store_id"] == store_id]  if not lpy_df.empty  else pd.DataFrame()
        result["stores"][store_name] = build_entry(
            totals(sc), totals(sp), totals(sl),
            df[df["store_id"] == store_id]
        )

    return result


# ══════════════════════════════════════════════════════════════════════════════
#  HTML GENERATION
# ══════════════════════════════════════════════════════════════════════════════

CSS = """
:root {
  --bg:        #0d0f14;
  --surface:   #161b26;
  --surface2:  #1e2535;
  --border:    #2a3347;
  --text:      #e2e8f0;
  --muted:     #64748b;
  --accent:    #f59e0b;
  --positive:  #22c55e;
  --negative:  #ef4444;
  --neutral:   #94a3b8;
  --blue:      #3b82f6;
  --purple:    #a855f7;
  --emerald:   #10b981;
  --radius:    8px;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: var(--bg); color: var(--text);
  min-height: 100vh; padding: 0 0 40px;
}
/* ── Header ── */
.header {
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  padding: 16px 24px;
  display: flex; align-items: center; justify-content: space-between;
  position: sticky; top: 0; z-index: 100;
}
.header h1 { font-size: 16px; font-weight: 600; color: var(--text); letter-spacing: .3px; }
.header h1 span { color: var(--accent); }
.updated { font-size: 11px; color: var(--muted); }
/* ── Controls bar ── */
.controls {
  display: flex; align-items: center; gap: 24px;
  padding: 12px 24px; background: var(--surface);
  border-bottom: 1px solid var(--border);
}
.tab-group { display: flex; gap: 2px; background: var(--bg); border-radius: 6px; padding: 3px; }
.tab {
  padding: 5px 14px; font-size: 12px; font-weight: 500; cursor: pointer;
  border-radius: 5px; border: none; background: transparent;
  color: var(--muted); transition: all .15s;
}
.tab.active { background: var(--surface2); color: var(--text); }
.tab:hover:not(.active) { color: var(--text); }
/* ── Section nav ── */
.section-nav {
  display: flex; gap: 0; border-bottom: 1px solid var(--border);
  padding: 0 24px; background: var(--surface);
}
.sec-tab {
  padding: 11px 18px; font-size: 13px; font-weight: 500; cursor: pointer;
  border: none; background: transparent; color: var(--muted);
  border-bottom: 2px solid transparent; transition: all .15s; margin-bottom: -1px;
}
.sec-tab.active { color: var(--accent); border-bottom-color: var(--accent); }
.sec-tab:hover:not(.active) { color: var(--text); }
/* ── Hero cards ── */
.hero-grid {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 12px; padding: 20px 24px 0;
}
.hero-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 16px; position: relative; overflow: hidden;
}
.hero-card::before {
  content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
  background: var(--accent);
}
.hero-label { font-size: 10px; text-transform: uppercase; letter-spacing: .8px; color: var(--muted); margin-bottom: 8px; }
.hero-value { font-size: 22px; font-weight: 700; color: var(--text); line-height: 1.1; }
.hero-delta { margin-top: 6px; }
/* ── Data table ── */
.section-body { padding: 20px 24px; }
.table-wrap { overflow-x: auto; }
table {
  width: 100%; border-collapse: collapse;
  font-size: 13px; background: var(--surface);
  border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden;
}
thead tr { background: var(--surface2); }
th {
  padding: 10px 14px; text-align: left; font-size: 11px; font-weight: 600;
  text-transform: uppercase; letter-spacing: .5px; color: var(--muted);
  border-bottom: 1px solid var(--border); white-space: nowrap;
}
td { padding: 10px 14px; border-bottom: 1px solid var(--border); white-space: nowrap; }
tr:last-child td { border-bottom: none; }
.total-row td { font-weight: 600; background: var(--surface2); cursor: pointer; }
.total-row td:first-child::before {
  content: attr(data-arrow); margin-right: 6px; font-size: 10px; color: var(--accent);
  transition: transform .2s;
}
.store-row td { color: var(--muted); }
.store-row td:first-child { padding-left: 36px; color: var(--text); }
.store-row { display: none; }
.store-row.visible { display: table-row; }
.store-dot {
  display: inline-block; width: 8px; height: 8px; border-radius: 50%;
  margin-right: 8px; vertical-align: middle;
}
/* ── Deltas ── */
.delta { font-size: 11px; font-weight: 600; padding: 2px 6px; border-radius: 4px; }
.delta.positive { color: var(--positive); background: rgba(34,197,94,.1); }
.delta.negative { color: var(--negative); background: rgba(239,68,68,.1); }
.delta.neutral  { color: var(--muted); }
/* ── Sections ── */
.section { display: none; }
.section.active { display: block; }
/* ── Period label ── */
.period-label {
  font-size: 11px; color: var(--muted); padding: 6px 24px 0;
}
/* ── Loading / empty ── */
.empty-row td { color: var(--muted); text-align: center; padding: 32px; font-style: italic; }
/* ── Sub-table for details ── */
.sub-label { font-size: 10px; text-transform: uppercase; letter-spacing: .5px;
             color: var(--muted); margin: 20px 0 8px; }
.metric-group { margin-bottom: 28px; }
.metric-group h3 { font-size: 11px; font-weight: 600; text-transform: uppercase;
                   letter-spacing: .7px; color: var(--accent); margin-bottom: 12px;
                   padding-bottom: 6px; border-bottom: 1px solid var(--border); }
"""

JS = """
const periodLabels = {daily:'Day', weekly:'Week', monthly:'Month'};
let currentPeriod = 'weekly';
let currentSection = 'growth';

function setPeriod(p) {
  currentPeriod = p;
  document.querySelectorAll('.period-tab').forEach(t => t.classList.remove('active'));
  document.getElementById('ptab-' + p).classList.add('active');
  refreshView();
}

function setSection(s) {
  currentSection = s;
  document.querySelectorAll('.sec-tab').forEach(t => t.classList.remove('active'));
  document.getElementById('stab-' + s).classList.add('active');
  document.querySelectorAll('.section').forEach(el => el.classList.remove('active'));
  document.getElementById('sec-' + s).classList.add('active');
  refreshView();
}

function toggleStores(sectionKey) {
  const rows = document.querySelectorAll('#' + sectionKey + '-store-rows .store-row');
  const arrow = document.getElementById(sectionKey + '-total-row');
  const isOpen = rows[0] && rows[0].classList.contains('visible');
  rows.forEach(r => r.classList.toggle('visible', !isOpen));
  if (arrow) arrow.setAttribute('data-arrow', isOpen ? '▶' : '▼');
}

function refreshView() {
  const d = window.DATA;
  if (!d) return;
  const sec = d[currentSection] && d[currentSection][currentPeriod];
  if (!sec) return;
  // update hero cards
  const hero = document.getElementById('hero-' + currentSection);
  if (hero && window.heroBuilders && window.heroBuilders[currentSection]) {
    hero.innerHTML = window.heroBuilders[currentSection](sec);
  }
  // update tables
  const tbody = document.getElementById('tbody-' + currentSection);
  if (tbody && window.tableBuilders && window.tableBuilders[currentSection]) {
    tbody.innerHTML = window.tableBuilders[currentSection](sec);
  }
  // update period label
  const lbl = document.getElementById('period-lbl-' + currentSection);
  if (lbl) lbl.textContent = 'Showing: ' + periodLabels[currentPeriod] + ' view  |  vs LP = vs last ' + periodLabels[currentPeriod];
}

document.addEventListener('DOMContentLoaded', function() {
  setSection('growth');
  setPeriod('weekly');
});
"""


def hero_card(label: str, value: str, delta_html: str = "") -> str:
    return (
        f'<div class="hero-card">'
        f'<div class="hero-label">{label}</div>'
        f'<div class="hero-value">{value}</div>'
        f'<div class="hero-delta">{delta_html}</div>'
        f"</div>"
    )


def store_dot(name: str) -> str:
    color = STORE_COLORS.get(name, "#94a3b8")
    return f'<span class="store-dot" style="background:{color}"></span>'


def table_row(cells: list, is_store: bool = False, store_name: str = "") -> str:
    cls = 'store-row' if is_store else 'total-row'
    tds = ""
    for i, c in enumerate(cells):
        if is_store and i == 0:
            tds += f"<td>{store_dot(store_name)}{c}</td>"
        else:
            tds += f"<td>{c}</td>"
    return f'<tr class="{cls}">{tds}</tr>'


# ── Section builders ───────────────────────────────────────────────────────────

def build_growth_section(data_by_period: dict) -> str:
    def hero(sec):
        t = sec.get("total", {})
        if not t:
            return '<p class="empty-row">No data available</p>'
        return (
            hero_card("Orders",       t.get("orders","—"),       t.get("orders_dlp","")) +
            hero_card("Buyers",       t.get("buyers","—"),       t.get("buyers_dlp","")) +
            hero_card("GMV",          t.get("gmv","—"),           t.get("gmv_dlp","")) +
            hero_card("Fresh Orders", t.get("fresh_orders","—"), t.get("fresh_dlp","")) +
            hero_card("Fresh Rate",   t.get("fresh_rate","—"),   "")
        )

    def tbody(sec):
        t = sec.get("total", {})
        stores = sec.get("stores", {})
        rows = ""
        # Total row
        if t:
            rows += (
                f'<tr class="total-row" id="growth-total-row" data-arrow="▶" '
                f'onclick="toggleStores(\'growth\')">'
                f'<td data-arrow="▶">Todas las tiendas</td>'
                f'<td>{t.get("orders","—")}</td>'
                f'<td>{t.get("orders_dlp","—")}</td>'
                f'<td>{t.get("buyers","—")}</td>'
                f'<td>{t.get("gmv","—")}</td>'
                f'<td>{t.get("gmv_dlp","—")}</td>'
                f'<td>{t.get("fresh_orders","—")}</td>'
                f'<td>{t.get("fresh_rate","—")}</td>'
                f'<td>{t.get("fresh_dlp","—")}</td>'
                f'<td>{t.get("sparkline","")}</td>'
                f"</tr>"
            )
        else:
            rows += '<tr class="empty-row"><td colspan="10">No data</td></tr>'
        # Store rows
        rows += f'<tbody id="growth-store-rows">'
        for sname in STORE_ORDER:
            s = stores.get(sname, {})
            if s:
                rows += (
                    f'<tr class="store-row">'
                    f'<td>{store_dot(sname)}{sname}</td>'
                    f'<td>{s.get("orders","—")}</td>'
                    f'<td>{s.get("orders_dlp","—")}</td>'
                    f'<td>{s.get("buyers","—")}</td>'
                    f'<td>{s.get("gmv","—")}</td>'
                    f'<td>{s.get("gmv_dlp","—")}</td>'
                    f'<td>{s.get("fresh_orders","—")}</td>'
                    f'<td>{s.get("fresh_rate","—")}</td>'
                    f'<td>{s.get("fresh_dlp","—")}</td>'
                    f'<td>{s.get("sparkline","")}</td>'
                    f"</tr>"
                )
        rows += "</tbody>"
        return rows

    # Build section for current weekly period (default shown on load)
    sec = data_by_period.get("weekly", {})
    return f"""
<div class="hero-grid" id="hero-growth">{hero(sec)}</div>
<div class="section-body">
  <p class="period-label" id="period-lbl-growth"></p>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Store</th>
        <th>Orders</th><th>vs LP</th>
        <th>Buyers</th>
        <th>GMV</th><th>vs LP</th>
        <th>Fresh Orders</th><th>Fresh Rate</th><th>vs LP</th>
        <th>Trend</th>
      </tr></thead>
      <tbody id="tbody-growth">{tbody(sec)}</tbody>
    </table>
  </div>
</div>"""


def build_ops_section(data_by_period: dict) -> str:
    def hero(sec):
        t = sec.get("total", {})
        if not t:
            return '<p class="empty-row">No data</p>'
        return (
            hero_card("Cancel Rate",    t.get("cancel_rate","—"),    t.get("cancel_rate_dlp","")) +
            hero_card("Fill Rate",      t.get("fill_rate","—"),      t.get("fill_rate_dlp","")) +
            hero_card("On-Time Rate",   t.get("ot_rate","—"),        t.get("ot_rate_dlp","")) +
            hero_card("Perfect Purch.", t.get("perfect_rate","—"),   t.get("perfect_rate_dlp",""))
        )

    def tbody(sec):
        t = sec.get("total", {}); stores = sec.get("stores", {})
        def row_cells(d, is_total=False):
            name = "Todas las tiendas" if is_total else ""
            return [
                name,
                d.get("cancel_rate","—"), d.get("cancel_rate_dlp","—"),
                d.get("cancels_seller","—"), d.get("cancels_buyer","—"), d.get("cancels_stockout","—"),
                d.get("fill_rate","—"), d.get("fill_rate_dlp","—"),
                d.get("ot_rate","—"), d.get("ot_early","—"), d.get("ot_delay","—"),
                d.get("perfect_rate","—"), d.get("perfect_rate_dlp","—"),
                d.get("cancel_sparkline",""),
            ]
        rows = ""
        if t:
            cells = row_cells(t, True)
            rows += (
                f'<tr class="total-row" onclick="toggleStores(\'ops\')">'
                + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"
            )
        rows += '<tbody id="ops-store-rows">'
        for sname in STORE_ORDER:
            s = stores.get(sname, {})
            if s:
                cells = row_cells(s)
                cells[0] = sname
                rows += (
                    f'<tr class="store-row">'
                    f'<td>{store_dot(sname)}{sname}</td>'
                    + "".join(f"<td>{c}</td>" for c in cells[1:]) + "</tr>"
                )
        rows += "</tbody>"
        return rows

    sec = data_by_period.get("weekly", {})
    return f"""
<div class="hero-grid" id="hero-ops">{hero(sec)}</div>
<div class="section-body">
  <p class="period-label" id="period-lbl-ops"></p>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Store</th>
        <th>Cancel Rate</th><th>vs LP</th>
        <th>⚠ Seller</th><th>⚠ Buyer</th><th>⚠ Stockout</th>
        <th>Fill Rate</th><th>vs LP</th>
        <th>On-Time</th><th>Early%</th><th>Delay%</th>
        <th>Perfect Purch.</th><th>vs LP</th>
        <th>Cancel Trend</th>
      </tr></thead>
      <tbody id="tbody-ops">{tbody(sec)}</tbody>
    </table>
  </div>
</div>"""


def build_cx_section(data_by_period: dict) -> str:
    def hero(sec):
        t = sec.get("total", {}); b = sec.get("bpp", {})
        if not t and not b:
            return '<p class="empty-row">No data</p>'
        cards = ""
        if t:
            cards += hero_card("Claims Total", t.get("claims_total","—"), t.get("claims_total_dlp",""))
            for ctype, val in (t.get("by_type") or {}).items():
                cards += hero_card(f"Claims {ctype}", val, "")
        if b:
            cards += hero_card("BPP Total", b.get("total","—"), b.get("total_dlp",""))
            cards += hero_card("BPP Recovery", b.get("recovery","—"), "")
        return cards

    def tbody(sec):
        t = sec.get("total", {}); stores = sec.get("stores", {})
        rows = ""
        if t:
            rows += (
                f'<tr class="total-row" onclick="toggleStores(\'cx\')">'
                f'<td data-arrow="▶">Todas las tiendas</td>'
                f'<td>{t.get("claims_total","—")}</td>'
                f'<td>{t.get("claims_total_dlp","—")}</td>'
                + "".join(f'<td>{v}</td>' for v in (t.get("by_type") or {}).values())
                + "</tr>"
            )
        all_types = sorted({k for s in stores.values() for k in (s.get("by_type") or {}).keys()})
        rows += '<tbody id="cx-store-rows">'
        for sname in STORE_ORDER:
            s = stores.get(sname, {})
            if s:
                rows += (
                    f'<tr class="store-row">'
                    f'<td>{store_dot(sname)}{sname}</td>'
                    f'<td>{s.get("claims_total","—")}</td>'
                    f'<td>{s.get("claims_total_dlp","—")}</td>'
                    + "".join(f'<td>{(s.get("by_type") or {}).get(ct,"—")}</td>' for ct in all_types)
                    + "</tr>"
                )
        rows += "</tbody>"
        return rows

    sec = data_by_period.get("weekly", {})
    bpp = sec.get("bpp", {})
    bpp_html = ""
    if bpp:
        bpp_rows = "".join(
            f"<tr><td>{k}</td><td>{v}</td></tr>"
            for k, v in (bpp.get("by_type") or {}).items()
        )
        bpp_html = f"""
<div class="metric-group">
  <h3>BPP — Bad Purchase Protection</h3>
  <div class="table-wrap"><table>
    <thead><tr><th>Type</th><th>Amount</th></tr></thead>
    <tbody>{bpp_rows}</tbody>
  </table></div>
</div>"""

    return f"""
<div class="hero-grid" id="hero-cx">{hero(sec)}</div>
<div class="section-body">
  <p class="period-label" id="period-lbl-cx"></p>
  <div class="metric-group">
    <h3>Claims</h3>
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th>Store</th><th>Total Claims</th><th>vs LP</th>
          <th>By Type →</th>
        </tr></thead>
        <tbody id="tbody-cx">{tbody(sec)}</tbody>
      </table>
    </div>
  </div>
  {bpp_html}
</div>"""


def build_pnl_section(data_by_period: dict) -> str:
    def hero(sec):
        t = sec.get("total", {})
        if not t:
            return '<p class="empty-row">No data</p>'
        return (
            hero_card("GMV",          t.get("gmv","—"),          t.get("gmv_dlp","")) +
            hero_card("NMV",          t.get("nmv","—"),          t.get("nmv_dlp","")) +
            hero_card("Take Rate",    t.get("take_rate","—"),    t.get("take_rate_dlp","")) +
            hero_card("Delivery Fee", t.get("delivery_fee","—"), "") +
            hero_card("Service Fee",  t.get("service_fee","—"),  "") +
            hero_card("Contribution", t.get("contribution","—"), "")
        )

    def tbody(sec):
        t = sec.get("total", {}); stores = sec.get("stores", {})
        rows = ""
        if t:
            rows += (
                f'<tr class="total-row" onclick="toggleStores(\'pnl\')">'
                f'<td data-arrow="▶">Todas las tiendas</td>'
                f'<td>{t.get("gmv","—")}</td><td>{t.get("gmv_dlp","—")}</td>'
                f'<td>{t.get("gmv_dly","—")}</td>'
                f'<td>{t.get("nmv","—")}</td><td>{t.get("nmv_dlp","—")}</td>'
                f'<td>{t.get("take_rate","—")}</td><td>{t.get("take_rate_dlp","—")}</td>'
                f'<td>{t.get("delivery_fee","—")}</td>'
                f'<td>{t.get("service_fee","—")}</td>'
                f'<td>{t.get("contribution","—")}</td>'
                f'<td>{t.get("sparkline","")}</td>'
                f"</tr>"
            )
        rows += '<tbody id="pnl-store-rows">'
        for sname in STORE_ORDER:
            s = stores.get(sname, {})
            if s:
                rows += (
                    f'<tr class="store-row">'
                    f'<td>{store_dot(sname)}{sname}</td>'
                    f'<td>{s.get("gmv","—")}</td><td>{s.get("gmv_dlp","—")}</td>'
                    f'<td>{s.get("gmv_dly","—")}</td>'
                    f'<td>{s.get("nmv","—")}</td><td>{s.get("nmv_dlp","—")}</td>'
                    f'<td>{s.get("take_rate","—")}</td><td>{s.get("take_rate_dlp","—")}</td>'
                    f'<td>{s.get("delivery_fee","—")}</td>'
                    f'<td>{s.get("service_fee","—")}</td>'
                    f'<td>{s.get("contribution","—")}</td>'
                    f'<td>{s.get("sparkline","")}</td>'
                    f"</tr>"
                )
        rows += "</tbody>"
        return rows

    sec = data_by_period.get("weekly", {})
    return f"""
<div class="hero-grid" id="hero-pnl">{hero(sec)}</div>
<div class="section-body">
  <p class="period-label" id="period-lbl-pnl"></p>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Store</th>
        <th>GMV</th><th>vs LP</th><th>vs LY</th>
        <th>NMV</th><th>vs LP</th>
        <th>Take Rate</th><th>vs LP</th>
        <th>Delivery Fee</th>
        <th>Service Fee</th>
        <th>Contribution</th>
        <th>GMV Trend</th>
      </tr></thead>
      <tbody id="tbody-pnl">{tbody(sec)}</tbody>
    </table>
  </div>
</div>"""


# ── JS data embedding ──────────────────────────────────────────────────────────

def build_js_data(all_data: dict) -> str:
    """Serialize the processed data into a JS object for client-side switching."""
    return f"window.DATA = {json.dumps(all_data, ensure_ascii=False, default=str)};"


def build_js_builders() -> str:
    """
    Client-side table/hero rebuilders for period switching.
    These mirror the Python builders but operate on the embedded JS data.
    """
    return """
window.heroBuilders = {
  growth: function(sec) {
    const t = sec.total || {};
    return card('Orders', t.orders, t.orders_dlp)
      + card('Buyers', t.buyers, t.buyers_dlp)
      + card('GMV', t.gmv, t.gmv_dlp)
      + card('Fresh Orders', t.fresh_orders, t.fresh_dlp)
      + card('Fresh Rate', t.fresh_rate, '');
  },
  ops: function(sec) {
    const t = sec.total || {};
    return card('Cancel Rate', t.cancel_rate, t.cancel_rate_dlp)
      + card('Fill Rate', t.fill_rate, t.fill_rate_dlp)
      + card('On-Time', t.ot_rate, t.ot_rate_dlp)
      + card('Perfect Purch.', t.perfect_rate, t.perfect_rate_dlp);
  },
  cx: function(sec) {
    const t = sec.total || {}; const b = sec.bpp || {};
    let h = card('Claims Total', t.claims_total, t.claims_total_dlp);
    if (t.by_type) Object.entries(t.by_type).forEach(([k,v]) => h += card('Claims '+k, v, ''));
    if (b.total) h += card('BPP Total', b.total, b.total_dlp);
    return h;
  },
  pnl: function(sec) {
    const t = sec.total || {};
    return card('GMV', t.gmv, t.gmv_dlp)
      + card('NMV', t.nmv, t.nmv_dlp)
      + card('Take Rate', t.take_rate, t.take_rate_dlp)
      + card('Delivery Fee', t.delivery_fee, '')
      + card('Service Fee', t.service_fee, '')
      + card('Contribution', t.contribution, '');
  }
};

window.tableBuilders = {
  growth: function(sec) {
    const t = sec.total || {}; const s = sec.stores || {};
    let rows = rowTotal('growth', [t.orders,'',t.orders_dlp, t.buyers, t.gmv,'',t.gmv_dlp,
                                    t.fresh_orders, t.fresh_rate,'',t.fresh_dlp, t.sparkline||'']);
    rows += '<tbody id="growth-store-rows">';
    storeOrder.forEach(name => {
      const d = s[name] || {};
      rows += rowStore(name, [d.orders,'',d.orders_dlp, d.buyers, d.gmv,'',d.gmv_dlp,
                               d.fresh_orders, d.fresh_rate,'',d.fresh_dlp, d.sparkline||'']);
    });
    return rows + '</tbody>';
  },
  ops: function(sec) {
    const t = sec.total || {}; const s = sec.stores || {};
    let rows = rowTotal('ops', [t.cancel_rate,'',t.cancel_rate_dlp,
                                 t.cancels_seller, t.cancels_buyer, t.cancels_stockout,
                                 t.fill_rate,'',t.fill_rate_dlp,
                                 t.ot_rate, t.ot_early, t.ot_delay,
                                 t.perfect_rate,'',t.perfect_rate_dlp,
                                 t.cancel_sparkline||'']);
    rows += '<tbody id="ops-store-rows">';
    storeOrder.forEach(name => {
      const d = s[name] || {};
      rows += rowStore(name, [d.cancel_rate,'',d.cancel_rate_dlp,
                               d.cancels_seller, d.cancels_buyer, d.cancels_stockout,
                               d.fill_rate,'',d.fill_rate_dlp,
                               d.ot_rate, d.ot_early, d.ot_delay,
                               d.perfect_rate,'',d.perfect_rate_dlp,
                               d.cancel_sparkline||'']);
    });
    return rows + '</tbody>';
  },
  cx: function(sec) {
    const t = sec.total || {}; const s = sec.stores || {};
    let rows = rowTotal('cx', [t.claims_total,'',t.claims_total_dlp]);
    rows += '<tbody id="cx-store-rows">';
    storeOrder.forEach(name => {
      const d = s[name] || {};
      rows += rowStore(name, [d.claims_total,'',d.claims_total_dlp]);
    });
    return rows + '</tbody>';
  },
  pnl: function(sec) {
    const t = sec.total || {}; const s = sec.stores || {};
    let rows = rowTotal('pnl', [t.gmv,'',t.gmv_dlp,t.gmv_dly,
                                  t.nmv,'',t.nmv_dlp,
                                  t.take_rate,'',t.take_rate_dlp,
                                  t.delivery_fee, t.service_fee, t.contribution,
                                  t.sparkline||'']);
    rows += '<tbody id="pnl-store-rows">';
    storeOrder.forEach(name => {
      const d = s[name] || {};
      rows += rowStore(name, [d.gmv,'',d.gmv_dlp,d.gmv_dly,
                               d.nmv,'',d.nmv_dlp,
                               d.take_rate,'',d.take_rate_dlp,
                               d.delivery_fee, d.service_fee, d.contribution,
                               d.sparkline||'']);
    });
    return rows + '</tbody>';
  }
};

const storeOrder = ['Scalabrini','Caballito','Villa Urquiza','Vicente Lopez'];
const storeColors = {
  'Scalabrini':'#3b82f6','Caballito':'#f59e0b',
  'Villa Urquiza':'#10b981','Vicente Lopez':'#a855f7'
};

function card(label, value, delta) {
  return '<div class="hero-card"><div class="hero-label">'+label+'</div>'
       + '<div class="hero-value">'+(value||'—')+'</div>'
       + '<div class="hero-delta">'+(delta||'')+'</div></div>';
}
function rowTotal(sec, cells) {
  return '<tr class="total-row" onclick="toggleStores(\''+sec+'\')">'
       + '<td data-arrow="▶">Todas las tiendas</td>'
       + cells.map(c => '<td>'+(c===''?'':c||'—')+'</td>').join('') + '</tr>';
}
function rowStore(name, cells) {
  const dot = '<span class="store-dot" style="background:'+(storeColors[name]||'#94a3b8')+'"></span>';
  return '<tr class="store-row">'
       + '<td>'+dot+name+'</td>'
       + cells.map(c => '<td>'+(c===''?'':c||'—')+'</td>').join('') + '</tr>';
}
"""


# ── Main assembly ──────────────────────────────────────────────────────────────

def build_html(meta_json: str) -> str:
    try:
        meta = json.loads(meta_json)
        updated_at = meta.get("fetched_at", "unknown")
    except Exception:
        updated_at = "unknown"

    periods = ["daily", "weekly", "monthly"]
    sections_map = {
        "growth": process_growth,
        "ops":    process_ops,
        "cx":     process_cx,
        "pnl":    process_pnl,
    }

    # Build all section data for all periods
    all_data: dict = {}
    for sec_key, func in sections_map.items():
        all_data[sec_key] = {}
        for p in periods:
            print(f"  processing {sec_key}/{p}…")
            all_data[sec_key][p] = func(p)

    # Build section HTML (initial render = weekly)
    growth_html = build_growth_section(all_data["growth"])
    ops_html    = build_ops_section(all_data["ops"])
    cx_html     = build_cx_section(all_data["cx"])
    pnl_html    = build_pnl_section(all_data["pnl"])

    js_data     = build_js_data(all_data)
    js_builders = build_js_builders()

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Proximity Groceries Scorecard</title>
<style>{CSS}</style>
</head>
<body>

<header class="header">
  <h1>Proximity <span>Groceries</span> Scorecard</h1>
  <span class="updated">Updated: {updated_at}</span>
</header>

<div class="controls">
  <div class="tab-group">
    <button class="tab period-tab active" id="ptab-daily"   onclick="setPeriod('daily')">Daily</button>
    <button class="tab period-tab active" id="ptab-weekly"  onclick="setPeriod('weekly')">Weekly</button>
    <button class="tab period-tab"        id="ptab-monthly" onclick="setPeriod('monthly')">Monthly</button>
  </div>
</div>

<nav class="section-nav">
  <button class="sec-tab active" id="stab-growth" onclick="setSection('growth')">📈 Growth</button>
  <button class="sec-tab"        id="stab-ops"    onclick="setSection('ops')">⚙️  Ops</button>
  <button class="sec-tab"        id="stab-cx"     onclick="setSection('cx')">💬 CX</button>
  <button class="sec-tab"        id="stab-pnl"    onclick="setSection('pnl')">💰 P&amp;L</button>
</nav>

<div id="sec-growth" class="section active">{growth_html}</div>
<div id="sec-ops"    class="section">{ops_html}</div>
<div id="sec-cx"     class="section">{cx_html}</div>
<div id="sec-pnl"    class="section">{pnl_html}</div>

<script>
{js_data}
{js_builders}
{JS}
</script>
</body>
</html>"""
    return html


def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Generating dashboard…\n")

    meta_path = os.path.join(DATA_DIR, "meta.json")
    meta_json = "{}"
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta_json = f.read()

    html = build_html(meta_json)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✓ Dashboard written to {OUTPUT_FILE} ({len(html):,} bytes)")


if __name__ == "__main__":
    main()
