"""
Phase 2 — Data Cleaning & KPI Engineering
Financial Health Tracker

Reads raw CSVs from data/raw/, cleans and validates them,
calculates financial KPIs, and saves a single master dataset
to data/processed/financials_clean.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

RAW_DIR       = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Columns we want from each statement (yfinance names)
INCOME_COLS = {
    "Total Revenue":              "revenue",
    "Net Income":                 "net_income",
    "Operating Income":           "operating_income",
    "Gross Profit":               "gross_profit",
    "Basic EPS":                  "eps",
    "EBITDA":                     "ebitda",   # yfinance returns this in all-caps
}

BALANCE_COLS = {
    "Total Assets":               "total_assets",
    "Total Liabilities Net Minority Interest": "total_liabilities",
    "Stockholders Equity":        "equity",
    "Current Assets":             "current_assets",
    "Current Liabilities":        "current_liabilities",
    "Long Term Debt":             "long_term_debt",
    "Cash And Cash Equivalents":  "cash",
}

CASHFLOW_COLS = {
    "Operating Cash Flow":        "operating_cashflow",
    "Free Cash Flow":             "free_cashflow",
    "Capital Expenditure":        "capex",
}


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_and_select(filename: str, col_map: dict) -> pd.DataFrame:
    """Load a raw CSV, keep only columns in col_map, rename them."""
    path = RAW_DIR / filename
    df = pd.read_csv(path, parse_dates=["fiscal_year"])

    # Keep only columns that actually exist in the file
    available = {k: v for k, v in col_map.items() if k in df.columns}
    missing   = set(col_map.keys()) - set(available.keys())
    if missing:
        print(f"  [info] {filename}: columns not found — {missing}")

    df = df[["symbol", "fiscal_year"] + list(available.keys())].copy()
    df = df.rename(columns=available)
    return df


# ── Cleaning helpers ──────────────────────────────────────────────────────────

def clean_numeric(df: pd.DataFrame, exclude=("symbol", "fiscal_year")) -> pd.DataFrame:
    """Coerce all non-key columns to numeric, replacing errors with NaN."""
    num_cols = [c for c in df.columns if c not in exclude]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df


def drop_near_empty_rows(df: pd.DataFrame, threshold=0.6) -> pd.DataFrame:
    """Drop rows where more than `threshold` fraction of numeric cols are NaN."""
    num_cols = [c for c in df.columns if c not in ("symbol", "fiscal_year")]
    null_frac = df[num_cols].isnull().mean(axis=1)
    before = len(df)
    df = df[null_frac < threshold].copy()
    dropped = before - len(df)
    if dropped:
        print(f"  [clean] dropped {dropped} near-empty rows")
    return df


def extract_year(df: pd.DataFrame) -> pd.DataFrame:
    """Add integer fiscal_year_int column for easier SQL joins."""
    df["fiscal_year_int"] = df["fiscal_year"].dt.year
    return df


# ── KPI engineering ───────────────────────────────────────────────────────────

def add_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate financial KPIs from the merged dataset."""

    # ── Profitability ─────────────────────────────────────────────────────────
    df["gross_margin"]     = df["gross_profit"]     / df["revenue"]
    df["operating_margin"] = df["operating_income"] / df["revenue"]
    df["net_margin"]       = df["net_income"]        / df["revenue"]
    df["ebitda_margin"]    = df["ebitda"] / df["revenue"] if "ebitda" in df.columns else np.nan

    # Return on equity (ROE)
    df["roe"] = df["net_income"] / df["equity"]

    # Return on assets (ROA)
    df["roa"] = df["net_income"] / df["total_assets"]

    # ── Liquidity ─────────────────────────────────────────────────────────────
    df["current_ratio"] = df["current_assets"] / df["current_liabilities"]
    df["cash_ratio"]    = df["cash"]            / df["current_liabilities"]

    # ── Leverage ──────────────────────────────────────────────────────────────
    df["debt_to_equity"]  = df["long_term_debt"] / df["equity"]
    df["debt_to_assets"]  = df["total_liabilities"] / df["total_assets"]

    # ── Cash quality ──────────────────────────────────────────────────────────
    # FCF margin = how much of every revenue dollar becomes free cash
    df["fcf_margin"] = df["free_cashflow"] / df["revenue"]

    # Cash conversion: operating cash vs net income (>1 = high quality earnings)
    df["cash_conversion"] = df["operating_cashflow"] / df["net_income"]

    # ── Growth (YoY) — requires sorting by company + year ────────────────────
    df = df.sort_values(["symbol", "fiscal_year"])

    for metric, col_name in [
        ("revenue",    "revenue_growth"),
        ("net_income", "net_income_growth"),
        ("eps",        "eps_growth"),
    ]:
        if metric in df.columns:
            df[col_name] = (
                df.groupby("symbol")[metric]
                .pct_change()          # (current - prior) / prior
            )

    return df


# ── Validation report ─────────────────────────────────────────────────────────

def validation_report(df: pd.DataFrame):
    """Print a quick data quality summary."""
    print("\n── Validation Report ────────────────────────────────────────")
    print(f"  Rows:      {len(df)}")
    print(f"  Companies: {df['symbol'].nunique()}")
    print(f"  Years:     {sorted(df['fiscal_year_int'].unique())}")

    num_cols = [c for c in df.columns if c not in ("symbol", "fiscal_year", "fiscal_year_int",
                                                     "sector", "industry", "country", "name", "currency")]
    null_pct = df[num_cols].isnull().mean().sort_values(ascending=False)
    high_null = null_pct[null_pct > 0.1]
    if not high_null.empty:
        print("\n  Columns with >10% nulls:")
        for col, pct in high_null.items():
            print(f"    {col:<30} {pct:.1%}")
    else:
        print("  All numeric columns have ≤10% nulls ✓")

    # Sanity checks
    issues = []
    if (df["revenue"] < 0).any():
        issues.append("Negative revenue values found")
    if (df["current_ratio"] < 0).any():
        issues.append("Negative current ratio found")
    if df["net_margin"].abs().gt(5).any():
        issues.append("Net margin > 500% or < -500% — check outliers")

    if issues:
        print("\n  Sanity check warnings:")
        for i in issues:
            print(f"    ⚠  {i}")
    else:
        print("  Sanity checks passed ✓")
    print("─────────────────────────────────────────────────────────────\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading raw data ...")

    income   = load_and_select("income_statements.csv", INCOME_COLS)
    balance  = load_and_select("balance_sheets.csv",    BALANCE_COLS)
    cashflow = load_and_select("cash_flows.csv",        CASHFLOW_COLS)
    info     = pd.read_csv(RAW_DIR / "company_info.csv")

    print(f"  income:   {income.shape}")
    print(f"  balance:  {balance.shape}")
    print(f"  cashflow: {cashflow.shape}")
    print(f"  info:     {info.shape}")

    # ── Merge all three financial statements on symbol + fiscal_year ──────────
    print("\nMerging statements ...")
    merged = (
        income
        .merge(balance,  on=["symbol", "fiscal_year"], how="outer")
        .merge(cashflow, on=["symbol", "fiscal_year"], how="outer")
        .merge(info,     on="symbol",                  how="left")
    )

    # ── Clean ─────────────────────────────────────────────────────────────────
    print("Cleaning ...")
    merged = clean_numeric(merged, exclude=("symbol", "fiscal_year", "name",
                                             "sector", "industry", "country", "currency"))
    merged = drop_near_empty_rows(merged, threshold=0.6)
    merged = extract_year(merged)

    # ── KPIs ──────────────────────────────────────────────────────────────────
    print("Calculating KPIs ...")
    merged = add_kpis(merged)

    # Round all float columns to 4 decimal places for readability
    float_cols = merged.select_dtypes(include="float").columns
    merged[float_cols] = merged[float_cols].round(4)

    # ── Validate ──────────────────────────────────────────────────────────────
    validation_report(merged)

    # ── Save ──────────────────────────────────────────────────────────────────
    out_path = PROCESSED_DIR / "financials_clean.csv"
    merged.to_csv(out_path, index=False)
    print(f"Saved → {out_path}  ({len(merged)} rows, {merged.shape[1]} columns)")
    print("\nPhase 2 complete. Next step: run scripts/03_load_db.py")


if __name__ == "__main__":
    main()