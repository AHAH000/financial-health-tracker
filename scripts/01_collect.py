"""
Phase 1 — Data Collection
Financial Health Tracker
Pulls 4 years of annual financials for 25 companies using yfinance
and saves raw CSVs to data/raw/
"""

import yfinance as yf
import pandas as pd
from pathlib import Path
import time

# ── Configuration ────────────────────────────────────────────────────────────

OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

COMPANIES = {
    # Technology
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "GOOGL": "Alphabet",
    "META": "Meta",
    "NVDA": "NVIDIA",
    # E-commerce / Cloud
    "AMZN": "Amazon",
    "CRM":  "Salesforce",
    # Finance
    "JPM":  "JPMorgan Chase",
    "GS":   "Goldman Sachs",
    "BAC":  "Bank of America",
    # Healthcare / Pharma
    "PFE":  "Pfizer",
    "JNJ":  "Johnson & Johnson",
    "MRK":  "Merck",
    # Energy
    "XOM":  "ExxonMobil",
    "CVX":  "Chevron",
    # Consumer
    "PG":   "Procter & Gamble",
    "KO":   "Coca-Cola",
    "MCD":  "McDonald's",
    # Automotive / EV
    "TSLA": "Tesla",
    "F":    "Ford",
    # Telecom
    "VZ":   "Verizon",
    "T":    "AT&T",
    # Retail
    "WMT":  "Walmart",
    "TGT":  "Target",
    # Industrial
    "CAT":  "Caterpillar",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_get(ticker_obj, attr):
    """Return a DataFrame attribute or an empty DataFrame on failure."""
    try:
        df = getattr(ticker_obj, attr)
        if df is None or df.empty:
            return pd.DataFrame()
        return df.T  # transpose: dates become rows, metrics become columns
    except Exception as e:
        print(f"    [warn] could not fetch {attr}: {e}")
        return pd.DataFrame()


def collect_company(symbol: str, name: str) -> dict[str, pd.DataFrame]:
    """Fetch income statement, balance sheet, cash flow, and info for one ticker."""
    ticker = yf.Ticker(symbol)

    income    = safe_get(ticker, "financials")        # annual income statement
    balance   = safe_get(ticker, "balance_sheet")     # annual balance sheet
    cashflow  = safe_get(ticker, "cashflow")           # annual cash flow

    # Basic info (sector, industry, market cap, etc.)
    try:
        info = ticker.info
        info_df = pd.DataFrame([{
            "symbol":      symbol,
            "name":        name,
            "sector":      info.get("sector", "Unknown"),
            "industry":    info.get("industry", "Unknown"),
            "country":     info.get("country", "Unknown"),
            "market_cap":  info.get("marketCap"),
            "currency":    info.get("financialCurrency", "USD"),
        }])
    except Exception as e:
        print(f"    [warn] could not fetch info: {e}")
        info_df = pd.DataFrame([{"symbol": symbol, "name": name}])

    return {
        "income":   income,
        "balance":  balance,
        "cashflow": cashflow,
        "info":     info_df,
    }


def add_symbol_column(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Insert symbol as first column and reset index to fiscal_year."""
    if df.empty:
        return df
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    df.index.name = "fiscal_year"
    df = df.reset_index()
    df.insert(0, "symbol", symbol)
    return df


# ── Main collection loop ──────────────────────────────────────────────────────

def main():
    all_income   = []
    all_balance  = []
    all_cashflow = []
    all_info     = []

    total = len(COMPANIES)
    for i, (symbol, name) in enumerate(COMPANIES.items(), 1):
        print(f"[{i:>2}/{total}] Fetching {symbol:5s} — {name} ...")
        data = collect_company(symbol, name)

        all_income.append(add_symbol_column(data["income"],   symbol))
        all_balance.append(add_symbol_column(data["balance"],  symbol))
        all_cashflow.append(add_symbol_column(data["cashflow"], symbol))
        all_info.append(data["info"])

        # Be polite to the API — avoid rate limiting
        time.sleep(1.2)

    # ── Combine and save ──────────────────────────────────────────────────────
    print("\nSaving raw CSVs ...")

    dfs = {
        "income_statements": all_income,
        "balance_sheets":    all_balance,
        "cash_flows":        all_cashflow,
        "company_info":      all_info,
    }

    for filename, frames in dfs.items():
        combined = pd.concat([f for f in frames if not f.empty], ignore_index=True)
        out_path = OUTPUT_DIR / f"{filename}.csv"
        combined.to_csv(out_path, index=False)
        print(f"  ✓ {out_path}  ({len(combined)} rows, {combined['symbol'].nunique()} companies)")

    print("\nPhase 1 complete. Check data/raw/ for your CSVs.")
    print("Next step: run scripts/02_clean.py")


if __name__ == "__main__":
    main()