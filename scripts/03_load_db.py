"""
Phase 3 — Load to PostgreSQL
Financial Health Tracker

Creates the schema, loads the cleaned CSV into PostgreSQL,
and verifies row counts.

Prerequisites:
  pip install psycopg2-binary sqlalchemy
  A running PostgreSQL instance with a database already created.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path

# ── Configuration — edit these to match your PostgreSQL setup ─────────────────

DB_USER     = "postgres"
DB_PASSWORD = "ahah2003"       
DB_HOST     = "localhost"
DB_PORT     = "5432"
DB_NAME     = "financial_tracker"   

CLEAN_CSV   = Path("data/processed/financials_clean.csv")

# ─────────────────────────────────────────────────────────────────────────────


def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


# ── Schema DDL ────────────────────────────────────────────────────────────────

CREATE_SCHEMA = """
-- Drop and recreate for a clean load
DROP TABLE IF EXISTS financials CASCADE;
DROP TABLE IF EXISTS companies  CASCADE;

-- companies: one row per ticker
CREATE TABLE companies (
    symbol      VARCHAR(10)  PRIMARY KEY,
    name        VARCHAR(100),
    sector      VARCHAR(100),
    industry    VARCHAR(150),
    country     VARCHAR(50),
    currency    VARCHAR(10),
    market_cap  BIGINT
);

-- financials: one row per ticker per fiscal year
CREATE TABLE financials (
    id                  SERIAL PRIMARY KEY,
    symbol              VARCHAR(10)  REFERENCES companies(symbol),
    fiscal_year         DATE,
    fiscal_year_int     SMALLINT,

    -- Income statement
    revenue             BIGINT,
    net_income          BIGINT,
    operating_income    BIGINT,
    gross_profit        BIGINT,
    eps                 NUMERIC(12, 4),
    ebitda              BIGINT,

    -- Balance sheet
    total_assets        BIGINT,
    total_liabilities   BIGINT,
    equity              BIGINT,
    current_assets      BIGINT,
    current_liabilities BIGINT,
    long_term_debt      BIGINT,
    cash                BIGINT,

    -- Cash flow
    operating_cashflow  BIGINT,
    free_cashflow       BIGINT,
    capex               BIGINT,

    -- Profitability KPIs
    gross_margin        NUMERIC(8, 4),
    operating_margin    NUMERIC(8, 4),
    net_margin          NUMERIC(8, 4),
    ebitda_margin       NUMERIC(8, 4),
    roe                 NUMERIC(8, 4),
    roa                 NUMERIC(8, 4),

    -- Liquidity KPIs
    current_ratio       NUMERIC(8, 4),
    cash_ratio          NUMERIC(8, 4),

    -- Leverage KPIs
    debt_to_equity      NUMERIC(8, 4),
    debt_to_assets      NUMERIC(8, 4),

    -- Cash quality KPIs
    fcf_margin          NUMERIC(8, 4),
    cash_conversion     NUMERIC(8, 4),

    -- Growth KPIs
    revenue_growth      NUMERIC(8, 4),
    net_income_growth   NUMERIC(8, 4),
    eps_growth          NUMERIC(8, 4)
);

-- Indexes for common query patterns
CREATE INDEX idx_financials_symbol      ON financials(symbol);
CREATE INDEX idx_financials_year        ON financials(fiscal_year_int);
CREATE INDEX idx_financials_symbol_year ON financials(symbol, fiscal_year_int);
"""


# ── Load helpers ──────────────────────────────────────────────────────────────

COMPANY_COLS = ["symbol", "name", "sector", "industry", "country", "currency", "market_cap"]

FINANCIAL_COLS = [
    "symbol", "fiscal_year", "fiscal_year_int",
    "revenue", "net_income", "operating_income", "gross_profit", "eps", "ebitda",
    "total_assets", "total_liabilities", "equity",
    "current_assets", "current_liabilities", "long_term_debt", "cash",
    "operating_cashflow", "free_cashflow", "capex",
    "gross_margin", "operating_margin", "net_margin", "ebitda_margin", "roe", "roa",
    "current_ratio", "cash_ratio",
    "debt_to_equity", "debt_to_assets",
    "fcf_margin", "cash_conversion",
    "revenue_growth", "net_income_growth", "eps_growth",
]

BIGINT_COLS = [
    "revenue", "net_income", "operating_income", "gross_profit", "ebitda",
    "total_assets", "total_liabilities", "equity",
    "current_assets", "current_liabilities", "long_term_debt", "cash",
    "operating_cashflow", "free_cashflow", "capex", "market_cap",
]


def prepare_companies(df: pd.DataFrame) -> pd.DataFrame:
    """One row per symbol with company metadata."""
    available = [c for c in COMPANY_COLS if c in df.columns]
    companies = df[available].drop_duplicates(subset="symbol").copy()
    # market_cap can be float from yfinance — cast to nullable Int64
    if "market_cap" in companies.columns:
        companies["market_cap"] = pd.to_numeric(companies["market_cap"], errors="coerce")
        companies["market_cap"] = companies["market_cap"].astype("Int64")
    return companies


def prepare_financials(df: pd.DataFrame) -> pd.DataFrame:
    """Select and type-cast financial columns."""
    available = [c for c in FINANCIAL_COLS if c in df.columns]
    fin = df[available].copy()

    # Cast large dollar amounts to nullable Int64 (handles NaN without crashing)
    for col in BIGINT_COLS:
        if col in fin.columns:
            fin[col] = pd.to_numeric(fin[col], errors="coerce").astype("Int64")

    fin["fiscal_year"] = pd.to_datetime(fin["fiscal_year"])
    return fin


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Connecting to PostgreSQL ...")
    engine = get_engine()

    # Test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("  Connected ✓")

    # ── Create schema ─────────────────────────────────────────────────────────
    print("Creating schema ...")
    with engine.connect() as conn:
        conn.execute(text(CREATE_SCHEMA))
        conn.commit()
    print("  Tables created ✓")

    # ── Load CSV ──────────────────────────────────────────────────────────────
    print(f"Reading {CLEAN_CSV} ...")
    df = pd.read_csv(CLEAN_CSV, parse_dates=["fiscal_year"])
    print(f"  {len(df)} rows loaded")

    # ── Insert companies ──────────────────────────────────────────────────────
    companies = prepare_companies(df)
    companies.to_sql("companies", engine, if_exists="append", index=False)
    print(f"  Inserted {len(companies)} companies ✓")

    # ── Insert financials ─────────────────────────────────────────────────────
    financials = prepare_financials(df)
    financials.to_sql("financials", engine, if_exists="append", index=False, chunksize=50)
    print(f"  Inserted {len(financials)} financial rows ✓")

    # ── Verify ────────────────────────────────────────────────────────────────
    print("\nVerification:")
    with engine.connect() as conn:
        for table in ("companies", "financials"):
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"  {table}: {count} rows")

        # Quick sample
        result = conn.execute(text("""
            SELECT c.name, f.fiscal_year_int, f.revenue, f.net_margin
            FROM financials f
            JOIN companies c USING (symbol)
            WHERE c.symbol = 'AAPL'
            ORDER BY f.fiscal_year_int
        """))
        print("\n  Apple sample:")
        for row in result:
            rev_b = f"${row.revenue/1e9:.1f}B" if row.revenue else "N/A"
            margin = f"{row.net_margin*100:.1f}%" if row.net_margin else "N/A"
            print(f"    {row.fiscal_year_int}  revenue={rev_b}  net_margin={margin}")

    print("\nPhase 3 complete. Next step: run sql/analysis.sql in pgAdmin or psql")


if __name__ == "__main__":
    main()