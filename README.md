# Financial Health Tracker

An end-to-end data analytics project analyzing the financial health of 25 major public companies across 8 sectors using Python, PostgreSQL, and Power BI.

---

## Dashboard Preview

> *Add a screenshot of your Power BI dashboard here*
> `<img width="1127" height="637" alt="dashboard_preview" src="https://github.com/user-attachments/assets/40f24e94-8ee5-4148-a04c-0a929aa564d3" />
`

---

## Project Overview

This project builds a complete analytics pipeline — from raw data collection to an interactive executive dashboard — answering three core business questions:

- **Which companies and sectors are most profitable?**
- **How has revenue grown over the past 4 years?**
- **Which companies carry the highest financial risk?**

---

## Tech Stack

| Layer | Tools |
|---|---|
| Data collection | Python, `yfinance` |
| Data cleaning & KPI engineering | Python, `pandas` |
| Data storage & analysis | PostgreSQL, SQL (CTEs, window functions) |
| Visualization | Power BI (DAX, data modeling) |
| Version control | Git, GitHub |

---

## Dataset

- **Source:** Yahoo Finance via `yfinance` API
- **Coverage:** 25 companies across 8 sectors (Technology, Finance, Healthcare, Energy, Consumer, Telecom, Retail, Industrial)
- **Period:** 2021–2025 (4 years of annual financials)
- **Volume:** 100 rows × 40 columns after cleaning

**Companies included:**
Apple, Microsoft, Alphabet, Meta, NVIDIA, Amazon, Salesforce, JPMorgan Chase, Goldman Sachs, Bank of America, Pfizer, Johnson & Johnson, Merck, ExxonMobil, Chevron, Procter & Gamble, Coca-Cola, McDonald's, Tesla, Ford, Verizon, AT&T, Walmart, Target, Caterpillar

---

## Project Structure

```
financial-health-tracker/
│
├── data/
│   ├── raw/                    ← yfinance CSVs (income, balance, cashflow, info)
│   └── processed/
│       └── financials_clean.csv
│
├── scripts/
│   ├── 01_collect.py           ← Pulls data from Yahoo Finance API
│   ├── 02_clean.py             ← Cleans data and engineers KPIs
│   └── 03_load_db.py           ← Loads data into PostgreSQL
│
├── sql/
│   └── analysis.sql            ← Window function queries and KPI analysis
│
├── assets/
│   └── dashboard_preview.png   ← Dashboard screenshot
│
├── requirements.txt
└── README.md
```

---

## Pipeline

```
yfinance API → 01_collect.py → data/raw/
             → 02_clean.py  → data/processed/financials_clean.csv
             → 03_load_db.py → PostgreSQL
             → analysis.sql  → Views & KPI queries
             → Power BI      → Interactive dashboard
```

---

## KPIs Engineered

**Profitability**
- Gross margin, operating margin, net margin, EBITDA margin
- Return on equity (ROE), return on assets (ROA)

**Liquidity**
- Current ratio, cash ratio

**Leverage**
- Debt-to-equity, debt-to-assets

**Cash Quality**
- Free cash flow margin, cash conversion ratio

**Growth**
- Year-over-year revenue growth, net income growth, EPS growth

---

## SQL Highlights

The `analysis.sql` file demonstrates advanced SQL including:

```sql
-- Rank companies by profitability within each sector
RANK() OVER (PARTITION BY sector ORDER BY net_margin DESC)

-- Year-over-year revenue growth with cumulative total
SUM(revenue) OVER (PARTITION BY symbol ORDER BY fiscal_year ROWS UNBOUNDED PRECEDING)

-- Composite financial health score using quartile buckets
NTILE(4) OVER (ORDER BY net_margin)

-- Flag companies with debt exceeding 2x their sector average
debt_to_equity / AVG(debt_to_equity) OVER (PARTITION BY sector) AS ratio_vs_sector
```

---

## Power BI Dashboard

The dashboard has 3 report pages:

**Page 1 — Executive Summary**
Total revenue, avg net margin, avg ROE, high risk count. Revenue by company, revenue share by sector, revenue trend over time, company snapshot table.

**Page 2 — Profitability Analysis**
Gross/net/FCF margin comparison, sector avg net margin, revenue vs net margin scatter plot, revenue growth trend by sector.

**Page 3 — Financial Health & Risk**
Debt-to-equity by company (conditional red/amber/green), financial health score ranking, liquidity vs leverage scatter plot, risk summary table.

---

## Key Findings

**1. Technology dominates profitability**
The Technology sector leads with an average net margin of 29.2%, nearly double the dataset average of 18.8%. NVIDIA and Meta are standout performers driven by AI-related revenue growth.

**2. Ford and Goldman Sachs carry the highest leverage risk**
Both companies have debt-to-equity ratios exceeding 2x their sector averages, flagging them as high risk. This is partly structural — auto manufacturers and investment banks typically operate with high leverage — but warrants monitoring.

**3. Consumer Defensive sector shows the most consistent growth**
Despite lower absolute margins, companies like Walmart, Coca-Cola, and Procter & Gamble show the most stable year-over-year revenue growth, making them lower-risk holdings.

---

## How to Run

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/financial-health-tracker.git
cd financial-health-tracker
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Collect data
```bash
python scripts/01_collect.py
```

### 4. Clean and engineer KPIs
```bash
python scripts/02_clean.py
```

### 5. Set up PostgreSQL
Create a database named `financial_tracker`, then edit the credentials in `03_load_db.py`:
```python
DB_PASSWORD = "your_password"
```

### 6. Load to database
```bash
python scripts/03_load_db.py
```

### 7. Run SQL analysis
```bash
psql -U postgres -d financial_tracker -f sql/analysis.sql
```

### 8. Open Power BI
Connect Power BI Desktop to `localhost/financial_tracker` and open the `.pbix` file.

---

## Requirements

```
yfinance
pandas
sqlalchemy
psycopg2-binary
```

---

## Author

**Ahmed Hamdi Hegazi**
Computer Science graduate — British University in Egypt
[LinkedIn](https://linkedin.com/in/YOUR_PROFILE) · [Portfolio](YOUR_PORTFOLIO_URL)
