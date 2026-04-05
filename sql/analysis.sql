-- ─────────────────────────────────────────────────────────────────────────────
-- sql/analysis.sql
-- Financial Health Tracker — KPI Analysis Queries
-- Run these in pgAdmin or psql after 03_load_db.py completes
-- ─────────────────────────────────────────────────────────────────────────────


-- ── 1. Overview: latest year snapshot per company ────────────────────────────
-- Good first check — shows every company's most recent financials

CREATE OR REPLACE VIEW v_latest_snapshot AS
SELECT
    c.name,
    c.sector,
    f.fiscal_year_int                          AS year,
    ROUND(f.revenue        / 1e9::NUMERIC, 2)  AS revenue_b,
    ROUND(f.net_margin     * 100, 2)           AS net_margin_pct,
    ROUND(f.roe            * 100, 2)           AS roe_pct,
    ROUND(f.current_ratio,        2)           AS current_ratio,
    ROUND(f.debt_to_equity,       2)           AS debt_to_equity,
    ROUND(f.revenue_growth * 100, 2)           AS revenue_growth_pct
FROM financials f
JOIN companies c USING (symbol)
WHERE f.fiscal_year_int = (
    SELECT MAX(fiscal_year_int) FROM financials f2
    WHERE f2.symbol = f.symbol
)
ORDER BY f.revenue DESC;

SELECT * FROM v_latest_snapshot;


-- ── 2. Profitability ranking by sector (latest year) ─────────────────────────
-- Uses RANK() window function to rank companies within each sector

SELECT
    c.sector,
    c.name,
    f.fiscal_year_int                                   AS year,
    ROUND(f.net_margin * 100, 2)                        AS net_margin_pct,
    RANK() OVER (
        PARTITION BY c.sector
        ORDER BY f.net_margin DESC NULLS LAST
    )                                                   AS sector_rank,
    ROUND(AVG(f.net_margin) OVER (
        PARTITION BY c.sector
    ) * 100, 2)                                         AS sector_avg_margin_pct
FROM financials f
JOIN companies c USING (symbol)
WHERE f.fiscal_year_int = (
    SELECT MAX(fiscal_year_int) FROM financials f2
    WHERE f2.symbol = f.symbol
)
ORDER BY c.sector, sector_rank;


-- ── 3. Year-over-year revenue growth with running total ──────────────────────
-- Uses LAG() to calculate growth and SUM() for cumulative revenue

SELECT
    c.name,
    f.fiscal_year_int                                               AS year,
    ROUND(f.revenue / 1e9::NUMERIC, 2)                             AS revenue_b,
    ROUND(f.revenue_growth * 100, 2)                               AS yoy_growth_pct,
    ROUND(SUM(f.revenue) OVER (
        PARTITION BY f.symbol
        ORDER BY f.fiscal_year_int
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) / 1e9::NUMERIC, 2)                                           AS cumulative_revenue_b,
    LAG(ROUND(f.revenue / 1e9::NUMERIC, 2)) OVER (
        PARTITION BY f.symbol
        ORDER BY f.fiscal_year_int
    )                                                               AS prior_year_revenue_b
FROM financials f
JOIN companies c USING (symbol)
ORDER BY c.name, year;


-- ── 4. Financial health score (composite ranking) ────────────────────────────
-- Combines profitability, liquidity, and growth into a single score
-- Uses NTILE(4) to bucket companies into quartiles per metric

WITH scored AS (
    SELECT
        c.name,
        c.sector,
        f.fiscal_year_int AS year,
        f.net_margin,
        f.current_ratio,
        f.revenue_growth,
        f.roe,
        f.fcf_margin,
        -- Quartile rank per metric (4 = best, 1 = worst)
        NTILE(4) OVER (ORDER BY f.net_margin    NULLS LAST) AS q_margin,
        NTILE(4) OVER (ORDER BY f.current_ratio NULLS LAST) AS q_liquidity,
        NTILE(4) OVER (ORDER BY f.revenue_growth NULLS LAST) AS q_growth,
        NTILE(4) OVER (ORDER BY f.roe           NULLS LAST) AS q_roe,
        NTILE(4) OVER (ORDER BY f.fcf_margin    NULLS LAST) AS q_fcf
    FROM financials f
    JOIN companies c USING (symbol)
    WHERE f.fiscal_year_int = (
        SELECT MAX(fiscal_year_int) FROM financials f2
        WHERE f2.symbol = f.symbol
    )
)
SELECT
    name,
    sector,
    year,
    ROUND(net_margin    * 100, 2) AS net_margin_pct,
    ROUND(current_ratio,      2) AS current_ratio,
    ROUND(revenue_growth * 100, 2) AS revenue_growth_pct,
    -- Weighted health score out of 100
    ROUND(
        (q_margin * 0.30 + q_liquidity * 0.20 + q_growth * 0.25 + q_roe * 0.15 + q_fcf * 0.10)
        / 4.0 * 100
    , 1) AS health_score
FROM scored
ORDER BY health_score DESC;


-- ── 5. Sector benchmarks — average KPIs per sector per year ─────────────────
-- Good source for Power BI sector comparison visuals

SELECT
    c.sector,
    f.fiscal_year_int                              AS year,
    COUNT(DISTINCT f.symbol)                       AS company_count,
    ROUND(AVG(f.revenue)       / 1e9::NUMERIC, 2) AS avg_revenue_b,
    ROUND(AVG(f.net_margin)    * 100, 2)           AS avg_net_margin_pct,
    ROUND(AVG(f.roe)           * 100, 2)           AS avg_roe_pct,
    ROUND(AVG(f.current_ratio),       2)           AS avg_current_ratio,
    ROUND(AVG(f.debt_to_equity),      2)           AS avg_debt_to_equity,
    ROUND(AVG(f.revenue_growth) * 100, 2)          AS avg_revenue_growth_pct
FROM financials f
JOIN companies c USING (symbol)
GROUP BY c.sector, f.fiscal_year_int
ORDER BY c.sector, year;


-- ── 6. Top 5 companies by net margin per year ────────────────────────────────
-- Uses ROW_NUMBER() to pick top 5 globally each year

WITH ranked AS (
    SELECT
        c.name,
        c.sector,
        f.fiscal_year_int                  AS year,
        ROUND(f.net_margin * 100, 2)       AS net_margin_pct,
        ROW_NUMBER() OVER (
            PARTITION BY f.fiscal_year_int
            ORDER BY f.net_margin DESC NULLS LAST
        )                                  AS rn
    FROM financials f
    JOIN companies c USING (symbol)
)
SELECT name, sector, year, net_margin_pct
FROM ranked
WHERE rn <= 5
ORDER BY year, net_margin_pct DESC;


-- ── 7. Debt risk flag — companies with high leverage ────────────────────────
-- Flags companies whose debt-to-equity exceeds 2x the sector average

WITH sector_avg AS (
    SELECT
        c.sector,
        f.fiscal_year_int,
        AVG(f.debt_to_equity) AS avg_dte
    FROM financials f
    JOIN companies c USING (symbol)
    GROUP BY c.sector, f.fiscal_year_int
)
SELECT
    c.name,
    c.sector,
    f.fiscal_year_int                             AS year,
    ROUND(f.debt_to_equity, 2)                    AS debt_to_equity,
    ROUND(s.avg_dte, 2)                           AS sector_avg_dte,
    ROUND(f.debt_to_equity / NULLIF(s.avg_dte, 0), 2) AS ratio_vs_sector,
    CASE
        WHEN f.debt_to_equity > s.avg_dte * 2    THEN 'High Risk'
        WHEN f.debt_to_equity > s.avg_dte * 1.25 THEN 'Elevated'
        ELSE 'Normal'
    END                                           AS debt_flag
FROM financials f
JOIN companies  c USING (symbol)
JOIN sector_avg s ON s.sector = c.sector
                 AND s.fiscal_year_int = f.fiscal_year_int
WHERE f.fiscal_year_int = (
    SELECT MAX(fiscal_year_int) FROM financials f2 WHERE f2.symbol = f.symbol
)
ORDER BY ratio_vs_sector DESC NULLS LAST;