#!/usr/bin/env python3
"""
Pick Buy Candidates from J-Quants SQLite DB

Filters:
1. ROE >= 8%
2. Forecast EPS > 0
3. PER between 5 and 20
4. Forecast NetSales > NetSales
5. Volume > 100,000

Score:
- ROE
- EPS growth
- Liquidity (Volume)
"""

import sqlite3
import math

DB_PATH = "jquants.db"

# =========================
# DB 接続
# =========================
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# =========================
# 最新日付取得
# =========================
cur.execute("SELECT MAX(Date) FROM daily_quotes")
latest_price_date = cur.fetchone()[0]

cur.execute("SELECT MAX(DisclosedDate) FROM financials")
latest_fin_date = cur.fetchone()[0]

print(f"Latest price date : {latest_price_date}")
print(f"Latest fin date   : {latest_fin_date}")

# =========================
# 銘柄抽出（SQL）
# =========================
sql = """
WITH latest_price AS (
    SELECT *
    FROM daily_quotes
    WHERE Date = ?
),
latest_fin AS (
    SELECT *
    FROM financials
    WHERE DisclosedDate = ?
)
SELECT
    f.LocalCode,
    p.Close AS close_price,
    p.Volume,

    CAST(f.NetSales AS REAL) AS net_sales,
    CAST(f.ForecastNetSales AS REAL) AS forecast_net_sales,

    CAST(f.Profit AS REAL) AS profit,
    CAST(f.Equity AS REAL) AS equity,

    CAST(f.EarningsPerShare AS REAL) AS eps,
    CAST(f.ForecastEarningsPerShare AS REAL) AS forecast_eps,

    (CAST(f.Profit AS REAL) / CAST(f.Equity AS REAL)) AS roe,
    (p.Close / CAST(f.ForecastEarningsPerShare AS REAL)) AS per
FROM latest_fin f
JOIN latest_price p
  ON f.LocalCode = p.Code
WHERE
    -- Profit / Equity
    f.Profit IS NOT NULL AND f.Profit != ''
    AND f.Equity IS NOT NULL AND f.Equity != ''
    AND CAST(f.Profit AS REAL) > 0
    AND CAST(f.Equity AS REAL) > 0

    -- EPS（★ゼロ排除）
    AND f.ForecastEarningsPerShare IS NOT NULL
    AND f.ForecastEarningsPerShare != ''
    AND CAST(f.ForecastEarningsPerShare AS REAL) > 0

    -- NetSales growth
    AND f.NetSales IS NOT NULL AND f.NetSales != ''
    AND f.ForecastNetSales IS NOT NULL AND f.ForecastNetSales != ''
    AND CAST(f.ForecastNetSales AS REAL) > CAST(f.NetSales AS REAL) * 0.95

    -- Liquidity
    AND CAST(p.Volume AS REAL) > 10000

    -- ROE
    AND (CAST(f.Profit AS REAL) / CAST(f.Equity AS REAL)) >= 0.08

    -- PER
    AND (p.Close / CAST(f.ForecastEarningsPerShare AS REAL)) BETWEEN 5 AND 40
"""

rows = cur.execute(sql, (latest_price_date, latest_fin_date)).fetchall()

# =========================
# スコアリング
# =========================
candidates = []

for r in rows:
    roe = r["roe"]                    # 0.15 = 15%
    eps = r["eps"] or 0
    forecast_eps = r["forecast_eps"]
    volume = r["Volume"]

    # EPS growth（マイナス防止）
    eps_growth = (
        (forecast_eps - eps) / abs(eps)
        if eps > 0 else 0
    )

    # Liquidity score（対数）
    liquidity_score = math.log10(volume)

    # 総合スコア
    score = (
        roe * 100 * 0.5 +
        eps_growth * 100 * 0.3 +
        liquidity_score * 10 * 0.2
    )

    candidates.append({
        "code": r["LocalCode"],
        "price": int(r["close_price"]),
        "roe": roe * 100,
        "per": r["per"],
        "eps": forecast_eps,
        "sales_growth": (r["forecast_net_sales"] / r["net_sales"] - 1) * 100,
        "score": score
    })

# =========================
# ソート & 表示
# =========================
candidates.sort(key=lambda x: x["score"], reverse=True)

print("\n=== BUY CANDIDATES (SCORED) ===")

for c in candidates[:20]:
    print(
        f"{c['code']} | "
        f"Price: {c['price']} | "
        f"ROE: {c['roe']:.2f}% | "
        f"PER: {c['per']:.1f} | "
        f"EPS(f): {c['eps']:.2f} | "
        f"Sales:+{c['sales_growth']:.1f}% | "
        f"Score: {c['score']:.1f}"
    )

conn.close()
