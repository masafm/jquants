#!/usr/bin/env python3
"""
Buy Candidates (Schema-less JSON Version)
- SQLiteのJSON関数を使ってクエリ実行
"""

import sqlite3
import math

DB_PATH = "jquants.db"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# 最新日付の取得
cur.execute("SELECT MAX(Date) FROM daily_quotes")
latest_price_date = cur.fetchone()[0]
print(f"Target Price Date: {latest_price_date}")

# =========================
# JSON抽出用SQL
# =========================
# ポイント:
# 1. json_extract(data, '$.KeyName') で値を取り出す
# 2. 取り出した値は文字列扱いなので、CAST(... AS REAL) で数値化する
# =========================

sql = """
WITH latest_fin AS (
    SELECT 
        Code,
        data,
        ROW_NUMBER() OVER (PARTITION BY Code ORDER BY Date DESC) as rn
    FROM financials
),
target_price AS (
    SELECT 
        Code, 
        data 
    FROM daily_quotes
    WHERE Date = ?
)
SELECT
    f.Code,
    
    -- 株価データ (JSONから抽出)
    CAST(json_extract(p.data, '$.Close') AS REAL) AS close_price,
    CAST(json_extract(p.data, '$.Volume') AS REAL) AS volume,

    -- 財務データ (JSONから抽出)
    CAST(json_extract(f.data, '$.NetSales') AS REAL) AS net_sales,
    CAST(json_extract(f.data, '$.ForecastNetSales') AS REAL) AS forecast_net_sales,
    
    CAST(json_extract(f.data, '$.Profit') AS REAL) AS profit,
    CAST(json_extract(f.data, '$.Equity') AS REAL) AS equity,
    
    CAST(json_extract(f.data, '$.EarningsPerShare') AS REAL) AS eps,
    CAST(json_extract(f.data, '$.ForecastEarningsPerShare') AS REAL) AS forecast_eps

FROM latest_fin f
JOIN target_price p
  ON f.Code = p.Code
WHERE
    f.rn = 1 -- 最新決算のみ
    
    -- フィルタリング条件もJSON抽出値に対して行う
    AND profit > 0
    AND equity > 0
    AND forecast_eps > 0
    AND volume > 10000
    
    -- 売上維持率 > 95%
    AND forecast_net_sales > net_sales * 0.95
    
    -- ROE >= 8% (計算式)
    AND (profit / equity) >= 0.08
    
    -- PER 5~40倍
    AND (close_price / forecast_eps) BETWEEN 5 AND 40
"""

try:
    rows = cur.execute(sql, (latest_price_date,)).fetchall()
except sqlite3.OperationalError as e:
    print(f"SQL Error: {e}")
    print("※ SQLiteのバージョンが古い可能性があります。Python 3.9以上推奨。")
    exit()

# =========================
# スコアリング (ロジックは同じ)
# =========================
candidates = []
for r in rows:
    try:
        # 既にSQLでCASTしているので、ここでは数値として扱える
        close_price = r["close_price"]
        roe = r["profit"] / r["equity"]
        eps = r["eps"]
        f_eps = r["forecast_eps"]
        volume = r["volume"]
        
        # EPS成長率
        eps_growth = ((f_eps - eps) / abs(eps)) if eps > 0 else 0
        
        # PER
        per = close_price / f_eps if f_eps > 0 else 0

        # 流動性スコア
        liquidity_score = math.log10(volume) if volume > 0 else 0

        score = (roe * 100 * 0.5) + (eps_growth * 100 * 0.3) + (liquidity_score * 10 * 0.2)

        candidates.append({
            "code": r["Code"],
            "price": int(close_price),
            "roe": roe * 100,
            "per": per,
            "eps": f_eps,
            "sales_growth": (r["forecast_net_sales"] / r["net_sales"] - 1) * 100,
            "score": score
        })
    except Exception:
        continue

candidates.sort(key=lambda x: x["score"], reverse=True)

print(f"\n=== BUY CANDIDATES ({len(candidates)} records) ===")
for c in candidates[:50]:
    print(
        f"{c['code']} | "
        f"Price: {c['price']:6,} | "
        f"ROE: {c['roe']:5.2f}% | "
        f"PER: {c['per']:5.1f} | "
        f"EPS(f): {c['eps']:6.1f} | "
        f"Sales: {c['sales_growth']:+5.1f}% | "
        f"Score: {c['score']:.1f}"
    )

conn.close()
