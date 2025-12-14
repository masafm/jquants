#!/usr/bin/env python3
"""
Pick Buy Candidates (Fixed SQL Version)
- 修正点: 全銘柄の「それぞれの最新決算」を取得するように変更
"""

import sqlite3
import math

DB_PATH = "jquants.db"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# 株価は「市場全体の最新日」でOK（全銘柄の株価はその日に存在するため）
cur.execute("SELECT MAX(Date) FROM daily_quotes")
latest_price_date = cur.fetchone()[0]
print(f"Target Price Date: {latest_price_date}")

# =========================
# 銘柄抽出（修正版SQL）
# =========================
sql = """
WITH latest_fin AS (
    -- 各銘柄ごとに、DisclosedDate が最も新しい行に rn=1 を振る
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY LocalCode ORDER BY DisclosedDate DESC) as rn
    FROM financials
),
target_price AS (
    -- 指定した日付の株価のみ抽出
    SELECT *
    FROM daily_quotes
    WHERE Date = ?
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
    
    -- 指標計算 (ゼロ除算回避のためNULLチェック推奨だが簡易化)
    (CAST(f.Profit AS REAL) / CAST(f.Equity AS REAL)) AS roe,
    (p.Close / NULLIF(CAST(f.ForecastEarningsPerShare AS REAL), 0)) AS per

FROM latest_fin f
JOIN target_price p
  ON f.LocalCode = p.Code
WHERE
    f.rn = 1  -- 各銘柄の最新決算のみを対象にする
    
    -- 以下、フィルタ条件
    AND f.Profit IS NOT NULL AND CAST(f.Profit AS REAL) > 0
    AND f.Equity IS NOT NULL AND CAST(f.Equity AS REAL) > 0
    
    -- EPS > 0
    AND f.ForecastEarningsPerShare IS NOT NULL 
    AND CAST(f.ForecastEarningsPerShare AS REAL) > 0
    
    -- 売上が大きく落ち込んでいない ( > 95%)
    AND CAST(f.ForecastNetSales AS REAL) > CAST(f.NetSales AS REAL) * 0.95
    
    -- 流動性 (Volume > 10,000)
    AND CAST(p.Volume AS REAL) > 10000
    
    -- ROE >= 8%
    AND (CAST(f.Profit AS REAL) / CAST(f.Equity AS REAL)) >= 0.08
    
    -- PER 5倍〜40倍
    AND (p.Close / CAST(f.ForecastEarningsPerShare AS REAL)) BETWEEN 5 AND 40
"""

rows = cur.execute(sql, (latest_price_date,)).fetchall()

# =========================
# スコアリング (変更なし)
# =========================
candidates = []
for r in rows:
    try:
        roe = r["roe"]
        eps = r["eps"] or 0
        forecast_eps = r["forecast_eps"]
        volume = r["Volume"]

        # EPS成長率
        eps_growth = ((forecast_eps - eps) / abs(eps)) if eps > 0 else 0
        
        # 流動性スコア
        liquidity_score = math.log10(volume) if volume > 0 else 0

        # 総合スコア
        score = (roe * 100 * 0.5) + (eps_growth * 100 * 0.3) + (liquidity_score * 10 * 0.2)

        candidates.append({
            "code": r["LocalCode"],
            "price": int(r["close_price"]),
            "roe": roe * 100,
            "per": r["per"] if r["per"] else 0,
            "eps": forecast_eps,
            "sales_growth": (r["forecast_net_sales"] / r["net_sales"] - 1) * 100,
            "score": score
        })
    except Exception as e:
        continue # 計算エラーの銘柄はスキップ

candidates.sort(key=lambda x: x["score"], reverse=True)

print(f"\n=== BUY CANDIDATES ({len(candidates)} records found) ===")
for c in candidates[:20]:
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
