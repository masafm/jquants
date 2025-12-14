#!/usr/bin/env python3
"""
J-Quants FULL PIPELINE
- daily_quotes (RAW gzip + 正規化)
- financials   (RAW gzip + 全カラム正規化)
- 営業日対応
- 失敗日リトライ管理
"""

import os
import json
import gzip
import sqlite3
import requests
from datetime import date, timedelta, datetime

# =========================
# 設定
# =========================
API_URL = "https://api.jquants.com"
DB_PATH = "jquants.db"
MAX_RETRY = 3

REFRESH_TOKEN = os.getenv("JQUANTS_REFRESH_TOKEN")
if not REFRESH_TOKEN:
    raise RuntimeError("環境変数 JQUANTS_REFRESH_TOKEN が未設定です")

# =========================
# SQLite 初期化
# =========================
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA auto_vacuum = FULL;")
conn.execute("VACUUM;")
cur = conn.cursor()

FIN_COL_MAP = {
    "DistributionsPerUnit(REIT)": "DistributionsPerUnit_REIT",
    "ForecastDistributionsPerUnit(REIT)": "ForecastDistributionsPerUnit_REIT",
    "NextYearForecastDistributionsPerUnit(REIT)": "NextYearForecastDistributionsPerUnit_REIT",
}

# =====================================================
# 株価テーブル
# =====================================================
cur.execute("""
CREATE TABLE IF NOT EXISTS daily_quotes_raw (
    Date TEXT,
    Code TEXT,
    payload BLOB,
    PRIMARY KEY (Date, Code)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS daily_quotes (
    Date TEXT,
    Code TEXT,
    Open REAL,
    High REAL,
    Low REAL,
    Close REAL,
    UpperLimit TEXT,
    LowerLimit TEXT,
    Volume REAL,
    TurnoverValue REAL,
    AdjustmentFactor REAL,
    AdjustmentOpen REAL,
    AdjustmentHigh REAL,
    AdjustmentLow REAL,
    AdjustmentClose REAL,
    AdjustmentVolume REAL,
    MorningOpen REAL,
    MorningHigh REAL,
    MorningLow REAL,
    MorningClose REAL,
    MorningUpperLimit TEXT,
    MorningLowerLimit TEXT,
    MorningVolume REAL,
    MorningTurnoverValue REAL,
    MorningAdjustmentOpen REAL,
    MorningAdjustmentHigh REAL,
    MorningAdjustmentLow REAL,
    MorningAdjustmentClose REAL,
    MorningAdjustmentVolume REAL,
    AfternoonOpen REAL,
    AfternoonHigh REAL,
    AfternoonLow REAL,
    AfternoonClose REAL,
    AfternoonUpperLimit TEXT,
    AfternoonLowerLimit TEXT,
    AfternoonVolume REAL,
    AfternoonTurnoverValue REAL,
    AfternoonAdjustmentOpen REAL,
    AfternoonAdjustmentHigh REAL,
    AfternoonAdjustmentLow REAL,
    AfternoonAdjustmentClose REAL,
    AfternoonAdjustmentVolume REAL,
    PRIMARY KEY (Date, Code)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS failed_dates (
    Date TEXT PRIMARY KEY,
    last_error TEXT,
    retry_count INTEGER DEFAULT 0
)
""")

# =====================================================
# 財務テーブル
# =====================================================
cur.execute("""
CREATE TABLE IF NOT EXISTS financials_raw (
    DisclosedDate TEXT,
    LocalCode TEXT,
    payload BLOB,
    PRIMARY KEY (DisclosedDate, LocalCode)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS financials (
    DisclosedDate TEXT,
    DisclosedTime TEXT,
    LocalCode TEXT,
    DisclosureNumber TEXT,
    TypeOfDocument TEXT,
    TypeOfCurrentPeriod TEXT,
    CurrentPeriodStartDate TEXT,
    CurrentPeriodEndDate TEXT,
    CurrentFiscalYearStartDate TEXT,
    CurrentFiscalYearEndDate TEXT,
    NextFiscalYearStartDate TEXT,
    NextFiscalYearEndDate TEXT,

    NetSales TEXT,
    OperatingProfit TEXT,
    OrdinaryProfit TEXT,
    Profit TEXT,
    EarningsPerShare TEXT,
    DilutedEarningsPerShare TEXT,
    TotalAssets TEXT,
    Equity TEXT,
    EquityToAssetRatio TEXT,
    BookValuePerShare TEXT,

    CashFlowsFromOperatingActivities TEXT,
    CashFlowsFromInvestingActivities TEXT,
    CashFlowsFromFinancingActivities TEXT,
    CashAndEquivalents TEXT,

    ResultDividendPerShare1stQuarter TEXT,
    ResultDividendPerShare2ndQuarter TEXT,
    ResultDividendPerShare3rdQuarter TEXT,
    ResultDividendPerShareFiscalYearEnd TEXT,
    ResultDividendPerShareAnnual TEXT,
    "DistributionsPerUnit_REIT" TEXT,
    ResultTotalDividendPaidAnnual TEXT,
    ResultPayoutRatioAnnual TEXT,

    ForecastDividendPerShare1stQuarter TEXT,
    ForecastDividendPerShare2ndQuarter TEXT,
    ForecastDividendPerShare3rdQuarter TEXT,
    ForecastDividendPerShareFiscalYearEnd TEXT,
    ForecastDividendPerShareAnnual TEXT,
    "ForecastDistributionsPerUnit_REIT" TEXT,
    ForecastTotalDividendPaidAnnual TEXT,
    ForecastPayoutRatioAnnual TEXT,

    NextYearForecastDividendPerShare1stQuarter TEXT,
    NextYearForecastDividendPerShare2ndQuarter TEXT,
    NextYearForecastDividendPerShare3rdQuarter TEXT,
    NextYearForecastDividendPerShareFiscalYearEnd TEXT,
    NextYearForecastDividendPerShareAnnual TEXT,
    "NextYearForecastDistributionsPerUnit_REIT" TEXT,
    NextYearForecastPayoutRatioAnnual TEXT,

    ForecastNetSales2ndQuarter TEXT,
    ForecastOperatingProfit2ndQuarter TEXT,
    ForecastOrdinaryProfit2ndQuarter TEXT,
    ForecastProfit2ndQuarter TEXT,
    ForecastEarningsPerShare2ndQuarter TEXT,

    NextYearForecastNetSales2ndQuarter TEXT,
    NextYearForecastOperatingProfit2ndQuarter TEXT,
    NextYearForecastOrdinaryProfit2ndQuarter TEXT,
    NextYearForecastProfit2ndQuarter TEXT,
    NextYearForecastEarningsPerShare2ndQuarter TEXT,

    ForecastNetSales TEXT,
    ForecastOperatingProfit TEXT,
    ForecastOrdinaryProfit TEXT,
    ForecastProfit TEXT,
    ForecastEarningsPerShare TEXT,

    NextYearForecastNetSales TEXT,
    NextYearForecastOperatingProfit TEXT,
    NextYearForecastOrdinaryProfit TEXT,
    NextYearForecastProfit TEXT,
    NextYearForecastEarningsPerShare TEXT,

    MaterialChangesInSubsidiaries TEXT,
    SignificantChangesInTheScopeOfConsolidation TEXT,
    ChangesBasedOnRevisionsOfAccountingStandard TEXT,
    ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard TEXT,
    ChangesInAccountingEstimates TEXT,
    RetrospectiveRestatement TEXT,

    NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock TEXT,
    NumberOfTreasuryStockAtTheEndOfFiscalYear TEXT,
    AverageNumberOfShares TEXT,

    NonConsolidatedNetSales TEXT,
    NonConsolidatedOperatingProfit TEXT,
    NonConsolidatedOrdinaryProfit TEXT,
    NonConsolidatedProfit TEXT,
    NonConsolidatedEarningsPerShare TEXT,
    NonConsolidatedTotalAssets TEXT,
    NonConsolidatedEquity TEXT,
    NonConsolidatedEquityToAssetRatio TEXT,
    NonConsolidatedBookValuePerShare TEXT,

    ForecastNonConsolidatedNetSales2ndQuarter TEXT,
    ForecastNonConsolidatedOperatingProfit2ndQuarter TEXT,
    ForecastNonConsolidatedOrdinaryProfit2ndQuarter TEXT,
    ForecastNonConsolidatedProfit2ndQuarter TEXT,
    ForecastNonConsolidatedEarningsPerShare2ndQuarter TEXT,

    NextYearForecastNonConsolidatedNetSales2ndQuarter TEXT,
    NextYearForecastNonConsolidatedOperatingProfit2ndQuarter TEXT,
    NextYearForecastNonConsolidatedOrdinaryProfit2ndQuarter TEXT,
    NextYearForecastNonConsolidatedProfit2ndQuarter TEXT,
    NextYearForecastNonConsolidatedEarningsPerShare2ndQuarter TEXT,

    ForecastNonConsolidatedNetSales TEXT,
    ForecastNonConsolidatedOperatingProfit TEXT,
    ForecastNonConsolidatedOrdinaryProfit TEXT,
    ForecastNonConsolidatedProfit TEXT,
    ForecastNonConsolidatedEarningsPerShare TEXT,

    NextYearForecastNonConsolidatedNetSales TEXT,
    NextYearForecastNonConsolidatedOperatingProfit TEXT,
    NextYearForecastNonConsolidatedOrdinaryProfit TEXT,
    NextYearForecastNonConsolidatedProfit TEXT,
    NextYearForecastNonConsolidatedEarningsPerShare TEXT,

    PRIMARY KEY (DisclosedDate, LocalCode)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS financial_failed_dates (
    DisclosedDate TEXT PRIMARY KEY,
    last_error TEXT,
    retry_count INTEGER DEFAULT 0
)
""")

conn.commit()

# =========================
# 共通リクエスト
# =========================
def request_api(method, url, **kwargs):
    res = requests.request(method, url, **kwargs)
    if not res.ok:
        raise RuntimeError(f"HTTP {res.status_code}: {res.text}")
    return res

# =========================
# Token取得
# =========================
def get_id_token():
    res = request_api(
        "POST",
        f"{API_URL}/v1/token/auth_refresh",
        params={"refreshtoken": REFRESH_TOKEN}
    )
    return res.json()["idToken"]

headers = {"Authorization": f"Bearer {get_id_token()}"}
print("idToken 取得成功")

# =====================================================
# 株価ロジック
# =====================================================
def get_trading_days(start: date, end: date):
    res = request_api(
        "GET",
        f"{API_URL}/v1/markets/trading_calendar",
        headers=headers,
        params={"from": start.strftime("%Y%m%d"), "to": end.strftime("%Y%m%d")}
    )
    return [
        datetime.strptime(d["Date"], "%Y-%m-%d").date()
        for d in res.json()["trading_calendar"]
        if d["HolidayDivision"] == "1"
    ]

def get_latest_price_date():
    cur.execute("SELECT MAX(Date) FROM daily_quotes_raw")
    r = cur.fetchone()
    return datetime.strptime(r[0], "%Y-%m-%d").date() if r and r[0] else None

def fetch_daily_quotes_one_day(d: date):
    pagination_key = None
    while True:
        params = {"date": d.strftime("%Y%m%d")}
        if pagination_key:
            params["pagination_key"] = pagination_key

        res = request_api(
            "GET",
            f"{API_URL}/v1/prices/daily_quotes",
            headers=headers,
            params=params
        )

        data = res.json()
        pagination_key = data.get("pagination_key")

        for q in data.get("daily_quotes", []):
            cur.execute(
                "INSERT OR IGNORE INTO daily_quotes_raw VALUES (?, ?, ?)",
                (
                    q["Date"],
                    q["Code"],
                    gzip.compress(json.dumps(q, ensure_ascii=False).encode("utf-8"))
                )
            )

            cols = ",".join(q.keys())
            ph = ",".join(["?"] * len(q))
            cur.execute(
                f"INSERT OR IGNORE INTO daily_quotes ({cols}) VALUES ({ph})",
                list(q.values())
            )

        conn.commit()
        if not pagination_key:
            break

# =====================================================
# 財務ロジック
# =====================================================
def get_latest_fin_date():
    cur.execute("SELECT MAX(DisclosedDate) FROM financials_raw")
    r = cur.fetchone()
    return datetime.strptime(r[0], "%Y-%m-%d").date() if r and r[0] else None

def fetch_financials_one_day(d: date):
    pagination_key = None
    while True:
        params = {"date": d.strftime("%Y%m%d")}
        if pagination_key:
            params["pagination_key"] = pagination_key

        res = request_api(
            "GET",
            f"{API_URL}/v1/fins/statements",
            headers=headers,
            params=params
        )

        data = res.json()
        pagination_key = data.get("pagination_key")

        for s in data.get("statements", []):
            cur.execute(
                "INSERT OR IGNORE INTO financials_raw VALUES (?, ?, ?)",
                (
                    s["DisclosedDate"],
                    s["LocalCode"],
                    gzip.compress(json.dumps(s, ensure_ascii=False).encode("utf-8"))
                )
            )

            # --- 列名安全化 ---
            safe_cols = []
            values = []

            for k, v in s.items():
                col = FIN_COL_MAP.get(k, k)
                safe_cols.append(col)
                values.append(v)

            cols = ",".join(safe_cols)
            ph = ",".join(["?"] * len(values))

            cur.execute(
                f"INSERT OR IGNORE INTO financials ({cols}) VALUES ({ph})",
                values
            )

        conn.commit()
        if not pagination_key:
            break

# =====================================================
# エラー記録用ヘルパー
# =====================================================
def log_failure(table_name, date_val, error_msg):
    """失敗した日付と理由をDBに保存"""
    try:
        cur.execute(
            f"INSERT OR REPLACE INTO {table_name} (Date, last_error, retry_count) VALUES (?, ?, COALESCE((SELECT retry_count FROM {table_name} WHERE Date=?), 0) + 1)",
            (date_val, str(error_msg), date_val)
        )
        conn.commit()
        print(f"  [ERROR LOGGED] {date_val}: {error_msg}")
    except Exception as e:
        print(f"  [CRITICAL] Failed to log error: {e}")

# =====================================================
# 実行 (修正版)
# =====================================================
def main():
    today = date.today()

    # --- 株価データ収集 ---
    print("=== DAILY QUOTES ===")
    latest_price = get_latest_price_date()
    # 初回実行時は少し長めに取る (例: 1年前から)
    start = latest_price + timedelta(days=1) if latest_price else today - timedelta(days=365)
    
    target_days = get_trading_days(start, today)
    print(f"Fetching {len(target_days)} days...")

    for d in target_days:
        try:
            fetch_daily_quotes_one_day(d)
            print(f"[PRICE] {d} - OK")
            
            # 成功したら失敗リストから削除（リトライ成功時用）
            cur.execute("DELETE FROM failed_dates WHERE Date = ?", (d.strftime("%Y-%m-%d"),))
            conn.commit()
            
        except Exception as e:
            print(f"[PRICE] {d} - FAILED: {e}")
            log_failure("failed_dates", d.strftime("%Y-%m-%d"), e)

    # --- 財務データ収集 ---
    print("\n=== FINANCIALS ===")
    latest_fin = get_latest_fin_date()
    d = latest_fin + timedelta(days=1) if latest_fin else today - timedelta(days=365 * 2) # 財務は過去2年分くらい欲しい
    
    while d <= today:
        try:
            fetch_financials_one_day(d)
            print(f"[FIN]   {d} - OK")
            
            # 成功したら失敗リストから削除
            cur.execute("DELETE FROM financial_failed_dates WHERE DisclosedDate = ?", (d.strftime("%Y-%m-%d"),))
            conn.commit()
            
        except Exception as e:
            print(f"[FIN]   {d} - FAILED: {e}")
            # financial_failed_dates テーブルのカラム名は DisclosedDate なので注意
            # ただし log_failure は汎用的に作っているので、テーブル定義の Date カラム名を合わせるか、
            # ここだけ個別SQLにする必要があります。
            # 簡略化のため、failed_dates テーブルの定義を統一するか、以下のように個別処理します。
            try:
                d_str = d.strftime("%Y-%m-%d")
                cur.execute(
                    "INSERT OR REPLACE INTO financial_failed_dates (DisclosedDate, last_error, retry_count) VALUES (?, ?, COALESCE((SELECT retry_count FROM financial_failed_dates WHERE DisclosedDate=?), 0) + 1)",
                    (d_str, str(e), d_str)
                )
                conn.commit()
            except:
                pass
        
        d += timedelta(days=1)

if __name__ == "__main__":
    try:
        main()
    finally:
        conn.close()
        print("ALL DONE")
