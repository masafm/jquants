#!/usr/bin/env python3
"""
J-Quants JSON-Based Pipeline (Schema-less)
- RAWデータをそのままJSONカラムに格納
- カラム定義管理からの解放
"""

import os
import json
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
# SQLite 初期化 (超シンプル化)
# =========================
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL;")
cur = conn.cursor()

# 株価テーブル: 日付とコード以外は全て "data" に入れる
cur.execute("""
CREATE TABLE IF NOT EXISTS daily_quotes (
    Date TEXT,
    Code TEXT,
    data JSON,
    PRIMARY KEY (Date, Code)
)
""")

# 財務テーブル: 日付とコード以外は全て "data" に入れる
cur.execute("""
CREATE TABLE IF NOT EXISTS financials (
    Date TEXT,
    Code TEXT,
    data JSON,
    PRIMARY KEY (Date, Code)
)
""")

# エラー管理用
cur.execute("""
CREATE TABLE IF NOT EXISTS failed_log (
    Date TEXT,
    Type TEXT,
    Msg TEXT,
    Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

# =========================
# 共通処理
# =========================
def request_api(method, url, **kwargs):
    res = requests.request(method, url, **kwargs)
    if not res.ok:
        raise RuntimeError(f"HTTP {res.status_code}: {res.text}")
    return res

def get_id_token():
    res = request_api(
        "POST",
        f"{API_URL}/v1/token/auth_refresh",
        params={"refreshtoken": REFRESH_TOKEN}
    )
    return res.json()["idToken"]

headers = {"Authorization": f"Bearer {get_id_token()}"}
print("idToken 取得成功")

def log_error(date_str, type_str, msg):
    try:
        cur.execute("INSERT INTO failed_log (Date, Type, Msg) VALUES (?, ?, ?)", (date_str, type_str, str(msg)))
        conn.commit()
    except:
        pass

# =========================
# 株価ロジック
# =========================
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

def fetch_daily_quotes(d: date):
    pagination_key = None
    while True:
        params = {"date": d.strftime("%Y%m%d")}
        if pagination_key:
            params["pagination_key"] = pagination_key

        res = request_api("GET", f"{API_URL}/v1/prices/daily_quotes", headers=headers, params=params)
        data = res.json()
        pagination_key = data.get("pagination_key")

        # バッチインサート用リスト
        rows = []
        for q in data.get("daily_quotes", []):
            # JSONとしてそのまま保存
            rows.append((
                q["Date"],
                q["Code"],
                json.dumps(q, ensure_ascii=False)
            ))
        
        if rows:
            cur.executemany(
                "INSERT OR REPLACE INTO daily_quotes (Date, Code, data) VALUES (?, ?, ?)",
                rows
            )
            conn.commit()

        if not pagination_key:
            break

# =========================
# 財務ロジック
# =========================
def fetch_financials(d: date):
    pagination_key = None
    while True:
        params = {"date": d.strftime("%Y%m%d")}
        if pagination_key:
            params["pagination_key"] = pagination_key

        res = request_api("GET", f"{API_URL}/v1/fins/statements", headers=headers, params=params)
        data = res.json()
        pagination_key = data.get("pagination_key")

        rows = []
        for s in data.get("statements", []):
            rows.append((
                s["DisclosedDate"],
                s["LocalCode"],
                json.dumps(s, ensure_ascii=False)
            ))

        if rows:
            cur.executemany(
                "INSERT OR REPLACE INTO financials (Date, Code, data) VALUES (?, ?, ?)",
                rows
            )
            conn.commit()

        if not pagination_key:
            break

# =========================
# メイン実行
# =========================
def main():
    today = date.today()

    # --- Daily Quotes ---
    print("=== DAILY QUOTES ===")
    cur.execute("SELECT MAX(Date) FROM daily_quotes")
    latest_price = cur.fetchone()[0]
    
    start_price = datetime.strptime(latest_price, "%Y-%m-%d").date() + timedelta(days=1) if latest_price else today - timedelta(days=365)
    
    target_days = get_trading_days(start_price, today)
    print(f"Fetching {len(target_days)} days for Prices...")
    
    for d in target_days:
        try:
            fetch_daily_quotes(d)
            print(f"[PRICE] {d} - OK")
        except Exception as e:
            print(f"[PRICE] {d} - FAIL: {e}")
            log_error(str(d), "PRICE", e)

    # --- Financials ---
    print("\n=== FINANCIALS ===")
    cur.execute("SELECT MAX(Date) FROM financials")
    latest_fin = cur.fetchone()[0]
    
    start_fin = datetime.strptime(latest_fin, "%Y-%m-%d").date() + timedelta(days=1) if latest_fin else today - timedelta(days=365*2)
    d = start_fin
    
    while d <= today:
        try:
            fetch_financials(d)
            print(f"[FIN]   {d} - OK")
        except Exception as e:
            # 財務は休日でもAPIレスポンスがある場合がある（空など）のでエラーログだけ残して進む
            print(f"[FIN]   {d} - FAIL: {e}")
            log_error(str(d), "FIN", e)
        d += timedelta(days=1)

if __name__ == "__main__":
    main()
    conn.close()
