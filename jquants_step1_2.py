#!/usr/bin/env python3
"""
J-Quants daily_quotes
RAW(JSON gzip圧縮) + 正規化(SQLite)
営業日対応 + 失敗日リトライ管理
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

# RAW（gzip圧縮JSON）
cur.execute("""
CREATE TABLE IF NOT EXISTS daily_quotes_raw (
    Date TEXT,
    Code TEXT,
    payload BLOB,
    PRIMARY KEY (Date, Code)
)
""")

# 正規化テーブル
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

# 失敗日管理
cur.execute("""
CREATE TABLE IF NOT EXISTS failed_dates (
    Date TEXT PRIMARY KEY,
    last_error TEXT,
    retry_count INTEGER DEFAULT 0
)
""")

conn.commit()

# =========================
# 共通リクエスト
# =========================
def request_with_error_log(method, url, **kwargs):
    res = requests.request(method, url, **kwargs)
    if not res.ok:
        raise RuntimeError(f"HTTP {res.status_code}: {res.text}")
    return res

# =========================
# Token取得
# =========================
def get_id_token(refresh_token: str) -> str:
    res = request_with_error_log(
        "POST",
        f"{API_URL}/v1/token/auth_refresh",
        params={"refreshtoken": refresh_token}
    )
    return res.json()["idToken"]

headers = {
    "Authorization": f"Bearer {get_id_token(REFRESH_TOKEN)}"
}
print("idToken 取得成功")

# =========================
# 営業日取得
# =========================
def get_trading_days(start: date, end: date):
    res = request_with_error_log(
        "GET",
        f"{API_URL}/v1/markets/trading_calendar",
        headers=headers,
        params={
            "from": start.strftime("%Y%m%d"),
            "to": end.strftime("%Y%m%d")
        }
    )

    return [
        datetime.strptime(d["Date"], "%Y-%m-%d").date()
        for d in res.json()["trading_calendar"]
        if d["HolidayDivision"] == "1"
    ]

# =========================
# DB最新日
# =========================
def get_latest_date():
    cur.execute("SELECT MAX(Date) FROM daily_quotes_raw")
    row = cur.fetchone()
    if row and row[0]:
        return datetime.strptime(row[0], "%Y-%m-%d").date()
    return None

# =========================
# 失敗日取得
# =========================
def get_failed_dates():
    cur.execute("""
        SELECT Date FROM failed_dates
        WHERE retry_count < ?
        ORDER BY retry_count, Date
    """, (MAX_RETRY,))
    return [datetime.strptime(r[0], "%Y-%m-%d").date() for r in cur.fetchall()]

# =========================
# 日次株価取得（1日）
# =========================
def fetch_one_day(headers, target_date: date):
    date_str = target_date.strftime("%Y%m%d")
    pagination_key = None
    saved = 0

    while True:
        params = {"date": date_str}
        if pagination_key:
            params["pagination_key"] = pagination_key

        res = request_with_error_log(
            "GET",
            f"{API_URL}/v1/prices/daily_quotes",
            headers=headers,
            params=params
        )

        data = res.json()
        quotes = data.get("daily_quotes", [])
        pagination_key = data.get("pagination_key")

        if not quotes:
            break

        for q in quotes:
            d = q["Date"]
            c = q["Code"]

            # --- RAW gzip圧縮 ---
            compressed = gzip.compress(
                json.dumps(q, ensure_ascii=False).encode("utf-8")
            )

            cur.execute("""
            INSERT OR IGNORE INTO daily_quotes_raw
            VALUES (?, ?, ?)
            """, (d, c, compressed))

            # --- 正規化 ---
            cols = ",".join(q.keys())
            ph = ",".join(["?"] * len(q))
            cur.execute(
                f"INSERT OR IGNORE INTO daily_quotes ({cols}) VALUES ({ph})",
                list(q.values())
            )

            saved += 1

        conn.commit()

        if not pagination_key:
            break

    return saved

# =========================
# メイン処理
# =========================
def run_update(initial_days=180):
    today = date.today()

    # ① 失敗日の再取得
    for d in get_failed_dates():
        print(f"[RETRY] {d}")
        try:
            cnt = fetch_one_day(headers, d)
            cur.execute("DELETE FROM failed_dates WHERE Date=?", (d.strftime("%Y-%m-%d"),))
            conn.commit()
            print(f"[OK] {d}: {cnt} 件")
        except Exception as e:
            cur.execute("""
                UPDATE failed_dates
                SET retry_count = retry_count + 1,
                    last_error = ?
                WHERE Date=?
            """, (str(e), d.strftime("%Y-%m-%d")))
            conn.commit()
            print(f"[FAIL] {d}: {e}")

    # ② 新規日取得
    latest = get_latest_date()
    start = latest + timedelta(days=1) if latest else today - timedelta(days=initial_days)

    trading_days = get_trading_days(start, today)

    for d in trading_days:
        print(f"[FETCH] {d}")
        try:
            cnt = fetch_one_day(headers, d)
            print(f"[SAVE] {d}: {cnt} 件")
        except Exception as e:
            cur.execute("""
                INSERT OR IGNORE INTO failed_dates(Date, last_error, retry_count)
                VALUES (?, ?, 0)
            """, (d.strftime("%Y-%m-%d"), str(e)))
            conn.commit()
            print(f"[ERROR] {d}: failed 登録")

# =========================
# 実行
# =========================
run_update()
conn.close()
print("DBクローズ完了")

