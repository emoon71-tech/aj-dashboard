#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_chip_day.py

用途：
1. 自動抓 TWSE 上市三大法人日資料
2. 自動抓 TPEX 上櫃三大法人日資料
3. 只取 code / date / foreign / trust
4. 輸出成 raw_chip/YYYY-MM-DD.json

輸出格式：
[
  { "code": "2330", "date": "2026-04-02", "foreign": 1200, "trust": 300 },
  ...
]

使用方式：
python fetch_chip_day.py --date 2026-04-02
python fetch_chip_day.py --date 2026-04-02 --outdir raw_chip
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import urllib3
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TWSE_URL = "https://www.twse.com.tw/rwd/zh/fund/T86"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

TPEX_CANDIDATES = [
    "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php",
    "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_result.php",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取單日 TWSE / TPEX 法人資料，輸出 raw_chip JSON")
    parser.add_argument("--date", required=True, help="日期，格式 YYYY-MM-DD，例如 2026-04-02")
    parser.add_argument("--outdir", default="raw_chip", help="輸出資料夾，預設 raw_chip")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout 秒數，預設 20")
    parser.add_argument("--sleep", type=float, default=0.6, help="來源間隔秒數，預設 0.6")
    return parser.parse_args()


def validate_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"日期格式錯誤：{date_str}，請用 YYYY-MM-DD") from exc


def roc_date(dt: datetime) -> str:
    return f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"


def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def clean_text(value: object) -> str:
    if value is None:
        return ""
    s = str(value)
    s = s.replace("\u3000", " ").replace("\xa0", " ").strip()
    return s


def to_int(value: object) -> int:
    s = clean_text(value)
    if not s or s in {"-", "--", "—", "除權", "除息", "除權息", "N/A"}:
        return 0

    s = s.replace(",", "").replace(" ", "")
    s = s.replace("＋", "").replace("+", "")

    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]

    s = re.sub(r"[^0-9\-]", "", s)
    if s in {"", "-"}:
        return 0
    return int(s)


def is_stock_code(code: str) -> bool:
    code = clean_text(code)
    return bool(re.fullmatch(r"[0-9A-Z]{4,6}", code))


def unique_by_code(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    seen: Dict[str, Dict[str, object]] = {}
    for row in rows:
        seen[str(row["code"])] = row
    return [seen[k] for k in sorted(seen.keys())]


def request_json(url: str, params: dict, timeout: int) -> dict:
    resp = requests.get(
        url,
        params=params,
        headers=HEADERS,
        timeout=timeout,
        verify=False,
    )
    resp.raise_for_status()
    return resp.json()


def request_text(url: str, params: dict, timeout: int) -> str:
    resp = requests.get(
        url,
        params=params,
        headers=HEADERS,
        timeout=timeout,
        verify=False,
    )
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def match_twse_indices(fields: List[str]) -> tuple[int | None, int | None, int | None]:
    code_idx = None
    foreign_idx = None
    trust_idx = None

    for idx, name in enumerate(fields):
        name_clean = clean_text(name).replace(" ", "")

        if name_clean == "證券代號":
            code_idx = idx

        elif (
            "外陸資買賣超股數" in name_clean
            or "外資及陸資(不含外資自營商)買賣超股數" in name_clean
            or "外資及陸資買賣超股數" in name_clean
            or "外資買賣超股數" in name_clean
        ):
            foreign_idx = idx

        elif "投信買賣超股數" in name_clean:
            trust_idx = idx

    return code_idx, foreign_idx, trust_idx


def fetch_twse_day(dt: datetime, timeout: int) -> List[Dict[str, object]]:
    params = {
        "response": "json",
        "date": yyyymmdd(dt),
        "selectType": "ALLBUT0999",
        "_": int(time.time() * 1000),
    }

    data = request_json(TWSE_URL, params=params, timeout=timeout)

    stat = str(data.get("stat", "")).strip()
    if stat not in {"OK", "OK "}:
        raise RuntimeError(f"TWSE 回傳異常：{data.get('stat')}")

    fields = [clean_text(x) for x in data.get("fields", [])]
    rows = data.get("data", []) or []

    if not fields or not rows:
        raise RuntimeError("TWSE 沒有抓到資料，可能是該日休市或官方尚未更新。")

    code_idx, foreign_idx, trust_idx = match_twse_indices(fields)

    if code_idx is None or foreign_idx is None or trust_idx is None:
        raise RuntimeError(f"TWSE 欄位結構變動，抓不到必要欄位。fields={fields}")

    result: List[Dict[str, object]] = []
    date_str = dt.strftime("%Y-%m-%d")

    for row in rows:
        if len(row) <= max(code_idx, foreign_idx, trust_idx):
            continue

        code = clean_text(row[code_idx])
        if not is_stock_code(code):
            continue

        result.append(
            {
                "code": code,
                "date": date_str,
                "foreign": to_int(row[foreign_idx]),
                "trust": to_int(row[trust_idx]),
            }
        )

    if not result:
        raise RuntimeError("TWSE 解析後沒有有效個股資料。")

    return unique_by_code(result)


def parse_tpex_csv_text(csv_text: str, dt: datetime) -> List[Dict[str, object]]:
    text = csv_text.replace("\ufeff", "")
    lines = [line for line in text.splitlines() if clean_text(line)]

    parsed_rows: List[List[str]] = []
    for line in lines:
        try:
            row = next(csv.reader([line]))
        except Exception:
            continue
        parsed_rows.append([clean_text(x) for x in row])

    header_idx = None
    code_idx = None
    foreign_idx = None
    trust_idx = None

    for i, row in enumerate(parsed_rows):
        temp_code_idx = None
        temp_foreign_idx = None
        temp_trust_idx = None

        for j, cell in enumerate(row):
            cell_no_space = cell.replace(" ", "")
            if cell_no_space == "代號":
                temp_code_idx = j
            elif "外資及陸資" in cell_no_space and "買賣超股數" in cell_no_space:
                temp_foreign_idx = j
            elif "外陸資買賣超股數" in cell_no_space:
                temp_foreign_idx = j
            elif "投信" in cell_no_space and "買賣超股數" in cell_no_space:
                temp_trust_idx = j

        if temp_code_idx is not None and temp_foreign_idx is not None and temp_trust_idx is not None:
            header_idx = i
            code_idx = temp_code_idx
            foreign_idx = temp_foreign_idx
            trust_idx = temp_trust_idx
            break

    if header_idx is None or code_idx is None or foreign_idx is None or trust_idx is None:
        raise RuntimeError("TPEX CSV 欄位結構變動，抓不到必要欄位。")

    result: List[Dict[str, object]] = []
    date_str = dt.strftime("%Y-%m-%d")

    for row in parsed_rows[header_idx + 1:]:
        if len(row) <= max(code_idx, foreign_idx, trust_idx):
            continue

        code = clean_text(row[code_idx])
        if not is_stock_code(code):
            continue

        result.append(
            {
                "code": code,
                "date": date_str,
                "foreign": to_int(row[foreign_idx]),
                "trust": to_int(row[trust_idx]),
            }
        )

    if not result:
        raise RuntimeError("TPEX CSV 解析後沒有有效個股資料。")

    return unique_by_code(result)


def fetch_tpex_day(dt: datetime, timeout: int) -> List[Dict[str, object]]:
    roc = roc_date(dt)
    params = {
        "l": "zh-tw",
        "o": "csv",
        "se": "EW",
        "t": "D",
        "d": roc,
        "s": "0,asc,0",
    }

    errors: List[str] = []

    for url in TPEX_CANDIDATES:
        try:
            text = request_text(url, params=params, timeout=timeout)
            rows = parse_tpex_csv_text(text, dt)
            if rows:
                return rows
        except Exception as exc:
            errors.append(f"{url} -> {exc}")

    joined = "\n".join(errors)
    raise RuntimeError(f"TPEX 抓取失敗。\n{joined}")


def merge_rows(twse_rows: List[Dict[str, object]], tpex_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    merged: Dict[str, Dict[str, object]] = {}

    for row in twse_rows + tpex_rows:
        merged[str(row["code"])] = row

    return [merged[k] for k in sorted(merged.keys())]


def main() -> None:
    args = parse_args()
    dt = validate_date(args.date)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"{dt.strftime('%Y-%m-%d')}.json"

    print(f"=== 開始抓資料：{dt.strftime('%Y-%m-%d')} ===")

    twse_rows = fetch_twse_day(dt, timeout=args.timeout)
    print(f"TWSE 成功：{len(twse_rows)} 筆")

    time.sleep(args.sleep)

    tpex_rows = fetch_tpex_day(dt, timeout=args.timeout)
    print(f"TPEX 成功：{len(tpex_rows)} 筆")

    merged = merge_rows(twse_rows, tpex_rows)

    with outpath.open("w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"輸出完成：{outpath}")
    print(f"合併總筆數：{len(merged)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("已中止")
        sys.exit(1)
    except Exception as exc:
        print(f"執行失敗：{exc}")
        sys.exit(1)