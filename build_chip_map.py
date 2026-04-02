#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_chip_map.py

用途：
讀取 raw_chip/*.json，彙總成 chipMap.json

輸入格式（raw_chip/YYYY-MM-DD.json）：
[
  { "code": "2330", "date": "2026-04-02", "foreign": 1200, "trust": 300 },
  ...
]

輸出格式（chipMap.json）：
{
  "2330": {
    "foreign3d": 5200,
    "foreign5d": 8600,
    "trust3d": 300,
    "trust5d": 900,
    "foreignBuyDays3": 2,
    "trustBuyDays3": 1
  }
}
"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Any


RAW_DIR = Path("raw_chip")
OUT_FILE = Path("chipMap.json")


def is_date_filename(path: Path) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}\.json", path.name))


def load_day_file(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"{path} 不是陣列格式")

    rows: List[Dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue

        code = str(item.get("code", "")).strip()
        date = str(item.get("date", "")).strip()

        if not code or not date:
            continue

        rows.append(
            {
                "code": code,
                "date": date,
                "foreign": to_int(item.get("foreign", 0)),
                "trust": to_int(item.get("trust", 0)),
            }
        )

    return rows


def to_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)

        s = str(value).strip()
        if not s:
            return 0

        s = s.replace(",", "").replace("＋", "").replace("+", "")
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1]

        s = re.sub(r"[^0-9\-]", "", s)
        if s in {"", "-"}:
            return 0

        return int(s)
    except Exception:
        return 0


def collect_all_rows(raw_dir: Path) -> List[Dict[str, Any]]:
    if not raw_dir.exists():
        raise FileNotFoundError(f"找不到資料夾：{raw_dir}")

    files = sorted([p for p in raw_dir.glob("*.json") if is_date_filename(p)], key=lambda p: p.stem)

    if not files:
        raise FileNotFoundError(f"{raw_dir} 裡沒有 YYYY-MM-DD.json 檔案")

    all_rows: List[Dict[str, Any]] = []
    for path in files:
        rows = load_day_file(path)
        all_rows.extend(rows)

    return all_rows


def build_chip_map(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    by_code: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for row in rows:
        by_code[row["code"]].append(row)

    chip_map: Dict[str, Dict[str, int]] = {}

    for code, code_rows in by_code.items():
        # 依日期舊 -> 新 排序，取最後 3 / 5 天
        code_rows = sorted(code_rows, key=lambda x: x["date"])
        last3 = code_rows[-3:]
        last5 = code_rows[-5:]

        foreign3d = sum(r["foreign"] for r in last3)
        foreign5d = sum(r["foreign"] for r in last5)
        trust3d = sum(r["trust"] for r in last3)
        trust5d = sum(r["trust"] for r in last5)

        foreign_buy_days_3 = sum(1 for r in last3 if r["foreign"] > 0)
        trust_buy_days_3 = sum(1 for r in last3 if r["trust"] > 0)

        chip_map[code] = {
            "foreign3d": foreign3d,
            "foreign5d": foreign5d,
            "trust3d": trust3d,
            "trust5d": trust5d,
            "foreignBuyDays3": foreign_buy_days_3,
            "trustBuyDays3": trust_buy_days_3,
        }

    return dict(sorted(chip_map.items(), key=lambda kv: kv[0]))


def main() -> None:
    rows = collect_all_rows(RAW_DIR)
    chip_map = build_chip_map(rows)

    with OUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(chip_map, f, ensure_ascii=False, indent=2)

    print(f"讀取 raw 資料筆數：{len(rows)}")
    print(f"輸出完成：{OUT_FILE}")
    print(f"股票總數：{len(chip_map)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("已中止")
        sys.exit(1)
    except Exception as exc:
        print(f"執行失敗：{exc}")
        sys.exit(1)