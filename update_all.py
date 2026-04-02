#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
update_all.py

用途：
1. 抓最近 N 天（預設 5 天）TWSE / TPEX 法人資料
2. 產出 raw_chip/YYYY-MM-DD.json
3. 重建 chipMap.json
4. 順手同步一份到 data/chipMap.json 給 GitHub Pages 用

使用方式：
python update_all.py
python update_all.py --days 5
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path


RAW_DIR = Path("raw_chip")
CHIPMAP_FILE = Path("chipMap.json")
DATA_DIR = Path("data")
DATA_CHIPMAP_FILE = DATA_DIR / "chipMap.json"


def parse_args():
    parser = argparse.ArgumentParser(description="一鍵更新 raw_chip + chipMap.json")
    parser.add_argument("--days", type=int, default=5, help="回推幾天，預設 5")
    return parser.parse_args()


def iter_recent_dates(days: int):
    today = datetime.today().date()
    dates = []
    for i in range(days * 3):
        d = today - timedelta(days=i)
        if d.weekday() < 5:  # 0~4 = 週一到週五
            dates.append(d)
        if len(dates) >= days:
            break
    return sorted(dates)


def run_cmd(cmd: list[str]) -> None:
    print(">>", " ".join(cmd))
    result = subprocess.run(cmd, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"指令失敗：{' '.join(cmd)}")


def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def validate_chipmap(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"找不到 {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError(f"{path} 格式錯誤，不是 object")

    print(f"✅ 驗證成功：{path}，股票數 {len(data)}")


def main():
    args = parse_args()
    dates = iter_recent_dates(args.days)

    print("=== 開始一鍵更新 ===")
    print("目標日期：")
    for d in dates:
        print(" -", d.isoformat())

    for d in dates:
        cmd = [
            sys.executable,
            "fetch_chip_day.py",
            "--date",
            d.isoformat(),
            "--outdir",
            str(RAW_DIR),
        ]
        try:
            run_cmd(cmd)
        except Exception as e:
            print(f"⚠️ 跳過 {d.isoformat()}：{e}")

    run_cmd([sys.executable, "build_chip_map.py"])

    validate_chipmap(CHIPMAP_FILE)

    ensure_data_dir()
    shutil.copy2(CHIPMAP_FILE, DATA_CHIPMAP_FILE)

    validate_chipmap(DATA_CHIPMAP_FILE)

    print("=== 全部完成 ===")
    print(f"raw 資料夾：{RAW_DIR}")
    print(f"主輸出：{CHIPMAP_FILE}")
    print(f"網站用輸出：{DATA_CHIPMAP_FILE}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("已中止")
        sys.exit(1)
    except Exception as exc:
        print(f"執行失敗：{exc}")
        sys.exit(1)