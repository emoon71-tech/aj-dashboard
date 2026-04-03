#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_eps.py

用途：
1. 透過 FinMind TaiwanStockPER 抓取個股本益比（PER）資料
2. 從近 3 年 PER 歷史，計算合理 PE 區間（20 / 80 百分位）
3. 輸出成 data/epsMap.json（與前端 EPS 物件相容格式）
4. 內建 90 天快取，不重複抓取

輸出格式（與前端 EPS 物件相容）：
{
  "2330": { "n": "台積電", "e": 110.72, "mn": 14.82, "mx": 27.76, "updatedAt": "2026-04-03" },
  "__meta__": { "updatedAt": "2026-04-03", "count": 20 }
}

使用方式：
python fetch_eps.py
python fetch_eps.py --force        # 強制重抓，忽略快取
python fetch_eps.py --token YOUR_TOKEN

環境變數（建議用這個）：
FINMIND_TOKEN=your_token_here
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import requests

# ── 設定 ──────────────────────────────────────────────────────
FINMIND_API = "https://api.finmindtrade.com/api/v4/data"
CACHE_DAYS  = 90
OUTPUT_FILE = Path("data/epsMap.json")
SLEEP_SEC   = 0.8   # 每次 API 請求間隔（保守一點避免超限）

# 預設追蹤股票（對應前端 EPS 物件，ETF 不列入）
DEFAULT_CODES = [
    # AI 股池
    "2330","2317","2382","2308","6669","3231","2356","2376",
    "3706","3017","3324","3037","8046","3034","2379","3711",
    "2454","2449","2345","6274",
    # 自選股
    "2886","2890","3045","1216","2891","2880","2887","2002",
    "2883","2892","6505","3661","5880","2301","2412","2615",
    "3665","6919","2881","2882","2603","1301","2395","2383",
    "2408","1303","2059","2327",
]

NAME_MAP = {
    # AI 股池
    "2330": "台積電", "2317": "鴻海",   "2382": "廣達",   "2308": "台達電",
    "6669": "緯穎",   "3231": "緯創",   "2356": "英業達", "2376": "技嘉",
    "3706": "神達",   "3017": "奇鋐",   "3324": "雙鴻",   "3037": "欣興",
    "8046": "南電",   "3034": "聯詠",   "2379": "瑞昱",   "3711": "日月光",
    "2454": "聯發科", "2449": "京元電", "2345": "智邦",   "6274": "台燿",
    # 自選股
    "2886": "兆豐金", "2890": "永豐金", "3045": "台灣大", "1216": "統一",
    "2891": "中信金", "2880": "華南金", "2887": "台新金", "2002": "中鋼",
    "2883": "開發金", "2892": "第一金", "6505": "台塑化", "3661": "世芯",
    "5880": "合庫金", "2301": "光寶科", "2412": "中華電", "2615": "萬海",
    "3665": "貿聯",   "6919": "康霈",   "2881": "富邦金", "2882": "國泰金",
    "2603": "長榮",   "1301": "台塑",   "2395": "研華",   "2383": "台光電",
    "2408": "南亞科", "1303": "南亞",   "2059": "川湖",   "2327": "國巨",
}


# ── 工具函式 ───────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="透過 FinMind PER 資料輸出 epsMap.json")
    parser.add_argument("--token",   default="", help="FinMind API Token")
    parser.add_argument("--codes",   default="", help="指定股票代號，逗號分隔")
    parser.add_argument("--force",   action="store_true", help="強制重抓，忽略快取")
    parser.add_argument("--outfile", default=str(OUTPUT_FILE), help="輸出路徑")
    parser.add_argument("--sleep",   type=float, default=SLEEP_SEC)
    return parser.parse_args()


def get_token(args_token: str) -> str:
    token = args_token or os.environ.get("FINMIND_TOKEN", "")
    if not token:
        raise SystemExit(
            "❌ 找不到 FinMind Token！\n"
            "請用：export FINMIND_TOKEN=your_token\n"
            "或：  python fetch_eps.py --token your_token"
        )
    return token


def load_cache(outfile: Path) -> Optional[Dict]:
    if not outfile.exists():
        return None
    try:
        return json.loads(outfile.read_text(encoding="utf-8"))
    except Exception:
        return None


def is_cache_fresh(cache: Dict, max_days: int) -> bool:
    updated = cache.get("__meta__", {}).get("updatedAt", "")
    if not updated:
        return False
    try:
        return (datetime.now() - datetime.fromisoformat(updated)).days < max_days
    except Exception:
        return False


def finmind_get(token: str, dataset: str, stock_id: str, start_date: str, timeout: int = 20) -> List[Dict]:
    params = {
        "dataset":    dataset,
        "data_id":    stock_id,
        "start_date": start_date,
        "token":      token,
    }
    resp = requests.get(FINMIND_API, params=params, timeout=timeout)
    resp.raise_for_status()
    result = resp.json()
    if result.get("status") != 200:
        raise RuntimeError(f"FinMind 錯誤：{result.get('msg', '未知錯誤')}")
    return result.get("data", [])


def get_per_data(token: str, code: str, sleep: float) -> Optional[Dict]:
    """
    抓 TaiwanStockPER，回傳：
    {
      "per_latest": 最新 PER（等同股價/EPS，即時計算）
      "per_min":    近3年 PER 20百分位
      "per_max":    近3年 PER 80百分位
      "eps":        用最新 PER 反推（price / PER）— 僅供參考，不一定準
    }
    """
    start_date = (datetime.now() - timedelta(days=3*365)).strftime("%Y-%m-%d")

    try:
        rows = finmind_get(token, "TaiwanStockPER", code, start_date=start_date)
    except Exception as e:
        print(f"  ⚠️  {code} PER 抓取失敗：{e}")
        return None

    time.sleep(sleep)

    if not rows:
        return None

    # 過濾掉 PER <= 0 的資料（虧損季）
    valid = [r for r in rows if float(r.get("PER") or 0) > 0]
    if not valid:
        return None

    # 依日期排序
    valid.sort(key=lambda r: r.get("date", ""))

    per_list = [float(r["PER"]) for r in valid]
    per_list.sort()
    n = len(per_list)

    per_min = round(per_list[int(n * 0.20)], 2)
    per_max = round(per_list[int(n * 0.80)], 2)
    per_latest = round(float(valid[-1]["PER"]), 2)

    return {
        "per_latest": per_latest,
        "per_min":    per_min,
        "per_max":    per_max,
    }


def get_eps_from_financial(token: str, code: str, sleep: float) -> Optional[float]:
    """
    抓 TaiwanStockFinancialStatements，取最近 4 季 EPS 加總
    """
    start_date = (datetime.now() - timedelta(days=2*365)).strftime("%Y-%m-%d")

    try:
        rows = finmind_get(token, "TaiwanStockFinancialStatements", code, start_date=start_date)
    except Exception as e:
        print(f"  ⚠️  {code} 財報抓取失敗：{e}")
        return None

    time.sleep(sleep)

    eps_rows = [r for r in rows if str(r.get("type", "")).strip().upper() == "EPS"]
    if not eps_rows:
        return None

    eps_rows.sort(key=lambda r: r.get("date", ""), reverse=True)
    recent = eps_rows[:4]
    total = sum(float(r.get("value", 0) or 0) for r in recent)
    return round(total, 2) if total != 0 else None


# ── 主流程 ────────────────────────────────────────────────────
def main() -> None:
    args   = parse_args()
    token  = get_token(args.token)
    outfile = Path(args.outfile)

    codes = (
        [c.strip() for c in args.codes.split(",") if c.strip()]
        if args.codes else DEFAULT_CODES
    )

    print(f"=== fetch_eps.py 開始 ===")
    print(f"股票數量：{len(codes)}")
    print(f"輸出路徑：{outfile}")

    # 快取判斷
    cache = load_cache(outfile)
    if not args.force and cache and is_cache_fresh(cache, CACHE_DAYS):
        meta = cache.get("__meta__", {})
        print(f"✅ 快取還新鮮（更新於 {meta.get('updatedAt')}），跳過。加 --force 可強制重抓。")
        return

    print("📥 開始抓取..." if not args.force else "⚡ 強制重抓模式")

    result: Dict = {}
    today = datetime.now().strftime("%Y-%m-%d")
    success = 0
    fail = 0

    for i, code in enumerate(codes, 1):
        name = NAME_MAP.get(code, code)
        print(f"[{i:2d}/{len(codes)}] {code} {name} ...", end=" ", flush=True)

        # Step 1：抓 PER 歷史
        per_data = get_per_data(token, code, args.sleep)

        if not per_data:
            print("❌ 無 PER 資料，跳過")
            fail += 1
            continue

        # Step 2：抓 EPS（財報）
        eps = get_eps_from_financial(token, code, args.sleep)

        # 如果財報抓不到，用 PER 反推 EPS（不準但有值）
        if eps is None or eps == 0:
            eps = None  # 讓前端知道沒有 EPS

        result[code] = {
            "n":         name,
            "e":         eps,
            "mn":        per_data["per_min"],
            "mx":        per_data["per_max"],
            "per":       per_data["per_latest"],
            "updatedAt": today,
        }

        eps_str = str(eps) if eps else "無"
        print(f"✅ EPS={eps_str}  PE {per_data['per_min']}x ~ {per_data['per_max']}x  現在={per_data['per_latest']}x")
        success += 1

    result["__meta__"] = {
        "updatedAt": today,
        "count":     success,
    }

    outfile.parent.mkdir(parents=True, exist_ok=True)
    outfile.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n=== 完成 ===")
    print(f"成功：{success} 檔　失敗：{fail} 檔")
    print(f"輸出：{outfile}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n已中止")
        sys.exit(1)
    except Exception as exc:
        print(f"\n執行失敗：{exc}")
        sys.exit(1)
