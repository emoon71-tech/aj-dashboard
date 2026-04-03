#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_eps.py

用途：
1. 透過 FinMind API 抓取台股年度 EPS（每股盈餘）
2. 計算合理本益比區間（歷史 PE 20 / 80 百分位）
3. 輸出成 data/epsMap.json
4. 內建 90 天快取，不重複抓取

輸出格式（與前端 EPS 物件相容）：
{
  "2330": { "n": "台積電", "e": 110.72, "mn": 14.82, "mx": 27.76, "updatedAt": "2026-04-03" },
  ...
  "__meta__": { "updatedAt": "2026-04-03", "count": 20 }
}

使用方式：
python fetch_eps.py
python fetch_eps.py --force        # 強制重抓，忽略快取
python fetch_eps.py --token YOUR_TOKEN
python fetch_eps.py --codes 2330,2317,2454

環境變數（建議用這個，不要寫在程式裡）：
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

# ── 設定 ───────────────────────────────────────────────────────
FINMIND_API = "https://api.finmindtrade.com/api/v4/data"
CACHE_DAYS  = 90       # 幾天內不重抓
OUTPUT_FILE = Path("data/epsMap.json")
SLEEP_SEC   = 0.5      # 每次 API 請求間隔

# 預設追蹤股票清單（對應前端 EPS 物件）
DEFAULT_CODES = [
    "2330", "2317", "2454", "2382", "3711", "2308",
    "2881", "2882", "2886", "2891", "2884", "2885", "2892",
    "6669", "2303", "2379", "3034", "3014", "6770",
    "6274", "3037", "8046", "3017", "3324", "3231",
    "2356", "2376", "3706", "2449", "2345",
]

# 股票名稱對照（FinMind 有時候回傳英文，補一份中文）
NAME_MAP = {
    "2330": "台積電", "2317": "鴻海",   "2454": "聯發科", "2382": "廣達",
    "3711": "日月光", "2308": "台達電", "2881": "富邦金", "2882": "國泰金",
    "2886": "兆豐金", "2891": "中信金", "2884": "玉山金", "2885": "元大金",
    "2892": "第一金", "6669": "緯穎",   "2303": "聯電",   "2379": "瑞昱",
    "3034": "聯詠",   "3014": "安勤",   "6770": "力積電", "6274": "台燿",
    "3037": "欣興",   "8046": "南電",   "3017": "奇鋐",   "3324": "雙鴻",
    "3231": "緯創",   "2356": "英業達", "2376": "技嘉",   "3706": "神達",
    "2449": "京元電", "2345": "智邦",
}


# ── 工具函式 ────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="透過 FinMind 抓取 EPS，輸出 epsMap.json")
    parser.add_argument("--token",  default="", help="FinMind API Token（也可用環境變數 FINMIND_TOKEN）")
    parser.add_argument("--codes",  default="", help="指定股票代號，逗號分隔，例如 2330,2317")
    parser.add_argument("--force",  action="store_true", help="強制重抓，忽略快取")
    parser.add_argument("--outfile", default=str(OUTPUT_FILE), help="輸出檔案路徑")
    parser.add_argument("--sleep",  type=float, default=SLEEP_SEC, help="API 請求間隔秒數")
    return parser.parse_args()


def get_token(args_token: str) -> str:
    token = args_token or os.environ.get("FINMIND_TOKEN", "")
    if not token:
        raise SystemExit(
            "❌ 找不到 FinMind Token！\n"
            "請用以下任一方式提供：\n"
            "  1. 環境變數：export FINMIND_TOKEN=your_token\n"
            "  2. 指令參數：python fetch_eps.py --token your_token"
        )
    return token


def load_cache(outfile: Path) -> Optional[Dict]:
    if not outfile.exists():
        return None
    try:
        data = json.loads(outfile.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def is_cache_fresh(cache: Dict, max_days: int) -> bool:
    meta = cache.get("__meta__", {})
    updated = meta.get("updatedAt", "")
    if not updated:
        return False
    try:
        dt = datetime.fromisoformat(updated)
        return (datetime.now() - dt).days < max_days
    except Exception:
        return False


def finmind_get(token: str, dataset: str, stock_id: str, timeout: int = 20) -> List[Dict]:
    params = {
        "dataset":  dataset,
        "data_id":  stock_id,
        "token":    token,
    }
    resp = requests.get(FINMIND_API, params=params, timeout=timeout)
    resp.raise_for_status()
    result = resp.json()
    if result.get("status") != 200:
        raise RuntimeError(f"FinMind 錯誤：{result.get('msg', '未知錯誤')}")
    return result.get("data", [])


def get_latest_eps(token: str, code: str) -> Optional[float]:
    """
    抓 TaiwanStockEPS（免費版可用）
    欄位格式：{ "date": "2024-Q4", "stock_id": "2330", "eps": 14.45 }
    取最近 4 季加總 = 年度 EPS
    """
    try:
        rows = finmind_get(token, "TaiwanStockEPS", code)
    except Exception as e:
        print(f"  ⚠️  {code} EPS 抓取失敗：{e}")
        return None

    if not rows:
        return None

    # 依日期排序（最新在前）
    rows.sort(key=lambda r: r.get("date", ""), reverse=True)

    # 取最近 4 季加總
    recent = rows[:4]
    if not recent:
        return None

    total = sum(float(r.get("eps", 0) or 0) for r in recent)
    return round(total, 2)


def get_pe_range(token: str, code: str, eps: float) -> tuple[float, float]:
    """
    抓歷史股價，算本益比區間
    取近 3 年歷史 PE，20 百分位 = mn，80 百分位 = mx
    """
    if eps <= 0:
        return 10.0, 20.0  # 無法計算時給預設值

    try:
        rows = finmind_get(token, "TaiwanStockPrice", code)
    except Exception as e:
        print(f"  ⚠️  {code} 股價抓取失敗：{e}")
        return 10.0, 20.0

    # 只取近 3 年資料
    cutoff = (datetime.now() - timedelta(days=3*365)).strftime("%Y-%m-%d")
    rows = [r for r in rows if r.get("date", "") >= cutoff]

    if not rows:
        return 10.0, 20.0

    # 算每天的 PE
    pe_list = []
    for r in rows:
        close = float(r.get("close", 0) or 0)
        if close > 0 and eps > 0:
            pe = close / eps
            if 3 <= pe <= 100:  # 過濾異常值
                pe_list.append(pe)

    if len(pe_list) < 10:
        return 10.0, 20.0

    pe_list.sort()
    n = len(pe_list)
    mn = round(pe_list[int(n * 0.20)], 2)
    mx = round(pe_list[int(n * 0.80)], 2)

    return mn, mx


# ── 主流程 ───────────────────────────────────────────────────────
def main() -> None:
    args   = parse_args()
    token  = get_token(args.token)
    outfile = Path(args.outfile)

    codes = (
        [c.strip() for c in args.codes.split(",") if c.strip()]
        if args.codes
        else DEFAULT_CODES
    )

    print(f"=== fetch_eps.py 開始 ===")
    print(f"股票數量：{len(codes)}")
    print(f"輸出路徑：{outfile}")

    # ── 快取判斷 ──────────────────────────────────────────────────
    cache = load_cache(outfile)

    if not args.force and cache and is_cache_fresh(cache, CACHE_DAYS):
        meta = cache.get("__meta__", {})
        print(f"✅ 快取還新鮮（更新於 {meta.get('updatedAt')}），跳過抓取。")
        print("   如需強制重抓，請加 --force 參數。")
        return

    if args.force:
        print("⚡ 強制重抓模式")
    else:
        print("📥 快取過期或不存在，開始抓取...")

    # ── 開始抓資料 ─────────────────────────────────────────────────
    result: Dict = {}
    today = datetime.now().strftime("%Y-%m-%d")
    success_count = 0
    fail_count = 0

    for i, code in enumerate(codes, 1):
        print(f"[{i:2d}/{len(codes)}] {code} {NAME_MAP.get(code, '')} ...", end=" ", flush=True)

        # 抓 EPS
        eps = get_latest_eps(token, code)
        if eps is None or eps == 0:
            print("❌ 無 EPS 資料，跳過")
            fail_count += 1
            time.sleep(args.sleep)
            continue

        time.sleep(args.sleep)

        # 算 PE 區間
        mn, mx = get_pe_range(token, code, eps)

        time.sleep(args.sleep)

        result[code] = {
            "n":         NAME_MAP.get(code, code),
            "e":         eps,
            "mn":        mn,
            "mx":        mx,
            "updatedAt": today,
        }

        print(f"✅ EPS={eps}  PE {mn}x ~ {mx}x")
        success_count += 1

    # ── 加 meta ────────────────────────────────────────────────────
    result["__meta__"] = {
        "updatedAt": today,
        "count":     success_count,
    }

    # ── 輸出 ───────────────────────────────────────────────────────
    outfile.parent.mkdir(parents=True, exist_ok=True)
    outfile.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n=== 完成 ===")
    print(f"成功：{success_count} 檔")
    print(f"失敗：{fail_count} 檔")
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
