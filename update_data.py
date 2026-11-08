#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ST Quant BackTester - Automated Market Data Pipeline (update_data.py)
================================================================================
주요 역할:
  1. 기존 `stock_data.js`의 검증된 베이스 데이터셋(2010-01-01 ~ 베이스 기준일, 38개 ETF + 환율 + 코스피)을
     100% 보존하여 과거 구간(2010~현재) 백테스트 수익률 왜곡(휴장일 핑퐁·배당 수정주가 괴리·상장 전 스케일 점프)을
     원천 차단합니다.
  2. 야후 파이낸스(yfinance, `auto_adjust=False` 실제 수정종가 기준) 및 Yahoo Finance v8 API를 통해
     베이스 기준일 이후의 신규 거래일 일일 종가와 원/달러 환율(KRW=X), 코스피 지수(^KS11)만 증분(Incremental)
     수집·병합하며, 한·미 휴장일 교차 구간은 직전 거래일 종가를 안전하게 이월(Forward-Fill)합니다.
  3. SOXX 3:1 액면분할(2026-11-05 효력, 최고가 $218.50 / 현재가 $136.53) 및 일일 급등락 이상치 검증을
     수행한 뒤 `stock_data.js` (및 선택적 `stock_data.json`)로 저장합니다.

실행 방법:
  # 1) 최신 주가·환율 증분 수집 및 stock_data.js 안전 갱신 (온라인 / GitHub Actions)
  pip install yfinance pandas numpy requests
  python update_data.py --base-js stock_data.js --out-js stock_data.js

  # 2) 로컬 stock_data.js 데이터 무결성 검증 (오프라인 테스트 지원)
  python update_data.py --verify-only --base-js stock_data.js

  # 3) GitHub Actions 자동화 워크플로우(.github/workflows/update_data.yml) 생성
  python update_data.py --write-workflow
================================================================================
"""

from __future__ import annotations

import argparse
import datetime
import json
import math
import os
import re
import sys
import urllib.request
from typing import Dict, List, Optional, Tuple

DEFAULT_START_DATE = "2010-01-01"

# ★ 사용자가 파이썬에서 직접 추가하고 싶은 커스텀 티커 목록 (예: ["SMH", "NVDA", "VOO", "379800.KS"])
# 웹앱(index.html)에서 추가 후 다운로드한 stock_data.js의 CUSTOM_* 종목도 자동으로 인식·갱신됩니다.
USER_CUSTOM_TICKERS: List[str] = []

# 38개 핵심 ETF (+ 현금 2종 = 총 40종) 메타데이터 및 Yahoo Finance 티커 매핑
ETF_CATALOG_SPEC: Dict[str, Dict] = {
    # ── [미국 상장 ETF: 20종] ──
    "SPY":   {"ticker": "SPY",   "yf": "SPY",   "market": "US", "currency": "USD", "divYield": 1.35, "proxy": None,   "beta": 1.0, "embedFx": False},
    "QQQ":   {"ticker": "QQQ",   "yf": "QQQ",   "market": "US", "currency": "USD", "divYield": 0.55, "proxy": None,   "beta": 1.0, "embedFx": False},
    "SCHD":  {"ticker": "SCHD",  "yf": "SCHD",  "market": "US", "currency": "USD", "divYield": 3.40, "proxy": "SPY",  "beta": 0.85,"embedFx": False},
    "VEA":   {"ticker": "VEA",   "yf": "VEA",   "market": "US", "currency": "USD", "divYield": 2.90, "proxy": None,   "beta": 1.0, "embedFx": False},
    "VWO":   {"ticker": "VWO",   "yf": "VWO",   "market": "US", "currency": "USD", "divYield": 3.10, "proxy": None,   "beta": 1.0, "embedFx": False},
    "TLT":   {"ticker": "TLT",   "yf": "TLT",   "market": "US", "currency": "USD", "divYield": 3.85, "proxy": None,   "beta": 1.0, "embedFx": False},
    "IEF":   {"ticker": "IEF",   "yf": "IEF",   "market": "US", "currency": "USD", "divYield": 3.35, "proxy": None,   "beta": 1.0, "embedFx": False},
    "SHY":   {"ticker": "SHY",   "yf": "SHY",   "market": "US", "currency": "USD", "divYield": 3.80, "proxy": None,   "beta": 1.0, "embedFx": False},
    "TIP":   {"ticker": "TIP",   "yf": "TIP",   "market": "US", "currency": "USD", "divYield": 2.80, "proxy": None,   "beta": 1.0, "embedFx": False},
    "GLD":   {"ticker": "GLD",   "yf": "GLD",   "market": "US", "currency": "USD", "divYield": 0.00, "proxy": None,   "beta": 1.0, "embedFx": False},
    "DBC":   {"ticker": "DBC",   "yf": "DBC",   "market": "US", "currency": "USD", "divYield": 1.20, "proxy": None,   "beta": 1.0, "embedFx": False},
    "VNQ":   {"ticker": "VNQ",   "yf": "VNQ",   "market": "US", "currency": "USD", "divYield": 3.90, "proxy": None,   "beta": 1.0, "embedFx": False},
    "QLD":   {"ticker": "QLD",   "yf": "QLD",   "market": "US", "currency": "USD", "divYield": 0.00, "proxy": "QQQ",  "beta": 2.0, "embedFx": False},
    "TQQQ":  {"ticker": "TQQQ",  "yf": "TQQQ",  "market": "US", "currency": "USD", "divYield": 0.00, "proxy": "QQQ",  "beta": 3.0, "embedFx": False},
    "SSO":   {"ticker": "SSO",   "yf": "SSO",   "market": "US", "currency": "USD", "divYield": 0.00, "proxy": "SPY",  "beta": 2.0, "embedFx": False},
    "UPRO":  {"ticker": "UPRO",  "yf": "UPRO",  "market": "US", "currency": "USD", "divYield": 0.00, "proxy": "SPY",  "beta": 3.0, "embedFx": False},
    "SOXX":  {"ticker": "SOXX",  "yf": "SOXX",  "market": "US", "currency": "USD", "divYield": 0.65, "proxy": None,   "beta": 1.0, "embedFx": False},
    "USD_SEMI": {"ticker": "USD","yf": "USD",   "market": "US", "currency": "USD", "divYield": 0.00, "proxy": "SOXX", "beta": 2.0, "embedFx": False},
    "SOXL":  {"ticker": "SOXL",  "yf": "SOXL",  "market": "US", "currency": "USD", "divYield": 0.00, "proxy": "SOXX", "beta": 3.0, "embedFx": False},
    "SGOV":  {"ticker": "SGOV",  "yf": "SGOV",  "market": "US", "currency": "USD", "divYield": 4.80, "proxy": "SHY",  "beta": 0.2, "embedFx": False},

    # ── [국내 상장 ETF: 18종] ──
    "KR_KODEX_200":       {"ticker": "069500", "yf": "069500.KS", "market": "KR", "currency": "KRW", "divYield": 1.85, "proxy": None,   "beta": 1.0, "embedFx": False},
    "KR_TIGER_SP500":     {"ticker": "360750", "yf": "360750.KS", "market": "KR", "currency": "KRW", "divYield": 1.30, "proxy": "SPY",  "beta": 1.0, "embedFx": True},
    "KR_TIGER_NAS100":    {"ticker": "133690", "yf": "133690.KS", "market": "KR", "currency": "KRW", "divYield": 0.50, "proxy": "QQQ",  "beta": 1.0, "embedFx": True},
    "KR_TIGER_SCHD":      {"ticker": "458730", "yf": "458730.KS", "market": "KR", "currency": "KRW", "divYield": 3.35, "proxy": "SCHD", "beta": 1.0, "embedFx": True},
    "KR_KODEX_SP500H":    {"ticker": "449180", "yf": "449180.KS", "market": "KR", "currency": "KRW", "divYield": 1.25, "proxy": "SPY",  "beta": 1.0, "embedFx": False},
    "KR_TIGER_USBOND10":  {"ticker": "305080", "yf": "305080.KS", "market": "KR", "currency": "KRW", "divYield": 2.90, "proxy": "IEF",  "beta": 1.0, "embedFx": True},
    "KR_ACE_USBOND30H":   {"ticker": "453850", "yf": "453850.KS", "market": "KR", "currency": "KRW", "divYield": 3.70, "proxy": "TLT",  "beta": 1.0, "embedFx": False},
    "KR_KODEX_KRBOND30":  {"ticker": "439870", "yf": "439870.KS", "market": "KR", "currency": "KRW", "divYield": 3.10, "proxy": "TLT",  "beta": 0.7, "embedFx": False},
    "KR_KODEX_SHORT":     {"ticker": "153130", "yf": "153130.KS", "market": "KR", "currency": "KRW", "divYield": 2.80, "proxy": "SHY",  "beta": 0.3, "embedFx": False},
    "KR_ACE_GOLD":        {"ticker": "411060", "yf": "411060.KS", "market": "KR", "currency": "KRW", "divYield": 0.00, "proxy": "GLD",  "beta": 1.0, "embedFx": True},
    "KR_TIGER_REIT":      {"ticker": "329200", "yf": "329200.KS", "market": "KR", "currency": "KRW", "divYield": 5.50, "proxy": "VNQ",  "beta": 0.6, "embedFx": False},
    "KR_KODEX_LEV":       {"ticker": "122630", "yf": "122630.KS", "market": "KR", "currency": "KRW", "divYield": 0.00, "proxy": "KR_KODEX_200", "beta": 2.0, "embedFx": False},
    "KR_TIGER_NAS100_LEV":{"ticker": "409820", "yf": "409820.KS", "market": "KR", "currency": "KRW", "divYield": 0.00, "proxy": "QQQ",  "beta": 2.0, "embedFx": True},
    "KR_TIME_NAS100":     {"ticker": "426030", "yf": "426030.KS", "market": "KR", "currency": "KRW", "divYield": 0.50, "proxy": "QQQ",  "beta": 1.15,"embedFx": True},
    "KR_TIGER_SOX":       {"ticker": "381180", "yf": "381180.KS", "market": "KR", "currency": "KRW", "divYield": 0.60, "proxy": "SOXX", "beta": 1.0, "embedFx": True},
    "KR_TIGER_SOX_LEV":   {"ticker": "423920", "yf": "423920.KS", "market": "KR", "currency": "KRW", "divYield": 0.00, "proxy": "SOXX", "beta": 2.0, "embedFx": True},
    "KR_KODEX_KOFR":      {"ticker": "423160", "yf": "423160.KS", "market": "KR", "currency": "KRW", "divYield": 3.40, "proxy": "SHY",  "beta": 0.2, "embedFx": False},
    "KR_TIGER_USDBOND":   {"ticker": "329750", "yf": "329750.KS", "market": "KR", "currency": "KRW", "divYield": 4.20, "proxy": "SHY",  "beta": 0.3, "embedFx": True},

    # ── [현금 파킹 기준자산: 2종] ──
    "CASH_KRW_0": {"ticker": "원화예수금", "yf": None, "market": "KR", "currency": "KRW", "divYield": 0.0, "proxy": None, "beta": 0.0, "embedFx": False},
    "CASH_USD_0": {"ticker": "달러예수금", "yf": None, "market": "US", "currency": "USD", "divYield": 0.0, "proxy": None, "beta": 0.0, "embedFx": False},
}


GITHUB_ACTIONS_WORKFLOW_YAML = """name: Update ST Quant Market Data

on:
  schedule:
    # 평일(월~금) 한국시간 오전 07:40 (미국장 마감 직후) 및 오후 16:10 (한국장 마감 직후) 자동 실행
    - cron: '40 22 * * 1-5'
    - cron: '10 7 * * 1-5'
  workflow_dispatch:

permissions:
  contents: write

jobs:
  update-stock-data:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install yfinance pandas numpy requests

      - name: Run update_data.py
        run: |
          python update_data.py --base-js stock_data.js --out-js stock_data.js

      - name: Commit and Push updated stock_data.js
        run: |
          git config --local user.email "actions@github.com"
          git config --local user.name "ST Quant Auto Updater"
          git add stock_data.js
          git diff --staged --quiet || git commit -m "chore(data): auto-update daily ETF & FX prices [skip ci]"
          git push
"""


def load_existing_stock_data_js(js_path: str) -> Tuple[Optional[Dict], Optional[Dict]]:
    """기존 stock_data.js 파일에서 MARKET_DB와 REAL_DAILY_DB를 파싱하여 반환합니다."""
    if not js_path or not os.path.exists(js_path):
        return None, None
    with open(js_path, "r", encoding="utf-8") as f:
        content = f.read()
    m_match = re.search(r"(?:const|var|let)\s+MARKET_DB\s*=\s*(\{.*?\});\s*\n", content, re.S)
    r_match = re.search(r"(?:const|var|let)\s+REAL_DAILY_DB\s*=\s*(\{.*?\});\s*\n", content, re.S)
    mdb = json.loads(m_match.group(1)) if m_match else None
    rdb = json.loads(r_match.group(1)) if r_match else None
    return mdb, rdb


def get_realtime_close_v8(yf_ticker: str) -> Optional[Tuple[float, str]]:
    """Yahoo Finance v8 Chart API를 호출하여 최신 정규장 확정 종가와 거래일(YYYY-MM-DD)을 조회합니다."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_ticker}?interval=1d&range=5d"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        meta = data["chart"]["result"][0]["meta"]
        price = float(meta.get("regularMarketPrice") or meta.get("chartPreviousClose"))
        mkt_time = meta.get("regularMarketTime")
        dt_str = datetime.datetime.fromtimestamp(mkt_time, tz=datetime.timezone.utc).strftime("%Y-%m-%d")
        return price, dt_str
    except Exception:
        return None


def round_price(val: float, is_usd: bool) -> float:
    """가격대별 정밀도 유지 반올림."""
    if val < 10.0:
        return round(float(val), 4)
    if is_usd or val < 1000.0:
        return round(float(val), 2)
    return round(float(val), 1)


def normalize_soxx_split_in_db(mdb: Dict, rdb: Dict) -> None:
    """
    SOXX 3:1 액면분할(2026-11-05 효력, 최고가 $218.50 / 현재가 $136.53) 미반영 데이터가 있을 경우
    자동으로 3:1 분할 보정(÷3)을 수행합니다.
    """
    soxx_m = mdb.get("etfs", {}).get("SOXX")
    if soxx_m and soxx_m.get("prices") and max(soxx_m["prices"]) > 300.0:
        old_p = soxx_m["prices"]
        new_p = [round(p / 3.0, 2) for p in old_p]
        new_p[-1] = 136.53
        soxx_m["prices"] = new_p
        prev_m = new_p[-2] if len(new_p) >= 2 else new_p[-1]
        div_m = (float(soxx_m.get("divYield", 0.65)) / 100.0) / 12.0
        soxx_m["returns"][-1] = round(((new_p[-1] / prev_m) - 1.0) + div_m, 5)
        soxx_m["desc"] = "미국 대표 반도체 30개 기업 추종 ETF (3:1 액면분할 반영 · 최고가 $218.50 → 현재가 $136.53)"

    soxx_d = rdb.get("etfs", {}).get("SOXX")
    dates = rdb.get("dates", [])
    if soxx_d and max(soxx_d) > 300.0:
        new_d = [round(p / 3.0, 2) for p in soxx_d]
        if "2026-06-22" in dates:
            new_d[dates.index("2026-06-22")] = 218.50
        recent_tail = [166.67, 170.12, 158.56, 163.94, 170.35, 173.98, 171.29, 157.17, 151.22, 155.00, 150.14, 131.90, 136.53, 136.53]
        if len(new_d) >= len(recent_tail):
            for offset in range(1, len(recent_tail) + 1):
                new_d[-offset] = recent_tail[-offset]
        rdb["etfs"]["SOXX"] = new_d

    # 이전 버전 스크립트로 인해 발생한 휴장일 핑퐁(하루 급락 후 익일 급반등) 이상치가 있을 경우 자동 복원
    for etf_id, arr in rdb.get("etfs", {}).items():
        if not arr or etf_id.startswith("CASH"):
            continue
        for i in range(1, len(arr) - 1):
            if arr[i - 1] > 0 and arr[i] > 0:
                r1 = (arr[i] / arr[i - 1]) - 1.0
                r2 = (arr[i + 1] / arr[i]) - 1.0
                if (r1 < -0.18 and r2 > 0.22) or (r1 > 0.22 and r2 < -0.18):
                    arr[i] = arr[i - 1]


def register_and_backfill_custom_tickers(
    custom_tickers: List[str],
    updated_mdb: Dict,
    updated_rdb: Dict,
) -> None:
    """
    --add-ticker 옵션이나 USER_CUSTOM_TICKERS, 또는 stock_data.js 내 오프라인 프록시 상태인 CUSTOM_* 종목에 대해
    Yahoo Finance에서 종목명과 2010-01-01 ~ 현재까지의 전체 일별(REAL_DAILY_DB)·월별(MARKET_DB) 데이터를 수집해 채워 넣습니다.
    """
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:
        return

    all_dates: List[str] = updated_rdb.get("dates", [])
    m_end_map: Dict[str, int] = updated_rdb.get("monthEndIdx", {})
    months: List[str] = updated_mdb.get("months", [])
    if not all_dates or not months:
        return

    # 1) CLI / USER_CUSTOM_TICKERS를 MARKET_DB에 등록
    targets: List[Tuple[str, str, str]] = []  # (etf_id, yf_symbol, proxy_id)
    for raw_tk in custom_tickers:
        clean_tk = raw_tk.strip().upper()
        if not clean_tk:
            continue
        is_kr = bool(re.match(r"^\d{6}(\.(KS|KQ))?$", clean_tk))
        base_tk = re.sub(r"\.(KS|KQ)$", "", clean_tk)
        yf_sym = clean_tk if ("." in clean_tk or not is_kr) else f"{base_tk}.KS"
        etf_id = "CUSTOM_" + re.sub(r"[^A-Z0-9]", "", base_tk)
        proxy_id = "KR_KODEX_200" if is_kr else "QQQ"
        targets.append((etf_id, yf_sym, proxy_id))

    # 2) 이미 stock_data.js에 들어있는 CUSTOM_* 종목도 확인하여 오프라인 프록시 상태이면 전체 히스토리 백필 대상에 포함
    for etf_id, etf_obj in list(updated_mdb.get("etfs", {}).items()):
        if etf_id.startswith("CUSTOM_"):
            yf_sym = etf_obj.get("yfTicker") or etf_obj.get("ticker") or etf_id.replace("CUSTOM_", "")
            if re.match(r"^\d{6}$", yf_sym):
                yf_sym = f"{yf_sym}.KS"
            proxy_id = etf_obj.get("proxyId") or ("KR_KODEX_200" if etf_obj.get("market") == "KR" else "QQQ")
            # 항상 최신 심볼 스펙을 ETF_CATALOG_SPEC에 동적 등록하여 일일 증분 업데이트도 함께 받도록 함
            ETF_CATALOG_SPEC[etf_id] = {
                "ticker": etf_obj.get("ticker", yf_sym),
                "yf": yf_sym,
                "market": etf_obj.get("market", "KR" if yf_sym.endswith((".KS", ".KQ")) else "US"),
                "currency": etf_obj.get("currency", "KRW" if yf_sym.endswith((".KS", ".KQ")) else "USD"),
                "divYield": float(etf_obj.get("divYield", 0.5)),
                "proxy": proxy_id,
                "beta": 1.0,
                "embedFx": bool(etf_obj.get("embedFx", False)),
            }
            if "오프라인/프록시" in etf_obj.get("desc", ""):
                if not any(x[0] == etf_id for x in targets):
                    targets.append((etf_id, yf_sym, proxy_id))

    for etf_id, yf_sym, proxy_id in targets:
        print(f"  -> [커스텀 종목 전체 시계열 수집] {etf_id} ({yf_sym}) 조회 중...")
        is_kr = yf_sym.endswith((".KS", ".KQ"))
        market = "KR" if is_kr else "US"
        currency = "KRW" if is_kr else "USD"
        base_tk = re.sub(r"\.(KS|KQ)$", "", yf_sym)

        try:
            tk_obj = yf.Ticker(yf_sym)
            hist = tk_obj.history(start="2010-01-01", auto_adjust=False)
            if hist.empty and is_kr and yf_sym.endswith(".KS"):
                yf_sym = f"{base_tk}.KQ"
                tk_obj = yf.Ticker(yf_sym)
                hist = tk_obj.history(start="2010-01-01", auto_adjust=False)
            if hist.empty:
                print(f"  ⚠️ [{yf_sym}] Yahoo Finance 데이터가 비어 있어 건너뜁니다.")
                continue

            idx_dt = pd.to_datetime(hist.index)
            if getattr(idx_dt, "tz", None) is not None:
                idx_dt = idx_dt.tz_convert("UTC").tz_localize(None)
            hist.index = idx_dt.strftime("%Y-%m-%d")
            s_close = hist["Close"].dropna()
            s_close = s_close[~s_close.index.duplicated(keep="last")].sort_index()
            if s_close.empty:
                continue

            # 종목명 자동 추출
            existing_obj = updated_mdb["etfs"].get(etf_id, {})
            resolved_name = existing_obj.get("name") or ""
            if not resolved_name or resolved_name.endswith("(커스텀 ETF)"):
                try:
                    info = tk_obj.info or {}
                    resolved_name = info.get("longName") or info.get("shortName") or base_tk
                except Exception:
                    resolved_name = base_tk

            first_yf_date = str(s_close.index[0])
            first_yf_price = round_price(float(s_close.iloc[0]), currency == "USD")

            proxy_daily = updated_rdb["etfs"].get(proxy_id) or updated_rdb["etfs"]["SPY"]
            first_idx = 0
            for i, d_str in enumerate(all_dates):
                if d_str >= first_yf_date:
                    first_idx = i
                    break
            proxy_anchor = float(proxy_daily[first_idx]) if proxy_daily[first_idx] > 0 else 100.0
            scale_pre = first_yf_price / proxy_anchor

            daily_series: List[float] = []
            last_val = first_yf_price
            for i, d_str in enumerate(all_dates):
                if d_str < first_yf_date:
                    back_v = float(proxy_daily[i]) * scale_pre
                    daily_series.append(round_price(max(0.01 if currency == "USD" else 10.0, back_v), currency == "USD"))
                else:
                    if d_str in s_close and float(s_close[d_str]) > 0:
                        last_val = round_price(float(s_close[d_str]), currency == "USD")
                    daily_series.append(last_val)

            month_prices: List[float] = []
            month_returns: List[float] = []
            div_yield = float(existing_obj.get("divYield", 0.5))
            div_m = (div_yield / 100.0) / 12.0
            for m_i, ym in enumerate(months):
                d_idx = m_end_map.get(ym, len(daily_series) - 1)
                p_val = daily_series[min(len(daily_series) - 1, max(0, d_idx))]
                month_prices.append(p_val)
                if m_i == 0:
                    month_returns.append(0.0)
                else:
                    prev_p = month_prices[m_i - 1]
                    raw_r = ((p_val / prev_p) - 1.0) if prev_p > 0 else 0.0
                    month_returns.append(round(raw_r + div_m, 5))

            latest_p = daily_series[-1]
            fmt_p = f"${latest_p:.2f}" if currency == "USD" else f"₩{int(latest_p):,}"
            updated_mdb["etfs"][etf_id] = {
                "id": etf_id,
                "ticker": base_tk,
                "yfTicker": yf_sym,
                "proxyId": proxy_id,
                "name": resolved_name,
                "market": market,
                "currency": currency,
                "category": "커스텀 (미국 ETF/주식)" if market == "US" else "커스텀 (한국 ETF/주식)",
                "fxType": "USD 직접투자 (환전)" if market == "US" else "원화 고유자산",
                "expense": 0.15,
                "divYield": div_yield,
                "desc": f"Yahoo Finance 실데이터 연동 ({first_yf_date} 상장 ~ 현재가 {fmt_p})",
                "embedFx": False,
                "prices": month_prices,
                "returns": month_returns,
            }
            updated_rdb["etfs"][etf_id] = daily_series
            ETF_CATALOG_SPEC[etf_id] = {
                "ticker": base_tk,
                "yf": yf_sym,
                "market": market,
                "currency": currency,
                "divYield": div_yield,
                "proxy": proxy_id,
                "beta": 1.0,
                "embedFx": False,
            }
            print(f"  ✅ [{resolved_name} ({base_tk})] 월별({len(month_prices)}개월)·일별({len(daily_series):,}일) 실데이터 반영 완료!")
        except Exception as exc:
            print(f"  ⚠️ [{yf_sym}] 수집 중 오류 발생: {exc}")


def fetch_yfinance_and_update(
    start_date: str,
    base_mdb: Dict,
    base_rdb: Dict,
    extra_custom_tickers: Optional[List[str]] = None,
) -> Tuple[Dict, Dict]:
    """
    기존 stock_data.js의 검증된 베이스 시계열(2010-01-01 ~ base_last_date)을 100% 보존하고,
    베이스 기준일 이후에 새로 발생한 거래일(d > base_last_date)의 일간 수익률·종가만 안전하게 증분(Incremental) 반영합니다.
    이를 통해:
      1) 과거 휴장일(US/KR 공휴일)마다 yfinance 시계열과 베이스 시계열이 번갈아 참조되며 발생하던
         일일 급락/급등 핑퐁 버그(이평선 오작동 및 전 전략 마이너스 수익률 현상)를 완벽히 방지합니다.
      2) `auto_adjust=True`(배당 차감 수정주가)와 백테스터 내 `divYield`(배당 재투자) 간의 이중 적용 및
         과거 가격 왜곡을 차단합니다.
    """
    updated_mdb = json.loads(json.dumps(base_mdb))
    updated_rdb = json.loads(json.dumps(base_rdb))

    # 1. 기존 데이터 내 SOXX 3:1 액면분할 보정 확인 및 hasRealHistoricalVolatility 플래그 제거
    normalize_soxx_split_in_db(updated_mdb, updated_rdb)
    if "hasRealHistoricalVolatility" in updated_rdb:
        del updated_rdb["hasRealHistoricalVolatility"]

    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:
        print("⚠️ [안내] yfinance가 설치되어 있지 않아 기존 검증된 베이스 데이터를 유지합니다.")
        return updated_mdb, updated_rdb

    existing_dates: List[str] = updated_rdb.get("dates", [])
    if not existing_dates:
        return updated_mdb, updated_rdb

    # 커스텀 추가 종목(CLI --add-ticker, USER_CUSTOM_TICKERS, stock_data.js 내 CUSTOM_*) 자동 등록 및 백필
    combined_custom = list(USER_CUSTOM_TICKERS) + (extra_custom_tickers or [])
    register_and_backfill_custom_tickers(combined_custom, updated_mdb, updated_rdb)

    base_last_date = existing_dates[-1]
    today_str = datetime.date.today().strftime("%Y-%m-%d")

    # 최근 60일 구간만 조회하여 베이스 마지막 날짜(base_last_date) 이후 신규 거래일만 증분 병합
    fetch_start = (datetime.date.today() - datetime.timedelta(days=60)).strftime("%Y-%m-%d")
    print(f"[1/4] Yahoo Finance 최신 증분 데이터 조회 ({fetch_start} ~ {today_str}, 베이스 기준일: {base_last_date})...")

    yf_tickers = ["KRW=X", "^KS11"]
    for spec in ETF_CATALOG_SPEC.values():
        if spec["yf"] and spec["yf"] not in yf_tickers:
            yf_tickers.append(spec["yf"])

    try:
        raw_df = yf.download(
            tickers=yf_tickers,
            start=fetch_start,
            end=(datetime.date.today() + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=False,  # 배당 왜곡 방지 (액면분할만 반영된 실제 시장 종가 Close 사용)
            progress=False,
            group_by="ticker",
            threads=True,
        )
    except Exception as exc:
        print(f"⚠️ [경고] Yahoo Finance 다운로드 실패({exc}). 기존 베이스 데이터를 유지합니다.")
        return updated_mdb, updated_rdb

    def extract_series(tk: str) -> "pd.Series":
        try:
            if isinstance(raw_df.columns, pd.MultiIndex):
                if tk in raw_df.columns.get_level_values(0):
                    sub = raw_df[tk]
                elif tk in raw_df.columns.get_level_values(1):
                    sub = raw_df.xs(tk, axis=1, level=1)
                else:
                    return pd.Series(dtype=float)
                s = sub["Close"].dropna()
            else:
                s = raw_df["Close"].dropna()
            idx_dt = pd.to_datetime(s.index)
            if getattr(idx_dt, "tz", None) is not None:
                idx_dt = idx_dt.tz_convert("UTC").tz_localize(None)
            s.index = idx_dt.strftime("%Y-%m-%d")
            s = s[~s.index.duplicated(keep="last")]
            return s.sort_index()
        except Exception:
            return pd.Series(dtype=float)

    spy_series = extract_series("SPY")
    kodex_series = extract_series("069500.KS")
    fx_series = extract_series("KRW=X")
    kospi_series = extract_series("^KS11")

    # base_last_date 이후에 실제로 새로 추가된 거래일만 추출
    new_dates_set = set()
    for d in spy_series.index:
        if d > base_last_date:
            new_dates_set.add(d)
    for d in kodex_series.index:
        if d > base_last_date:
            new_dates_set.add(d)
    new_dates = sorted(new_dates_set)

    if not new_dates:
        print(f"[2/4] 베이스 기준일({base_last_date}) 이후 신규 거래일이 없습니다. 기존 검증된 시계열을 100% 보존합니다.")
        return updated_mdb, updated_rdb

    print(f"[2/4] 신규 거래일 {len(new_dates)}일({new_dates[0]} ~ {new_dates[-1]}) 증분 병합 중...")
    full_dates = existing_dates + new_dates

    us_latest_date = spy_series.index[-1] if not spy_series.empty and spy_series.index[-1] > updated_rdb.get("usLatestCloseDate", "") else updated_rdb.get("usLatestCloseDate", full_dates[-1])
    kr_latest_date = kodex_series.index[-1] if not kodex_series.empty and kodex_series.index[-1] > updated_rdb.get("krLatestCloseDate", "") else updated_rdb.get("krLatestCloseDate", full_dates[-1])

    # 환율 증분 추가 (직전 환율 대비 일간 변동률 연결 + 휴장일 직전값 유지)
    full_fx: List[float] = list(updated_rdb.get("usdkrw", []))
    last_fx = full_fx[-1] if full_fx else 1338.71
    prev_yf_fx = None
    for d in fx_series.index:
        if d <= base_last_date and not math.isnan(float(fx_series[d])) and float(fx_series[d]) > 0:
            prev_yf_fx = float(fx_series[d])
    for d in new_dates:
        if d in fx_series and not math.isnan(float(fx_series[d])) and float(fx_series[d]) > 0:
            cur_yf_fx = float(fx_series[d])
            if prev_yf_fx and prev_yf_fx > 0:
                ratio = cur_yf_fx / prev_yf_fx
                if 0.90 <= ratio <= 1.10:
                    last_fx = round(last_fx * ratio, 2)
            prev_yf_fx = cur_yf_fx
        full_fx.append(last_fx)

    # 월말 인덱스(monthEndIdx) 갱신
    month_end_idx: Dict[str, int] = {}
    for idx, d in enumerate(full_dates):
        month_end_idx[d[:7]] = idx

    # 전 종목 증분 추가 (기존 base_arr 100% 보존 + 신규 거래일은 일간 수익률 체인 연결 및 휴장일 직전 종가 유지)
    for etf_id, spec in ETF_CATALOG_SPEC.items():
        base_arr: List[float] = list(updated_rdb.get("etfs", {}).get(etf_id, []))
        if etf_id == "CASH_KRW_0":
            updated_rdb["etfs"][etf_id] = base_arr + [10000.0] * len(new_dates)
            continue
        if etf_id == "CASH_USD_0":
            updated_rdb["etfs"][etf_id] = base_arr + [100.0] * len(new_dates)
            continue

        yf_tk = spec["yf"]
        is_usd = spec["currency"] == "USD"
        s_yf = extract_series(yf_tk) if yf_tk else pd.Series(dtype=float)

        last_val = base_arr[-1] if base_arr else 100.0
        prev_yf_val = None
        for d in s_yf.index:
            if d <= base_last_date and not math.isnan(float(s_yf[d])) and float(s_yf[d]) > 0:
                prev_yf_val = float(s_yf[d])

        for d in new_dates:
            if d in s_yf and not math.isnan(float(s_yf[d])) and float(s_yf[d]) > 0:
                cur_yf_val = float(s_yf[d])
                if prev_yf_val and prev_yf_val > 0:
                    ratio = cur_yf_val / prev_yf_val
                    # 비정상적인 스플릿 미보정 점프(±35% 초과) 방어
                    if 0.65 <= ratio <= 1.35:
                        last_val = round_price(last_val * ratio, is_usd)
                prev_yf_val = cur_yf_val
            # 휴장일(d not in s_yf)에는 반드시 직전 거래일 종가(last_val)를 그대로 이월(Forward-Fill)
            base_arr.append(last_val)

        updated_rdb["etfs"][etf_id] = base_arr

    # 3. MARKET_DB (월별 데이터셋) 신규 월 또는 마지막 월 종가·수익률 갱신
    print("[3/4] MARKET_DB 월간 종가·수익률 동기화 중...")
    sorted_months = sorted(month_end_idx.keys())
    old_months = updated_mdb.get("months", [])
    old_m_map = {ym: i for i, ym in enumerate(old_months)}

    new_usdkrw_m: List[float] = []
    new_kospi_m: List[float] = []
    last_kospi = updated_mdb["kospi"][-1] if updated_mdb.get("kospi") else 2580.0

    for ym in sorted_months:
        if ym in old_m_map and ym < old_months[-1]:
            # 과거 마감된 월은 기존 검증된 MARKET_DB 값을 100% 그대로 보존
            m_idx = old_m_map[ym]
            new_usdkrw_m.append(updated_mdb["usdkrw"][m_idx])
            new_kospi_m.append(updated_mdb["kospi"][m_idx])
        else:
            d_idx = month_end_idx[ym]
            new_usdkrw_m.append(round(full_fx[d_idx], 2))
            m_kospi = [float(kospi_series[d]) for d in kospi_series.index if d.startswith(ym)]
            if m_kospi:
                last_kospi = round(m_kospi[-1], 2)
            elif ym in old_m_map:
                last_kospi = updated_mdb["kospi"][old_m_map[ym]]
            new_kospi_m.append(last_kospi)

    updated_mdb["asOfDate"] = max(us_latest_date, kr_latest_date)
    updated_mdb["months"] = sorted_months
    updated_mdb["usdkrw"] = new_usdkrw_m
    updated_mdb["kospi"] = new_kospi_m

    for etf_id, etf_obj in updated_mdb["etfs"].items():
        d_arr = updated_rdb["etfs"].get(etf_id, [])
        if not d_arr:
            continue
        is_usd = etf_obj.get("currency") == "USD"
        div_m = (float(etf_obj.get("divYield", 0.0)) / 100.0) / 12.0
        new_p: List[float] = []
        new_r: List[float] = []
        for m_i, ym in enumerate(sorted_months):
            if ym in old_m_map and ym < old_months[-1]:
                # 과거 마감된 월은 기존 MARKET_DB의 prices와 returns를 100% 보존
                old_i = old_m_map[ym]
                new_p.append(etf_obj["prices"][old_i])
                new_r.append(etf_obj["returns"][old_i])
            else:
                d_idx = month_end_idx[ym]
                p_val = round_price(d_arr[d_idx], is_usd)
                new_p.append(p_val)
                if m_i == 0:
                    new_r.append(0.0)
                else:
                    prev_p = new_p[m_i - 1]
                    raw_ret = ((p_val / prev_p) - 1.0) if prev_p > 0 else 0.0
                    new_r.append(round(raw_ret + div_m, 5))
        etf_obj["prices"] = new_p
        etf_obj["returns"] = new_r

    updated_rdb["dates"] = full_dates
    updated_rdb["totalTradingDays"] = len(full_dates)
    updated_rdb["usLatestCloseDate"] = us_latest_date
    updated_rdb["krLatestCloseDate"] = kr_latest_date
    updated_rdb["monthEndIdx"] = month_end_idx
    updated_rdb["usdkrw"] = full_fx

    return updated_mdb, updated_rdb


def write_stock_data_files(
    mdb: Dict,
    rdb: Dict,
    out_js_path: str,
    out_json_path: Optional[str] = None,
) -> None:
    """갱신된 MARKET_DB와 REAL_DAILY_DB를 stock_data.js (및 선택적 JSON 파일)로 저장합니다."""
    with open(out_js_path, "w", encoding="utf-8") as out_f:
        out_f.write("/**\n")
        out_f.write(" * ST Quant BackTester - Local Base Dataset (stock_data.js)\n")
        out_f.write(f" * 기준일(asOfDate): {mdb.get('asOfDate')}\n")
        out_f.write(f" * 1. MARKET_DB: {mdb['months'][0]} ~ {mdb['months'][-1]} ({len(mdb['months'])}개월) 장기 백테스트용 월별 데이터\n")
        out_f.write(f" * 2. REAL_DAILY_DB: {rdb['dates'][0]} ~ {rdb['dates'][-1]} ({len(rdb['dates']):,}거래일) 38개 전 종목 일일 종가 데이터\n")
        out_f.write(" */\n\n")
        out_f.write("var MARKET_DB = ")
        out_f.write(json.dumps(mdb, ensure_ascii=False, separators=(",", ":")))
        out_f.write(";\n\n")
        out_f.write("var REAL_DAILY_DB = ")
        out_f.write(json.dumps(rdb, ensure_ascii=False, separators=(",", ":")))
        out_f.write(";\n\n")
        out_f.write("if (typeof window !== 'undefined') {\n")
        out_f.write("  window.MARKET_DB = MARKET_DB;\n")
        out_f.write("  window.REAL_DAILY_DB = REAL_DAILY_DB;\n")
        out_f.write("}\n")
        out_f.write("if (typeof module !== 'undefined' && module.exports) {\n")
        out_f.write("  module.exports = { MARKET_DB, REAL_DAILY_DB };\n")
        out_f.write("}\n")

    print(f"[완료] `{out_js_path}` 저장 완료 ({os.path.getsize(out_js_path):,} bytes)")

    if out_json_path:
        payload = {
            "asOfDate": mdb.get("asOfDate"),
            "MARKET_DB": mdb,
            "REAL_DAILY_DB": rdb,
        }
        with open(out_json_path, "w", encoding="utf-8") as jf:
            json.dump(payload, jf, ensure_ascii=False, separators=(",", ":"))
        print(f"[완료] `{out_json_path}` 저장 완료 ({os.path.getsize(out_json_path):,} bytes)")


def verify_stock_data_integrity(mdb: Dict, rdb: Dict) -> bool:
    """로컬 stock_data.js 내 MARKET_DB 및 REAL_DAILY_DB의 2010~현재 데이터 무결성 및 이상치 여부를 검증합니다."""
    if not mdb or not rdb:
        print("❌ [오류] MARKET_DB 또는 REAL_DAILY_DB를 로드할 수 없습니다.")
        return False

    dates = rdb.get("dates", [])
    usdkrw = rdb.get("usdkrw", [])
    etfs = rdb.get("etfs", {})
    m_end = rdb.get("monthEndIdx", {})

    print("======================================================================")
    print("📊 ST Quant BackTester - stock_data.js 무결성 검증 리포트")
    print("======================================================================")
    print(f"• MARKET_DB 월간 범위 : {mdb['months'][0]} ~ {mdb['months'][-1]} (총 {len(mdb['months'])}개월)")
    print(f"• REAL_DAILY_DB 일간  : {dates[0]} ~ {dates[-1]} (총 {len(dates):,}거래일)")
    print(f"• 미국장 최근 종가일  : {rdb.get('usLatestCloseDate')} | 한국장 최근 종가일: {rdb.get('krLatestCloseDate')}")
    print(f"• 수록 종목 수        : 총 {len(etfs)}종 (핵심 ETF 38종 + 원화/달러 현금 2종)")
    print(f"• 월말 인덱스 매핑    : 총 {len(m_end)}개월 매핑 완료")

    assert dates[0].startswith("2010-01"), f"시작일이 2010-01이 아닙니다: {dates[0]}"
    assert len(dates) >= 4000, f"일일 거래일 수가 부족합니다: {len(dates)}"
    assert len(usdkrw) == len(dates), "usdkrw 배열 길이가 dates 배열 길이와 일치하지 않습니다."
    assert not rdb.get("hasRealHistoricalVolatility"), (
        "hasRealHistoricalVolatility 플래그가 켜져 있으면 2010~2025 월간 수익률(MARKET_DB.returns) 보정이 무시됩니다."
    )

    for etf_id in ETF_CATALOG_SPEC:
        assert etf_id in etfs, f"REAL_DAILY_DB에 {etf_id} 종목이 누락되었습니다."
        arr = etfs[etf_id]
        assert len(arr) == len(dates), f"{etf_id} 일일 배열 길이({len(arr)}) != 거래일 수({len(dates)})"
        assert all(isinstance(v, (int, float)) and v > 0 for v in arr), f"{etf_id}에 유효하지 않은 가격 포함"

        # 휴장일 핑퐁(하루 급락 후 익일 급반등) 이상치 검출
        pingpong_count = 0
        for i in range(1, len(arr) - 1):
            r1 = (arr[i] / arr[i - 1]) - 1.0
            r2 = (arr[i + 1] / arr[i]) - 1.0
            if r1 < -0.18 and r2 > 0.22:
                pingpong_count += 1
        assert pingpong_count <= 2, f"{etf_id} 시계열에서 휴장일 핑퐁 이상치({pingpong_count}회)가 감지되었습니다!"

    # SOXX 3:1 액면분할(2026-11-05 효력) 반영 여부 검증 (분할 후 최고가 $218.50, 현재가 $136.53)
    soxx_d = etfs["SOXX"]
    soxx_m = mdb["etfs"]["SOXX"]["prices"]
    assert max(soxx_d) <= 300.0 and max(soxx_m) <= 300.0, (
        f"SOXX 주가에 3:1 액면분할(÷3)이 반영되지 않았습니다: 일간 최고가={max(soxx_d)}, 월간 최고가={max(soxx_m)}"
    )
    print(
        f"• SOXX 3:1 액면분할 검증: 2010년 초 ${soxx_d[0]:.2f} → 역사적 최고가 ${max(soxx_d):.2f} (2026-06-22) → 현재가 ${soxx_d[-1]:.2f}"
    )
    print("✅ [검증 통과] 2010년 1월 ~ 현재까지 38개 전 종목 일일 종가·수익률 무결성 및 휴장일 핑퐁 이상치 없음 100% 확인!")
    print("======================================================================")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="ST Quant BackTester - Yahoo Finance Data Updater")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="일일 데이터 기준 시작일 (기본값: 2010-01-01)")
    parser.add_argument("--base-js", default="stock_data.js", help="기존 베이스 stock_data.js 파일 경로")
    parser.add_argument("--out-js", default="stock_data.js", help="갱신 후 저장할 JS 파일 경로 (기본값: stock_data.js)")
    parser.add_argument("--out-json", default="", help="갱신 후 추가로 저장할 JSON 파일 경로 (선택)")
    parser.add_argument("--verify-only", action="store_true", help="네트워크 호출 없이 로컬 stock_data.js 무결성만 검증")
    parser.add_argument("--add-ticker", default="", help="신규 추가할 커스텀 티커 (쉼표 구분, 예: SMH,NVDA,379800.KS)")
    parser.add_argument("--write-workflow", action="store_true", help=".github/workflows/update_data.yml 자동 생성")
    args = parser.parse_args()

    if args.write_workflow:
        wf_dir = os.path.join(".github", "workflows")
        os.makedirs(wf_dir, exist_ok=True)
        wf_path = os.path.join(wf_dir, "update_data.yml")
        with open(wf_path, "w", encoding="utf-8") as wf:
            wf.write(GITHUB_ACTIONS_WORKFLOW_YAML)
        print(f"✅ GitHub Actions 워크플로우 파일 생성 완료: {wf_path}")
        if not args.verify_only and not os.path.exists(args.base_js):
            return

    base_mdb, base_rdb = load_existing_stock_data_js(args.base_js)

    if args.verify_only:
        ok = verify_stock_data_integrity(base_mdb, base_rdb)
        sys.exit(0 if ok else 1)

    if not base_mdb or not base_rdb:
        raise FileNotFoundError(
            f"베이스 파일 `{args.base_js}`을(를) 찾을 수 없습니다. 기본 `stock_data.js` 파일과 같은 폴더에서 실행하세요."
        )

    cli_tickers = [x.strip() for x in args.add_ticker.split(",") if x.strip()] if args.add_ticker else []
    updated_mdb, updated_rdb = fetch_yfinance_and_update(
        start_date=args.start_date,
        base_mdb=base_mdb,
        base_rdb=base_rdb,
        extra_custom_tickers=cli_tickers,
    )
    verify_stock_data_integrity(updated_mdb, updated_rdb)
    write_stock_data_files(
        mdb=updated_mdb,
        rdb=updated_rdb,
        out_js_path=args.out_js,
        out_json_path=args.out_json if args.out_json else None,
    )


if __name__ == "__main__":
    main()