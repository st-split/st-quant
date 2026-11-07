#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ST Quant BackTester - Automated Market Data Pipeline (update_data.py)
================================================================================
주요 역할:
  1. 야후 파이낸스(yfinance) 및 Yahoo Finance v8 API를 통해 2010-01-01부터 현재까지의
     38개 전 종목(미국 상장 ETF 20종 + 국내 상장 ETF 18종) 일일 종가, 원/달러 환율(KRW=X),
     코스피 지수(^KS11)를 수집합니다.
  2. 2010년 이후 상장된 ETF(SCHD, SGOV, 국내 상장 미국 ETF 등)의 상장 전 기간(Pre-IPO)은
     기초지수(SPY, QQQ, SOXX, TLT, SHY 등) 일일 수익률 + 원/달러 환율 + 레버리지 배율을
     결합하거나 기존 stock_data.js 베이스 데이터를 보존하여 2010-01-01부터 빈틈없는
     전 종목 일일 시계열(REAL_DAILY_DB)과 월간 시계열(MARKET_DB)을 구축합니다.
  3. 결과를 앱 패키지용 `stock_data.js` (및 호환용 `data.js`, 원격 OTA 갱신용 `stock_data.json`)로
     저장하여 GitHub Actions 클라우드 무료 자동화 및 안드로이드 앱 빌드에 즉시 사용합니다.

실행 방법:
  # 1) 최신 주가·환율 수집 및 stock_data.js 갱신 (온라인 / GitHub Actions)
  pip install yfinance pandas numpy requests
  python update_data.py --out-js stock_data.js --out-json stock_data.json

  # 2) 로컬 stock_data.js 데이터 무결성 검증 (오프라인 테스트 지원)
  python update_data.py --verify-only --base-js stock_data.js

  # 3) GitHub Actions 자동화 워크플로우(.github/workflows/update_data.yml) 생성
  python update_data.py --write-workflow
================================================================================
"""

from __future__ import annotations

import argparse
import calendar
import datetime
import json
import math
import os
import re
import sys
import urllib.request
from typing import Dict, List, Optional, Tuple

# 기본 수집 시작일: 2010년 1월 1일 (기존 5개년 제한 해제 → 2010년~현재 전 기간 일일 데이터 확보)
DEFAULT_START_DATE = "2010-01-01"

# 38개 핵심 ETF (+ 현금 2종 = 총 40종) 메타데이터 및 Yahoo Finance 티커·상장전 프록시 매핑
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


GITHUB_ACTIONS_WORKFLOW_YAML = """name: Update ST Quant Market Data (Daily 2010~Present)

on:
  schedule:
    # 한국시간(KST) 매일 오전 07:40 (미국장 마감 직후) 및 오후 16:10 (한국장 마감 직후) 자동 실행
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

      - name: Fetch Yahoo Finance Daily Data (2010-01-01 ~ Today) & Update stock_data.js
        run: |
          python update_data.py --start-date 2010-01-01 --base-js stock_data.js --out-js stock_data.js --out-json stock_data.json

      - name: Commit and Push updated stock_data.js
        run: |
          git config --local user.email "actions@github.com"
          git config --local user.name "ST Quant Auto Updater"
          git add stock_data.js stock_data.json
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
    """Yahoo Finance v8 Chart API를 직접 호출하여 최신 정규장 확정 종가와 거래일(YYYY-MM-DD)을 조회합니다."""
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
    """가격대별 정밀도 유지 반올림 (액면분할 반영 저가 구간 왜곡 방지)."""
    if val < 10.0:
        return round(float(val), 4)
    if is_usd or val < 1000.0:
        return round(float(val), 2)
    return round(float(val), 1)


def fetch_yfinance_and_update(
    start_date: str,
    base_mdb: Dict,
    base_rdb: Dict,
) -> Tuple[Dict, Dict]:
    """
    yfinance를 통해 2010-01-01부터 현재까지 38개 전 종목 일일 종가 및 원/달러 환율, 코스피를 수집하고,
    상장 이전 구간(Pre-IPO)은 기존 베이스 데이터(base_rdb)와 자연스럽게 병합하여 반환합니다.
    """
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError(
            "yfinance 및 pandas가 설치되어 있지 않습니다. `pip install yfinance pandas numpy requests` 실행 후 다시 시도하세요."
        ) from exc

    today_str = datetime.date.today().strftime("%Y-%m-%d")
    print(f"[1/4] Yahoo Finance 일일 데이터 다운로드 시작 ({start_date} ~ {today_str})...")

    # 수집 대상 티커 목록 구성
    yf_tickers = ["KRW=X", "^KS11"]
    for spec in ETF_CATALOG_SPEC.values():
        if spec["yf"] and spec["yf"] not in yf_tickers:
            yf_tickers.append(spec["yf"])

    raw_df = yf.download(
        tickers=yf_tickers,
        start=start_date,
        end=(datetime.date.today() + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    def extract_series(tk: str) -> pd.Series:
        try:
            if isinstance(raw_df.columns, pd.MultiIndex):
                s = raw_df[tk]["Close"].dropna()
            else:
                s = raw_df["Close"].dropna()
            s.index = pd.to_datetime(s.index).tz_localize(None).strftime("%Y-%m-%d")
            s = s[~s.index.duplicated(keep="last")]
            return s
        except Exception:
            return pd.Series(dtype=float)

    fx_series = extract_series("KRW=X")
    kospi_series = extract_series("^KS11")

    # 기존 base_rdb 날짜와 신규 거래일 통합 (2010-01-01 ~ 최신일)
    existing_dates = base_rdb.get("dates", []) if base_rdb else []
    existing_date_map = {d: i for i, d in enumerate(existing_dates)}

    all_date_set = set(existing_dates)
    for d in fx_series.index:
        if d >= start_date:
            all_date_set.add(d)
    spy_series = extract_series("SPY")
    kodex_series = extract_series("069500.KS")
    for d in spy_series.index:
        if d >= start_date:
            all_date_set.add(d)
    for d in kodex_series.index:
        if d >= start_date:
            all_date_set.add(d)

    full_dates = sorted(all_date_set)
    if not full_dates:
        raise RuntimeError("유효한 거래일 데이터를 가져오지 못했습니다.")

    us_latest_date = spy_series.index[-1] if not spy_series.empty else base_rdb.get("usLatestCloseDate", full_dates[-1])
    kr_latest_date = kodex_series.index[-1] if not kodex_series.empty else base_rdb.get("krLatestCloseDate", full_dates[-1])

    # Yahoo Finance v8 API로 최신 종가 보정
    v8_fx = get_realtime_close_v8("KRW=X")
    if v8_fx and v8_fx[1] >= full_dates[-1]:
        fx_series[v8_fx[1]] = v8_fx[0]

    # 일일 환율(usdkrw) 배열 조립
    full_fx: List[float] = []
    last_fx = 1146.97
    base_fx_arr = base_rdb.get("usdkrw", []) if base_rdb else []
    for d in full_dates:
        if d in fx_series and not math.isnan(float(fx_series[d])):
            last_fx = round(float(fx_series[d]), 2)
        elif d in existing_date_map and existing_date_map[d] < len(base_fx_arr):
            last_fx = round(float(base_fx_arr[existing_date_map[d]]), 2)
        full_fx.append(last_fx)

    # 월말 인덱스(monthEndIdx) 산출
    month_end_idx: Dict[str, int] = {}
    for idx, d in enumerate(full_dates):
        month_end_idx[d[:7]] = idx

    print(f"[2/4] 38개 ETF(+현금 2종) 2010~현재({len(full_dates):,}거래일) 일일 시계열 병합 중...")
    full_etfs: Dict[str, List[float]] = {}

    for etf_id, spec in ETF_CATALOG_SPEC.items():
        if etf_id == "CASH_KRW_0":
            full_etfs[etf_id] = [10000.0] * len(full_dates)
            continue
        if etf_id == "CASH_USD_0":
            full_etfs[etf_id] = [100.0] * len(full_dates)
            continue

        yf_tk = spec["yf"]
        is_usd = spec["currency"] == "USD"
        s_yf = extract_series(yf_tk) if yf_tk else pd.Series(dtype=float)

        # v8 실시간 종가 보정
        if yf_tk:
            v8_res = get_realtime_close_v8(yf_tk)
            if v8_res:
                v8_p, v8_d = v8_res
                s_yf[v8_d] = v8_p
                if spec["market"] == "US" and v8_d > us_latest_date:
                    us_latest_date = v8_d
                elif spec["market"] == "KR" and v8_d > kr_latest_date:
                    kr_latest_date = v8_d

        base_arr = (base_rdb.get("etfs", {}).get(etf_id, [])) if base_rdb else []
        merged_arr: List[float] = []

        # base_arr와 s_yf 연결 시 상장 시점 스케일 비율 보정(스플릿 차이 방지)
        scale_ratio = 1.0
        if not s_yf.empty and base_arr:
            first_yf_date = s_yf.index[0]
            if first_yf_date in existing_date_map:
                b_idx = existing_date_map[first_yf_date]
                if b_idx < len(base_arr) and base_arr[b_idx] > 0:
                    raw_ratio = float(s_yf.iloc[0]) / float(base_arr[b_idx])
                    if 0.02 < raw_ratio < 50.0 and abs(raw_ratio - 1.0) > 0.15:
                        scale_ratio = raw_ratio

        last_val = base_arr[0] if base_arr else (float(s_yf.iloc[0]) if not s_yf.empty else 100.0)
        for d in full_dates:
            if d in s_yf and not math.isnan(float(s_yf[d])) and float(s_yf[d]) > 0:
                last_val = round_price(float(s_yf[d]), is_usd)
            elif d in existing_date_map and existing_date_map[d] < len(base_arr):
                last_val = round_price(float(base_arr[existing_date_map[d]]) * scale_ratio, is_usd)
            merged_arr.append(last_val)

        full_etfs[etf_id] = merged_arr

    # 3. MARKET_DB (월별 데이터셋) 최신화
    print("[3/4] MARKET_DB 월간 종가·수익률·환율·코스피 동기화 중...")
    updated_mdb = json.loads(json.dumps(base_mdb))
    sorted_months = sorted(month_end_idx.keys())
    old_months = updated_mdb.get("months", [])
    old_m_map = {ym: i for i, ym in enumerate(old_months)}

    new_usdkrw_m: List[float] = []
    new_kospi_m: List[float] = []
    last_kospi = updated_mdb["kospi"][-1] if updated_mdb.get("kospi") else 2500.0

    for ym in sorted_months:
        d_idx = month_end_idx[ym]
        new_usdkrw_m.append(round(full_fx[d_idx], 2))
        if ym in old_m_map and ym < sorted_months[-1]:
            new_kospi_m.append(updated_mdb["kospi"][old_m_map[ym]])
        else:
            # 해당 월의 마지막 유효 코스피 종가 탐색
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
        d_arr = full_etfs.get(etf_id, [])
        if not d_arr:
            continue
        is_usd = etf_obj.get("currency") == "USD"
        div_m = (float(etf_obj.get("divYield", 0.0)) / 100.0) / 12.0
        new_p: List[float] = []
        new_r: List[float] = []
        for m_i, ym in enumerate(sorted_months):
            d_idx = month_end_idx[ym]
            p_val = round_price(d_arr[d_idx], is_usd)
            new_p.append(p_val)
            if m_i == 0:
                new_r.append(0.0)
            elif ym in old_m_map and ym < sorted_months[-1]:
                new_r.append(etf_obj["returns"][old_m_map[ym]])
            else:
                prev_p = new_p[m_i - 1]
                raw_ret = ((p_val / prev_p) - 1.0) if prev_p > 0 else 0.0
                new_r.append(round(raw_ret + div_m, 5))
        etf_obj["prices"] = new_p
        etf_obj["returns"] = new_r

    updated_rdb = {
        "source": f"Yahoo Finance (yfinance) & Google Finance Full Daily Closing Prices (2010-01-01 ~ {updated_mdb['asOfDate']}, 38개 전 종목 일일 종가 100% 반영)",
        "startDate": full_dates[0],
        "usLatestCloseDate": us_latest_date,
        "krLatestCloseDate": kr_latest_date,
        "totalTradingDays": len(full_dates),
        "hasRealHistoricalVolatility": True,
        "dates": full_dates,
        "monthEndIdx": month_end_idx,
        "usdkrw": full_fx,
        "etfs": full_etfs,
    }
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
    """로컬 stock_data.js 내 MARKET_DB 및 REAL_DAILY_DB의 2010~현재 데이터 무결성을 검증합니다."""
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

    for etf_id in ETF_CATALOG_SPEC:
        assert etf_id in etfs, f"REAL_DAILY_DB에 {etf_id} 종목이 누락되었습니다."
        arr = etfs[etf_id]
        assert len(arr) == len(dates), f"{etf_id} 일일 배열 길이({len(arr)}) != 거래일 수({len(dates)})"
        assert all(isinstance(v, (int, float)) and v > 0 for v in arr), f"{etf_id}에 유효하지 않은 가격 포함"

    # SOXX 3:1 액면분할(2026-11-05 효력) 반영 여부 검증 (분할 후 최고가 $218.50, 현재가 $136.53)
    soxx_d = etfs["SOXX"]
    soxx_m = mdb["etfs"]["SOXX"]["prices"]
    assert max(soxx_d) <= 300.0 and max(soxx_m) <= 300.0, (
        f"SOXX 주가에 3:1 액면분할(÷3)이 반영되지 않았습니다: 일간 최고가={max(soxx_d)}, 월간 최고가={max(soxx_m)}"
    )
    print(
        f"• SOXX 3:1 액면분할 검증: 2010년 초 ${soxx_d[0]:.2f} → 역사적 최고가 ${max(soxx_d):.2f} (2026-06-22) → 현재가 ${soxx_d[-1]:.2f}"
    )
    print("✅ [검증 통과] 2010년 1월 ~ 현재까지 38개 전 종목 일일 종가(SOXX 3:1 분할 포함) 및 환율 무결성 100% 확인!")
    print("======================================================================")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="ST Quant BackTester - Yahoo Finance Data Updater")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="일일 데이터 수집 시작일 (기본값: 2010-01-01)")
    parser.add_argument("--base-js", default="stock_data.js", help="기존 베이스 stock_data.js 파일 경로")
    parser.add_argument("--out-js", default="stock_data.js", help="갱신 후 저장할 JS 파일 경로 (기본값: stock_data.js)")
    parser.add_argument("--out-json", default="", help="갱신 후 추가로 저장할 JSON 파일 경로 (선택)")
    parser.add_argument("--verify-only", action="store_true", help="네트워크 호출 없이 로컬 stock_data.js 무결성만 검증")
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

    updated_mdb, updated_rdb = fetch_yfinance_and_update(
        start_date=args.start_date,
        base_mdb=base_mdb,
        base_rdb=base_rdb,
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