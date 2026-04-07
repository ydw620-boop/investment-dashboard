"""
투자 대시보드 데이터 수집 파이프라인
====================================
매일 장 마감 후 18:30 이후 실행

데이터 소스:
  - pykrx: 시가총액, 외국인/기관 순매수 거래대금, 업종지수
  - KIS API: 보조/실시간 (백업)
  - KRX Open API: 지수 시세 (보조)

출력: dashboard_data.json → React 대시보드에서 로드

사용법:
  1. 아래 설정값 입력 (KIS_APP_KEY 등)
  2. pip install pykrx requests pandas numpy
  3. python collect_data.py
  4. 생성된 dashboard_data.json을 웹서버에 배포
"""

import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
import requests
import os

# ═══════════════════════════════════════════
# 설정
# ═══════════════════════════════════════════
CONFIG = {
    # KIS API (한국투자증권) - 선택사항, 없으면 pykrx만 사용
    "KIS_APP_KEY": os.environ.get("KIS_APP_KEY", "YOUR_APP_KEY_HERE"),
    "KIS_APP_SECRET": os.environ.get("KIS_APP_SECRET", "YOUR_APP_SECRET_HERE"),
    "KIS_BASE_URL": "https://openapi.koreainvestment.com:9443",
    
    # KRX Open API - 선택사항
    "KRX_API_KEY": os.environ.get("KRX_API_KEY", "YOUR_KRX_KEY_HERE"),
    
    # 데이터 수집 범위
    "LOOKBACK_DAYS": 120,       # 오실레이터 계산에 필요한 과거 일수
    "TOP_N_STOCKS": 30,         # 시총 상위 N개 종목
    
    # 오실레이터 파라미터 (엑셀과 동일)
    "EMA_SHORT": 12,            # EMA 단기
    "EMA_LONG": 26,             # EMA 장기
    "SIGNAL_PERIOD": 9,         # Signal EMA
    "FLOW_WINDOW": 5,           # 수급 누적 일수
    
    # 업종 목록 (WI 업종분류)
    "SECTOR_TICKERS": {
        "1001": "코스피",
        "2001": "코스닥",
        # KOSPI 업종
        "1003": "에너지", "1005": "화학", "1007": "비철,목재등",
        "1009": "철강", "1011": "건설,건축관련", "1013": "기계",
        "1015": "조선", "1017": "상사,자본재", "1019": "운송",
        "1021": "자동차", "1023": "화장품,의류,완구",
        "1027": "소매(유통)", "1029": "필수소비재",
        "1031": "건강관리", "1033": "은행", "1035": "증권",
        "1037": "보험", "1039": "소프트웨어", "1041": "IT하드웨어",
        "1043": "반도체", "1045": "IT가전", "1047": "디스플레이",
        "1049": "통신서비스", "1051": "유틸리티",
    },
    
    # 출력
    "OUTPUT_FILE": "dashboard_data.json",
}

# EMA 계수
K_SHORT = 2 / (CONFIG["EMA_SHORT"] + 1)
K_LONG = 2 / (CONFIG["EMA_LONG"] + 1)
K_SIGNAL = 2 / (CONFIG["SIGNAL_PERIOD"] + 1)


# ═══════════════════════════════════════════
# 1. pykrx 데이터 수집
# ═══════════════════════════════════════════
def collect_via_pykrx():
    """pykrx로 시가총액, 외국인/기관 순매수, 업종지수 수집"""
    from pykrx import stock
    
    today = datetime.now()
    start = today - timedelta(days=CONFIG["LOOKBACK_DAYS"] + 30)  # 여유분
    start_str = start.strftime("%Y%m%d")
    today_str = today.strftime("%Y%m%d")
    
    print(f"[1/5] 시가총액 상위 {CONFIG['TOP_N_STOCKS']}개 종목 확인...")
    
    # 최근 거래일의 시총 상위 종목
    cap_df = stock.get_market_cap(today_str)
    if cap_df.empty:
        # 오늘이 휴일이면 최근 거래일
        for d in range(1, 10):
            dt = (today - timedelta(days=d)).strftime("%Y%m%d")
            cap_df = stock.get_market_cap(dt)
            if not cap_df.empty:
                break
    
    top_tickers = cap_df.nlargest(CONFIG["TOP_N_STOCKS"], "시가총액").index.tolist()
    ticker_names = {t: stock.get_market_ticker_name(t) for t in top_tickers}
    
    print(f"   상위 종목: {', '.join([ticker_names[t] for t in top_tickers[:5]])}...")
    
    # ─── 종목별 데이터 수집 ───
    print(f"[2/5] 종목별 시가총액 + 수급 데이터 수집 ({len(top_tickers)}개)...")
    
    stock_data = {}
    for i, ticker in enumerate(top_tickers):
        name = ticker_names[ticker]
        print(f"   [{i+1}/{len(top_tickers)}] {name} ({ticker})...", end=" ")
        
        try:
            # 시가총액 (일별)
            cap_daily = stock.get_market_cap(start_str, today_str, ticker)
            
            # 투자자별 거래대금 (순매수) - 기관합계, 외국인합계
            trading = stock.get_market_trading_value_by_date(
                start_str, today_str, ticker
            )
            
            if cap_daily.empty or trading.empty:
                print("SKIP (no data)")
                continue
            
            # 날짜 기준으로 병합
            merged = cap_daily[["시가총액"]].join(
                trading[["기관합계", "외국인합계"]], how="inner"
            )
            merged = merged.dropna()
            
            dates = [d.strftime("%Y-%m-%d") for d in merged.index]
            caps = merged["시가총액"].values.astype(float) / 1e8  # 억원 단위
            inst_net = merged["기관합계"].values.astype(float) / 1e8
            fore_net = merged["외국인합계"].values.astype(float) / 1e8
            
            # 5일 누적 순매수
            inst_5d = pd.Series(inst_net).rolling(CONFIG["FLOW_WINDOW"]).sum().fillna(0).values
            fore_5d = pd.Series(fore_net).rolling(CONFIG["FLOW_WINDOW"]).sum().fillna(0).values
            
            # 20일 누적 매도대금 (절대값 합산)
            inst_sell_20d = pd.Series(
                np.where(inst_net < 0, np.abs(inst_net), 0)
            ).rolling(20).sum().fillna(0).values
            fore_sell_20d = pd.Series(
                np.where(fore_net < 0, np.abs(fore_net), 0)
            ).rolling(20).sum().fillna(0).values
            sell_20d_sum = inst_sell_20d + fore_sell_20d
            
            # 오실레이터 계산
            osc = calculate_oscillator(fore_5d + inst_5d, caps)
            
            stock_data[ticker] = {
                "n": name,
                "o": [round(float(v), 8) for v in osc],
                "c": [round(float(v), 2) for v in caps],
                "s": [round(float(v), 2) for v in sell_20d_sum],
            }
            
            print(f"OK ({len(dates)} days)")
            sleep(0.3)  # Rate limit 방지
            
        except Exception as e:
            print(f"ERROR: {e}")
            continue
    
    # 유효한 날짜 (마지막 종목 기준)
    stock_dates = dates if stock_data else []
    
    # ─── 업종별 데이터 수집 ───
    print(f"[3/5] 업종별 수급 오실레이터 수집...")
    
    sector_data = []
    sector_dates = []
    
    for idx_ticker, idx_name in CONFIG["SECTOR_TICKERS"].items():
        try:
            # 업종 지수 시세
            idx_ohlcv = stock.get_index_ohlcv(start_str, today_str, idx_ticker)
            if idx_ohlcv.empty:
                continue
            
            # 업종 투자자별 거래대금
            idx_trading = stock.get_index_trading_value_by_date(
                start_str, today_str, idx_ticker
            )
            
            if idx_trading.empty:
                continue
            
            # 병합
            merged = idx_ohlcv[["종가"]].join(
                idx_trading[["기관합계", "외국인합계"]], how="inner"
            )
            merged = merged.dropna()
            
            if len(merged) < 30:
                continue
            
            caps = merged["종가"].values.astype(float)
            inst_net = merged["기관합계"].values.astype(float) / 1e8
            fore_net = merged["외국인합계"].values.astype(float) / 1e8
            
            flow_5d = pd.Series(inst_net + fore_net).rolling(CONFIG["FLOW_WINDOW"]).sum().fillna(0).values
            
            # 업종은 시총 대신 지수값 사용 (상대 비율로 정규화)
            osc = calculate_oscillator(flow_5d, caps)
            
            if not sector_dates:
                sector_dates = [d.strftime("%Y-%m-%d") for d in merged.index]
            
            last_osc = round(float(osc[-1]) * 10000, 2)  # bps
            
            sector_data.append({
                "n": idx_name,
                "c": idx_ticker,
                "o": [round(float(v) * 10000, 2) for v in osc],
                "last": last_osc,
            })
            
            print(f"   {idx_name}: {last_osc:+.1f} bps")
            sleep(0.2)
            
        except Exception as e:
            print(f"   {idx_name}: ERROR {e}")
            continue
    
    # ─── 쏠림지수 계산 ───
    print(f"[4/5] 업종 쏠림지수 계산...")
    tilt_data = calculate_tilt_index(start_str, today_str)
    
    # ─── ETF 구성종목 (pykrx에서는 불가 → KRX 크롤링) ───
    print(f"[5/5] 액티브 ETF 데이터 수집...")
    etf_data = collect_active_etf_data()
    
    return {
        "stock": {"d": stock_dates, "s": stock_data},
        "sector": {"d": sector_dates, "s": sector_data},
        "tilt": tilt_data,
        "etf": etf_data,
    }


# ═══════════════════════════════════════════
# 2. 오실레이터 계산 (엑셀 로직 완전 동일)
# ═══════════════════════════════════════════
def calculate_oscillator(flow_5d, market_cap):
    """
    수급 오실레이터 = MACD(12,26,9) of (수급비율)
    
    수급비율 = 5일 누적 (외국인+기관 순매수) / 시가총액
    EMA12 = 수급비율의 12일 지수이동평균
    EMA26 = 수급비율의 26일 지수이동평균
    MACD = EMA12 - EMA26
    Signal = MACD의 9일 지수이동평균
    Oscillator = MACD - Signal
    """
    n = len(flow_5d)
    
    # 수급비율
    flow_ratio = np.where(market_cap > 0, flow_5d / market_cap, 0)
    
    # EMA 12
    ema12 = np.zeros(n)
    ema12[0] = flow_ratio[0]
    for t in range(1, n):
        ema12[t] = flow_ratio[t] * K_SHORT + ema12[t-1] * (1 - K_SHORT)
    
    # EMA 26
    ema26 = np.zeros(n)
    ema26[0] = flow_ratio[0]
    for t in range(1, n):
        ema26[t] = flow_ratio[t] * K_LONG + ema26[t-1] * (1 - K_LONG)
    
    # MACD
    macd = ema12 - ema26
    
    # Signal (EMA 9 of MACD)
    signal = np.zeros(n)
    signal[0] = macd[0]
    for t in range(1, n):
        signal[t] = macd[t] * K_SIGNAL + signal[t-1] * (1 - K_SIGNAL)
    
    # Oscillator
    return macd - signal


# ═══════════════════════════════════════════
# 3. 쏠림지수 계산
# ═══════════════════════════════════════════
def calculate_tilt_index(start_str, today_str):
    """
    특정업종 쏠림지수:
    1. 각 업종의 30일 수익률 계산
    2. 코스피200 동일가중지수 대비 업종별 수익률의 표준편차 = 쏠림
    3. 쏠림의 MACD 오실레이터
    """
    from pykrx import stock
    
    try:
        # 코스피, 코스닥 지수
        kospi = stock.get_index_ohlcv(start_str, today_str, "1001")
        kosdaq = stock.get_index_ohlcv(start_str, today_str, "2001")
        
        if kospi.empty or kosdaq.empty:
            return {"kp": {"d":[],"v":[],"o":[],"c":[]}, "kd": {"d":[],"v":[],"o":[],"c":[]}}
        
        # 업종별 30일 수익률
        sector_tickers = ["1003","1005","1009","1011","1013","1015",
                          "1017","1019","1021","1023","1027","1029",
                          "1031","1033","1035","1037","1039","1041",
                          "1043","1045","1047","1049","1051"]
        
        returns_list = []
        for st in sector_tickers:
            try:
                idx = stock.get_index_ohlcv(start_str, today_str, st)
                if not idx.empty:
                    ret_30d = idx["종가"].pct_change(30) * 100
                    returns_list.append(ret_30d)
            except:
                continue
            sleep(0.1)
        
        if not returns_list:
            return {"kp": {"d":[],"v":[],"o":[],"c":[]}, "kd": {"d":[],"v":[],"o":[],"c":[]}}
        
        # 업종 수익률 DataFrame
        returns_df = pd.concat(returns_list, axis=1).dropna()
        
        # 쏠림 = 업종별 수익률의 표준편차
        tilt_std = returns_df.std(axis=1)
        
        # 롱숏 시그널 (코스피 대비 코스닥 상대강도)
        common_idx = kospi.index.intersection(kosdaq.index).intersection(tilt_std.index)
        
        kp = kospi.loc[common_idx, "종가"].values
        kd = kosdaq.loc[common_idx, "종가"].values
        tilt = tilt_std.loc[common_idx].values
        dates = [d.strftime("%Y-%m-%d") for d in common_idx]
        
        # 쏠림지수 지수화 (1000 기준)
        indexed = np.cumsum(tilt) + 1000
        
        # MACD 오실레이터
        n = len(tilt)
        ema12 = np.zeros(n); ema12[0] = indexed[0]
        ema26 = np.zeros(n); ema26[0] = indexed[0]
        for t in range(1, n):
            ema12[t] = indexed[t] * K_SHORT + ema12[t-1] * (1 - K_SHORT)
            ema26[t] = indexed[t] * K_LONG + ema26[t-1] * (1 - K_LONG)
        macd = ema12 - ema26
        signal = np.zeros(n); signal[0] = macd[0]
        for t in range(1, n):
            signal[t] = macd[t] * K_SIGNAL + signal[t-1] * (1 - K_SIGNAL)
        osc = macd - signal
        
        # 상관관계 (코스피/코스닥 vs 쏠림지수의 20일 롤링 상관)
        kp_ret = pd.Series(kp).pct_change().fillna(0)
        kd_ret = pd.Series(kd).pct_change().fillna(0)
        tilt_s = pd.Series(tilt)
        corr_kp = kp_ret.rolling(20).corr(tilt_s).fillna(0).values
        corr_kd = kd_ret.rolling(20).corr(tilt_s).fillna(0).values
        
        # 최근 120일만
        sl = max(0, n - 120)
        
        return {
            "kp": {
                "d": dates[sl:],
                "v": [round(float(v), 2) for v in kp[sl:]],
                "o": [round(float(v), 4) for v in osc[sl:]],
                "c": [round(float(v), 4) for v in corr_kp[sl:]],
            },
            "kd": {
                "d": dates[sl:],
                "v": [round(float(v), 2) for v in kd[sl:]],
                "o": [round(float(v), 4) for v in osc[sl:]],
                "c": [round(float(v), 4) for v in corr_kd[sl:]],
            },
        }
    except Exception as e:
        print(f"   쏠림지수 계산 오류: {e}")
        return {"kp": {"d":[],"v":[],"o":[],"c":[]}, "kd": {"d":[],"v":[],"o":[],"c":[]}}


# ═══════════════════════════════════════════
# 4. 액티브 ETF 구성종목 (KRX 크롤링)
# ═══════════════════════════════════════════
def collect_active_etf_data():
    """KRX data.krx.co.kr에서 액티브 ETF PDF(구성종목) 크롤링"""
    
    # 주요 액티브 ETF 목록 (종목코드, 이름, 카테고리)
    etf_list = [
        ("472170", "KoAct 코스닥액티브", "코스닥액티브"),
        ("395170", "TIMEFOLIO 코스닥액티브", "코스닥액티브"),
        ("429760", "PLUS 코스닥150액티브", "코스닥액티브"),
        ("456600", "UNICORN SK하이닉스밸류체인액티브", "반도체"),
        ("457480", "WON 반도체밸류체인액티브", "반도체"),
        ("448290", "RISE 비메모리반도체액티브", "반도체"),
        ("473640", "SOL 코리아메가테크액티브", "수급"),
        ("472160", "KoAct 배당성장액티브", "배당성장"),
        ("459350", "TIMEFOLIO Korea플러스배당액티브", "배당성장"),
        ("400590", "KODEX 신재생에너지액티브", "신재생,2차전지"),
        ("396520", "TIMEFOLIO K이노베이션액티브", "이노베이션"),
        ("395160", "TIMEFOLIO 코스피액티브", "코스피"),
        ("442100", "KODEX 친환경조선해운액티브", "조선"),
        ("449190", "KODEX 로봇액티브", "로봇"),
        ("385720", "TIMEFOLIO K바이오액티브", "바이오"),
    ]
    
    etf_data = {}
    today_str = datetime.now().strftime("%Y%m%d")
    
    for etf_code, etf_name, category in etf_list:
        try:
            # KRX ETF PDF 조회
            url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
            params = {
                "bld": "dbms/MDC/STAT/standard/MDCSTAT05001",
                "tboxisuCd_finder_secuprodisu1_1": f"{etf_code}/{etf_name}",
                "isuCd": etf_code,
                "isuCd2": etf_code,
                "codeNmisuCd_finder_secuprodisu1_1": etf_name,
                "param1isuCd_finder_secuprodisu1_1": "",
                "strtDd": today_str,
                "endDd": today_str,
                "csvxls_isNo": "false",
            }
            
            resp = requests.post(url, data=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if "output" in data and data["output"]:
                    holdings = []
                    for item in data["output"][:15]:
                        holdings.append({
                            "n": item.get("ISU_ABBRV", ""),
                            "w": float(item.get("COMPN_WT", "0").replace(",", "")),
                            "p": 0,  # 이전 비중은 별도 조회 필요
                            "d": 0,
                        })
                    
                    etf_data[etf_name] = {
                        "c": category,
                        "cd": today_str,
                        "pd": "",
                        "h": holdings,
                    }
                    print(f"   {etf_name}: {len(holdings)}종목")
            
            sleep(0.3)
        except Exception as e:
            print(f"   {etf_name}: ERROR {e}")
            continue
    
    # ETF 크롤링 실패 시 빈 데이터
    if not etf_data:
        print("   ⚠ ETF 크롤링 실패 - 수동 업데이트 필요")
    
    return etf_data


# ═══════════════════════════════════════════
# 5. KIS API 보조 함수 (백업용)
# ═══════════════════════════════════════════
def get_kis_token():
    """KIS API 접근 토큰 발급"""
    url = f"{CONFIG['KIS_BASE_URL']}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": CONFIG["KIS_APP_KEY"],
        "appsecret": CONFIG["KIS_APP_SECRET"],
    }
    resp = requests.post(url, json=body)
    return resp.json().get("access_token", "")


def get_kis_stock_investor(ticker, token):
    """KIS API로 종목별 투자자 매매동향 조회"""
    url = f"{CONFIG['KIS_BASE_URL']}/uapi/domestic-stock/v1/quotations/investor"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": CONFIG["KIS_APP_KEY"],
        "appsecret": CONFIG["KIS_APP_SECRET"],
        "tr_id": "FHKST01010900",
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": ticker,
    }
    resp = requests.get(url, headers=headers, params=params)
    return resp.json()


# ═══════════════════════════════════════════
# 메인 실행
# ═══════════════════════════════════════════
def main():
    print("=" * 60)
    print(f"📊 투자 대시보드 데이터 수집")
    print(f"   시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 데이터 수집
    data = collect_via_pykrx()
    
    # 결과 요약
    n_stocks = len(data["stock"]["s"])
    n_sectors = len(data["sector"]["s"])
    n_etfs = len(data["etf"])
    
    print(f"\n{'=' * 60}")
    print(f"✅ 수집 완료")
    print(f"   종목: {n_stocks}개")
    print(f"   업종: {n_sectors}개")
    print(f"   ETF: {n_etfs}개")
    print(f"   쏠림지수: 코스피 {len(data['tilt']['kp']['d'])}일, 코스닥 {len(data['tilt']['kd']['d'])}일")
    
    # JSON 저장
    output_path = CONFIG["OUTPUT_FILE"]
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    
    file_size = os.path.getsize(output_path)
    print(f"   파일: {output_path} ({file_size/1024:.1f} KB)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
