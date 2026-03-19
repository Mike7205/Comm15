"""
news_bucket.py
==============
Twice-daily news aggregator for 27 global market tickers.

Buckets (pandas DataFrames, persisted as CSV):
  fyahoo_bucket       – Yahoo Finance / yfinance
  alpha_vantage_bucket – Alpha Vantage NEWS_SENTIMENT
  finnhub_bucket      – Finnhub company/market news
  twelve_data_bucket  – Twelve Data news
  eodhd_bucket        – EODHD news
  coingecko_bucket    – CoinGecko (BTC, ETH only)

Columns: Date + all 27 ticker symbols
Each cell: pipe-separated news headlines for that ticker on that day.

Scheduler: runs update at 09:00 and 16:00 daily.
API keys: set as environment variables or in .env file.
"""

import os
import time
import logging
import requests
import schedule
import pandas as pd
import yfinance as yf
from datetime import date, datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("news_bucket.log")]
)
log = logging.getLogger(__name__)

# ── API Keys ─────────────────────────────────────────────────────────────────
AV_KEY      = os.getenv("ALPHA_VANTAGE_KEY", "")
FH_KEY      = os.getenv("FINNHUB_KEY", "")
TD_KEY      = os.getenv("TWELVE_DATA_KEY", "")
EODHD_KEY   = os.getenv("EODHD_KEY", "")
# CoinGecko public API – no key required

# ── Data directory ────────────────────────────────────────────────────────────
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# ── 27 Tickers ───────────────────────────────────────────────────────────────
TICKERS = [
    '^GSPC', '^DJI', '^IXIC', '000001.SS', '^HSI', '^VIX',
    '^RUT', '^BVSP', '^FTSE', '^GDAXI', '^N225',
    'EURUSD=X', 'EURCHF=X', 'CNY=X', 'GBPUSD=X', 'JPY=X',
    'EURPLN=X', 'PLN=X', 'RUB=X', 'DX-Y.NYB',
    '^FVX', '^TNX', '^TYX',
    'CL=F', 'GC=F', 'SI=F',
    'BTC-USD', 'ETH-USD',
]

# ── Ticker → friendly search keyword (for APIs without direct index support) ──
KEYWORDS = {
    '^GSPC': 'S&P 500',        '^DJI': 'Dow Jones',       '^IXIC': 'NASDAQ',
    '000001.SS': 'SSE Composite', '^HSI': 'Hang Seng',    '^VIX': 'VIX volatility',
    '^RUT': 'Russell 2000',    '^BVSP': 'Ibovespa',       '^FTSE': 'FTSE 100',
    '^GDAXI': 'DAX',           '^N225': 'Nikkei 225',
    'EURUSD=X': 'EUR USD',     'EURCHF=X': 'EUR CHF',     'CNY=X': 'USD CNY',
    'GBPUSD=X': 'GBP USD',     'JPY=X': 'USD JPY',        'EURPLN=X': 'EUR PLN',
    'PLN=X': 'USD PLN',        'RUB=X': 'USD RUB',        'DX-Y.NYB': 'Dollar Index DXY',
    '^FVX': 'US 5-year Treasury yield', '^TNX': 'US 10-year Treasury yield',
    '^TYX': 'US 30-year Treasury yield',
    'CL=F': 'crude oil',       'GC=F': 'gold price',      'SI=F': 'silver price',
    'BTC-USD': 'Bitcoin',      'ETH-USD': 'Ethereum',
}

# ── Alpha Vantage ticker format ───────────────────────────────────────────────
AV_MAP = {
    '^GSPC': 'SPX',         '^DJI': 'DJI',          '^IXIC': 'IXIC',
    '000001.SS': None,      '^HSI': None,            '^VIX': None,
    '^RUT': 'RUT',          '^BVSP': None,           '^FTSE': None,
    '^GDAXI': None,         '^N225': None,
    'EURUSD=X': 'FOREX:EURUSD', 'EURCHF=X': 'FOREX:EURCHF', 'CNY=X': 'FOREX:USDCNY',
    'GBPUSD=X': 'FOREX:GBPUSD', 'JPY=X': 'FOREX:USDJPY',  'EURPLN=X': 'FOREX:EURPLN',
    'PLN=X': 'FOREX:USDPLN', 'RUB=X': 'FOREX:USDRUB',     'DX-Y.NYB': None,
    '^FVX': None,           '^TNX': None,            '^TYX': None,
    'CL=F': None,           'GC=F': None,            'SI=F': None,
    'BTC-USD': 'CRYPTO:BTC', 'ETH-USD': 'CRYPTO:ETH',
}

# ── Finnhub symbol format (ETF proxies for indices) ──────────────────────────
FH_MAP = {
    '^GSPC': 'SPY',     '^DJI': 'DIA',      '^IXIC': 'QQQ',
    '000001.SS': None,  '^HSI': None,        '^VIX': None,
    '^RUT': 'IWM',      '^BVSP': 'EWZ',     '^FTSE': 'ISF',
    '^GDAXI': None,     '^N225': 'EWJ',
    'EURUSD=X': None,   'EURCHF=X': None,   'CNY=X': None,
    'GBPUSD=X': None,   'JPY=X': None,      'EURPLN=X': None,
    'PLN=X': None,      'RUB=X': None,      'DX-Y.NYB': None,
    '^FVX': None,       '^TNX': None,        '^TYX': None,
    'CL=F': None,       'GC=F': None,        'SI=F': None,
    'BTC-USD': 'BINANCE:BTCUSDT', 'ETH-USD': 'BINANCE:ETHUSDT',
}

# ── Twelve Data symbol format ─────────────────────────────────────────────────
TD_MAP = {
    '^GSPC': 'SPX',     '^DJI': 'DJI',      '^IXIC': 'IXIC',
    '000001.SS': None,  '^HSI': 'HSI',       '^VIX': 'VIX',
    '^RUT': 'RUT',      '^BVSP': 'IBOV',    '^FTSE': 'UKX',
    '^GDAXI': 'DAX',    '^N225': 'N225',
    'EURUSD=X': 'EUR/USD', 'EURCHF=X': 'EUR/CHF', 'CNY=X': 'USD/CNY',
    'GBPUSD=X': 'GBP/USD', 'JPY=X': 'USD/JPY',   'EURPLN=X': 'EUR/PLN',
    'PLN=X': 'USD/PLN', 'RUB=X': 'USD/RUB',       'DX-Y.NYB': 'DXY',
    '^FVX': None,       '^TNX': None,        '^TYX': None,
    'CL=F': 'WTI/USD',  'GC=F': 'XAU/USD',  'SI=F': 'XAG/USD',
    'BTC-USD': 'BTC/USD', 'ETH-USD': 'ETH/USD',
}

# ── EODHD symbol format ───────────────────────────────────────────────────────
EODHD_MAP = {
    '^GSPC': 'GSPC.INDX',  '^DJI': 'DJI.INDX',    '^IXIC': 'IXIC.INDX',
    '000001.SS': None,     '^HSI': 'HSI.INDX',     '^VIX': 'VIX.INDX',
    '^RUT': 'RUT.INDX',    '^BVSP': 'BVSP.INDX',  '^FTSE': 'FTSE.INDX',
    '^GDAXI': 'GDAXI.INDX', '^N225': 'N225.INDX',
    'EURUSD=X': 'EURUSD.FOREX', 'EURCHF=X': 'EURCHF.FOREX', 'CNY=X': 'USDCNY.FOREX',
    'GBPUSD=X': 'GBPUSD.FOREX', 'JPY=X': 'USDJPY.FOREX',   'EURPLN=X': 'EURPLN.FOREX',
    'PLN=X': 'USDPLN.FOREX', 'RUB=X': 'USDRUB.FOREX',      'DX-Y.NYB': 'DX.INDX',
    '^FVX': None,          '^TNX': None,           '^TYX': None,
    'CL=F': 'CL.COMM',    'GC=F': 'GC.COMM',      'SI=F': 'SI.COMM',
    'BTC-USD': 'BTC-USD.CC', 'ETH-USD': 'ETH-USD.CC',
}

# ── CoinGecko – only BTC and ETH ─────────────────────────────────────────────
CG_MAP = {
    'BTC-USD': 'bitcoin',
    'ETH-USD': 'ethereum',
}

# ─────────────────────────────────────────────────────────────────────────────
# Bucket persistence helpers
# ─────────────────────────────────────────────────────────────────────────────

def _csv_path(name: str) -> Path:
    return DATA_DIR / f"{name}.csv"


def load_bucket(name: str) -> pd.DataFrame:
    """Load bucket CSV or create empty DataFrame with correct columns."""
    path = _csv_path(name)
    if path.exists():
        df = pd.read_csv(path, parse_dates=['Date'])
        return df
    df = pd.DataFrame(columns=['Date'] + TICKERS)
    return df


def save_bucket(name: str, df: pd.DataFrame) -> None:
    df.to_csv(_csv_path(name), index=False)
    log.info(f"Saved {name} → {_csv_path(name)}")


def upsert_row(df: pd.DataFrame, today_str: str, updates: dict) -> pd.DataFrame:
    """Insert or update today's row in the bucket."""
    if today_str in df['Date'].astype(str).values:
        mask = df['Date'].astype(str) == today_str
        for col, val in updates.items():
            if col in df.columns and val:
                df.loc[mask, col] = val
    else:
        row = {'Date': today_str}
        row.update({t: '' for t in TICKERS})
        row.update({k: v for k, v in updates.items() if v})
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    return df


def _join(items: list, max_items: int = 5) -> str:
    """Join headline list into a pipe-separated string."""
    cleaned = [str(h).strip() for h in items if h][:max_items]
    return ' | '.join(cleaned)


# ─────────────────────────────────────────────────────────────────────────────
# Fetch functions – one per source
# ─────────────────────────────────────────────────────────────────────────────

def fetch_yfinance(ticker: str) -> str:
    """Return pipe-separated headlines from yfinance .news"""
    try:
        news = yf.Ticker(ticker).news or []
        headlines = [
            n.get('content', {}).get('title') or n.get('title', '')
            for n in news
        ]
        return _join(headlines)
    except Exception as e:
        log.warning(f"[yfinance] {ticker}: {e}")
        return ''


def fetch_alpha_vantage(ticker: str) -> str:
    """Return pipe-separated headlines from Alpha Vantage NEWS_SENTIMENT."""
    if not AV_KEY:
        return ''
    av_sym = AV_MAP.get(ticker)
    params = {
        'function': 'NEWS_SENTIMENT',
        'apikey': AV_KEY,
        'limit': 5,
        'sort': 'LATEST',
    }
    if av_sym:
        params['tickers'] = av_sym
    else:
        # Fall back to keyword topic search
        params['topics'] = KEYWORDS.get(ticker, ticker)

    try:
        r = requests.get('https://www.alphavantage.co/query', params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        items = data.get('feed', [])
        headlines = [item.get('title', '') for item in items]
        return _join(headlines)
    except Exception as e:
        log.warning(f"[Alpha Vantage] {ticker}: {e}")
        return ''


def fetch_finnhub(ticker: str) -> str:
    """Return pipe-separated headlines from Finnhub."""
    if not FH_KEY:
        return ''
    fh_sym = FH_MAP.get(ticker)
    today_str = date.today().isoformat()

    try:
        if fh_sym:
            # Stock / ETF / crypto news by symbol
            r = requests.get(
                'https://finnhub.io/api/v1/company-news',
                params={'symbol': fh_sym, 'from': today_str, 'to': today_str, 'token': FH_KEY},
                timeout=10
            )
        else:
            # General market news by category
            category = 'crypto' if ticker in ('BTC-USD', 'ETH-USD') else 'forex' \
                if '=X' in ticker or ticker == 'DX-Y.NYB' else 'general'
            r = requests.get(
                'https://finnhub.io/api/v1/news',
                params={'category': category, 'token': FH_KEY},
                timeout=10
            )
        r.raise_for_status()
        items = r.json() if isinstance(r.json(), list) else []
        headlines = [item.get('headline', '') for item in items]
        return _join(headlines)
    except Exception as e:
        log.warning(f"[Finnhub] {ticker}: {e}")
        return ''


def fetch_twelve_data(ticker: str) -> str:
    """Return pipe-separated headlines from Twelve Data."""
    if not TD_KEY:
        return ''
    td_sym = TD_MAP.get(ticker)
    if not td_sym:
        return ''

    try:
        r = requests.get(
            'https://api.twelvedata.com/news',
            params={'symbol': td_sym, 'apikey': TD_KEY, 'outputsize': 5},
            timeout=10
        )
        r.raise_for_status()
        data = r.json()
        items = data.get('data', [])
        headlines = [item.get('title', '') for item in items]
        return _join(headlines)
    except Exception as e:
        log.warning(f"[Twelve Data] {ticker}: {e}")
        return ''


def fetch_eodhd(ticker: str) -> str:
    """Return pipe-separated headlines from EODHD."""
    if not EODHD_KEY:
        return ''
    eodhd_sym = EODHD_MAP.get(ticker)
    if not eodhd_sym:
        return ''

    try:
        r = requests.get(
            'https://eodhd.com/api/news',
            params={'s': eodhd_sym, 'api_token': EODHD_KEY, 'limit': 5, 'fmt': 'json'},
            timeout=10
        )
        r.raise_for_status()
        items = r.json() if isinstance(r.json(), list) else []
        headlines = [item.get('title', '') for item in items]
        return _join(headlines)
    except Exception as e:
        log.warning(f"[EODHD] {ticker}: {e}")
        return ''


def fetch_coingecko(ticker: str) -> str:
    """Return market summary text from CoinGecko (BTC/ETH only)."""
    cg_id = CG_MAP.get(ticker)
    if not cg_id:
        return ''

    try:
        r = requests.get(
            f'https://api.coingecko.com/api/v3/coins/{cg_id}',
            params={'localization': 'false', 'tickers': 'false',
                    'community_data': 'false', 'developer_data': 'false'},
            timeout=10
        )
        r.raise_for_status()
        data = r.json()
        market = data.get('market_data', {})
        price   = market.get('current_price', {}).get('usd', 'n/a')
        chg_24h = market.get('price_change_percentage_24h', 'n/a')
        cap     = market.get('market_cap', {}).get('usd', 'n/a')
        vol     = market.get('total_volume', {}).get('usd', 'n/a')
        name    = data.get('name', cg_id)
        return (f"{name}: price=${price:,.2f}" if isinstance(price, float) else f"{name}: price=${price}") + \
               (f" | 24h change={chg_24h:.2f}%" if isinstance(chg_24h, float) else '') + \
               (f" | mktcap=${cap:,.0f}" if isinstance(cap, (int, float)) else '') + \
               (f" | vol=${vol:,.0f}" if isinstance(vol, (int, float)) else '')
    except Exception as e:
        log.warning(f"[CoinGecko] {ticker}: {e}")
        return ''


# ─────────────────────────────────────────────────────────────────────────────
# Update engine
# ─────────────────────────────────────────────────────────────────────────────

SOURCES = [
    ('fyahoo_bucket',        fetch_yfinance),
    ('alpha_vantage_bucket', fetch_alpha_vantage),
    ('finnhub_bucket',       fetch_finnhub),
    ('twelve_data_bucket',   fetch_twelve_data),
    ('eodhd_bucket',         fetch_eodhd),
    ('coingecko_bucket',     fetch_coingecko),
]


def update_bucket(bucket_name: str, fetch_fn) -> None:
    """Fetch news for all tickers and upsert into the named bucket."""
    log.info(f"▶ Updating {bucket_name} ...")
    df = load_bucket(bucket_name)
    today_str = date.today().isoformat()
    updates = {}

    for ticker in TICKERS:
        text = fetch_fn(ticker)
        updates[ticker] = text
        log.debug(f"  {ticker}: {text[:60]}..." if text else f"  {ticker}: (empty)")
        time.sleep(0.4)   # polite rate-limit pause between tickers

    df = upsert_row(df, today_str, updates)
    save_bucket(bucket_name, df)
    log.info(f"✔ {bucket_name} done ({len([v for v in updates.values() if v])}/{len(TICKERS)} tickers filled)")


def run_all_updates() -> None:
    """Run updates for all 6 buckets."""
    log.info("═══ Starting full news update ═══")
    for bucket_name, fetch_fn in SOURCES:
        try:
            update_bucket(bucket_name, fetch_fn)
        except Exception as e:
            log.error(f"[{bucket_name}] Unexpected error: {e}")
    log.info("═══ Full update complete ═══")


# ─────────────────────────────────────────────────────────────────────────────
# Scheduler – twice daily at 09:00 and 16:00
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("news_bucket.py started – scheduled at 09:00 and 16:00 daily")
    log.info(f"API keys loaded: AV={'✔' if AV_KEY else '✗'}  "
             f"Finnhub={'✔' if FH_KEY else '✗'}  "
             f"TwelveData={'✔' if TD_KEY else '✗'}  "
             f"EODHD={'✔' if EODHD_KEY else '✗'}")

    # Run immediately on startup so buckets are populated right away
    run_all_updates()

    schedule.every().day.at("09:00").do(run_all_updates)
    schedule.every().day.at("16:00").do(run_all_updates)

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
