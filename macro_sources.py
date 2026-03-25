import csv
import io
import json
import math
import re
import time
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote as urlquote, urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import feedparser

from digest_runtime import LOGGER, write_json_artifact

GOOGLE_NEWS_SEARCH_URL = "https://news.google.com/rss/search"
YAHOO_QUERY_HOSTS = [
    "query1.finance.yahoo.com",
    "query2.finance.yahoo.com",
]
FRANKFURTER_RANGE_URL_TEMPLATE = "https://api.frankfurter.app/{date_range}"
FRED_SERIES_URL_TEMPLATE = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
TREASURY_TEXTVIEW_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "TextView?type=daily_treasury_yield_curve"
)
TREASURY_TEXTVIEW_MONTH_URL_TEMPLATE = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "TextView?field_tdr_date_value_month={year_month}&type=daily_treasury_yield_curve"
)
TREASURY_CSV_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "daily-treasury-rates.csv/all/all?_format=csv&page=&type=daily_treasury_yield_curve"
)
MACRO_MARKET_CACHE_PATH = Path("macro_market_cache.json")
YAHOO_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
TREASURY_TEXTVIEW_HEADING = "Daily Treasury Par Yield Curve Rates"
STOOQ_DAILY_URL_TEMPLATE = "https://stooq.com/q/d/l/?s={symbol}&i=d"
FRANKFURTER_LOOKBACK_DAYS = 10
DXY_INDEX_MULTIPLIER = 50.14348112
DXY_COMPONENT_WEIGHTS = {
    "eurusd": -0.576,
    "usdjpy": 0.136,
    "gbpusd": -0.119,
    "usdcad": 0.091,
    "usdsek": 0.042,
    "usdchf": 0.036,
}
DXY_STOOQ_SUPPORT_SPECS = [
    {
        "pair_key": "gbpusd",
        "symbol": "GBPUSD_INTERNAL",
        "label": "GBP/USD support",
        "stooq_symbol": "gbpusd",
        "source": "stooq_fx_support",
    },
    {
        "pair_key": "usdcad",
        "symbol": "USDCAD_INTERNAL",
        "label": "USD/CAD support",
        "stooq_symbol": "usdcad",
        "source": "stooq_fx_support",
    },
    {
        "pair_key": "usdsek",
        "symbol": "USDSEK_INTERNAL",
        "label": "USD/SEK support",
        "stooq_symbol": "usdsek",
        "source": "stooq_fx_support",
    },
    {
        "pair_key": "usdchf",
        "symbol": "USDCHF_INTERNAL",
        "label": "USD/CHF support",
        "stooq_symbol": "usdchf",
        "source": "stooq_fx_support",
    },
]
HTTP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

MACRO_NEWS_BUCKETS = {
    "macro_geopolitics": {
        "label": "Macro/Geopolitics",
        "queries": [
            "Federal Reserve ECB BOJ PBOC central bank policy",
            "tariffs sanctions export controls trade policy",
            "OPEC geopolitics sovereign debt",
        ],
    },
    "commodities": {
        "label": "Commodities",
        "queries": [
            "oil supply OPEC crude",
            "natural gas LNG",
            "gold copper commodity markets",
        ],
    },
    "rates": {
        "label": "Rates",
        "queries": [
            "Treasury yields yield curve",
            "2-year Treasury 10-year Treasury",
            "rate cuts bond market",
        ],
    },
    "equities": {
        "label": "Equities",
        "queries": [
            "technology sector rotation",
            "energy sector financial sector",
            "defense sector market",
        ],
    },
    "fx": {
        "label": "FX",
        "queries": [
            "dollar index DXY",
            "yen intervention USDJPY",
            "euro yuan FX market",
        ],
    },
}

MARKET_GROUPS = {
    "commodities": [
        {
            "key": "wti_crude",
            "label": "WTI Crude (USO proxy)",
            "symbol": "CL=F",
            "stooq_symbol": "uso.us",
            "yfinance_symbol": "CL=F",
            "source": "stooq_proxy",
            "currency": "USD",
        },
        {
            "key": "natural_gas",
            "label": "Natural Gas (UNG proxy)",
            "symbol": "NG=F",
            "stooq_symbol": "ung.us",
            "yfinance_symbol": "NG=F",
            "source": "stooq_proxy",
            "currency": "USD",
        },
        {
            "key": "gold",
            "label": "Gold (GLD proxy)",
            "symbol": "GC=F",
            "stooq_symbol": "gld.us",
            "yfinance_symbol": "GC=F",
            "source": "stooq_proxy",
            "currency": "USD",
        },
        {
            "key": "copper",
            "label": "Copper (CPER proxy)",
            "symbol": "HG=F",
            "stooq_symbol": "cper.us",
            "yfinance_symbol": "HG=F",
            "yfinance_label": "Copper",
            "yfinance_unit": "US$ per tonne",
            "yfinance_price_multiplier": 2204.62262185,
            "source": "stooq_proxy",
            "currency": "USD",
        },
    ],
    "equities": [
        {"key": "technology", "label": "Technology", "symbol": "XLK", "stooq_symbol": "xlk.us", "yfinance_symbol": "XLK", "currency": "USD"},
        {"key": "energy", "label": "Energy", "symbol": "XLE", "stooq_symbol": "xle.us", "yfinance_symbol": "XLE", "currency": "USD"},
        {"key": "financials", "label": "Financials", "symbol": "XLF", "stooq_symbol": "xlf.us", "yfinance_symbol": "XLF", "currency": "USD"},
        {"key": "defense", "label": "Aerospace & Defense", "symbol": "ITA", "stooq_symbol": "ita.us", "yfinance_symbol": "ITA", "currency": "USD"},
    ],
    "fx": [
        {
            "key": "dxy",
            "label": "US Dollar Index (DXY)",
            "symbol": "DXY",
            "yfinance_symbol": "DX-Y.NYB",
            "currency": "USD",
        },
        {
            "key": "eurusd",
            "label": "EUR/USD",
            "symbol": "EURUSD=X",
            "stooq_symbol": "eurusd",
            "yfinance_symbol": "EURUSD=X",
            "source": "stooq_fx",
            "fx_source": "frankfurter",
            "currency": "USD",
        },
        {
            "key": "usdjpy",
            "label": "USD/JPY",
            "symbol": "JPY=X",
            "stooq_symbol": "usdjpy",
            "yfinance_symbol": "JPY=X",
            "source": "stooq_fx",
            "fx_source": "frankfurter",
            "currency": "JPY",
        },
        {
            "key": "usdcny",
            "label": "USD/CNY",
            "symbol": "CNY=X",
            "stooq_symbol": "usdcny",
            "yfinance_symbol": "CNY=X",
            "source": "stooq_fx",
            "fx_source": "frankfurter",
            "currency": "CNY",
        },
    ],
}


def clean_text(value):
    return " ".join(str(value or "").split())


def load_macro_market_cache():
    if not MACRO_MARKET_CACHE_PATH.exists():
        return {}

    try:
        with MACRO_MARKET_CACHE_PATH.open(encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception as exc:
        LOGGER.warning(
            "Failed to load macro market cache, ignoring cache | path=%s error=%s",
            MACRO_MARKET_CACHE_PATH,
            exc,
        )
        return {}

    if not isinstance(payload, dict):
        LOGGER.warning(
            "Macro market cache must be a JSON object, ignoring cache | path=%s",
            MACRO_MARKET_CACHE_PATH,
        )
        return {}

    quote_map = payload.get("quote_map")
    if isinstance(quote_map, dict) and quote_map:
        sanitized_quote_map, dropped_symbols = sanitize_quote_map_records(quote_map)
        if sanitized_quote_map != quote_map:
            payload["quote_map"] = sanitized_quote_map
            if dropped_symbols:
                LOGGER.warning(
                    "Removed invalid quote records from macro market cache | dropped=%d symbols=%s",
                    len(dropped_symbols),
                    dropped_symbols,
                )
            else:
                LOGGER.info("Normalized macro market cache quote metadata | count=%d", len(sanitized_quote_map))
            try:
                save_macro_market_cache(payload)
            except Exception as exc:
                LOGGER.warning(
                    "Failed to rewrite sanitized macro market cache | path=%s error=%s",
                    MACRO_MARKET_CACHE_PATH,
                    exc,
                )

    return payload


def save_macro_market_cache(payload):
    with MACRO_MARKET_CACHE_PATH.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, allow_nan=False)
        handle.write("\n")


def update_macro_market_cache(quote_map=None, rates_snapshot=None):
    cache = load_macro_market_cache()
    cache["updated_at"] = datetime.now(timezone.utc).isoformat()

    if quote_map:
        sanitized_quote_map, dropped_symbols = sanitize_quote_map_records(quote_map)
        cache["quote_cached_at"] = cache["updated_at"]
        cache["quote_map"] = sanitized_quote_map
        if dropped_symbols:
            LOGGER.warning(
                "Dropped invalid quote records before updating market cache | dropped=%d symbols=%s",
                len(dropped_symbols),
                dropped_symbols,
            )

    if rates_snapshot:
        cache["rates_cached_at"] = cache["updated_at"]
        cache["rates_snapshot"] = rates_snapshot

    save_macro_market_cache(cache)


def get_cached_quote_map():
    cache = load_macro_market_cache()
    quote_map = cache.get("quote_map")
    cached_at = cache.get("quote_cached_at")
    if not isinstance(quote_map, dict) or not quote_map:
        return None, None

    cached_quote_map = deepcopy(quote_map)
    for item in cached_quote_map.values():
        item["source"] = f"cache:{item.get('source', 'unknown')}"
        item["stale"] = True
        item["cached_at"] = cached_at

    return cached_quote_map, cached_at


def get_cached_rates_snapshot():
    cache = load_macro_market_cache()
    rates_snapshot = cache.get("rates_snapshot")
    cached_at = cache.get("rates_cached_at")
    if not isinstance(rates_snapshot, dict) or not rates_snapshot:
        return None, None

    cached_snapshot = deepcopy(rates_snapshot)
    cached_snapshot["source"] = f"cache:{cached_snapshot.get('source', 'unknown')}"
    cached_snapshot["stale"] = True
    cached_snapshot["cached_at"] = cached_at
    return cached_snapshot, cached_at


def build_http_headers(accept=None):
    headers = {
        "User-Agent": HTTP_USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if accept:
        headers["Accept"] = accept
    return headers


def to_float(value):
    if value in (None, ""):
        return None
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric_value):
        return None
    return numeric_value


def isoformat_or_none(value):
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def normalize_headline_key(title, source):
    normalized_title = clean_text(title).lower()
    normalized_source = clean_text(source).lower()
    return f"{normalized_title}::{normalized_source}"


def build_google_news_url(query):
    params = {
        "q": query,
        "hl": "en-US",
        "gl": "US",
        "ceid": "US:en",
    }
    return f"{GOOGLE_NEWS_SEARCH_URL}?{urlencode(params)}"


def extract_feed_timestamp(entry, local_tz):
    parsed_time = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if not parsed_time:
        return None

    timestamp = datetime(
        parsed_time.tm_year,
        parsed_time.tm_mon,
        parsed_time.tm_mday,
        parsed_time.tm_hour,
        parsed_time.tm_min,
        parsed_time.tm_sec,
        tzinfo=timezone.utc,
    )
    return timestamp.astimezone(local_tz)


def extract_feed_source(entry):
    source = getattr(entry, "source", None)
    if hasattr(source, "title"):
        return clean_text(getattr(source, "title"))
    if isinstance(source, dict):
        return clean_text(source.get("title"))
    return ""


def fetch_google_news_query(bucket_key, bucket_label, query, config):
    local_tz = ZoneInfo(config["local_timezone"])
    cutoff_local = datetime.now(local_tz) - timedelta(hours=config["macro_news_lookback_hours"])
    url = build_google_news_url(query)
    start_time = time.perf_counter()
    LOGGER.info(
        "Fetching macro news feed | bucket=%s query=%s lookback_hours=%s url=%s",
        bucket_key,
        query,
        config["macro_news_lookback_hours"],
        url,
    )

    xml_text = fetch_text_url_with_retries(
        url,
        config["macro_news_timeout_seconds"],
        config["macro_news_retries"],
        label=f"google-news:{bucket_key}",
        failure_level="info",
    )
    feed = feedparser.parse(xml_text.encode("utf-8"))
    duration = time.perf_counter() - start_time
    if getattr(feed, "bozo", False):
        LOGGER.warning(
            "Macro news feed parser warning | bucket=%s query=%s error=%s",
            bucket_key,
            query,
            getattr(feed, "bozo_exception", "unknown"),
        )

    headlines = []
    for entry in getattr(feed, "entries", []):
        published_local = extract_feed_timestamp(entry, local_tz)
        if published_local is not None and published_local < cutoff_local:
            continue

        headlines.append(
            {
                "bucket": bucket_key,
                "bucket_label": bucket_label,
                "query": query,
                "title": clean_text(getattr(entry, "title", "")),
                "link": clean_text(getattr(entry, "link", "")),
                "source": extract_feed_source(entry) or "Unknown Source",
                "published_at": isoformat_or_none(published_local),
            }
        )

    LOGGER.info(
        "Fetched macro news feed | bucket=%s query=%s entries=%d duration=%.2fs",
        bucket_key,
        query,
        len(headlines),
        duration,
    )
    return headlines


def sort_headlines(headlines):
    def sort_key(item):
        published_at = item.get("published_at") or ""
        return (published_at, item.get("title", ""))

    return sorted(headlines, key=sort_key, reverse=True)


def fetch_macro_news(config):
    tasks = []
    for bucket_key, bucket_spec in MACRO_NEWS_BUCKETS.items():
        for query in bucket_spec["queries"]:
            tasks.append((bucket_key, bucket_spec["label"], query))

    worker_count = min(config["macro_news_max_workers"], len(tasks)) if tasks else 1
    LOGGER.info(
        "Macro news aggregation started | tasks=%d workers=%d",
        len(tasks),
        worker_count,
    )

    bucket_results = {
        bucket_key: {
            "label": bucket_spec["label"],
            "headlines": [],
            "errors": [],
            "queries": list(bucket_spec["queries"]),
        }
        for bucket_key, bucket_spec in MACRO_NEWS_BUCKETS.items()
    }

    if not tasks:
        payload = {"buckets": bucket_results, "total_headlines": 0}
        write_json_artifact("macro_news_inputs.json", payload)
        return payload

    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="macro-news") as executor:
        future_map = {
            executor.submit(fetch_google_news_query, bucket_key, bucket_label, query, config): (
                bucket_key,
                query,
            )
            for bucket_key, bucket_label, query in tasks
        }

        for future in as_completed(future_map):
            bucket_key, query = future_map[future]
            try:
                headlines = future.result()
                bucket_results[bucket_key]["headlines"].extend(headlines)
            except Exception as exc:
                LOGGER.warning(
                    "Macro news fetch failed | bucket=%s query=%s error=%s",
                    bucket_key,
                    query,
                    exc,
                )
                bucket_results[bucket_key]["errors"].append(
                    {
                        "query": query,
                        "error": str(exc),
                    }
                )

    total_headlines = 0
    for bucket_key, bucket_result in bucket_results.items():
        deduped = []
        seen_keys = set()
        for item in sort_headlines(bucket_result["headlines"]):
            key = normalize_headline_key(item["title"], item["source"])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(item)
            if len(deduped) >= config["macro_max_headlines_per_bucket"]:
                break
        bucket_result["headlines"] = deduped
        total_headlines += len(deduped)
        LOGGER.info(
            "Macro news bucket ready | bucket=%s headlines=%d errors=%d",
            bucket_key,
            len(bucket_result["headlines"]),
            len(bucket_result["errors"]),
        )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_hours": config["macro_news_lookback_hours"],
        "max_headlines_per_bucket": config["macro_max_headlines_per_bucket"],
        "buckets": bucket_results,
        "total_headlines": total_headlines,
    }
    write_json_artifact("macro_news_inputs.json", payload)
    return payload


def fetch_json_url(url, timeout_seconds):
    request = Request(
        url,
        headers=build_http_headers(accept="application/json"),
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_text_url(url, timeout_seconds):
    request = Request(
        url,
        headers=build_http_headers(),
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8")


def fetch_json_url_with_retries(urls, timeout_seconds, attempts, label, failure_level="warning"):
    if isinstance(urls, str):
        urls = [urls]

    last_exc = None
    log_failure = getattr(LOGGER, failure_level, LOGGER.warning)
    for attempt in range(1, attempts + 1):
        url = urls[(attempt - 1) % len(urls)]
        start_time = time.perf_counter()
        try:
            LOGGER.info(
                "HTTP json fetch started | label=%s attempt=%d/%d timeout=%ss url=%s",
                label,
                attempt,
                attempts,
                timeout_seconds,
                url,
            )
            payload = fetch_json_url(url, timeout_seconds)
            duration = time.perf_counter() - start_time
            LOGGER.info(
                "HTTP json fetch finished | label=%s attempt=%d/%d duration=%.2fs",
                label,
                attempt,
                attempts,
                duration,
            )
            return payload
        except HTTPError as exc:
            duration = time.perf_counter() - start_time
            last_exc = exc
            log_failure(
                "HTTP json fetch failed | label=%s attempt=%d/%d duration=%.2fs status=%s reason=%s url=%s",
                label,
                attempt,
                attempts,
                duration,
                exc.code,
                exc.reason,
                url,
            )
            if exc.code not in YAHOO_RETRYABLE_STATUS_CODES or attempt >= attempts:
                raise
            time.sleep(min(2.0 * attempt, 6.0))
        except Exception as exc:
            duration = time.perf_counter() - start_time
            last_exc = exc
            log_failure(
                "HTTP json fetch failed | label=%s attempt=%d/%d duration=%.2fs error=%s url=%s",
                label,
                attempt,
                attempts,
                duration,
                exc,
                url,
            )
            if attempt >= attempts:
                raise
            time.sleep(min(2.0 * attempt, 6.0))

    raise last_exc


def fetch_text_url_with_retries(url, timeout_seconds, attempts, label, failure_level="warning"):
    last_exc = None
    log_failure = getattr(LOGGER, failure_level, LOGGER.warning)
    for attempt in range(1, attempts + 1):
        start_time = time.perf_counter()
        try:
            LOGGER.info(
                "HTTP text fetch started | label=%s attempt=%d/%d timeout=%ss url=%s",
                label,
                attempt,
                attempts,
                timeout_seconds,
                url,
            )
            text = fetch_text_url(url, timeout_seconds)
            duration = time.perf_counter() - start_time
            LOGGER.info(
                "HTTP text fetch finished | label=%s attempt=%d/%d duration=%.2fs",
                label,
                attempt,
                attempts,
                duration,
            )
            return text
        except Exception as exc:
            duration = time.perf_counter() - start_time
            last_exc = exc
            log_failure(
                "HTTP text fetch failed | label=%s attempt=%d/%d duration=%.2fs error=%s",
                label,
                attempt,
                attempts,
                duration,
                exc,
            )
            if attempt < attempts:
                time.sleep(min(1.5 * attempt, 3.0))

    raise last_exc


def build_yahoo_quote_urls(symbols):
    query = urlencode({"symbols": ",".join(symbols)})
    return [f"https://{host}/v7/finance/quote?{query}" for host in YAHOO_QUERY_HOSTS]


def build_yahoo_chart_urls(symbol):
    encoded_symbol = urlquote(symbol, safe="")
    query = urlencode(
        {
            "interval": "1d",
            "range": "5d",
            "includePrePost": "false",
            "events": "div,splits",
        }
    )
    return [f"https://{host}/v8/finance/chart/{encoded_symbol}?{query}" for host in YAHOO_QUERY_HOSTS]


def iter_market_specs():
    for group_key, specs in MARKET_GROUPS.items():
        for spec in specs:
            yield group_key, spec


def build_market_spec_map():
    return {spec["symbol"]: spec for _, spec in iter_market_specs()}


def parse_csv_text(csv_text):
    sample = csv_text[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;")
    except csv.Error:
        dialect = csv.excel
    return list(csv.DictReader(io.StringIO(csv_text), dialect=dialect))


def build_stooq_url(symbol):
    return STOOQ_DAILY_URL_TEMPLATE.format(symbol=urlquote(symbol, safe=""))


def build_treasury_textview_month_url(target_date):
    return TREASURY_TEXTVIEW_MONTH_URL_TEMPLATE.format(year_month=target_date.strftime("%Y%m"))


def build_frankfurter_range_url(start_date, end_date, symbols):
    date_range = f"{start_date.isoformat()}..{end_date.isoformat()}"
    params = {"to": ",".join(symbols)}
    return f"{FRANKFURTER_RANGE_URL_TEMPLATE.format(date_range=date_range)}?{urlencode(params)}"


def compute_change_metrics(latest_price, previous_price):
    change = None
    change_pct = None
    if latest_price is not None and previous_price is not None:
        change = round(latest_price - previous_price, 6)
        if previous_price != 0:
            change_pct = round((change / previous_price) * 100, 6)
    return change, change_pct


def compute_previous_price(price, change):
    numeric_price = to_float(price)
    numeric_change = to_float(change)
    if numeric_price is None or numeric_change is None:
        return None
    return round(numeric_price - numeric_change, 6)


def build_price_quote(symbol, name, latest_price, previous_price, currency, source):
    change, change_pct = compute_change_metrics(latest_price, previous_price)
    return normalize_quote_record(
        symbol=symbol,
        name=name,
        price=latest_price,
        change=change,
        change_pct=change_pct,
        currency=currency,
        market_time_epoch=None,
        source=source,
    )


def build_price_quote_with_unit(symbol, name, latest_price, previous_price, currency, source, unit=None):
    change, change_pct = compute_change_metrics(latest_price, previous_price)
    return normalize_quote_record(
        symbol=symbol,
        name=name,
        price=latest_price,
        change=change,
        change_pct=change_pct,
        currency=currency,
        market_time_epoch=None,
        source=source,
        unit=unit,
    )


def parse_stooq_quote_csv(spec, csv_text):
    rows = parse_csv_text(csv_text)
    valid_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        date_value = clean_text(row.get("Date"))
        close_value = to_float(row.get("Close"))
        if not date_value or close_value is None:
            continue
        valid_rows.append({"date": date_value, "close": close_value})

    if not valid_rows:
        raise RuntimeError(f"Stooq returned no usable rows for {spec['stooq_symbol']}")

    valid_rows.sort(key=lambda item: item["date"])
    latest_row = valid_rows[-1]
    previous_row = valid_rows[-2] if len(valid_rows) > 1 else None
    price = latest_row["close"]
    previous_price = previous_row["close"] if previous_row is not None else None
    return build_price_quote(
        symbol=spec["symbol"],
        name=spec["label"],
        latest_price=price,
        previous_price=previous_price,
        currency=spec.get("currency", "USD"),
        source=spec.get("source", "stooq"),
    )


def fetch_stooq_symbol_quote(spec, config):
    url = build_stooq_url(spec["stooq_symbol"])
    csv_text = fetch_text_url_with_retries(
        url,
        config["macro_market_timeout_seconds"],
        config["stooq_max_retries"],
        label=f"stooq:{spec['stooq_symbol']}",
    )
    return parse_stooq_quote_csv(spec, csv_text)


def fetch_stooq_quotes_for_specs(config, specs, label):
    if not specs:
        return {}

    quote_map = {}
    errors = []
    worker_count = min(config["stooq_max_workers"], len(specs))
    LOGGER.info(
        "Fetching Stooq support quotes | label=%s symbols=%d workers=%d",
        label,
        len(specs),
        worker_count,
    )

    if worker_count == 1:
        for spec in specs:
            try:
                quote_map[spec["symbol"]] = fetch_stooq_symbol_quote(spec, config)
            except Exception as exc:
                LOGGER.info(
                    "Stooq support quote fetch failed | label=%s symbol=%s error=%s",
                    label,
                    spec["symbol"],
                    exc,
                )
                errors.append({"symbol": spec["symbol"], "error": str(exc)})
            time.sleep(0.6)
    else:
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="stooq-support") as executor:
            future_map = {
                executor.submit(fetch_stooq_symbol_quote, spec, config): spec
                for spec in specs
            }
            for future in as_completed(future_map):
                spec = future_map[future]
                try:
                    quote_map[spec["symbol"]] = future.result()
                except Exception as exc:
                    LOGGER.info(
                        "Stooq support quote fetch failed | label=%s symbol=%s error=%s",
                        label,
                        spec["symbol"],
                        exc,
                    )
                    errors.append({"symbol": spec["symbol"], "error": str(exc)})

    if errors:
        LOGGER.info(
            "Stooq support quote fetch completed with gaps | label=%s requested=%d succeeded=%d failed=%d",
            label,
            len(specs),
            len(quote_map),
            len(errors),
        )
    else:
        LOGGER.info(
            "Stooq support quote fetch succeeded | label=%s count=%d",
            label,
            len(quote_map),
        )

    return quote_map


def fetch_stooq_quotes(config):
    specs = [spec for _, spec in iter_market_specs() if spec.get("stooq_symbol")]
    if not specs:
        return {}

    worker_count = min(config["stooq_max_workers"], len(specs))
    LOGGER.info(
        "Fetching market quotes from Stooq | symbols=%d workers=%d",
        len(specs),
        worker_count,
    )

    quote_map = {}
    errors = []

    if worker_count == 1:
        for spec in specs:
            try:
                quote_map[spec["symbol"]] = fetch_stooq_symbol_quote(spec, config)
            except Exception as exc:
                LOGGER.info("Stooq quote fetch failed | symbol=%s error=%s", spec["symbol"], exc)
                errors.append({"symbol": spec["symbol"], "error": str(exc)})
            time.sleep(0.6)
    else:
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="stooq") as executor:
            future_map = {
                executor.submit(fetch_stooq_symbol_quote, spec, config): spec
                for spec in specs
            }
            for future in as_completed(future_map):
                spec = future_map[future]
                try:
                    quote_map[spec["symbol"]] = future.result()
                except Exception as exc:
                    LOGGER.info("Stooq quote fetch failed | symbol=%s error=%s", spec["symbol"], exc)
                    errors.append({"symbol": spec["symbol"], "error": str(exc)})

    if errors:
        LOGGER.info(
            "Stooq quote fetch completed with gaps | requested=%d succeeded=%d failed=%d",
            len(specs),
            len(quote_map),
            len(errors),
        )
    else:
        LOGGER.info("Stooq quote fetch succeeded for all symbols | count=%d", len(quote_map))

    return quote_map


def compute_dxy_value(pair_values):
    if not isinstance(pair_values, dict):
        return None

    product = DXY_INDEX_MULTIPLIER
    for pair_key, weight in DXY_COMPONENT_WEIGHTS.items():
        value = to_float(pair_values.get(pair_key))
        if value is None or value <= 0:
            return None
        product *= value ** weight
    return round(product, 6)


def extract_dxy_pair_values_from_frankfurter_row(row):
    usd = to_float(row.get("USD"))
    jpy = to_float(row.get("JPY"))
    gbp = to_float(row.get("GBP"))
    cad = to_float(row.get("CAD"))
    sek = to_float(row.get("SEK"))
    chf = to_float(row.get("CHF"))
    if usd in (None, 0):
        return None
    if any(value in (None, 0) for value in [jpy, gbp, cad, sek, chf]):
        return None
    return {
        "eurusd": usd,
        "usdjpy": jpy / usd,
        "gbpusd": usd / gbp,
        "usdcad": cad / usd,
        "usdsek": sek / usd,
        "usdchf": chf / usd,
    }


def build_dxy_quote(symbol, latest_value, previous_value, source):
    return build_price_quote(
        symbol=symbol,
        name="US Dollar Index (DXY)",
        latest_price=latest_value,
        previous_price=previous_value,
        currency="USD",
        source=source,
    )


def extract_close_values_from_history_frame(frame):
    candidates = []
    for column_name in ["Close", "Adj Close"]:
        if column_name not in frame:
            continue
        values = [to_float(value) for value in frame[column_name].tolist()]
        values = [value for value in values if value is not None]
        if values:
            candidates = values
            break
    return candidates


def parse_yfinance_history_frame(spec, frame):
    close_values = extract_close_values_from_history_frame(frame)
    if not close_values:
        raise RuntimeError(f"yfinance returned no usable close values for {spec['symbol']}")

    multiplier = spec.get("yfinance_price_multiplier")
    if multiplier is not None:
        close_values = [round(value * multiplier, 6) for value in close_values]

    latest_value = close_values[-1]
    previous_value = close_values[-2] if len(close_values) > 1 else None
    return build_price_quote_with_unit(
        symbol=spec["symbol"],
        name=spec.get("yfinance_label", spec["label"]),
        latest_price=latest_value,
        previous_price=previous_value,
        currency=spec.get("currency", "USD"),
        source="yfinance",
        unit=spec.get("yfinance_unit"),
    )


def fetch_yfinance_market_quotes(config, symbols=None):
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed") from exc

    spec_map = build_market_spec_map()
    if symbols is None:
        specs = list(spec_map.values())
    else:
        specs = [spec_map[symbol] for symbol in symbols if symbol in spec_map]

    if not specs:
        return {}

    yfinance_symbol_map = {
        spec.get("yfinance_symbol", spec["symbol"]): spec
        for spec in specs
    }
    yfinance_symbols = list(yfinance_symbol_map.keys())
    start_time = time.perf_counter()
    LOGGER.info(
        "Fetching market quotes from yfinance | symbols=%d tickers=%s",
        len(yfinance_symbols),
        yfinance_symbols,
    )
    history = yf.download(
        tickers=" ".join(yfinance_symbols),
        period="5d",
        interval="1d",
        auto_adjust=False,
        progress=False,
        group_by="ticker",
        threads=False,
    )

    quote_map = {}
    if history is None or history.empty:
        LOGGER.info(
            "yfinance batch download returned no market history, retrying individually | requested=%d",
            len(yfinance_symbols),
        )
    else:
        columns = getattr(history, "columns", None)
        nlevels = getattr(columns, "nlevels", 1)
        if len(yfinance_symbols) == 1:
            spec = next(iter(yfinance_symbol_map.values()))
            quote_map[spec["symbol"]] = parse_yfinance_history_frame(spec, history)
        elif nlevels == 1:
            LOGGER.info(
                "yfinance returned a non-multiindex frame for multiple symbols, retrying individually | requested=%d",
                len(yfinance_symbols),
            )
        else:
            available_symbols = set(history.columns.get_level_values(0))
            for yfinance_symbol, spec in yfinance_symbol_map.items():
                if yfinance_symbol not in available_symbols:
                    continue
                frame = history[yfinance_symbol]
                try:
                    quote_map[spec["symbol"]] = parse_yfinance_history_frame(spec, frame)
                except Exception as exc:
                    LOGGER.info(
                        "yfinance quote parse failed | symbol=%s ticker=%s error=%s",
                        spec["symbol"],
                        yfinance_symbol,
                        exc,
                    )

    missing_specs = [
        spec
        for yfinance_symbol, spec in yfinance_symbol_map.items()
        if spec["symbol"] not in quote_map
    ]
    if missing_specs:
        LOGGER.info(
            "Retrying yfinance individually for missing symbols | missing=%d symbols=%s",
            len(missing_specs),
            [spec["symbol"] for spec in missing_specs],
        )
        for spec in missing_specs:
            yfinance_symbol = spec.get("yfinance_symbol", spec["symbol"])
            try:
                frame = yf.Ticker(yfinance_symbol).history(
                    period="5d",
                    interval="1d",
                    auto_adjust=False,
                )
                if frame is None or frame.empty:
                    raise RuntimeError("yfinance individual history was empty")
                quote_map[spec["symbol"]] = parse_yfinance_history_frame(spec, frame)
                LOGGER.info(
                    "yfinance individual retry succeeded | symbol=%s ticker=%s rows=%d",
                    spec["symbol"],
                    yfinance_symbol,
                    len(frame),
                )
            except Exception as exc:
                LOGGER.info(
                    "yfinance individual retry failed | symbol=%s ticker=%s error=%s",
                    spec["symbol"],
                    yfinance_symbol,
                    exc,
                )
            time.sleep(0.4)

    if not quote_map:
        raise RuntimeError("yfinance returned no usable market history")

    duration = time.perf_counter() - start_time
    LOGGER.info(
        "Fetched market quotes from yfinance | requested=%d received=%d duration=%.2fs",
        len(yfinance_symbols),
        len(quote_map),
        duration,
    )
    return quote_map


def build_dxy_quote_from_stooq(config, quote_map):
    support_quotes = fetch_stooq_quotes_for_specs(
        config,
        DXY_STOOQ_SUPPORT_SPECS,
        label="dxy-components",
    )

    latest_pairs = {
        "eurusd": to_float(quote_map.get("EURUSD=X", {}).get("price")),
        "usdjpy": to_float(quote_map.get("JPY=X", {}).get("price")),
        "gbpusd": to_float(support_quotes.get("GBPUSD_INTERNAL", {}).get("price")),
        "usdcad": to_float(support_quotes.get("USDCAD_INTERNAL", {}).get("price")),
        "usdsek": to_float(support_quotes.get("USDSEK_INTERNAL", {}).get("price")),
        "usdchf": to_float(support_quotes.get("USDCHF_INTERNAL", {}).get("price")),
    }
    previous_pairs = {
        "eurusd": compute_previous_price(
            quote_map.get("EURUSD=X", {}).get("price"),
            quote_map.get("EURUSD=X", {}).get("change"),
        ),
        "usdjpy": compute_previous_price(
            quote_map.get("JPY=X", {}).get("price"),
            quote_map.get("JPY=X", {}).get("change"),
        ),
        "gbpusd": compute_previous_price(
            support_quotes.get("GBPUSD_INTERNAL", {}).get("price"),
            support_quotes.get("GBPUSD_INTERNAL", {}).get("change"),
        ),
        "usdcad": compute_previous_price(
            support_quotes.get("USDCAD_INTERNAL", {}).get("price"),
            support_quotes.get("USDCAD_INTERNAL", {}).get("change"),
        ),
        "usdsek": compute_previous_price(
            support_quotes.get("USDSEK_INTERNAL", {}).get("price"),
            support_quotes.get("USDSEK_INTERNAL", {}).get("change"),
        ),
        "usdchf": compute_previous_price(
            support_quotes.get("USDCHF_INTERNAL", {}).get("price"),
            support_quotes.get("USDCHF_INTERNAL", {}).get("change"),
        ),
    }

    latest_value = compute_dxy_value(latest_pairs)
    if latest_value is None:
        raise RuntimeError("Stooq DXY support quotes were insufficient to derive DXY")

    previous_value = compute_dxy_value(previous_pairs)
    return build_dxy_quote(
        symbol="DXY",
        latest_value=latest_value,
        previous_value=previous_value,
        source="dxy_formula_stooq",
    )


def parse_frankfurter_fx_payload(payload):
    rates_payload = payload.get("rates")
    if not isinstance(rates_payload, dict) or not rates_payload:
        raise RuntimeError("Frankfurter FX payload is empty")

    rows = []
    for date_key, rates in rates_payload.items():
        if not isinstance(rates, dict):
            continue
        usd = to_float(rates.get("USD"))
        jpy = to_float(rates.get("JPY"))
        cny = to_float(rates.get("CNY"))
        gbp = to_float(rates.get("GBP"))
        cad = to_float(rates.get("CAD"))
        sek = to_float(rates.get("SEK"))
        chf = to_float(rates.get("CHF"))
        if all(value is None for value in [usd, jpy, cny, gbp, cad, sek, chf]):
            continue
        rows.append(
            {
                "date": clean_text(date_key),
                "USD": usd,
                "JPY": jpy,
                "CNY": cny,
                "GBP": gbp,
                "CAD": cad,
                "SEK": sek,
                "CHF": chf,
            }
        )

    if not rows:
        raise RuntimeError("Frankfurter FX payload contained no usable rows")

    rows.sort(key=lambda item: item["date"])

    eurusd_rows = [row for row in rows if row["USD"] is not None]
    usdjpy_rows = [row for row in rows if row["USD"] not in (None, 0) and row["JPY"] is not None]
    usdcny_rows = [row for row in rows if row["USD"] not in (None, 0) and row["CNY"] is not None]

    if not eurusd_rows and not usdjpy_rows and not usdcny_rows:
        raise RuntimeError("Frankfurter FX payload did not include any required pairs")

    quote_map = {}

    if eurusd_rows:
        latest_row = eurusd_rows[-1]
        previous_row = eurusd_rows[-2] if len(eurusd_rows) > 1 else None
        quote_map["EURUSD=X"] = build_price_quote(
            symbol="EURUSD=X",
            name="EUR/USD",
            latest_price=latest_row["USD"],
            previous_price=previous_row["USD"] if previous_row is not None else None,
            currency="USD",
            source="frankfurter_fx",
        )

    if usdjpy_rows:
        latest_row = usdjpy_rows[-1]
        previous_row = usdjpy_rows[-2] if len(usdjpy_rows) > 1 else None
        latest_price = latest_row["JPY"] / latest_row["USD"]
        previous_price = None
        if previous_row is not None:
            previous_price = previous_row["JPY"] / previous_row["USD"]
        quote_map["JPY=X"] = build_price_quote(
            symbol="JPY=X",
            name="USD/JPY",
            latest_price=latest_price,
            previous_price=previous_price,
            currency="JPY",
            source="frankfurter_fx",
        )

    if usdcny_rows:
        latest_row = usdcny_rows[-1]
        previous_row = usdcny_rows[-2] if len(usdcny_rows) > 1 else None
        latest_price = latest_row["CNY"] / latest_row["USD"]
        previous_price = None
        if previous_row is not None:
            previous_price = previous_row["CNY"] / previous_row["USD"]
        quote_map["CNY=X"] = build_price_quote(
            symbol="CNY=X",
            name="USD/CNY",
            latest_price=latest_price,
            previous_price=previous_price,
            currency="CNY",
            source="frankfurter_fx",
        )

    dxy_rows = []
    for row in rows:
        dxy_pairs = extract_dxy_pair_values_from_frankfurter_row(row)
        dxy_value = compute_dxy_value(dxy_pairs)
        if dxy_value is None:
            continue
        dxy_rows.append({"date": row["date"], "value": dxy_value})

    if dxy_rows:
        latest_row = dxy_rows[-1]
        previous_row = dxy_rows[-2] if len(dxy_rows) > 1 else None
        quote_map["DXY"] = build_dxy_quote(
            symbol="DXY",
            latest_value=latest_row["value"],
            previous_value=previous_row["value"] if previous_row is not None else None,
            source="dxy_formula_frankfurter",
        )

    return quote_map


def fetch_frankfurter_fx_quotes(config):
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=FRANKFURTER_LOOKBACK_DAYS)
    url = build_frankfurter_range_url(
        start_date,
        end_date,
        ["USD", "JPY", "CNY", "GBP", "CAD", "SEK", "CHF"],
    )
    start_time = time.perf_counter()
    LOGGER.info(
        "Fetching FX quotes from Frankfurter | url=%s lookback_days=%d",
        url,
        FRANKFURTER_LOOKBACK_DAYS,
    )
    payload = fetch_json_url_with_retries(
        url,
        config["macro_market_timeout_seconds"],
        config["macro_market_retries"],
        label="frankfurter:fx",
        failure_level="info",
    )
    quote_map = parse_frankfurter_fx_payload(payload)
    duration = time.perf_counter() - start_time
    LOGGER.info(
        "Fetched FX quotes from Frankfurter | symbols_returned=%d duration=%.2fs",
        len(quote_map),
        duration,
    )
    return quote_map


def last_non_null(values):
    for value in reversed(values or []):
        numeric_value = to_float(value)
        if numeric_value is not None:
            return numeric_value
    return None


def previous_non_null(values):
    seen_latest = False
    for value in reversed(values or []):
        numeric_value = to_float(value)
        if numeric_value is None:
            continue
        if not seen_latest:
            seen_latest = True
            continue
        return numeric_value
    return None


def normalize_quote_record(symbol, name, price, change, change_pct, currency, market_time_epoch, source, unit=None):
    return {
        "symbol": symbol,
        "name": clean_text(name or symbol),
        "price": to_float(price),
        "change": to_float(change),
        "change_pct": to_float(change_pct),
        "currency": clean_text(currency),
        "market_time_epoch": market_time_epoch,
        "source": clean_text(source),
        "unit": clean_text(unit),
    }


def sanitize_quote_map_records(quote_map):
    sanitized_map = {}
    dropped_symbols = []
    if not isinstance(quote_map, dict):
        return sanitized_map, dropped_symbols

    spec_map = build_market_spec_map()
    for symbol, item in quote_map.items():
        if not isinstance(item, dict):
            dropped_symbols.append(clean_text(symbol) or str(symbol))
            continue

        record_symbol = clean_text(item.get("symbol") or symbol)
        spec = spec_map.get(record_symbol, {})
        record_source = clean_text(item.get("source") or "unknown")
        record_name = item.get("name") or record_symbol
        record_unit = item.get("unit")
        record_currency = item.get("currency") or spec.get("currency")

        if record_source.startswith("yfinance"):
            record_name = spec.get("yfinance_label", record_name)
            record_unit = record_unit or spec.get("yfinance_unit")

        sanitized_record = normalize_quote_record(
            symbol=record_symbol,
            name=record_name,
            price=item.get("price"),
            change=item.get("change"),
            change_pct=item.get("change_pct"),
            currency=record_currency,
            market_time_epoch=item.get("market_time_epoch"),
            source=record_source,
            unit=record_unit,
        )
        if sanitized_record["price"] is None:
            dropped_symbols.append(record_symbol)
            continue

        for key in ["stale", "cached_at"]:
            if key in item:
                sanitized_record[key] = item.get(key)
        sanitized_map[record_symbol] = sanitized_record

    return sanitized_map, dropped_symbols


def parse_quote_endpoint_payload(payload):
    quote_map = {}
    for item in payload.get("quoteResponse", {}).get("result", []):
        symbol = clean_text(item.get("symbol"))
        if not symbol:
            continue
        quote_map[symbol] = normalize_quote_record(
            symbol=symbol,
            name=(
                item.get("shortName")
                or item.get("displayName")
                or item.get("longName")
                or symbol
            ),
            price=item.get("regularMarketPrice"),
            change=item.get("regularMarketChange"),
            change_pct=item.get("regularMarketChangePercent"),
            currency=item.get("currency"),
            market_time_epoch=item.get("regularMarketTime"),
            source="yahoo_quote",
        )
    return quote_map


def fetch_yahoo_batch_quotes(symbols, config):
    urls = build_yahoo_quote_urls(symbols)
    start_time = time.perf_counter()
    LOGGER.info(
        "Fetching market quotes from Yahoo quote endpoint | symbols=%d hosts=%s",
        len(symbols),
        YAHOO_QUERY_HOSTS,
    )
    payload = fetch_json_url_with_retries(
        urls,
        config["macro_market_timeout_seconds"],
        config["yahoo_max_retries"],
        label="yahoo:quote",
    )
    duration = time.perf_counter() - start_time
    quote_map = parse_quote_endpoint_payload(payload)
    LOGGER.info(
        "Fetched market quotes from Yahoo quote endpoint | symbols_returned=%d duration=%.2fs",
        len(quote_map),
        duration,
    )
    return quote_map


def parse_chart_endpoint_payload(symbol, payload):
    chart = payload.get("chart", {})
    error = chart.get("error")
    if error:
        raise RuntimeError(f"Yahoo chart returned error for {symbol}: {error}")

    results = chart.get("result") or []
    if not results:
        raise RuntimeError(f"Yahoo chart returned no result for {symbol}")

    result = results[0]
    meta = result.get("meta", {})
    timestamps = result.get("timestamp") or []
    indicators = result.get("indicators", {})
    quote_items = indicators.get("quote") or [{}]
    quote_item = quote_items[0] if quote_items else {}
    closes = quote_item.get("close") or []

    price = to_float(meta.get("regularMarketPrice"))
    if price is None:
        price = last_non_null(closes)

    previous_close = (
        to_float(meta.get("chartPreviousClose"))
        or to_float(meta.get("previousClose"))
        or previous_non_null(closes)
    )

    change = None
    change_pct = None
    if price is not None and previous_close is not None:
        change = round(price - previous_close, 6)
        if previous_close != 0:
            change_pct = round((change / previous_close) * 100, 6)

    return normalize_quote_record(
        symbol=symbol,
        name=meta.get("shortName") or meta.get("symbol") or symbol,
        price=price,
        change=change,
        change_pct=change_pct,
        currency=meta.get("currency"),
        market_time_epoch=meta.get("regularMarketTime") or (timestamps[-1] if timestamps else None),
        source="yahoo_chart",
    )


def fetch_yahoo_chart_quote(symbol, config):
    start_time = time.perf_counter()
    urls = build_yahoo_chart_urls(symbol)
    LOGGER.info(
        "Fetching market quote from Yahoo chart endpoint | symbol=%s hosts=%s",
        symbol,
        YAHOO_QUERY_HOSTS,
    )
    payload = fetch_json_url_with_retries(
        urls,
        config["macro_market_timeout_seconds"],
        config["yahoo_max_retries"],
        label=f"yahoo:chart:{symbol}",
    )
    duration = time.perf_counter() - start_time
    quote = parse_chart_endpoint_payload(symbol, payload)
    LOGGER.info(
        "Fetched market quote from Yahoo chart endpoint | symbol=%s price=%s duration=%.2fs",
        symbol,
        quote["price"],
        duration,
    )
    return quote


def fetch_yahoo_chart_quotes(symbols, config):
    if not symbols:
        return {}

    worker_count = min(config["yahoo_chart_max_workers"], len(symbols))
    quote_map = {}
    errors = []

    if worker_count == 1:
        for index, symbol in enumerate(symbols, start=1):
            try:
                quote_map[symbol] = fetch_yahoo_chart_quote(symbol, config)
            except Exception as exc:
                LOGGER.warning("Yahoo chart fallback failed | symbol=%s error=%s", symbol, exc)
                errors.append({"symbol": symbol, "error": str(exc)})
            if index < len(symbols):
                time.sleep(1.25)

        if errors:
            LOGGER.warning(
                "Yahoo chart fallback completed with gaps | requested=%d succeeded=%d failed=%d workers=%d",
                len(symbols),
                len(quote_map),
                len(errors),
                worker_count,
            )
        else:
            LOGGER.info(
                "Yahoo chart fallback succeeded for all symbols | count=%d workers=%d",
                len(quote_map),
                worker_count,
            )
        return quote_map

    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="yahoo-chart") as executor:
        future_map = {
            executor.submit(fetch_yahoo_chart_quote, symbol, config): symbol
            for symbol in symbols
        }
        for future in as_completed(future_map):
            symbol = future_map[future]
            try:
                quote_map[symbol] = future.result()
            except Exception as exc:
                LOGGER.warning("Yahoo chart fallback failed | symbol=%s error=%s", symbol, exc)
                errors.append({"symbol": symbol, "error": str(exc)})

    if errors:
        LOGGER.warning(
            "Yahoo chart fallback completed with gaps | requested=%d succeeded=%d failed=%d workers=%d",
            len(symbols),
            len(quote_map),
            len(errors),
            worker_count,
        )
    else:
        LOGGER.info(
            "Yahoo chart fallback succeeded for all symbols | count=%d workers=%d",
            len(quote_map),
            worker_count,
        )

    return quote_map


def fetch_yahoo_quotes(config, symbols=None, allow_cache_write=True):
    if symbols is None:
        symbols = [spec["symbol"] for _, spec in iter_market_specs()]
    else:
        symbols = list(symbols)

    LOGGER.info(
        "Yahoo live fetch strategy | primary=chart fallback=quote chart_workers=%d",
        config["yahoo_chart_max_workers"],
    )

    quote_map = fetch_yahoo_chart_quotes(symbols, config)
    missing_symbols = [symbol for symbol in symbols if symbol not in quote_map]

    if missing_symbols:
        LOGGER.warning(
            "Yahoo chart endpoint returned partial coverage, trying quote endpoint for missing symbols | missing=%d symbols=%s",
            len(missing_symbols),
            missing_symbols,
        )
        try:
            batch_map = fetch_yahoo_batch_quotes(missing_symbols, config)
            quote_map.update(batch_map)
        except HTTPError as exc:
            LOGGER.warning(
                "Yahoo quote endpoint failed after chart fallback | status=%s reason=%s",
                exc.code,
                exc.reason,
            )
        except Exception as exc:
            LOGGER.warning("Yahoo quote endpoint failed after chart fallback | error=%s", exc)

    if quote_map:
        if allow_cache_write:
            cached_payload = load_macro_market_cache().get("quote_map")
            cached_count = len(cached_payload) if isinstance(cached_payload, dict) else 0
            if len(quote_map) >= cached_count:
                update_macro_market_cache(quote_map=quote_map)
            else:
                LOGGER.warning(
                    "Skipping Yahoo cache update because live coverage is worse than cached coverage | live=%d cached=%d",
                    len(quote_map),
                    cached_count,
                )
        LOGGER.info(
            "Yahoo market quotes ready | requested=%d received=%d sources=%s",
            len(symbols),
            len(quote_map),
            sorted({item.get("source", "unknown") for item in quote_map.values()}),
        )
        return quote_map

    cached_quote_map, cached_at = get_cached_quote_map()
    if cached_quote_map:
        LOGGER.warning(
            "Using cached Yahoo market data after live fetch failure | count=%d cached_at=%s",
            len(cached_quote_map),
            cached_at,
        )
        return cached_quote_map

    raise RuntimeError("Yahoo market data was unavailable from live endpoints and cache")


def maybe_update_quote_cache(quote_map, label):
    if not quote_map:
        return

    cached_payload = load_macro_market_cache().get("quote_map")
    cached_count = len(cached_payload) if isinstance(cached_payload, dict) else 0
    if len(quote_map) >= cached_count:
        update_macro_market_cache(quote_map=quote_map)
        LOGGER.info(
            "Updated market quote cache | label=%s live=%d previous_cached=%d",
            label,
            len(quote_map),
            cached_count,
        )
    else:
        LOGGER.info(
            "Skipping market cache update because %s coverage is worse than cached coverage | live=%d cached=%d",
            label,
            len(quote_map),
            cached_count,
        )


def fill_market_quote_gaps_from_cache(quote_map, symbols, reason):
    cached_quote_map, cached_at = get_cached_quote_map()
    if not cached_quote_map:
        return dict(quote_map), []

    merged_map = dict(quote_map)
    filled_symbols = []
    for symbol in symbols:
        if symbol in merged_map:
            continue
        cached_quote = cached_quote_map.get(symbol)
        if not cached_quote:
            continue
        merged_map[symbol] = cached_quote
        filled_symbols.append(symbol)

    if filled_symbols:
        LOGGER.info(
            "Filled market quote gaps from cache | reason=%s filled=%d cached_at=%s symbols=%s",
            reason,
            len(filled_symbols),
            cached_at,
            filled_symbols,
        )

    return merged_map, filled_symbols


def summarize_quote_sources(quote_map):
    summary = {
        "stooq": 0,
        "frankfurter": 0,
        "yahoo": 0,
        "yfinance": 0,
        "cache": 0,
        "other": 0,
    }
    for item in quote_map.values():
        source = clean_text(item.get("source"))
        if source.startswith("cache:"):
            summary["cache"] += 1
        elif source.startswith("stooq"):
            summary["stooq"] += 1
        elif source.startswith("frankfurter"):
            summary["frankfurter"] += 1
        elif source.startswith("yahoo"):
            summary["yahoo"] += 1
        elif source.startswith("yfinance"):
            summary["yfinance"] += 1
        else:
            summary["other"] += 1
    return summary


def fetch_market_quotes(config):
    symbols = [spec["symbol"] for _, spec in iter_market_specs()]
    live_map = {}

    try:
        yfinance_map = fetch_yfinance_market_quotes(config, symbols=symbols)
        live_map.update(yfinance_map)
    except Exception as exc:
        LOGGER.warning("yfinance market quote fetch failed | error=%s", exc)

    missing_symbols = [symbol for symbol in symbols if symbol not in live_map]

    if missing_symbols:
        stooq_map = fetch_stooq_quotes(config)
        for symbol in missing_symbols:
            quote = stooq_map.get(symbol)
            if quote:
                live_map[symbol] = quote

    missing_symbols = [symbol for symbol in symbols if symbol not in live_map]

    if any(symbol in missing_symbols for symbol in ["EURUSD=X", "JPY=X", "CNY=X", "DXY"]):
        try:
            fx_map = fetch_frankfurter_fx_quotes(config)
            for symbol, quote in fx_map.items():
                live_map.setdefault(symbol, quote)
        except Exception as exc:
            LOGGER.warning("Frankfurter FX fetch failed | error=%s", exc)

    if "DXY" not in live_map:
        try:
            live_map["DXY"] = build_dxy_quote_from_stooq(config, live_map)
        except Exception as exc:
            LOGGER.warning("Stooq DXY derivation failed | error=%s", exc)

    missing_symbols = [symbol for symbol in symbols if symbol not in live_map]

    yahoo_map = {}
    if missing_symbols and config["yahoo_enabled"]:
        LOGGER.info(
            "Primary non-Yahoo sources returned partial coverage, trying Yahoo for missing symbols | missing=%d symbols=%s",
            len(missing_symbols),
            missing_symbols,
        )
        try:
            yahoo_map = fetch_yahoo_quotes(config, symbols=missing_symbols, allow_cache_write=False)
            live_map.update(yahoo_map)
        except Exception as exc:
            LOGGER.warning("Yahoo gap-fill failed after primary source partial coverage | error=%s", exc)
    elif missing_symbols:
        LOGGER.info(
            "Yahoo gap fill disabled | missing=%d symbols=%s",
            len(missing_symbols),
            missing_symbols,
        )

    if live_map:
        maybe_update_quote_cache(live_map, label="macro_market_live")

    combined_map, cache_filled_symbols = fill_market_quote_gaps_from_cache(
        live_map,
        symbols,
        reason="macro_market_missing_symbols",
    )
    missing_symbols = [symbol for symbol in symbols if symbol not in combined_map]

    if missing_symbols:
        LOGGER.info(
            "Market quotes remain partially unavailable after aggregation | missing=%d symbols=%s",
            len(missing_symbols),
            missing_symbols,
        )

    if combined_map:
        source_summary = summarize_quote_sources(combined_map)
        LOGGER.info(
            "Market quotes ready | requested=%d received=%d stooq=%d frankfurter=%d yahoo=%d yfinance=%d cache=%d other=%d missing=%d yahoo_enabled=%s cache_fills=%d",
            len(symbols),
            len(combined_map),
            source_summary["stooq"],
            source_summary["frankfurter"],
            source_summary["yahoo"],
            source_summary["yfinance"],
            source_summary["cache"],
            source_summary["other"],
            len(missing_symbols),
            config["yahoo_enabled"],
            len(cache_filled_symbols),
        )
        return combined_map

    cached_quote_map, cached_at = get_cached_quote_map()
    if cached_quote_map:
        LOGGER.warning(
            "Using cached market quotes after live source failure | count=%d cached_at=%s",
            len(cached_quote_map),
            cached_at,
        )
        return cached_quote_map

    raise RuntimeError("Market quotes were unavailable from live sources and cache")


def find_last_two_values(rows, column_name):
    values = []
    for row in rows:
        value = row.get(column_name)
        numeric_value = to_float(value)
        if numeric_value is None:
            continue
        values.append(
            {
                "date": row.get("DATE"),
                "value": numeric_value,
            }
        )

    if not values:
        return None, None
    if len(values) == 1:
        return values[-1], None
    return values[-1], values[-2]


def normalize_column_name(name):
    return re.sub(r"[^a-z0-9]+", "", str(name or "").lower())


def find_matching_column(fieldnames, candidates):
    normalized_candidates = {normalize_column_name(item) for item in candidates}
    for fieldname in fieldnames:
        if normalize_column_name(fieldname) in normalized_candidates:
            return fieldname
    return None


def parse_supported_date(value):
    raw_value = clean_text(value)
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw_value, fmt)
        except ValueError:
            continue
    return None


def build_rates_snapshot(latest_2y, previous_2y, latest_10y, previous_10y, source):
    change_2y_bps = None
    if previous_2y is not None:
        change_2y_bps = round((latest_2y["value"] - previous_2y["value"]) * 100, 2)

    change_10y_bps = None
    if previous_10y is not None:
        change_10y_bps = round((latest_10y["value"] - previous_10y["value"]) * 100, 2)

    curve_bps = round((latest_10y["value"] - latest_2y["value"]) * 100, 2)

    return {
        "as_of_date": latest_10y["date"] or latest_2y["date"],
        "series": {
            "us_2y": {
                "label": "US 2Y",
                "value": latest_2y["value"],
                "change_bps": change_2y_bps,
            },
            "us_10y": {
                "label": "US 10Y",
                "value": latest_10y["value"],
                "change_bps": change_10y_bps,
            },
        },
        "curve_10y_2y_bps": curve_bps,
        "source": source,
    }


def validate_rates_snapshot_freshness(snapshot, config, source_label):
    as_of_raw = snapshot.get("as_of_date") if isinstance(snapshot, dict) else None
    as_of_date = parse_supported_date(as_of_raw)
    if as_of_date is None:
        raise RuntimeError(f"{source_label} rates snapshot is missing a valid as_of_date")

    local_today = datetime.now(ZoneInfo(config["local_timezone"])).date()
    age_days = (local_today - as_of_date.date()).days
    if age_days < 0:
        raise RuntimeError(
            f"{source_label} rates snapshot is dated in the future | as_of_date={as_of_date.date().isoformat()}"
        )
    if age_days > config["macro_rates_max_age_days"]:
        raise RuntimeError(
            f"{source_label} rates snapshot is stale | as_of_date={as_of_date.date().isoformat()} age_days={age_days} max_age_days={config['macro_rates_max_age_days']}"
        )

    return snapshot


def parse_treasury_rows(rows, date_key, two_year_key, ten_year_key, source):
    parsed_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        parsed_date = parse_supported_date(row.get(date_key))
        if parsed_date is None:
            continue

        two_year = to_float(row.get(two_year_key))
        ten_year = to_float(row.get(ten_year_key))
        if two_year is None or ten_year is None:
            continue

        parsed_rows.append(
            {
                "date": parsed_date.date().isoformat(),
                "us_2y": two_year,
                "us_10y": ten_year,
            }
        )

    if not parsed_rows:
        raise RuntimeError(f"{source} payload is missing valid 2Y/10Y rows")

    parsed_rows.sort(key=lambda item: item["date"])
    latest_row = parsed_rows[-1]
    previous_row = parsed_rows[-2] if len(parsed_rows) > 1 else None

    latest_2y = {"date": latest_row["date"], "value": latest_row["us_2y"]}
    latest_10y = {"date": latest_row["date"], "value": latest_row["us_10y"]}
    previous_2y = (
        {"date": previous_row["date"], "value": previous_row["us_2y"]}
        if previous_row is not None
        else None
    )
    previous_10y = (
        {"date": previous_row["date"], "value": previous_row["us_10y"]}
        if previous_row is not None
        else None
    )

    return build_rates_snapshot(latest_2y, previous_2y, latest_10y, previous_10y, source=source)


def parse_fred_series_csv(csv_text, series_id):
    rows = list(csv.DictReader(io.StringIO(csv_text)))
    latest_value, previous_value = find_last_two_values(rows, series_id)
    if latest_value is None:
        raise RuntimeError(f"FRED series {series_id} payload is missing valid values")
    return latest_value, previous_value


def fetch_fred_series(series_id, config):
    url = FRED_SERIES_URL_TEMPLATE.format(series_id=series_id)
    csv_text = fetch_text_url_with_retries(
        url,
        config["macro_market_timeout_seconds"],
        config["fred_max_retries"],
        label=f"fred:{series_id}",
        failure_level="info",
    )
    return parse_fred_series_csv(csv_text, series_id)


def fetch_fred_treasury_snapshot(config):
    start_time = time.perf_counter()
    LOGGER.info(
        "Fetching treasury data from FRED | series=%s retries=%s",
        ["DGS2", "DGS10"],
        config["fred_max_retries"],
    )

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="fred-series") as executor:
        future_2y = executor.submit(fetch_fred_series, "DGS2", config)
        future_10y = executor.submit(fetch_fred_series, "DGS10", config)
        latest_2y, previous_2y = future_2y.result()
        latest_10y, previous_10y = future_10y.result()

    duration = time.perf_counter() - start_time
    LOGGER.info("Fetched treasury data from FRED | duration=%.2fs", duration)
    return build_rates_snapshot(latest_2y, previous_2y, latest_10y, previous_10y, source="fred")


def parse_treasury_csv_snapshot(csv_text):
    rows = list(csv.DictReader(io.StringIO(csv_text)))
    if not rows:
        raise RuntimeError("Treasury CSV payload is empty")

    fieldnames = rows[0].keys()
    date_key = find_matching_column(fieldnames, ["Date"])
    two_year_key = find_matching_column(fieldnames, ["2 Yr", "2 Year", "BC_2YEAR"])
    ten_year_key = find_matching_column(fieldnames, ["10 Yr", "10 Year", "BC_10YEAR"])

    if not date_key or not two_year_key or not ten_year_key:
        raise RuntimeError(
            f"Treasury CSV columns not found | fields={list(fieldnames)}"
        )

    return parse_treasury_rows(rows, date_key, two_year_key, ten_year_key, source="treasury_csv")


def strip_html_fragment(fragment):
    text = re.sub(r"(?is)<script.*?</script>", " ", fragment)
    text = re.sub(r"(?is)<style.*?</style>", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    return clean_text(unescape(text))


def extract_html_table_rows(html_text, heading_text):
    table_match = re.search(
        rf"{re.escape(heading_text)}.*?<table[^>]*>(.*?)</table>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not table_match:
        for candidate_table in re.findall(r"(?is)<table[^>]*>(.*?)</table>", html_text):
            candidate_text = strip_html_fragment(candidate_table)
            if "2 Yr" in candidate_text and "10 Yr" in candidate_text:
                table_match = re.match(r"(?is)(.*)", candidate_table)
                break

    if not table_match:
        raise RuntimeError("Treasury HTML table was not found")

    table_html = table_match.group(1)
    rows = []
    for row_html in re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", table_html):
        cells = [
            strip_html_fragment(cell_html)
            for cell_html in re.findall(r"(?is)<t[hd][^>]*>(.*?)</t[hd]>", row_html)
        ]
        cells = [cell for cell in cells if cell]
        if cells:
            rows.append(cells)

    if not rows:
        raise RuntimeError("Treasury HTML table rows were not found")

    return rows


def parse_treasury_html_snapshot(html_text):
    rows = extract_html_table_rows(html_text, TREASURY_TEXTVIEW_HEADING)
    header = None
    data_rows = []
    for index, row in enumerate(rows):
        normalized_row = {normalize_column_name(cell) for cell in row}
        if "date" in normalized_row and "2yr" in normalized_row and "10yr" in normalized_row:
            header = row
            data_rows = rows[index + 1 :]
            break

    if header is None:
        raise RuntimeError("Treasury HTML header row was not found")

    column_index = {normalize_column_name(name): index for index, name in enumerate(header)}

    required_columns = {
        "date": column_index.get(normalize_column_name("Date")),
        "2yr": column_index.get(normalize_column_name("2 Yr")),
        "10yr": column_index.get(normalize_column_name("10 Yr")),
    }
    if any(value is None for value in required_columns.values()):
        raise RuntimeError(f"Treasury HTML columns not found | header={header}")

    normalized_rows = []
    for row in data_rows:
        if len(row) <= max(required_columns.values()):
            continue
        normalized_rows.append(
            {
                "Date": row[required_columns["date"]],
                "2 Yr": row[required_columns["2yr"]],
                "10 Yr": row[required_columns["10yr"]],
            }
        )

    return parse_treasury_rows(
        normalized_rows,
        "Date",
        "2 Yr",
        "10 Yr",
        source="treasury_html",
    )


def fetch_treasury_html_snapshot(config):
    local_now = datetime.now(ZoneInfo(config["local_timezone"]))
    current_month = local_now.replace(day=1)
    previous_month = (current_month - timedelta(days=1)).replace(day=1)
    candidate_urls = [
        ("treasury:html:current_month", build_treasury_textview_month_url(current_month)),
        ("treasury:html:previous_month", build_treasury_textview_month_url(previous_month)),
        ("treasury:html", TREASURY_TEXTVIEW_URL),
    ]

    last_exc = None
    for label, url in candidate_urls:
        try:
            LOGGER.info("Fetching treasury backup data from Treasury TextView endpoint | label=%s", label)
            html_text = fetch_text_url_with_retries(
                url,
                config["macro_market_timeout_seconds"],
                config["macro_market_retries"],
                label=label,
                failure_level="info",
            )
            snapshot = parse_treasury_html_snapshot(html_text)
            LOGGER.info(
                "Treasury TextView backup source succeeded | label=%s as_of_date=%s",
                label,
                snapshot.get("as_of_date"),
            )
            return snapshot
        except Exception as exc:
            last_exc = exc
            LOGGER.info("Treasury TextView backup source failed | label=%s error=%s", label, exc)

    raise last_exc


def fetch_treasury_backup_snapshot(config):
    try:
        LOGGER.info("Fetching treasury backup data from Treasury CSV endpoint")
        csv_text = fetch_text_url_with_retries(
            TREASURY_CSV_URL,
            config["macro_market_timeout_seconds"],
            config["macro_market_retries"],
            label="treasury:csv",
            failure_level="info",
        )
        snapshot = parse_treasury_csv_snapshot(csv_text)
        LOGGER.info("Treasury CSV backup source succeeded")
        return snapshot
    except Exception as csv_exc:
        LOGGER.warning("Treasury CSV backup source failed | error=%s", csv_exc)

    return fetch_treasury_html_snapshot(config)


def fetch_rates_snapshot(config):
    try:
        snapshot = validate_rates_snapshot_freshness(
            fetch_fred_treasury_snapshot(config),
            config,
            source_label="FRED",
        )
        update_macro_market_cache(rates_snapshot=snapshot)
        return snapshot
    except Exception as fred_exc:
        LOGGER.info("FRED treasury source failed, trying Treasury backup | error=%s", fred_exc)

    try:
        snapshot = validate_rates_snapshot_freshness(
            fetch_treasury_backup_snapshot(config),
            config,
            source_label="Treasury backup",
        )
        update_macro_market_cache(rates_snapshot=snapshot)
        return snapshot
    except Exception as treasury_exc:
        LOGGER.warning("Treasury backup source failed, trying cache | error=%s", treasury_exc)

    cached_snapshot, cached_at = get_cached_rates_snapshot()
    if cached_snapshot:
        try:
            validated_snapshot = validate_rates_snapshot_freshness(
                cached_snapshot,
                config,
                source_label=f"cached rates snapshot ({cached_snapshot.get('source')})",
            )
            LOGGER.warning(
                "Using cached rates snapshot after live fetch failure | cached_at=%s source=%s",
                cached_at,
                cached_snapshot.get("source"),
            )
            return validated_snapshot
        except Exception as cache_exc:
            LOGGER.warning(
                "Cached rates snapshot rejected | cached_at=%s source=%s error=%s",
                cached_at,
                cached_snapshot.get("source"),
                cache_exc,
            )

    raise RuntimeError("Rates data was unavailable from FRED, Treasury backup, and cache")


def build_market_items(group_key, quote_map):
    items = []
    for spec in MARKET_GROUPS[group_key]:
        quote = quote_map.get(spec["symbol"], {})
        items.append(
            {
                "key": spec["key"],
                "label": quote.get("name") or spec["label"],
                "symbol": spec["symbol"],
                "price": quote.get("price"),
                "change": quote.get("change"),
                "change_pct": quote.get("change_pct"),
                "currency": quote.get("currency"),
                "unit": quote.get("unit"),
                "market_time_epoch": quote.get("market_time_epoch"),
                "available": spec["symbol"] in quote_map,
                "source": quote.get("source"),
                "stale": bool(quote.get("stale")),
                "cached_at": quote.get("cached_at"),
            }
        )
    return items


def fetch_market_snapshot(config):
    start_time = time.perf_counter()
    errors = []

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="macro-market") as executor:
        quotes_future = executor.submit(fetch_market_quotes, config)
        rates_future = executor.submit(fetch_rates_snapshot, config)

        quote_map = {}
        treasury_snapshot = {}

        try:
            quote_map = quotes_future.result()
        except Exception as exc:
            LOGGER.warning("Market quote fetch failed | error=%s", exc)
            errors.append({"source": "market_quotes", "error": str(exc)})

        try:
            treasury_snapshot = rates_future.result()
        except Exception as exc:
            LOGGER.warning("Rates fetch failed | error=%s", exc)
            errors.append({"source": "rates", "error": str(exc)})

    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "commodities": build_market_items("commodities", quote_map),
        "rates": treasury_snapshot,
        "equities": build_market_items("equities", quote_map),
        "fx": build_market_items("fx", quote_map),
        "errors": errors,
    }

    duration = time.perf_counter() - start_time
    LOGGER.info(
        "Macro market snapshot ready | commodities=%d equities=%d fx=%d rates_available=%s errors=%d duration=%.2fs",
        len(snapshot["commodities"]),
        len(snapshot["equities"]),
        len(snapshot["fx"]),
        bool(snapshot["rates"]),
        len(errors),
        duration,
    )
    write_json_artifact("macro_market_snapshot.json", snapshot)
    return snapshot
