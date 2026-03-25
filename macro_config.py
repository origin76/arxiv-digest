import os
from zoneinfo import ZoneInfo

from digest_config import bool_env, get_smtp_config, int_env
from digest_runtime import DEFAULT_LLM_MODEL, LOGGER, mask_value, write_json_artifact

DEFAULT_MACRO_NEWS_LOOKBACK_HOURS = 36
DEFAULT_MACRO_MAX_HEADLINES_PER_BUCKET = 8
DEFAULT_MACRO_NEWS_MAX_WORKERS = 8
DEFAULT_MACRO_NEWS_TIMEOUT_SECONDS = 15
DEFAULT_MACRO_NEWS_RETRIES = 2
DEFAULT_MACRO_MARKET_TIMEOUT_SECONDS = 15
DEFAULT_MACRO_MARKET_RETRIES = 3
DEFAULT_MACRO_RATES_MAX_AGE_DAYS = 10
DEFAULT_FRED_MAX_RETRIES = 1
DEFAULT_STOOQ_MAX_RETRIES = 2
DEFAULT_STOOQ_MAX_WORKERS = 2
DEFAULT_YAHOO_ENABLED = False
DEFAULT_YAHOO_MAX_RETRIES = 3
DEFAULT_YAHOO_CHART_MAX_WORKERS = 1


def get_macro_runtime_config():
    thinking_budget_raw = os.getenv("LLM_THINKING_BUDGET", "").strip()

    return {
        "dry_run": bool_env("DRY_RUN", False),
        "log_raw_llm": bool_env("LOG_RAW_LLM", False),
        "llm_model": os.getenv("LLM_MODEL", DEFAULT_LLM_MODEL).strip() or DEFAULT_LLM_MODEL,
        "llm_timeout_seconds": int_env("LLM_TIMEOUT_SECONDS", 90),
        "llm_enable_thinking": bool_env("LLM_ENABLE_THINKING", False),
        "llm_thinking_budget": int(thinking_budget_raw) if thinking_budget_raw else None,
        "local_timezone": os.getenv("LOCAL_TIMEZONE", "Asia/Shanghai").strip() or "Asia/Shanghai",
        "macro_news_lookback_hours": int_env(
            "MACRO_NEWS_LOOKBACK_HOURS",
            DEFAULT_MACRO_NEWS_LOOKBACK_HOURS,
        ),
        "macro_max_headlines_per_bucket": int_env(
            "MACRO_MAX_HEADLINES_PER_BUCKET",
            DEFAULT_MACRO_MAX_HEADLINES_PER_BUCKET,
        ),
        "macro_news_max_workers": int_env(
            "MACRO_NEWS_MAX_WORKERS",
            DEFAULT_MACRO_NEWS_MAX_WORKERS,
        ),
        "macro_news_timeout_seconds": int_env(
            "MACRO_NEWS_TIMEOUT_SECONDS",
            DEFAULT_MACRO_NEWS_TIMEOUT_SECONDS,
        ),
        "macro_news_retries": int_env(
            "MACRO_NEWS_RETRIES",
            DEFAULT_MACRO_NEWS_RETRIES,
        ),
        "macro_market_timeout_seconds": int_env(
            "MACRO_MARKET_TIMEOUT_SECONDS",
            DEFAULT_MACRO_MARKET_TIMEOUT_SECONDS,
        ),
        "macro_market_retries": int_env(
            "MACRO_MARKET_RETRIES",
            DEFAULT_MACRO_MARKET_RETRIES,
        ),
        "macro_rates_max_age_days": int_env(
            "MACRO_RATES_MAX_AGE_DAYS",
            DEFAULT_MACRO_RATES_MAX_AGE_DAYS,
        ),
        "fred_max_retries": int_env(
            "FRED_MAX_RETRIES",
            DEFAULT_FRED_MAX_RETRIES,
        ),
        "stooq_max_retries": int_env(
            "STOOQ_MAX_RETRIES",
            DEFAULT_STOOQ_MAX_RETRIES,
        ),
        "stooq_max_workers": int_env(
            "STOOQ_MAX_WORKERS",
            DEFAULT_STOOQ_MAX_WORKERS,
        ),
        "yahoo_enabled": bool_env("YAHOO_ENABLED", DEFAULT_YAHOO_ENABLED),
        "yahoo_max_retries": int_env(
            "YAHOO_MAX_RETRIES",
            DEFAULT_YAHOO_MAX_RETRIES,
        ),
        "yahoo_chart_max_workers": int_env(
            "YAHOO_CHART_MAX_WORKERS",
            DEFAULT_YAHOO_CHART_MAX_WORKERS,
        ),
    }


def get_macro_smtp_config():
    smtp_config = get_smtp_config()
    macro_email_to = os.getenv("MACRO_EMAIL_TO", "").strip()
    if macro_email_to:
        smtp_config = dict(smtp_config)
        smtp_config["to"] = macro_email_to
    return smtp_config


def validate_macro_runtime_config(config, smtp_config):
    missing = []

    if not os.getenv("DASHSCOPE_API_KEY"):
        missing.append("DASHSCOPE_API_KEY")

    if not config["dry_run"]:
        for name, value in {
            "EMAIL_USER": smtp_config["user"],
            "EMAIL_PASS": smtp_config["password"],
            "MACRO_EMAIL_TO/EMAIL_TO": smtp_config["to"],
        }.items():
            if not value:
                missing.append(name)

    if config["llm_timeout_seconds"] <= 0:
        raise RuntimeError("LLM_TIMEOUT_SECONDS must be greater than 0")

    if config["macro_news_lookback_hours"] <= 0:
        raise RuntimeError("MACRO_NEWS_LOOKBACK_HOURS must be greater than 0")

    if config["macro_max_headlines_per_bucket"] <= 0:
        raise RuntimeError("MACRO_MAX_HEADLINES_PER_BUCKET must be greater than 0")

    if config["macro_news_max_workers"] <= 0:
        raise RuntimeError("MACRO_NEWS_MAX_WORKERS must be greater than 0")

    if config["macro_news_timeout_seconds"] <= 0:
        raise RuntimeError("MACRO_NEWS_TIMEOUT_SECONDS must be greater than 0")

    if config["macro_news_retries"] <= 0:
        raise RuntimeError("MACRO_NEWS_RETRIES must be greater than 0")

    if config["macro_market_timeout_seconds"] <= 0:
        raise RuntimeError("MACRO_MARKET_TIMEOUT_SECONDS must be greater than 0")

    if config["macro_market_retries"] <= 0:
        raise RuntimeError("MACRO_MARKET_RETRIES must be greater than 0")

    if config["macro_rates_max_age_days"] <= 0:
        raise RuntimeError("MACRO_RATES_MAX_AGE_DAYS must be greater than 0")

    if config["fred_max_retries"] <= 0:
        raise RuntimeError("FRED_MAX_RETRIES must be greater than 0")

    if config["stooq_max_retries"] <= 0:
        raise RuntimeError("STOOQ_MAX_RETRIES must be greater than 0")

    if config["stooq_max_workers"] <= 0:
        raise RuntimeError("STOOQ_MAX_WORKERS must be greater than 0")

    if config["yahoo_max_retries"] <= 0:
        raise RuntimeError("YAHOO_MAX_RETRIES must be greater than 0")

    if config["yahoo_chart_max_workers"] <= 0:
        raise RuntimeError("YAHOO_CHART_MAX_WORKERS must be greater than 0")

    try:
        ZoneInfo(config["local_timezone"])
    except Exception as exc:
        raise RuntimeError(f"LOCAL_TIMEZONE is invalid: {config['local_timezone']!r}") from exc

    if smtp_config["use_ssl"] and smtp_config["use_starttls"]:
        raise RuntimeError("EMAIL_USE_SSL and EMAIL_USE_STARTTLS cannot both be true")

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


def log_macro_runtime_config(config, smtp_config):
    safe_config = {
        "dry_run": config["dry_run"],
        "log_raw_llm": config["log_raw_llm"],
        "llm_model": config["llm_model"],
        "llm_timeout_seconds": config["llm_timeout_seconds"],
        "llm_enable_thinking": config["llm_enable_thinking"],
        "llm_thinking_budget": config["llm_thinking_budget"],
        "local_timezone": config["local_timezone"],
        "macro_news_lookback_hours": config["macro_news_lookback_hours"],
        "macro_max_headlines_per_bucket": config["macro_max_headlines_per_bucket"],
        "macro_news_max_workers": config["macro_news_max_workers"],
        "macro_news_timeout_seconds": config["macro_news_timeout_seconds"],
        "macro_news_retries": config["macro_news_retries"],
        "macro_market_timeout_seconds": config["macro_market_timeout_seconds"],
        "macro_market_retries": config["macro_market_retries"],
        "macro_rates_max_age_days": config["macro_rates_max_age_days"],
        "fred_max_retries": config["fred_max_retries"],
        "stooq_max_retries": config["stooq_max_retries"],
        "stooq_max_workers": config["stooq_max_workers"],
        "yahoo_enabled": config["yahoo_enabled"],
        "yahoo_max_retries": config["yahoo_max_retries"],
        "yahoo_chart_max_workers": config["yahoo_chart_max_workers"],
        "smtp_host": smtp_config["host"],
        "smtp_port": smtp_config["port"],
        "smtp_use_ssl": smtp_config["use_ssl"],
        "smtp_use_starttls": smtp_config["use_starttls"],
        "email_user": smtp_config["user"],
        "email_to": smtp_config["to"],
        "dashscope_api_key_masked": mask_value(os.getenv("DASHSCOPE_API_KEY", "")),
        "email_pass_masked": mask_value(smtp_config["password"]),
    }
    LOGGER.info(
        "Macro runtime configuration loaded | dry_run=%s model=%s llm_timeout=%ss lookback=%sh headlines_per_bucket=%s news_workers=%s news_timeout=%ss news_retries=%s market_timeout=%ss market_retries=%s rates_max_age_days=%s fred_retries=%s stooq_retries=%s stooq_workers=%s yahoo_enabled=%s yahoo_retries=%s yahoo_chart_workers=%s smtp_host=%s smtp_port=%s smtp_ssl=%s smtp_starttls=%s log_raw_llm=%s",
        safe_config["dry_run"],
        safe_config["llm_model"],
        safe_config["llm_timeout_seconds"],
        safe_config["macro_news_lookback_hours"],
        safe_config["macro_max_headlines_per_bucket"],
        safe_config["macro_news_max_workers"],
        safe_config["macro_news_timeout_seconds"],
        safe_config["macro_news_retries"],
        safe_config["macro_market_timeout_seconds"],
        safe_config["macro_market_retries"],
        safe_config["macro_rates_max_age_days"],
        safe_config["fred_max_retries"],
        safe_config["stooq_max_retries"],
        safe_config["stooq_max_workers"],
        safe_config["yahoo_enabled"],
        safe_config["yahoo_max_retries"],
        safe_config["yahoo_chart_max_workers"],
        safe_config["smtp_host"],
        safe_config["smtp_port"],
        safe_config["smtp_use_ssl"],
        safe_config["smtp_use_starttls"],
        safe_config["log_raw_llm"],
    )
    write_json_artifact("macro_config.json", safe_config)
