from concurrent.futures import ThreadPoolExecutor

from digest_email import send_email
from digest_runtime import LOGGER, get_run_dir, setup_logging, write_json_artifact, write_text_artifact
from macro_config import (
    get_macro_runtime_config,
    get_macro_smtp_config,
    log_macro_runtime_config,
    validate_macro_runtime_config,
)
from macro_email import build_macro_email
from macro_llm import synthesize_macro_report
from macro_sources import fetch_macro_news, fetch_market_snapshot


def build_macro_summary(news_payload, market_snapshot, report):
    return {
        "headline": report["headline"],
        "regime": report["regime"],
        "top_signal_count": len(report.get("top_signals", [])),
        "watchlist_count": len(report.get("tomorrow_watchlist", [])),
        "news_total_headlines": news_payload.get("total_headlines", 0),
        "news_bucket_counts": {
            bucket_key: len(bucket.get("headlines", []))
            for bucket_key, bucket in news_payload.get("buckets", {}).items()
        },
        "market_errors": market_snapshot.get("errors", []),
        "run_dir": str(get_run_dir()) if get_run_dir() is not None else None,
    }


def ensure_macro_inputs(news_payload, market_snapshot):
    has_news = news_payload.get("total_headlines", 0) > 0
    has_market = bool(market_snapshot.get("rates", {}).get("series"))
    if not has_market:
        for key in ["commodities", "equities", "fx"]:
            if any(item.get("price") is not None for item in market_snapshot.get(key, [])):
                has_market = True
                break
    if not has_news and not has_market:
        raise RuntimeError("No macro inputs were collected from either news or market sources")


def main():
    setup_logging()

    config = get_macro_runtime_config()
    smtp_config = get_macro_smtp_config()
    validate_macro_runtime_config(config, smtp_config)
    log_macro_runtime_config(config, smtp_config)

    LOGGER.info("Macro pipeline started")

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="macro-root") as executor:
        news_future = executor.submit(fetch_macro_news, config)
        market_future = executor.submit(fetch_market_snapshot, config)
        news_payload = news_future.result()
        market_snapshot = market_future.result()

    ensure_macro_inputs(news_payload, market_snapshot)

    report = synthesize_macro_report(news_payload, market_snapshot, config)
    write_json_artifact("macro_report.json", report)

    subject, html = build_macro_email(report, market_snapshot, config)
    preview_path = write_text_artifact("macro_email_preview.html", html)
    LOGGER.info("Macro email rendered | preview=%s subject=%s", preview_path, subject)

    summary = build_macro_summary(news_payload, market_snapshot, report)
    write_json_artifact("macro_pipeline_summary.json", summary)

    if config["dry_run"]:
        LOGGER.info("Dry run enabled, skipping macro email send")
        LOGGER.info("Macro pipeline completed | run_dir=%s", get_run_dir())
        return

    send_email(html, smtp_config, subject=subject)
    LOGGER.info("Macro pipeline completed | run_dir=%s", get_run_dir())
