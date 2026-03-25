import json
import time

from digest_llm import build_extra_body, parse_json_response
from digest_runtime import LOGGER, get_client, write_text_artifact
from macro_prompts import MACRO_REPORT_PROMPT, MACRO_SYSTEM_PROMPT

MACRO_MODULE_KEYS = [
    "macro_geopolitics",
    "commodities",
    "rates",
    "equities",
    "fx",
]

MACRO_MODULE_LABELS = {
    "macro_geopolitics": "Macro/Geopolitics",
    "commodities": "Commodities",
    "rates": "Rates",
    "equities": "Equities",
    "fx": "FX",
}

MACRO_REGIME_ALIASES = {
    "risk-on": "Risk-on",
    "risk on": "Risk-on",
    "risk-off": "Risk-off",
    "risk off": "Risk-off",
    "mixed": "Mixed",
    "neutral": "Mixed",
}


def compact_macro_inputs(news_payload, market_snapshot):
    compact_news = {
        "lookback_hours": news_payload.get("lookback_hours"),
        "buckets": {},
    }
    for bucket_key, bucket in news_payload.get("buckets", {}).items():
        compact_news["buckets"][bucket_key] = {
            "label": bucket.get("label"),
            "headlines": [
                {
                    "title": item.get("title"),
                    "source": item.get("source"),
                    "published_at": item.get("published_at"),
                }
                for item in bucket.get("headlines", [])
            ],
            "errors": bucket.get("errors", []),
        }

    compact_market = {
        "commodities": market_snapshot.get("commodities", []),
        "rates": market_snapshot.get("rates", {}),
        "equities": market_snapshot.get("equities", []),
        "fx": market_snapshot.get("fx", []),
        "errors": market_snapshot.get("errors", []),
    }
    return {
        "news": compact_news,
        "market_snapshot": compact_market,
    }


def normalize_regime(value):
    normalized = str(value or "").strip().lower()
    if not normalized:
        return "Mixed"
    return MACRO_REGIME_ALIASES.get(normalized, "Mixed")


def ensure_string(value, default):
    text = str(value or "").strip()
    if text:
        return text
    default_text = str(default or "").strip()
    return default_text


def validate_top_signals(payload):
    if not isinstance(payload, list):
        return []

    validated = []
    for item in payload[:5]:
        if not isinstance(item, dict):
            continue
        module = ensure_string(item.get("module"), "Macro/Geopolitics")
        signal = ensure_string(item.get("signal"), "No clear signal extracted.")
        signal_zh = ensure_string(item.get("signal_zh"), signal)
        why_it_matters = ensure_string(
            item.get("why_it_matters"),
            "Why it matters was not provided.",
        )
        why_it_matters_zh = ensure_string(
            item.get("why_it_matters_zh"),
            why_it_matters,
        )
        market_impact = ensure_string(
            item.get("market_impact"),
            "Cross-asset impact was not provided.",
        )
        market_impact_zh = ensure_string(
            item.get("market_impact_zh"),
            market_impact,
        )
        validated.append(
            {
                "module": module,
                "signal": signal,
                "signal_zh": signal_zh,
                "why_it_matters": why_it_matters,
                "why_it_matters_zh": why_it_matters_zh,
                "market_impact": market_impact,
                "market_impact_zh": market_impact_zh,
            }
        )
    return validated


def validate_modules(payload):
    modules = payload if isinstance(payload, dict) else {}
    validated = {}
    for module_key in MACRO_MODULE_KEYS:
        module_payload = modules.get(module_key, {})
        if not isinstance(module_payload, dict):
            module_payload = {}
        summary = ensure_string(module_payload.get("summary"), "No strong signal detected.")
        summary_zh = ensure_string(
            module_payload.get("summary_zh"),
            summary,
        )
        watch = ensure_string(module_payload.get("watch"), "No specific watch item provided.")
        watch_zh = ensure_string(
            module_payload.get("watch_zh"),
            watch,
        )
        validated[module_key] = {
            "label": MACRO_MODULE_LABELS[module_key],
            "summary": summary,
            "summary_zh": summary_zh,
            "watch": watch,
            "watch_zh": watch_zh,
        }
    return validated


def validate_watchlist(payload):
    if not isinstance(payload, list):
        return []

    items = []
    for item in payload[:5]:
        text = ensure_string(item, "")
        if text:
            items.append(text)
    return items


def validate_watchlist_zh(payload, fallback):
    items = validate_watchlist(payload)
    if items:
        return items
    return list(fallback)


def validate_macro_report(payload):
    if not isinstance(payload, dict):
        raise ValueError("macro report payload must be a JSON object")

    headline = ensure_string(payload.get("headline"), "Macro daily brief")
    cross_asset_take = ensure_string(
        payload.get("cross_asset_take"),
        "No cross-asset view was produced.",
    )
    tomorrow_watchlist = validate_watchlist(payload.get("tomorrow_watchlist"))
    return {
        "headline": headline,
        "headline_zh": ensure_string(payload.get("headline_zh"), headline),
        "regime": normalize_regime(payload.get("regime")),
        "cross_asset_take": cross_asset_take,
        "cross_asset_take_zh": ensure_string(payload.get("cross_asset_take_zh"), cross_asset_take),
        "top_signals": validate_top_signals(payload.get("top_signals")),
        "modules": validate_modules(payload.get("modules")),
        "tomorrow_watchlist": tomorrow_watchlist,
        "tomorrow_watchlist_zh": validate_watchlist_zh(
            payload.get("tomorrow_watchlist_zh"),
            tomorrow_watchlist,
        ),
    }


def normalize_json_text_candidate(text):
    candidate = str(text or "").strip()
    if not candidate:
        return candidate

    if candidate.startswith("```"):
        lines = candidate.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        candidate = "\n".join(lines).strip()

    start = candidate.find("{")
    end = candidate.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = candidate[start : end + 1]

    candidate = candidate.replace("\u201c", '"').replace("\u201d", '"')
    candidate = candidate.replace("\u2018", "'").replace("\u2019", "'")
    candidate = candidate.replace("\u00a0", " ")
    return candidate


def remove_trailing_commas(candidate):
    output = []
    in_string = False
    escape_next = False
    index = 0
    while index < len(candidate):
        char = candidate[index]
        if in_string:
            output.append(char)
            if escape_next:
                escape_next = False
            elif char == "\\":
                escape_next = True
            elif char == '"':
                in_string = False
            index += 1
            continue

        if char == '"':
            in_string = True
            output.append(char)
            index += 1
            continue

        if char == ",":
            lookahead_index = index + 1
            while lookahead_index < len(candidate) and candidate[lookahead_index].isspace():
                lookahead_index += 1
            if lookahead_index < len(candidate) and candidate[lookahead_index] in "}]":
                index += 1
                continue

        output.append(char)
        index += 1

    return "".join(output)


def attempt_local_json_repair(text):
    candidate = normalize_json_text_candidate(text)
    if not candidate:
        return candidate
    candidate = remove_trailing_commas(candidate)
    return candidate


def repair_macro_json_with_llm(content, config):
    repair_prompt = (
        "Repair the following malformed JSON so it becomes valid JSON.\n"
        "Preserve the original meaning and field names.\n"
        "Return JSON only with no markdown fences and no extra commentary.\n\n"
        f"Malformed JSON:\n{content}"
    )
    extra_body = build_extra_body(config)
    LOGGER.info(
        "Macro LLM JSON repair started | model=%s timeout=%ss",
        config["llm_model"],
        config["llm_timeout_seconds"],
    )
    start_time = time.perf_counter()
    response = get_client().chat.completions.create(
        model=config["llm_model"],
        messages=[
            {"role": "system", "content": "You repair malformed JSON and return strict valid JSON only."},
            {"role": "user", "content": repair_prompt},
        ],
        temperature=0,
        timeout=config["llm_timeout_seconds"],
        extra_body=extra_body,
    )
    content = response.choices[0].message.content or ""
    duration = time.perf_counter() - start_time
    LOGGER.info(
        "Macro LLM JSON repair finished | duration=%.2fs response_chars=%d response_id=%s",
        duration,
        len(content),
        getattr(response, "id", "n/a"),
    )
    if config["log_raw_llm"]:
        write_text_artifact("llm/macro-report-repair-response.txt", content)
    return content


def call_macro_synthesis_model(prompt, config):
    start_time = time.perf_counter()
    extra_body = build_extra_body(config)
    LOGGER.info(
        "Macro LLM synthesis started | model=%s timeout=%ss thinking=%s thinking_budget=%s",
        config["llm_model"],
        config["llm_timeout_seconds"],
        config["llm_enable_thinking"],
        config["llm_thinking_budget"],
    )

    response = get_client().chat.completions.create(
        model=config["llm_model"],
        messages=[
            {"role": "system", "content": MACRO_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        timeout=config["llm_timeout_seconds"],
        extra_body=extra_body,
    )

    duration = time.perf_counter() - start_time
    content = response.choices[0].message.content or ""
    usage = getattr(response, "usage", None)
    if usage is not None:
        LOGGER.info(
            "Macro LLM synthesis finished | duration=%.2fs total_tokens=%s prompt_tokens=%s completion_tokens=%s response_chars=%d response_id=%s",
            duration,
            getattr(usage, "total_tokens", "n/a"),
            getattr(usage, "prompt_tokens", "n/a"),
            getattr(usage, "completion_tokens", "n/a"),
            len(content),
            getattr(response, "id", "n/a"),
        )
    else:
        LOGGER.info(
            "Macro LLM synthesis finished | duration=%.2fs response_chars=%d response_id=%s",
            duration,
            len(content),
            getattr(response, "id", "n/a"),
        )
    return content


def parse_macro_report_with_repairs(content, config):
    last_exc = None
    candidates = [
        ("raw", content),
        ("local_repair", attempt_local_json_repair(content)),
    ]

    for label, candidate in candidates:
        if not candidate:
            continue
        try:
            payload = parse_json_response(candidate)
            if label != "raw":
                LOGGER.info("Macro report JSON parsed after repair | strategy=%s", label)
            return validate_macro_report(payload)
        except Exception as exc:
            last_exc = exc

    repaired_content = repair_macro_json_with_llm(content, config)
    try:
        payload = parse_json_response(repaired_content)
        LOGGER.info("Macro report JSON parsed after LLM repair")
        return validate_macro_report(payload)
    except Exception as exc:
        last_exc = exc
        repaired_candidate = attempt_local_json_repair(repaired_content)
        if repaired_candidate:
            payload = parse_json_response(repaired_candidate)
            LOGGER.info("Macro report JSON parsed after LLM repair + local cleanup")
            return validate_macro_report(payload)

    raise last_exc


def build_macro_prompt(news_payload, market_snapshot):
    prompt_payload = compact_macro_inputs(news_payload, market_snapshot)
    return (
        f"{MACRO_REPORT_PROMPT}\n\n"
        f"Input JSON:\n{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}"
    )


def synthesize_macro_report(news_payload, market_snapshot, config):
    prompt = build_macro_prompt(news_payload, market_snapshot)
    if config["log_raw_llm"]:
        write_text_artifact("llm/macro-report-prompt.txt", prompt)
    content = call_macro_synthesis_model(prompt, config)
    if config["log_raw_llm"]:
        write_text_artifact("llm/macro-report-response.txt", content)

    try:
        return parse_macro_report_with_repairs(content, config)
    except Exception as exc:
        LOGGER.warning("Failed to parse macro report response on first attempt | error=%s", exc)
        write_text_artifact("llm/macro-report-parse-error.txt", content)

    retry_prompt = (
        f"{prompt}\n\n"
        "Return strict valid JSON only. Do not include markdown fences, comments, or prose."
    )
    retry_content = call_macro_synthesis_model(retry_prompt, config)
    if config["log_raw_llm"]:
        write_text_artifact("llm/macro-report-retry-response.txt", retry_content)

    try:
        return parse_macro_report_with_repairs(retry_content, config)
    except Exception as exc:
        LOGGER.warning("Failed to parse macro report response after retry | error=%s", exc)
        write_text_artifact("llm/macro-report-retry-parse-error.txt", retry_content)
        raise
