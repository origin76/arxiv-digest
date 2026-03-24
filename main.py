import json
import logging
import os
import re
import smtplib
import ssl
import time
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from html import escape
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import feedparser
from openai import OpenAI

from prompts import ASSESS_PROMPT, SUMMARY_PROMPT, SYSTEM_PROMPT

DASHSCOPE_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_LLM_MODEL = "qwen3.5-plus"
SEEN_IDS_PATH = Path("seen_ids.json")
DEFAULT_LOG_DIR = "logs"
DEFAULT_ARXIV_PAGE_SIZE = 100
DEFAULT_MAX_SELECTED_PAPERS = 10
ARXIV_SEARCH_QUERY = "(cat:cs.OS OR cat:cs.PL OR cat:cs.LG OR cat:cs.DC OR cat:cs.AR)"
HARD_EXCLUDE_PATTERNS = [
    (
        "edge-power-hardware",
        re.compile(
            r"(dvfs|dynamic voltage frequency scaling|power-sensitive edge|power sensitive edge|"
            r"energy[- ]efficient edge|edge device|edge devices|embedded device|embedded devices)"
            r".{0,120}"
            r"(dnn|neural network|deep neural network|hardware|accelerator|power|energy)",
            re.IGNORECASE | re.DOTALL,
        ),
        "Edge/embedded hardware power optimization is outside this digest focus.",
    ),
    (
        "edge-hardware-deployment",
        re.compile(
            r"(deploy|deployment|inference|runtime).{0,120}"
            r"(edge|embedded).{0,120}"
            r"(hardware|accelerator|fpga|asic|dvfs|power|energy)",
            re.IGNORECASE | re.DOTALL,
        ),
        "Hardware-oriented edge deployment work is excluded from this digest.",
    ),
]

LOGGER = logging.getLogger("arxiv_digest")
RUN_DIR = None
CLIENT = None


def bool_env(name, default=False):
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def int_env(name, default):
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return default

    try:
        return int(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw_value!r}") from exc


def slugify(value):
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
    return slug or "item"


def setup_logging():
    global RUN_DIR

    log_dir = Path(os.getenv("LOG_DIR", DEFAULT_LOG_DIR))
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    RUN_DIR = log_dir / run_id
    RUN_DIR.mkdir(parents=True, exist_ok=True)

    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    LOGGER.handlers.clear()
    LOGGER.setLevel(log_level)
    LOGGER.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(RUN_DIR / "run.log", encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    LOGGER.addHandler(stream_handler)
    LOGGER.addHandler(file_handler)

    LOGGER.info("Logging initialized | run_dir=%s", RUN_DIR)


def write_text_artifact(name, content):
    if RUN_DIR is None:
        return None

    path = RUN_DIR / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def write_json_artifact(name, payload):
    if RUN_DIR is None:
        return None

    path = RUN_DIR / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def build_arxiv_url(max_results):
    params = {
        "search_query": ARXIV_SEARCH_QUERY,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "max_results": max_results,
    }
    return f"http://export.arxiv.org/api/query?{urlencode(params)}"


def get_runtime_config():
    thinking_budget_raw = os.getenv("LLM_THINKING_BUDGET", "").strip()

    return {
        "dry_run": bool_env("DRY_RUN", False),
        "log_raw_llm": bool_env("LOG_RAW_LLM", False),
        "llm_model": os.getenv("LLM_MODEL", DEFAULT_LLM_MODEL).strip() or DEFAULT_LLM_MODEL,
        "llm_timeout_seconds": int_env("LLM_TIMEOUT_SECONDS", 90),
        "llm_enable_thinking": bool_env("LLM_ENABLE_THINKING", False),
        "llm_thinking_budget": int(thinking_budget_raw) if thinking_budget_raw else None,
        "max_selected_papers": int_env(
            "MAX_SELECTED_PAPERS",
            DEFAULT_MAX_SELECTED_PAPERS,
        ),
        "arxiv_page_size": int_env(
            "ARXIV_PAGE_SIZE",
            DEFAULT_ARXIV_PAGE_SIZE,
        ),
        "target_days_ago": int_env("TARGET_DAYS_AGO", 1),
        "local_timezone": os.getenv("LOCAL_TIMEZONE", "Asia/Shanghai").strip() or "Asia/Shanghai",
    }


def get_smtp_config():
    return {
        "host": os.getenv("EMAIL_SMTP_HOST", "mail.tiaozhan.com"),
        "port": int_env("EMAIL_SMTP_PORT", 465),
        "use_ssl": bool_env("EMAIL_USE_SSL", True),
        "use_starttls": bool_env("EMAIL_USE_STARTTLS", False),
        "user": os.getenv("EMAIL_USER", "").strip(),
        "password": os.getenv("EMAIL_PASS", ""),
        "to": os.getenv("EMAIL_TO", "").strip(),
    }


def mask_value(value):
    if not value:
        return "<unset>"
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-4:]}"


def validate_runtime_config(config, smtp_config):
    missing = []

    if not os.getenv("DASHSCOPE_API_KEY"):
        missing.append("DASHSCOPE_API_KEY")

    if not config["dry_run"]:
        for name, value in {
            "EMAIL_USER": smtp_config["user"],
            "EMAIL_PASS": smtp_config["password"],
            "EMAIL_TO": smtp_config["to"],
        }.items():
            if not value:
                missing.append(name)

    if config["max_selected_papers"] <= 0:
        raise RuntimeError("MAX_SELECTED_PAPERS must be greater than 0")

    if config["arxiv_page_size"] <= 0:
        raise RuntimeError("ARXIV_PAGE_SIZE must be greater than 0")

    if config["target_days_ago"] <= 0:
        raise RuntimeError("TARGET_DAYS_AGO must be greater than 0")

    if config["llm_timeout_seconds"] <= 0:
        raise RuntimeError("LLM_TIMEOUT_SECONDS must be greater than 0")

    try:
        ZoneInfo(config["local_timezone"])
    except Exception as exc:
        raise RuntimeError(f"LOCAL_TIMEZONE is invalid: {config['local_timezone']!r}") from exc

    if smtp_config["use_ssl"] and smtp_config["use_starttls"]:
        raise RuntimeError("EMAIL_USE_SSL and EMAIL_USE_STARTTLS cannot both be true")

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


def log_runtime_config(config, smtp_config):
    safe_config = {
        "dry_run": config["dry_run"],
        "log_raw_llm": config["log_raw_llm"],
        "llm_model": config["llm_model"],
        "llm_timeout_seconds": config["llm_timeout_seconds"],
        "llm_enable_thinking": config["llm_enable_thinking"],
        "llm_thinking_budget": config["llm_thinking_budget"],
        "max_selected_papers": config["max_selected_papers"],
        "arxiv_page_size": config["arxiv_page_size"],
        "target_days_ago": config["target_days_ago"],
        "local_timezone": config["local_timezone"],
        "smtp_host": smtp_config["host"],
        "smtp_port": smtp_config["port"],
        "smtp_use_ssl": smtp_config["use_ssl"],
        "smtp_use_starttls": smtp_config["use_starttls"],
        "email_user": smtp_config["user"],
        "email_to": smtp_config["to"],
        "dashscope_api_key_masked": mask_value(os.getenv("DASHSCOPE_API_KEY", "")),
        "email_pass_masked": mask_value(smtp_config["password"]),
    }
    LOGGER.info("Runtime configuration loaded | dry_run=%s model=%s llm_timeout=%ss thinking=%s thinking_budget=%s max_selected=%s arxiv_page_size=%s target_days_ago=%s timezone=%s smtp_host=%s smtp_port=%s smtp_ssl=%s smtp_starttls=%s log_raw_llm=%s",
                safe_config["dry_run"],
                safe_config["llm_model"],
                safe_config["llm_timeout_seconds"],
                safe_config["llm_enable_thinking"],
                safe_config["llm_thinking_budget"],
                safe_config["max_selected_papers"],
                safe_config["arxiv_page_size"],
                safe_config["target_days_ago"],
                safe_config["local_timezone"],
                safe_config["smtp_host"],
                safe_config["smtp_port"],
                safe_config["smtp_use_ssl"],
                safe_config["smtp_use_starttls"],
                safe_config["log_raw_llm"])
    write_json_artifact("config.json", safe_config)


def get_client():
    global CLIENT

    if CLIENT is None:
        CLIENT = OpenAI(
            api_key=os.getenv("DASHSCOPE_API_KEY"),
            base_url=DASHSCOPE_BASE_URL,
        )

    return CLIENT


def build_extra_body(config):
    extra_body = {
        "enable_thinking": config["llm_enable_thinking"],
    }
    if config["llm_thinking_budget"] is not None:
        extra_body["thinking_budget"] = config["llm_thinking_budget"]
    return extra_body


def get_target_date(config):
    local_tz = ZoneInfo(config["local_timezone"])
    now_local = datetime.now(local_tz)
    return now_local.date() - timedelta(days=config["target_days_ago"])


def parse_entry_published(entry, local_tz):
    published_parsed = getattr(entry, "published_parsed", None)
    if not published_parsed:
        raise ValueError("entry is missing published_parsed")

    published_utc = datetime(
        published_parsed.tm_year,
        published_parsed.tm_mon,
        published_parsed.tm_mday,
        published_parsed.tm_hour,
        published_parsed.tm_min,
        published_parsed.tm_sec,
        tzinfo=timezone.utc,
    )
    return published_utc.astimezone(local_tz)


def load_seen():
    if not SEEN_IDS_PATH.exists():
        LOGGER.info("seen_ids.json not found, starting with an empty history")
        return set()

    with SEEN_IDS_PATH.open(encoding="utf-8") as handle:
        data = json.load(handle)

    seen = set(data)
    LOGGER.info("Loaded seen ids | count=%d path=%s", len(seen), SEEN_IDS_PATH)
    return seen


def save_seen(seen):
    with SEEN_IDS_PATH.open("w", encoding="utf-8") as handle:
        json.dump(sorted(seen), handle, ensure_ascii=False, indent=2)
        handle.write("\n")

    LOGGER.info("Saved seen ids | count=%d path=%s", len(seen), SEEN_IDS_PATH)


def parse_json_response(text):
    text = text.strip()

    if text.startswith("```"):
        lines = text.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1:
        text = text[start : end + 1]

    return json.loads(text)


def get_author_value(author, field_name):
    if hasattr(author, field_name):
        return getattr(author, field_name)
    if isinstance(author, dict):
        return author.get(field_name)
    return None


def extract_authors(entry):
    authors = []
    for author in getattr(entry, "authors", []):
        name = get_author_value(author, "name") or "Unknown Author"
        affiliation = (
            get_author_value(author, "affiliation")
            or get_author_value(author, "arxiv_affiliation")
            or ""
        )
        authors.append(
            {
                "name": " ".join(str(name).split()),
                "affiliation": " ".join(str(affiliation).split()),
            }
        )

    if not authors and getattr(entry, "author", None):
        authors.append(
            {
                "name": " ".join(str(getattr(entry, "author", "Unknown Author")).split()),
                "affiliation": "",
            }
        )

    return authors


def format_authors_for_prompt(authors):
    if not authors:
        return "- Unknown authors / no affiliation metadata available"

    lines = []
    for author in authors:
        affiliation = author["affiliation"] or "No affiliation metadata available"
        lines.append(f"- {author['name']} | Affiliation: {affiliation}")
    return "\n".join(lines)


def format_authors_for_email(authors):
    if not authors:
        return "Unknown authors"

    formatted = []
    for author in authors:
        if author["affiliation"]:
            formatted.append(f"{author['name']} ({author['affiliation']})")
        else:
            formatted.append(author["name"])
    return "; ".join(formatted)


def score_to_color(score):
    if score >= 90:
        return "#0f766e"
    if score >= 80:
        return "#2563eb"
    if score >= 70:
        return "#7c3aed"
    return "#6b7280"


def maybe_hard_exclude_paper(title, abstract):
    text = f"{title}\n{abstract}"
    for rule_name, pattern, reason in HARD_EXCLUDE_PATTERNS:
        if pattern.search(text):
            return {
                "rule": rule_name,
                "reason": reason,
            }
    return None


def llm_call(prompt, stage, paper_tag, config):
    prompt_path = None
    if config["log_raw_llm"]:
        prompt_path = write_text_artifact(
            f"llm/{paper_tag}-{stage}-prompt.txt",
            prompt,
        )

    start_time = time.perf_counter()
    extra_body = build_extra_body(config)
    LOGGER.info(
        "LLM request started | stage=%s paper=%s model=%s timeout=%ss thinking=%s thinking_budget=%s",
        stage,
        paper_tag,
        config["llm_model"],
        config["llm_timeout_seconds"],
        config["llm_enable_thinking"],
        config["llm_thinking_budget"],
    )
    try:
        response = get_client().chat.completions.create(
            model=config["llm_model"],
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            timeout=config["llm_timeout_seconds"],
            extra_body=extra_body,
        )
    except Exception as exc:
        duration = time.perf_counter() - start_time
        LOGGER.exception(
            "LLM request failed | stage=%s paper=%s model=%s duration=%.2fs error_type=%s",
            stage,
            paper_tag,
            config["llm_model"],
            duration,
            type(exc).__name__,
        )
        raise

    duration = time.perf_counter() - start_time

    content = response.choices[0].message.content or ""
    usage = getattr(response, "usage", None)
    if usage is not None:
        LOGGER.info(
            "LLM request finished | stage=%s paper=%s model=%s duration=%.2fs total_tokens=%s prompt_tokens=%s completion_tokens=%s response_chars=%d response_id=%s",
            stage,
            paper_tag,
            config["llm_model"],
            duration,
            getattr(usage, "total_tokens", "n/a"),
            getattr(usage, "prompt_tokens", "n/a"),
            getattr(usage, "completion_tokens", "n/a"),
            len(content),
            getattr(response, "id", "n/a"),
        )
    else:
        LOGGER.info(
            "LLM request finished | stage=%s paper=%s model=%s duration=%.2fs response_chars=%d response_id=%s",
            stage,
            paper_tag,
            config["llm_model"],
            duration,
            len(content),
            getattr(response, "id", "n/a"),
        )

    if config["log_raw_llm"]:
        response_path = write_text_artifact(
            f"llm/{paper_tag}-{stage}-response.txt",
            content,
        )
        LOGGER.debug(
            "Saved raw LLM artifacts | stage=%s paper=%s prompt_path=%s response_path=%s",
            stage,
            paper_tag,
            prompt_path,
            response_path,
        )

    return content


def validate_assessment_payload(payload):
    if not isinstance(payload, dict):
        raise ValueError("assessment payload must be a JSON object")

    relevant = payload.get("relevant")
    score = payload.get("score")
    fit_area = payload.get("fit_area")
    reason = payload.get("reason")
    affiliation_signal = payload.get("affiliation_signal")

    if isinstance(relevant, str):
        normalized_relevant = relevant.strip().lower()
        if normalized_relevant in {"true", "yes"}:
            relevant = True
        elif normalized_relevant in {"false", "no"}:
            relevant = False

    if not isinstance(relevant, bool):
        raise ValueError("relevant must be a boolean")

    if isinstance(score, str):
        stripped_score = score.strip()
        if stripped_score.isdigit():
            score = int(stripped_score)
        else:
            try:
                score = int(round(float(stripped_score)))
            except ValueError:
                pass
    elif isinstance(score, float):
        score = int(round(score))

    if not isinstance(score, int):
        score = 0 if not relevant else None
    if score is None:
        raise ValueError("score must be an integer")
    if score < 0 or score > 100:
        raise ValueError("score must be between 0 and 100")

    fit_area = str(fit_area).strip() if fit_area is not None else ""
    if not fit_area:
        fit_area = "Irrelevant" if not relevant else "Mixed"

    normalized_fit_area = fit_area.lower()
    fit_area_map = {
        "pl": "PL",
        "programming languages": "PL",
        "os": "OS",
        "operating systems": "OS",
        "ai-infra": "AI-Infra",
        "ai infra": "AI-Infra",
        "mlsys": "AI-Infra",
        "ml systems": "AI-Infra",
        "ai-compiler": "AI-Compiler",
        "ai compiler": "AI-Compiler",
        "ml compiler": "AI-Compiler",
        "compiler": "Compiler",
        "compilers": "Compiler",
        "program-analysis": "Program-Analysis",
        "program analysis": "Program-Analysis",
        "static analysis": "Program-Analysis",
        "dynamic analysis": "Program-Analysis",
        "mixed": "Mixed",
        "irrelevant": "Irrelevant",
        "none": "Irrelevant",
        "n/a": "Irrelevant",
    }
    fit_area = fit_area_map.get(normalized_fit_area, fit_area)

    reason = str(reason).strip() if reason is not None else ""
    if not reason:
        reason = (
            "The paper does not appear to be a strong fit for this OS / AI-infra / compiler / program-analysis digest."
            if not relevant
            else "The paper appears relevant to this OS / AI-infra / compiler / program-analysis digest."
        )

    affiliation_signal = str(affiliation_signal).strip() if affiliation_signal is not None else ""
    if not affiliation_signal:
        affiliation_signal = "No useful affiliation signal is available."

    return {
        "relevant": relevant,
        "score": score if relevant else 0,
        "fit_area": fit_area,
        "reason": reason,
        "affiliation_signal": affiliation_signal,
    }


def assess_paper(title, abstract, authors, paper_tag, config):
    prompt = (
        f"{ASSESS_PROMPT}\n\n"
        f"Title: {title}\n"
        f"Authors:\n{format_authors_for_prompt(authors)}\n\n"
        f"Abstract: {abstract}"
    )
    result = llm_call(prompt, "assess", paper_tag, config)

    try:
        payload = parse_json_response(result)
        validated_payload = validate_assessment_payload(payload)
        LOGGER.info(
            "Paper assessed | paper=%s relevant=%s score=%d fit_area=%s",
            paper_tag,
            validated_payload["relevant"],
            validated_payload["score"],
            validated_payload["fit_area"],
        )
        return validated_payload
    except Exception as exc:
        LOGGER.warning("Failed to parse assessment response | paper=%s error=%s", paper_tag, exc)
        write_text_artifact(f"llm/{paper_tag}-assess-parse-error.txt", result)
        return None


def validate_summary_payload(payload):
    if not isinstance(payload, dict):
        raise ValueError("summary payload must be a JSON object")

    summary = payload.get("summary")
    translation = payload.get("translation")

    if not isinstance(summary, list) or not summary:
        raise ValueError("summary must be a non-empty list")

    if not all(isinstance(item, str) and item.strip() for item in summary):
        raise ValueError("summary items must be non-empty strings")

    if not isinstance(translation, str) or not translation.strip():
        raise ValueError("translation must be a non-empty string")

    explanation = payload.get("explanation")
    if explanation is not None and not isinstance(explanation, str):
        raise ValueError("explanation must be a string when present")

    return payload


def summarize(title, abstract, paper_tag, config):
    prompt = f"{SUMMARY_PROMPT}\n\nTitle: {title}\nAbstract: {abstract}"
    result = llm_call(prompt, "summary", paper_tag, config)

    try:
        payload = parse_json_response(result)
        validated_payload = validate_summary_payload(payload)
        LOGGER.info(
            "Summary parsed successfully | paper=%s bullet_count=%d",
            paper_tag,
            len(validated_payload["summary"]),
        )
        return validated_payload
    except Exception as exc:
        LOGGER.warning("Failed to parse summary response | paper=%s error=%s", paper_tag, exc)
        write_text_artifact(f"llm/{paper_tag}-summary-parse-error.txt", result)
        return None


def fetch_papers(config):
    page_size = config["arxiv_page_size"]
    target_date = get_target_date(config)
    local_tz = ZoneInfo(config["local_timezone"])
    selected_entries = []
    artifact_entries = []
    page_index = 0
    start = 0
    stop_fetching = False

    while True:
        url = build_arxiv_url(page_size)
        paged_url = f"{url}&start={start}"
        start_time = time.perf_counter()
        LOGGER.info(
            "Fetching arXiv feed page | page=%d start=%d page_size=%d target_date=%s timezone=%s url=%s",
            page_index + 1,
            start,
            page_size,
            target_date.isoformat(),
            config["local_timezone"],
            paged_url,
        )
        feed = feedparser.parse(paged_url)
        duration = time.perf_counter() - start_time

        if getattr(feed, "bozo", False):
            LOGGER.warning(
                "Feed parser reported a warning | page=%d error=%s",
                page_index + 1,
                getattr(feed, "bozo_exception", "unknown"),
            )

        entries = feed.entries
        LOGGER.info(
            "Fetched arXiv feed page | page=%d entries=%d duration=%.2fs",
            page_index + 1,
            len(entries),
            duration,
        )

        if not entries:
            LOGGER.info("No more arXiv entries returned, stopping pagination")
            break

        for entry in entries:
            title = " ".join(getattr(entry, "title", "").split())
            entry_id = getattr(entry, "id", "")
            entry_link = getattr(entry, "link", "")
            published_local = parse_entry_published(entry, local_tz)
            published_local_iso = published_local.isoformat()
            published_local_date = published_local.date()

            artifact_entries.append(
                {
                    "id": entry_id,
                    "title": title,
                    "link": entry_link,
                    "published_local": published_local_iso,
                    "published_local_date": published_local_date.isoformat(),
                }
            )

            if published_local_date > target_date:
                continue

            if published_local_date == target_date:
                selected_entries.append(entry)
                continue

            LOGGER.info(
                "Reached entries older than target date, stopping pagination | first_older_id=%s published_local=%s target_date=%s",
                entry_id,
                published_local_iso,
                target_date.isoformat(),
            )
            stop_fetching = True
            break

        if stop_fetching or len(entries) < page_size:
            break

        page_index += 1
        start += page_size

    write_json_artifact("fetched_entries.json", artifact_entries)
    LOGGER.info(
        "Collected target-day papers from arXiv | target_date=%s count=%d pages_fetched=%d",
        target_date.isoformat(),
        len(selected_entries),
        page_index + 1,
    )

    return selected_entries, target_date, page_index + 1


def build_email(papers):
    cards = []
    for index, paper in enumerate(papers, start=1):
        score_color = score_to_color(paper["score"])
        summary_items = "".join(
            f"<li>{escape(item)}</li>"
            for item in paper["summary"]
        )
        cards.append(
            f"""
            <section style="background:#ffffff;border:1px solid #e5e7eb;border-radius:20px;padding:24px 24px 20px;margin:0 0 18px;box-shadow:0 10px 30px rgba(15,23,42,0.06);">
              <div style="display:flex;justify-content:space-between;gap:16px;align-items:flex-start;flex-wrap:wrap;margin-bottom:14px;">
                <div style="flex:1;min-width:280px;">
                  <div style="font-size:12px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;margin-bottom:10px;">Rank #{index}</div>
                  <h2 style="margin:0;font-size:24px;line-height:1.3;color:#0f172a;">{escape(paper['title'])}</h2>
                </div>
                <div style="background:{score_color};color:#ffffff;border-radius:999px;padding:10px 14px;font-size:14px;font-weight:700;white-space:nowrap;">
                  Score {paper['score']}/100
                </div>
              </div>

              <div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px;">
                <span style="background:#eff6ff;color:#1d4ed8;border-radius:999px;padding:6px 10px;font-size:12px;font-weight:700;">{escape(paper['fit_area'])}</span>
                <span style="background:#f8fafc;color:#475569;border-radius:999px;padding:6px 10px;font-size:12px;font-weight:600;">OS / AI-Infra / Compiler digest</span>
              </div>

              <p style="margin:0 0 10px;color:#334155;font-size:14px;line-height:1.7;"><strong style="color:#0f172a;">Authors:</strong> {escape(paper['authors_display'])}</p>
              <p style="margin:0 0 10px;color:#334155;font-size:14px;line-height:1.7;"><strong style="color:#0f172a;">Why Read:</strong> {escape(paper['reason'])}</p>
              <p style="margin:0 0 18px;color:#334155;font-size:14px;line-height:1.7;"><strong style="color:#0f172a;">Affiliation Signal:</strong> {escape(paper['affiliation_signal'])}</p>

              <div style="background:#f8fafc;border-radius:16px;padding:16px 18px;margin-bottom:16px;">
                <div style="font-size:13px;font-weight:800;letter-spacing:0.04em;text-transform:uppercase;color:#475569;margin-bottom:10px;">Key Points</div>
                <ul style="margin:0;padding-left:20px;color:#1e293b;font-size:14px;line-height:1.75;">
                  {summary_items}
                </ul>
              </div>

              <div style="background:linear-gradient(135deg,#fff7ed 0%,#fffbeb 100%);border:1px solid #fed7aa;border-radius:16px;padding:16px 18px;margin-bottom:16px;">
                <div style="font-size:13px;font-weight:800;letter-spacing:0.04em;text-transform:uppercase;color:#9a3412;margin-bottom:8px;">中文速览</div>
                <p style="margin:0;color:#7c2d12;font-size:14px;line-height:1.75;">{escape(paper['translation'])}</p>
              </div>

              <a href="{escape(paper['link'])}" style="display:inline-block;background:#111827;color:#ffffff;text-decoration:none;padding:11px 16px;border-radius:12px;font-size:14px;font-weight:700;">Read on arXiv</a>
            </section>
            """
        )

    return f"""
    <html>
      <body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#0f172a;">
        <div style="max-width:920px;margin:0 auto;padding:32px 18px 40px;">
          <header style="background:linear-gradient(135deg,#0f172a 0%,#1d4ed8 60%,#0f766e 100%);border-radius:28px;padding:28px 28px 24px;color:#ffffff;box-shadow:0 18px 60px rgba(15,23,42,0.22);margin-bottom:22px;">
            <div style="font-size:13px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;opacity:0.82;margin-bottom:10px;">Daily Research Digest</div>
            <h1 style="margin:0 0 10px;font-size:34px;line-height:1.15;">Top {len(papers)} Papers For OS, AI Infra, AI Compilers, and Program Analysis</h1>
            <p style="margin:0;font-size:16px;line-height:1.7;max-width:680px;opacity:0.92;">
              Ranked by overall quality and worth-reading score within the digest scope, using abstract as the primary signal and author affiliations as a secondary confidence signal.
            </p>
          </header>
          {''.join(cards)}
        </div>
      </body>
    </html>
    """


def send_email(html, smtp_config):
    recipients = [item.strip() for item in smtp_config["to"].split(",") if item.strip()]
    msg = MIMEText(html, "html", "utf-8")
    msg["Subject"] = "Top 10: OS, AI Infra, AI Compilers, Program Analysis"
    msg["From"] = smtp_config["user"]
    msg["To"] = ", ".join(recipients)

    smtp_class = smtplib.SMTP_SSL if smtp_config["use_ssl"] else smtplib.SMTP

    LOGGER.info(
        "Sending email | host=%s port=%s recipients=%d use_ssl=%s use_starttls=%s",
        smtp_config["host"],
        smtp_config["port"],
        len(recipients),
        smtp_config["use_ssl"],
        smtp_config["use_starttls"],
    )
    start_time = time.perf_counter()

    with smtp_class(smtp_config["host"], smtp_config["port"]) as server:
        if smtp_config["use_starttls"]:
            server.ehlo()
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
        server.login(smtp_config["user"], smtp_config["password"])
        server.send_message(msg)

    duration = time.perf_counter() - start_time
    LOGGER.info("Email sent successfully | duration=%.2fs recipients=%s", duration, recipients)


def main():
    setup_logging()

    config = get_runtime_config()
    smtp_config = get_smtp_config()
    validate_runtime_config(config, smtp_config)
    log_runtime_config(config, smtp_config)

    stats = {
        "fetched_target_day": 0,
        "pages_fetched": 0,
        "skipped_seen": 0,
        "hard_filtered": 0,
        "assessed": 0,
        "assessment_failed": 0,
        "relevance_filtered": 0,
        "relevant_candidates": 0,
        "summary_failed": 0,
        "selected": 0,
    }

    LOGGER.info("Pipeline started")

    seen = load_seen()
    new_seen = set(seen)
    selected = []
    candidates = []
    all_assessments = []

    papers, target_date, pages_fetched = fetch_papers(config)
    stats["fetched_target_day"] = len(papers)
    stats["pages_fetched"] = pages_fetched

    for index, entry in enumerate(papers, start=1):
        title = " ".join(getattr(entry, "title", "").split())
        abstract = " ".join(getattr(entry, "summary", "").split())
        paper_id = getattr(entry, "id", "")
        paper_link = getattr(entry, "link", "")
        authors = extract_authors(entry)
        paper_tag = f"{index:02d}-{slugify(title)[:60]}"

        LOGGER.info(
            "Processing paper | paper=%s id=%s title=%s abstract_chars=%d author_count=%d",
            paper_tag,
            paper_id,
            title,
            len(abstract),
            len(authors),
        )

        if paper_id in seen:
            stats["skipped_seen"] += 1
            LOGGER.info("Skipping already seen paper | paper=%s id=%s", paper_tag, paper_id)
            continue

        hard_exclusion = maybe_hard_exclude_paper(title, abstract)
        if hard_exclusion:
            stats["hard_filtered"] += 1
            new_seen.add(paper_id)
            hard_filtered_record = {
                "id": paper_id,
                "title": title,
                "link": paper_link,
                "authors": authors,
                "authors_display": format_authors_for_email(authors),
                "relevant": False,
                "score": 0,
                "fit_area": "Irrelevant",
                "reason": hard_exclusion["reason"],
                "affiliation_signal": "Skipped by hard filter before LLM assessment.",
                "hard_filter_rule": hard_exclusion["rule"],
            }
            all_assessments.append(hard_filtered_record)
            LOGGER.info(
                "Paper removed by hard filter | paper=%s id=%s rule=%s",
                paper_tag,
                paper_id,
                hard_exclusion["rule"],
            )
            continue

        try:
            assessment = assess_paper(title, abstract, authors, paper_tag, config)
        except Exception:
            stats["assessment_failed"] += 1
            LOGGER.exception("Assessment failed | paper=%s id=%s", paper_tag, paper_id)
            continue

        if not assessment:
            stats["assessment_failed"] += 1
            continue

        stats["assessed"] += 1
        new_seen.add(paper_id)

        assessment_record = {
            "id": paper_id,
            "title": title,
            "link": paper_link,
            "authors": authors,
            "authors_display": format_authors_for_email(authors),
            "relevant": assessment["relevant"],
            "score": assessment["score"],
            "fit_area": assessment["fit_area"],
            "reason": assessment["reason"],
            "affiliation_signal": assessment["affiliation_signal"],
        }
        all_assessments.append(assessment_record)

        if not assessment["relevant"]:
            stats["relevance_filtered"] += 1
            continue

        stats["relevant_candidates"] += 1
        candidate = {
            "id": paper_id,
            "title": title,
            "link": paper_link,
            "abstract": abstract,
            "authors": authors,
            "authors_display": assessment_record["authors_display"],
            "score": assessment["score"],
            "fit_area": assessment["fit_area"],
            "reason": assessment["reason"],
            "affiliation_signal": assessment["affiliation_signal"],
        }
        candidates.append(candidate)

    ranked_candidates = sorted(
        candidates,
        key=lambda item: (-item["score"], item["title"].lower()),
    )
    assessments_path = write_json_artifact("paper_assessments.json", all_assessments)
    ranked_path = write_json_artifact("ranked_candidates.json", ranked_candidates)
    LOGGER.info(
        "Assessment artifacts written | assessments_path=%s ranked_path=%s relevant_candidates=%d",
        assessments_path,
        ranked_path,
        len(ranked_candidates),
    )

    for index, candidate in enumerate(ranked_candidates, start=1):
        if len(selected) >= config["max_selected_papers"]:
            LOGGER.info(
                "Reached top-N limit after summarization | max_selected=%d",
                config["max_selected_papers"],
            )
            break

        paper_tag = f"ranked-{index:02d}-{slugify(candidate['title'])[:60]}"

        try:
            result = summarize(candidate["title"], candidate["abstract"], paper_tag, config)
        except Exception:
            stats["summary_failed"] += 1
            LOGGER.exception("Summary generation crashed | paper=%s id=%s", paper_tag, candidate["id"])
            continue

        if not result:
            stats["summary_failed"] += 1
            continue

        selected_item = {
            "id": candidate["id"],
            "title": candidate["title"],
            "summary": result["summary"],
            "translation": result["translation"],
            "explanation": result.get("explanation", ""),
            "link": candidate["link"],
            "authors": candidate["authors"],
            "authors_display": candidate["authors_display"],
            "score": candidate["score"],
            "fit_area": candidate["fit_area"],
            "reason": candidate["reason"],
            "affiliation_signal": candidate["affiliation_signal"],
        }
        selected.append(selected_item)
        stats["selected"] = len(selected)
        LOGGER.info(
            "Paper selected for final digest | rank=%d selected_count=%d score=%d title=%s",
            index,
            len(selected),
            candidate["score"],
            candidate["title"],
        )

    selected_path = write_json_artifact("selected_papers.json", selected)
    LOGGER.info("Selected papers artifact written | path=%s count=%d", selected_path, len(selected))

    email_preview_path = None
    if selected:
        html = build_email(selected)
        email_preview_path = write_text_artifact("email_preview.html", html)
        LOGGER.info("Email preview written | path=%s", email_preview_path)

        if config["dry_run"]:
            LOGGER.info("DRY_RUN enabled, skipping email send")
        else:
            send_email(html, smtp_config)
    else:
        LOGGER.info("No papers selected, skipping email generation and email send")

    if config["dry_run"]:
        preview_seen_path = write_json_artifact("seen_ids.preview.json", sorted(new_seen))
        LOGGER.info(
            "DRY_RUN enabled, skipping seen_ids.json update | preview_path=%s count=%d",
            preview_seen_path,
            len(new_seen),
        )
    else:
        save_seen(new_seen)

    summary_payload = {
        "dry_run": config["dry_run"],
        "target_date": target_date.isoformat(),
        "local_timezone": config["local_timezone"],
        "stats": stats,
        "selected_titles": [paper["title"] for paper in selected],
        "selected_ids": [paper["id"] for paper in selected],
        "log_dir": str(RUN_DIR) if RUN_DIR else None,
        "email_preview_path": str(email_preview_path) if email_preview_path else None,
    }
    summary_path = write_json_artifact("pipeline_summary.json", summary_payload)
    LOGGER.info("Pipeline finished successfully | summary_path=%s", summary_path)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        if LOGGER.handlers:
            LOGGER.exception("Pipeline failed")
        raise
