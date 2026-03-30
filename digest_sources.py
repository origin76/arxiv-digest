import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import feedparser

from digest_runtime import LOGGER, write_json_artifact

SEEN_IDS_PATH = Path("seen_ids.json")
OPENALEX_CACHE_PATH = Path("openalex_cache.json")
ARXIV_SEARCH_QUERY = "(cat:cs.OS OR cat:cs.PL OR cat:cs.LG OR cat:cs.DC OR cat:cs.AR)"
ARXIV_API_BASE = "https://export.arxiv.org/api/query"
ARXIV_TIMEOUT_SECONDS = 30
ARXIV_MAX_ATTEMPTS = 5
ARXIV_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
ARXIV_BASE_BACKOFF_SECONDS = 5.0
ARXIV_MAX_BACKOFF_SECONDS = 60.0
ARXIV_PAGE_DELAY_SECONDS = 3.0
OPENALEX_API_BASE = "https://api.openalex.org"
OPENALEX_AUTHOR_SEARCH_SELECT_FIELDS = "id,display_name,works_count,orcid"
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


def build_arxiv_url(max_results):
    params = {
        "search_query": ARXIV_SEARCH_QUERY,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "max_results": max_results,
    }
    return f"{ARXIV_API_BASE}?{urlencode(params)}"


def parse_retry_after_seconds(value):
    text = str(value or "").strip()
    if not text:
        return None

    try:
        return max(float(text), 0.0)
    except ValueError:
        pass

    try:
        retry_at = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None

    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)

    return max((retry_at - datetime.now(timezone.utc)).total_seconds(), 0.0)


def compute_arxiv_backoff_seconds(attempt, retry_after_header=None):
    retry_after_seconds = parse_retry_after_seconds(retry_after_header)
    if retry_after_seconds is not None:
        return retry_after_seconds

    return min(
        ARXIV_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)),
        ARXIV_MAX_BACKOFF_SECONDS,
    )


def fetch_arxiv_feed(url):
    headers = {
        "User-Agent": "arxiv-digest/1.0 (+https://github.com/)",
        "Accept": "application/atom+xml, application/xml;q=0.9, */*;q=0.8",
    }
    request = Request(url, headers=headers)

    for attempt in range(1, ARXIV_MAX_ATTEMPTS + 1):
        start_time = time.perf_counter()
        LOGGER.info(
            "arXiv feed request started | attempt=%d/%d timeout=%ss url=%s",
            attempt,
            ARXIV_MAX_ATTEMPTS,
            ARXIV_TIMEOUT_SECONDS,
            url,
        )
        try:
            with urlopen(request, timeout=ARXIV_TIMEOUT_SECONDS) as response:
                payload = response.read()
        except HTTPError as exc:
            duration = time.perf_counter() - start_time
            retry_after_header = exc.headers.get("Retry-After") if exc.headers else None
            LOGGER.warning(
                "arXiv feed request failed | attempt=%d/%d duration=%.2fs status=%s reason=%s retry_after=%s url=%s",
                attempt,
                ARXIV_MAX_ATTEMPTS,
                duration,
                exc.code,
                exc.reason,
                retry_after_header or "<none>",
                url,
            )

            if exc.code not in ARXIV_RETRYABLE_STATUS_CODES or attempt == ARXIV_MAX_ATTEMPTS:
                raise

            sleep_seconds = compute_arxiv_backoff_seconds(
                attempt,
                retry_after_header=retry_after_header,
            )
            LOGGER.info(
                "Retrying arXiv feed request after HTTP error | attempt=%d/%d sleep=%.2fs status=%s url=%s",
                attempt,
                ARXIV_MAX_ATTEMPTS,
                sleep_seconds,
                exc.code,
                url,
            )
            time.sleep(sleep_seconds)
            continue
        except (URLError, TimeoutError) as exc:
            duration = time.perf_counter() - start_time
            LOGGER.warning(
                "arXiv feed request failed | attempt=%d/%d duration=%.2fs error=%s url=%s",
                attempt,
                ARXIV_MAX_ATTEMPTS,
                duration,
                exc,
                url,
            )

            if attempt == ARXIV_MAX_ATTEMPTS:
                raise

            sleep_seconds = compute_arxiv_backoff_seconds(attempt)
            LOGGER.info(
                "Retrying arXiv feed request after transport error | attempt=%d/%d sleep=%.2fs url=%s",
                attempt,
                ARXIV_MAX_ATTEMPTS,
                sleep_seconds,
                url,
            )
            time.sleep(sleep_seconds)
            continue

        duration = time.perf_counter() - start_time
        LOGGER.info(
            "arXiv feed request finished | attempt=%d/%d duration=%.2fs bytes=%d url=%s",
            attempt,
            ARXIV_MAX_ATTEMPTS,
            duration,
            len(payload),
            url,
        )
        return feedparser.parse(payload)

    raise RuntimeError("arXiv feed request exhausted all retry attempts")


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
        if page_index > 0:
            LOGGER.info(
                "Respecting arXiv API pacing before next page | sleep=%.2fs next_page=%d",
                ARXIV_PAGE_DELAY_SECONDS,
                page_index + 1,
            )
            time.sleep(ARXIV_PAGE_DELAY_SECONDS)
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
        feed = fetch_arxiv_feed(paged_url)
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


def load_openalex_cache():
    if not OPENALEX_CACHE_PATH.exists():
        LOGGER.info("OpenAlex cache not found, starting without cache | path=%s", OPENALEX_CACHE_PATH)
        return {}

    try:
        with OPENALEX_CACHE_PATH.open(encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:
        LOGGER.warning(
            "Failed to load OpenAlex cache, starting without cache | path=%s error=%s",
            OPENALEX_CACHE_PATH,
            exc,
        )
        return {}

    if isinstance(data, dict) and isinstance(data.get("entries"), dict):
        cache = data["entries"]
    elif isinstance(data, dict):
        cache = data
    else:
        raise RuntimeError("openalex_cache.json must contain a JSON object")

    LOGGER.info("Loaded OpenAlex cache | count=%d path=%s", len(cache), OPENALEX_CACHE_PATH)
    return cache


def save_openalex_cache(cache):
    payload = {
        "version": 1,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "entries": cache,
    }
    with OPENALEX_CACHE_PATH.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")

    LOGGER.info("Saved OpenAlex cache | count=%d path=%s", len(cache), OPENALEX_CACHE_PATH)


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


def normalize_text(value):
    cleaned = " ".join(str(value or "").split()).lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    return " ".join(cleaned.split())


def author_name_signature(name):
    tokens = normalize_text(name).split()
    if not tokens:
        return None

    return {
        "full": " ".join(tokens),
        "first": tokens[0],
        "last": tokens[-1],
        "first_initial": tokens[0][0],
    }


def compare_author_names(left, right):
    left_sig = author_name_signature(left)
    right_sig = author_name_signature(right)

    if not left_sig or not right_sig:
        return 0
    if left_sig["full"] == right_sig["full"]:
        return 4
    if left_sig["last"] == right_sig["last"] and left_sig["first"] == right_sig["first"]:
        return 3
    if (
        left_sig["last"] == right_sig["last"]
        and left_sig["first_initial"] == right_sig["first_initial"]
    ):
        return 2
    if left_sig["last"] == right_sig["last"]:
        return 1
    return 0


def authors_need_enrichment(authors):
    return bool(authors) and any(not author.get("affiliation") for author in authors)


def build_openalex_url(endpoint, params):
    query = dict(params)
    if not query.get("mailto"):
        query.pop("mailto", None)
    base_url = f"{OPENALEX_API_BASE}/{endpoint}"
    if not query:
        return base_url
    return f"{base_url}?{urlencode(query)}"


def fetch_openalex_json(endpoint, params, config, paper_tag, lookup_mode, subject):
    url = build_openalex_url(endpoint, params)
    headers = {
        "User-Agent": "arxiv-digest/1.0 (+https://github.com/)",
        "Accept": "application/json",
    }
    request = Request(url, headers=headers)
    start_time = time.perf_counter()
    LOGGER.info(
        "OpenAlex request started | paper=%s mode=%s subject=%s url=%s timeout=%ss",
        paper_tag,
        lookup_mode,
        subject,
        url,
        config["openalex_timeout_seconds"],
    )
    try:
        with urlopen(request, timeout=config["openalex_timeout_seconds"]) as response:
            payload = json.load(response)
    except Exception:
        duration = time.perf_counter() - start_time
        LOGGER.exception(
            "OpenAlex request failed | paper=%s mode=%s subject=%s duration=%.2fs url=%s",
            paper_tag,
            lookup_mode,
            subject,
            duration,
            url,
        )
        raise

    duration = time.perf_counter() - start_time
    results = payload.get("results", []) if isinstance(payload, dict) else []
    LOGGER.info(
        "OpenAlex request finished | paper=%s mode=%s subject=%s duration=%.2fs results=%d",
        paper_tag,
        lookup_mode,
        subject,
        duration,
        len(results),
    )
    return payload, url, duration


def summarize_openalex_author_institutions(author_payload):
    institutions = []
    for institution in author_payload.get("last_known_institutions") or []:
        display_name = " ".join(str(institution.get("display_name", "")).split())
        if display_name and display_name not in institutions:
            institutions.append(display_name)

    deprecated_last_known = author_payload.get("last_known_institution") or {}
    deprecated_display_name = " ".join(
        str(deprecated_last_known.get("display_name", "")).split()
    )
    if deprecated_display_name and deprecated_display_name not in institutions:
        institutions.append(deprecated_display_name)

    affiliations = author_payload.get("affiliations") or []
    sorted_affiliations = sorted(
        affiliations,
        key=lambda item: max(item.get("years") or [0]),
        reverse=True,
    )
    for affiliation in sorted_affiliations:
        institution = affiliation.get("institution") or {}
        display_name = " ".join(str(institution.get("display_name", "")).split())
        if display_name and display_name not in institutions:
            institutions.append(display_name)

    return "; ".join(institutions[:2])


def iter_openalex_author_names(candidate):
    names = []
    for value in [candidate.get("display_name"), *(candidate.get("display_name_alternatives") or [])]:
        cleaned = " ".join(str(value or "").split())
        if cleaned and cleaned not in names:
            names.append(cleaned)
    return names


def evaluate_openalex_author_candidate(author_name, candidate):
    candidate_names = iter_openalex_author_names(candidate)
    name_score = max(
        (compare_author_names(author_name, candidate_name) for candidate_name in candidate_names),
        default=0,
    )
    exact_name = any(
        normalize_text(candidate_name) == normalize_text(author_name)
        for candidate_name in candidate_names
    )
    institution_summary = summarize_openalex_author_institutions(candidate)
    works_count = candidate.get("works_count") or 0
    score = (name_score * 100) + (20 if exact_name else 0) + (5 if institution_summary else 0)
    return {
        "candidate": candidate,
        "name_score": name_score,
        "exact_name": exact_name,
        "institution_summary": institution_summary,
        "works_count": works_count,
        "candidate_score": score,
    }


def choose_openalex_author(author_name, results):
    evaluations = [
        evaluate_openalex_author_candidate(author_name, candidate)
        for candidate in results
    ]
    evaluations.sort(
        key=lambda item: (
            item["candidate_score"],
            item["works_count"],
        ),
        reverse=True,
    )

    if not evaluations:
        return None, {
            "match_type": "none",
            "name_score": 0,
            "exact_name": False,
            "candidate_count": 0,
            "works_count": 0,
            "institution_summary": "",
        }

    best = evaluations[0]
    second = evaluations[1] if len(evaluations) > 1 else None
    exact_matches = [item for item in evaluations if item["exact_name"]]

    metrics = {
        "match_type": "ambiguous",
        "name_score": best["name_score"],
        "exact_name": best["exact_name"],
        "candidate_count": len(evaluations),
        "works_count": best["works_count"],
        "institution_summary": best["institution_summary"],
    }

    if len(exact_matches) == 1 and best["exact_name"]:
        metrics["match_type"] = "unique_exact"
        return best["candidate"], metrics

    if len(exact_matches) > 1:
        metrics["match_type"] = "multiple_exact"
        return None, metrics

    if best["name_score"] < 3:
        metrics["match_type"] = "weak_name_match"
        return None, metrics

    if second and second["name_score"] >= best["name_score"]:
        metrics["match_type"] = "ambiguous_name_match"
        return None, metrics

    metrics["match_type"] = "unique_name_match"
    return best["candidate"], metrics


def get_openalex_author_cache_key(author_name):
    return f"author_name::{normalize_text(author_name)}"


def fetch_openalex_author_lookup(author_name, paper_tag, config):
    params = {
        "search": author_name,
        "select": OPENALEX_AUTHOR_SEARCH_SELECT_FIELDS,
        "per-page": 5,
        "mailto": config["openalex_email"] or None,
    }

    try:
        payload, query_url, _duration = fetch_openalex_json(
            "authors",
            params,
            config,
            paper_tag,
            "author_search",
            author_name,
        )
        results = payload.get("results", []) if isinstance(payload, dict) else []
        matched_author, metrics = choose_openalex_author(author_name, results)
        if not matched_author:
            return {
                "status": "not_found",
                "cache_hit": False,
                "query_url": query_url,
                "matched_author_id": None,
                "matched_author_name": None,
                "match_type": metrics["match_type"],
                "name_score": metrics["name_score"],
                "exact_name": metrics["exact_name"],
                "candidate_count": metrics["candidate_count"],
                "works_count": metrics["works_count"],
                "institution_summary": "",
            }

        author_id = matched_author.get("id", "")
        detail_endpoint = author_id.removeprefix("https://openalex.org/").removeprefix("http://openalex.org/")
        detail_payload, detail_url, _detail_duration = fetch_openalex_json(
            detail_endpoint,
            {"mailto": config["openalex_email"] or None},
            config,
            paper_tag,
            "author_detail",
            author_name,
        )
        author_detail = detail_payload if isinstance(detail_payload, dict) else {}
        institution_summary = summarize_openalex_author_institutions(author_detail)

        return {
            "status": "matched" if institution_summary else "matched_no_institution",
            "cache_hit": False,
            "query_url": query_url,
            "detail_url": detail_url,
            "matched_author_id": author_id,
            "matched_author_name": author_detail.get("display_name") or matched_author.get("display_name"),
            "match_type": metrics["match_type"],
            "name_score": metrics["name_score"],
            "exact_name": metrics["exact_name"],
            "candidate_count": metrics["candidate_count"],
            "works_count": author_detail.get("works_count", metrics["works_count"]),
            "institution_summary": institution_summary,
        }
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {
            "status": "error",
            "cache_hit": False,
            "query_url": None,
            "matched_author_id": None,
            "matched_author_name": None,
            "match_type": "error",
            "name_score": 0,
            "exact_name": False,
            "candidate_count": 0,
            "works_count": 0,
            "institution_summary": "",
            "error": str(exc),
        }


def lookup_openalex_author(author_name, paper_tag, config, openalex_cache):
    cache_key = get_openalex_author_cache_key(author_name)
    cached = openalex_cache.get(cache_key)
    if cached:
        result = dict(cached)
        result["cache_hit"] = True
        LOGGER.info(
            "OpenAlex author cache hit | paper=%s author=%s status=%s",
            paper_tag,
            author_name,
            result["status"],
        )
        return result

    result = fetch_openalex_author_lookup(author_name, paper_tag, config)
    if result["status"] != "error":
        openalex_cache[cache_key] = dict(result)
    return result


def collect_missing_affiliation_author_names(papers):
    author_names = []
    for paper in papers:
        for author in paper["authors"]:
            if author.get("affiliation"):
                continue
            author_name = " ".join(str(author.get("name", "")).split())
            if author_name:
                author_names.append(author_name)
    return author_names


def batch_lookup_openalex_authors(author_names, config, openalex_cache):
    lookups_by_key = {}
    unique_author_names = {}

    for author_name in author_names:
        cache_key = get_openalex_author_cache_key(author_name)
        if cache_key not in unique_author_names:
            unique_author_names[cache_key] = author_name

    stats = {
        "unique_authors": len(unique_author_names),
        "network_fetches": 0,
        "cache_hits": 0,
        "workers": 0,
    }

    if not unique_author_names:
        LOGGER.info("OpenAlex batch lookup skipped | unique_authors=0")
        return lookups_by_key, stats

    authors_to_fetch = {}
    for cache_key, author_name in unique_author_names.items():
        cached = openalex_cache.get(cache_key)
        if cached:
            result = dict(cached)
            result["cache_hit"] = True
            lookups_by_key[cache_key] = result
            stats["cache_hits"] += 1
            continue
        authors_to_fetch[cache_key] = author_name

    worker_count = min(config["openalex_max_workers"], max(len(authors_to_fetch), 1))
    stats["workers"] = worker_count
    stats["network_fetches"] = len(authors_to_fetch)

    LOGGER.info(
        "OpenAlex batch lookup prepared | unique_authors=%d cache_hits=%d network_fetches=%d max_workers=%d",
        stats["unique_authors"],
        stats["cache_hits"],
        stats["network_fetches"],
        worker_count,
    )

    if not authors_to_fetch:
        return lookups_by_key, stats

    if worker_count == 1:
        for cache_key, author_name in authors_to_fetch.items():
            result = fetch_openalex_author_lookup(author_name, "openalex-batch", config)
            lookups_by_key[cache_key] = result
            if result["status"] != "error":
                openalex_cache[cache_key] = dict(result)
        return lookups_by_key, stats

    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="openalex") as executor:
        future_map = {
            executor.submit(fetch_openalex_author_lookup, author_name, "openalex-batch", config): (cache_key, author_name)
            for cache_key, author_name in authors_to_fetch.items()
        }
        for future in as_completed(future_map):
            cache_key, author_name = future_map[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "status": "error",
                    "cache_hit": False,
                    "query_url": None,
                    "matched_author_id": None,
                    "matched_author_name": None,
                    "match_type": "thread_error",
                    "name_score": 0,
                    "exact_name": False,
                    "candidate_count": 0,
                    "works_count": 0,
                    "institution_summary": "",
                    "error": str(exc),
                }
                LOGGER.warning(
                    "OpenAlex batch future failed unexpectedly | author=%s error=%s",
                    author_name,
                    exc,
                )

            lookups_by_key[cache_key] = result
            if result["status"] != "error":
                openalex_cache[cache_key] = dict(result)

    LOGGER.info(
        "OpenAlex batch lookup finished | unique_authors=%d cache_hits=%d network_fetches=%d workers=%d",
        stats["unique_authors"],
        stats["cache_hits"],
        stats["network_fetches"],
        stats["workers"],
    )
    return lookups_by_key, stats


def enrich_authors_with_openalex(authors, paper_id, paper_tag, config, openalex_cache, lookup_results_by_key=None):
    record = {
        "paper": paper_tag,
        "paper_id": paper_id,
        "status": "skipped",
        "lookup_mode": "author_name_only",
        "filled_affiliations": 0,
        "authors_before": authors,
        "authors_after": authors,
        "author_lookups": [],
    }

    if not config["openalex_enrichment_enabled"]:
        record["status"] = "disabled"
        return authors, record

    if not authors:
        record["status"] = "skipped_no_authors"
        return authors, record

    if not authors_need_enrichment(authors):
        record["status"] = "skipped_no_missing_affiliation"
        return authors, record

    enriched_authors = []
    cached_hits = 0
    filled_affiliations = 0
    matched_authors = 0
    failed_lookups = 0

    for author in authors:
        updated_author = dict(author)
        updated_author["affiliation_source"] = "arxiv" if author.get("affiliation") else "missing"

        if author.get("affiliation"):
            record["author_lookups"].append(
                {
                    "author_name": author["name"],
                    "status": "skipped_existing_affiliation",
                }
            )
            enriched_authors.append(updated_author)
            continue

        cache_key = get_openalex_author_cache_key(author["name"])
        if lookup_results_by_key is not None:
            lookup = dict(
                lookup_results_by_key.get(
                    cache_key,
                    {
                        "status": "error",
                        "cache_hit": False,
                        "query_url": None,
                        "matched_author_id": None,
                        "matched_author_name": None,
                        "match_type": "missing_batch_lookup",
                        "name_score": 0,
                        "exact_name": False,
                        "candidate_count": 0,
                        "works_count": 0,
                        "institution_summary": "",
                        "error": "Missing precomputed OpenAlex lookup result.",
                    },
                )
            )
        else:
            lookup = lookup_openalex_author(author["name"], paper_tag, config, openalex_cache)

        if lookup.get("cache_hit"):
            cached_hits += 1

        author_record = {"author_name": author["name"], **lookup}
        record["author_lookups"].append(author_record)

        if lookup["status"] == "error":
            failed_lookups += 1
        elif lookup["status"] in {"matched", "matched_no_institution"}:
            matched_authors += 1

        institution_summary = lookup.get("institution_summary", "")
        if institution_summary:
            updated_author["affiliation"] = institution_summary
            updated_author["affiliation_source"] = "openalex"
            filled_affiliations += 1

        enriched_authors.append(updated_author)

    record.update(
        {
            "cache_hit_count": cached_hits,
            "matched_authors": matched_authors,
            "failed_lookups": failed_lookups,
            "filled_affiliations": filled_affiliations,
            "authors_after": enriched_authors,
        }
    )

    if filled_affiliations > 0:
        record["status"] = "enriched"
    elif failed_lookups > 0 and matched_authors == 0:
        record["status"] = "error"
    elif matched_authors > 0:
        record["status"] = "matched_no_fill"
    else:
        record["status"] = "not_found"

    LOGGER.info(
        "OpenAlex enrichment finished | paper=%s status=%s matched_authors=%d filled_affiliations=%d cache_hits=%d failed_lookups=%d",
        paper_tag,
        record["status"],
        matched_authors,
        filled_affiliations,
        cached_hits,
        failed_lookups,
    )
    return enriched_authors, record


def maybe_hard_exclude_paper(title, abstract):
    text = f"{title}\n{abstract}"
    for rule_name, pattern, reason in HARD_EXCLUDE_PATTERNS:
        if pattern.search(text):
            return {
                "rule": rule_name,
                "reason": reason,
            }
    return None
