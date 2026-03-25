from digest_config import (
    get_runtime_config,
    get_smtp_config,
    log_runtime_config,
    validate_runtime_config,
)
from digest_email import build_email, send_email
from digest_llm import batch_assess_papers, batch_summarize_papers
from digest_runtime import (
    LOGGER,
    get_run_dir,
    setup_logging,
    slugify,
    write_json_artifact,
    write_text_artifact,
)
from digest_sources import (
    batch_lookup_openalex_authors,
    collect_missing_affiliation_author_names,
    enrich_authors_with_openalex,
    extract_authors,
    fetch_papers,
    format_authors_for_email,
    load_openalex_cache,
    load_seen,
    maybe_hard_exclude_paper,
    save_openalex_cache,
    save_seen,
)


def build_stats():
    return {
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
        "openalex_attempted": 0,
        "openalex_enriched": 0,
        "openalex_cache_hits": 0,
        "openalex_not_found": 0,
        "openalex_failed": 0,
        "openalex_author_queries": 0,
        "openalex_author_matches": 0,
    }


def build_hard_filtered_record(paper_id, title, paper_link, authors, hard_exclusion):
    return {
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


def prepare_pending_papers(entries, seen, stats, new_seen, all_assessments):
    pending_papers = []

    for index, entry in enumerate(entries, start=1):
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
            all_assessments.append(
                build_hard_filtered_record(
                    paper_id,
                    title,
                    paper_link,
                    authors,
                    hard_exclusion,
                )
            )
            LOGGER.info(
                "Paper removed by hard filter | paper=%s id=%s rule=%s",
                paper_tag,
                paper_id,
                hard_exclusion["rule"],
            )
            continue

        pending_papers.append(
            {
                "id": paper_id,
                "title": title,
                "abstract": abstract,
                "link": paper_link,
                "authors": authors,
                "paper_tag": paper_tag,
            }
        )

    return pending_papers


def apply_openalex_enrichment(pending_papers, config, openalex_cache, stats):
    openalex_records = []
    if not pending_papers:
        return openalex_records

    stats["openalex_attempted"] = len(pending_papers)
    missing_author_names = collect_missing_affiliation_author_names(pending_papers)
    openalex_lookup_results, openalex_batch_stats = batch_lookup_openalex_authors(
        missing_author_names,
        config,
        openalex_cache,
    )
    LOGGER.info(
        "Applying OpenAlex enrichment before LLM assessment | papers=%d unique_missing_authors=%d network_fetches=%d cache_hits=%d workers=%d",
        len(pending_papers),
        openalex_batch_stats["unique_authors"],
        openalex_batch_stats["network_fetches"],
        openalex_batch_stats["cache_hits"],
        openalex_batch_stats["workers"],
    )

    for paper in pending_papers:
        authors, openalex_record = enrich_authors_with_openalex(
            paper["authors"],
            paper["id"],
            paper["paper_tag"],
            config,
            openalex_cache,
            lookup_results_by_key=openalex_lookup_results,
        )
        paper["authors"] = authors
        paper["authors_display"] = format_authors_for_email(authors)
        paper["openalex"] = openalex_record

        openalex_records.append(openalex_record)
        stats["openalex_cache_hits"] += openalex_record.get("cache_hit_count", 0)
        stats["openalex_author_queries"] += sum(
            1
            for item in openalex_record.get("author_lookups", [])
            if item.get("status") not in {"skipped_existing_affiliation"}
        )
        stats["openalex_author_matches"] += openalex_record.get("matched_authors", 0)
        if openalex_record["status"] == "enriched":
            stats["openalex_enriched"] += 1
        elif openalex_record["status"] == "not_found":
            stats["openalex_not_found"] += 1
        elif openalex_record["status"] == "error":
            stats["openalex_failed"] += 1

    return openalex_records


def process_assessment_results(assessment_results, stats, new_seen, all_assessments):
    candidates = []

    for result in assessment_results:
        paper = result["paper"]
        paper_id = paper["id"]
        title = paper["title"]
        abstract = paper["abstract"]
        paper_link = paper["link"]
        authors = paper["authors"]
        paper_tag = paper["paper_tag"]
        openalex_record = paper.get("openalex", {})
        assessment = result["assessment"]

        if result["error"]:
            stats["assessment_failed"] += 1
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
            "authors_display": paper.get("authors_display", format_authors_for_email(authors)),
            "openalex": openalex_record,
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
        candidates.append(
            {
                "id": paper_id,
                "title": title,
                "link": paper_link,
                "abstract": abstract,
                "authors": authors,
                "authors_display": assessment_record["authors_display"],
                "openalex": openalex_record,
                "score": assessment["score"],
                "fit_area": assessment["fit_area"],
                "reason": assessment["reason"],
                "affiliation_signal": assessment["affiliation_signal"],
            }
        )

    return candidates


def summarize_ranked_candidates(ranked_candidates, config, stats):
    selected = []
    next_index = 0

    while next_index < len(ranked_candidates) and len(selected) < config["max_selected_papers"]:
        remaining = config["max_selected_papers"] - len(selected)
        batch_candidates = []

        while next_index < len(ranked_candidates) and len(batch_candidates) < remaining:
            rank = next_index + 1
            candidate = dict(ranked_candidates[next_index])
            candidate["rank"] = rank
            candidate["summary_tag"] = f"ranked-{rank:02d}-{slugify(candidate['title'])[:60]}"
            batch_candidates.append(candidate)
            next_index += 1

        summary_results = batch_summarize_papers(batch_candidates, config)
        for result in summary_results:
            candidate = result["candidate"]
            summary = result["summary"]
            if result["error"]:
                stats["summary_failed"] += 1
                continue

            if not summary:
                stats["summary_failed"] += 1
                continue

            selected_item = {
                "id": candidate["id"],
                "title": candidate["title"],
                "summary": summary["summary"],
                "translation": summary["translation"],
                "explanation": summary.get("explanation", ""),
                "link": candidate["link"],
                "authors": candidate["authors"],
                "authors_display": candidate["authors_display"],
                "openalex": candidate["openalex"],
                "score": candidate["score"],
                "fit_area": candidate["fit_area"],
                "reason": candidate["reason"],
                "affiliation_signal": candidate["affiliation_signal"],
            }
            selected.append(selected_item)
            stats["selected"] = len(selected)
            LOGGER.info(
                "Paper selected for final digest | rank=%d selected_count=%d score=%d title=%s",
                candidate["rank"],
                len(selected),
                candidate["score"],
                candidate["title"],
            )
            if len(selected) >= config["max_selected_papers"]:
                break

    if len(selected) >= config["max_selected_papers"]:
        LOGGER.info(
            "Reached top-N limit after summarization | max_selected=%d",
            config["max_selected_papers"],
        )

    return selected


def main():
    setup_logging()

    config = get_runtime_config()
    smtp_config = get_smtp_config()
    validate_runtime_config(config, smtp_config)
    log_runtime_config(config, smtp_config)

    stats = build_stats()

    LOGGER.info("Pipeline started")

    seen = load_seen()
    openalex_cache = load_openalex_cache()
    new_seen = set(seen)
    all_assessments = []

    papers, target_date, pages_fetched = fetch_papers(config)
    stats["fetched_target_day"] = len(papers)
    stats["pages_fetched"] = pages_fetched

    pending_papers = prepare_pending_papers(
        papers,
        seen,
        stats,
        new_seen,
        all_assessments,
    )
    openalex_records = apply_openalex_enrichment(
        pending_papers,
        config,
        openalex_cache,
        stats,
    )
    assessment_results = batch_assess_papers(pending_papers, config)
    candidates = process_assessment_results(
        assessment_results,
        stats,
        new_seen,
        all_assessments,
    )

    ranked_candidates = sorted(
        candidates,
        key=lambda item: (-item["score"], item["title"].lower()),
    )
    assessments_path = write_json_artifact("paper_assessments.json", all_assessments)
    openalex_path = write_json_artifact("openalex_enrichment.json", openalex_records)
    ranked_path = write_json_artifact("ranked_candidates.json", ranked_candidates)
    LOGGER.info(
        "Assessment artifacts written | assessments_path=%s openalex_path=%s ranked_path=%s relevant_candidates=%d",
        assessments_path,
        openalex_path,
        ranked_path,
        len(ranked_candidates),
    )

    selected = summarize_ranked_candidates(ranked_candidates, config, stats)
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

    save_openalex_cache(openalex_cache)

    summary_payload = {
        "dry_run": config["dry_run"],
        "target_date": target_date.isoformat(),
        "local_timezone": config["local_timezone"],
        "stats": stats,
        "selected_titles": [paper["title"] for paper in selected],
        "selected_ids": [paper["id"] for paper in selected],
        "log_dir": str(get_run_dir()) if get_run_dir() else None,
        "email_preview_path": str(email_preview_path) if email_preview_path else None,
        "openalex_enrichment_path": str(openalex_path) if openalex_path else None,
    }
    summary_path = write_json_artifact("pipeline_summary.json", summary_payload)
    LOGGER.info("Pipeline finished successfully | summary_path=%s", summary_path)
