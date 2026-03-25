import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from digest_runtime import LOGGER, get_client, write_text_artifact
from digest_sources import format_authors_for_prompt
from prompts import ASSESS_PROMPT, SUMMARY_PROMPT, SYSTEM_PROMPT


def build_extra_body(config):
    extra_body = {
        "enable_thinking": config["llm_enable_thinking"],
    }
    if config["llm_thinking_budget"] is not None:
        extra_body["thinking_budget"] = config["llm_thinking_budget"]
    return extra_body


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


def batch_assess_papers(papers, config):
    if not papers:
        LOGGER.info("LLM assessment batch skipped | papers=0")
        return []

    worker_count = min(config["llm_assess_max_workers"], len(papers))
    LOGGER.info(
        "LLM assessment batch started | papers=%d workers=%d",
        len(papers),
        worker_count,
    )

    results = [None] * len(papers)

    def build_result(index, assessment=None, error=None):
        return {
            "paper": papers[index],
            "assessment": assessment,
            "error": error,
        }

    if worker_count == 1:
        for index, paper in enumerate(papers):
            try:
                assessment = assess_paper(
                    paper["title"],
                    paper["abstract"],
                    paper["authors"],
                    paper["paper_tag"],
                    config,
                )
                results[index] = build_result(index, assessment=assessment)
            except Exception as exc:
                LOGGER.error(
                    "Assessment crashed | paper=%s id=%s error=%s",
                    paper["paper_tag"],
                    paper["id"],
                    exc,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                results[index] = build_result(index, error=str(exc))
        LOGGER.info(
            "LLM assessment batch finished | papers=%d workers=%d",
            len(papers),
            worker_count,
        )
        return results

    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="llm-assess") as executor:
        future_map = {
            executor.submit(
                assess_paper,
                paper["title"],
                paper["abstract"],
                paper["authors"],
                paper["paper_tag"],
                config,
            ): index
            for index, paper in enumerate(papers)
        }
        for future in as_completed(future_map):
            index = future_map[future]
            paper = papers[index]
            try:
                assessment = future.result()
                results[index] = build_result(index, assessment=assessment)
            except Exception as exc:
                LOGGER.error(
                    "Assessment crashed | paper=%s id=%s error=%s",
                    paper["paper_tag"],
                    paper["id"],
                    exc,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                results[index] = build_result(index, error=str(exc))

    LOGGER.info(
        "LLM assessment batch finished | papers=%d workers=%d",
        len(papers),
        worker_count,
    )
    return results


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


def batch_summarize_papers(candidates, config):
    if not candidates:
        LOGGER.info("LLM summary batch skipped | papers=0")
        return []

    worker_count = min(config["llm_summary_max_workers"], len(candidates))
    LOGGER.info(
        "LLM summary batch started | papers=%d workers=%d",
        len(candidates),
        worker_count,
    )

    results = [None] * len(candidates)

    def build_result(index, summary=None, error=None):
        return {
            "candidate": candidates[index],
            "summary": summary,
            "error": error,
        }

    if worker_count == 1:
        for index, candidate in enumerate(candidates):
            try:
                summary = summarize(
                    candidate["title"],
                    candidate["abstract"],
                    candidate["summary_tag"],
                    config,
                )
                results[index] = build_result(index, summary=summary)
            except Exception as exc:
                LOGGER.error(
                    "Summary crashed | paper=%s id=%s error=%s",
                    candidate["summary_tag"],
                    candidate["id"],
                    exc,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                results[index] = build_result(index, error=str(exc))
        LOGGER.info(
            "LLM summary batch finished | papers=%d workers=%d",
            len(candidates),
            worker_count,
        )
        return results

    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="llm-summary") as executor:
        future_map = {
            executor.submit(
                summarize,
                candidate["title"],
                candidate["abstract"],
                candidate["summary_tag"],
                config,
            ): index
            for index, candidate in enumerate(candidates)
        }
        for future in as_completed(future_map):
            index = future_map[future]
            candidate = candidates[index]
            try:
                summary = future.result()
                results[index] = build_result(index, summary=summary)
            except Exception as exc:
                LOGGER.error(
                    "Summary crashed | paper=%s id=%s error=%s",
                    candidate["summary_tag"],
                    candidate["id"],
                    exc,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                results[index] = build_result(index, error=str(exc))

    LOGGER.info(
        "LLM summary batch finished | papers=%d workers=%d",
        len(candidates),
        worker_count,
    )
    return results
