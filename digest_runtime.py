import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path

from openai import OpenAI

DASHSCOPE_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_LLM_MODEL = "qwen3.5-plus"
DEFAULT_LOG_DIR = "logs"

LOGGER = logging.getLogger("arxiv_digest")
RUN_DIR = None
CLIENT = None


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


def get_run_dir():
    return RUN_DIR


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


def mask_value(value):
    if not value:
        return "<unset>"
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-4:]}"


def get_client():
    global CLIENT

    if CLIENT is None:
        CLIENT = OpenAI(
            api_key=os.getenv("DASHSCOPE_API_KEY"),
            base_url=DASHSCOPE_BASE_URL,
        )

    return CLIENT
