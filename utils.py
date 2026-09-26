"""
utils.py - Pure helpers, configuration, and constants for the video collection bot.

No Telegram or database calls happen in this module - everything here is a
stateless function or a constant, safe to import from anywhere without risk
of circular imports.
"""
import logging
import os
import re
import shlex
from typing import Dict, List, Optional, Tuple

from telegram import Message

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Environment / Configuration
# ----------------------------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "default_secret")
PORT = int(os.getenv("PORT", "8080"))
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))

if not BOT_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_URL:
    logger.error("Missing required environment variables.")

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------
DEFAULT_COLLECTION = "default"
GET_BATCH_SIZE = 10
GET_PAGINATION_LIMIT = 5
GET_PAGE_TIMEOUT = 120
GET_MAX_BATCH_PAGES = 3
GET_ALBUM_SEND_DELAY = 1.5
NEARDUPES_PAIRS_PER_PAGE = 5
NEARDUP_ALBUM_DELAY = 1.0
SAVE_SUMMARY_DEBOUNCE_SECONDS = 2.5
SAVE_PROGRESS_INTERVAL = 20

NEAR_DUP_DURATION_TOLERANCE_SECONDS = 1
NEAR_DUP_SIZE_TOLERANCE_FRACTION = 0.015
NEAR_DUP_SIZE_ONLY_TOLERANCE_FRACTION = 0.008

# ----------------------------------------------------------------------
# Path & Validation Utilities
# ----------------------------------------------------------------------
def normalize_name(name: str) -> str:
    cleaned = name.strip().lower()
    cleaned = re.sub(r"[^\w\s/-]", "", cleaned)
    parts = [p.strip() for p in cleaned.split("/") if p.strip()]
    return "/".join(parts) if parts else DEFAULT_COLLECTION

def validate_collection_path(name: str) -> Optional[str]:
    if not name:
        return "empty"
    parts = name.split("/")
    if len(parts) > 5:
        return "too_deep"
    for p in parts:
        if not p or len(p) > 30:
            return "invalid_segment"
    return None

def describe_path_error(err_code: str) -> str:
    if err_code == "too_deep":
        return "Folder depth max level is 5."
    if err_code == "invalid_segment":
        return "Folder names must be 1-30 characters long."
    return "Invalid path format."

def _is_video_document(msg: Message) -> bool:
    if not msg.document:
        return False
    mime = msg.document.mime_type or ""
    name = msg.document.file_name or ""
    return mime.startswith("video/") or name.lower().endswith(
        (".mp4", ".mkv", ".mov", ".avi", ".webm", ".flv", ".wmv", ".m4v")
    )

def _parse_arrow_pair(args: List[str]) -> Optional[Tuple[str, str]]:
    text = " ".join(args).strip()
    if "->" in text:
        parts = text.split("->", 1)
        src = normalize_name(parts[0])
        dest = normalize_name(parts[1])
        if src and dest:
            return src, dest
    return None

# ----------------------------------------------------------------------
# /get sort-flag parsing
# ----------------------------------------------------------------------
GET_SORT_MODES = {
    "longest": "duration DESC NULLS LAST, added_at",
    "shortest": "duration ASC NULLS LAST, added_at",
    "largest": "file_size DESC NULLS LAST, added_at",
    "smallest": "file_size ASC NULLS LAST, added_at",
}

def _parse_get_args(raw_text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse the text after /get. Returns (name_or_None, sort_mode_or_None, error_or_None)."""
    parts = raw_text.split(maxsplit=1)
    remainder = parts[1] if len(parts) > 1 else ""
    try:
        tokens = shlex.split(remainder)
    except ValueError:
        return None, None, "Couldn't parse that — check your quotes."

    sort_mode = None
    name_tokens = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok.lower() == "--sort":
            if i + 1 >= len(tokens):
                return None, None, "Missing a value after --sort (longest/shortest/largest/smallest)."
            val = tokens[i + 1].lower()
            if val not in GET_SORT_MODES:
                return None, None, f"Unknown sort '{val}'. Use: longest, shortest, largest, or smallest."
            sort_mode = val
            i += 2
        else:
            name_tokens.append(tok)
            i += 1

    name_str = " ".join(name_tokens) if name_tokens else None
    return name_str, sort_mode, None

# ----------------------------------------------------------------------
# /search filter parsing & formatting
# ----------------------------------------------------------------------
SEARCH_FLAG_MAP = {
    "--min-duration": "min_duration",
    "--max-duration": "max_duration",
    "--min-size": "min_size_mb",
    "--max-size": "max_size_mb",
}
SEARCH_RESULT_LIMIT = 20

def _escape_ilike(s: str) -> str:
    return s.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")

def _parse_search_args(raw_text: str) -> Tuple[Optional[str], Dict[str, float], Optional[str]]:
    """Parse the text after /search. Returns (query_str_or_None, filters, error_or_None).
    Supports quoted phrases ("long video") and --min-duration/--max-duration/--min-size/--max-size,
    in any order relative to the keyword text."""
    parts = raw_text.split(maxsplit=1)
    remainder = parts[1] if len(parts) > 1 else ""
    try:
        tokens = shlex.split(remainder)
    except ValueError:
        return None, {}, "Couldn't parse that — check your quotes."

    filters: Dict[str, float] = {}
    query_tokens = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        key = SEARCH_FLAG_MAP.get(tok.lower())
        if key:
            if i + 1 >= len(tokens):
                return None, {}, f"Missing a number after {tok}."
            val_raw = tokens[i + 1]
            try:
                val = float(val_raw)
            except ValueError:
                return None, {}, f"{tok} needs a number, got '{val_raw}'."
            filters[key] = val
            i += 2
        else:
            query_tokens.append(tok)
            i += 1

    query_str = " ".join(query_tokens) if query_tokens else None
    return query_str, filters, None

def _format_duration(seconds: Optional[int]) -> str:
    if not seconds:
        return "?:??"
    m, s = divmod(int(seconds), 60)
    return f"{m}:{s:02d}"

def _format_size(num_bytes: Optional[int]) -> str:
    if not num_bytes:
        return "?MB"
    return f"{num_bytes / (1024 * 1024):.0f}MB"
