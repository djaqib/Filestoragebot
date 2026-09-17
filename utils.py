import os
import re
import logging
from typing import Dict, List, Set, Optional, Tuple
from telegram import Update, Message
from telegram.error import TelegramError

logger = logging.getLogger(__name__)

ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
DEFAULT_COLLECTION = "default"
GET_BATCH_SIZE = 10
NEARDUPES_PAIRS_PER_PAGE = 5
NEARDUP_ALBUM_DELAY = 1.0
SAVE_SUMMARY_DEBOUNCE_SECONDS = 2.5

# Shared Global State
active_collections: Dict[int, List[str]] = {}
paused_chats: Set[int] = set()
removing_chats: Set[int] = set()
min_video_length: Dict[int, int] = {}
_active_tasks: Dict[int, object] = {}
_get_sessions: Dict[int, Dict] = {}
_save_counts: Dict[int, Dict] = {}
_save_notify_tasks: Dict[int, object] = {}
_neardup_sessions: Dict[str, Tuple[List[Tuple[Tuple[str, str, int, int], Tuple[str, str, int, int]]], str, int]] = {}


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


def get_active_collections(chat_id: int) -> List[str]:
    return active_collections.get(chat_id, [DEFAULT_COLLECTION])


def under_clause(name: str) -> Tuple[str, Tuple[str, str]]:
    return "(collection = %s OR collection LIKE %s)", (name, f"{name}/%")


def is_video_document(msg: Message) -> bool:
    if not msg.document:
        return False
    mime = msg.document.mime_type or ""
    name = msg.document.file_name or ""
    return mime.startswith("video/") or name.lower().endswith(
        (".mp4", ".mkv", ".mov", ".avi", ".webm", ".flv", ".wmv", ".m4v")
    )


def parse_arrow_pair(args: List[str]) -> Optional[Tuple[str, str]]:
    text = " ".join(args).strip()
    if "->" in text:
        parts = text.split("->", 1)
        src = normalize_name(parts[0])
        dest = normalize_name(parts[1])
        if src and dest:
            return src, dest
    return None


async def reply_db_error(update: Update, action_desc: str, err: Exception):
    err_str = str(err).lower()
    if "connection" in err_str or "timeout" in err_str or "closed" in err_str:
        user_msg = f"⚠️ Database connection issue while trying to {action_desc}. Please try again in a moment."
    else:
        user_msg = f"⚠️ Database error while trying to {action_desc}."
    logger.exception("DB Exception during: %s", action_desc)

    try:
        if update.effective_message:
            await update.effective_message.reply_text(user_msg)
        elif update.callback_query:
            await update.callback_query.message.reply_text(user_msg)
    except TelegramError:
        pass
