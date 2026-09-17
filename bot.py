import asyncio
import io
import json
import logging
import os
import re
import shlex
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route
import uvicorn

from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaVideo,
    Message,
    Update,
)
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    TypeHandler,
    filters,
)

# ----------------------------------------------------------------------
# Logging & Environment Setup
# ----------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "default_secret")
PORT = int(os.getenv("PORT", "8080"))
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))

if not BOT_TOKEN or not DATABASE_URL or not RENDER_EXTERNAL_URL:
    logger.error("Missing required environment variables.")

# Constants
DEFAULT_COLLECTION = "default"
GET_BATCH_SIZE = 10
GET_PAGINATION_LIMIT = 5
GET_PAGE_TIMEOUT = 120
GET_MAX_BATCH_PAGES = 3
GET_ALBUM_SEND_DELAY = 1.5
NEARDUPES_PAIRS_PER_PAGE = 5
NEARDUP_ALBUM_DELAY = 1.0
SAVE_SUMMARY_DEBOUNCE_SECONDS = 2.5

NEAR_DUP_DURATION_TOLERANCE_SECONDS = 2
NEAR_DUP_SIZE_TOLERANCE_FRACTION = 0.05
NEAR_DUP_SIZE_ONLY_TOLERANCE_FRACTION = 0.02

# State Management
active_collections: Dict[int, List[str]] = {}
paused_chats: Set[int] = set()
removing_chats: Set[int] = set()
min_video_length: Dict[int, int] = {}
_active_tasks: Dict[int, asyncio.Task] = {}
_get_sessions: Dict[int, Tuple[List[Tuple[str, str]], str, int, Message]] = {}
_get_batch_pages: Dict[int, int] = {}
_awaiting_page_jump: Set[int] = set()
_search_sessions: Dict[int, List[Tuple[str, str, str, str, Optional[int], Optional[int]]]] = {}
_save_counts: Dict[int, Dict] = {}
_save_notify_tasks: Dict[int, asyncio.Task] = {}

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

def get_active_collections(chat_id: int) -> List[str]:
    return active_collections.get(chat_id, [DEFAULT_COLLECTION])

def _under_clause(name: str) -> Tuple[str, Tuple[str, str]]:
    """SQL fragment + params matching a collection or anything nested under it
    (e.g. 'movies' also matches 'movies/action'). Always use this instead of
    hand-writing a 'collection LIKE ... /%' clause: building the '/%' pattern
    directly into the SQL text is what caused the repeated escaping bug,
    since psycopg2 scans the whole query string for '%'. Passing the pattern
    as a bound parameter instead avoids that entirely.
    Usage: clause, params = _under_clause(name); cur.execute(f"... WHERE {clause}", params)
    """
    return "(collection = %s OR collection LIKE %s)", (name, f"{name}/%")

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
# Database Operations
# ----------------------------------------------------------------------
def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def _db_call(fn):
    conn = get_db_connection()
    try:
        res = fn(conn)
        conn.commit()
        return res
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

async def db_run(fn):
    return await asyncio.to_thread(_db_call, fn)

def init_db():
    def _schema(conn):
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS videos (
                    id SERIAL PRIMARY KEY,
                    collection TEXT NOT NULL,
                    file_id TEXT NOT NULL,
                    file_unique_id TEXT NOT NULL,
                    duration INTEGER,
                    file_size BIGINT,
                    file_name TEXT,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(collection, file_unique_id)
                );
                CREATE INDEX IF NOT EXISTS idx_videos_col ON videos(collection);
                CREATE INDEX IF NOT EXISTS idx_videos_fuid ON videos(file_unique_id);

                CREATE TABLE IF NOT EXISTS sent_videos (
                    chat_id BIGINT NOT NULL,
                    collection TEXT NOT NULL,
                    file_unique_id TEXT NOT NULL,
                    sent_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(chat_id, collection, file_unique_id)
                );

                CREATE TABLE IF NOT EXISTS dead_files (
                    file_unique_id TEXT PRIMARY KEY,
                    detected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS collection_settings (
                    collection TEXT PRIMARY KEY,
                    expiry_days INTEGER DEFAULT 0
                );
                """
            )
    _db_call(_schema)
    logger.info("Database schema initialized.")

# ----------------------------------------------------------------------
# Access Control
# ----------------------------------------------------------------------
async def access_control(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ADMIN_USER_ID and update.effective_user:
        if update.effective_user.id != ADMIN_USER_ID:
            if update.effective_message:
                await update.effective_message.reply_text("⛔ Unauthorized access.")
            elif update.callback_query:
                await update.callback_query.answer("⛔ Unauthorized.", show_alert=True)
            return False
    return True

async def admin_check(update: Update) -> bool:
    if ADMIN_USER_ID and update.effective_user and update.effective_user.id != ADMIN_USER_ID:
        if update.effective_message:
            await update.effective_message.reply_text("⛔ Admin rights required.")
        return False
    return True

# ----------------------------------------------------------------------
# Video Ingestion & Handling
# ----------------------------------------------------------------------
async def _save_video_to_db(collection: str, file_id: str, file_unique_id: str, duration: Optional[int], file_size: Optional[int], file_name: Optional[str]) -> bool:
    def _insert(conn):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO videos (collection, file_id, file_unique_id, duration, file_size, file_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (collection, file_unique_id) DO NOTHING
                """,
                (collection, file_id, file_unique_id, duration, file_size, file_name),
            )
            return cur.rowcount > 0
    return await db_run(_insert)

async def _delete_video_from_collection(collection: str, file_unique_id: str) -> bool:
    def _delete(conn):
        with conn.cursor() as cur:
            cur.execute("DELETE FROM videos WHERE collection = %s AND file_unique_id = %s", (collection, file_unique_id))
            return cur.rowcount > 0
    return await db_run(_delete)

async def _flush_save_summary(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    try:
        await asyncio.sleep(SAVE_SUMMARY_DEBOUNCE_SECONDS)
    except asyncio.CancelledError:
        return
    stats = _save_counts.pop(chat_id, None)
    _save_notify_tasks.pop(chat_id, None)
    if not stats:
        return

    parts = []
    if stats["saved"]:
        parts.append(f"✅ Saved {stats['saved']} video(s)")
    if stats["removed"]:
        parts.append(f"🗑️ Removed {stats['removed']} video(s)")
    if stats["skipped"]:
        parts.append(f"↩️ Skipped {stats['skipped']} duplicate(s)")
    if not parts:
        return

    cols = ", ".join(f"`{c}`" for c in sorted(stats["cols"]))
    text = " · ".join(parts) + (f" — {cols}" if cols else "")
    try:
        await context.bot.send_message(chat_id, text, parse_mode="Markdown")
    except TelegramError:
        pass

def _record_activity(chat_id: int, collection: str, kind: str, context: ContextTypes.DEFAULT_TYPE):
    stats = _save_counts.setdefault(chat_id, {"saved": 0, "skipped": 0, "removed": 0, "cols": set()})
    stats[kind] += 1
    stats["cols"].add(collection)

    existing = _save_notify_tasks.get(chat_id)
    if existing and not existing.done():
        existing.cancel()
    _save_notify_tasks[chat_id] = asyncio.create_task(_flush_save_summary(chat_id, context))

async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in paused_chats:
        return

    video = update.message.video
    if not video:
        return

    min_len = min_video_length.get(chat_id)
    if min_len is not None and video.duration is not None and video.duration < min_len:
        return

    collections = get_active_collections(chat_id)
    is_remove = chat_id in removing_chats

    for col in collections:
        if is_remove:
            removed = await _delete_video_from_collection(col, video.file_unique_id)
            _record_activity(chat_id, col, "removed" if removed else "skipped", context)
        else:
            saved = await _save_video_to_db(col, video.file_id, video.file_unique_id, video.duration, video.file_size, getattr(video, "file_name", None))
            _record_activity(chat_id, col, "saved" if saved else "skipped", context)

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in paused_chats:
        return

    msg = update.message
    if not _is_video_document(msg):
        return

    doc = msg.document
    collections = get_active_collections(chat_id)
    is_remove = chat_id in removing_chats

    for col in collections:
        if is_remove:
            removed = await _delete_video_from_collection(col, doc.file_unique_id)
            _record_activity(chat_id, col, "removed" if removed else "skipped", context)
        else:
            saved = await _save_video_to_db(col, doc.file_id, doc.file_unique_id, None, doc.file_size, doc.file_name)
            _record_activity(chat_id, col, "saved" if saved else "skipped", context)

async def handle_non_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pass

# ----------------------------------------------------------------------
# Helper Video Sending Routines
# ----------------------------------------------------------------------
async def _send_single_video_with_fallback(chat_id: int, file_id: str, file_unique_id: str, caption: str, context: ContextTypes.DEFAULT_TYPE, collection: str) -> Optional[Message]:
    try:
        return await context.bot.send_video(chat_id=chat_id, video=file_id, caption=caption)
    except TelegramError as e:
        err_str = str(e).lower()
        if "wrong remote file identifier" in err_str or "file reference" in err_str or "not found" in err_str:
            def _mark_dead(conn):
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO dead_files (file_unique_id) VALUES (%s) ON CONFLICT DO NOTHING", (file_unique_id,))
            await db_run(_mark_dead)
        return None

# ----------------------------------------------------------------------
# Navigation & Commands
# ----------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Welcome to Video Collector Bot!\n\n"
        "Forward videos here to automatically save them to your active collection.\n"
        "Use /menu to browse collections or /help to view all available commands."
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📚 *Bot Commands Reference*\n\n"
        "📌 *Basic Commands*\n"
        "• /menu - Interactive collection menu\n"
        "• /collect <name> - Set active collection\n"
        "• /fav - Quick access to 'favorites'\n"
        "• /current - View active collection\n"
        "• /finish - Reset to default collection\n"
        "• /stop - Stop tasks and pause saving\n\n"
        "📦 *Collection Operations*\n"
        "• /get [name] - Retrieve videos\n"
        "• /list - Browse all collections\n"
        "• /random [name] - Send random video\n"
        "• /status - Show active collection stats\n"
        "• /info <name> - Detailed storage info\n"
        "• /delete <name> - Delete collection\n"
        "• /rename <old> -> <new> - Rename collection\n"
        "• /move <src> -> <dest> - Move videos\n"
        "• /copy <src> -> <dest> - Copy videos\n"
        "• /merge <src> -> <dest> - Merge collections\n\n"
        "🔍 *Search*\n"
        "• `/search <query>` - keyword search by filename\n"
        "• Use quotes for a multi-word phrase: `/search \"long video\"`\n"
        "• Filters can be combined with a query, or used alone:\n"
        "   ◦ `--min-duration <sec>` - minimum length in seconds\n"
        "   ◦ `--max-duration <sec>` - maximum length in seconds\n"
        "   ◦ `--min-size <MB>` - minimum file size in MB\n"
        "   ◦ `--max-size <MB>` - maximum file size in MB\n"
        "• Examples:\n"
        "   ◦ `/search mix --max-duration 300`\n"
        "   ◦ `/search --min-size 10 --max-size 100`\n"
        "   ◦ `/search \"long video\" --min-duration 600 --max-size 500`\n"
        "• Tap any result to receive that video.\n\n"
        "🧹 *Other Utilities*\n"
        "• /dups <name> - Find exact duplicates\n"
        "• /neardupes <name> - Visual near-duplicate cleanup\n"
        "• /removemode on|off - Toggle auto-delete mode\n"
        "• /minlength <sec> - Filter short videos"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _show_main_menu(update.effective_chat.id, context, edit_message=None)

async def _show_main_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE, edit_message: Optional[Message] = None):
    try:
        def _get_folders(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT collection FROM videos")
                rows = cur.fetchall()
                cols = [r[0] for r in rows]
                top_folders = set()
                for c in cols:
                    top_folders.add(c.split("/")[0])
                return sorted(list(top_folders))
        folders = await db_run(_get_folders)
    except Exception as e:
        logger.exception("Error loading main menu")
        folders = []

    folder_buttons = [InlineKeyboardButton(f"📁 {f}", callback_data=f"menufolder:{f}") for f in folders]
    keyboard = [folder_buttons[i:i + 2] for i in range(0, len(folder_buttons), 2)]
    keyboard.append([InlineKeyboardButton("⚙️ Settings", callback_data="menu_settings")])

    text = "📁 *Main Menu*\nSelect a folder to browse:"
    if edit_message:
        await edit_message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "menu_settings":
        await settings_command(update, context)

async def menu_folder_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    folder_prefix = query.data[len("menufolder:"):]

    try:
        def _get_sub_items(conn):
            with conn.cursor() as cur:
                clause, params = _under_clause(folder_prefix)
                cur.execute(
                    f"SELECT DISTINCT collection FROM videos WHERE {clause}",
                    params,
                )
                cols = [r[0] for r in cur.fetchall()]
                subfolders = set()
                exact_match = False
                for c in cols:
                    if c == folder_prefix:
                        exact_match = True
                    else:
                        rel = c[len(folder_prefix) + 1:]
                        subfolders.add(rel.split("/")[0])
                return sorted(list(subfolders)), exact_match
        subfolders, exact_match = await db_run(_get_sub_items)
    except Exception as e:
        await reply_db_error(update, f"fetch items for '{folder_prefix}'", e)
        return

    keyboard = []
    if exact_match:
        keyboard.append([InlineKeyboardButton("📄 View exact collection", callback_data=f"menuview:{folder_prefix}")])

    for sf in subfolders:
        full_path = f"{folder_prefix}/{sf}"
        keyboard.append([InlineKeyboardButton(f"📁 {sf}", callback_data=f"menufolder:{full_path}")])

    keyboard.append([
        InlineKeyboardButton("📂 Set Active", callback_data=f"menuset:{folder_prefix}"),
        InlineKeyboardButton("📩 Get All", callback_data=f"menugetall:{folder_prefix}"),
    ])
    keyboard.append([
        InlineKeyboardButton("🎲 Random All", callback_data=f"menurandall:{folder_prefix}"),
        InlineKeyboardButton("🗑️ Delete", callback_data=f"listdelete:{folder_prefix}"),
    ])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu_back")])

    await query.edit_message_text(
        f"📁 Folder: `{folder_prefix}`",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )

async def menu_view_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("menuview:"):]

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📂 Set Active", callback_data=f"menuset:{name}"),
            InlineKeyboardButton("🎲 Random Video", callback_data=f"menurandom:{name}"),
        ],
        [
            InlineKeyboardButton("🗑️ Delete Folder/Collection", callback_data=f"listdelete:{name}"),
        ],
        [
            InlineKeyboardButton("⬅️ Back to menu", callback_data="menu_back"),
        ],
    ])

    await query.edit_message_text(
        f"📁 Collection: `{name}`\nSelect an action:",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )

async def menu_get_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("menugetall:"):]
    await query.edit_message_text(f"Fetching videos from `{name}`...", parse_mode="Markdown")
    context.args = [name]
    await get_collection(update, context)

async def menu_rand_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("menurandall:"):]
    context.args = [name]
    await random_video(update, context)

async def menu_set_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("menuset:"):]
    chat_id = update.effective_chat.id
    active_collections[chat_id] = [name]
    await query.edit_message_text(f"✅ Active collection set to `{name}`", parse_mode="Markdown")

async def menu_random_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("menurandom:"):]
    context.args = [name]
    await random_video(update, context)

async def menu_back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await _show_main_menu(update.effective_chat.id, context, edit_message=query.message)

# ----------------------------------------------------------------------
# List Collections & UI Navigation
# ----------------------------------------------------------------------
async def list_collections(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        def _fetch(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT collection FROM videos ORDER BY collection")
                return [r[0] for r in cur.fetchall()]
        cols = await db_run(_fetch)
    except Exception as e:
        await reply_db_error(update, "list collections", e)
        return

    if not cols:
        await update.message.reply_text("No collections found.")
        return

    top_folders = sorted(list({c.split("/")[0] for c in cols}))
    folder_buttons = [
        InlineKeyboardButton(f"📁 {f}", callback_data=f"listfolder:{f}")
        for f in top_folders
    ]
    keyboard = [folder_buttons[i:i + 2] for i in range(0, len(folder_buttons), 2)]

    await update.message.reply_text(
        "📁 *Collections Hierarchy*\nSelect a folder to inspect:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )

async def list_folder_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    folder = query.data[len("listfolder:"):]

    try:
        def _fetch_sub(conn):
            with conn.cursor() as cur:
                clause, params = _under_clause(folder)
                cur.execute(
                    f"SELECT DISTINCT collection FROM videos WHERE {clause}",
                    params,
                )
                all_cols = [r[0] for r in cur.fetchall()]
                subitems = set()
                exact = False
                for c in all_cols:
                    if c == folder:
                        exact = True
                    else:
                        sub = c[len(folder) + 1:].split("/")[0]
                        subitems.add(f"{folder}/{sub}")
                return sorted(list(subitems)), exact
        subs, exact = await db_run(_fetch_sub)
    except Exception as e:
        await reply_db_error(update, "expand folder", e)
        return

    keyboard = []

    # Build folder buttons
    folder_buttons = [
        InlineKeyboardButton(f"📁 {s}", callback_data=f"listfolder:{s}")
        for s in subs
    ]
    
    # Grid layout: group subfolders into rows of 2 buttons each
    for i in range(0, len(folder_buttons), 2):
        keyboard.append(folder_buttons[i:i + 2])

    # Action buttons
    keyboard.append([
        InlineKeyboardButton("📂 Set Active", callback_data=f"listset:{folder}"),
        InlineKeyboardButton("📩 Get Videos", callback_data=f"listget:{folder}"),
    ])
    keyboard.append([
        InlineKeyboardButton("🎲 Random", callback_data=f"listrandom:{folder}"),
        InlineKeyboardButton("🗑️ Delete", callback_data=f"listdelete:{folder}"),
    ])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu_back")])

    await query.edit_message_text(
        f"📁 Location: `{folder}`",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


async def list_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("listdelete:"):]

    try:
        def _count(conn):
            with conn.cursor() as cur:
                clause, params = _under_clause(name)
                cur.execute(
                    f"SELECT COUNT(*) FROM videos WHERE {clause}",
                    params,
                )
                return cur.fetchone()[0]

        total = await db_run(_count)
    except Exception as e:
        await reply_db_error(update, f"check '{name}'", e)
        return

    keyboard = [
        [
            InlineKeyboardButton("❌ Yes, Delete", callback_data=f"confirmdelete:{name}"),
            InlineKeyboardButton("⬅️ Cancel", callback_data=f"listfolder:{name}"),
        ]
    ]

    await query.edit_message_text(
        f"⚠️ Are you sure you want to delete **{name}**?\n"
        f"This will delete **{total}** item(s).",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


async def list_set_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("listset:"):]
    chat_id = update.effective_chat.id
    active_collections[chat_id] = [name]
    await query.edit_message_text(f"✅ Active collection set to `{name}`.", parse_mode="Markdown")

async def list_get_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("listget:"):]
    context.args = [name]
    await get_collection(update, context)

async def list_random_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("listrandom:"):]
    context.args = [name]
    await random_video(update, context)

# ----------------------------------------------------------------------
# Retrieving Videos (Get / Pagination / Random / Search / Find)
# ----------------------------------------------------------------------
async def get_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    name = normalize_name(" ".join(context.args)) if context.args else get_active_collections(chat_id)[0]

    try:
        def _fetch(conn):
            with conn.cursor() as cur:
                clause, params = _under_clause(name)
                cur.execute(f"SELECT file_id, file_unique_id FROM videos WHERE {clause} ORDER BY added_at", params)
                return cur.fetchall()
        rows = await db_run(_fetch)
    except Exception as e:
        await reply_db_error(update, f"get '{name}'", e)
        return

    if not rows:
        msg = f"No videos found in `{name}`."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, parse_mode="Markdown")
        else:
            await update.message.reply_text(msg, parse_mode="Markdown")
        return

    file_rows = [(r[0], r[1]) for r in rows]
    total = len(file_rows)
    pages = (total + GET_BATCH_SIZE - 1) // GET_BATCH_SIZE

    session_msg = await context.bot.send_message(
        chat_id,
        f"📦 Preparing to send {total} video(s) from `{name}` in pages...",
        parse_mode="Markdown",
    )

    _get_sessions[chat_id] = (file_rows, name, 1, session_msg)
    await _render_get_page(chat_id, context)

async def _send_pages(chat_id: int, file_rows: List[Tuple[str, str]], name: str, start_page: int, end_page: int, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Send pages [start_page, end_page] (inclusive, 1-indexed), one album per page.
    Falls back to individual sends (marking dead files) if an album fails.
    A short delay between page-albums keeps multi-page taps flood-safe.
    Returns the number of videos that failed to send."""
    failed = 0
    for pg in range(start_page, end_page + 1):
        s = (pg - 1) * GET_BATCH_SIZE
        e = min(s + GET_BATCH_SIZE, len(file_rows))
        batch = file_rows[s:e]
        if not batch:
            break

        sent_ok = True
        if len(batch) >= 2:
            # Telegram albums require 2-10 items; GET_BATCH_SIZE (10) fits this exactly.
            media = [InputMediaVideo(fid) for fid, _ in batch]
            try:
                await context.bot.send_media_group(chat_id, media)
            except TelegramError:
                sent_ok = False
        else:
            sent_ok = False  # single leftover video can't be an album

        if not sent_ok:
            for fid, funid in batch:
                result = await _send_single_video_with_fallback(chat_id, fid, funid, "", context, name)
                if result is None:
                    failed += 1
                await asyncio.sleep(0.5)

        if pg < end_page:
            await asyncio.sleep(GET_ALBUM_SEND_DELAY)
    return failed

async def _render_get_page(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    session = _get_sessions.get(chat_id)
    if not session:
        return

    file_rows, name, page, msg = session
    total = len(file_rows)
    total_pages = max(1, (total + GET_BATCH_SIZE - 1) // GET_BATCH_SIZE)
    page = min(max(1, page), total_pages)
    batch_pages = _get_batch_pages.get(chat_id, 1)

    end_page = min(page + batch_pages - 1, total_pages)
    start_idx = (page - 1) * GET_BATCH_SIZE
    end_idx = min(start_idx + (end_page - page + 1) * GET_BATCH_SIZE, total)

    page_label = f"Page {page}/{total_pages}" if end_page == page else f"Pages {page}-{end_page}/{total_pages}"
    text = (
        f"📦 *Collection:* `{name}`\n"
        f"{page_label} · Videos {start_idx + 1}-{end_idx} of {total}\n"
        f"Prev/Next send {batch_pages} page(s) (~{end_idx - start_idx} videos) per tap"
    )
    keyboard = [
        [
            InlineKeyboardButton("◀️ Prev", callback_data="getprev"),
            InlineKeyboardButton("🔢 Jump", callback_data="getjump"),
            InlineKeyboardButton("Next ▶️", callback_data="getnext"),
        ],
        [InlineKeyboardButton(f"📚 Pages/tap: {batch_pages}", callback_data="getbatch")],
        [InlineKeyboardButton("🛑 Cancel", callback_data="getcancel")],
    ]

    try:
        await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    except TelegramError:
        pass

async def _handle_get_nav(update: Update, context: ContextTypes.DEFAULT_TYPE, direction: str):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id

    session = _get_sessions.get(chat_id)
    if not session:
        await query.edit_message_text("Session expired.")
        return

    file_rows, name, page, msg = session
    total_pages = max(1, (len(file_rows) + GET_BATCH_SIZE - 1) // GET_BATCH_SIZE)
    batch_pages = _get_batch_pages.get(chat_id, 1)

    if direction == "next":
        if page > total_pages:
            await query.answer("Already at the end.", show_alert=True)
            return
        start_page = page
    else:
        start_page = max(1, page - 2 * batch_pages)
    end_page = min(start_page + batch_pages - 1, total_pages)

    try:
        await msg.edit_text(f"🚀 Sending page(s) {start_page}-{end_page}...", parse_mode="Markdown")
    except TelegramError:
        pass

    failed = await _send_pages(chat_id, file_rows, name, start_page, end_page, context)

    new_page = min(end_page + 1, total_pages)
    _get_sessions[chat_id] = (file_rows, name, new_page, msg)
    if failed:
        await context.bot.send_message(chat_id, f"⚠️ {failed} video(s) could not be sent (marked dead).")
    await _render_get_page(chat_id, context)

async def get_next_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _handle_get_nav(update, context, "next")

async def get_prev_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _handle_get_nav(update, context, "prev")

async def get_batch_toggle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = update.effective_chat.id
    current = _get_batch_pages.get(chat_id, 1)
    new_val = current + 1 if current < GET_MAX_BATCH_PAGES else 1
    _get_batch_pages[chat_id] = new_val
    await query.answer(f"Now sending {new_val} page(s) per tap.")
    await _render_get_page(chat_id, context)

async def get_jump_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id

    session = _get_sessions.get(chat_id)
    if not session:
        await query.edit_message_text("Session expired.")
        return

    file_rows, name, page, msg = session
    total_pages = max(1, (len(file_rows) + GET_BATCH_SIZE - 1) // GET_BATCH_SIZE)
    _awaiting_page_jump.add(chat_id)
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Cancel", callback_data="getcancel")]])
    try:
        await msg.edit_text(
            f"🔢 Send the page number to jump to (1-{total_pages}) as a message.",
            reply_markup=keyboard,
        )
    except TelegramError:
        pass

async def handle_get_page_jump_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in _awaiting_page_jump:
        return

    session = _get_sessions.get(chat_id)
    if not session:
        _awaiting_page_jump.discard(chat_id)
        return

    text = (update.message.text or "").strip()
    file_rows, name, page, msg = session
    total_pages = max(1, (len(file_rows) + GET_BATCH_SIZE - 1) // GET_BATCH_SIZE)

    if not text.isdigit() or not (1 <= int(text) <= total_pages):
        await update.message.reply_text(f"Please send a number between 1 and {total_pages}.")
        return

    target = int(text)
    _awaiting_page_jump.discard(chat_id)
    _get_sessions[chat_id] = (file_rows, name, target, msg)
    try:
        await update.message.delete()
    except TelegramError:
        pass
    await _render_get_page(chat_id, context)

async def get_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    _get_sessions.pop(chat_id, None)
    _awaiting_page_jump.discard(chat_id)
    await query.edit_message_text("Cancelled retrieval.")

async def random_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    name = normalize_name(" ".join(context.args)) if context.args else get_active_collections(chat_id)[0]

    try:
        def _fetch_rand(conn):
            with conn.cursor() as cur:
                clause, params = _under_clause(name)
                cur.execute(
                    f"SELECT file_id, file_unique_id FROM videos WHERE {clause} ORDER BY RANDOM() LIMIT 1",
                    params,
                )
                return cur.fetchone()
        res = await db_run(_fetch_rand)
    except Exception as e:
        await reply_db_error(update, "fetch random video", e)
        return

    if not res:
        await update.message.reply_text(f"No videos found in `{name}`.", parse_mode="Markdown")
        return

    fid, fuid = res
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🎲 Another Random", callback_data=f"random_next:{name}")]])
    await context.bot.send_video(chat_id, fid, caption=f"🎲 Random from `{name}`", reply_markup=kb, parse_mode="Markdown")

async def random_next_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("random_next:"):]
    context.args = [name]
    await random_video(update, context)

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

async def search_videos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw_text = update.message.text or ""
    query_str, filters, err = _parse_search_args(raw_text)
    if err:
        await update.message.reply_text(f"⚠️ {err}\nSee /help for /search syntax.")
        return
    if not query_str and not filters:
        await update.message.reply_text(
            "Usage: /search <query> [--min-duration s] [--max-duration s] [--min-size MB] [--max-size MB]\n"
            "See /help for the full list of filters and examples."
        )
        return

    where_clauses = []
    params: List = []
    if query_str:
        where_clauses.append("file_name ILIKE %s")
        params.append(f"%{_escape_ilike(query_str)}%")
    if "min_duration" in filters:
        where_clauses.append("duration >= %s")
        params.append(int(filters["min_duration"]))
    if "max_duration" in filters:
        where_clauses.append("duration <= %s")
        params.append(int(filters["max_duration"]))
    if "min_size_mb" in filters:
        where_clauses.append("file_size >= %s")
        params.append(int(filters["min_size_mb"] * 1024 * 1024))
    if "max_size_mb" in filters:
        where_clauses.append("file_size <= %s")
        params.append(int(filters["max_size_mb"] * 1024 * 1024))
    where_sql = " AND ".join(where_clauses)

    try:
        def _search(conn):
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM videos WHERE {where_sql}", params)
                total = cur.fetchone()[0]
                cur.execute(
                    f"""SELECT collection, file_id, file_unique_id, file_name, duration, file_size
                        FROM videos WHERE {where_sql}
                        ORDER BY added_at DESC LIMIT {SEARCH_RESULT_LIMIT}""",
                    params,
                )
                return total, cur.fetchall()
        total, rows = await db_run(_search)
    except Exception as e:
        await reply_db_error(update, "search videos", e)
        return

    if not rows:
        await update.message.reply_text("No videos matched those filters.")
        return

    chat_id = update.effective_chat.id
    results = [(fid, funid, col, fname, dur, size) for col, fid, funid, fname, dur, size in rows]
    _search_sessions[chat_id] = results

    keyboard = []
    for idx, (fid, funid, col, fname, dur, size) in enumerate(results):
        label = f"{fname or 'Unnamed'} · {_format_duration(dur)} · {_format_size(size)}"
        if len(label) > 60:
            label = label[:57] + "..."
        keyboard.append([InlineKeyboardButton(f"🎬 {label}", callback_data=f"searchview:{idx}")])

    desc_bits = []
    if query_str:
        desc_bits.append(f"'{query_str}'")
    if filters:
        desc_bits.append(", ".join(f"{k}={v:g}" for k, v in filters.items()))
    header = " ".join(desc_bits) or "all videos"

    text = f"🔍 *Search:* {header}\nShowing {len(rows)} of {total} match(es). Tap one to view it."
    if total > len(rows):
        text += "\nAdd filters to narrow this down further."

    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def search_view_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    idx = int(query.data.split(":")[1])

    results = _search_sessions.get(chat_id)
    if not results or idx >= len(results):
        await query.answer("This search result has expired — run /search again.", show_alert=True)
        return

    fid, funid, col, fname, dur, size = results[idx]
    caption = f"{col} — {fname or 'Unnamed'}"
    result = await _send_single_video_with_fallback(chat_id, fid, funid, caption, context, col)
    if result is None:
        await query.answer("That video couldn't be sent (it's marked dead).", show_alert=True)

# ----------------------------------------------------------------------
# Settings & State Switchers
# ----------------------------------------------------------------------
async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    collections = get_active_collections(chat_id)
    rem = "ON" if chat_id in removing_chats else "OFF"
    paused = "YES" if chat_id in paused_chats else "NO"
    min_l = min_video_length.get(chat_id, "OFF")

    text = (
        f"⚙️ *Settings*\n\n"
        f"• Active Collection: `{', '.join(collections)}`\n"
        f"• Remove Mode: {rem}\n"
        f"• Paused: {paused}\n"
        f"• Min Length Filter: {min_l}\n"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Toggle Remove Mode", callback_data="settings:toggle_remove")],
        [InlineKeyboardButton("Toggle Pause", callback_data="settings:toggle_pause")],
        [InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu_back")],
    ])

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")

async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action = query.data.split(":")[1]
    chat_id = update.effective_chat.id

    if action == "toggle_remove":
        if chat_id in removing_chats:
            removing_chats.discard(chat_id)
        else:
            removing_chats.add(chat_id)
    elif action == "toggle_pause":
        if chat_id in paused_chats:
            paused_chats.discard(chat_id)
        else:
            paused_chats.add(chat_id)

    await settings_command(update, context)

async def collect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        cols = get_active_collections(chat_id)
        await update.message.reply_text(f"📁 Active collection: `{', '.join(cols)}`", parse_mode="Markdown")
        return

    name = normalize_name(" ".join(context.args))
    err = validate_collection_path(name)
    if err:
        await update.message.reply_text(f"⚠️ {describe_path_error(err)}")
        return

    active_collections[chat_id] = [name]
    await update.message.reply_text(f"✅ Active collection set to: `{name}`", parse_mode="Markdown")

async def fav_shortcut(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    active_collections[chat_id] = ["favorites"]
    await update.message.reply_text("⭐ Active collection set to: `favorites`", parse_mode="Markdown")

async def current(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    names = get_active_collections(chat_id)
    suffix = ""
    if chat_id in removing_chats:
        suffix = " (🗑️ REMOVE MODE ON)"
    elif chat_id in paused_chats:
        suffix = " (⏸️ PAUSED)"

    if len(names) == 1:
        await update.message.reply_text(f"📁 Active collection: {names[0]}{suffix}")
    else:
        await update.message.reply_text(f"📁 Active collections: {', '.join(names)}{suffix}")

async def finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    active_collections[chat_id] = [DEFAULT_COLLECTION]
    paused_chats.discard(chat_id)
    removing_chats.discard(chat_id)
    await update.message.reply_text(f"✅ Reset active collection to: {DEFAULT_COLLECTION}")

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    task = _active_tasks.get(chat_id)
    if task and not task.done():
        task.cancel()
    paused_chats.add(chat_id)
    await update.message.reply_text("🛑 Cancelled active processes and paused saving.")

async def minlength(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        curr = min_video_length.get(chat_id)
        msg = f"⏱️ Current min length filter: {curr} seconds" if curr else "⏱️ Min length filter is currently OFF."
        await update.message.reply_text(f"{msg}\nUsage: /minlength <seconds> or /minlength off")
        return

    val = context.args[0].lower()
    if val in ("off", "0", "disable", "none"):
        min_video_length.pop(chat_id, None)
        await update.message.reply_text("⏱️ Min length filter turned OFF.")
    elif val.isdigit():
        secs = int(val)
        if secs < 0:
            await update.message.reply_text("⚠️ Duration cannot be negative.")
            return
        min_video_length[chat_id] = secs
        await update.message.reply_text(f"⏱️ Min length set to {secs} seconds. Shorter videos will be skipped.")
    else:
        await update.message.reply_text("⚠️ Please provide a number in seconds or 'off'.")

async def removemode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        status = "ON" if chat_id in removing_chats else "OFF"
        await update.message.reply_text(f"🗑️ Remove mode is currently {status}.\nUsage: /removemode on|off")
        return

    val = context.args[0].lower()
    if val in ("on", "1", "enable", "true"):
        removing_chats.add(chat_id)
        await update.message.reply_text("🗑️ Remove mode ON. Forwarded videos will be deleted from active collection(s).")
    elif val in ("off", "0", "disable", "false"):
        removing_chats.discard(chat_id)
        await update.message.reply_text("🗑️ Remove mode OFF. Videos will be saved normally.")
    else:
        await update.message.reply_text("⚠️ Please specify 'on' or 'off'.")

# ----------------------------------------------------------------------
# Remove by reply, status, count, info, setexpiry
# ----------------------------------------------------------------------
async def remove_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    reply = update.message.reply_to_message
    if not reply:
        await update.message.reply_text("⚠️ Reply to a video message with /remove to delete it.")
        return

    file_unique_id = None
    if reply.video:
        file_unique_id = reply.video.file_unique_id
    elif reply.document and _is_video_document(reply):
        file_unique_id = reply.document.file_unique_id

    if not file_unique_id:
        await update.message.reply_text("⚠️ The replied message doesn't contain a valid video.")
        return

    collections = get_active_collections(chat_id)
    deleted_from = []
    for col in collections:
        if await _delete_video_from_collection(col, file_unique_id):
            deleted_from.append(col)

    if deleted_from:
        await update.message.reply_text(f"🗑️ Deleted video from: {', '.join(deleted_from)}")
    else:
        await update.message.reply_text("⚠️ Video was not found in active collection(s).")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    collections = get_active_collections(chat_id)

    try:
        def _query(conn):
            with conn.cursor() as cur:
                counts = {}
                for col in collections:
                    cur.execute("SELECT COUNT(*) FROM videos WHERE collection = %s", (col,))
                    counts[col] = cur.fetchone()[0]
                return counts
        counts = await db_run(_query)
    except Exception as e:
        await reply_db_error(update, "fetch status", e)
        return

    lines = [f"📊 *Status* (Active: {', '.join(collections)})"]
    for col, count in counts.items():
        lines.append(f"• `{col}`: {count} video(s)")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def count_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /count <collection>")
        return
    name = normalize_name(" ".join(context.args))

    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM videos WHERE collection = %s", (name,))
                return cur.fetchone()[0]
        count = await db_run(_query)
        await update.message.reply_text(f"📊 Collection `{name}` contains {count} video(s).", parse_mode="Markdown")
    except Exception as e:
        await reply_db_error(update, f"count '{name}'", e)

async def collection_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /info <collection>")
        return
    name = normalize_name(" ".join(context.args))

    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*), SUM(file_size), AVG(duration), MIN(added_at), MAX(added_at)
                    FROM videos WHERE collection = %s
                    """,
                    (name,),
                )
                return cur.fetchone()
        count, total_size, avg_dur, first_added, last_added = await db_run(_query)
    except Exception as e:
        await reply_db_error(update, f"fetch info for '{name}'", e)
        return

    if not count:
        await update.message.reply_text(f"No videos in '{name}'.")
        return

    size_mb = (total_size / 1024 / 1024) if total_size else 0
    avg_dur_str = f"{int(avg_dur)}s" if avg_dur else "Unknown"
    first_str = first_added.strftime("%Y-%m-%d %H:%M") if first_added else "Unknown"
    last_str = last_added.strftime("%Y-%m-%d %H:%M") if last_added else "Unknown"

    info = (
        f"ℹ️ *Collection Info:* `{name}`\n"
        f"• Total Videos: {count}\n"
        f"• Storage Used: {size_mb:.2f} MB\n"
        f"• Avg Duration: {avg_dur_str}\n"
        f"• First Added: {first_str}\n"
        f"• Last Added: {last_str}"
    )
    await update.message.reply_text(info, parse_mode="Markdown")

async def set_expiry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_check(update):
        return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Usage: /setexpiry <collection> <days>\nUse 0 days to disable expiry.")
        return

    days_str = context.args[-1]
    name = normalize_name(" ".join(context.args[:-1]))

    if not days_str.isdigit():
        await update.message.reply_text("⚠️ Days must be a positive integer.")
        return
    days = int(days_str)

    try:
        def _set(conn):
            with conn.cursor() as cur:
                if days <= 0:
                    cur.execute("DELETE FROM collection_settings WHERE collection = %s", (name,))
                else:
                    cur.execute(
                        """
                        INSERT INTO collection_settings (collection, expiry_days)
                        VALUES (%s, %s)
                        ON CONFLICT (collection) DO UPDATE SET expiry_days = EXCLUDED.expiry_days
                        """,
                        (name, days),
                    )
        await db_run(_set)
        if days > 0:
            await update.message.reply_text(f"⏰ Set auto-expiry for `{name}` to {days} days.", parse_mode="Markdown")
        else:
            await update.message.reply_text(f"⏰ Disabled auto-expiry for `{name}`.", parse_mode="Markdown")
    except Exception as e:
        await reply_db_error(update, "set expiry", e)

# ----------------------------------------------------------------------
# Folder & collection management: delete, rename, move, copy, merge, dups
# ----------------------------------------------------------------------
async def delete_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /delete <name>")
        return
    name = normalize_name(" ".join(context.args))

    try:
        def _count(conn):
            with conn.cursor() as cur:
                clause, params = _under_clause(name)
                cur.execute(f"SELECT COUNT(*) FROM videos WHERE {clause}", params)
                return cur.fetchone()[0]
        total = await db_run(_count)
    except Exception as e:
        await reply_db_error(update, f"check '{name}'", e)
        return

    if total == 0:
        await update.message.reply_text(f"No collection or folder matching '{name}'.")
        return

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("YES, DELETE EVERYTHING", callback_data=f"confirmdelete:{name}")],
        [InlineKeyboardButton("CANCEL", callback_data="canceldelete")],
    ])

    await update.message.reply_text(
        f"⚠️ Are you sure you want to delete `{name}` and all nested folders? ({total} video(s) will be permanently lost)",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )

async def confirm_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("confirmdelete:"):]

    try:
        def _delete(conn):
            with conn.cursor() as cur:
                clause, params = _under_clause(name)
                cur.execute(
                    f"DELETE FROM videos WHERE {clause}",
                    params,
                )
                return cur.rowcount

        count = await db_run(_delete)
    except Exception as e:
        await reply_db_error(update, f"delete '{name}'", e)
        return

    await query.edit_message_text(f"✅ Deleted folder `{name}` ({count} items deleted).")

async def cancel_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("❎ Deletion cancelled.")


async def rename_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parsed = _parse_arrow_pair(context.args or [])
    if not parsed:
        await update.message.reply_text(
            "Usage: /rename <old> -> <new>\n"
            "Examples:\n"
            "  /rename movies -> films\n"
            "  /rename movies/action -> movies/classic-action"
        )
        return
    src, dest = parsed
    err = validate_collection_path(dest)
    if err:
        await update.message.reply_text(f"⚠️ Destination path is invalid: {describe_path_error(err)}")
        return

    try:
        def _rename(conn):
            with conn.cursor() as cur:
                clause, params = _under_clause(src)
                cur.execute(
                    f"SELECT DISTINCT collection FROM videos WHERE {clause}",
                    params,
                )
                affected = [c for (c,) in cur.fetchall()]
                if not affected:
                    return 0

                renamed_count = 0
                for old in affected:
                    if old == src:
                        new_col = dest
                    else:
                        suffix = old[len(src) + 1:]
                        new_col = f"{dest}/{suffix}"
                    err_col = validate_collection_path(new_col)
                    if err_col:
                        raise ValueError(f"Resulting path '{new_col}' is invalid: {describe_path_error(err_col)}")

                    cur.execute("SELECT file_id, file_unique_id, duration, file_size, file_name FROM videos WHERE collection = %s", (old,))
                    rows = cur.fetchall()
                    for fid, fuid, dur, sz, fn in rows:
                        cur.execute(
                            "SELECT 1 FROM videos WHERE collection = %s AND file_unique_id = %s",
                            (new_col, fuid),
                        )
                        if cur.fetchone():
                            cur.execute("DELETE FROM videos WHERE collection = %s AND file_unique_id = %s", (old, fuid))
                        else:
                            cur.execute(
                                """
                                UPDATE videos
                                SET collection = %s
                                WHERE collection = %s AND file_unique_id = %s
                                """,
                                (new_col, old, fuid),
                            )
                        renamed_count += 1
                    cur.execute("UPDATE sent_videos SET collection = %s WHERE collection = %s", (new_col, old))
                    cur.execute("UPDATE collection_settings SET collection = %s WHERE collection = %s", (new_col, old))
                return renamed_count
        count = await db_run(_rename)
        if count == 0:
            await update.message.reply_text(f"No collection or folder found matching '{src}'.")
        else:
            await update.message.reply_text(f"✏️ Renamed `{src}` -> `{dest}` ({count} video(s) updated).", parse_mode="Markdown")
    except ValueError as ve:
        await update.message.reply_text(f"⚠️ {ve}")
    except Exception as e:
        await reply_db_error(update, f"rename '{src}'", e)

async def move_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parsed = _parse_arrow_pair(context.args or [])
    if not parsed:
        await update.message.reply_text("Usage: /move <src> -> <dest>")
        return
    src, dest = parsed
    err = validate_collection_path(dest)
    if err:
        await update.message.reply_text(f"⚠️ Destination path invalid: {describe_path_error(err)}")
        return

    try:
        def _move(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT file_unique_id FROM videos WHERE collection = %s", (src,))
                fuids = [r[0] for r in cur.fetchall()]
                if not fuids:
                    return 0
                moved = 0
                for fuid in fuids:
                    cur.execute(
                        "SELECT 1 FROM videos WHERE collection = %s AND file_unique_id = %s",
                        (dest, fuid),
                    )
                    if cur.fetchone():
                        cur.execute("DELETE FROM videos WHERE collection = %s AND file_unique_id = %s", (src, fuid))
                    else:
                        cur.execute(
                            "UPDATE videos SET collection = %s WHERE collection = %s AND file_unique_id = %s",
                            (dest, src, fuid),
                        )
                    moved += 1
                return moved
        count = await db_run(_move)
        if count == 0:
            await update.message.reply_text(f"No videos found in '{src}'.")
        else:
            await update.message.reply_text(f"📦 Moved {count} video(s) from `{src}` -> `{dest}`.", parse_mode="Markdown")
    except Exception as e:
        await reply_db_error(update, "move videos", e)

async def copy_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parsed = _parse_arrow_pair(context.args or [])
    if not parsed:
        await update.message.reply_text("Usage: /copy <src> -> <dest>")
        return
    src, dest = parsed
    err = validate_collection_path(dest)
    if err:
        await update.message.reply_text(f"⚠️ Destination path invalid: {describe_path_error(err)}")
        return

    try:
        def _copy(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO videos (collection, file_id, file_unique_id, duration, file_size, file_name)
                    SELECT %s, file_id, file_unique_id, duration, file_size, file_name
                    FROM videos WHERE collection = %s
                    ON CONFLICT (collection, file_unique_id) DO NOTHING
                    """,
                    (dest, src),
                )
                return cur.rowcount
        count = await db_run(_copy)
        await update.message.reply_text(f"📋 Copied {count} video(s) from `{src}` to `{dest}`.", parse_mode="Markdown")
    except Exception as e:
        await reply_db_error(update, "copy videos", e)

async def merge_collections(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parsed = _parse_arrow_pair(context.args or [])
    if not parsed:
        await update.message.reply_text("Usage: /merge <source> -> <target>")
        return
    src, dest = parsed
    err = validate_collection_path(dest)
    if err:
        await update.message.reply_text(f"⚠️ Destination path invalid: {describe_path_error(err)}")
        return

    try:
        def _merge(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO videos (collection, file_id, file_unique_id, duration, file_size, file_name)
                    SELECT %s, file_id, file_unique_id, duration, file_size, file_name
                    FROM videos WHERE collection = %s
                    ON CONFLICT (collection, file_unique_id) DO NOTHING
                    """,
                    (dest, src),
                )
                cur.execute("DELETE FROM videos WHERE collection = %s", (src,))
                return cur.rowcount
        count = await db_run(_merge)
        await update.message.reply_text(f"🔀 Merged `{src}` into `{dest}` ({count} video(s) total in destination).", parse_mode="Markdown")
    except Exception as e:
        await reply_db_error(update, "merge collections", e)

async def dups_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /dups <collection>")
        return
    name = normalize_name(" ".join(context.args))

    try:
        def _find_dups(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT file_unique_id, COUNT(*)
                    FROM videos WHERE collection = %s
                    GROUP BY file_unique_id HAVING COUNT(*) > 1
                    """,
                    (name,),
                )
                return cur.fetchall()
        dups = await db_run(_find_dups)
        if not dups:
            await update.message.reply_text(f"✅ No duplicates found in `{name}`.", parse_mode="Markdown")
        else:
            await update.message.reply_text(f"⚠️ Found {len(dups)} duplicate video ID(s) in `{name}`.", parse_mode="Markdown")
    except Exception as e:
        await reply_db_error(update, f"check duplicates in '{name}'", e)

# ----------------------------------------------------------------------
# Near duplicates detection & pagination UI
# ----------------------------------------------------------------------
_neardup_sessions: Dict[str, Tuple[List[Tuple[Tuple[str, str, int, int], Tuple[str, str, int, int]]], str, int]] = {}

def _fetch_near_duplicates(collection: str) -> List[Tuple[Tuple[str, str, int, int], Tuple[str, str, int, int]]]:
    def _query(conn):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT v1.file_id, v1.file_unique_id, v1.duration, v1.file_size,
                       v2.file_id, v2.file_unique_id, v2.duration, v2.file_size
                FROM videos v1
                JOIN videos v2 ON v1.collection = v2.collection AND v1.id < v2.id
                WHERE v1.collection = %s
                  AND (
                    (
                      v1.duration IS NOT NULL AND v2.duration IS NOT NULL
                      AND ABS(v1.duration - v2.duration) <= %s
                      AND v1.file_size IS NOT NULL AND v2.file_size IS NOT NULL
                      AND v2.file_size BETWEEN v1.file_size * (1 - %s) AND v1.file_size * (1 + %s)
                    )
                    OR
                    (
                      (v1.duration IS NULL OR v2.duration IS NULL)
                      AND v1.file_size IS NOT NULL AND v2.file_size IS NOT NULL
                      AND v2.file_size BETWEEN v1.file_size * (1 - %s) AND v1.file_size * (1 + %s)
                    )
                  )
                ORDER BY v1.id
                """,
                (
                    collection,
                    NEAR_DUP_DURATION_TOLERANCE_SECONDS,
                    NEAR_DUP_SIZE_TOLERANCE_FRACTION,
                    NEAR_DUP_SIZE_TOLERANCE_FRACTION,
                    NEAR_DUP_SIZE_ONLY_TOLERANCE_FRACTION,
                    NEAR_DUP_SIZE_ONLY_TOLERANCE_FRACTION,
                ),
            )
            rows = cur.fetchall()
            pairs = []
            for r in rows:
                v1 = (r[0], r[1], r[2], r[3])
                v2 = (r[4], r[5], r[6], r[7])
                pairs.append((v1, v2))
            return pairs
    return _db_call(_query)

async def near_duplicates_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /neardupes <collection>")
        return
    name = normalize_name(" ".join(context.args))

    try:
        pairs = await db_run(lambda: _fetch_near_duplicates(name))
    except Exception as e:
        await reply_db_error(update, f"find near dupes in '{name}'", e)
        return

    if not pairs:
        await update.message.reply_text(f"✅ No possible near-duplicates found in `{name}`.", parse_mode="Markdown")
        return

    token = f"{chat_id}:{name}"
    _neardup_sessions[token] = (pairs, name, chat_id)
    await _show_neardup_page(chat_id, token, 1, context, edit_msg=None)

async def _show_neardup_page(
    chat_id: int,
    token: str,
    page: int,
    context: ContextTypes.DEFAULT_TYPE,
    edit_msg: Optional[Message] = None,
):
    session = _neardup_sessions.get(token)
    if not session:
        msg = "⏱️ Near-dupe session expired. Run `/neardupes <collection>` again."
        if edit_msg:
            await edit_msg.edit_text(msg, parse_mode="Markdown")
        else:
            await context.bot.send_message(chat_id, msg, parse_mode="Markdown")
        return

    pairs, collection, _ = session
    total_pairs = len(pairs)
    total_pages = (total_pairs + NEARDUPES_PAIRS_PER_PAGE - 1) // NEARDUPES_PAIRS_PER_PAGE
    page = min(max(1, page), total_pages)
    start_idx = (page - 1) * NEARDUPES_PAIRS_PER_PAGE
    page_pairs = pairs[start_idx:start_idx + NEARDUPES_PAIRS_PER_PAGE]

    status_text = f"🔎 *Near-duplicates in* `{collection}` — Page {page}/{total_pages} ({total_pairs} pair(s) total)\nSending side-by-side videos..."
    if edit_msg:
        progress_msg = await edit_msg.edit_text(status_text, parse_mode="Markdown")
    else:
        progress_msg = await context.bot.send_message(chat_id, status_text, parse_mode="Markdown")

    for pair_idx, (v1, v2) in enumerate(page_pairs, start=start_idx + 1):
        cap1 = f"Pair #{pair_idx} — Video A\n⏱ {v1[2] or '?'}s • 📦 {(v1[3] or 0)/1024/1024:.2f}MB"
        cap2 = f"Pair #{pair_idx} — Video B\n⏱ {v2[2] or '?'}s • 📦 {(v2[3] or 0)/1024/1024:.2f}MB"

        msg_a = await _send_single_video_with_fallback(chat_id, v1[0], v1[1], cap1, context, collection)
        msg_b = await _send_single_video_with_fallback(chat_id, v2[0], v2[1], cap2, context, collection)

        t1 = f"{v1[1]}:{collection}"
        t2 = f"{v2[1]}:{collection}"
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Keep A (Delete B)", callback_data=f"nddel:{t2}"),
                InlineKeyboardButton("Keep B (Delete A)", callback_data=f"nddel:{t1}"),
            ],
            [
                InlineKeyboardButton("Delete Both", callback_data=f"nddelboth:{t1}:{t2}"),
                InlineKeyboardButton("Keep Both", callback_data="ndkeep"),
            ],
        ])
        if msg_b:
            try:
                await msg_b.reply_text("Choose action for Pair #" + str(pair_idx) + ":", reply_markup=kb)
            except TelegramError:
                await context.bot.send_message(chat_id, "Choose action for Pair #" + str(pair_idx) + ":", reply_markup=kb)
        elif msg_a:
            try:
                await msg_a.reply_text("Choose action for Pair #" + str(pair_idx) + ":", reply_markup=kb)
            except TelegramError:
                await context.bot.send_message(chat_id, "Choose action for Pair #" + str(pair_idx) + ":", reply_markup=kb)

        await asyncio.sleep(NEARDUP_ALBUM_DELAY)

    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️ Prev Page", callback_data=f"ndpage:{token}:{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Next Page ▶️", callback_data=f"ndpage:{token}:{page+1}"))

    rows_kb = []
    if nav_buttons:
        rows_kb.append(nav_buttons)
    rows_kb.append([InlineKeyboardButton("Done / Close", callback_data=f"ndclose:{token}")])

    await context.bot.send_message(
        chat_id,
        f"✅ Finished page {page}/{total_pages}.",
        reply_markup=InlineKeyboardMarkup(rows_kb),
    )

async def neardup_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":")
    token = f"{parts[1]}:{parts[2]}"
    page = int(parts[3])
    chat_id = update.effective_chat.id
    await _show_neardup_page(chat_id, token, page, context, edit_msg=query.message)

async def neardup_del_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    token = query.data[len("nddel:"):]
    try:
        fuid, collection = token.split(":", 1)
    except ValueError:
        await query.edit_message_text("⚠️ Invalid action.")
        return

    ok = await _delete_video_from_collection(collection, fuid)
    if ok:
        await query.edit_message_text(f"🗑️ Deleted target video from `{collection}`.", parse_mode="Markdown")
    else:
        await query.edit_message_text(f"⚠️ Video was not found in `{collection}`.", parse_mode="Markdown")

async def neardup_delboth_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    token = query.data[len("nddelboth:"):]
    try:
        t1, t2 = token.split(":", 1)
        fuid1, col1 = t1.split(":", 1)
        fuid2, col2 = t2.split(":", 1)
    except ValueError:
        await query.edit_message_text("⚠️ Invalid action.")
        return

    ok1 = await _delete_video_from_collection(col1, fuid1)
    ok2 = await _delete_video_from_collection(col2, fuid2)
    msgs = []
    msgs.append(f"Deleted Video A from `{col1}`" if ok1 else f"Video A not found in `{col1}`")
    msgs.append(f"Deleted Video B from `{col2}`" if ok2 else f"Video B not found in `{col2}`")
    await query.edit_message_text("🗑️ " + " | ".join(msgs), parse_mode="Markdown")

async def neardup_keep_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("👍 Kept both videos.")

async def neardup_close_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    token = query.data[len("ndclose:"):]
    _neardup_sessions.pop(token, None)
    await query.edit_message_text("✅ Near-duplicates review closed.")

# ----------------------------------------------------------------------
# Export & Cleanup commands
# ----------------------------------------------------------------------
async def export_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /export <collection>")
        return
    name = normalize_name(" ".join(context.args))

    try:
        def _fetch(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT file_id FROM videos WHERE collection = %s ORDER BY added_at", (name,))
                return [r[0] for r in cur.fetchall()]
        file_ids = await db_run(_fetch)
    except Exception as e:
        await reply_db_error(update, f"export '{name}'", e)
        return

    if not file_ids:
        await update.message.reply_text(f"No videos found in '{name}'.")
        return

    content = f"# Collection: {name}\n# Total: {len(file_ids)}\n" + "\n".join(file_ids)
    bio = io.BytesIO(content.encode("utf-8"))
    bio.name = f"{name}_export.txt"

    await update.message.reply_document(
        document=bio,
        caption=f"📄 Exported {len(file_ids)} video reference(s) from `{name}`.",
        parse_mode="Markdown",
    )

async def export_json(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /exportjson <collection>")
        return
    name = normalize_name(" ".join(context.args))

    try:
        def _fetch(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT file_id, file_unique_id, duration, file_size, file_name, added_at FROM videos WHERE collection = %s ORDER BY added_at",
                    (name,),
                )
                return cur.fetchall()
        rows = await db_run(_fetch)
    except Exception as e:
        await reply_db_error(update, f"export json '{name}'", e)
        return

    if not rows:
        await update.message.reply_text(f"No videos found in '{name}'.")
        return

    data = {
        "collection": name,
        "exported_at": datetime.utcnow().isoformat(),
        "count": len(rows),
        "videos": [
            {
                "file_id": r[0],
                "file_unique_id": r[1],
                "duration": r[2],
                "file_size": r[3],
                "file_name": r[4],
                "added_at": r[5].isoformat() if r[5] else None,
            }
            for r in rows
        ],
    }

    bio = io.BytesIO(json.dumps(data, indent=2).encode("utf-8"))
    bio.name = f"{name}_export.json"

    await update.message.reply_document(
        document=bio,
        caption=f"📋 Exported JSON for `{name}` ({len(rows)} videos).",
        parse_mode="Markdown",
    )

async def import_json(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_check(update):
        return
    reply = update.message.reply_to_message
    if not reply or not reply.document or not reply.document.file_name.endswith(".json"):
        await update.message.reply_text("Usage: Reply to a JSON backup file with /importjson")
        return

    try:
        doc = await reply.document.get_file()
        content = await doc.download_as_bytearray()
        data = json.loads(content.decode("utf-8"))
        collection = data.get("collection")
        videos = data.get("videos", [])
        if not collection or not videos:
            await update.message.reply_text("⚠️ Invalid JSON file format.")
            return

        imported = 0
        skipped = 0

        def _import(conn):
            nonlocal imported, skipped
            with conn.cursor() as cur:
                for v in videos:
                    cur.execute(
                        """
                        INSERT INTO videos (collection, file_id, file_unique_id, duration, file_size, file_name)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (collection, file_unique_id) DO NOTHING
                        """,
                        (collection, v["file_id"], v["file_unique_id"], v.get("duration"), v.get("file_size"), v.get("file_name")),
                    )
                    if cur.rowcount > 0:
                        imported += 1
                    else:
                        skipped += 1
        await db_run(_import)
        await update.message.reply_text(
            f"📥 Imported JSON into `{collection}`:\n• Imported: {imported}\n• Skipped (duplicates): {skipped}",
            parse_mode="Markdown",
        )
    except Exception as e:
        await reply_db_error(update, "import JSON", e)

async def cleanup_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /cleanup <collection>")
        return
    name = normalize_name(" ".join(context.args))

    try:
        def _cleanup(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM videos WHERE collection = %s AND file_unique_id IN (SELECT file_unique_id FROM dead_files)",
                    (name,),
                )
                removed = cur.rowcount
                cur.execute(
                    "DELETE FROM dead_files d WHERE NOT EXISTS (SELECT 1 FROM videos v WHERE v.file_unique_id = d.file_unique_id)"
                )
                return removed
        removed = await db_run(_cleanup)
        await update.message.reply_text(f"🧹 Cleaned up `{name}`. Removed {removed} dead file reference(s).", parse_mode="Markdown")
    except Exception as e:
        await reply_db_error(update, f"cleanup '{name}'", e)

async def backup_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_check(update):
        return

    try:
        def _dump(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT collection, file_id, file_unique_id, duration, file_size, file_name, added_at FROM videos")
                return cur.fetchall()
        rows = await db_run(_dump)
        data = [
            {
                "collection": r[0],
                "file_id": r[1],
                "file_unique_id": r[2],
                "duration": r[3],
                "file_size": r[4],
                "file_name": r[5],
                "added_at": r[6].isoformat() if r[6] else None,
            }
            for r in rows
        ]

        bio = io.BytesIO(json.dumps(data, indent=2).encode("utf-8"))
        bio.name = f"backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

        await update.message.reply_document(
            document=bio,
            caption=f"💾 Full database backup ({len(rows)} records).",
        )
    except Exception as e:
        await reply_db_error(update, "backup database", e)

# ----------------------------------------------------------------------
# Helper: format DB errors cleanly
# ----------------------------------------------------------------------
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

# ----------------------------------------------------------------------
# Webhook & Server Setup
# ----------------------------------------------------------------------
async def health_check(request):
    return PlainTextResponse("OK", status_code=200)

async def telegram_webhook(request):
    secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if secret != WEBHOOK_SECRET:
        logger.warning("Invalid webhook secret token.")
        return Response(status_code=403)
    try:
        body = await request.json()
        update = Update.de_json(body, app.bot)
        await app.process_update(update)
        return Response(status_code=200)
    except Exception as e:
        logger.exception("Error processing webhook update: %s", e)
        return Response(status_code=500)

async def _filter_filter(update: Update) -> bool:
    chat_id = update.effective_chat.id
    min_len = min_video_length.get(chat_id)
    if min_len is None:
        return True
    video = update.message.video
    if video and video.duration is not None:
        return video.duration >= min_len
    return True

# ----------------------------------------------------------------------
# Application Initialization
# ----------------------------------------------------------------------
init_db()

app = Application.builder().token(BOT_TOKEN).build()

app.add_handler(TypeHandler(Update, access_control), group=-1)

# Handlers
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("help", help_command))
app.add_handler(CommandHandler("menu", menu_command))
app.add_handler(CommandHandler("settings", settings_command))
app.add_handler(CommandHandler("collect", collect))
app.add_handler(CommandHandler("fav", fav_shortcut))
app.add_handler(CommandHandler("current", current))
app.add_handler(CommandHandler("finish", finish))
app.add_handler(CommandHandler("stop", stop_command))
app.add_handler(CommandHandler("minlength", minlength))
app.add_handler(CommandHandler("removemode", removemode))
app.add_handler(CommandHandler("remove", remove_video))
app.add_handler(CommandHandler("status", status))
app.add_handler(CommandHandler("count", count_collection))
app.add_handler(CommandHandler("info", collection_info))
app.add_handler(CommandHandler("setexpiry", set_expiry))
app.add_handler(CommandHandler("get", get_collection))
app.add_handler(CommandHandler("list", list_collections))
app.add_handler(CommandHandler("random", random_video))
app.add_handler(CommandHandler("search", search_videos))
app.add_handler(CommandHandler("delete", delete_collection))
app.add_handler(CommandHandler("rename", rename_collection))
app.add_handler(CommandHandler("move", move_collection))
app.add_handler(CommandHandler("copy", copy_collection))
app.add_handler(CommandHandler("merge", merge_collections))
app.add_handler(CommandHandler("dups", dups_collection))
app.add_handler(CommandHandler("neardupes", near_duplicates_command))
app.add_handler(CommandHandler("export", export_collection))
app.add_handler(CommandHandler("exportjson", export_json))
app.add_handler(CommandHandler("importjson", import_json))
app.add_handler(CommandHandler("cleanup", cleanup_collection))
app.add_handler(CommandHandler("backup", backup_database))

# Callbacks
app.add_handler(CallbackQueryHandler(menu_back_callback, pattern="^menu_back$"))
app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_"))
app.add_handler(CallbackQueryHandler(menu_folder_callback, pattern="^menufolder:"))
app.add_handler(CallbackQueryHandler(menu_get_all_callback, pattern="^menugetall:"))
app.add_handler(CallbackQueryHandler(menu_rand_all_callback, pattern="^menurandall:"))
app.add_handler(CallbackQueryHandler(menu_set_callback, pattern="^menuset:"))
app.add_handler(CallbackQueryHandler(menu_view_callback, pattern="^menuview:"))
app.add_handler(CallbackQueryHandler(menu_random_callback, pattern="^menurandom:"))
app.add_handler(CallbackQueryHandler(settings_callback, pattern="^settings:"))

app.add_handler(CallbackQueryHandler(list_folder_callback, pattern="^listfolder:"))
app.add_handler(CallbackQueryHandler(list_delete_callback, pattern="^listdelete:"))
app.add_handler(CallbackQueryHandler(list_set_callback, pattern="^listset:"))
app.add_handler(CallbackQueryHandler(list_get_callback, pattern="^listget:"))
app.add_handler(CallbackQueryHandler(list_random_callback, pattern="^listrandom:"))

app.add_handler(CallbackQueryHandler(random_next_callback, pattern="^random_next:"))

app.add_handler(CallbackQueryHandler(get_next_callback, pattern="^getnext$"))
app.add_handler(CallbackQueryHandler(get_prev_callback, pattern="^getprev$"))
app.add_handler(CallbackQueryHandler(get_batch_toggle_callback, pattern="^getbatch$"))
app.add_handler(CallbackQueryHandler(get_jump_callback, pattern="^getjump$"))
app.add_handler(CallbackQueryHandler(get_cancel_callback, pattern="^getcancel$"))
app.add_handler(CallbackQueryHandler(search_view_callback, pattern="^searchview:"))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_get_page_jump_text))

app.add_handler(CallbackQueryHandler(confirm_delete_callback, pattern="^confirmdelete:"))
app.add_handler(CallbackQueryHandler(cancel_delete_callback, pattern="^canceldelete$"))

app.add_handler(CallbackQueryHandler(neardup_page_callback, pattern="^ndpage:"))
app.add_handler(CallbackQueryHandler(neardup_del_callback, pattern="^nddel:"))
app.add_handler(CallbackQueryHandler(neardup_delboth_callback, pattern="^nddelboth:"))
app.add_handler(CallbackQueryHandler(neardup_keep_callback, pattern="^ndkeep$"))
app.add_handler(CallbackQueryHandler(neardup_close_callback, pattern="^ndclose:"))

# Video and file handlers
app.add_handler(MessageHandler(filters.VIDEO, handle_video))
app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
app.add_handler(MessageHandler(filters.PHOTO, handle_non_video))

# ----------------------------------------------------------------------
# Starlette Web Server
# ----------------------------------------------------------------------
starlette_app = Starlette(
    routes=[
        Route("/health", health_check, methods=["GET"]),
        Route("/telegram-webhook", telegram_webhook, methods=["POST"]),
    ]
)

# ----------------------------------------------------------------------
# Lifecycle Management
# ----------------------------------------------------------------------
async def main():
    webhook_url = f"{RENDER_EXTERNAL_URL}/telegram-webhook"
    logger.info("Initializing Telegram bot application...")
    await app.initialize()

    logger.info("Setting webhook to %s", webhook_url)
    await app.bot.set_webhook(
        url=webhook_url,
        secret_token=WEBHOOK_SECRET,
        allowed_updates=["message", "callback_query"],
    )

    commands = [
        BotCommand("menu", "Open main menu"),
        BotCommand("collect", "Set active collection"),
        BotCommand("fav", "Shortcut for favorites collection"),
        BotCommand("get", "Get videos from collection"),
        BotCommand("list", "List all collections"),
        BotCommand("random", "Get random video(s)"),
        BotCommand("search", "Search videos (supports filters, see /help)"),
        BotCommand("status", "Show active collection status"),
        BotCommand("current", "Show current active collection"),
        BotCommand("finish", "Reset active collection to default"),
        BotCommand("stop", "Stop active processes and pause"),
        BotCommand("settings", "Open settings menu"),
        BotCommand("help", "Show help and command list"),
    ]
    await app.bot.set_my_commands(commands)

    logger.info("Starting bot application...")
    await app.start()

    logger.info("Starting Web server on port %d...", PORT)
    config = uvicorn.Config(app=starlette_app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)
    
    try:
        await server.serve()
    finally:
        logger.info("Stopping Telegram application...")
        await app.stop()
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
