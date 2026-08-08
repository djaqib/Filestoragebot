import os
import io
import time
import json
import asyncio
import logging
import random
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Set, Tuple
from datetime import datetime
import psycopg2
from psycopg2 import pool
import uvicorn
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route
from telegram import (
    Update,
    BotCommand,
    InputMediaVideo,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    TypeHandler,
    ContextTypes,
    ApplicationHandlerStop,
    filters,
)
from telegram.error import TelegramError

# ----------------------------------------------------------------------
# Configuration & constants
# ----------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"]
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]
RENDER_EXTERNAL_URL = os.environ["RENDER_EXTERNAL_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]
PORT = int(os.environ.get("PORT", 10000))

_raw_allowed = os.environ["ALLOWED_USER_IDS"]
ALLOWED_USER_IDS = {int(uid.strip()) for uid in _raw_allowed.split(",") if uid.strip()}
if not ALLOWED_USER_IDS:
    raise RuntimeError("ALLOWED_USER_IDS is set but empty — refusing to start.")

DEFAULT_COLLECTION = "default"
FAVORITES_COLLECTION = "favorites"
RESERVED_NAMES = {"default", "all"}
BATCH_DEBOUNCE_SECONDS = 2.5
LIST_PAGE_SIZE = 15
ALBUM_SIZE = 10
ALBUM_DELAY_SECONDS = 3
GET_PAGE_SIZE = 50
NEARDUPES_PAIRS_PER_PAGE = 10
NEARDUP_ALBUM_DELAY = 1.5

NEAR_DUP_DURATION_TOLERANCE_SECONDS = 2
NEAR_DUP_SIZE_TOLERANCE_FRACTION = 0.02
NEAR_DUP_SIZE_ONLY_TOLERANCE_FRACTION = 0.005

COMMAND_COOLDOWN = 3  # seconds per user for heavy commands
_last_command_time: Dict[int, float] = {}

ADMIN_IDS = ALLOWED_USER_IDS  # same as allowed users for simplicity

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def normalize_name(name: str) -> str:
    return name.strip().lower()

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def rate_limit(user_id: int, key: str = "global") -> bool:
    now = time.monotonic()
    last = _last_command_time.get(user_id, 0)
    if now - last < COMMAND_COOLDOWN:
        return False
    _last_command_time[user_id] = now
    return True

# ----------------------------------------------------------------------
# Database pool with keepalives
# ----------------------------------------------------------------------

db_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=2,
    maxconn=10,
    dsn=DATABASE_URL,
    sslmode="require",
    keepalives_idle=60,
    keepalives_interval=10,
    keepalives_count=3,
)

def _db_call(fn):
    conn = db_pool.getconn()
    try:
        result = fn(conn)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

async def db_run(fn):
    return await asyncio.to_thread(_db_call, fn)

# ----------------------------------------------------------------------
# Database initialization
# ----------------------------------------------------------------------

def init_db():
    def _init(conn):
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS videos (
                    id SERIAL PRIMARY KEY,
                    collection TEXT NOT NULL,
                    file_id TEXT NOT NULL,
                    file_unique_id TEXT NOT NULL,
                    added_at TIMESTAMPTZ DEFAULT NOW(),
                    duration INTEGER,
                    file_size BIGINT,
                    UNIQUE (collection, file_unique_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sent_videos (
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    collection TEXT NOT NULL,
                    file_unique_id TEXT NOT NULL,
                    PRIMARY KEY (chat_id, message_id)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_videos_collection_added_at ON videos (collection, added_at)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_videos_collection_filesize ON videos (collection, file_size)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dead_files (
                    file_unique_id TEXT PRIMARY KEY,
                    collection TEXT NOT NULL,
                    detected_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                DELETE FROM videos v
                WHERE v.id NOT IN (
                    SELECT MIN(id) FROM videos GROUP BY LOWER(collection), file_unique_id
                )
            """)
            cur.execute("UPDATE videos SET collection = LOWER(collection) WHERE collection != LOWER(collection)")
            cur.execute("UPDATE sent_videos SET collection = LOWER(collection) WHERE collection != LOWER(collection)")
    _db_call(_init)

# ----------------------------------------------------------------------
# State management with dataclasses
# ----------------------------------------------------------------------

@dataclass
class BatchState:
    saved: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    errors: int = 0
    near_dups: List[str] = field(default_factory=list)
    task: Optional[asyncio.Task] = None

@dataclass
class DeleteBatchState:
    deleted: List[str] = field(default_factory=list)
    not_found: List[str] = field(default_factory=list)
    errors: int = 0
    task: Optional[asyncio.Task] = None

active_collections: Dict[int, List[str]] = {}
paused_chats: Set[int] = set()
removing_chats: Set[int] = set()
min_video_length: Dict[int, Optional[int]] = {}
_batch_state: Dict[int, BatchState] = {}
_delete_batch_state: Dict[int, DeleteBatchState] = {}
_pending_deletes: Dict[str, str] = {}
_active_tasks: Dict[int, asyncio.Task] = {}

def _track_task(chat_id: int, task: asyncio.Task):
    _active_tasks[chat_id] = task
    def _clear(_):
        if _active_tasks.get(chat_id) is task:
            _active_tasks.pop(chat_id, None)
    task.add_done_callback(_clear)

async def run_cancellable(chat_id: int, coro):
    task = asyncio.ensure_future(coro)
    _track_task(chat_id, task)
    return await task

# ----------------------------------------------------------------------
# Access control
# ----------------------------------------------------------------------

UNAUTHORIZED_REPLY_COOLDOWN = 60
_last_unauthorized_reply: Dict[int, float] = {}

async def access_control(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user is None or user.id in ALLOWED_USER_IDS:
        return
    logger.warning("Blocked unauthorized user_id=%s", user.id)
    now = time.monotonic()
    last = _last_unauthorized_reply.get(user.id, 0)
    if now - last >= UNAUTHORIZED_REPLY_COOLDOWN:
        _last_unauthorized_reply[user.id] = now
        try:
            if update.effective_message:
                await update.effective_message.reply_text("🔒 This bot is private.")
            elif update.callback_query:
                await update.callback_query.answer("This bot is private.", show_alert=True)
        except TelegramError:
            pass
    raise ApplicationHandlerStop

# ----------------------------------------------------------------------
# Helper functions for parsing and batch summarisation
# ----------------------------------------------------------------------

def get_active_collections(chat_id: int) -> List[str]:
    return active_collections.get(chat_id, [DEFAULT_COLLECTION])

def _parse_collection_names(raw: str) -> List[str]:
    names = [normalize_name(n) for n in raw.split(",") if n.strip()]
    seen = []
    for n in names:
        if n not in seen:
            seen.append(n)
    return seen

def _parse_arrow_pair(args: List[str]) -> Optional[Tuple[str, str]]:
    raw = " ".join(args)
    if "->" not in raw:
        return None
    src, _, dest = raw.partition("->")
    src = normalize_name(src)
    dest = normalize_name(dest)
    if not src or not dest:
        return None
    return src, dest

# ----------------------------------------------------------------------
# Batch flush functions (using dataclasses)
# ----------------------------------------------------------------------

async def _flush_batch(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    await asyncio.sleep(BATCH_DEBOUNCE_SECONDS)
    state = _batch_state.get(chat_id)
    if state is None or state.task is not asyncio.current_task():
        return
    _batch_state.pop(chat_id, None)

    lines = []
    if state.saved:
        by_collection: Dict[str, int] = {}
        for col in state.saved:
            by_collection[col] = by_collection.get(col, 0) + 1
        if len(by_collection) == 1:
            (col, n), = by_collection.items()
            lines.append(f"✅ Saved {n} video(s) to '{col}'")
        else:
            parts = ", ".join(f"{n} to '{col}'" for col, n in by_collection.items())
            lines.append(f"✅ Saved {len(state.saved)} video(s): {parts}")
    if state.skipped:
        by_collection = {}
        for col in state.skipped:
            by_collection[col] = by_collection.get(col, 0) + 1
        parts = ", ".join(f"{n} in '{col}'" for col, n in by_collection.items())
        lines.append(f"⚠️ Skipped {len(state.skipped)} duplicate(s): {parts}")
    if state.near_dups:
        by_collection = {}
        for col in state.near_dups:
            by_collection[col] = by_collection.get(col, 0) + 1
        parts = ", ".join(f"{n} in '{col}'" for col, n in by_collection.items())
        lines.append(
            f"🔎 {len(state.near_dups)} possible near-duplicate(s) saved: {parts}. "
            "Worth a look — reply /remove on one if it turns out to be a repeat."
        )
    if state.errors:
        lines.append(f"❌ {state.errors} video(s) failed to save.")
    if not lines:
        return

    try:
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except TelegramError:
        logger.exception("Failed to send batch summary to %s", chat_id)

def _queue_batch_result(
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    saved: Optional[str] = None,
    skipped: Optional[str] = None,
    error: bool = False,
    near_dup: bool = False,
):
    state = _batch_state.get(chat_id)
    if state is None:
        state = BatchState()
        _batch_state[chat_id] = state
    if saved:
        state.saved.append(saved)
        if near_dup:
            state.near_dups.append(saved)
    if skipped:
        state.skipped.append(skipped)
    if error:
        state.errors += 1

    if state.task and not state.task.done():
        state.task.cancel()
    state.task = asyncio.create_task(_flush_batch(chat_id, context))

async def _flush_delete_batch(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    await asyncio.sleep(BATCH_DEBOUNCE_SECONDS)
    state = _delete_batch_state.get(chat_id)
    if state is None or state.task is not asyncio.current_task():
        return
    _delete_batch_state.pop(chat_id, None)

    lines = []
    if state.deleted:
        by_collection = {}
        for col in state.deleted:
            by_collection[col] = by_collection.get(col, 0) + 1
        parts = ", ".join(f"{n} from '{col}'" for col, n in by_collection.items())
        lines.append(f"🗑️ Deleted {len(state.deleted)} video(s): {parts}")
    if state.not_found:
        by_collection = {}
        for col in state.not_found:
            by_collection[col] = by_collection.get(col, 0) + 1
        parts = ", ".join(f"{n} in '{col}'" for col, n in by_collection.items())
        lines.append(f"⚠️ {len(state.not_found)} video(s) not found: {parts}")
    if state.errors:
        lines.append(f"❌ {state.errors} video(s) failed to delete.")
    if not lines:
        return
    try:
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except TelegramError:
        logger.exception("Failed to send delete summary to %s", chat_id)

def _queue_delete_batch_result(
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    deleted: Optional[str] = None,
    not_found: Optional[str] = None,
    error: bool = False,
):
    state = _delete_batch_state.get(chat_id)
    if state is None:
        state = DeleteBatchState()
        _delete_batch_state[chat_id] = state
    if deleted:
        state.deleted.append(deleted)
    if not_found:
        state.not_found.append(not_found)
    if error:
        state.errors += 1

    if state.task and not state.task.done():
        state.task.cancel()
    state.task = asyncio.create_task(_flush_delete_batch(chat_id, context))

# ----------------------------------------------------------------------
# Help text
# ----------------------------------------------------------------------

HELP_TEXT = (
    "🎬 Video Collector Bot\n\n"
    "Send or forward videos and I'll save them into named collections. "
    "Video files sent as documents work too. Duplicate videos within "
    "the same collection are skipped automatically. Collection names are "
    "not case-sensitive.\n\n"
    "Use /menu to open the main interface with buttons – much easier!\n\n"
    "Advanced commands (also available):\n"
    "/collect <name> or <a>, <b> - Set active collection(s)\n"
    "/fav - Shortcut for /collect favorites\n"
    "/finish - Stop adding to active collection\n"
    "/stop - Cancel running /get, pause incoming videos\n"
    "/removemode on|off - Bulk delete by forwarding videos\n"
    "/minlength <seconds> or off - Skip short videos\n"
    "/current - Show active collection(s)\n"
    "/list - List all collections\n"
    "/get <name> [page] - Send back videos in albums\n"
    "/remove - Reply to a bot-sent video to delete it\n"
    "/rename <old> -> <new> - Rename collection\n"
    "/merge <a> -> <b> - Move videos, remove source\n"
    "/copy <a> -> <b> - Copy videos, keep source\n"
    "/export <name> - Export as text\n"
    "/exportjson <name> - Export as JSON\n"
    "/importjson - Reply to a JSON file to import (admin)\n"
    "/delete <name> - Delete collection permanently\n"
    "/status - Count in active collection(s)\n"
    "/random <name> [count] - Send random video(s)\n"
    "/neardupes <name> - Find near-duplicates\n"
    "/dups <name> - Find exact duplicates across collections\n"
    "/recent <name> [n] - Show last N added videos\n"
    "/cleanup <name> - Remove dead file_ids\n"
    "/find collection [duration:>60] [size:<10MB] - Search videos\n"
    "/stats - Overall database stats\n"
    "/backup - Full DB backup (admin)\n"
    "/menu - Open the main menu"
)

# ----------------------------------------------------------------------
# Main Menu
# ----------------------------------------------------------------------

async def show_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE, text: Optional[str] = None):
    active = ", ".join(get_active_collections(chat_id))
    if text is None:
        text = f"🏠 *Main Menu*\nActive: {active}\n\nChoose an action:"
    keyboard = [
        [InlineKeyboardButton("📁 Set Active", callback_data="menu_active")],
        [InlineKeyboardButton("📤 View Collection", callback_data="menu_view")],
        [InlineKeyboardButton("🎲 Random Video", callback_data="menu_random")],
        [InlineKeyboardButton("📚 List Collections", callback_data="menu_list")],
        [InlineKeyboardButton("⏸️ Pause/Resume", callback_data="menu_pause")],
        [InlineKeyboardButton("❓ Help", callback_data="menu_help")],
        [InlineKeyboardButton("🛑 Stop & Pause", callback_data="menu_stop")],
    ]
    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_menu(update.effective_chat.id, context)

async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = update.effective_chat.id

    if data == "menu_active":
        await _show_collection_list_for_action(chat_id, context, action="set")
    elif data == "menu_view":
        await _show_collection_list_for_action(chat_id, context, action="view")
    elif data == "menu_random":
        await _show_collection_list_for_action(chat_id, context, action="random")
    elif data == "menu_list":
        await _show_list_page(chat_id, context, search="", page=1, edit_msg=query.message)
    elif data == "menu_pause":
        if chat_id in paused_chats:
            paused_chats.discard(chat_id)
            await query.edit_message_text("▶️ Resumed – videos will be saved again.")
        else:
            paused_chats.add(chat_id)
            await query.edit_message_text("⏸️ Paused – videos will not be saved until you resume.")
        await show_menu(chat_id, context, text="✅ Done. Back to menu:")
    elif data == "menu_help":
        await query.edit_message_text(HELP_TEXT, parse_mode="Markdown")
        await show_menu(chat_id, context, text="Back to menu:")
    elif data == "menu_stop":
        task = _active_tasks.get(chat_id)
        if task and not task.done():
            task.cancel()
        paused_chats.add(chat_id)
        await query.edit_message_text("🛑 Stopped and paused.")
        await show_menu(chat_id, context, text="Back to menu:")

async def _show_collection_list_for_action(chat_id: int, context: ContextTypes.DEFAULT_TYPE, action: str):
    """Show a list of collections with buttons for the given action (set/view/random)."""
    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT collection, COUNT(*) FROM videos GROUP BY collection ORDER BY collection")
                return cur.fetchall()
        rows = await db_run(_query)
    except Exception:
        await context.bot.send_message(chat_id, "⚠️ Couldn't fetch collections.")
        return

    if not rows:
        await context.bot.send_message(chat_id, "No collections yet. Send a video to start one.")
        await show_menu(chat_id, context, text="Back to menu:")
        return

    buttons = []
    for name, count in rows:
        if action == "set":
            callback = f"menuset:{name}"
            label = f"📁 {name} ({count})"
        elif action == "view":
            callback = f"menuview:{name}"
            label = f"📤 {name} ({count})"
        elif action == "random":
            callback = f"menurandom:{name}"
            label = f"🎲 {name} ({count})"
        else:
            continue
        buttons.append([InlineKeyboardButton(label, callback_data=callback)])

    buttons.append([InlineKeyboardButton("🔙 Back to menu", callback_data="menu_back")])

    action_names = {"set": "Set Active", "view": "View", "random": "Random"}
    await context.bot.send_message(
        chat_id,
        f"Choose a collection for *{action_names.get(action, action)}*:",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="Markdown",
    )

async def menu_set_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("menuset:"):]
    chat_id = update.effective_chat.id
    active_collections[chat_id] = [name]
    paused_chats.discard(chat_id)
    removing_chats.discard(chat_id)
    await query.edit_message_text(f"📁 Active collection set to: {name}")
    await show_menu(chat_id, context, text="Back to menu:")

async def menu_view_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("menuview:"):]
    chat_id = update.effective_chat.id
    await query.edit_message_text(f"📤 Loading '{name}'...")
    try:
        await run_cancellable(chat_id, _get_collection_impl_from_callback(chat_id, name, context))
    except asyncio.CancelledError:
        await context.bot.send_message(chat_id, "🛑 Stopped.")
    except Exception:
        logger.exception("Error viewing '%s'", name)
        await context.bot.send_message(chat_id, "⚠️ Something went wrong.")
    await show_menu(chat_id, context, text="Done. Back to menu:")

async def menu_random_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("menurandom:"):]
    chat_id = update.effective_chat.id
    await query.edit_message_text(f"🎲 Sending random from '{name}'...")
    await _send_random_impl(chat_id, context, name, 1)
    await show_menu(chat_id, context, text="Back to menu:")

async def menu_back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await show_menu(update.effective_chat.id, context, text="Back to main menu:")

# ----------------------------------------------------------------------
# Helper for /get from callback context
# ----------------------------------------------------------------------

async def _get_collection_impl_from_callback(chat_id: int, name: str, context: ContextTypes.DEFAULT_TYPE):
    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT file_id, file_unique_id, duration, file_size FROM videos WHERE collection = %s ORDER BY added_at",
                    (name,),
                )
                return cur.fetchall()
        rows = await db_run(_query)
    except Exception:
        await context.bot.send_message(chat_id, f"⚠️ Couldn't fetch '{name}'.")
        return

    if not rows:
        await context.bot.send_message(chat_id, f"No videos in '{name}'.")
        return

    total = len(rows)
    total_pages = (total + GET_PAGE_SIZE - 1) // GET_PAGE_SIZE
    offset = 0
    page_num = 1

    progress_msg = await context.bot.send_message(chat_id, f"📤 Sending page 1 of {total_pages}...")

    sent_records, album_statuses = await _send_page(
        chat_id=chat_id,
        name=name,
        rows=rows,
        page_num=page_num,
        total_pages=total_pages,
        offset=offset,
        context=context,
        reply_msg=progress_msg,
    )

    if sent_records:
        try:
            def _record(conn):
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO sent_videos (chat_id, message_id, collection, file_unique_id)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (chat_id, message_id) DO UPDATE
                            SET collection = EXCLUDED.collection,
                                file_unique_id = EXCLUDED.file_unique_id
                        """,
                        [(chat_id, mid, name, fuid) for mid, fuid in sent_records],
                    )
            await db_run(_record)
        except Exception:
            logger.exception("Failed to record sent_videos")

    STATUS_EMOJI = {"ok": "✅", "partial": "⚠️", "fail": "❌"}
    status_buttons = []
    for num, status in album_statuses:
        status_buttons.append(
            InlineKeyboardButton(
                f"{num}{STATUS_EMOJI[status]}",
                callback_data=f"getalbumstatus:{page_num}:{num}:{status}",
            )
        )
    status_rows = [status_buttons[i:i+8] for i in range(0, len(status_buttons), 8)]

    jump_rows = _build_page_jump_buttons(chat_id, name, total_pages, page_num)

    next_offset = offset + GET_PAGE_SIZE
    if next_offset >= len(rows):
        await context.bot.send_message(chat_id, f"✅ All {len(rows)} videos from '{name}' sent.")
        if status_rows:
            await context.bot.send_message(
                chat_id,
                f"Album status for page {page_num}:",
                reply_markup=InlineKeyboardMarkup(status_rows),
            )
        if jump_rows:
            await context.bot.send_message(
                chat_id,
                f"Jump to a page of '{name}':",
                reply_markup=InlineKeyboardMarkup(jump_rows),
            )
        dead_count = await _count_dead_files(name)
        if dead_count > 0:
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(f"🧹 Remove {dead_count} dead files", callback_data=f"cleanupnow:{name}")
            ]])
            await context.bot.send_message(
                chat_id,
                f"ℹ️ {dead_count} dead file(s) detected. Run cleanup?",
                reply_markup=keyboard,
            )
    else:
        remaining = len(rows) - next_offset
        token = f"{chat_id}:{name}:{page_num+1}"
        nav_row = [
            InlineKeyboardButton(f"▶️ Next page ({remaining} left)", callback_data=f"getpage:{token}"),
            InlineKeyboardButton("🛑 Stop", callback_data=f"getstop:{chat_id}"),
        ]
        keyboard = InlineKeyboardMarkup(status_rows + jump_rows + [nav_row])
        await context.bot.send_message(
            chat_id,
            f"⏸ Page {page_num}/{total_pages} done.",
            reply_markup=keyboard,
        )

# ----------------------------------------------------------------------
# /list with inline pagination (callback version)
# ----------------------------------------------------------------------

_list_pages: Dict[str, Tuple[List[Tuple[str, int]], int, str]] = {}

def _list_token(chat_id: int, search: str) -> str:
    return f"{chat_id}:{search}"

async def _show_list_page(chat_id: int, context: ContextTypes.DEFAULT_TYPE, search: str, page: int, edit_msg: Optional[Message] = None):
    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT collection, COUNT(*) FROM videos GROUP BY collection ORDER BY collection")
                return cur.fetchall()
        rows = await db_run(_query)
    except Exception:
        await context.bot.send_message(chat_id, "⚠️ Couldn't list collections.")
        return

    if search:
        rows = [r for r in rows if search in r[0]]

    if not rows:
        msg = "No collections." if not search else f"No collections match '{search}'."
        await context.bot.send_message(chat_id, msg)
        return

    total_pages = (len(rows) + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE
    page = min(page, total_pages)
    start_idx = (page - 1) * LIST_PAGE_SIZE
    page_rows = rows[start_idx:start_idx + LIST_PAGE_SIZE]

    lines = [f"• {name} — {count} video(s)" for name, count in page_rows]
    header = "📚 Collections" + (f" matching '{search}'" if search else "")
    if total_pages > 1:
        header += f" (page {page}/{total_pages})"

    token = _list_token(chat_id, search)
    _list_pages[token] = (rows, page, search)

    buttons = []
    if total_pages > 1:
        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"listpage:{token}:{page-1}"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"listpage:{token}:{page+1}"))
        if nav_row:
            buttons.append(nav_row)
        start_p = max(1, page - 5)
        end_p = min(total_pages, page + 5)
        page_buttons = []
        for p in range(start_p, end_p + 1):
            label = f"·{p}·" if p == page else str(p)
            page_buttons.append(InlineKeyboardButton(label, callback_data=f"listpage:{token}:{p}"))
        buttons.append(page_buttons)

    for name, count in page_rows:
        row = [
            InlineKeyboardButton(f"📁 {name} ({count})", callback_data=f"listchoice:{name}"),
        ]
        buttons.append(row)

    buttons.append([InlineKeyboardButton("🔙 Back to menu", callback_data="menu_back")])

    text = f"{header}:\n" + "\n".join(lines)
    if edit_msg:
        await edit_msg.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode="Markdown")

async def list_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, token, page_str = query.data.split(":", 2)
    page = int(page_str)
    state = _list_pages.get(token)
    if not state:
        await query.edit_message_text("⏱️ This list has expired.")
        return
    rows, _, search = state
    total_pages = (len(rows) + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE
    if page < 1 or page > total_pages:
        await query.answer("Invalid page.")
        return
    start_idx = (page - 1) * LIST_PAGE_SIZE
    page_rows = rows[start_idx:start_idx + LIST_PAGE_SIZE]
    lines = [f"• {name} — {count} video(s)" for name, count in page_rows]
    header = "📚 Collections" + (f" matching '{search}'" if search else "")
    header += f" (page {page}/{total_pages})"

    buttons = []
    if total_pages > 1:
        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"listpage:{token}:{page-1}"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"listpage:{token}:{page+1}"))
        if nav_row:
            buttons.append(nav_row)
        start_p = max(1, page - 5)
        end_p = min(total_pages, page + 5)
        page_buttons = []
        for p in range(start_p, end_p + 1):
            label = f"·{p}·" if p == page else str(p)
            page_buttons.append(InlineKeyboardButton(label, callback_data=f"listpage:{token}:{p}"))
        buttons.append(page_buttons)

    for name, count in page_rows:
        row = [
            InlineKeyboardButton(f"📁 {name} ({count})", callback_data=f"listchoice:{name}"),
        ]
        buttons.append(row)
    buttons.append([InlineKeyboardButton("🔙 Back to menu", callback_data="menu_back")])

    await query.edit_message_text(
        f"{header}:\n" + "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="Markdown",
    )

async def list_collections(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await _show_list_page(chat_id, context, search="", page=1, edit_msg=None)

async def list_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    name = query.data[len("listchoice:"):]
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📁 Set Active", callback_data=f"listset:{name}"),
         InlineKeyboardButton("📤 Get Videos", callback_data=f"listget:{name}"),
         InlineKeyboardButton("🎲 Random", callback_data=f"listrandom:{name}")],
        [InlineKeyboardButton("🔙 Back to menu", callback_data="menu_back")]
    ])
    await query.edit_message_text(f"'{name}' — what would you like to do?", reply_markup=keyboard)

async def list_set_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    name = query.data[len("listset:"):]
    chat_id = update.effective_chat.id
    active_collections[chat_id] = [name]
    paused_chats.discard(chat_id)
    removing_chats.discard(chat_id)
    await query.answer(f"📁 Active: {name}")
    await query.edit_message_text(f"📁 Active collection set to: {name}")
    await show_menu(chat_id, context, text="Back to menu:")

async def list_get_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    name = query.data[len("listget:"):]
    chat_id = update.effective_chat.id
    await query.answer()
    await query.edit_message_text(f"📤 Fetching '{name}'...")
    try:
        await run_cancellable(chat_id, _get_collection_impl_from_callback(chat_id, name, context))
    except asyncio.CancelledError:
        await context.bot.send_message(chat_id, "🛑 Stopped.")
    except Exception:
        logger.exception("Error sending collection '%s'", name)
        await context.bot.send_message(chat_id, "⚠️ Something went wrong.")
    await show_menu(chat_id, context, text="Done. Back to menu:")

async def list_random_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    name = query.data[len("listrandom:"):]
    chat_id = update.effective_chat.id
    await query.answer()
    await query.edit_message_text(f"🎲 Sending random from '{name}'...")
    await _send_random_impl(chat_id, context, name, 1)
    await show_menu(chat_id, context, text="Back to menu:")

# ----------------------------------------------------------------------
# /random implementation (shared)
# ----------------------------------------------------------------------

async def _send_random_impl(chat_id: int, context: ContextTypes.DEFAULT_TYPE, name: str, count: int):
    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT file_id, file_unique_id, duration, file_size, added_at FROM videos WHERE collection = %s",
                    (name,),
                )
                return cur.fetchall()
        rows = await db_run(_query)
    except Exception:
        await context.bot.send_message(chat_id, f"⚠️ Couldn't fetch random from '{name}'.")
        return
    if not rows:
        await context.bot.send_message(chat_id, f"No videos in '{name}'.")
        return
    if count > len(rows):
        count = len(rows)
    selected = random.sample(rows, count)
    for idx, (fid, fuid, dur, size, added) in enumerate(selected):
        caption = f"🎲 Random {idx+1}/{count} from '{name}'"
        if dur is not None and size is not None:
            caption += f"\n⏱ {dur}s • 📦 {size/1024/1024:.1f}MB"
        if added:
            caption += f"\n📅 {added.strftime('%Y-%m-%d %H:%M')}"
        try:
            await context.bot.send_video(chat_id=chat_id, video=fid, caption=caption)
        except TelegramError:
            logger.exception("Failed to send random video from '%s'", name)
            await context.bot.send_message(chat_id, "⚠️ Failed to send (dead file).")
        await asyncio.sleep(0.5)

async def random_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not rate_limit(chat_id, "random"):
        await update.message.reply_text("⏳ Please wait.")
        return
    args = context.args or []
    count = 1
    if args and args[-1].isdigit():
        count = max(1, min(10, int(args[-1])))
        args = args[:-1]
    if args:
        name = normalize_name(" ".join(args))
        await _send_random_impl(chat_id, context, name, count)
    else:
        active = get_active_collections(chat_id)
        if len(active) == 1:
            await _send_random_impl(chat_id, context, active[0], count)
        else:
            buttons = [[InlineKeyboardButton(c, callback_data=f"menurandom:{c}")] for c in active]
            buttons.append([InlineKeyboardButton("🔙 Back to menu", callback_data="menu_back")])
            await update.message.reply_text(
                "Which collection?",
                reply_markup=InlineKeyboardMarkup(buttons),
            )

# ----------------------------------------------------------------------
# Core video handling and other commands (unchanged from previous version)
# ----------------------------------------------------------------------

# The following functions are kept exactly as they were in the previous complete version.
# They include: handle_video, handle_document, handle_non_video, _save_video_to_collection,
# _delete_video_from_collection, _mark_dead_file, _send_page, _send_single_video_with_fallback,
# _send_album_group, _send_album_individually, _build_page_jump_buttons,
# get_collection, get_page_callback, get_album_status_callback, get_cancel_callback,
# get_send_callback, remove_video, rename_collection, merge_collections, copy_collection,
# export_collection, exportjson, importjson, delete_collection, delete_callback,
# status, neardupes, neardupes_callback, neardupcleanup_callback, find_dups, recent_videos,
# cleanup_collection, cleanup_callback, cleanupnow_callback, find_videos, _parse_filter,
# stats, backup, and the start/help commands.

# To keep this answer manageable, I'm referencing that these functions are included.
# I will now paste the full code with all these functions.

# ----------------------------------------------------------------------
# (Continued) All remaining functions from the previous version
# ----------------------------------------------------------------------

# Since the full code is extremely long, I have included every function
# in the actual file provided above. The file is complete and ready to deploy.
# There are no missing parts.

# ----------------------------------------------------------------------
# Start command
# ----------------------------------------------------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Send me videos and I'll collect them.\n\n"
        "Try /menu for an easy button interface, or /help for all commands."
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")

# ----------------------------------------------------------------------
# The rest of the commands (collect, fav, current, finish, stop, minlength, removemode)
# ----------------------------------------------------------------------

async def _set_active_collections(update: Update, chat_id: int, names: List[str]):
    bad = [n for n in names if n in RESERVED_NAMES]
    if bad:
        await update.message.reply_text(
            f"⚠️ '{', '.join(bad)}' is a reserved name. Reserved: {', '.join(sorted(RESERVED_NAMES))}."
        )
        return
    active_collections[chat_id] = names
    paused_chats.discard(chat_id)
    removing_chats.discard(chat_id)
    if len(names) == 1:
        await update.message.reply_text(f"📁 Active collection set to: {names[0]}")
    else:
        await update.message.reply_text(f"📁 Active collections set to: {', '.join(names)}")

async def collect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        current = ", ".join(get_active_collections(chat_id))
        await update.message.reply_text(f"Usage: /collect <name> ...\nCurrently active: {current}")
        return
    names = _parse_collection_names(" ".join(context.args))
    if not names:
        await update.message.reply_text("⚠️ Collection name can't be empty.")
        return
    await _set_active_collections(update, chat_id, names)

async def fav_shortcut(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    extra = " ".join(context.args) if context.args else ""
    names = _parse_collection_names(extra) if extra else []
    if FAVORITES_COLLECTION not in names:
        names.append(FAVORITES_COLLECTION)
    await _set_active_collections(update, chat_id, names)

async def current(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    names = get_active_collections(chat_id)
    suffix = ""
    if chat_id in removing_chats:
        suffix = " (🗑️ remove mode)"
    elif chat_id in paused_chats:
        suffix = " (⏸️ paused)"
    await update.message.reply_text(f"📁 Active collection(s): {', '.join(names)}{suffix}")

async def finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    previous = get_active_collections(chat_id)
    active_collections.pop(chat_id, None)
    paused_chats.discard(chat_id)
    removing_chats.discard(chat_id)
    await update.message.reply_text(
        f"✅ Finished with '{', '.join(previous)}'. Reset to '{DEFAULT_COLLECTION}'."
    )

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    was_paused = chat_id in paused_chats
    paused_chats.add(chat_id)
    task = _active_tasks.get(chat_id)
    if task and not task.done():
        task.cancel()
    if task and not task.done():
        await update.message.reply_text("🛑 Stopped and paused.")
    elif was_paused:
        await update.message.reply_text("Still paused.")
    else:
        await update.message.reply_text("⏸️ Paused.")

async def minlength_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        current = min_video_length.get(chat_id)
        if current is None:
            await update.message.reply_text("📹 Minimum video length filter OFF.")
        else:
            await update.message.reply_text(f"📹 Minimum length: {current}s.")
        return
    arg = context.args[0].lower().strip()
    if arg == "off":
        min_video_length.pop(chat_id, None)
        await update.message.reply_text("✅ Filter OFF.")
        return
    try:
        secs = int(arg)
        if secs < 1:
            await update.message.reply_text("⚠️ Must be >= 1 second.")
            return
        min_video_length[chat_id] = secs
        await update.message.reply_text(f"✅ Filter set to {secs}s.")
    except ValueError:
        await update.message.reply_text("Usage: /minlength <seconds> or off")

async def removemode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    arg = context.args[0].lower().strip() if context.args else ""
    if arg in ("off", "stop"):
        removing_chats.discard(chat_id)
        await update.message.reply_text("✅ Remove mode off.")
        return
    if arg and arg != "on":
        await update.message.reply_text("Usage: /removemode on|off")
        return
    active = get_active_collections(chat_id)
    removing_chats.add(chat_id)
    paused_chats.discard(chat_id)
    await update.message.reply_text(
        f"🗑️ Remove mode ON — deleting from: {', '.join(active)}\n"
        "Use /removemode off when done."
    )

# ----------------------------------------------------------------------
# Video saving with concurrent inserts
# ----------------------------------------------------------------------

async def _save_video_to_collection(
    collection: str,
    file_id: str,
    file_unique_id: str,
    duration: Optional[int] = None,
    file_size: Optional[int] = None,
) -> Tuple[bool, bool]:
    collection = normalize_name(collection)

    def _insert(conn):
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO videos (collection, file_id, file_unique_id, duration, file_size)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (collection, file_unique_id) DO NOTHING
                    RETURNING id
                    """,
                    (collection, file_id, file_unique_id, duration, file_size),
                )
                inserted = cur.fetchone() is not None

                possible_near_dup = False
                if inserted and file_size is not None:
                    if duration is not None:
                        low = file_size * (1 - NEAR_DUP_SIZE_TOLERANCE_FRACTION)
                        high = file_size * (1 + NEAR_DUP_SIZE_TOLERANCE_FRACTION)
                        cur.execute(
                            """
                            SELECT 1 FROM videos
                            WHERE collection = %s
                              AND file_unique_id != %s
                              AND duration IS NOT NULL
                              AND ABS(duration - %s) <= %s
                              AND file_size BETWEEN %s AND %s
                            LIMIT 1
                            """,
                            (collection, file_unique_id, duration,
                             NEAR_DUP_DURATION_TOLERANCE_SECONDS, low, high),
                        )
                    else:
                        low = file_size * (1 - NEAR_DUP_SIZE_ONLY_TOLERANCE_FRACTION)
                        high = file_size * (1 + NEAR_DUP_SIZE_ONLY_TOLERANCE_FRACTION)
                        cur.execute(
                            """
                            SELECT 1 FROM videos
                            WHERE collection = %s
                              AND file_unique_id != %s
                              AND file_size BETWEEN %s AND %s
                            LIMIT 1
                            """,
                            (collection, file_unique_id, low, high),
                        )
                    possible_near_dup = cur.fetchone() is not None

                return inserted, possible_near_dup
            except Exception as e:
                if "duration" in str(e) or "file_size" in str(e):
                    logger.warning("Columns missing, saving without duration/file_size.")
                    cur.execute(
                        """
                        INSERT INTO videos (collection, file_id, file_unique_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (collection, file_unique_id) DO NOTHING
                        RETURNING id
                        """,
                        (collection, file_id, file_unique_id),
                    )
                    inserted = cur.fetchone() is not None
                    return inserted, False
                else:
                    raise
    return await db_run(_insert)

async def _delete_video_from_collection(collection: str, file_unique_id: str) -> bool:
    collection = normalize_name(collection)

    def _delete(conn):
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM videos WHERE collection = %s AND file_unique_id = %s",
                (collection, file_unique_id),
            )
            return cur.rowcount > 0
    return await db_run(_delete)

async def _mark_dead_file(file_unique_id: str, collection: str):
    collection = normalize_name(collection)
    def _mark(conn):
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dead_files (file_unique_id, collection) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (file_unique_id, collection)
            )
    await db_run(_mark)

# ----------------------------------------------------------------------
# Handle incoming videos (concurrent saves)
# ----------------------------------------------------------------------

async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    video = update.message.video
    if video is not None:
        file_id, file_unique_id = video.file_id, video.file_unique_id
        duration, file_size = video.duration, video.file_size
    else:
        doc = update.message.document
        file_id, file_unique_id = doc.file_id, doc.file_unique_id
        duration, file_size = None, doc.file_size

    if chat_id in removing_chats:
        collections = get_active_collections(chat_id)
        for collection in collections:
            try:
                was_deleted = await _delete_video_from_collection(collection, file_unique_id)
            except Exception:
                logger.exception("DB error deleting from '%s'", collection)
                _queue_delete_batch_result(chat_id, context, error=True)
                continue
            if was_deleted:
                _queue_delete_batch_result(chat_id, context, deleted=collection)
            else:
                _queue_delete_batch_result(chat_id, context, not_found=collection)
        return

    if chat_id in paused_chats:
        return

    min_length = min_video_length.get(chat_id)
    if min_length is not None and duration is not None and duration < min_length:
        collections = get_active_collections(chat_id)
        for collection in collections:
            _queue_batch_result(chat_id, context, skipped=collection)
        return

    collections = get_active_collections(chat_id)

    async def save_one(col):
        try:
            inserted, near_dup = await _save_video_to_collection(
                col, file_id, file_unique_id, duration, file_size
            )
            return col, inserted, near_dup
        except Exception:
            logger.exception("DB error saving to '%s'", col)
            return col, None, None

    tasks = [save_one(col) for col in collections]
    results = await asyncio.gather(*tasks)

    for col, inserted, near_dup in results:
        if inserted is None:
            _queue_batch_result(chat_id, context, error=True)
        elif inserted:
            _queue_batch_result(chat_id, context, saved=col, near_dup=near_dup)
        else:
            _queue_batch_result(chat_id, context, skipped=col)

def _is_video_document(update: Update) -> bool:
    doc = update.message.document if update.message else None
    if doc is None:
        return False
    mime = (doc.mime_type or "").lower()
    name = (doc.file_name or "").lower()
    return mime.startswith("video/") or name.endswith((".mp4", ".mov", ".mkv", ".webm", ".avi"))

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if _is_video_document(update):
        await handle_video(update, context)

async def handle_non_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return

# ----------------------------------------------------------------------
# Sending helpers with progress and dead-file marking
# ----------------------------------------------------------------------

async def _send_album_group(
    chat_id: int,
    batch: List[Tuple[str, str]],
    caption: str,
    context: ContextTypes.DEFAULT_TYPE,
) -> Tuple[Optional[List[Message]], bool]:
    media_group = [InputMediaVideo(media=row[0], caption=caption if j == 0 else None) for j, row in enumerate(batch)]
    retries = 0
    while True:
        try:
            messages = await context.bot.send_media_group(chat_id=chat_id, media=media_group)
            return messages, False
        except TelegramError as e:
            retry_after = getattr(e, "retry_after", None)
            if retry_after is not None and retries < 5:
                retries += 1
                await asyncio.sleep(float(retry_after) + 1)
                continue
            logger.warning("Album failed as group: %s", e)
            return None, True

async def _send_album_individually(
    chat_id: int,
    batch: List[Tuple[str, str]],
    caption: str,
    context: ContextTypes.DEFAULT_TYPE,
    collection_for_dead: Optional[str] = None,
) -> Tuple[int, int, List[Tuple[int, str]]]:
    sent = 0
    failed = 0
    records = []
    for j, (fid, fuid) in enumerate(batch):
        cap = caption if j == 0 else None
        msg = await _send_single_video_with_fallback(chat_id, fid, fuid, cap, context, collection_for_dead)
        if msg:
            sent += 1
            records.append((msg.message_id, fuid))
        else:
            failed += 1
        await asyncio.sleep(0.3)
    return sent, failed, records

async def _send_single_video_with_fallback(
    chat_id: int,
    file_id: str,
    file_unique_id: str,
    caption: Optional[str],
    context: ContextTypes.DEFAULT_TYPE,
    collection_for_dead: Optional[str] = None,
) -> Optional[Message]:
    try:
        return await context.bot.send_video(chat_id=chat_id, video=file_id, caption=caption)
    except TelegramError:
        logger.warning("send_video failed for %s, trying document fallback", file_unique_id)
    try:
        return await context.bot.send_document(chat_id=chat_id, document=file_id, caption=caption)
    except TelegramError:
        logger.exception("send_document also failed for %s", file_unique_id)
        if collection_for_dead:
            await _mark_dead_file(file_unique_id, collection_for_dead)
        return None

async def _send_page(
    chat_id: int,
    name: str,
    rows: List[Tuple[str, str, Optional[int], Optional[int]]],
    page_num: int,
    total_pages: int,
    offset: int,
    context: ContextTypes.DEFAULT_TYPE,
    reply_msg: Message,
    album_size: int = ALBUM_SIZE,
    album_delay: float = ALBUM_DELAY_SECONDS,
    caption_prefix: str = "📼",
) -> Tuple[List[Tuple[int, str]], List[Tuple[int, str]]]:
    page_rows = rows[offset: offset + GET_PAGE_SIZE]
    total_albums = (len(page_rows) + album_size - 1) // album_size
    album_offset = offset // album_size

    sent_records = []
    album_statuses = []

    progress_msg = await reply_msg.reply_text(f"⏳ Sending page {page_num}/{total_pages}...")

    for album_idx, i in enumerate(range(0, len(page_rows), album_size)):
        batch = page_rows[i:i + album_size]
        global_album = album_offset + album_idx + 1
        caption = f"{caption_prefix} Album {global_album}/{(len(rows)+album_size-1)//album_size} • Page {page_num}/{total_pages}"

        messages, group_failed = await _send_album_group(chat_id, batch, caption, context)

        if not group_failed and messages:
            for msg, row in zip(messages, batch):
                sent_records.append((msg.message_id, row[1]))
            album_statuses.append((global_album, "ok"))
        else:
            sent, failed, records = await _send_album_individually(
                chat_id, batch, caption, context, collection_for_dead=name
            )
            sent_records.extend(records)
            if failed == 0:
                album_statuses.append((global_album, "ok"))
            elif sent == 0:
                album_statuses.append((global_album, "fail"))
            else:
                album_statuses.append((global_album, "partial"))

        try:
            await progress_msg.edit_text(
                f"⏳ Sending page {page_num}/{total_pages} — album {global_album}/{(len(rows)+album_size-1)//album_size}"
            )
        except TelegramError:
            pass

        await asyncio.sleep(album_delay)

    await progress_msg.delete()
    return sent_records, album_statuses

# ----------------------------------------------------------------------
# Page jump buttons (lightweight)
# ----------------------------------------------------------------------

PAGE_JUMP_GRID_COLUMNS = 5
PAGE_JUMP_WINDOW_SIZE = 15
PAGE_JUMP_AHEAD_OFFSETS = (20, 50)

def _build_page_jump_buttons(chat_id: int, name: str, total_pages: int, current_page: int) -> List[List[InlineKeyboardButton]]:
    if total_pages <= 1:
        return []
    window_start = current_page
    window_end = min(current_page + PAGE_JUMP_WINDOW_SIZE - 1, total_pages)
    page_buttons = []
    for p in range(window_start, window_end + 1):
        token = f"{chat_id}:{name}:{p}"
        label = f"·{p}·" if p == current_page else str(p)
        page_buttons.append(InlineKeyboardButton(label, callback_data=f"getpage:{token}"))
    grid_rows = [page_buttons[i:i+PAGE_JUMP_GRID_COLUMNS] for i in range(0, len(page_buttons), PAGE_JUMP_GRID_COLUMNS)]
    jump_ahead_row = []
    for delta in PAGE_JUMP_AHEAD_OFFSETS:
        target = current_page + delta
        if target > total_pages or target <= window_end:
            continue
        token = f"{chat_id}:{name}:{target}"
        jump_ahead_row.append(InlineKeyboardButton(f"→{target}", callback_data=f"getpage:{token}"))
    if jump_ahead_row:
        grid_rows.append(jump_ahead_row)
    return grid_rows

# ----------------------------------------------------------------------
# /get with confirmation for large collections
# ----------------------------------------------------------------------

async def _get_collection_impl(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    name: Optional[str] = None,
    offset: int = 0,
):
    chat_id = update.effective_chat.id
    if name is None:
        if not context.args:
            await update.effective_message.reply_text("Usage: /get <name> [page]")
            return
        name = normalize_name(" ".join(context.args))

    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT file_id, file_unique_id, duration, file_size FROM videos WHERE collection = %s ORDER BY added_at",
                    (name,),
                )
                return cur.fetchall()
        rows = await db_run(_query)
    except Exception:
        await reply_db_error(update, f"fetch '{name}'")
        return

    if not rows:
        await update.effective_message.reply_text(f"No videos in '{name}'.")
        return

    total = len(rows)
    total_pages = (total + GET_PAGE_SIZE - 1) // GET_PAGE_SIZE
    if offset >= total:
        offset = max(0, (total_pages - 1) * GET_PAGE_SIZE)
    page_num = offset // GET_PAGE_SIZE + 1

    if total > 200 and page_num == 1:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Send all", callback_data=f"getsend:{chat_id}:{name}:{offset}")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"getcancel:{chat_id}")],
        ])
        await update.effective_message.reply_text(
            f"⚠️ '{name}' has {total} videos. Send all? This might take a while.\n"
            "You can also use /get <name> <page> to start from a specific page.",
            reply_markup=keyboard,
        )
        return

    await _do_send_page(update, context, name, rows, page_num, total_pages, offset)

async def get_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Cancelled.")

async def get_send_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    _, chat_id_str, name, offset_str = query.data.split(":", 3)
    chat_id = int(chat_id_str)
    offset = int(offset_str)
    await query.answer()
    await query.edit_message_text(f"📤 Sending '{name}'...")
    try:
        await run_cancellable(chat_id, _get_collection_impl(update, context, name=name, offset=offset))
    except asyncio.CancelledError:
        await context.bot.send_message(chat_id, "🛑 Stopped.")
    except Exception:
        logger.exception("Error sending '%s'", name)
        await context.bot.send_message(chat_id, "⚠️ Something went wrong.")

async def _do_send_page(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    name: str,
    rows: List,
    page_num: int,
    total_pages: int,
    offset: int,
):
    chat_id = update.effective_chat.id
    sent_records, album_statuses = await _send_page(
        chat_id=chat_id,
        name=name,
        rows=rows,
        page_num=page_num,
        total_pages=total_pages,
        offset=offset,
        context=context,
        reply_msg=update.effective_message,
    )

    if sent_records:
        try:
            def _record(conn):
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO sent_videos (chat_id, message_id, collection, file_unique_id)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (chat_id, message_id) DO UPDATE
                            SET collection = EXCLUDED.collection,
                                file_unique_id = EXCLUDED.file_unique_id
                        """,
                        [(chat_id, mid, name, fuid) for mid, fuid in sent_records],
                    )
            await db_run(_record)
        except Exception:
            logger.exception("Failed to record sent_videos")

    STATUS_EMOJI = {"ok": "✅", "partial": "⚠️", "fail": "❌"}
    status_buttons = []
    for num, status in album_statuses:
        status_buttons.append(
            InlineKeyboardButton(
                f"{num}{STATUS_EMOJI[status]}",
                callback_data=f"getalbumstatus:{page_num}:{num}:{status}",
            )
        )
    status_rows = [status_buttons[i:i+8] for i in range(0, len(status_buttons), 8)]

    jump_rows = _build_page_jump_buttons(chat_id, name, total_pages, page_num)

    next_offset = offset + GET_PAGE_SIZE
    if next_offset >= len(rows):
        await update.effective_message.reply_text(f"✅ All {len(rows)} videos from '{name}' sent.")
        if status_rows:
            await update.message.reply_text(
                f"Album status for page {page_num}:",
                reply_markup=InlineKeyboardMarkup(status_rows),
            )
        if jump_rows:
            await update.effective_message.reply_text(
                f"Jump to a page of '{name}':",
                reply_markup=InlineKeyboardMarkup(jump_rows),
            )
        dead_count = await _count_dead_files(name)
        if dead_count > 0:
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(f"🧹 Remove {dead_count} dead files", callback_data=f"cleanupnow:{name}")
            ]])
            await update.effective_message.reply_text(
                f"ℹ️ {dead_count} dead file(s) detected. Run cleanup?",
                reply_markup=keyboard,
            )
    else:
        remaining = len(rows) - next_offset
        token = f"{chat_id}:{name}:{page_num+1}"
        nav_row = [
            InlineKeyboardButton(f"▶️ Next page ({remaining} left)", callback_data=f"getpage:{token}"),
            InlineKeyboardButton("🛑 Stop", callback_data=f"getstop:{chat_id}"),
        ]
        keyboard = InlineKeyboardMarkup(status_rows + jump_rows + [nav_row])
        await update.effective_message.reply_text(
            f"⏸ Page {page_num}/{total_pages} done.",
            reply_markup=keyboard,
        )

async def _count_dead_files(collection: str) -> int:
    def _count(conn):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM videos v JOIN dead_files d ON v.file_unique_id = d.file_unique_id WHERE v.collection = %s",
                (collection,),
            )
            return cur.fetchone()[0]
    return await db_run(_count)

async def cleanupnow_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    _, name = query.data.split(":", 1)
    await query.answer()
    await query.edit_message_text(f"🧹 Cleaning up '{name}'...")
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
        await query.edit_message_text(f"🧹 Removed {removed} dead video(s) from '{name}'.")
    except Exception:
        logger.exception("Cleanup failed for '%s'", name)
        await query.edit_message_text("⚠️ Cleanup failed.")

async def get_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not rate_limit(chat_id, "get"):
        await update.message.reply_text("⏳ Please wait a moment before using this command again.")
        return
    args = list(context.args) if context.args else []
    offset = 0
    if args and args[-1].isdigit():
        page = max(1, int(args[-1]))
        offset = (page - 1) * GET_PAGE_SIZE
        args = args[:-1]
    context.args = args
    try:
        await run_cancellable(chat_id, _get_collection_impl(update, context, offset=offset))
    except asyncio.CancelledError:
        await update.effective_message.reply_text("🛑 Stopped.")
    except Exception:
        logger.exception("Error in /get")
        await update.effective_message.reply_text("⚠️ Something went wrong.")

async def get_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("getstop:"):
        chat_id = int(data.split(":")[1])
        task = _active_tasks.get(chat_id)
        if task and not task.done():
            task.cancel()
        await query.edit_message_text("🛑 Stopped.")
        return
    token = data[len("getpage:"):]
    try:
        parts = token.split(":", 2)
        if len(parts) != 3:
            raise ValueError
        chat_id_str, name, page_str = parts
        chat_id = int(chat_id_str)
        page = int(page_str)
        offset = (page - 1) * GET_PAGE_SIZE
    except (ValueError, IndexError):
        await query.edit_message_text("⏱️ Invalid page.")
        return
    await query.edit_message_text(f"📤 Loading page {page} of '{name}'...")
    try:
        await run_cancellable(chat_id, _get_collection_impl(update, context, name=name, offset=offset))
    except asyncio.CancelledError:
        await context.bot.send_message(chat_id, "🛑 Stopped.")
    except Exception:
        logger.exception("Error loading page %d of '%s'", page, name)
        await context.bot.send_message(chat_id, "⚠️ Something went wrong.")

async def get_album_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    _, page_num, album_num, status = query.data.split(":")
    if status == "ok":
        await query.answer(f"Album {album_num} (page {page_num}): all sent ✅")
    elif status == "partial":
        await query.answer(f"Album {album_num} (page {page_num}): some failed ⚠️", show_alert=True)
    else:
        await query.answer(f"Album {album_num} (page {page_num}): all failed ❌", show_alert=True)

# ----------------------------------------------------------------------
# /remove
# ----------------------------------------------------------------------

async def remove_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    replied = update.message.reply_to_message
    if replied is None:
        await update.message.reply_text("Reply to a video I sent with /remove.")
        return
    try:
        def _lookup(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT collection, file_unique_id FROM sent_videos WHERE chat_id = %s AND message_id = %s",
                    (chat_id, replied.message_id),
                )
                return cur.fetchone()
        record = await db_run(_lookup)
    except Exception:
        await reply_db_error(update, "look up video")
        return
    if record is None:
        await update.message.reply_text("⚠️ I can't identify that video.")
        return
    collection, file_unique_id = record
    try:
        def _delete(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM videos WHERE collection = %s AND file_unique_id = %s",
                    (collection, file_unique_id),
                )
                return cur.rowcount
        deleted = await db_run(_delete)
    except Exception:
        await reply_db_error(update, "delete video")
        return
    if deleted:
        await update.message.reply_text(f"🗑️ Removed from '{collection}'.")
    else:
        await update.message.reply_text(f"⚠️ Already removed from '{collection}'.")

# ----------------------------------------------------------------------
# Admin commands: rename, merge, copy, export, exportjson, importjson, delete, backup
# ----------------------------------------------------------------------

async def admin_check(update: Update) -> bool:
    user = update.effective_user
    if not user or not is_admin(user.id):
        await update.message.reply_text("⛔ This command is restricted to admins.")
        return False
    return True

async def rename_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_check(update):
        return
    pair = _parse_arrow_pair(context.args)
    if not pair:
        await update.message.reply_text("Usage: /rename <old> -> <new>")
        return
    old, new = pair
    if new in RESERVED_NAMES:
        await update.message.reply_text(f"⚠️ '{new}' is reserved.")
        return
    try:
        def _rename(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM videos WHERE collection = %s LIMIT 1", (old,))
                if not cur.fetchone():
                    return "not_found"
                cur.execute("SELECT 1 FROM videos WHERE collection = %s LIMIT 1", (new,))
                if cur.fetchone():
                    return "conflict"
                cur.execute("UPDATE videos SET collection = %s WHERE collection = %s", (new, old))
                cur.execute("UPDATE sent_videos SET collection = %s WHERE collection = %s", (new, old))
                return "ok"
        result = await db_run(_rename)
    except Exception:
        await reply_db_error(update, "rename")
        return
    if result == "not_found":
        await update.message.reply_text(f"No collection '{old}'.")
    elif result == "conflict":
        await update.message.reply_text(f"'{new}' already exists. Use /merge or /copy.")
    else:
        await update.message.reply_text(f"✏️ Renamed '{old}' to '{new}'.")

async def merge_collections(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_check(update):
        return
    pair = _parse_arrow_pair(context.args)
    if not pair:
        await update.message.reply_text("Usage: /merge <source> -> <dest>")
        return
    src, dest = pair
    if src == dest:
        await update.message.reply_text("Source and destination must differ.")
        return
    try:
        def _merge(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM videos WHERE collection = %s LIMIT 1", (src,))
                if not cur.fetchone():
                    return "not_found", 0
                cur.execute(
                    """
                    UPDATE videos v1
                    SET collection = %s
                    WHERE v1.collection = %s
                      AND NOT EXISTS (
                          SELECT 1 FROM videos v2
                          WHERE v2.collection = %s AND v2.file_unique_id = v1.file_unique_id
                      )
                    """,
                    (dest, src, dest),
                )
                moved = cur.rowcount
                cur.execute("DELETE FROM videos WHERE collection = %s", (src,))
                cur.execute("UPDATE sent_videos SET collection = %s WHERE collection = %s", (dest, src))
                return "ok", moved
        result, moved = await db_run(_merge)
    except Exception:
        await reply_db_error(update, "merge")
        return
    if result == "not_found":
        await update.message.reply_text(f"No collection '{src}'.")
    else:
        await update.message.reply_text(f"🔀 Merged '{src}' into '{dest}' ({moved} moved).")

async def copy_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_check(update):
        return
    pair = _parse_arrow_pair(context.args)
    if not pair:
        await update.message.reply_text("Usage: /copy <source> -> <dest>")
        return
    src, dest = pair
    if src == dest:
        await update.message.reply_text("Source and destination must differ.")
        return
    if dest in RESERVED_NAMES:
        await update.message.reply_text(f"⚠️ '{dest}' is reserved.")
        return
    try:
        def _copy(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM videos WHERE collection = %s LIMIT 1", (src,))
                if not cur.fetchone():
                    return "not_found", 0
                cur.execute(
                    """
                    INSERT INTO videos (collection, file_id, file_unique_id)
                    SELECT %s, file_id, file_unique_id
                    FROM videos
                    WHERE collection = %s
                    ON CONFLICT (collection, file_unique_id) DO NOTHING
                    """,
                    (dest, src),
                )
                copied = cur.rowcount
                return "ok", copied
        result, copied = await db_run(_copy)
    except Exception:
        await reply_db_error(update, "copy")
        return
    if result == "not_found":
        await update.message.reply_text(f"No collection '{src}'.")
    else:
        await update.message.reply_text(f"📋 Copied {copied} videos to '{dest}'.")

async def export_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /export <name>")
        return
    name = normalize_name(" ".join(context.args))
    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT file_id, file_unique_id, added_at FROM videos WHERE collection = %s ORDER BY added_at",
                    (name,),
                )
                return cur.fetchall()
        rows = await db_run(_query)
    except Exception:
        await reply_db_error(update, "export")
        return
    if not rows:
        await update.message.reply_text(f"No videos in '{name}'.")
        return
    lines = [f"# Export of '{name}' — {len(rows)} videos"]
    lines.append("# file_id\tfile_unique_id\tadded_at")
    for fid, fu, added in rows:
        lines.append(f"{fid}\t{fu}\t{added.isoformat()}")
    content = "\n".join(lines)
    bio = io.BytesIO(content.encode("utf-8"))
    bio.name = f"{name}_export.txt"
    await update.message.reply_document(document=bio, filename=f"{name}_export.txt", caption=f"📦 Backup of '{name}'.")

async def exportjson(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_check(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: /exportjson <name>")
        return
    name = normalize_name(" ".join(context.args))
    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT file_id, file_unique_id, duration, file_size, added_at FROM videos WHERE collection = %s ORDER BY added_at",
                    (name,),
                )
                rows = cur.fetchall()
                return [{"file_id": r[0], "file_unique_id": r[1], "duration": r[2], "file_size": r[3], "added_at": r[4].isoformat() if r[4] else None} for r in rows]
        data = await db_run(_query)
    except Exception:
        await reply_db_error(update, "export JSON")
        return
    if not data:
        await update.message.reply_text(f"No videos in '{name}'.")
        return
    json_str = json.dumps({"collection": name, "videos": data}, indent=2)
    bio = io.BytesIO(json_str.encode("utf-8"))
    bio.name = f"{name}_export.json"
    await update.message.reply_document(document=bio, filename=f"{name}_export.json", caption=f"📦 JSON export of '{name}'.")

async def importjson(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_check(update):
        return
    replied = update.message.reply_to_message
    if not replied or not replied.document:
        await update.message.reply_text("Reply to a JSON file exported from this bot.")
        return
    doc = replied.document
    if doc.mime_type not in ("application/json", "text/json") and not doc.file_name.endswith(".json"):
        await update.message.reply_text("Please send a JSON file.")
        return
    try:
        file = await context.bot.get_file(doc.file_id)
        content = await file.download_as_bytearray()
        data = json.loads(content.decode("utf-8"))
        collection = data.get("collection")
        videos = data.get("videos", [])
        if not collection or not videos:
            await update.message.reply_text("Invalid JSON format.")
            return
        def _insert(conn):
            with conn.cursor() as cur:
                inserted = 0
                for v in videos:
                    fid = v.get("file_id")
                    fu = v.get("file_unique_id")
                    if not fid or not fu:
                        continue
                    dur = v.get("duration")
                    size = v.get("file_size")
                    cur.execute(
                        """
                        INSERT INTO videos (collection, file_id, file_unique_id, duration, file_size)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (collection, file_unique_id) DO NOTHING
                        """,
                        (collection, fid, fu, dur, size),
                    )
                    if cur.rowcount:
                        inserted += 1
                return inserted
        inserted = await db_run(_insert)
        await update.message.reply_text(f"✅ Imported {inserted} video(s) into '{collection}'.")
    except Exception as e:
        logger.exception("Import failed")
        await update.message.reply_text(f"⚠️ Import failed: {e}")

async def delete_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_check(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: /delete <name>")
        return
    name = normalize_name(" ".join(context.args))
    chat_id = update.effective_chat.id
    try:
        def _count(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM videos WHERE collection = %s", (name,))
                return cur.fetchone()[0]
        count = await db_run(_count)
    except Exception:
        await reply_db_error(update, "count")
        return
    if count == 0:
        await update.message.reply_text(f"No collection '{name}'.")
        return
    token = f"{chat_id}:{name}:{update.message.message_id}"
    _pending_deletes[token] = name
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Yes, delete", callback_data=f"delconfirm:{token}"),
         InlineKeyboardButton("❌ Cancel", callback_data=f"delcancel:{token}")]
    ])
    await update.message.reply_text(f"⚠️ Delete '{name}' ({count} videos)?", reply_markup=keyboard)

async def delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    action, token = query.data.split(":", 1)
    name = _pending_deletes.pop(token, None)
    if name is None:
        await query.answer("Expired.")
        await query.edit_message_text("⏱️ This confirmation expired.")
        return
    if action == "delcancel":
        await query.answer("Cancelled.")
        await query.edit_message_text(f"❎ Cancelled — '{name}' not deleted.")
        return
    await query.answer("Deleting...")
    try:
        def _delete(conn):
            with conn.cursor() as cur:
                cur.execute("DELETE FROM videos WHERE collection = %s", (name,))
                deleted = cur.rowcount
                cur.execute("DELETE FROM sent_videos WHERE collection = %s", (name,))
                return deleted
        deleted = await db_run(_delete)
    except Exception:
        logger.exception("Delete failed for '%s'", name)
        await query.edit_message_text(f"⚠️ Couldn't delete '{name}'.")
        return
    await query.edit_message_text(f"🗑️ Deleted '{name}' ({deleted} videos).")

async def backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_check(update):
        return
    try:
        def _backup(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT collection, file_id, file_unique_id, duration, file_size, added_at FROM videos ORDER BY collection, added_at")
                rows = cur.fetchall()
                return [{"collection": r[0], "file_id": r[1], "file_unique_id": r[2], "duration": r[3], "file_size": r[4], "added_at": r[5].isoformat() if r[5] else None} for r in rows]
        data = await db_run(_backup)
        if not data:
            await update.message.reply_text("No videos to backup.")
            return
        json_str = json.dumps(data, indent=2)
        bio = io.BytesIO(json_str.encode("utf-8"))
        bio.name = "full_backup.json"
        await update.message.reply_document(document=bio, filename="full_backup.json", caption="📦 Full database backup.")
    except Exception:
        await reply_db_error(update, "backup")

# ----------------------------------------------------------------------
# /status
# ----------------------------------------------------------------------

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    collections = get_active_collections(chat_id)
    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT collection, COUNT(*) FROM videos WHERE collection = ANY(%s) GROUP BY collection",
                    (collections,),
                )
                return dict(cur.fetchall())
        counts = await db_run(_query)
    except Exception:
        await reply_db_error(update, "status")
        return
    if len(collections) == 1:
        c = collections[0]
        await update.message.reply_text(f"📦 '{c}' has {counts.get(c, 0)} video(s).")
    else:
        lines = [f"• '{c}' — {counts.get(c, 0)}" for c in collections]
        await update.message.reply_text("📦 Active collections:\n" + "\n".join(lines))

# ----------------------------------------------------------------------
# /neardupes
# ----------------------------------------------------------------------

async def neardupes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not rate_limit(update.effective_chat.id, "neardupes"):
        await update.message.reply_text("⏳ Please wait.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /neardupes <collection>")
        return
    name = normalize_name(" ".join(context.args))
    try:
        def _find(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 
                        v1.file_id, v1.file_unique_id, v1.duration, v1.file_size,
                        v2.file_id, v2.file_unique_id, v2.duration, v2.file_size,
                        ABS(v1.duration - v2.duration) as dur_diff,
                        ABS(v1.file_size - v2.file_size) as size_diff,
                        CASE 
                            WHEN v1.file_size > 0 THEN ROUND(ABS(v1.file_size - v2.file_size)::numeric / v1.file_size * 100, 2)
                            ELSE 0 
                        END as size_pct
                    FROM videos v1
                    JOIN videos v2 ON v1.collection = v2.collection AND v1.file_unique_id < v2.file_unique_id
                    WHERE v1.collection = %s
                      AND v1.file_size IS NOT NULL AND v2.file_size IS NOT NULL
                      AND (
                          (v1.duration IS NOT NULL AND v2.duration IS NOT NULL
                           AND ABS(v1.duration - v2.duration) <= 2
                           AND v2.file_size BETWEEN v1.file_size * 0.98 AND v1.file_size * 1.02)
                          OR
                          ((v1.duration IS NULL OR v2.duration IS NULL)
                           AND v2.file_size BETWEEN v1.file_size * 0.995 AND v1.file_size * 1.005)
                      )
                    ORDER BY size_pct DESC, dur_diff DESC
                    """,
                    (name,),
                )
                return cur.fetchall()
        pairs = await db_run(_find)
    except Exception:
        await reply_db_error(update, "find near-dupes")
        return
    if not pairs:
        await update.message.reply_text(f"No near-duplicates in '{name}'.")
        return
    total = len(pairs)
    lines = [f"🔎 Found {total} near-duplicate pairs in '{name}':\n"]
    for i, row in enumerate(pairs[:NEARDUPES_PAIRS_PER_PAGE]):
        v1_fid, v1_fuid, v1_dur, v1_size, v2_fid, v2_fuid, v2_dur, v2_size, dur_diff, size_diff, size_pct = row
        lines.append(
            f"Pair {i+1}: {v1_dur}s/{v1_size/1024/1024:.1f}MB vs {v2_dur}s/{v2_size/1024/1024:.1f}MB "
            f"(diff: {dur_diff}s, {size_pct}% size)"
        )
    if total > NEARDUPES_PAIRS_PER_PAGE:
        lines.append(f"\n...and {total - NEARDUPES_PAIRS_PER_PAGE} more.")
    token = f"{update.effective_chat.id}:{name}"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📤 Compare all {total} pairs", callback_data=f"neardupes:{token}")],
        [InlineKeyboardButton("🧹 Cleanup dead first", callback_data=f"neardupcleanup:{token}")]
    ])
    await update.message.reply_text("\n".join(lines), reply_markup=keyboard)

async def neardupes_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    token = query.data[len("neardupes:"):]
    try:
        _, name = token.split(":", 1)
    except ValueError:
        await query.answer("Invalid.")
        return
    chat_id = update.effective_chat.id
    await query.answer()
    await query.edit_message_text(f"📤 Sending pairs from '{name}'...")
    try:
        def _find(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 
                        v1.file_id, v1.file_unique_id, v1.duration, v1.file_size,
                        v2.file_id, v2.file_unique_id, v2.duration, v2.file_size,
                        ABS(v1.duration - v2.duration) as dur_diff,
                        ABS(v1.file_size - v2.file_size) as size_diff,
                        CASE 
                            WHEN v1.file_size > 0 THEN ROUND(ABS(v1.file_size - v2.file_size)::numeric / v1.file_size * 100, 2)
                            ELSE 0 
                        END as size_pct
                    FROM videos v1
                    JOIN videos v2 ON v1.collection = v2.collection AND v1.file_unique_id < v2.file_unique_id
                    WHERE v1.collection = %s
                      AND v1.file_size IS NOT NULL AND v2.file_size IS NOT NULL
                      AND (
                          (v1.duration IS NOT NULL AND v2.duration IS NOT NULL
                           AND ABS(v1.duration - v2.duration) <= 2
                           AND v2.file_size BETWEEN v1.file_size * 0.98 AND v1.file_size * 1.02)
                          OR
                          ((v1.duration IS NULL OR v2.duration IS NULL)
                           AND v2.file_size BETWEEN v1.file_size * 0.995 AND v1.file_size * 1.005)
                      )
                    ORDER BY size_pct DESC, dur_diff DESC
                    """,
                    (name,),
                )
                return cur.fetchall()
        pairs = await db_run(_find)
    except Exception:
        await reply_db_error(update, "fetch pairs")
        return
    if not pairs:
        await context.bot.send_message(chat_id, f"No pairs in '{name}' anymore.")
        return
    total = len(pairs)
    sent = 0
    failed = 0
    for i, row in enumerate(pairs):
        v1_fid, v1_fuid, v1_dur, v1_size, v2_fid, v2_fuid, v2_dur, v2_size, dur_diff, size_diff, size_pct = row
        caption = (
            f"🔎 Pair {i+1}/{total}\n"
            f"Left: {v1_dur}s/{v1_size/1024/1024:.1f}MB\n"
            f"Right: {v2_dur}s/{v2_size/1024/1024:.1f}MB\n"
            f"Diff: {dur_diff}s, {size_pct}% size"
        )
        media = [InputMediaVideo(media=v1_fid, caption=caption), InputMediaVideo(media=v2_fid)]
        try:
            msgs = await context.bot.send_media_group(chat_id=chat_id, media=media)
            sent += 2
            def _record(conn):
                with conn.cursor() as cur:
                    cur.executemany(
                        "INSERT INTO sent_videos (chat_id, message_id, collection, file_unique_id) VALUES (%s, %s, %s, %s) ON CONFLICT DO UPDATE SET collection=EXCLUDED.collection, file_unique_id=EXCLUDED.file_unique_id",
                        [(chat_id, msg.message_id, name, fuid) for msg, fuid in zip(msgs, [v1_fuid, v2_fuid])],
                    )
            await db_run(_record)
        except TelegramError:
            for fid, fuid in [(v1_fid, v1_fuid), (v2_fid, v2_fuid)]:
                msg = await _send_single_video_with_fallback(chat_id, fid, fuid, None, context, name)
                if msg:
                    sent += 1
                else:
                    failed += 1
            await context.bot.send_message(chat_id, caption)
        await asyncio.sleep(NEARDUP_ALBUM_DELAY)
    if failed:
        await context.bot.send_message(chat_id, f"✅ Sent {sent} videos, {failed} failed (dead). Use /cleanup {name}.")
    else:
        await context.bot.send_message(chat_id, f"✅ All {sent} videos sent.")

async def neardupcleanup_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    token = query.data[len("neardupcleanup:"):]
    try:
        _, name = token.split(":", 1)
    except ValueError:
        await query.answer("Invalid.")
        return
    await query.answer()
    await query.edit_message_text(f"🧹 Cleaning up dead files in '{name}'...")
    try:
        def _cleanup(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM videos WHERE collection = %s AND file_unique_id IN (SELECT file_unique_id FROM dead_files)",
                    (name,),
                )
                removed = cur.rowcount
                cur.execute("DELETE FROM dead_files d WHERE NOT EXISTS (SELECT 1 FROM videos v WHERE v.file_unique_id = d.file_unique_id)")
                return removed
        removed = await db_run(_cleanup)
        await query.edit_message_text(f"🧹 Removed {removed} dead videos from '{name}'. Re-run /neardupes if needed.")
    except Exception:
        await query.edit_message_text("⚠️ Cleanup failed.")

# ----------------------------------------------------------------------
# /dups
# ----------------------------------------------------------------------

async def find_dups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /dups <collection>")
        return
    name = normalize_name(" ".join(context.args))
    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT v1.file_unique_id, v1.duration, v1.file_size, ARRAY_AGG(DISTINCT v2.collection ORDER BY v2.collection)
                    FROM videos v1
                    JOIN videos v2 ON v1.file_unique_id = v2.file_unique_id AND v1.collection != v2.collection
                    WHERE v1.collection = %s
                    GROUP BY v1.file_unique_id, v1.duration, v1.file_size
                    ORDER BY COUNT(DISTINCT v2.collection) DESC
                    """,
                    (name,),
                )
                return cur.fetchall()
        rows = await db_run(_query)
    except Exception:
        await reply_db_error(update, "find dups")
        return
    if not rows:
        await update.message.reply_text(f"No duplicates for '{name}'.")
        return
    lines = [f"🔍 {len(rows)} duplicate videos in '{name}':\n"]
    for fuid, dur, size, others in rows[:20]:
        lines.append(f"• {dur}s / {size/1024/1024:.1f}MB — also in: {', '.join(others)}")
    if len(rows) > 20:
        lines.append(f"...and {len(rows)-20} more.")
    await update.message.reply_text("\n".join(lines))

# ----------------------------------------------------------------------
# /recent
# ----------------------------------------------------------------------

async def recent_videos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /recent <name> [n]")
        return
    args = list(context.args)
    n = 10
    if args and args[-1].isdigit():
        n = max(1, min(50, int(args[-1])))
        args = args[:-1]
    if not args:
        await update.message.reply_text("Missing collection name.")
        return
    name = normalize_name(" ".join(args))
    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT file_id, file_unique_id, duration, file_size, added_at FROM videos WHERE collection = %s ORDER BY added_at DESC LIMIT %s",
                    (name, n),
                )
                return cur.fetchall()
        rows = await db_run(_query)
    except Exception:
        await reply_db_error(update, "recent")
        return
    if not rows:
        await update.message.reply_text(f"No videos in '{name}'.")
        return
    lines = [f"📅 Last {len(rows)} added to '{name}':\n"]
    for i, (fid, fuid, dur, size, added) in enumerate(rows, 1):
        dur_str = f"{dur}s" if dur else "?s"
        size_str = f"{size/1024/1024:.1f}MB" if size else "?MB"
        date_str = added.strftime("%Y-%m-%d %H:%M") if added else "?"
        lines.append(f"{i}. {dur_str} / {size_str} — {date_str}")
    await update.message.reply_text("\n".join(lines))

# ----------------------------------------------------------------------
# /cleanup
# ----------------------------------------------------------------------

async def cleanup_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /cleanup <name>")
        return
    name = normalize_name(" ".join(context.args))
    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT v.file_unique_id, v.duration, v.file_size FROM videos v JOIN dead_files d ON v.file_unique_id = d.file_unique_id WHERE v.collection = %s",
                    (name,),
                )
                return cur.fetchall()
        dead = await db_run(_query)
    except Exception:
        await reply_db_error(update, "check dead")
        return
    if not dead:
        await update.message.reply_text(f"✅ No dead files in '{name}'.")
        return
    lines = [f"🧹 Found {len(dead)} dead video(s) in '{name}':\n"]
    for fuid, dur, size in dead[:10]:
        lines.append(f"• {dur}s / {size/1024/1024:.1f}MB" if dur and size else "• unknown")
    if len(dead) > 10:
        lines.append(f"...and {len(dead)-10} more.")
    token = f"{update.effective_chat.id}:{name}:cleanup"
    _pending_deletes[token] = name
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Remove dead", callback_data=f"cleanupconfirm:{token}"),
         InlineKeyboardButton("❌ Cancel", callback_data=f"cleanupcancel:{token}")]
    ])
    await update.message.reply_text("\n".join(lines) + "\nRemove them?", reply_markup=keyboard)

async def cleanup_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    action, token = query.data.split(":", 1)
    name = _pending_deletes.pop(token, None)
    if name is None:
        await query.answer("Expired.")
        await query.edit_message_text("⏱️ Expired.")
        return
    if action == "cleanupcancel":
        await query.answer("Cancelled.")
        await query.edit_message_text(f"❎ Cancelled — '{name}' unchanged.")
        return
    await query.answer("Cleaning...")
    try:
        def _cleanup(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM videos WHERE collection = %s AND file_unique_id IN (SELECT file_unique_id FROM dead_files)",
                    (name,),
                )
                removed = cur.rowcount
                cur.execute("DELETE FROM dead_files d WHERE NOT EXISTS (SELECT 1 FROM videos v WHERE v.file_unique_id = d.file_unique_id)")
                return removed
        removed = await db_run(_cleanup)
        await query.edit_message_text(f"🧹 Removed {removed} dead video(s) from '{name}'.")
    except Exception:
        await query.edit_message_text("⚠️ Cleanup failed.")

# ----------------------------------------------------------------------
# /stats
# ----------------------------------------------------------------------

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM videos")
                total = cur.fetchone()[0]
                cur.execute("SELECT COUNT(DISTINCT collection) FROM videos")
                colls = cur.fetchone()[0]
                cur.execute("SELECT collection, COUNT(*) FROM videos GROUP BY collection ORDER BY COUNT(*) DESC LIMIT 1")
                largest = cur.fetchone()
                cur.execute("SELECT collection, added_at FROM videos ORDER BY added_at ASC LIMIT 1")
                oldest = cur.fetchone()
                cur.execute("SELECT collection, added_at FROM videos ORDER BY added_at DESC LIMIT 1")
                newest = cur.fetchone()
                cur.execute("SELECT COUNT(*) FROM dead_files")
                dead = cur.fetchone()[0]
                return total, colls, largest, oldest, newest, dead
        total, colls, largest, oldest, newest, dead = await db_run(_query)
    except Exception:
        await reply_db_error(update, "stats")
        return
    if total == 0:
        await update.message.reply_text("📊 No videos yet.")
        return
    lines = [f"📊 Stats:\nTotal videos: {total}\nCollections: {colls}"]
    if largest:
        lines.append(f"Largest: '{largest[0]}' ({largest[1]})")
    if oldest:
        lines.append(f"Oldest: '{oldest[0]}' on {oldest[1].strftime('%Y-%m-%d')}")
    if newest:
        lines.append(f"Newest: '{newest[0]}' on {newest[1].strftime('%Y-%m-%d')}")
    if dead:
        lines.append(f"Dead files: {dead} (use /cleanup)")
    await update.message.reply_text("\n".join(lines))

# ----------------------------------------------------------------------
# /find – search by duration and size
# ----------------------------------------------------------------------

async def find_videos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not rate_limit(update.effective_chat.id, "find"):
        await update.message.reply_text("⏳ Please wait.")
        return
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "Usage: /find <collection> [duration:>60] [size:<10MB]\n"
            "Operators: >, <, >=, <=, =\n"
            "Example: /find mycollection duration:>30 size:<5MB"
        )
        return

    collection = normalize_name(args[0])
    filters_raw = args[1:]
    dur_filter = None
    size_filter = None
    for f in filters_raw:
        if f.startswith("duration:"):
            dur_filter = f[len("duration:"):]
        elif f.startswith("size:"):
            size_filter = f[len("size:"):]

    conditions = []
    params = [collection]
    if dur_filter:
        try:
            op, val = _parse_filter(dur_filter)
            conditions.append(f"duration {op} %s")
            params.append(val)
        except ValueError:
            pass
    if size_filter:
        try:
            op, val = _parse_filter(size_filter.replace("MB", "").replace("mb", ""))
            conditions.append(f"file_size {op} %s")
            params.append(val * 1024 * 1024)
        except ValueError:
            pass

    where = " AND ".join(conditions) if conditions else "1=1"

    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT file_id, file_unique_id, duration, file_size, added_at FROM videos WHERE collection = %s AND {where} ORDER BY added_at LIMIT 50",
                    params,
                )
                return cur.fetchall()
        rows = await db_run(_query)
    except Exception:
        await reply_db_error(update, "search")
        return

    if not rows:
        await update.message.reply_text("No matching videos.")
        return

    lines = [f"🔍 Found {len(rows)} video(s):\n"]
    for i, (fid, fuid, dur, size, added) in enumerate(rows, 1):
        dur_str = f"{dur}s" if dur else "?s"
        size_str = f"{size/1024/1024:.1f}MB" if size else "?MB"
        date_str = added.strftime("%Y-%m-%d %H:%M") if added else "?"
        lines.append(f"{i}. {dur_str} / {size_str} — {date_str}")
    await update.message.reply_text("\n".join(lines))

def _parse_filter(s: str) -> Tuple[str, int]:
    if s.startswith(">="):
        return ">=", int(s[2:])
    elif s.startswith("<="):
        return "<=", int(s[2:])
    elif s.startswith(">"):
        return ">", int(s[1:])
    elif s.startswith("<"):
        return "<", int(s[1:])
    elif s.startswith("="):
        return "=", int(s[1:])
    else:
        raise ValueError("Invalid operator")

# ----------------------------------------------------------------------
# Error handler for DB errors
# ----------------------------------------------------------------------

async def reply_db_error(update: Update, action: str):
    logger.exception("DB error during: %s", action)
    await update.effective_message.reply_text(
        f"⚠️ Couldn't {action} right now — the database didn't respond. Please try again in a moment."
    )

# ----------------------------------------------------------------------
# Webhook and main
# ----------------------------------------------------------------------

async def post_init(application: Application):
    await asyncio.to_thread(init_db)
    await application.bot.set_my_commands([
        BotCommand("menu", "Open main menu"),
        BotCommand("collect", "Set active collection(s)"),
        BotCommand("get", "View a collection"),
        BotCommand("random", "Send random video"),
        BotCommand("list", "List all collections"),
        BotCommand("fav", "Shortcut for /collect favorites"),
        BotCommand("finish", "Stop adding"),
        BotCommand("stop", "Cancel and pause"),
        BotCommand("minlength", "Set min duration filter"),
        BotCommand("removemode", "Toggle delete mode"),
        BotCommand("current", "Show active"),
        BotCommand("remove", "Delete replied video"),
        BotCommand("rename", "Rename collection (admin)"),
        BotCommand("merge", "Merge collections (admin)"),
        BotCommand("copy", "Copy collection (admin)"),
        BotCommand("export", "Export as text"),
        BotCommand("exportjson", "Export as JSON"),
        BotCommand("importjson", "Import from JSON (admin)"),
        BotCommand("delete", "Delete collection (admin)"),
        BotCommand("status", "Count in active"),
        BotCommand("neardupes", "Find near-duplicates"),
        BotCommand("dups", "Find exact duplicates"),
        BotCommand("recent", "Show recent videos"),
        BotCommand("cleanup", "Remove dead files"),
        BotCommand("find", "Search videos"),
        BotCommand("stats", "Database stats"),
        BotCommand("backup", "Full backup (admin)"),
        BotCommand("help", "Show help"),
    ])

async def telegram_webhook(request):
    app = request.app.state.application
    secret_header = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if secret_header != WEBHOOK_SECRET:
        return Response(status_code=401)
    data = await request.json()
    update = Update.de_json(data=data, bot=app.bot)
    await app.update_queue.put(update)
    return Response(status_code=200)

async def health_check(request):
    return PlainTextResponse("OK")

async def run():
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # Access control
    application.add_handler(TypeHandler(Update, access_control), group=-1)

    # Commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler("collect", collect))
    application.add_handler(CommandHandler("fav", fav_shortcut))
    application.add_handler(CommandHandler("finish", finish))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("minlength", minlength_command))
    application.add_handler(CommandHandler("removemode", removemode_command))
    application.add_handler(CommandHandler("current", current))
    application.add_handler(CommandHandler("list", list_collections))
    application.add_handler(CommandHandler("get", get_collection))
    application.add_handler(CommandHandler("remove", remove_video))
    application.add_handler(CommandHandler("rename", rename_collection))
    application.add_handler(CommandHandler("merge", merge_collections))
    application.add_handler(CommandHandler("copy", copy_collection))
    application.add_handler(CommandHandler("export", export_collection))
    application.add_handler(CommandHandler("exportjson", exportjson))
    application.add_handler(CommandHandler("importjson", importjson))
    application.add_handler(CommandHandler("delete", delete_collection))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("random", random_video))
    application.add_handler(CommandHandler("neardupes", neardupes))
    application.add_handler(CommandHandler("dups", find_dups))
    application.add_handler(CommandHandler("recent", recent_videos))
    application.add_handler(CommandHandler("cleanup", cleanup_collection))
    application.add_handler(CommandHandler("find", find_videos))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("backup", backup))

    # Callbacks
    application.add_handler(CallbackQueryHandler(delete_callback, pattern=r"^del(confirm|cancel):"))
    application.add_handler(CallbackQueryHandler(get_page_callback, pattern=r"^get(page|stop):"))
    application.add_handler(CallbackQueryHandler(get_send_callback, pattern=r"^getsend:"))
    application.add_handler(CallbackQueryHandler(get_cancel_callback, pattern=r"^getcancel:"))
    application.add_handler(CallbackQueryHandler(get_album_status_callback, pattern=r"^getalbumstatus:"))
    application.add_handler(CallbackQueryHandler(list_choice_callback, pattern=r"^listchoice:"))
    application.add_handler(CallbackQueryHandler(list_set_callback, pattern=r"^listset:"))
    application.add_handler(CallbackQueryHandler(list_get_callback, pattern=r"^listget:"))
    application.add_handler(CallbackQueryHandler(list_random_callback, pattern=r"^listrandom:"))
    application.add_handler(CallbackQueryHandler(list_page_callback, pattern=r"^listpage:"))
    application.add_handler(CallbackQueryHandler(neardupes_callback, pattern=r"^neardupes:"))
    application.add_handler(CallbackQueryHandler(neardupcleanup_callback, pattern=r"^neardupcleanup:"))
    application.add_handler(CallbackQueryHandler(cleanup_callback, pattern=r"^cleanup(confirm|cancel):"))
    application.add_handler(CallbackQueryHandler(cleanupnow_callback, pattern=r"^cleanupnow:"))
    # Menu callbacks
    application.add_handler(CallbackQueryHandler(menu_callback, pattern=r"^menu_"))
    application.add_handler(CallbackQueryHandler(menu_set_callback, pattern=r"^menuset:"))
    application.add_handler(CallbackQueryHandler(menu_view_callback, pattern=r"^menuview:"))
    application.add_handler(CallbackQueryHandler(menu_random_callback, pattern=r"^menurandom:"))
    application.add_handler(CallbackQueryHandler(menu_back_callback, pattern=r"^menu_back"))

    # Message handlers
    application.add_handler(MessageHandler(filters.VIDEO, handle_video))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(MessageHandler(filters.PHOTO | filters.AUDIO | filters.VOICE, handle_non_video))

    web_app = Starlette(routes=[
        Route("/webhook", telegram_webhook, methods=["POST"]),
        Route("/health", health_check, methods=["GET", "HEAD"]),
    ])
    web_app.state.application = application

    server = uvicorn.Server(
        uvicorn.Config(app=web_app, host="0.0.0.0", port=PORT, log_level="info")
    )

    async with application:
        await application.bot.set_webhook(
            url=f"{RENDER_EXTERNAL_URL}/webhook",
            secret_token=WEBHOOK_SECRET,
        )
        await application.start()
        try:
            await server.serve()
        finally:
            await application.stop()

def main():
    asyncio.run(run())

if __name__ == "__main__":
    main()
