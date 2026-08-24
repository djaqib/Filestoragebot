
# I'll build the complete fixed bot.py in parts
# The main fixes:
# 1. neardupes_callback: use individual execute() instead of executemany() with ON CONFLICT
# 2. post_init: wrap init_db in asyncio.to_thread
# 3. HELP_TEXT: plain text, no MarkdownV2 escaping issues
# 4. _send_page: handle 4-column rows properly

part1 = r'''import os
import io
import time
import asyncio
import logging
import random
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

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
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
    raise RuntimeError("ALLOWED_USER_IDS is set but empty.")

DEFAULT_COLLECTION = "default"
FAVORITES_COLLECTION = "favorites"
RESERVED_NAMES = {"default", "all"}
BATCH_DEBOUNCE_SECONDS = 2.5
LIST_PAGE_SIZE = 15
ALBUM_SIZE = 10
ALBUM_DELAY_SECONDS = 3
GET_PAGE_SIZE = 50
NEARDUPES_PAIRS_PER_PAGE = 10
NEARDUP_ALBUM_SIZE = 2
NEARDUP_ALBUM_DELAY = 1.5


def normalize_name(name: str) -> str:
    return name.strip().lower()


UNAUTHORIZED_REPLY_COOLDOWN = 60
_last_unauthorized_reply: dict[int, float] = {}


async def access_control(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user is None or user.id in ALLOWED_USER_IDS:
        return
    logger.warning("Blocked message from unauthorized user_id=%s username=%s", user.id, user.username)
    now = time.monotonic()
    last = _last_unauthorized_reply.get(user.id, 0)
    if now - last >= UNAUTHORIZED_REPLY_COOLDOWN:
        _last_unauthorized_reply[user.id] = now
        try:
            if update.effective_message is not None:
                await update.effective_message.reply_text("🔒 This bot is private and not available for public use.")
            elif update.callback_query is not None:
                await update.callback_query.answer("This bot is private.", show_alert=True)
        except TelegramError:
            logger.exception("Failed to send 'private bot' notice to user_id=%s", user.id)
    raise ApplicationHandlerStop


db_pool = psycopg2.pool.SimpleConnectionPool(1, 5, DATABASE_URL, sslmode="require")
active_collections: dict[int, list[str]] = {}
paused_chats: set[int] = set()
removing_chats: set[int] = set()
min_video_length: dict[int, int | None] = {}
_batch_state: dict[int, dict] = {}
_pending_deletes: dict[str, str] = {}
_active_tasks: dict[int, asyncio.Task] = {}
_last_get_page: dict[str, int] = {}
_dead_file_ids: set[str] = set()


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
            cur.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS duration INTEGER")
            cur.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS file_size BIGINT")
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


def get_active_collections(chat_id: int) -> list[str]:
    return active_collections.get(chat_id, [DEFAULT_COLLECTION])


async def reply_db_error(update: Update, action: str):
    logger.exception("DB error during: %s", action)
    await update.effective_message.reply_text(
        f"⚠️ Couldn't {action} right now — the database didn't respond. Please try again in a moment."
    )


def _parse_collection_names(raw: str) -> list[str]:
    names = [normalize_name(n) for n in raw.split(",")]
    names = [n for n in names if n]
    seen = []
    for n in names:
        if n not in seen:
            seen.append(n)
    return seen


def _parse_arrow_pair(args: list[str]) -> tuple[str, str] | None:
    raw = " ".join(args)
    if "->" not in raw:
        return None
    src, _, dest = raw.partition("->")
    src = normalize_name(src)
    dest = normalize_name(dest)
    if not src or not dest:
        return None
    return src, dest
'''

with open('/mnt/agents/output/bot.py', 'w') as f:
    f.write(part1)
print("Part 1 done")

part2 = r'''
async def _flush_batch(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    await asyncio.sleep(BATCH_DEBOUNCE_SECONDS)
    state = _batch_state.get(chat_id)
    if state is None or state["task"] is not asyncio.current_task():
        return
    _batch_state.pop(chat_id, None)
    lines = []
    if state["saved"]:
        by_collection: dict[str, int] = {}
        for col in state["saved"]:
            by_collection[col] = by_collection.get(col, 0) + 1
        if len(by_collection) == 1:
            (col, n), = by_collection.items()
            lines.append(f"✅ Saved {n} video(s) to '{col}'")
        else:
            parts = ", ".join(f"{n} to '{col}'" for col, n in by_collection.items())
            lines.append(f"✅ Saved {len(state['saved'])} video(s): {parts}")
    if state["skipped"]:
        by_collection = {}
        for col in state["skipped"]:
            by_collection[col] = by_collection.get(col, 0) + 1
        parts = ", ".join(f"{n} in '{col}'" for col, n in by_collection.items())
        lines.append(f"⚠️ Skipped {len(state['skipped'])} duplicate(s): {parts}")
    if state["near_dups"]:
        by_collection = {}
        for col in state["near_dups"]:
            by_collection[col] = by_collection.get(col, 0) + 1
        parts = ", ".join(f"{n} in '{col}'" for col, n in by_collection.items())
        lines.append(
            f"🔎 {len(state['near_dups'])} possible near-duplicate(s) saved (similar duration/size to "
            f"an existing video, but not an exact match): {parts}. Worth a look — reply /remove on one "
            f"if it turns out to be a repeat."
        )
    if state["errors"]:
        lines.append(f"❌ {state['errors']} video(s) failed to save due to a database error.")
    if not lines:
        return
    try:
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except TelegramError:
        logger.exception("Failed to send batch summary to chat %s", chat_id)


def _queue_batch_result(chat_id: int, context: ContextTypes.DEFAULT_TYPE, *,
                         saved: str | None = None, skipped: str | None = None,
                         error: bool = False, near_dup: bool = False):
    state = _batch_state.get(chat_id)
    if state is None:
        state = {"saved": [], "skipped": [], "errors": 0, "near_dups": [], "task": None}
        _batch_state[chat_id] = state
    if saved:
        state["saved"].append(saved)
        if near_dup:
            state["near_dups"].append(saved)
    if skipped:
        state["skipped"].append(skipped)
    if error:
        state["errors"] += 1
    if state["task"] is not None and not state["task"].done():
        state["task"].cancel()
    state["task"] = asyncio.create_task(_flush_batch(chat_id, context))


_delete_batch_state: dict[int, dict] = {}


async def _flush_delete_batch(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    await asyncio.sleep(BATCH_DEBOUNCE_SECONDS)
    state = _delete_batch_state.get(chat_id)
    if state is None or state["task"] is not asyncio.current_task():
        return
    _delete_batch_state.pop(chat_id, None)
    lines = []
    if state["deleted"]:
        by_collection: dict[str, int] = {}
        for col in state["deleted"]:
            by_collection[col] = by_collection.get(col, 0) + 1
        parts = ", ".join(f"{n} from '{col}'" for col, n in by_collection.items())
        lines.append(f"🗑️ Deleted {len(state['deleted'])} video(s): {parts}")
    if state["not_found"]:
        by_collection = {}
        for col in state["not_found"]:
            by_collection[col] = by_collection.get(col, 0) + 1
        parts = ", ".join(f"{n} in '{col}'" for col, n in by_collection.items())
        lines.append(f"⚠️ {len(state['not_found'])} video(s) weren't found: {parts}")
    if state["errors"]:
        lines.append(f"❌ {state['errors']} video(s) failed to delete due to a database error.")
    if not lines:
        return
    try:
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except TelegramError:
        logger.exception("Failed to send delete batch summary to chat %s", chat_id)


def _queue_delete_batch_result(chat_id: int, context: ContextTypes.DEFAULT_TYPE, *,
                                deleted: str | None = None, not_found: str | None = None, error: bool = False):
    state = _delete_batch_state.get(chat_id)
    if state is None:
        state = {"deleted": [], "not_found": [], "errors": 0, "task": None}
        _delete_batch_state[chat_id] = state
    if deleted:
        state["deleted"].append(deleted)
    if not_found:
        state["not_found"].append(not_found)
    if error:
        state["errors"] += 1
    if state["task"] is not None and not state["task"].done():
        state["task"].cancel()
    state["task"] = asyncio.create_task(_flush_delete_batch(chat_id, context))


HELP_TEXT = (
    "🎬 Video Collector Bot\n\n"
    "Send or forward videos and I'll save them into named collections. "
    "Video files sent as documents work too. Duplicate videos within "
    "the same collection are skipped automatically. Collection names are "
    "not case-sensitive ('Mix' and 'mix' are the same collection).\n\n"
    "Commands:\n"
    "/collect <name> or <a>, <b> - Set the active collection(s)\n"
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
    "/export <name> - Backup file_ids as text\n"
    "/delete <name> - Delete collection permanently\n"
    "/status - Count in active collection(s)\n"
    "/random <name> - Send one random video\n"
    "/neardupes <name> - Find near-duplicates with comparison\n"
    "/dups <name> - Find exact duplicates across collections\n"
    "/recent <name> [n] - Show last N added videos\n"
    "/cleanup <name> - Remove dead/unavailable file_ids\n"
    "/stats - Overall database stats\n"
    "/help - Show this message"
)
'''

with open('/mnt/agents/output/bot.py', 'a') as f:
    f.write(part2)
print("Part 2 done")

part2 = r'''
async def _flush_batch(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    await asyncio.sleep(BATCH_DEBOUNCE_SECONDS)
    state = _batch_state.get(chat_id)
    if state is None or state["task"] is not asyncio.current_task():
        return
    _batch_state.pop(chat_id, None)
    lines = []
    if state["saved"]:
        by_collection: dict[str, int] = {}
        for col in state["saved"]:
            by_collection[col] = by_collection.get(col, 0) + 1
        if len(by_collection) == 1:
            (col, n), = by_collection.items()
            lines.append(f"✅ Saved {n} video(s) to '{col}'")
        else:
            parts = ", ".join(f"{n} to '{col}'" for col, n in by_collection.items())
            lines.append(f"✅ Saved {len(state['saved'])} video(s): {parts}")
    if state["skipped"]:
        by_collection = {}
        for col in state["skipped"]:
            by_collection[col] = by_collection.get(col, 0) + 1
        parts = ", ".join(f"{n} in '{col}'" for col, n in by_collection.items())
        lines.append(f"⚠️ Skipped {len(state['skipped'])} duplicate(s): {parts}")
    if state["near_dups"]:
        by_collection = {}
        for col in state["near_dups"]:
            by_collection[col] = by_collection.get(col, 0) + 1
        parts = ", ".join(f"{n} in '{col}'" for col, n in by_collection.items())
        lines.append(
            f"🔎 {len(state['near_dups'])} possible near-duplicate(s) saved (similar duration/size to "
            f"an existing video, but not an exact match): {parts}. Worth a look — reply /remove on one "
            f"if it turns out to be a repeat."
        )
    if state["errors"]:
        lines.append(f"❌ {state['errors']} video(s) failed to save due to a database error.")
    if not lines:
        return
    try:
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except TelegramError:
        logger.exception("Failed to send batch summary to chat %s", chat_id)


def _queue_batch_result(chat_id: int, context: ContextTypes.DEFAULT_TYPE, *,
                         saved: str | None = None, skipped: str | None = None,
                         error: bool = False, near_dup: bool = False):
    state = _batch_state.get(chat_id)
    if state is None:
        state = {"saved": [], "skipped": [], "errors": 0, "near_dups": [], "task": None}
        _batch_state[chat_id] = state
    if saved:
        state["saved"].append(saved)
        if near_dup:
            state["near_dups"].append(saved)
    if skipped:
        state["skipped"].append(skipped)
    if error:
        state["errors"] += 1
    if state["task"] is not None and not state["task"].done():
        state["task"].cancel()
    state["task"] = asyncio.create_task(_flush_batch(chat_id, context))


_delete_batch_state: dict[int, dict] = {}


async def _flush_delete_batch(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    await asyncio.sleep(BATCH_DEBOUNCE_SECONDS)
    state = _delete_batch_state.get(chat_id)
    if state is None or state["task"] is not asyncio.current_task():
        return
    _delete_batch_state.pop(chat_id, None)
    lines = []
    if state["deleted"]:
        by_collection: dict[str, int] = {}
        for col in state["deleted"]:
            by_collection[col] = by_collection.get(col, 0) + 1
        parts = ", ".join(f"{n} from '{col}'" for col, n in by_collection.items())
        lines.append(f"🗑️ Deleted {len(state['deleted'])} video(s): {parts}")
    if state["not_found"]:
        by_collection = {}
        for col in state["not_found"]:
            by_collection[col] = by_collection.get(col, 0) + 1
        parts = ", ".join(f"{n} in '{col}'" for col, n in by_collection.items())
        lines.append(f"⚠️ {len(state['not_found'])} video(s) weren't found: {parts}")
    if state["errors"]:
        lines.append(f"❌ {state['errors']} video(s) failed to delete due to a database error.")
    if not lines:
        return
    try:
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except TelegramError:
        logger.exception("Failed to send delete batch summary to chat %s", chat_id)


def _queue_delete_batch_result(chat_id: int, context: ContextTypes.DEFAULT_TYPE, *,
                                deleted: str | None = None, not_found: str | None = None, error: bool = False):
    state = _delete_batch_state.get(chat_id)
    if state is None:
        state = {"deleted": [], "not_found": [], "errors": 0, "task": None}
        _delete_batch_state[chat_id] = state
    if deleted:
        state["deleted"].append(deleted)
    if not_found:
        state["not_found"].append(not_found)
    if error:
        state["errors"] += 1
    if state["task"] is not None and not state["task"].done():
        state["task"].cancel()
    state["task"] = asyncio.create_task(_flush_delete_batch(chat_id, context))


HELP_TEXT = (
    "🎬 Video Collector Bot\n\n"
    "Send or forward videos and I'll save them into named collections. "
    "Video files sent as documents work too. Duplicate videos within "
    "the same collection are skipped automatically. Collection names are "
    "not case-sensitive ('Mix' and 'mix' are the same collection).\n\n"
    "Commands:\n"
    "/collect <name> or <a>, <b> - Set the active collection(s)\n"
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
    "/export <name> - Backup file_ids as text\n"
    "/delete <name> - Delete collection permanently\n"
    "/status - Count in active collection(s)\n"
    "/random <name> - Send one random video\n"
    "/neardupes <name> - Find near-duplicates with comparison\n"
    "/dups <name> - Find exact duplicates across collections\n"
    "/recent <name> [n] - Show last N added videos\n"
    "/cleanup <name> - Remove dead/unavailable file_ids\n"
    "/stats - Overall database stats\n"
    "/help - Show this message"
)
'''

with open('/mnt/agents/output/bot.py', 'a') as f:
    f.write(part2)
print("Part 2 done")

part3 = r'''
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Send me videos and I'll collect them. Use /collect <name> to start "
        "a named collection, then /help to see everything I can do."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT)


async def _set_active_collections(update: Update, chat_id: int, names: list[str]):
    bad = [n for n in names if n in RESERVED_NAMES]
    if bad:
        await update.message.reply_text(
            f"⚠️ '{', '.join(bad)}' is a reserved name and can't be used as a collection. "
            f"Reserved names: {', '.join(sorted(RESERVED_NAMES))}."
        )
        return
    active_collections[chat_id] = names
    paused_chats.discard(chat_id)
    removing_chats.discard(chat_id)
    if len(names) == 1:
        await update.message.reply_text(f"📁 Active collection set to: {names[0]}")
    else:
        await update.message.reply_text(
            f"📁 Active collections set to: {', '.join(names)}\nNew videos will be saved to all of them."
        )


async def collect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        current = ", ".join(get_active_collections(chat_id))
        await update.message.reply_text(
            f"Usage: /collect <name> or /collect <name1>, <name2>\nCurrently active: {current}"
        )
        return
    raw = " ".join(context.args)
    names = _parse_collection_names(raw)
    if not names:
        await update.message.reply_text("⚠️ Collection name can't be empty or just whitespace.")
        return
    await _set_active_collections(update, chat_id, names)


async def fav_shortcut(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    extra_raw = " ".join(context.args) if context.args else ""
    names = _parse_collection_names(extra_raw) if extra_raw else []
    if FAVORITES_COLLECTION not in names:
        names.append(FAVORITES_COLLECTION)
    await _set_active_collections(update, chat_id, names)


async def current(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    names = get_active_collections(chat_id)
    if chat_id in removing_chats:
        suffix = " (🗑️ remove mode — incoming videos are being deleted, not saved)"
    elif chat_id in paused_chats:
        suffix = " (⏸️ paused — incoming videos are not being saved)"
    else:
        suffix = ""
    await update.message.reply_text(f"📁 Active collection(s): {', '.join(names)}{suffix}")


async def finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    previous = get_active_collections(chat_id)
    active_collections.pop(chat_id, None)
    paused_chats.discard(chat_id)
    removing_chats.discard(chat_id)
    await update.message.reply_text(
        f"✅ Finished with '{', '.join(previous)}'. Active collection reset to '{DEFAULT_COLLECTION}'.\n"
        f"Use /collect <name> before sending more videos to start a new one."
    )


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    was_paused = chat_id in paused_chats
    paused_chats.add(chat_id)
    task = _active_tasks.get(chat_id)
    task_was_running = task is not None and not task.done()
    if task_was_running:
        task.cancel()
    if task_was_running:
        await update.message.reply_text(
            "🛑 Stopping the current operation, and pausing — any videos you send now won't be saved "
            "until you /collect or /fav again."
        )
    elif was_paused:
        await update.message.reply_text("Still paused — videos you send won't be saved until you /collect or /fav again.")
    else:
        await update.message.reply_text(
            "⏸️ Paused — videos you send now won't be saved until you /collect or /fav again."
        )


async def minlength_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        current = min_video_length.get(chat_id)
        if current is None:
            await update.message.reply_text("📹 Minimum video length filter is OFF — all videos are saved.")
        else:
            await update.message.reply_text(
                f"📹 Minimum video length filter is ON — videos shorter than {current} second(s) are skipped."
            )
        return
    arg = context.args[0].lower().strip()
    if arg == "off":
        min_video_length.pop(chat_id, None)
        await update.message.reply_text("✅ Minimum video length filter turned OFF.")
        return
    try:
        min_secs = int(arg)
        if min_secs < 1:
            await update.message.reply_text("⚠️ Minimum length must be at least 1 second.")
            return
        min_video_length[chat_id] = min_secs
        await update.message.reply_text(
            f"✅ Minimum video length filter set to {min_secs} second(s)."
        )
    except ValueError:
        await update.message.reply_text("Usage: /minlength <seconds> or /minlength off\nExample: /minlength 15")


async def removemode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    arg = context.args[0].lower().strip() if context.args else ""
    if arg in ("off", "stop"):
        was_on = chat_id in removing_chats
        removing_chats.discard(chat_id)
        if was_on:
            await update.message.reply_text("✅ Remove mode off — videos you send will be saved normally again.")
        else:
            await update.message.reply_text("Remove mode wasn't on.")
        return
    if arg and arg != "on":
        await update.message.reply_text("Usage: /removemode on|off")
        return
    active = get_active_collections(chat_id)
    removing_chats.add(chat_id)
    paused_chats.discard(chat_id)
    await update.message.reply_text(
        f"🗑️ Remove mode ON — send or forward videos and I'll delete them from: {', '.join(active)}\n"
        f"Use /removemode off when done."
    )


NEAR_DUP_DURATION_TOLERANCE_SECONDS = 2
NEAR_DUP_SIZE_TOLERANCE_FRACTION = 0.02
NEAR_DUP_SIZE_ONLY_TOLERANCE_FRACTION = 0.005


async def _save_video_to_collection(
    collection: str,
    file_id: str,
    file_unique_id: str,
    duration: int | None = None,
    file_size: int | None = None,
) -> tuple[bool, bool]:
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
                    logger.warning("Columns duration/file_size don't exist yet. Saving without them.")
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
                logger.exception("DB error deleting video from '%s'", collection)
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
    for collection in collections:
        try:
            inserted, possible_near_dup = await _save_video_to_collection(
                collection, file_id, file_unique_id, duration, file_size
            )
        except Exception:
            logger.exception("DB error saving video to '%s'", collection)
            _queue_batch_result(chat_id, context, error=True)
            continue
        if inserted:
            _queue_batch_result(chat_id, context, saved=collection, near_dup=possible_near_dup)
        else:
            _queue_batch_result(chat_id, context, skipped=collection)


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
'''

with open('/mnt/agents/output/bot.py', 'a') as f:
    f.write(part3)
print("Part 3 done")