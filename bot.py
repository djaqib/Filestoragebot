import os
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
    raise RuntimeError("ALLOWED_USER_IDS is set but empty — refusing to start with no allowed users.")

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
                await update.effective_message.reply_text(
                    "🔒 This bot is private and not available for public use."
                )
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
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_videos_collection_filesize ON videos (collection, file_size)"
            )
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
        lines.append(
            f"⚠️ {len(state['not_found'])} video(s) weren't found (already gone or never saved there): {parts}"
        )

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
    "🎬 *Video Collector Bot*\n\n"
    "Send or forward videos and I'll save them into named collections\. "
    "Video files sent as documents work too\. Duplicate videos within "
    "the same collection are skipped automatically\. Collection names are "
    "not case\-sensitive \('Mix' and 'mix' are the same collection\)\. "
    "If a saved video's duration and file size closely match another video "
    "already in the collection, I'll flag it as a possible near\-duplicate "
    "\(not an exact match, so it's saved either way — just a heads\-up\)\.\n\n"
    "*Commands:*\n"
    "/collect `<name>` or `<a>, <b>` \- Set the active collection\(s\)\n"
    "/fav \- Shortcut for /collect favorites\n"
    "/finish \- Stop adding to active collection\n"
    "/stop \- Cancel running /get, pause incoming videos\n"
    "/removemode `on|off` \- Bulk delete by forwarding videos\n"
    "/minlength `<seconds>` or `off` \- Skip short videos\n"
    "/current \- Show active collection\(s\)\n"
    "/list \- List all collections\n"
    "/get `<name>` `[page]` \- Send back videos in albums\n"
    "/remove \- Reply to a bot-sent video to delete it\n"
    "/rename `<old> -> <new>` \- Rename collection\n"
    "/merge `<a> -> <b>` \- Move videos, remove source\n"
    "/copy `<a> -> <b>` \- Copy videos, keep source\n"
    "/export `<name>` \- Backup file_ids as text\n"
    "/delete `<name>` \- Delete collection permanently\n"
    "/status \- Count in active collection\(s\)\n"
    "/random `<name>` \- Send one random video\n"
    "/neardupes `<name>` \- Find near-duplicates with comparison\n"
    "/dups `<name>` \- Find exact duplicates across collections\n"
    "/recent `<name>` `[n]` \- Show last N added videos\n"
    "/cleanup `<name>` \- Remove dead/unavailable file_ids\n"
    "/stats \- Overall database stats\n"
    "/help \- Show this message"
)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Send me videos and I'll collect them. Use /collect <name> to start "
        "a named collection, then /help to see everything I can do."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode="MarkdownV2")


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
            f"Usage: /collect <name> or /collect <name1>, <name2>\nCurrently active: *{current}*",
            parse_mode="MarkdownV2",
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
        await update.message.reply_text("✅ Minimum video length filter turned OFF — all videos will be saved.")
        return

    try:
        min_secs = int(arg)
        if min_secs < 1:
            await update.message.reply_text("⚠️ Minimum length must be at least 1 second.")
            return
        min_video_length[chat_id] = min_secs
        await update.message.reply_text(
            f"✅ Minimum video length filter set to {min_secs} second(s) — shorter videos will be skipped."
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
        f"(Only from the currently active collection(s), even if a video exists elsewhere too.)\n"
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
                    logger.warning(
                        "Columns duration/file_size don't exist yet (migration pending). "
                        "Saving without them. Run: ALTER TABLE videos ADD COLUMN IF NOT EXISTS duration INTEGER; "
                        "ALTER TABLE videos ADD COLUMN IF NOT EXISTS file_size BIGINT;"
                    )
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


async def list_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    name = query.data[len("listchoice:"):]
    await query.answer()

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📁 Set active", callback_data=f"listset:{name}"),
        InlineKeyboardButton("📤 Get videos", callback_data=f"listget:{name}"),
    ]])
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


async def list_get_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    name = query.data[len("listget:"):]
    chat_id = update.effective_chat.id

    await query.answer()
    await query.edit_message_text(f"📤 Fetching '{name}'...")

    try:
        await run_cancellable(chat_id, _get_collection_impl(update, context, name=name, offset=0))
    except asyncio.CancelledError:
        await context.bot.send_message(
            chat_id=chat_id,
            text="🛑 Stopped — albums already sent stay sent, nothing else will go out.",
        )
    except Exception:
        logger.exception("Unexpected error while sending collection '%s' from /list", name)
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "⚠️ Something went wrong partway through — some albums may have sent. "
                f"Try /get {name} <page> to resume from where it stopped."
            ),
        )


async def list_collections(update: Update, context: ContextTypes.DEFAULT_TYPE):
    search = None
    page = 1

    if context.args:
        args = list(context.args)
        if args[-1].isdigit():
            page = max(1, int(args[-1]))
            args = args[:-1]
        if args:
            search = normalize_name(" ".join(args))

    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT collection, COUNT(*) FROM videos GROUP BY collection ORDER BY collection"
                )
                return cur.fetchall()
        rows = await db_run(_query)
    except Exception:
        await reply_db_error(update, "list collections")
        return

    if search:
        rows = [r for r in rows if search in r[0]]

    if not rows:
        msg = "No collections yet. Send a video to start one." if not search else f"No collections match '{search}'."
        await update.message.reply_text(msg)
        return

    total_pages = (len(rows) + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE
    page = min(page, total_pages)
    start_idx = (page - 1) * LIST_PAGE_SIZE
    page_rows = rows[start_idx:start_idx + LIST_PAGE_SIZE]

    lines = [f"• {name} — {count} video(s)" for name, count in page_rows]
    header = "📚 Collections"
    if search:
        header += f" matching '{search}'"
    footer = ""
    if total_pages > 1:
        header += f" (page {page}/{total_pages})"
        if page < total_pages:
            next_args = f"{search + ' ' if search else ''}{page + 1}"
            footer = f"\n\nUse /list {next_args} for the next page."

    await update.message.reply_text(
        f"{header}:\n" + "\n".join(lines) + footer,
        reply_markup=InlineKeyboardMarkup(_build_list_set_buttons(page_rows)),
    )


def _build_list_set_buttons(page_rows) -> list:
    buttons = []
    for name, count in page_rows:
        data = f"listchoice:{name}"
        if len(data.encode("utf-8")) > 64:
            continue
        buttons.append([InlineKeyboardButton(f"📁 {name} ({count})", callback_data=data)])
    return buttons


_pending_pages: dict[str, dict] = {}


async def _send_single_video_with_fallback(
    chat_id: int,
    file_id: str,
    file_unique_id: str,
    caption: str | None,
    context: ContextTypes.DEFAULT_TYPE,
):
    try:
        return await context.bot.send_video(chat_id=chat_id, video=file_id, caption=caption)
    except TelegramError as e:
        logger.warning(
            "send_video failed for file_unique_id=%s (%s) — trying send_document fallback",
            file_unique_id, e,
        )
    try:
        return await context.bot.send_document(chat_id=chat_id, document=file_id, caption=caption)
    except TelegramError:
        logger.exception("send_document fallback also failed for file_unique_id=%s", file_unique_id)
        _dead_file_ids.add(file_unique_id)
        return None


async def _send_page(
    chat_id: int,
    name: str,
    rows: list,
    page_num: int,
    total_pages: int,
    offset: int,
    context: ContextTypes.DEFAULT_TYPE,
    reply_msg,
    album_size: int = ALBUM_SIZE,
    album_delay: float = ALBUM_DELAY_SECONDS,
    caption_prefix: str = "📼",
):
    page_rows = rows[offset: offset + GET_PAGE_SIZE]
    total_albums_overall = (len(rows) + album_size - 1) // album_size
    album_offset = offset // album_size

    sent_records = []
    album_statuses = []

    for album_idx, i in enumerate(range(0, len(page_rows), album_size)):
        batch = page_rows[i: i + album_size]
        global_album_num = album_offset + album_idx + 1
        local_album_num = album_idx + 1
        caption = f"{caption_prefix} Album {global_album_num}/{total_albums_overall} • Page {page_num}/{total_pages}"

        media_group = [
            InputMediaVideo(
                media=fid,
                caption=caption if j == 0 else None,
            )
            for j, (fid, _) in enumerate(batch)
        ]

        retries = 0
        last_sent_messages = None
        group_failed = False
        while True:
            try:
                last_sent_messages = await context.bot.send_media_group(
                    chat_id=chat_id, media=media_group
                )
                break
            except TelegramError as e:
                retry_after = getattr(e, "retry_after", None)
                if retry_after is not None and retries < 5:
                    retries += 1
                    logger.warning(
                        "Flood control on album %d: waiting %ss (retry %d/5)",
                        global_album_num, retry_after, retries,
                    )
                    await asyncio.sleep(float(retry_after) + 1)
                    continue
                logger.warning(
                    "Album %d failed to send as a group (%s) — retrying its videos individually",
                    global_album_num, e,
                )
                group_failed = True
                break

        if not group_failed and last_sent_messages:
            for msg, (_, file_unique_id) in zip(last_sent_messages, batch):
                sent_records.append((msg.message_id, file_unique_id))
            album_statuses.append((local_album_num, "ok"))
        else:
            failed_count = 0
            sent_count = 0
            for j, (fid, file_unique_id) in enumerate(batch):
                single_caption = caption if j == 0 else None
                msg = await _send_single_video_with_fallback(
                    chat_id, fid, file_unique_id, single_caption, context
                )
                if msg is not None:
                    sent_records.append((msg.message_id, file_unique_id))
                    sent_count += 1
                else:
                    failed_count += 1
                await asyncio.sleep(0.3)

            if failed_count == 0:
                album_statuses.append((local_album_num, "ok"))
            elif sent_count == 0:
                album_statuses.append((local_album_num, "fail"))
            else:
                album_statuses.append((local_album_num, "partial"))

            if failed_count:
                await reply_msg.reply_text(
                    f"⚠️ Album {global_album_num}: {failed_count} of {len(batch)} video(s) couldn't be sent "
                    f"at all — likely a dead file_id (expired or the original file is gone), not just an "
                    f"unsupported format. The other {sent_count} went through individually."
                )

        await asyncio.sleep(album_delay)

    return sent_records, album_statuses


PAGE_JUMP_GRID_COLUMNS = 5
PAGE_JUMP_WINDOW_SIZE = 15
PAGE_JUMP_AHEAD_OFFSETS = (20, 50)


def _register_jump_token(chat_id: int, name: str, rows: list, target_page: int) -> str:
    offset = (target_page - 1) * GET_PAGE_SIZE
    token = f"{chat_id}:{name}:{offset}"
    _pending_pages[token] = {"name": name, "offset": offset, "rows": rows}
    return token


def _build_page_jump_rows(chat_id: int, name: str, rows: list, page_num: int, total_pages: int) -> list:
    if total_pages <= 1:
        return []

    window_start = page_num
    window_end = min(page_num + PAGE_JUMP_WINDOW_SIZE - 1, total_pages)

    page_buttons = []
    for p in range(window_start, window_end + 1):
        token = _register_jump_token(chat_id, name, rows, p)
        label = f"·{p}·" if p == page_num else str(p)
        page_buttons.append(InlineKeyboardButton(label, callback_data=f"getpage:{token}"))

    grid_rows = [
        page_buttons[i:i + PAGE_JUMP_GRID_COLUMNS]
        for i in range(0, len(page_buttons), PAGE_JUMP_GRID_COLUMNS)
    ]

    jump_ahead_row = []
    for delta in PAGE_JUMP_AHEAD_OFFSETS:
        target = page_num + delta
        if target > total_pages or target <= window_end:
            continue
        token = _register_jump_token(chat_id, name, rows, target)
        jump_ahead_row.append(InlineKeyboardButton(f"→{target}", callback_data=f"getpage:{token}"))

    if jump_ahead_row:
        grid_rows.append(jump_ahead_row)

    return grid_rows


async def _get_collection_impl(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    name: str | None = None,
    offset: int = 0,
    rows: list | None = None,
):
    chat_id = update.effective_chat.id

    if name is None:
        if not context.args:
            await update.effective_message.reply_text("Usage: /get <name> [page]")
            return
        name = normalize_name(" ".join(context.args))

    if rows is None:
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
            await reply_db_error(update, f"fetch collection '{name}'")
            return

        if not rows:
            await update.effective_message.reply_text(f"No videos found in '{name}' ❌")
            return

    total = len(rows)
    total_pages = (total + GET_PAGE_SIZE - 1) // GET_PAGE_SIZE

    if offset >= total:
        offset = max(0, (total_pages - 1) * GET_PAGE_SIZE)

    page_num = offset // GET_PAGE_SIZE + 1
    total_albums = (total + ALBUM_SIZE - 1) // ALBUM_SIZE
    page_end = min(offset + GET_PAGE_SIZE, total)
    page_video_count = page_end - offset
    page_albums = (page_video_count + ALBUM_SIZE - 1) // ALBUM_SIZE

    _last_get_page[f"{chat_id}:{name}"] = page_num

    await update.effective_message.reply_text(
        f"📤 Page {page_num}/{total_pages} — sending {page_albums} album(s) "
        f"({page_video_count} of {total} video(s)) from '{name}'... "
        f"(send /stop to cancel)"
    )

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
            logger.exception("Failed to record sent_videos for '%s'", name)

    STATUS_EMOJI = {"ok": "✅", "partial": "⚠️", "fail": "❌"}
    STATUS_BUTTONS_PER_ROW = 8
    status_buttons = [
        InlineKeyboardButton(
            f"{num}{STATUS_EMOJI[status]}",
            callback_data=f"getalbumstatus:{page_num}:{num}:{status}",
        )
        for num, status in album_statuses
    ]
    status_rows = [
        status_buttons[i:i + STATUS_BUTTONS_PER_ROW]
        for i in range(0, len(status_buttons), STATUS_BUTTONS_PER_ROW)
    ]

    jump_rows = _build_page_jump_rows(chat_id, name, rows, page_num, total_pages)

    next_offset = offset + GET_PAGE_SIZE
    if next_offset >= total:
        await update.effective_message.reply_text(f"✅ Done — all {total} video(s) from '{name}' sent.")
        if status_rows:
            await update.message.reply_text(
                f"Album status for page {page_num} (tap a number for details):",
                reply_markup=InlineKeyboardMarkup(status_rows),
            )
        if jump_rows:
            await update.effective_message.reply_text(
                f"Jump to a page of '{name}' ({total_pages} pages total):",
                reply_markup=InlineKeyboardMarkup(jump_rows),
            )
    else:
        remaining = total - next_offset
        token = f"{chat_id}:{name}:{next_offset}"
        _pending_pages[token] = {"name": name, "offset": next_offset, "rows": rows}
        nav_row = [
            InlineKeyboardButton(
                f"▶️ Next page ({remaining} video(s) left)",
                callback_data=f"getpage:{token}",
            ),
            InlineKeyboardButton("🛑 Stop here", callback_data=f"getstop:{chat_id}"),
        ]
        keyboard = InlineKeyboardMarkup(status_rows + jump_rows + [nav_row])
        await update.effective_message.reply_text(
            f"⏸ Page {page_num}/{total_pages} done — tap to load the next page.",
            reply_markup=keyboard,
        )


async def get_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

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
        await update.effective_message.reply_text(
            "🛑 Stopped — albums already sent stay sent, nothing else will go out."
        )
    except Exception:
        logger.exception("Unexpected error while sending collection")
        await update.effective_message.reply_text(
            "⚠️ Something went wrong partway through — some albums may have sent. "
            "Try /get <name> <page> to resume from where it stopped."
        )


async def get_album_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    _, page_num, album_num, status = query.data.split(":")
    if status == "ok":
        await query.answer(f"Album {album_num} (page {page_num}): all 10 sent successfully ✅")
    elif status == "partial":
        await query.answer(
            f"Album {album_num} (page {page_num}): some videos sent, some couldn't ⚠️ — "
            f"see the message above for which ones failed.",
            show_alert=True,
        )
    else:
        await query.answer(
            f"Album {album_num} (page {page_num}): none of these sent ❌ — the file_id(s) are likely dead.",
            show_alert=True,
        )


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
    state = _pending_pages.pop(token, None)
    if state is None:
        await query.edit_message_text("⏱️ This page button has expired or was already used.")
        return

    await query.edit_message_text(f"📤 Loading next page of '{state['name']}'...")

    chat_id = update.effective_chat.id
    try:
        await run_cancellable(
            chat_id,
            _get_collection_impl(
                update=update,
                context=context,
                name=state["name"],
                offset=state["offset"],
                rows=state["rows"],
            ),
        )
    except asyncio.CancelledError:
        await context.bot.send_message(
            chat_id=chat_id,
            text="🛑 Stopped — albums already sent stay sent, nothing else will go out.",
        )
    except Exception:
        logger.exception("Unexpected error while sending next page for '%s'", state["name"])
        await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ Something went wrong loading the next page. Try /get again.",
        )

async def remove_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    replied = update.message.reply_to_message

    if replied is None:
        await update.message.reply_text(
            "Reply to a video I sent (via /get) with /remove to delete just that one."
        )
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
        await reply_db_error(update, "look up that video")
        return

    if record is None:
        await update.message.reply_text(
            "⚠️ I can't tell which video that is — either it wasn't sent by me via /get, "
            "or it's from before this feature was added."
        )
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
        await reply_db_error(update, "delete that video")
        return

    if deleted:
        await update.message.reply_text(f"🗑️ Removed that video from '{collection}'.")
    else:
        await update.message.reply_text(f"⚠️ That video was already removed from '{collection}'.")


async def _rename_collection_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pair = _parse_arrow_pair(context.args)
    if pair is None:
        await update.message.reply_text(
            "Usage: /rename <old name> -> <new name>\nExample: /rename mix -> favorites"
        )
        return
    old_name, new_name = pair

    if new_name in RESERVED_NAMES:
        await update.message.reply_text(f"⚠️ '{new_name}' is a reserved name.")
        return
    if old_name == new_name:
        await update.message.reply_text("⚠️ Old and new names are the same (after normalizing case).")
        return

    try:
        def _rename(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM videos WHERE collection = %s LIMIT 1", (old_name,))
                if cur.fetchone() is None:
                    return "not_found"
                cur.execute("SELECT 1 FROM videos WHERE collection = %s LIMIT 1", (new_name,))
                if cur.fetchone() is not None:
                    return "conflict"
                cur.execute(
                    "UPDATE videos SET collection = %s WHERE collection = %s",
                    (new_name, old_name),
                )
                cur.execute(
                    "UPDATE sent_videos SET collection = %s WHERE collection = %s",
                    (new_name, old_name),
                )
                return "ok"
        result = await db_run(_rename)
    except Exception:
        await reply_db_error(update, "rename that collection")
        return

    if result == "not_found":
        await update.message.reply_text(f"No collection named '{old_name}' found.")
    elif result == "conflict":
        await update.message.reply_text(
            f"⚠️ A collection named '{new_name}' already exists. Use /merge or /copy instead if you want to combine them."
        )
    else:
        await update.message.reply_text(f"✏️ Renamed '{old_name}' to '{new_name}'.")


async def rename_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        await run_cancellable(chat_id, _rename_collection_impl(update, context))
    except asyncio.CancelledError:
        await update.message.reply_text(
            "🛑 Stopped. Either nothing changed, or the rename already committed just before the stop — "
            "use /list to check."
        )


async def _merge_collections_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pair = _parse_arrow_pair(context.args)
    if pair is None:
        await update.message.reply_text(
            "Usage: /merge <source> -> <destination>\n"
            "Videos move from <source> into <destination>, then <source> is removed.\n"
            "Example: /merge mix -> favorites"
        )
        return
    src_name, dest_name = pair

    if src_name == dest_name:
        await update.message.reply_text("⚠️ Source and destination must be different collections.")
        return

    try:
        def _merge(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM videos WHERE collection = %s LIMIT 1", (src_name,))
                if cur.fetchone() is None:
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
                    (dest_name, src_name, dest_name),
                )
                moved = cur.rowcount
                cur.execute("DELETE FROM videos WHERE collection = %s", (src_name,))
                cur.execute(
                    "UPDATE sent_videos SET collection = %s WHERE collection = %s",
                    (dest_name, src_name),
                )
                return "ok", moved
        result, moved = await db_run(_merge)
    except Exception:
        await reply_db_error(update, "merge those collections")
        return

    if result == "not_found":
        await update.message.reply_text(f"No collection named '{src_name}' found.")
    else:
        await update.message.reply_text(
            f"🔀 Merged '{src_name}' into '{dest_name}' ({moved} video(s) moved; "
            f"any duplicates already in '{dest_name}' were skipped)."
        )


async def merge_collections(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        await run_cancellable(chat_id, _merge_collections_impl(update, context))
    except asyncio.CancelledError:
        await update.message.reply_text(
            "🛑 Stopped. The merge either fully completed or didn't run at all — "
            "Postgres commits the whole operation or none of it. Use /list to check."
        )


async def _copy_collection_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pair = _parse_arrow_pair(context.args)
    if pair is None:
        await update.message.reply_text(
            "Usage: /copy <source> -> <destination>\n"
            "Videos are copied into <destination>; <source> is left untouched.\n"
            "Example: /copy mix -> favorites"
        )
        return
    src_name, dest_name = pair

    if src_name == dest_name:
        await update.message.reply_text("⚠️ Source and destination must be different collections.")
        return
    if dest_name in RESERVED_NAMES:
        await update.message.reply_text(f"⚠️ '{dest_name}' is a reserved name.")
        return

    try:
        def _copy(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM videos WHERE collection = %s LIMIT 1", (src_name,))
                if cur.fetchone() is None:
                    return "not_found", 0
                cur.execute(
                    """
                    INSERT INTO videos (collection, file_id, file_unique_id)
                    SELECT %s, file_id, file_unique_id
                    FROM videos
                    WHERE collection = %s
                    ON CONFLICT (collection, file_unique_id) DO NOTHING
                    """,
                    (dest_name, src_name),
                )
                copied = cur.rowcount
                return "ok", copied
        result, copied = await db_run(_copy)
    except Exception:
        await reply_db_error(update, "copy that collection")
        return

    if result == "not_found":
        await update.message.reply_text(f"No collection named '{src_name}' found.")
    else:
        await update.message.reply_text(
            f"📋 Copied {copied} video(s) from '{src_name}' into '{dest_name}'. "
            f"'{src_name}' is unchanged."
        )


async def copy_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        await run_cancellable(chat_id, _copy_collection_impl(update, context))
    except asyncio.CancelledError:
        await update.message.reply_text(
            "🛑 Stopped. The copy either fully completed or didn't run at all — use /list to check."
        )


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
        await reply_db_error(update, f"export '{name}'")
        return

    if not rows:
        await update.message.reply_text(f"No videos found in '{name}' ❌")
        return

    lines = [f"# Export of collection '{name}' — {len(rows)} video(s)"]
    lines.append("# file_id\tfile_unique_id\tadded_at")
    for file_id, file_unique_id, added_at in rows:
        lines.append(f"{file_id}\t{file_unique_id}\t{added_at.isoformat()}")
    content = "\n".join(lines)

    bio = io.BytesIO(content.encode("utf-8"))
    bio.name = f"{name}_export.txt"

    await update.message.reply_document(
        document=bio,
        filename=f"{name}_export.txt",
        caption=(
            f"📦 Backup of '{name}' ({len(rows)} video(s)).\n"
            f"Note: file_ids can expire or become invalid if the bot's Telegram session changes — "
            f"this is a reference backup, not a guaranteed restore mechanism."
        ),
    )


async def delete_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        await reply_db_error(update, f"look up '{name}'")
        return

    if count == 0:
        await update.message.reply_text(f"No collection named '{name}' found.")
        return

    token = f"{chat_id}:{name}:{update.message.message_id}"
    _pending_deletes[token] = name

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, delete it", callback_data=f"delconfirm:{token}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"delcancel:{token}"),
        ]
    ])
    await update.message.reply_text(
        f"⚠️ Delete '{name}' and all {count} video(s) in it? This can't be undone.",
        reply_markup=keyboard,
    )


async def delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    action, token = query.data.split(":", 1)
    name = _pending_deletes.pop(token, None)

    if name is None:
        await query.answer("This confirmation has expired.")
        await query.edit_message_text("⏱️ This delete confirmation expired or was already used.")
        return

    if action == "delcancel":
        await query.answer("Cancelled")
        await query.edit_message_text(f"❎ Cancelled — '{name}' was not deleted.")
        return

    await query.answer("Deleting...")
    chat_id = update.effective_chat.id

    async def _do_delete():
        def _delete(conn):
            with conn.cursor() as cur:
                cur.execute("DELETE FROM videos WHERE collection = %s", (name,))
                deleted_count = cur.rowcount
                cur.execute("DELETE FROM sent_videos WHERE collection = %s", (name,))
                return deleted_count
        return await db_run(_delete)

    try:
        deleted_count = await run_cancellable(chat_id, _do_delete())
    except asyncio.CancelledError:
        await query.edit_message_text(
            f"🛑 Stopped. The delete of '{name}' either fully completed or didn't run at all "
            f"— use /list to check."
        )
        return
    except Exception:
        logger.exception("DB error deleting collection '%s'", name)
        await query.edit_message_text(
            f"⚠️ Couldn't delete '{name}' — the database didn't respond. Please try again."
        )
        return

    await query.edit_message_text(f"🗑️ Deleted '{name}' ({deleted_count} video(s)).")


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
        await reply_db_error(update, "check status")
        return

    if len(collections) == 1:
        c = collections[0]
        await update.message.reply_text(f"📦 '{c}' has {counts.get(c, 0)} video(s).")
    else:
        lines = [f"• '{c}' — {counts.get(c, 0)} video(s)" for c in collections]
        await update.message.reply_text("📦 Active collections:\n" + "\n".join(lines))


async def random_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if context.args:
        name = normalize_name(" ".join(context.args))
    else:
        active = get_active_collections(chat_id)
        if len(active) == 1:
            name = active[0]
        else:
            await update.message.reply_text(
                "Usage: /random <name>\n"
                f"(You have multiple active collections — {', '.join(active)} — so I need to know which one.)"
            )
            return

    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT file_id FROM videos WHERE collection = %s",
                    (name,),
                )
                return cur.fetchall()
        rows = await db_run(_query)
    except Exception:
        await reply_db_error(update, f"fetch a random video from '{name}'")
        return

    if not rows:
        await update.message.reply_text(f"No videos found in '{name}' ❌")
        return

    file_id = random.choice(rows)[0]
    try:
        await context.bot.send_video(chat_id=chat_id, video=file_id, caption=f"🎲 Random pick from '{name}'")
    except TelegramError:
        logger.exception("Failed to send random video from '%s'", name)
        await update.message.reply_text(
            "⚠️ That video failed to send (it may have expired). Try /random again for another pick."
        )

# ---------------------------------------------------------------------------
# /neardupes — IMPROVED: finds PAIRS with similarity stats, sends side-by-side
# ---------------------------------------------------------------------------

# Tokens for pending /neardupes operations
_pending_neardupes: dict[str, dict] = {}


async def neardupes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Find near-duplicate videos in a collection, group them into PAIRS
    with similarity stats, and offer smart side-by-side comparison fetch."""
    if not context.args:
        await update.message.reply_text("Usage: /neardupes <collection>")
        return

    name = normalize_name(" ".join(context.args))

    try:
        def _find_pairs(conn):
            with conn.cursor() as cur:
                # FIXED SQL: proper parentheses around the OR condition for duration null handling
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
                        END as size_diff_pct
                    FROM videos v1
                    JOIN videos v2 ON v1.collection = v2.collection 
                        AND v1.file_unique_id < v2.file_unique_id
                    WHERE v1.collection = %s
                      AND v1.file_size IS NOT NULL
                      AND v2.file_size IS NOT NULL
                      AND (
                          (v1.duration IS NOT NULL AND v2.duration IS NOT NULL
                           AND ABS(v1.duration - v2.duration) <= 2
                           AND v2.file_size BETWEEN v1.file_size * 0.98 AND v1.file_size * 1.02)
                          OR
                          ((v1.duration IS NULL OR v2.duration IS NULL)
                           AND v2.file_size BETWEEN v1.file_size * 0.995 AND v1.file_size * 1.005)
                      )
                    ORDER BY size_diff_pct DESC, dur_diff DESC
                    """,
                    (name,),
                )
                return cur.fetchall()
        pair_rows = await db_run(_find_pairs)
    except Exception:
        await reply_db_error(update, f"fetch near-dupes in '{name}'")
        return

    if not pair_rows:
        await update.message.reply_text(
            f"No potential near-duplicates found in '{name}' — all clear! ✅"
        )
        return

    # Build pair summary messages
    total_pairs = len(pair_rows)

    # Store pairs for fetch callback
    token = f"{update.effective_chat.id}:{name}:{int(time.time())}"

    # Flatten pairs into rows for sending (each pair = 2 videos side by side)
    flat_rows = []
    for row in pair_rows:
        v1_fid, v1_fuid, v1_dur, v1_size, v2_fid, v2_fuid, v2_dur, v2_size, dur_diff, size_diff, size_pct = row
        flat_rows.append((v1_fid, v1_fuid, v1_dur, v1_size))
        flat_rows.append((v2_fid, v2_fuid, v2_dur, v2_size))

    _pending_neardupes[token] = {
        "name": name, 
        "pairs": pair_rows,
        "flat_rows": flat_rows,
        "pair_index": 0
    }

    # Show summary of first few pairs
    summary_lines = [f"🔎 Found {total_pairs} potential near-duplicate pair(s) in '{name}':\n"]

    for i, row in enumerate(pair_rows[:NEARDUPES_PAIRS_PER_PAGE]):
        v1_fid, v1_fuid, v1_dur, v1_size, v2_fid, v2_fuid, v2_dur, v2_size, dur_diff, size_diff, size_pct = row
        dur_str1 = f"{v1_dur}s" if v1_dur is not None else "?s"
        dur_str2 = f"{v2_dur}s" if v2_dur is not None else "?s"
        size_str1 = f"{v1_size/1024/1024:.1f}MB" if v1_size else "?MB"
        size_str2 = f"{v2_size/1024/1024:.1f}MB" if v2_size else "?MB"

        summary_lines.append(
            f"Pair {i+1}: {dur_str1}/{size_str1} vs {dur_str2}/{size_str2} "
            f"(diff: {dur_diff}s, {size_pct}% size)"
        )

    if total_pairs > NEARDUPES_PAIRS_PER_PAGE:
        summary_lines.append(f"\n...and {total_pairs - NEARDUPES_PAIRS_PER_PAGE} more pairs.")

    summary_lines.append("\nTap 📤 Compare to fetch pairs side-by-side in small albums (2 videos each) so you can compare and delete duplicates easily.")

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(f"📤 Compare all {total_pairs} pair(s)", callback_data=f"neardupes:{token}"),
        InlineKeyboardButton("🗑️ Delete dead files first", callback_data=f"neardupcleanup:{token}"),
    ]])

    await update.message.reply_text(
        "\n".join(summary_lines),
        reply_markup=keyboard,
    )


async def neardupes_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback for the 'Compare near-dupes' button — sends pairs side-by-side
    in albums of 2 so you can easily compare and decide which to delete."""
    query = update.callback_query
    token = query.data[len("neardupes:"):]
    state = _pending_neardupes.pop(token, None)

    if state is None:
        await query.answer("⏱️ This button has expired.")
        return

    await query.answer()
    await query.edit_message_text(f"📤 Fetching near-duplicate comparison pairs from '{state['name']}'...")

    chat_id = update.effective_chat.id
    name = state["name"]
    pairs = state["pairs"]

    # Send pairs as small albums (2 videos each) with comparison info
    total_pairs = len(pairs)
    sent_count = 0
    failed_count = 0

    for i, row in enumerate(pairs):
        v1_fid, v1_fuid, v1_dur, v1_size, v2_fid, v2_fuid, v2_dur, v2_size, dur_diff, size_diff, size_pct = row

        dur_str1 = f"{v1_dur}s" if v1_dur is not None else "?s"
        dur_str2 = f"{v2_dur}s" if v2_dur is not None else "?s"
        size_str1 = f"{v1_size/1024/1024:.1f}MB" if v1_size else "?MB"
        size_str2 = f"{v2_size/1024/1024:.1f}MB" if v2_size else "?MB"

        caption = (
            f"🔎 Pair {i+1}/{total_pairs}\n"
            f"Left: {dur_str1} / {size_str1}\n"
            f"Right: {dur_str2} / {size_str2}\n"
            f"Diff: {dur_diff}s, {size_pct}% size"
        )

        # Try to send as album of 2
        media_group = [
            InputMediaVideo(media=v1_fid, caption=caption),
            InputMediaVideo(media=v2_fid),
        ]

        try:
            messages = await context.bot.send_media_group(chat_id=chat_id, media=media_group)
            sent_count += 2

            # Record sent videos for /remove
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
                            [(chat_id, msg.message_id, name, fuid) for msg, fuid in zip(messages, [v1_fuid, v2_fuid])],
                        )
                await db_run(_record)
            except Exception:
                logger.exception("Failed to record sent_videos for neardup pair %d", i+1)

        except TelegramError as e:
            logger.warning("Album send failed for pair %d: %s", i+1, e)
            # Fallback: send individually
            for fid, fuid in [(v1_fid, v1_fuid), (v2_fid, v2_fuid)]:
                msg = await _send_single_video_with_fallback(chat_id, fid, fuid, None, context)
                if msg:
                    sent_count += 1
                else:
                    failed_count += 1
            # Send caption separately
            try:
                await context.bot.send_message(chat_id=chat_id, text=caption)
            except TelegramError:
                pass

        # Small delay between pairs
        await asyncio.sleep(NEARDUP_ALBUM_DELAY)

    # Summary message with delete-all-dead button
    if failed_count > 0:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"✅ Sent {sent_count} videos in {total_pairs} pair(s).\n"
                f"⚠️ {failed_count} video(s) failed to send (dead file_ids).\n\n"
                f"Reply /remove to any video I sent to delete it from '{name}'.\n"
                f"Or use /cleanup {name} to remove all dead files at once."
            )
        )
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"✅ All {sent_count} videos sent in {total_pairs} pair(s).\n\n"
                f"Reply /remove to any video I sent to delete it from '{name}'."
            )
        )


# ---------------------------------------------------------------------------
# /dups — find exact duplicates (same file_unique_id across different collections)
# ---------------------------------------------------------------------------

async def find_dups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Find videos that exist in multiple collections (exact duplicates by file_unique_id)."""
    if not context.args:
        await update.message.reply_text("Usage: /dups <collection>\nFinds videos in this collection that also exist in other collections.")
        return

    name = normalize_name(" ".join(context.args))

    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT v1.file_unique_id, v1.duration, v1.file_size, 
                           ARRAY_AGG(DISTINCT v2.collection ORDER BY v2.collection) as other_collections
                    FROM videos v1
                    JOIN videos v2 ON v1.file_unique_id = v2.file_unique_id 
                        AND v1.collection != v2.collection
                    WHERE v1.collection = %s
                    GROUP BY v1.file_unique_id, v1.duration, v1.file_size
                    ORDER BY COUNT(DISTINCT v2.collection) DESC
                    """,
                    (name,),
                )
                return cur.fetchall()
        rows = await db_run(_query)
    except Exception:
        await reply_db_error(update, f"find duplicates in '{name}'")
        return

    if not rows:
        await update.message.reply_text(f"No exact duplicates found in '{name}' — all videos are unique across collections! ✅")
        return

    lines = [f"🔍 Found {len(rows)} video(s) in '{name}' that also exist elsewhere:\n"]
    for file_unique_id, duration, file_size, other_collections in rows[:20]:
        dur_str = f"{duration}s" if duration else "?s"
        size_str = f"{file_size/1024/1024:.1f}MB" if file_size else "?MB"
        lines.append(f"• {dur_str} / {size_str} — also in: {', '.join(other_collections)}")

    if len(rows) > 20:
        lines.append(f"\n...and {len(rows) - 20} more.")

    lines.append(f"\nUse /removemode on with /collect {name} to bulk-delete these by forwarding them.")

    await update.message.reply_text("\n".join(lines))


# ---------------------------------------------------------------------------
# /recent — show last N videos added to a collection
# ---------------------------------------------------------------------------

async def recent_videos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show the last N videos added to a collection, with stats."""
    if not context.args:
        await update.message.reply_text("Usage: /recent <name> [n]\nShows the last n videos (default 10).")
        return

    args = list(context.args)
    n = 10
    if args[-1].isdigit():
        n = max(1, min(50, int(args[-1])))
        args = args[:-1]

    if not args:
        await update.message.reply_text("Usage: /recent <name> [n]")
        return

    name = normalize_name(" ".join(args))

    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT file_id, file_unique_id, duration, file_size, added_at
                    FROM videos
                    WHERE collection = %s
                    ORDER BY added_at DESC
                    LIMIT %s
                    """,
                    (name, n),
                )
                return cur.fetchall()
        rows = await db_run(_query)
    except Exception:
        await reply_db_error(update, f"fetch recent videos from '{name}'")
        return

    if not rows:
        await update.message.reply_text(f"No videos found in '{name}' ❌")
        return

    lines = [f"📅 Last {len(rows)} video(s) added to '{name}':\n"]
    for i, (file_id, file_unique_id, duration, file_size, added_at) in enumerate(rows, 1):
        dur_str = f"{duration}s" if duration else "?s"
        size_str = f"{file_size/1024/1024:.1f}MB" if file_size else "?MB"
        date_str = added_at.strftime("%Y-%m-%d %H:%M") if added_at else "?"
        lines.append(f"{i}. {dur_str} / {size_str} — {date_str}")

    await update.message.reply_text("\n".join(lines))


# ---------------------------------------------------------------------------
# /cleanup — remove dead file_ids from a collection
# ---------------------------------------------------------------------------

async def cleanup_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove videos whose file_ids are dead (failed to send) from a collection."""
    if not context.args:
        await update.message.reply_text("Usage: /cleanup <name>\nRemoves videos that consistently fail to send (dead file_ids).")
        return

    name = normalize_name(" ".join(context.args))

    try:
        def _query(conn):
            with conn.cursor() as cur:
                # Find videos that have been marked as dead
                cur.execute(
                    """
                    SELECT v.file_unique_id, v.duration, v.file_size
                    FROM videos v
                    JOIN dead_files d ON v.file_unique_id = d.file_unique_id
                    WHERE v.collection = %s
                    """,
                    (name,),
                )
                return cur.fetchall()
        dead_rows = await db_run(_query)
    except Exception:
        await reply_db_error(update, f"check dead files in '{name}'")
        return

    if not dead_rows:
        await update.message.reply_text(f"✅ No dead file_ids found in '{name}' — all videos should be sendable.")
        return

    # Show what will be deleted and ask for confirmation
    lines = [f"🧹 Found {len(dead_rows)} dead video(s) in '{name}':\n"]
    for file_unique_id, duration, file_size in dead_rows[:10]:
        dur_str = f"{duration}s" if duration else "?s"
        size_str = f"{file_size/1024/1024:.1f}MB" if file_size else "?MB"
        lines.append(f"• {dur_str} / {size_str}")

    if len(dead_rows) > 10:
        lines.append(f"...and {len(dead_rows) - 10} more.")

    token = f"{update.effective_chat.id}:{name}:cleanup"
    _pending_deletes[token] = name  # Reuse pending_deletes mechanism

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, remove dead files", callback_data=f"cleanupconfirm:{token}"),
        InlineKeyboardButton("❌ Cancel", callback_data=f"cleanupcancel:{token}"),
    ]])

    await update.message.reply_text(
        "\n".join(lines) + "\n\nThese videos can't be sent anymore (file_id expired or revoked). Remove them?",
        reply_markup=keyboard,
    )


async def cleanup_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle cleanup confirmation/cancel buttons."""
    query = update.callback_query
    action, token = query.data.split(":", 1)
    name = _pending_deletes.pop(token, None)

    if name is None:
        await query.answer("This confirmation has expired.")
        await query.edit_message_text("⏱️ This cleanup confirmation expired or was already used.")
        return

    if action == "cleanupcancel":
        await query.answer("Cancelled")
        await query.edit_message_text(f"❎ Cancelled — no files were removed from '{name}'.")
        return

    await query.answer("Cleaning up...")
    chat_id = update.effective_chat.id

    try:
        def _cleanup(conn):
            with conn.cursor() as cur:
                # Delete dead videos from the collection
                cur.execute(
                    """
                    DELETE FROM videos
                    WHERE collection = %s
                      AND file_unique_id IN (SELECT file_unique_id FROM dead_files)
                    """,
                    (name,),
                )
                removed = cur.rowcount
                # Clean up dead_files entries that no longer reference any videos
                cur.execute(
                    """
                    DELETE FROM dead_files d
                    WHERE NOT EXISTS (
                        SELECT 1 FROM videos v WHERE v.file_unique_id = d.file_unique_id
                    )
                    """
                )
                return removed
        removed = await db_run(_cleanup)
    except Exception:
        logger.exception("DB error cleaning up dead files from '%s'", name)
        await query.edit_message_text(
            f"⚠️ Couldn't clean up '{name}' — the database didn't respond. Please try again."
        )
        return

    await query.edit_message_text(f"🧹 Removed {removed} dead video(s) from '{name}'.")


# ---------------------------------------------------------------------------
# /stats — overview of the whole database
# ---------------------------------------------------------------------------

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM videos")
                total_videos = cur.fetchone()[0]

                cur.execute("SELECT COUNT(DISTINCT collection) FROM videos")
                total_collections = cur.fetchone()[0]

                cur.execute(
                    "SELECT collection, COUNT(*) AS n FROM videos GROUP BY collection ORDER BY n DESC LIMIT 1"
                )
                largest = cur.fetchone()

                cur.execute("SELECT collection, added_at FROM videos ORDER BY added_at ASC LIMIT 1")
                oldest = cur.fetchone()

                cur.execute("SELECT collection, added_at FROM videos ORDER BY added_at DESC LIMIT 1")
                newest = cur.fetchone()

                cur.execute("SELECT COUNT(*) FROM dead_files")
                dead_count = cur.fetchone()[0]

                return total_videos, total_collections, largest, oldest, newest, dead_count
        total_videos, total_collections, largest, oldest, newest, dead_count = await db_run(_query)
    except Exception:
        await reply_db_error(update, "compute stats")
        return

    if total_videos == 0:
        await update.message.reply_text("📊 No videos saved yet — send one to get started.")
        return

    lines = [
        "📊 *Stats*",
        f"Total videos: {total_videos}",
        f"Total collections: {total_collections}",
    ]
    if largest:
        lines.append(f"Largest collection: '{largest[0]}' ({largest[1]} video(s))")
    if oldest:
        lines.append(f"Oldest addition: '{oldest[0]}' on {oldest[1].strftime('%Y-%m-%d')}")
    if newest:
        lines.append(f"Newest addition: '{newest[0]}' on {newest[1].strftime('%Y-%m-%d')}")
    if dead_count:
        lines.append(f"Dead file_ids tracked: {dead_count} (use /cleanup to remove)")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

async def post_init(application: Application):
    init_db()
    await application.bot.set_my_commands([
        BotCommand("collect", "Set active collection(s)"),
        BotCommand("fav", "Shortcut for /collect favorites"),
        BotCommand("finish", "Stop adding to active collection"),
        BotCommand("stop", "Cancel whatever is currently running"),
        BotCommand("minlength", "Set minimum video duration filter"),
        BotCommand("removemode", "Toggle bulk-delete-by-forwarding mode"),
        BotCommand("current", "Show the active collection(s)"),
        BotCommand("list", "List all collections"),
        BotCommand("get", "Send back a collection's videos"),
        BotCommand("remove", "Reply to a video to delete it"),
        BotCommand("rename", "Rename a collection"),
        BotCommand("merge", "Move videos into another collection"),
        BotCommand("copy", "Copy videos into another collection"),
        BotCommand("export", "Export a collection's file_ids"),
        BotCommand("delete", "Delete a collection"),
        BotCommand("status", "Show count in active collection"),
        BotCommand("random", "Send a random video from a collection"),
        BotCommand("neardupes", "Find near-duplicates with comparison"),
        BotCommand("dups", "Find exact duplicates across collections"),
        BotCommand("recent", "Show last N added videos"),
        BotCommand("cleanup", "Remove dead/unavailable file_ids"),
        BotCommand("stats", "Show overall database stats"),
        BotCommand("help", "Show help and command list"),
    ])


async def telegram_webhook(request):
    """Receives Telegram's POST to /webhook."""
    application = request.app.state.application

    secret_header = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if secret_header != WEBHOOK_SECRET:
        logger.warning("Rejected webhook POST with missing/invalid secret token header")
        return Response(status_code=401)

    data = await request.json()
    update = Update.de_json(data=data, bot=application.bot)
    await application.update_queue.put(update)
    return Response(status_code=200)


async def health_check(request):
    """Plain 200 OK for uptime monitors."""
    return PlainTextResponse("OK")


async def run():
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    application.add_handler(TypeHandler(Update, access_control), group=-1)

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
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
    application.add_handler(CommandHandler("delete", delete_collection))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("random", random_video))
    application.add_handler(CommandHandler("neardupes", neardupes))
    application.add_handler(CommandHandler("dups", find_dups))
    application.add_handler(CommandHandler("recent", recent_videos))
    application.add_handler(CommandHandler("cleanup", cleanup_collection))
    application.add_handler(CommandHandler("stats", stats))

    application.add_handler(CallbackQueryHandler(delete_callback, pattern=r"^del(confirm|cancel):"))
    application.add_handler(CallbackQueryHandler(get_page_callback, pattern=r"^get(page|stop):"))
    application.add_handler(CallbackQueryHandler(get_album_status_callback, pattern=r"^getalbumstatus:"))
    application.add_handler(CallbackQueryHandler(list_choice_callback, pattern=r"^listchoice:"))
    application.add_handler(CallbackQueryHandler(list_set_callback, pattern=r"^listset:"))
    application.add_handler(CallbackQueryHandler(list_get_callback, pattern=r"^listget:"))
    application.add_handler(CallbackQueryHandler(neardupes_callback, pattern=r"^neardupes:"))
    application.add_handler(CallbackQueryHandler(cleanup_callback, pattern=r"^cleanup(confirm|cancel):"))

    application.add_handler(MessageHandler(filters.VIDEO, handle_video))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(MessageHandler(
        filters.PHOTO | filters.AUDIO | filters.VOICE,
        handle_non_video,
    ))

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
