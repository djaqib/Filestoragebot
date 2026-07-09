import os
import io
import time
import asyncio
import logging
import random
import psycopg2
from psycopg2 import pool
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

# Comma-separated list of Telegram user IDs allowed to use this bot, e.g.
# "123456789" or "123456789,987654321". Find your own ID by messaging
# @userinfobot on Telegram. Required — the bot refuses to start without it,
# since running with no allowlist would make it public again.
_raw_allowed = os.environ["ALLOWED_USER_IDS"]
ALLOWED_USER_IDS = {int(uid.strip()) for uid in _raw_allowed.split(",") if uid.strip()}
if not ALLOWED_USER_IDS:
    raise RuntimeError("ALLOWED_USER_IDS is set but empty — refusing to start with no allowed users.")

# Default collection name used until the user sets one with /collect
DEFAULT_COLLECTION = "default"

# The collection name that /fav is shorthand for.
FAVORITES_COLLECTION = "favorites"

# Reserved names that can't be used as a collection name via /collect, /rename,
# or /merge's destination (would be confusing or collide with internal concepts).
RESERVED_NAMES = {"default", "all"}

# How long to wait after the last video in a burst before sending one
# consolidated "Saved N videos" reply, instead of one reply per video.
BATCH_DEBOUNCE_SECONDS = 2.5

# How many collections are shown per page of /list.
LIST_PAGE_SIZE = 15

# Telegram's hard cap on videos per media group ("album").
ALBUM_SIZE = 10

# Seconds to wait between sending each album in /get. Keeps us under
# Telegram's flood limits; bump this up if you still hit RetryAfter errors.
ALBUM_DELAY_SECONDS = 3


def normalize_name(name: str) -> str:
    """Canonical form for a collection name: trimmed and lowercased, so
    'Mix', 'mix', and 'MIX' are all the same collection. Every collection
    name should be run through this before it touches the DB or any
    in-memory state (active_collections, etc)."""
    return name.strip().lower()


# ---------------------------------------------------------------------------
# Access control — this bot is for personal use only. Every update (command,
# video, button press) passes through here first, before any other handler
# runs. Anyone not in ALLOWED_USER_IDS gets a polite rejection and nothing
# else happens.
# ---------------------------------------------------------------------------

# Lightly rate-limit the "this bot is private" reply per unknown user, so
# someone poking at the bot repeatedly can't make it spam itself with outbound
# messages. One reply per UNAUTHORIZED_REPLY_COOLDOWN seconds per user id.
UNAUTHORIZED_REPLY_COOLDOWN = 60
_last_unauthorized_reply: dict[int, float] = {}


async def access_control(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user is None or user.id in ALLOWED_USER_IDS:
        return  # authorized (or no user attached, e.g. some channel posts) — let it through

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

    raise ApplicationHandlerStop  # stop processing — no other handler sees this update


# Simple connection pool so we don't open a fresh TCP/TLS handshake to Neon on
# every single message (Neon cold starts are fast, but no need to pay it twice).
db_pool = psycopg2.pool.SimpleConnectionPool(1, 5, DATABASE_URL, sslmode="require")

# Tracks which collection(s) each chat is currently adding videos to.
# Resets to [DEFAULT_COLLECTION] on bot restart; persisted videos themselves
# are NOT affected by a restart, only this "current pointer" in memory.
active_collections: dict[int, list[str]] = {}

# Chats where /stop was used to halt an in-progress *incoming* batch (e.g.
# forwarding 200 videos and changing your mind partway through). While a
# chat_id is in this set, handle_video silently ignores new videos. Cleared
# automatically the next time /collect, /fav, or /finish is used, since
# setting a new active collection is a clear signal you're starting again.
paused_chats: set[int] = set()

# Per-chat in-memory batch state for debounced "Saved" replies.
# Each entry: {"saved": [...names...], "skipped": [...names...], "errors": int, "task": asyncio.Task}
_batch_state: dict[int, dict] = {}

# Tracks /delete confirmations awaiting a button press: callback token -> collection name
_pending_deletes: dict[str, str] = {}

# Tracks the currently-running long operation per chat, so /cancel (or /stop)
# can interrupt it. Only one tracked task per chat at a time — starting a new
# tracked operation overwrites the previous entry (the old task, if somehow
# still running, just won't be cancellable anymore, which is fine since
# commands are processed one at a time per chat anyway).
_active_tasks: dict[int, asyncio.Task] = {}


def _track_task(chat_id: int, task: asyncio.Task):
    _active_tasks[chat_id] = task

    def _clear(_):
        if _active_tasks.get(chat_id) is task:
            _active_tasks.pop(chat_id, None)
    task.add_done_callback(_clear)


async def run_cancellable(chat_id: int, coro):
    """Wrap a coroutine as a cancellable task tracked for this chat, run it,
    and propagate CancelledError so callers can react (e.g. send a 'cancelled'
    message) without it looking like an unhandled crash."""
    task = asyncio.ensure_future(coro)
    _track_task(chat_id, task)
    return await task


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _db_call(fn):
    """Run a blocking DB function against a pooled connection. Meant to be
    wrapped in asyncio.to_thread() by callers so it never blocks the event loop."""
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
    """Async-friendly wrapper around _db_call. Raises on failure; callers
    should catch it and present a friendly error rather than crashing the handler."""
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
            # Maps a bot-sent message (chat_id, message_id) back to the
            # collection + video it came from, so /remove can work via reply.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sent_videos (
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    collection TEXT NOT NULL,
                    file_unique_id TEXT NOT NULL,
                    PRIMARY KEY (chat_id, message_id)
                )
            """)

            # One-time migration: collection names used to be case-sensitive,
            # so 'Mix' and 'mix' could exist as separate collections. Fold
            # everything down to lowercase now. Where two case-variants would
            # collide on the same video (same lower(collection), file_unique_id),
            # keep one copy and drop the rest so the UNIQUE constraint holds.
            cur.execute("""
                DELETE FROM videos v
                WHERE v.id NOT IN (
                    SELECT MIN(id) FROM videos GROUP BY LOWER(collection), file_unique_id
                )
            """)
            cur.execute("""
    CREATE TABLE IF NOT EXISTS shared_links (
        share_code TEXT PRIMARY KEY,
        share_type TEXT NOT NULL DEFAULT 'collection',
        target TEXT NOT NULL,
        created_by BIGINT NOT NULL,
        downloads INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW()
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
    """Split a comma-separated list of collection names, normalize (trim +
    lowercase), dedupe, drop empties."""
    names = [normalize_name(n) for n in raw.split(",")]
    names = [n for n in names if n]
    seen = []
    for n in names:
        if n not in seen:
            seen.append(n)
    return seen


def _parse_arrow_pair(args: list[str]) -> tuple[str, str] | None:
    """Parse '<source> -> <dest>' style command args (used by /rename,
    /merge, /copy). Accepts the arrow attached to a word too, e.g.
    'old->new' or 'old ->new'. Returns (source, dest) normalized, or None
    if no arrow was found."""
    raw = " ".join(args)
    if "->" not in raw:
        return None
    src, _, dest = raw.partition("->")
    src = normalize_name(src)
    dest = normalize_name(dest)
    if not src or not dest:
        return None
    return src, dest


# ---------------------------------------------------------------------------
# Batched "saved" replies (debounced so a burst of forwarded videos doesn't
# spam one reply per video)
# ---------------------------------------------------------------------------

async def _flush_batch(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    await asyncio.sleep(BATCH_DEBOUNCE_SECONDS)

    state = _batch_state.get(chat_id)
    # If our task was cancelled and replaced, a new task was registered under
    # state["task"]. We check whether the currently-running task is still the
    # one registered — if not, we're a stale task that should do nothing.
    # asyncio.current_task() returns this coroutine's own task object.
    if state is None or state["task"] is not asyncio.current_task():
        return

    # We're the authoritative flush — remove the state so it resets cleanly.
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

    if state["errors"]:
        lines.append(f"❌ {state['errors']} video(s) failed to save due to a database error.")

    if not lines:
        return  # nothing to report (shouldn't happen, but be safe)

    try:
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except TelegramError:
        logger.exception("Failed to send batch summary to chat %s", chat_id)


def _queue_batch_result(chat_id: int, context: ContextTypes.DEFAULT_TYPE, *,
                         saved: str | None = None, skipped: str | None = None, error: bool = False):
    state = _batch_state.get(chat_id)
    if state is None:
        state = {"saved": [], "skipped": [], "errors": 0, "task": None}
        _batch_state[chat_id] = state

    if saved:
        state["saved"].append(saved)
    if skipped:
        state["skipped"].append(skipped)
    if error:
        state["errors"] += 1

    # Reset the debounce timer: cancel any pending flush and schedule a new one.
    if state["task"] is not None and not state["task"].done():
        state["task"].cancel()
    state["task"] = asyncio.create_task(_flush_batch(chat_id, context))


# ---------------------------------------------------------------------------
# Help text
# ---------------------------------------------------------------------------

HELP_TEXT = (
    "🎬 *Video Collector Bot*\n\n"
    "Send or forward videos and I'll save them into named collections\\. "
    "Video files sent as documents work too\\. Duplicate videos within "
    "the same collection are skipped automatically\\. Collection names are "
    "not case\\-sensitive \\('Mix' and 'mix' are the same collection\\)\\.\n\n"
    "*Commands:*\n"
    "/collect `<name>` or `<a>, <b>` \\- Set the active collection\\(s\\)\n"
    "/fav \\- Shortcut for /collect favorites\n"
    "/finish \\- Stop adding to the active collection \\(resets to default\\)\n"
    "/stop \\- Cancel a running /get, and pause incoming videos until /collect or /fav\n"
    "/current \\- Show which collection\\(s\\) are active\n"
    "/list \\- List all collections and how many videos each has\n"
    "/get `<name>` `[page]` \\- Send back a collection's videos, in albums of 10 \\(optionally starting at a page\\)\n"
    "/remove \\- Reply to a video I sent with this to delete just that one\n"
    "/rename `<old> -> <new>` \\- Rename a collection\n"
    "/merge `<a> -> <b>` \\- Move all videos from a into b, then remove a\n"
    "/copy `<a> -> <b>` \\- Copy videos from a into b, keeping a intact\n"
    "/export `<name>` \\- Get a text file of file\\_ids for backup\n"
    "/delete `<name>` \\- Permanently delete a collection and its videos\n"
    "/status \\- Show video count in the active collection\\(s\\)\n"
    "/random `<name>` \\- Send back one random video from a collection\n"
    "/stats \\- Show overall stats across all collections\n"
    "/help \\- Show this help message"
)


# ---------------------------------------------------------------------------
# Basic commands
# ---------------------------------------------------------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Send me videos and I'll collect them. Use /collect <name> to start "
        "a named collection, then /help to see everything I can do."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode="MarkdownV2")


async def _set_active_collections(update: Update, chat_id: int, names: list[str]):
    """Shared logic for setting active collections, used by both /collect and /fav."""
    bad = [n for n in names if n in RESERVED_NAMES]
    if bad:
        await update.message.reply_text(
            f"⚠️ '{', '.join(bad)}' is a reserved name and can't be used as a collection. "
            f"Reserved names: {', '.join(sorted(RESERVED_NAMES))}."
        )
        return

    active_collections[chat_id] = names
    paused_chats.discard(chat_id)  # setting an active collection resumes saving if /stop paused it
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
    """/fav — shorthand for /collect favorites. If extra args are given,
    e.g. '/fav main', it sets active collections to ['main', 'favorites']
    so videos go to both at once."""
    chat_id = update.effective_chat.id
    extra_raw = " ".join(context.args) if context.args else ""
    names = _parse_collection_names(extra_raw) if extra_raw else []

    if FAVORITES_COLLECTION not in names:
        names.append(FAVORITES_COLLECTION)

    await _set_active_collections(update, chat_id, names)


async def current(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    names = get_active_collections(chat_id)
    suffix = " (⏸️ paused — incoming videos are not being saved)" if chat_id in paused_chats else ""
    await update.message.reply_text(f"📁 Active collection(s): {', '.join(names)}{suffix}")


async def finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    previous = get_active_collections(chat_id)
    active_collections.pop(chat_id, None)
    paused_chats.discard(chat_id)
    await update.message.reply_text(
        f"✅ Finished with '{', '.join(previous)}'. Active collection reset to '{DEFAULT_COLLECTION}'.\n"
        f"Use /collect <name> before sending more videos to start a new one."
    )


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/stop — two things, both relevant to "I changed my mind":
    1. Cancels whatever long-running operation is currently in progress for
       this chat (e.g. a /get mid-album-send). Safe to use: any DB statement
       already in flight either completes and commits, or gets interrupted
       and rolls back — there's no half-applied state left behind.
    2. Pauses incoming video saving — if you're mid-way through forwarding a
       big batch into a collection and want to stop, further videos you send
       are silently ignored until you /collect or /fav again."""
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


# ---------------------------------------------------------------------------
# Video handling
# ---------------------------------------------------------------------------

async def _save_video_to_collection(collection: str, file_id: str, file_unique_id: str) -> bool:
    """Returns True if inserted (new), False if it was a duplicate.
    Normalizes the collection name here as a final safety net so a
    non-lowercased name can never reach the DB regardless of call site."""
    collection = normalize_name(collection)

    def _insert(conn):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO videos (collection, file_id, file_unique_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (collection, file_unique_id) DO NOTHING
                RETURNING id
                """,
                (collection, file_id, file_unique_id),
            )
            return cur.fetchone() is not None
    return await db_run(_insert)


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if chat_id in paused_chats:
        return  # /stop was used — silently ignore until /collect or /fav resumes saving

    video = update.message.video
    if video is not None:
        file_id, file_unique_id = video.file_id, video.file_unique_id
    else:
        # A "document" that is actually a video (mime type video/* or .mp4/.mov/etc).
        doc = update.message.document
        file_id, file_unique_id = doc.file_id, doc.file_unique_id

    collections = get_active_collections(chat_id)

    for collection in collections:
        try:
            inserted = await _save_video_to_collection(collection, file_id, file_unique_id)
        except Exception:
            logger.exception("DB error saving video to '%s'", collection)
            _queue_batch_result(chat_id, context, error=True)
            continue

        if inserted:
            _queue_batch_result(chat_id, context, saved=collection)
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
    # else: a non-video document — ignored, same as photos/audio/voice.


async def handle_non_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Photos, audio, voice notes land here and are ignored.
    return


# ---------------------------------------------------------------------------
# /list
# ---------------------------------------------------------------------------

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

    await update.message.reply_text(f"{header}:\n" + "\n".join(lines) + footer)


# ---------------------------------------------------------------------------
# /get  (paginated, RetryAfter-safe, with album captions)
# ---------------------------------------------------------------------------

# How many videos per "page" before we pause and show a "Next page?" button.
GET_PAGE_SIZE = 50  # 5 albums of 10

# In-memory store for pending "next page" button state.
# token -> {"name": str, "offset": int, "rows": list}
_pending_pages: dict[str, dict] = {}


async def _send_page(
    chat_id: int,
    name: str,
    rows: list,
    page_num: int,
    total_pages: int,
    offset: int,
    context: ContextTypes.DEFAULT_TYPE,
    reply_msg,
):
    """Send one page worth of albums (up to GET_PAGE_SIZE videos).
    Returns list of (message_id, file_unique_id) for sent_videos recording.
    Handles RetryAfter by waiting the exact time Telegram specifies and retrying."""
    page_rows = rows[offset: offset + GET_PAGE_SIZE]
    total_albums_overall = (len(rows) + ALBUM_SIZE - 1) // ALBUM_SIZE
    album_offset = offset // ALBUM_SIZE

    sent_records = []
    last_sent_messages = None

    for album_idx, i in enumerate(range(0, len(page_rows), ALBUM_SIZE)):
        batch = page_rows[i: i + ALBUM_SIZE]
        global_album_num = album_offset + album_idx + 1
        caption = f"📼 Album {global_album_num}/{total_albums_overall} • Page {page_num}/{total_pages}"

        media_group = [
            InputMediaVideo(
                media=fid,
                caption=caption if j == 0 else None,
            )
            for j, (fid, _) in enumerate(batch)
        ]

        # Retry loop for Telegram flood control (RetryAfter).
        retries = 0
        last_sent_messages = None
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
                logger.exception("Failed to send album %d for '%s'", global_album_num, name)
                await reply_msg.reply_text(
                    f"⚠️ Album {global_album_num} failed to send (possibly an expired file) — skipping."
                )
                break

        if last_sent_messages:
            for msg, (_, file_unique_id) in zip(last_sent_messages, batch):
                sent_records.append((msg.message_id, file_unique_id))

        # Delay between albums keeps us well under Telegram's 20 messages/min
        # flood limit. This is also where /stop cancellation takes effect
        # between albums. See ALBUM_DELAY_SECONDS near the top of the file.
        await asyncio.sleep(ALBUM_DELAY_SECONDS)

    return sent_records


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
                        "SELECT file_id, file_unique_id FROM videos WHERE collection = %s ORDER BY added_at",
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

    # Clamp an out-of-range requested page (e.g. "/get default 999" on a
    # collection that only has 38 pages) to the last valid page instead of
    # producing an empty/negative slice.
    if offset >= total:
        offset = max(0, (total_pages - 1) * GET_PAGE_SIZE)

    page_num = offset // GET_PAGE_SIZE + 1
    total_albums = (total + ALBUM_SIZE - 1) // ALBUM_SIZE
    page_end = min(offset + GET_PAGE_SIZE, total)
    page_video_count = page_end - offset
    page_albums = (page_video_count + ALBUM_SIZE - 1) // ALBUM_SIZE

    await update.effective_message.reply_text(
        f"📤 Page {page_num}/{total_pages} — sending {page_albums} album(s) "
        f"({page_video_count} of {total} video(s)) from '{name}'... "
        f"(send /stop to cancel)"
    )

    sent_records = await _send_page(
        chat_id=chat_id,
        name=name,
        rows=rows,
        page_num=page_num,
        total_pages=total_pages,
        offset=offset,
        context=context,
        reply_msg=update.effective_message,
    )

    # Persist sent_videos per page (not just at the very end) so /remove works
    # even if a later page gets cancelled.
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

    next_offset = offset + GET_PAGE_SIZE
    if next_offset >= total:
        await update.effective_message.reply_text(f"✅ Done — all {total} video(s) from '{name}' sent.")
    else:
        remaining = total - next_offset
        token = f"{chat_id}:{name}:{next_offset}"
        _pending_pages[token] = {"name": name, "offset": next_offset, "rows": rows}
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                f"▶️ Next page ({remaining} video(s) left)",
                callback_data=f"getpage:{token}",
            ),
            InlineKeyboardButton("🛑 Stop here", callback_data=f"getstop:{chat_id}"),
        ]])
        await update.effective_message.reply_text(
            f"⏸ Page {page_num}/{total_pages} done — tap to load the next page.",
            reply_markup=keyboard,
        )


async def get_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    # Optional trailing page number, e.g. "/get default 28" jumps straight to
    # page 28 instead of starting from page 1. Useful if pagination ever gets
    # stuck on a previous run and you don't want to click "Next page" 27 times.
    args = list(context.args) if context.args else []
    offset = 0
    if args and args[-1].isdigit():
        page = max(1, int(args[-1]))
        offset = (page - 1) * GET_PAGE_SIZE
        args = args[:-1]
    context.args = args  # remaining args are just the collection name

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


async def get_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles 'Next page' and 'Stop here' inline buttons from /get pagination."""
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


# ---------------------------------------------------------------------------
# /remove  (reply to a bot-sent video)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# /rename   <old> -> <new>
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# /merge   <source> -> <dest>   (moves videos, removes source)
# ---------------------------------------------------------------------------

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
                # Move videos that don't already exist in dest (by file_unique_id);
                # duplicates are dropped rather than erroring on the UNIQUE constraint.
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
                # Whatever's left in src_name is duplicates of dest_name — drop them.
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


# ---------------------------------------------------------------------------
# /copy   <source> -> <dest>   (copies videos, source stays intact)
# ---------------------------------------------------------------------------

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
                # Insert into dest any (file_id, file_unique_id) from src that
                # dest doesn't already have. ON CONFLICT DO NOTHING handles the
                # case where it's already there.
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


# ---------------------------------------------------------------------------
# /export
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# /delete with inline-button confirmation
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# /status
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# /random — send back one random video from a collection
# ---------------------------------------------------------------------------

async def random_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if context.args:
        name = normalize_name(" ".join(context.args))
    else:
        # No name given — use the active collection if there's exactly one,
        # otherwise ask which one.
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

                return total_videos, total_collections, largest, oldest, newest
        total_videos, total_collections, largest, oldest, newest = await db_run(_query)
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
        BotCommand("stats", "Show overall database stats"),
        BotCommand("help", "Show help and command list"),
    ])


def main():
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # Access control runs first, in group -1, ahead of every other handler
    # (default group 0). Raises ApplicationHandlerStop on rejection so nothing
    # else processes the update.
    application.add_handler(TypeHandler(Update, access_control), group=-1)

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("collect", collect))
    application.add_handler(CommandHandler("fav", fav_shortcut))
    application.add_handler(CommandHandler("finish", finish))
    application.add_handler(CommandHandler("stop", stop_command))
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
    application.add_handler(CommandHandler("stats", stats))

    application.add_handler(CallbackQueryHandler(delete_callback, pattern=r"^del(confirm|cancel):"))
    application.add_handler(CallbackQueryHandler(get_page_callback, pattern=r"^get(page|stop):"))

    application.add_handler(MessageHandler(filters.VIDEO, handle_video))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(MessageHandler(
        filters.PHOTO | filters.AUDIO | filters.VOICE,
        handle_non_video,
    ))

    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        secret_token=WEBHOOK_SECRET,
        webhook_url=f"{RENDER_EXTERNAL_URL}/webhook",
        url_path="webhook",
    )


if __name__ == "__main__":
    main()
