"""
handlers.py - All Telegram command/callback handlers and in-memory bot state.

State dicts here (active_collections, _get_sessions, etc.) are pure in-memory
Python state with no persistence - they live here because every reader and
writer of them is a handler function in this file.
"""
import asyncio
import io
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaVideo,
    Message,
    Update,
)
from telegram.error import TelegramError
from telegram.ext import ContextTypes

from db import _fetch_near_duplicates, _save_video_to_db, _delete_video_from_collection, _under_clause, db_run
from utils import (
    ADMIN_USER_ID,
    DEFAULT_COLLECTION,
    GET_ALBUM_SEND_DELAY,
    GET_BATCH_SIZE,
    GET_MAX_BATCH_PAGES,
    GET_SORT_MODES,
    NEARDUP_ALBUM_DELAY,
    NEARDUPES_PAIRS_PER_PAGE,
    SAVE_PROGRESS_INTERVAL,
    SAVE_SUMMARY_DEBOUNCE_SECONDS,
    SEARCH_RESULT_LIMIT,
    _escape_ilike,
    _format_duration,
    _format_size,
    _is_video_document,
    _parse_arrow_pair,
    _parse_get_args,
    _parse_search_args,
    describe_path_error,
    normalize_name,
    validate_collection_path,
)

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# In-memory bot state
# ----------------------------------------------------------------------
active_collections: Dict[int, List[str]] = {}
paused_chats: Set[int] = set()
removing_chats: Set[int] = set()
auto_delete_chats: Set[int] = set()
min_video_length: Dict[int, int] = {}
_active_tasks: Dict[int, asyncio.Task] = {}
_get_sessions: Dict[int, Tuple[List[Tuple[str, str]], str, int, Message]] = {}
_get_batch_pages: Dict[int, int] = {}
_awaiting_page_jump: Set[int] = set()
_search_sessions: Dict[int, List[Tuple[str, str, str, str, Optional[int], Optional[int]]]] = {}
_save_counts: Dict[int, Dict] = {}
_save_notify_tasks: Dict[int, asyncio.Task] = {}

def get_active_collections(chat_id: int) -> List[str]:
    return active_collections.get(chat_id, [DEFAULT_COLLECTION])

# ----------------------------------------------------------------------
# Access control
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
# Saving videos & progress reporting
# ----------------------------------------------------------------------
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
    if stats["failed"]:
        parts.append(f"⚠️ {stats['failed']} failed")
    if not parts:
        return

    cols = ", ".join(f"`{c}`" for c in sorted(stats["cols"]))
    text = " · ".join(parts) + (f" — {cols}" if cols else "")
    try:
        await context.bot.send_message(chat_id, text, parse_mode="Markdown")
    except TelegramError:
        pass

async def _send_save_progress(chat_id: int, saved: int, skipped: int, failed: int, context: ContextTypes.DEFAULT_TYPE):
    parts = [f"⏳ {saved} saved so far"]
    if skipped:
        parts.append(f"{skipped} skipped")
    if failed:
        parts.append(f"{failed} failed")
    # Always a brand-new message (never edited in place) so it lands at the
    # current bottom of the chat instead of getting buried above incoming videos.
    try:
        await context.bot.send_message(chat_id, " · ".join(parts) + "...")
    except TelegramError:
        pass

def _record_activity(chat_id: int, collection: str, kind: str, context: ContextTypes.DEFAULT_TYPE):
    stats = _save_counts.setdefault(chat_id, {"saved": 0, "skipped": 0, "removed": 0, "failed": 0, "cols": set()})
    stats[kind] += 1
    stats["cols"].add(collection)

    if kind == "saved" and stats["saved"] % SAVE_PROGRESS_INTERVAL == 0:
        asyncio.create_task(_send_save_progress(chat_id, stats["saved"], stats["skipped"], stats["failed"], context))

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
    all_ok = True

    for col in collections:
        try:
            if is_remove:
                removed = await _delete_video_from_collection(col, video.file_unique_id)
                _record_activity(chat_id, col, "removed" if removed else "skipped", context)
            else:
                saved = await _save_video_to_db(col, video.file_id, video.file_unique_id, video.duration, video.file_size, getattr(video, "file_name", None))
                _record_activity(chat_id, col, "saved" if saved else "skipped", context)
        except Exception:
            logger.exception("Failed to save/remove video in '%s'", col)
            _record_activity(chat_id, col, "failed", context)
            all_ok = False

    if all_ok and not is_remove and chat_id in auto_delete_chats:
        try:
            await update.message.delete()
        except TelegramError:
            pass

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
    all_ok = True

    for col in collections:
        try:
            if is_remove:
                removed = await _delete_video_from_collection(col, doc.file_unique_id)
                _record_activity(chat_id, col, "removed" if removed else "skipped", context)
            else:
                saved = await _save_video_to_db(col, doc.file_id, doc.file_unique_id, None, doc.file_size, doc.file_name)
                _record_activity(chat_id, col, "saved" if saved else "skipped", context)
        except Exception:
            logger.exception("Failed to save/remove video in '%s'", col)
            _record_activity(chat_id, col, "failed", context)
            all_ok = False

    if all_ok and not is_remove and chat_id in auto_delete_chats:
        try:
            await update.message.delete()
        except TelegramError:
            pass

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
        "   ◦ `--sort longest|shortest|largest|smallest` - sort by duration or file size\n"
        "• /list - Browse all collections\n"
        "• /random [name] - Send random video\n"
        "• /status - Video count for your active collection(s)\n"
        "• /count <name> - Video count for any named collection\n"
        "• /info <name> - Storage size, avg duration, first/last added\n"
        "• /delete <name> - Delete collection\n"
        "• /rename <old> -> <new> - Rename collection\n"
        "• /move <src> -> <dest> - Move videos\n"
        "• /copy <src> -> <dest> - Copy videos\n"
        "• /merge <src> -> <dest> - Merge collections\n"
        "• /remove - Reply to a video with this to delete just that video\n"
        "• /removemode on|off - Auto-delete every video you forward instead of saving it\n"
        "• /autodelete on|off - Delete the forwarded message itself once it's saved to every active collection\n"
        "• /minlength <sec> - Ignore videos shorter than this when saving\n"
        "• /setexpiry <name> <days> - Auto-delete a collection's videos after N days (0 disables) — admin only\n\n"
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
        "🧹 *Cleanup*\n"
        "• /dups <name> - Find exact duplicate videos\n"
        "• /neardupes <name> - Visual near-duplicate cleanup\n"
        "• /cleanup <name> - Remove references to videos Telegram can no longer serve\n\n"
        "💾 *Backup & Transfer*\n"
        "• /export <name> - Export a plain-text list of file IDs\n"
        "• /exportjson <name> - Export full video metadata as JSON\n"
        "• /importjson - Reply to a JSON export file with this to re-import it — admin only\n"
        "• /backup - Export the entire database as JSON — admin only"
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
                for c in cols:
                    if c != folder_prefix:
                        rel = c[len(folder_prefix) + 1:]
                        subfolders.add(rel.split("/")[0])

                cur.execute(f"SELECT COUNT(*) FROM videos WHERE {clause}", params)
                total_videos = cur.fetchone()[0]
                return sorted(list(subfolders)), total_videos
        subfolders, total_videos = await db_run(_get_sub_items)
    except Exception as e:
        await reply_db_error(update, f"fetch items for '{folder_prefix}'", e)
        return

    keyboard = []

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
        f"📁 Folder: `{folder_prefix}` — {total_videos} video(s)",
        reply_markup=InlineKeyboardMarkup(keyboard),
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

    name_str = None
    sort_mode = None
    if update.message and update.message.text and update.message.text.startswith("/get"):
        # Real /get command - parse full syntax including --sort.
        name_str, sort_mode, err = _parse_get_args(update.message.text)
        if err:
            await update.message.reply_text(
                f"⚠️ {err}\nUsage: /get [name] [--sort longest|shortest|largest|smallest]"
            )
            return
    elif context.args:
        # Invoked programmatically by a button callback with context.args already set.
        name_str = " ".join(context.args)

    name = normalize_name(name_str) if name_str else get_active_collections(chat_id)[0]
    order_sql = GET_SORT_MODES.get(sort_mode, "added_at")

    try:
        def _fetch(conn):
            with conn.cursor() as cur:
                clause, params = _under_clause(name)
                cur.execute(f"SELECT file_id, file_unique_id FROM videos WHERE {clause} ORDER BY {order_sql}", params)
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

    sort_label = f" (sorted: {sort_mode})" if sort_mode else ""
    session_msg = await context.bot.send_message(
        chat_id,
        f"📦 Preparing to send {total} video(s) from `{name}`{sort_label} in pages...",
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

async def _render_get_page(chat_id: int, context: ContextTypes.DEFAULT_TYPE, as_new: bool = False):
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

    if as_new:
        # The old control card is stale (videos were just sent below where it
        # used to be) - drop it and send a fresh one so it lands at the
        # current bottom of the chat instead of staying pinned above new content.
        try:
            await msg.delete()
        except TelegramError:
            pass
        try:
            new_msg = await context.bot.send_message(
                chat_id, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
            )
            _get_sessions[chat_id] = (file_rows, name, page, new_msg)
        except TelegramError:
            pass
    else:
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

    # Drop the old control card up front - Telegram's native "uploading"
    # indicator covers feedback during the send, and this avoids a message
    # that would otherwise sit stranded above the videos we're about to send.
    try:
        await msg.delete()
    except TelegramError:
        pass

    failed = await _send_pages(chat_id, file_rows, name, start_page, end_page, context)

    new_page = min(end_page + 1, total_pages)
    if failed:
        await context.bot.send_message(chat_id, f"⚠️ {failed} video(s) could not be sent (marked dead).")

    # Placeholder msg reference for the session - _render_get_page(as_new=True)
    # sends the real message and overwrites this before anything reads it.
    _get_sessions[chat_id] = (file_rows, name, new_page, msg)
    await _render_get_page(chat_id, context, as_new=True)

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

async def autodelete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        status = "ON" if chat_id in auto_delete_chats else "OFF"
        await update.message.reply_text(f"🧹 Auto-delete is currently {status}.\nUsage: /autodelete on|off")
        return

    val = context.args[0].lower()
    if val in ("on", "1", "enable", "true"):
        auto_delete_chats.add(chat_id)
        await update.message.reply_text(
            "🧹 Auto-delete ON. Forwarded videos will be removed from this chat once saved to every active collection."
        )
    elif val in ("off", "0", "disable", "false"):
        auto_delete_chats.discard(chat_id)
        await update.message.reply_text("🧹 Auto-delete OFF. Forwarded videos will stay in this chat after saving.")
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

async def near_duplicates_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /neardupes <collection>")
        return
    name = normalize_name(" ".join(context.args))

    try:
        pairs = await asyncio.to_thread(_fetch_near_duplicates, name)
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
                InlineKeyboardButton("Keep Both", callback_data=f"ndkeep:{t1}:{t2}"),
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
    token = query.data[len("ndkeep:"):]
    try:
        t1, t2 = token.split(":", 1)
        fuid1, col1 = t1.split(":", 1)
        fuid2, col2 = t2.split(":", 1)
    except ValueError:
        await query.edit_message_text("⚠️ Invalid action.")
        return

    a, b = sorted([fuid1, fuid2])

    def _insert(conn):
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO neardup_ignored (collection, fuid_a, fuid_b) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (col1, a, b),
            )
    try:
        await db_run(_insert)
    except Exception as e:
        await reply_db_error(update, "save keep-both decision", e)
        return

    await query.edit_message_text("👍 Kept both — this pair won't be shown again.")

async def neardup_close_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    token = query.data[len("ndclose:"):]
    _neardup_sessions.pop(token, None)
    await query.edit_message_text("✅ Near-duplicates review closed.")

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
# Unused - kept from original for behavior parity (not wired to any handler)
# ----------------------------------------------------------------------
async def _filter_filter(update: Update) -> bool:
    chat_id = update.effective_chat.id
    min_len = min_video_length.get(chat_id)
    if min_len is None:
        return True
    video = update.message.video
    if video and video.duration is not None:
        return video.duration >= min_len
    return True
