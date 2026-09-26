import asyncio
import io
import json
import logging
from datetime import datetime
from typing import Optional, List

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaVideo,
    Message,
    Update,
)
from telegram.error import TelegramError
from telegram.ext import ContextTypes

import db
import utils

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Access Control
# ----------------------------------------------------------------------
async def access_control(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if utils.ADMIN_USER_ID and update.effective_user:
        if update.effective_user.id != utils.ADMIN_USER_ID:
            if update.effective_message:
                await update.effective_message.reply_text("⛔ Unauthorized access.")
            elif update.callback_query:
                await update.callback_query.answer("⛔ Unauthorized.", show_alert=True)
            return False
    return True


async def admin_check(update: Update) -> bool:
    if utils.ADMIN_USER_ID and update.effective_user and update.effective_user.id != utils.ADMIN_USER_ID:
        if update.effective_message:
            await update.effective_message.reply_text("⛔ Admin rights required.")
        return False
    return True


# ----------------------------------------------------------------------
# Video Processing & Activity Summaries
# ----------------------------------------------------------------------
async def _flush_save_summary(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    try:
        await asyncio.sleep(utils.SAVE_SUMMARY_DEBOUNCE_SECONDS)
    except asyncio.CancelledError:
        return
    stats = utils._save_counts.pop(chat_id, None)
    utils._save_notify_tasks.pop(chat_id, None)
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
    stats = utils._save_counts.setdefault(chat_id, {"saved": 0, "skipped": 0, "removed": 0, "cols": set()})
    stats[kind] += 1
    stats["cols"].add(collection)

    existing = utils._save_notify_tasks.get(chat_id)
    if existing and not existing.done():
        existing.cancel()
    utils._save_notify_tasks[chat_id] = asyncio.create_task(_flush_save_summary(chat_id, context))


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in utils.paused_chats:
        return

    video = update.message.video
    if not video:
        return

    min_len = utils.min_video_length.get(chat_id)
    if min_len is not None and video.duration is not None and video.duration < min_len:
        return

    collections = utils.get_active_collections(chat_id)
    is_remove = chat_id in utils.removing_chats

    for col in collections:
        if is_remove:
            removed = await db.delete_video_from_collection(col, video.file_unique_id)
            _record_activity(chat_id, col, "removed" if removed else "skipped", context)
        else:
            saved = await db.save_video_to_db(col, video.file_id, video.file_unique_id, video.duration, video.file_size, getattr(video, "file_name", None))
            _record_activity(chat_id, col, "saved" if saved else "skipped", context)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in utils.paused_chats:
        return

    msg = update.message
    if not utils.is_video_document(msg):
        return

    doc = msg.document
    collections = utils.get_active_collections(chat_id)
    is_remove = chat_id in utils.removing_chats

    for col in collections:
        if is_remove:
            removed = await db.delete_video_from_collection(col, doc.file_unique_id)
            _record_activity(chat_id, col, "removed" if removed else "skipped", context)
        else:
            saved = await db.save_video_to_db(col, doc.file_id, doc.file_unique_id, None, doc.file_size, doc.file_name)
            _record_activity(chat_id, col, "saved" if saved else "skipped", context)


async def handle_non_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pass


async def _send_single_video_with_fallback(chat_id: int, file_id: str, file_unique_id: str, caption: str, context: ContextTypes.DEFAULT_TYPE, collection: str) -> Optional[Message]:
    try:
        return await context.bot.send_video(chat_id=chat_id, video=file_id, caption=caption)
    except TelegramError as e:
        err_str = str(e).lower()
        if "wrong remote file identifier" in err_str or "file reference" in err_str or "not found" in err_str:
            def _mark_dead(conn):
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO dead_files (file_unique_id) VALUES (%s) ON CONFLICT DO NOTHING", (file_unique_id,))
            await db.db_run(_mark_dead)
        return None


# ----------------------------------------------------------------------
# Standard Commands
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
        "🔍 *Search & Utilities*\n"
        "• /search <query> - Search by filename\n"
        "• /find - Find by size/duration\n"
        "• /dups <name> - Find exact duplicates\n"
        "• /neardupes <name> - Visual near-duplicate cleanup\n"
        "• /removemode on|off - Toggle auto-delete mode\n"
        "• /minlength <sec> - Filter short videos"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")


# ----------------------------------------------------------------------
# Menu Navigation
# ----------------------------------------------------------------------
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
        folders = await db.db_run(_get_folders)
    except Exception:
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
    if query.data == "menu_settings":
        await settings_command(update, context)


async def menu_folder_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    folder_prefix = query.data[len("menufolder:"):]

    try:
        def _get_sub_items(conn):
            with conn.cursor() as cur:
                clause, params = utils.under_clause(folder_prefix)
                cur.execute(f"SELECT DISTINCT collection FROM videos WHERE {clause}", params)
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
        subfolders, exact_match = await db.db_run(_get_sub_items)
    except Exception as e:
        await utils.reply_db_error(update, f"fetch items for '{folder_prefix}'", e)
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

    await query.edit_message_text(f"📁 Folder: `{folder_prefix}`", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


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

    await query.edit_message_text(f"📁 Collection: `{name}`\nSelect an action:", reply_markup=keyboard, parse_mode="Markdown")


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
    utils.active_collections[chat_id] = [name]
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
# List Collections
# ----------------------------------------------------------------------
async def list_collections(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        def _fetch(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT collection FROM videos ORDER BY collection")
                return [r[0] for r in cur.fetchall()]
        cols = await db.db_run(_fetch)
    except Exception as e:
        await utils.reply_db_error(update, "list collections", e)
        return

    if not cols:
        await update.message.reply_text("No collections found.")
        return

    top_folders = sorted(list({c.split("/")[0] for c in cols}))
    folder_buttons = [InlineKeyboardButton(f"📁 {f}", callback_data=f"listfolder:{f}") for f in top_folders]
    keyboard = [folder_buttons[i:i + 2] for i in range(0, len(folder_buttons), 2)]

    await update.message.reply_text("📁 *Collections Hierarchy*\nSelect a folder to inspect:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def list_folder_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    folder = query.data[len("listfolder:"):]

    try:
        def _fetch_sub(conn):
            with conn.cursor() as cur:
                clause, params = utils.under_clause(folder)
                cur.execute(f"SELECT DISTINCT collection FROM videos WHERE {clause}", params)
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
        subs, exact = await db.db_run(_fetch_sub)
    except Exception as e:
        await utils.reply_db_error(update, "expand folder", e)
        return

    keyboard = []
    if exact:
        keyboard.append([InlineKeyboardButton("📄 View Action Menu", callback_data=f"listchoice:{folder}")])

    folder_buttons = [InlineKeyboardButton(f"📁 {s}", callback_data=f"listfolder:{s}") for s in subs]
    for i in range(0, len(folder_buttons), 2):
        keyboard.append(folder_buttons[i:i + 2])

    keyboard.append([
        InlineKeyboardButton("📂 Set Active", callback_data=f"listset:{folder}"),
        InlineKeyboardButton("📩 Get Videos", callback_data=f"listget:{folder}"),
    ])
    keyboard.append([
        InlineKeyboardButton("🎲 Random", callback_data=f"listrandom:{folder}"),
        InlineKeyboardButton("🗑️ Delete", callback_data=f"listdelete:{folder}"),
    ])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu_back")])

    await query.edit_message_text(f"📁 Location: `{folder}`", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def list_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("listchoice:"):]

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📂 Set Active", callback_data=f"listset:{name}"),
            InlineKeyboardButton("📩 Get Videos", callback_data=f"listget:{name}"),
            InlineKeyboardButton("🎲 Random", callback_data=f"listrandom:{name}"),
        ],
        [
            InlineKeyboardButton("🗑️ Delete Collection", callback_data=f"listdelete:{name}"),
        ],
        [
            InlineKeyboardButton("⬅️ Back to menu", callback_data="menu_back"),
        ],
    ])
    await query.edit_message_text(f"'{name}' — what would you like to do?", reply_markup=keyboard)


async def list_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("listdelete:"):]

    try:
        def _count(conn):
            with conn.cursor() as cur:
                clause, params = utils.under_clause(name)
                cur.execute(f"SELECT COUNT(*) FROM videos WHERE {clause}", params)
                return cur.fetchone()[0]
        total = await db.db_run(_count)
    except Exception as e:
        await utils.reply_db_error(update, f"check '{name}'", e)
        return

    keyboard = [[
        InlineKeyboardButton("❌ Yes, Delete", callback_data=f"confirmdelete:{name}"),
        InlineKeyboardButton("⬅️ Cancel", callback_data=f"listfolder:{name}"),
    ]]

    await query.edit_message_text(
        f"⚠️ Are you sure you want to delete **{name}**?\nThis will delete **{total}** item(s).",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


async def list_set_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data[len("listset:"):]
    chat_id = update.effective_chat.id
    utils.active_collections[chat_id] = [name]
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


async def list_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pass


# ----------------------------------------------------------------------
# Get Videos & Pagination
# ----------------------------------------------------------------------
async def get_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    name = utils.normalize_name(" ".join(context.args)) if context.args else utils.get_active_collections(chat_id)[0]

    try:
        def _count_videos(conn):
            with conn.cursor() as cur:
                clause, params = utils.under_clause(name)
                cur.execute(f"SELECT COUNT(*) FROM videos WHERE {clause}", params)
                return cur.fetchone()[0]
        total = await db.db_run(_count_videos)
    except Exception as e:
        await utils.reply_db_error(update, f"get '{name}'", e)
        return

    if total == 0:
        msg = f"No videos found in `{name}`."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg, parse_mode="Markdown")
        else:
            await update.message.reply_text(msg, parse_mode="Markdown")
        return

    utils._get_sessions[chat_id] = {
        "name": name,
        "page": 1,
        "total": total,
        "auto_send": True,
        "control_msg_id": None,
    }

    await _send_or_update_pagination(chat_id, context, send_videos_immediately=True)


async def _fetch_page_videos(name: str, page: int, limit: int = utils.GET_BATCH_SIZE):
    offset = (page - 1) * limit
    def _fetch(conn):
        with conn.cursor() as cur:
            clause, params = utils.under_clause(name)
            query = f"SELECT file_id, file_unique_id FROM videos WHERE {clause} ORDER BY added_at LIMIT %s OFFSET %s"
            cur.execute(query, params + (limit, offset))
            return cur.fetchall()
    return await db.db_run(_fetch)


async def _send_or_update_pagination(chat_id: int, context: ContextTypes.DEFAULT_TYPE, send_videos_immediately: bool = False):
    session = utils._get_sessions.get(chat_id)
    if not session:
        return

    name = session["name"]
    total = session["total"]
    total_pages = (total + utils.GET_BATCH_SIZE - 1) // utils.GET_BATCH_SIZE
    page = min(max(1, session["page"]), total_pages)
    session["page"] = page

    if send_videos_immediately:
        rows = await _fetch_page_videos(name, page, utils.GET_BATCH_SIZE)
        if rows:
            media_group = [InputMediaVideo(media=r[0]) for r in rows]
            try:
                await context.bot.send_media_group(chat_id=chat_id, media=media_group)
                await asyncio.sleep(1.0)
            except TelegramError as e:
                logger.warning(f"Album send failed, falling back to individual send: {e}")
                for fid, fuid in rows:
                    await _send_single_video_with_fallback(chat_id, fid, fuid, "", context, name)
                    await asyncio.sleep(0.4)

    start_idx = (page - 1) * utils.GET_BATCH_SIZE + 1
    end_idx = min(page * utils.GET_BATCH_SIZE, total)
    text = f"📦 *Collection:* `{name}`\n📄 *Page {page}/{total_pages}* (Videos {start_idx}-{end_idx} of {total})"

    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"getpage:{page-1}"))
    nav_row.append(InlineKeyboardButton(f"🔄 Resend ({end_idx-start_idx+1})", callback_data=f"getsend:{page}"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"getpage:{page+1}"))

    jump_row = [
        InlineKeyboardButton("⏪ -10", callback_data=f"getpage:{max(1, page-10)}"),
        InlineKeyboardButton("🔢 Jump", callback_data="getjump:prompt"),
        InlineKeyboardButton("+10 ⏩", callback_data=f"getpage:{min(total_pages, page+10)}"),
    ]

    keyboard = InlineKeyboardMarkup([nav_row, jump_row, [InlineKeyboardButton("🛑 Close Menu", callback_data="getcancel")]])

    if session.get("control_msg_id"):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=session["control_msg_id"])
        except TelegramError:
            pass

    new_msg = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard, parse_mode="Markdown")
    session["control_msg_id"] = new_msg.message_id


async def get_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    target_page = int(query.data.split(":")[1])

    session = utils._get_sessions.get(chat_id)
    if session:
        session["page"] = target_page
        await _send_or_update_pagination(chat_id, context, send_videos_immediately=True)


async def get_jump_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id

    session = utils._get_sessions.get(chat_id)
    if not session:
        return

    total_pages = (session["total"] + utils.GET_BATCH_SIZE - 1) // utils.GET_BATCH_SIZE
    context.user_data["awaiting_page_jump"] = True

    await context.bot.send_message(chat_id=chat_id, text=f"🔢 *Jump to Page*\nPlease reply with a page number between `1` and `{total_pages}`:", parse_mode="Markdown")


async def handle_page_jump_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_page_jump"):
        return

    chat_id = update.effective_chat.id
    session = utils._get_sessions.get(chat_id)
    if not session:
        context.user_data["awaiting_page_jump"] = False
        return

    text = update.message.text.strip()
    total_pages = (session["total"] + utils.GET_BATCH_SIZE - 1) // utils.GET_BATCH_SIZE

    if text.isdigit():
        target = int(text)
        if 1 <= target <= total_pages:
            context.user_data["awaiting_page_jump"] = False
            session["page"] = target
            await _send_or_update_pagination(chat_id, context, send_videos_immediately=True)
            return

    await update.message.reply_text(f"⚠️ Invalid page number. Enter a number from 1 to {total_pages}:")


async def get_send_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await _send_or_update_pagination(update.effective_chat.id, context, send_videos_immediately=True)


async def get_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id

    session = utils._get_sessions.pop(chat_id, None)
    if session and session.get("control_msg_id"):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=session["control_msg_id"])
        except TelegramError:
            pass


async def get_by_size(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pass


async def random_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    name = utils.normalize_name(" ".join(context.args)) if context.args else utils.get_active_collections(chat_id)[0]

    try:
        def _fetch_rand(conn):
            with conn.cursor() as cur:
                clause, params = utils.under_clause(name)
                cur.execute(f"SELECT file_id, file_unique_id FROM videos WHERE {clause} ORDER BY RANDOM() LIMIT 1", params)
                return cur.fetchone()
        res = await db.db_run(_fetch_rand)
    except Exception as e:
        await utils.reply_db_error(update, "fetch random video", e)
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


async def random_next_recursive_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pass


async def search_videos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /search <query>")
        return
    query_str = " ".join(context.args)

    try:
        def _search(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT collection, file_id, file_name FROM videos WHERE file_name ILIKE %s LIMIT 20", (f"%{query_str}%",))
                return cur.fetchall()
        rows = await db.db_run(_search)
    except Exception as e:
        await utils.reply_db_error(update, "search videos", e)
        return

    if not rows:
        await update.message.reply_text(f"No videos matching '{query_str}'.")
        return

    lines = [f"🔍 *Search results for:* `{query_str}`"]
    for col, fid, fname in rows:
        lines.append(f"• `{col}`: {fname or 'Unnamed'}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def find_videos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Usage: Use /search <query> or specify parameters.")


async def find_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pass


async def find_video_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pass


async def find_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pass


async def find_close_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pass


async def retry_failed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Retrying failed sends...")


# ----------------------------------------------------------------------
# Settings & Utility Controls
# ----------------------------------------------------------------------
async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    collections = utils.get_active_collections(chat_id)
    rem = "ON" if chat_id in utils.removing_chats else "OFF"
    paused = "YES" if chat_id in utils.paused_chats else "NO"
    min_l = utils.min_video_length.get(chat_id, "OFF")

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
        if chat_id in utils.removing_chats:
            utils.removing_chats.discard(chat_id)
        else:
            utils.removing_chats.add(chat_id)
    elif action == "toggle_pause":
        if chat_id in utils.paused_chats:
            utils.paused_chats.discard(chat_id)
        else:
            utils.paused_chats.add(chat_id)

    await settings_command(update, context)


async def collect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        cols = utils.get_active_collections(chat_id)
        await update.message.reply_text(f"📁 Active collection: `{', '.join(cols)}`", parse_mode="Markdown")
        return

    name = utils.normalize_name(" ".join(context.args))
    err = utils.validate_collection_path(name)
    if err:
        await update.message.reply_text(f"⚠️ {utils.describe_path_error(err)}")
        return

    utils.active_collections[chat_id] = [name]
    await update.message.reply_text(f"✅ Active collection set to: `{name}`", parse_mode="Markdown")


async def fav_shortcut(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    utils.active_collections[chat_id] = ["favorites"]
    await update.message.reply_text("⭐ Active collection set to: `favorites`", parse_mode="Markdown")


async def current(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    names = utils.get_active_collections(chat_id)
    suffix = ""
    if chat_id in utils.removing_chats:
        suffix = " (🗑️ REMOVE MODE ON)"
    elif chat_id in utils.paused_chats:
        suffix = " (⏸️ PAUSED)"

    if len(names) == 1:
        await update.message.reply_text(f"📁 Active collection: {names[0]}{suffix}")
    else:
        await update.message.reply_text(f"📁 Active collections: {', '.join(names)}{suffix}")


async def finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    utils.active_collections[chat_id] = [utils.DEFAULT_COLLECTION]
    utils.paused_chats.discard(chat_id)
    utils.removing_chats.discard(chat_id)
    await update.message.reply_text(f"✅ Reset active collection to: {utils.DEFAULT_COLLECTION}")


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    task = utils._active_tasks.get(chat_id)
    if task and not task.done():
        task.cancel()
    utils.paused_chats.add(chat_id)
    await update.message.reply_text("🛑 Cancelled active processes and paused saving.")


async def minlength(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        curr = utils.min_video_length.get(chat_id)
        msg = f"⏱️ Current min length filter: {curr} seconds" if curr else "⏱️ Min length filter is currently OFF."
        await update.message.reply_text(f"{msg}\nUsage: /minlength <seconds> or /minlength off")
        return

    val = context.args[0].lower()
    if val in ("off", "0", "disable", "none"):
        utils.min_video_length.pop(chat_id, None)
        await update.message.reply_text("⏱️ Min length filter turned OFF.")
    elif val.isdigit():
        secs = int(val)
        if secs < 0:
            await update.message.reply_text("⚠️ Duration cannot be negative.")
            return
        utils.min_video_length[chat_id] = secs
        await update.message.reply_text(f"⏱️ Min length set to {secs} seconds. Shorter videos will be skipped.")
    else:
        await update.message.reply_text("⚠️ Please provide a number in seconds or 'off'.")


async def removemode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        status = "ON" if chat_id in utils.removing_chats else "OFF"
        await update.message.reply_text(f"🗑️ Remove mode is currently {status}.\nUsage: /removemode on|off")
        return

    val = context.args[0].lower()
    if val in ("on", "1", "enable", "true"):
        utils.removing_chats.add(chat_id)
        await update.message.reply_text("🗑️ Remove mode ON. Forwarded videos will be deleted from active collection(s).")
    elif val in ("off", "0", "disable", "false"):
        utils.removing_chats.discard(chat_id)
        await update.message.reply_text("🗑️ Remove mode OFF. Videos will be saved normally.")
    else:
        await update.message.reply_text("⚠️ Please specify 'on' or 'off'.")


async def remove_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    reply = update.message.reply_to_message
    if not reply:
        await update.message.reply_text("⚠️ Reply to a video message with /remove to delete it.")
        return

    file_unique_id = None
    if reply.video:
        file_unique_id = reply.video.file_unique_id
    elif reply.document and utils.is_video_document(reply):
        file_unique_id = reply.document.file_unique_id

    if not file_unique_id:
        await update.message.reply_text("⚠️ The replied message doesn't contain a valid video.")
        return

    collections = utils.get_active_collections(chat_id)
    deleted_from = []
    for col in collections:
        if await db.delete_video_from_collection(col, file_unique_id):
            deleted_from.append(col)

    if deleted_from:
        await update.message.reply_text(f"🗑️ Deleted video from: {', '.join(deleted_from)}")
    else:
        await update.message.reply_text("⚠️ Video was not found in active collection(s).")


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    collections = utils.get_active_collections(chat_id)

    try:
        def _query(conn):
            with conn.cursor() as cur:
                counts = {}
                for col in collections:
                    cur.execute("SELECT COUNT(*) FROM videos WHERE collection = %s", (col,))
                    counts[col] = cur.fetchone()[0]
                return counts
        counts = await db.db_run(_query)
    except Exception as e:
        await utils.reply_db_error(update, "fetch status", e)
        return

    lines = [f"📊 *Status* (Active: {', '.join(collections)})"]
    for col, count in counts.items():
        lines.append(f"• `{col}`: {count} video(s)")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def count_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /count <collection>")
        return
    name = utils.normalize_name(" ".join(context.args))

    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM videos WHERE collection = %s", (name,))
                return cur.fetchone()[0]
        count = await db.db_run(_query)
        await update.message.reply_text(f"📊 Collection `{name}` contains {count} video(s).", parse_mode="Markdown")
    except Exception as e:
        await utils.reply_db_error(update, f"count '{name}'", e)


async def collection_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /info <collection>")
        return
    name = utils.normalize_name(" ".join(context.args))

    try:
        def _query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*), SUM(file_size), AVG(duration), MIN(added_at), MAX(added_at) FROM videos WHERE collection = %s",
                    (name,),
                )
                return cur.fetchone()
        count, total_size, avg_dur, first_added, last_added = await db.db_run(_query)
    except Exception as e:
        await utils.reply_db_error(update, f"fetch info for '{name}'", e)
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
    name = utils.normalize_name(" ".join(context.args[:-1]))

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
        await db.db_run(_set)
        if days > 0:
            await update.message.reply_text(f"⏰ Set auto-expiry for `{name}` to {days} days.", parse_mode="Markdown")
        else:
            await update.message.reply_text(f"⏰ Disabled auto-expiry for `{name}`.", parse_mode="Markdown")
    except Exception as e:
        await utils.reply_db_error(update, "set expiry", e)


# ----------------------------------------------------------------------
# Batch & Structure Operations (Delete, Rename, Move, Copy, Merge, Dups)
# ----------------------------------------------------------------------
async def delete_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /delete <name>")
        return
    name = utils.normalize_name(" ".join(context.args))

    try:
        def _count(conn):
            with conn.cursor() as cur:
                clause, params = utils.under_clause(name)
                cur.execute(f"SELECT COUNT(*) FROM videos WHERE {clause}", params)
                return cur.fetchone()[0]
        total = await db.db_run(_count)
    except Exception as e:
        await utils.reply_db_error(update, f"check '{name}'", e)
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
                clause, params = utils.under_clause(name)
                cur.execute(f"DELETE FROM videos WHERE {clause}", params)
                return cur.rowcount

        count = await db.db_run(_delete)
    except Exception as e:
        await utils.reply_db_error(update, f"delete '{name}'", e)
        return

    await query.edit_message_text(f"✅ Deleted folder `{name}` ({count} items deleted).")


async def cancel_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("❎ Deletion cancelled.")


async def rename_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parsed = utils.parse_arrow_pair(context.args or [])
    if not parsed:
        await update.message.reply_text(
            "Usage: /rename <old> -> <new>\n"
            "Examples:\n"
            "  /rename movies -> films\n"
            "  /rename movies/action -> movies/classic-action"
        )
        return
    src, dest = parsed
    err = utils.validate_collection_path(dest)
    if err:
        await update.message.reply_text(f"⚠️ Destination path is invalid: {utils.describe_path_error(err)}")
        return

    try:
        def _rename(conn):
            with conn.cursor() as cur:
                clause, params = utils.under_clause(src)
                cur.execute(f"SELECT DISTINCT collection FROM videos WHERE {clause}", params)
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
                    err_col = utils.validate_collection_path(new_col)
                    if err_col:
                        raise ValueError(f"Resulting path '{new_col}' is invalid: {utils.describe_path_error(err_col)}")

                    cur.execute("SELECT file_id, file_unique_id, duration, file_size, file_name FROM videos WHERE collection = %s", (old,))
                    rows = cur.fetchall()
                    for fid, fuid, dur, sz, fn in rows:
                        cur.execute("SELECT 1 FROM videos WHERE collection = %s AND file_unique_id = %s", (new_col, fuid))
                        if cur.fetchone():
                            cur.execute("DELETE FROM videos WHERE collection = %s AND file_unique_id = %s", (old, fuid))
                        else:
                            cur.execute(
                                "UPDATE videos SET collection = %s WHERE collection = %s AND file_unique_id = %s",
                                (new_col, old, fuid),
                            )
                        renamed_count += 1
                    cur.execute("UPDATE sent_videos SET collection = %s WHERE collection = %s", (new_col, old))
                    cur.execute("UPDATE collection_settings SET collection = %s WHERE collection = %s", (new_col, old))
                return renamed_count

        count = await db.db_run(_rename)
        if count == 0:
            await update.message.reply_text(f"No collection or folder found matching '{src}'.")
        else:
            await update.message.reply_text(f"✏️ Renamed `{src}` -> `{dest}` ({count} video(s) updated).", parse_mode="Markdown")
    except ValueError as ve:
        await update.message.reply_text(f"⚠️ {ve}")
    except Exception as e:
        await utils.reply_db_error(update, f"rename '{src}'", e)


async def move_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parsed = utils.parse_arrow_pair(context.args or [])
    if not parsed:
        await update.message.reply_text("Usage: /move <src> -> <dest>")
        return
    src, dest = parsed
    err = utils.validate_collection_path(dest)
    if err:
        await update.message.reply_text(f"⚠️ Destination path invalid: {utils.describe_path_error(err)}")
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
                    cur.execute("SELECT 1 FROM videos WHERE collection = %s AND file_unique_id = %s", (dest, fuid))
                    if cur.fetchone():
                        cur.execute("DELETE FROM videos WHERE collection = %s AND file_unique_id = %s", (src, fuid))
                    else:
                        cur.execute("UPDATE videos SET collection = %s WHERE collection = %s AND file_unique_id = %s", (dest, src, fuid))
                    moved += 1
                return moved
        count = await db.db_run(_move)
        if count == 0:
            await update.message.reply_text(f"No videos found in '{src}'.")
        else:
            await update.message.reply_text(f"📦 Moved {count} video(s) from `{src}` -> `{dest}`.", parse_mode="Markdown")
    except Exception as e:
        await utils.reply_db_error(update, "move videos", e)


async def copy_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parsed = utils.parse_arrow_pair(context.args or [])
    if not parsed:
        await update.message.reply_text("Usage: /copy <src> -> <dest>")
        return
    src, dest = parsed
    err = utils.validate_collection_path(dest)
    if err:
        await update.message.reply_text(f"⚠️ Destination path invalid: {utils.describe_path_error(err)}")
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
        count = await db.db_run(_copy)
        await update.message.reply_text(f"📋 Copied {count} video(s) from `{src}` to `{dest}`.", parse_mode="Markdown")
    except Exception as e:
        await utils.reply_db_error(update, "copy videos", e)


async def merge_collections(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parsed = utils.parse_arrow_pair(context.args or [])
    if not parsed:
        await update.message.reply_text("Usage: /merge <source> -> <target>")
        return
    src, dest = parsed
    err = utils.validate_collection_path(dest)
    if err:
        await update.message.reply_text(f"⚠️ Destination path invalid: {utils.describe_path_error(err)}")
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
        count = await db.db_run(_merge)
        await update.message.reply_text(f"🔀 Merged `{src}` into `{dest}` ({count} video(s) total in destination).", parse_mode="Markdown")
    except Exception as e:
        await utils.reply_db_error(update, "merge collections", e)


async def dups_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /dups <collection>")
        return
    name = utils.normalize_name(" ".join(context.args))

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
        dups = await db.db_run(_find_dups)
        if not dups:
            await update.message.reply_text(f"✅ No duplicates found in `{name}`.", parse_mode="Markdown")
        else:
            await update.message.reply_text(f"⚠️ Found {len(dups)} duplicate video ID(s) in `{name}`.", parse_mode="Markdown")
    except Exception as e:
        await utils.reply_db_error(update, f"check duplicates in '{name}'", e)


# ----------------------------------------------------------------------
# Near Duplicates Operations
# ----------------------------------------------------------------------
async def near_duplicates_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /neardupes <collection>")
        return
    name = utils.normalize_name(" ".join(context.args))

    try:
        pairs = await db.db_run(lambda: db.fetch_near_duplicates(name))
    except Exception as e:
        await utils.reply_db_error(update, f"find near dupes in '{name}'", e)
        return

    if not pairs:
        await update.message.reply_text(f"✅ No possible near-duplicates found in `{name}`.", parse_mode="Markdown")
        return

    token = f"{chat_id}:{name}"
    utils._neardup_sessions[token] = (pairs, name, chat_id)
    await _show_neardup_page(chat_id, token, 1, context, edit_msg=None)


async def _show_neardup_page(chat_id: int, token: str, page: int, context: ContextTypes.DEFAULT_TYPE, edit_msg: Optional[Message] = None):
    session = utils._neardup_sessions.get(token)
    if not session:
        msg = "⏱️ Near-dupe session expired. Run `/neardupes <collection>` again."
        if edit_msg:
            await edit_msg.edit_text(msg, parse_mode="Markdown")
        else:
            await context.bot.send_message(chat_id, msg, parse_mode="Markdown")
        return

    pairs, collection, _ = session
    total_pairs = len(pairs)
    total_pages = (total_pairs + utils.NEARDUPES_PAIRS_PER_PAGE - 1) // utils.NEARDUPES_PAIRS_PER_PAGE
    page = min(max(1, page), total_pages)
    start_idx = (page - 1) * utils.NEARDUPES_PAIRS_PER_PAGE
    page_pairs = pairs[start_idx:start_idx + utils.NEARDUPES_PAIRS_PER_PAGE]

    status_text = f"🔎 *Near-duplicates in* `{collection}` — Page {page}/{total_pages} ({total_pairs} pair(s) total)\nSending side-by-side videos..."
    if edit_msg:
        await edit_msg.edit_text(status_text, parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id, status_text, parse_mode="Markdown")

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
        target_msg = msg_b or msg_a
        if target_msg:
            try:
                await target_msg.reply_text("Choose action for Pair #" + str(pair_idx) + ":", reply_markup=kb)
            except TelegramError:
                await context.bot.send_message(chat_id, "Choose action for Pair #" + str(pair_idx) + ":", reply_markup=kb)

        await asyncio.sleep(utils.NEARDUP_ALBUM_DELAY)

    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️ Prev Page", callback_data=f"ndpage:{token}:{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Next Page ▶️", callback_data=f"ndpage:{token}:{page+1}"))

    rows_kb = []
    if nav_buttons:
        rows_kb.append(nav_buttons)
    rows_kb.append([InlineKeyboardButton("Done / Close", callback_data=f"ndclose:{token}")])

    await context.bot.send_message(chat_id, f"✅ Finished page {page}/{total_pages}.", reply_markup=InlineKeyboardMarkup(rows_kb))


async def neardup_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":")
    token = f"{parts[1]}:{parts[2]}"
    page = int(parts[3])
    await _show_neardup_page(update.effective_chat.id, token, page, context, edit_msg=query.message)


async def neardup_del_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    token = query.data[len("nddel:"):]
    try:
        fuid, collection = token.split(":", 1)
    except ValueError:
        await query.edit_message_text("⚠️ Invalid action.")
        return

    ok = await db.delete_video_from_collection(collection, fuid)
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

    ok1 = await db.delete_video_from_collection(col1, fuid1)
    ok2 = await db.delete_video_from_collection(col2, fuid2)
    msgs = [
        f"Deleted Video A from `{col1}`" if ok1 else f"Video A not found in `{col1}`",
        f"Deleted Video B from `{col2}`" if ok2 else f"Video B not found in `{col2}`",
    ]
    await query.edit_message_text("🗑️ " + " | ".join(msgs), parse_mode="Markdown")


async def neardup_keep_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("👍 Kept both videos.")


async def neardup_close_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    token = query.data[len("ndclose:"):]
    utils._neardup_sessions.pop(token, None)
    await query.edit_message_text("✅ Near-duplicates review closed.")


# ----------------------------------------------------------------------
# Export & Import & Cleanup
# ----------------------------------------------------------------------
async def export_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /export <collection>")
        return
    name = utils.normalize_name(" ".join(context.args))

    try:
        def _fetch(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT file_id FROM videos WHERE collection = %s ORDER BY added_at", (name,))
                return [r[0] for r in cur.fetchall()]
        file_ids = await db.db_run(_fetch)
    except Exception as e:
        await utils.reply_db_error(update, f"export '{name}'", e)
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
    name = utils.normalize_name(" ".join(context.args))

    try:
        def _fetch(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT file_id, file_unique_id, duration, file_size, file_name, added_at FROM videos WHERE collection = %s ORDER BY added_at",
                    (name,),
                )
                return cur.fetchall()
        rows = await db.db_run(_fetch)
    except Exception as e:
        await utils.reply_db_error(update, f"export json '{name}'", e)
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

    await update.message.reply_document(document=bio, caption=f"📋 Exported JSON for `{name}` ({len(rows)} videos).", parse_mode="Markdown")


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

        await db.db_run(_import)
        await update.message.reply_text(
            f"📥 Imported JSON into `{collection}`:\n• Imported: {imported}\n• Skipped (duplicates): {skipped}",
            parse_mode="Markdown",
        )
    except Exception as e:
        await utils.reply_db_error(update, "import JSON", e)


async def cleanup_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /cleanup <collection>")
        return
    name = utils.normalize_name(" ".join(context.args))

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
        removed = await db.db_run(_cleanup)
        await update.message.reply_text(f"🧹 Cleaned up `{name}`. Removed {removed} dead file reference(s).", parse_mode="Markdown")
    except Exception as e:
        await utils.reply_db_error(update, f"cleanup '{name}'", e)


async def cleanupnow_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pass


async def backup_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_check(update):
        return

    try:
        def _dump(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT collection, file_id, file_unique_id, duration, file_size, file_name, added_at FROM videos")
                return cur.fetchall()
        rows = await db.db_run(_dump)
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

        await update.message.reply_document(document=bio, caption=f"💾 Full database backup ({len(rows)} records).")
    except Exception as e:
        await utils.reply_db_error(update, "backup database", e)
