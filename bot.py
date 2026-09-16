#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Complete bot.py with improved pagination and Postgres-backed file_id fetching.

Features:
- Persistent navigation message with inline keyboard
- Auto-send albums (media_group) on Prev/Next
- Jump-to-page via /goto and inline Jump prompt
- Prefetch next pages into in-memory cache
- Uses Telegram file_id values stored in Postgres (no re-upload)
- Minimal in-memory state; recommended to replace with Redis for production

Before running:
- Install dependencies: pip install python-telegram-bot==13.XX psycopg2-binary
- Set environment variables:
    BOT_TOKEN or replace TOKEN below
    DATABASE_URL (preferred) or PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
- Backup your original bot.py before replacing.
"""

import os
import logging
import asyncio
import math
import re
from typing import Dict, Any, List, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from telegram import (
    Bot,
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaVideo,
)
from telegram.ext import (
    Updater,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    Filters,
    CallbackContext,
)

# -----------------------
# Configuration
# -----------------------
TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
PAGE_SIZE = 10  # Telegram media_group max is 10
PREFETCH_COUNT = 2
SEND_DELAY_SECONDS = 1.0
PREFETCH_DELAY_SECONDS = 0.2
TESTING = False

# -----------------------
# Logging
# -----------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# -----------------------
# In-memory pagination state
# -----------------------
# Key: (chat_id, folder_id) -> value: dict with keys:
#   page: int
#   page_size: int
#   total: int
#   message_id: int
#   cache: {page: [file_id, ...]}
#   awaiting_jump: bool
PAGINATION_STATE: Dict[Tuple[int, str], Dict[str, Any]] = {}


# -----------------------
# Helper functions
# -----------------------
def get_state(chat_id: int, folder_id: str) -> Dict[str, Any]:
    key = (chat_id, folder_id)
    if key not in PAGINATION_STATE:
        PAGINATION_STATE[key] = {
            "page": 1,
            "page_size": PAGE_SIZE,
            "total": 0,
            "message_id": None,
            "cache": {},
            "awaiting_jump": False,
        }
    return PAGINATION_STATE[key]


def compute_total_pages(total_items: int, page_size: int) -> int:
    if page_size <= 0:
        return 1
    return max(1, math.ceil(total_items / page_size))


def build_nav_keyboard(folder_id: str, page: int, total_pages: int) -> InlineKeyboardMarkup:
    kb = []
    kb.append(
        [
            InlineKeyboardButton("Prev", callback_data=f"nav:{folder_id}:prev"),
            InlineKeyboardButton(f"Page {page}/{total_pages}", callback_data="noop"),
            InlineKeyboardButton("Next", callback_data=f"nav:{folder_id}:next"),
        ]
    )
    kb.append(
        [
            InlineKeyboardButton(f"Send Page ({PAGE_SIZE})", callback_data=f"send:{folder_id}:page:{page}"),
            InlineKeyboardButton("Jump", callback_data=f"jump:{folder_id}"),
            InlineKeyboardButton("Cancel", callback_data=f"cancel:{folder_id}"),
        ]
    )
    return InlineKeyboardMarkup(kb)


# -----------------------
# Database / storage access
# -----------------------
def fetch_total_items_for_folder(folder_id: str) -> int:
    """
    Fetch total count of items for a folder from Postgres.
    Adjust table/column names if your schema differs.
    """
    database_url = os.environ.get("DATABASE_URL")
    conn = None
    try:
        if database_url:
            conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
        else:
            conn = psycopg2.connect(
                host=os.environ.get("PGHOST", "localhost"),
                port=int(os.environ.get("PGPORT", 5432)),
                user=os.environ.get("PGUSER", "postgres"),
                password=os.environ.get("PGPASSWORD", ""),
                dbname=os.environ.get("PGDATABASE", "postgres"),
                cursor_factory=RealDictCursor,
            )
        with conn.cursor() as cur:
            # Adjust table/column names if your schema differs.
            sql = """
                SELECT COUNT(*) AS cnt
                FROM media
                WHERE folder_id = %s
            """
            cur.execute(sql, (folder_id,))
            row = cur.fetchone()
            return int(row["cnt"]) if row and row.get("cnt") is not None else 0
    except Exception as e:
        logger.exception("DB count failed for folder %s: %s", folder_id, e)
        return 0
    finally:
        if conn:
            conn.close()


def fetch_file_ids_for_page(folder_id: str, page: int, page_size: int) -> List[str]:
    """
    Fetch Telegram file_id values from Postgres for the given folder and page.
    Expects environment variables:
      - DATABASE_URL (preferred) OR
      - PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE

    SQL assumes a table named 'media' with columns:
      - folder_id (text)
      - file_id (text)
      - id (serial) or created_at for ordering
    """
    offset = (page - 1) * page_size
    database_url = os.environ.get("DATABASE_URL")
    conn = None
    try:
        if database_url:
            conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
        else:
            conn = psycopg2.connect(
                host=os.environ.get("PGHOST", "localhost"),
                port=int(os.environ.get("PGPORT", 5432)),
                user=os.environ.get("PGUSER", "postgres"),
                password=os.environ.get("PGPASSWORD", ""),
                dbname=os.environ.get("PGDATABASE", "postgres"),
                cursor_factory=RealDictCursor,
            )

        with conn.cursor() as cur:
            sql = """
                SELECT file_id
                FROM media
                WHERE folder_id = %s
                ORDER BY id ASC
                OFFSET %s
                LIMIT %s
            """
            cur.execute(sql, (folder_id, offset, page_size))
            rows = cur.fetchall()
            file_ids = [r["file_id"] for r in rows if r.get("file_id")]
            return file_ids
    except Exception as e:
        logger.exception("DB fetch failed for folder %s page %s: %s", folder_id, page, e)
        return []
    finally:
        if conn:
            conn.close()


# -----------------------
# Prefetching
# -----------------------
async def prefetch_pages(chat_id: int, folder_id: str, start_page: int, count: int = PREFETCH_COUNT):
    state = get_state(chat_id, folder_id)
    for p in range(start_page + 1, start_page + 1 + count):
        total_pages = compute_total_pages(state["total"], state["page_size"])
        if p > total_pages:
            break
        if p in state["cache"]:
            continue
        try:
            file_ids = fetch_file_ids_for_page(folder_id, p, state["page_size"])
            state["cache"][p] = file_ids
            if TESTING:
                logger.info("Prefetched page %s for folder %s", p, folder_id)
            await asyncio.sleep(PREFETCH_DELAY_SECONDS)
        except Exception as e:
            logger.exception("Prefetch failed for %s page %s: %s", folder_id, p, e)


# -----------------------
# Sending media as album
# -----------------------
def build_media_group_from_file_ids(file_ids: List[str]) -> List[InputMediaVideo]:
    media = []
    for fid in file_ids:
        media.append(InputMediaVideo(media=fid))
    return media


def send_page_album_sync(bot: Bot, chat_id: int, folder_id: str, page: int):
    """
    Synchronous wrapper to send a page as a media_group.
    """
    state = get_state(chat_id, folder_id)
    page_size = state["page_size"]
    file_ids = state["cache"].get(page)
    if file_ids is None:
        file_ids = fetch_file_ids_for_page(folder_id, page, page_size)
        state["cache"][page] = file_ids

    if not file_ids:
        try:
            bot.send_message(chat_id=chat_id, text="No media found for this page.")
        except Exception:
            logger.exception("Failed to send 'no media' message to %s", chat_id)
        return

    media = build_media_group_from_file_ids(file_ids)
    try:
        bot.send_media_group(chat_id=chat_id, media=media)
    except Exception as e:
        logger.exception("Failed to send media_group for %s page %s: %s", folder_id, page, e)
        # Fallback: send individually with small delay
        for fid in file_ids:
            try:
                bot.send_video(chat_id=chat_id, video=fid)
                # small synchronous sleep to avoid hammering
                asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.2))
            except Exception:
                logger.exception("Failed to send individual video %s", fid)


# -----------------------
# Command handlers
# -----------------------
def start_command(update: Update, context: CallbackContext):
    update.message.reply_text("Hello! Use /list <folder> to view a folder.")


def list_command(update: Update, context: CallbackContext):
    """
    Usage: /list <folder_id>
    Shows the first page and a persistent navigation keyboard.
    """
    chat_id = update.effective_chat.id
    args = context.args
    if not args:
        update.message.reply_text("Usage: /list <folder_id>")
        return
    folder_id = args[0]
    state = get_state(chat_id, folder_id)
    total = fetch_total_items_for_folder(folder_id)
    state["total"] = total
    state["page"] = 1
    state["cache"].clear()
    state["awaiting_jump"] = False

    total_pages = compute_total_pages(total, state["page_size"])
    page = state["page"]

    text = f"Collection: {folder_id}\nPage {page}/{total_pages} (Videos {(page-1)*state['page_size']+1}–{min(page*state['page_size'], total)} of {total})"
    keyboard = build_nav_keyboard(folder_id, page, total_pages)

    sent = update.message.reply_text(text=text, reply_markup=keyboard)
    state["message_id"] = sent.message_id

    # auto-send first page
    send_page_album_sync(context.bot, chat_id, folder_id, page)

    # prefetch next pages asynchronously
    try:
        asyncio.create_task(prefetch_pages(chat_id, folder_id, page, PREFETCH_COUNT))
    except Exception:
        # In some environments create_task may not be available; ignore prefetch if so
        logger.debug("Could not create prefetch task")


def goto_command(update: Update, context: CallbackContext):
    """
    Usage: /goto <folder_id> <page>
    Example: /goto mix 42
    """
    chat_id = update.effective_chat.id
    args = context.args
    if len(args) < 2:
        update.message.reply_text("Usage: /goto <folder_id> <page>")
        return
    folder_id = args[0]
    try:
        page = int(args[1])
    except ValueError:
        update.message.reply_text("Page must be a number.")
        return

    state = get_state(chat_id, folder_id)
    if state["total"] == 0:
        state["total"] = fetch_total_items_for_folder(folder_id)
    total_pages = compute_total_pages(state["total"], state["page_size"])
    page = max(1, min(page, total_pages))
    state["page"] = page
    state["cache"].pop(page, None)

    if state.get("message_id"):
        try:
            update.effective_message.bot.edit_message_text(
                chat_id=chat_id,
                message_id=state["message_id"],
                text=f"Collection: {folder_id}\nPage {page}/{total_pages} (Videos {(page-1)*state['page_size']+1}–{min(page*state['page_size'], state['total'])} of {state['total']})",
                reply_markup=build_nav_keyboard(folder_id, page, total_pages),
            )
        except Exception:
            pass

    send_page_album_sync(context.bot, chat_id, folder_id, page)
    try:
        asyncio.create_task(prefetch_pages(chat_id, folder_id, page, PREFETCH_COUNT))
    except Exception:
        logger.debug("Could not create prefetch task")


# -----------------------
# Callback query handler
# -----------------------
def callback_query_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    data = query.data or ""
    chat_id = query.message.chat_id

    if data == "noop":
        query.answer()
        return

    parts = data.split(":")
    action = parts[0] if parts else ""

    if action == "nav" and len(parts) >= 3:
        folder_id = parts[1]
        nav_action = parts[2]
        state = get_state(chat_id, folder_id)
        if state["total"] == 0:
            state["total"] = fetch_total_items_for_folder(folder_id)
        total_pages = compute_total_pages(state["total"], state["page_size"])

        if nav_action == "next":
            if state["page"] < total_pages:
                state["page"] += 1
        elif nav_action == "prev":
            if state["page"] > 1:
                state["page"] -= 1

        page = state["page"]
        try:
            query.edit_message_text(
                text=f"Collection: {folder_id}\nPage {page}/{total_pages} (Videos {(page-1)*state['page_size']+1}–{min(page*state['page_size'], state['total'])} of {state['total']})",
                reply_markup=build_nav_keyboard(folder_id, page, total_pages),
            )
        except Exception:
            pass

        send_page_album_sync(context.bot, chat_id, folder_id, page)
        try:
            asyncio.create_task(prefetch_pages(chat_id, folder_id, page, PREFETCH_COUNT))
        except Exception:
            logger.debug("Could not create prefetch task")
        query.answer()

    elif action == "send" and len(parts) >= 4:
        folder_id = parts[1]
        try:
            page = int(parts[3])
        except Exception:
            page = 1
        send_page_album_sync(context.bot, chat_id, folder_id, page)
        query.answer(text=f"Sent page {page}")

    elif action == "jump" and len(parts) >= 2:
        folder_id = parts[1]
        state = get_state(chat_id, folder_id)
        state["awaiting_jump"] = True
        try:
            query.edit_message_text(
                text=f"Enter page number to jump to (1–{compute_total_pages(state['total'] or fetch_total_items_for_folder(folder_id), state['page_size'])}):",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cancel", callback_data=f"cancel:{folder_id}")]]),
            )
        except Exception:
            pass
        query.answer(text="Send the page number as a message now.")

    elif action == "cancel" and len(parts) >= 2:
        folder_id = parts[1]
        state = get_state(chat_id, folder_id)
        state["awaiting_jump"] = False
        total_pages = compute_total_pages(state["total"] or fetch_total_items_for_folder(folder_id), state["page_size"])
        try:
            query.edit_message_text(
                text=f"Collection: {folder_id}\nPage {state['page']}/{total_pages} (Videos {(state['page']-1)*state['page_size']+1}–{min(state['page']*state['page_size'], state['total'])} of {state['total']})",
                reply_markup=build_nav_keyboard(folder_id, state["page"], total_pages),
            )
        except Exception:
            pass
        query.answer(text="Cancelled.")

    else:
        query.answer()


# -----------------------
# Message handler for jump input
# -----------------------
def text_message_handler(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    text = update.message.text.strip()
    awaiting = [(k, v) for k, v in PAGINATION_STATE.items() if k[0] == chat_id and v.get("awaiting_jump")]
    if not awaiting:
        return

    (c_id, folder_id), state = awaiting[0]
    m = re.search(r"\d+", text)
    if not m:
        update.message.reply_text("Please send a valid page number.")
        return
    page = int(m.group(0))
    if state["total"] == 0:
        state["total"] = fetch_total_items_for_folder(folder_id)
    total_pages = compute_total_pages(state["total"], state["page_size"])
    page = max(1, min(page, total_pages))
    state["page"] = page
    state["awaiting_jump"] = False

    if state.get("message_id"):
        try:
            context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=state["message_id"],
                text=f"Collection: {folder_id}\nPage {page}/{total_pages} (Videos {(page-1)*state['page_size']+1}–{min(page*state['page_size'], state['total'])} of {state['total']})",
                reply_markup=build_nav_keyboard(folder_id, page, total_pages),
            )
        except Exception:
            pass

    send_page_album_sync(context.bot, chat_id, folder_id, page)
    try:
        asyncio.create_task(prefetch_pages(chat_id, folder_id, page, PREFETCH_COUNT))
    except Exception:
        logger.debug("Could not create prefetch task")


# -----------------------
# Bot setup
# -----------------------
def main():
    if TOKEN == "YOUR_BOT_TOKEN_HERE":
        logger.warning("BOT_TOKEN not set. Replace TOKEN or set BOT_TOKEN environment variable.")
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start_command))
    dp.add_handler(CommandHandler("list", list_command))
    dp.add_handler(CommandHandler("goto", goto_command))
    dp.add_handler(CallbackQueryHandler(callback_query_handler))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, text_message_handler))

    updater.start_polling()
    logger.info("Bot started")
    updater.idle()


if __name__ == "__main__":
    main()
