import argparse
import shlex
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
from db import search_videos
# ================================
# KEYBOARDS & MENUS
# ================================

def get_main_menu() -> InlineKeyboardMarkup:
    """Returns the main top-level menu keyboard."""
    keyboard = [
        [
            InlineKeyboardButton("📁 Categories", callback_data="categories"),
            InlineKeyboardButton("🔍 Search Help", callback_data="search_help"),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_location_menu() -> InlineKeyboardMarkup:
    """
    Returns the actions menu for a specific location.
    The redundant 'View Action Menu' button has been removed.
    """
    keyboard = [
        [
            InlineKeyboardButton("📁 Set Active", callback_data="set_active"),
            InlineKeyboardButton("📩 Get Videos", callback_data="get_videos"),
        ],
        [
            InlineKeyboardButton("🎲 Random", callback_data="random"),
            InlineKeyboardButton("🗑️ Delete", callback_data="delete"),
        ],
        [InlineKeyboardButton("⬅️ Back to Menu", callback_data="main_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


# ================================
# COMMAND HANDLERS
# ================================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /start command."""
    await update.message.reply_text(
        "<b>Welcome!</b> Choose an option below or use /help to see available commands.",
        reply_markup=get_main_menu(),
        parse_mode="HTML"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /help command with full /search parameter documentation."""
    help_text = (
        "<b>🤖 Bot Command Guide</b>\n\n"
        "<b>Search Syntax:</b>\n"
        "• <code>/search &lt;query&gt;</code> - Basic keyword search\n\n"
        "<b>Filter Parameters:</b>\n"
        "• <code>--min-duration &lt;sec&gt;</code> - Minimum length in seconds\n"
        "• <code>--max-duration &lt;sec&gt;</code> - Maximum length in seconds\n"
        "• <code>--min-size &lt;MB&gt;</code> - Minimum file size in MB\n"
        "• <code>--max-size &lt;MB&gt;</code> - Maximum file size in MB\n\n"
        "<b>Examples:</b>\n"
        "• <code>/search action --max-duration 300</code>\n"
        "• <code>/search --min-size 10 --max-size 100</code>\n"
        "• <code>/search \"long video\" --min-duration 600 --max-size 500</code>\n\n"
        "<b>Other Commands:</b>\n"
        "• <code>/start</code> - Display the main interactive menu"
    )
    await update.message.reply_text(help_text, parse_mode="HTML")


async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        args = shlex.split(" ".join(context.args))
    except ValueError:
        await update.message.reply_text("❌ Quote error. Make sure your quotes are closed properly.")
        return

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("query", nargs="*", default=[])
    parser.add_argument("--min-duration", type=int, help="Min duration in seconds")
    parser.add_argument("--max-duration", type=int, help="Max duration in seconds")
    parser.add_argument("--min-size", type=float, help="Min size in MB")
    parser.add_argument("--max-size", type=float, help="Max size in MB")

    try:
        parsed, _ = parser.parse_known_args(args)
    except Exception:
        await update.message.reply_text("❌ Invalid search flags format.")
        return

    search_query = " ".join(parsed.query).strip()

    # Query PostgreSQL database
    results = search_videos(
        query=search_query if search_query else None,
        min_duration=parsed.min_duration,
        max_duration=parsed.max_duration,
        min_size_mb=parsed.min_size,
        max_size_mb=parsed.max_size
    )

    if not results:
        await update.message.reply_text("❌ No videos found matching your criteria.")
        return

    # Format output message
    response_text = f"<b>Found {len(results)} Video(s):</b>\n\n"
    for vid in results:
        response_text += f"• <b>{vid['title']}</b> ({vid['duration']}s | {round(vid['file_size'] / (1024*1024), 1)} MB)\n"

    await update.message.reply_text(response_text, parse_mode="HTML")

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("query", nargs="*", default=[])
    parser.add_argument("--min-duration", type=int, help="Min duration in seconds")
    parser.add_argument("--max-duration", type=int, help="Max duration in seconds")
    parser.add_argument("--min-size", type=float, help="Min size in MB")
    parser.add_argument("--max-size", type=float, help="Max size in MB")

    try:
        parsed, _ = parser.parse_known_args(args)
    except Exception:
        await update.message.reply_text("❌ Invalid search flags format.")
        return

    search_query = " ".join(parsed.query)
    min_dur = parsed.min_duration
    max_dur = parsed.max_duration
    min_sz = parsed.min_size
    max_sz = parsed.max_size

    # Pass (search_query, min_dur, max_dur, min_sz, max_sz) into your db.py function here

    response = (
        f"🔍 <b>Searching for:</b> '{search_query if search_query else 'All'}'\n"
        f"⏱️ <b>Duration:</b> {min_dur or 0}s to {max_dur or '∞'}s\n"
        f"📦 <b>File Size:</b> {min_sz or 0}MB to {max_sz or '∞'}MB"
    )
    await update.message.reply_text(response, parse_mode="HTML")


# ================================
# CALLBACK QUERY HANDLER
# ================================

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles inline keyboard interactions and smooth back-navigation."""
    query = update.callback_query
    await query.answer()

    data = query.data

    if data == "main_menu":
        await query.edit_message_text("Main Menu:", reply_markup=get_main_menu())
    elif data == "search_help":
        await help_command(update, context)
    elif data == "set_active":
        await query.edit_message_text("Location set to active!", reply_markup=get_location_menu())
    elif data == "get_videos":
        await query.edit_message_text("Fetching videos...", reply_markup=get_location_menu())
    elif data == "random":
        await query.edit_message_text("Selecting random video...", reply_markup=get_location_menu())
    elif data == "delete":
        await query.edit_message_text("Item deleted.", reply_markup=get_main_menu())
