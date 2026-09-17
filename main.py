import asyncio
import logging
import os

import uvicorn
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route

from telegram import BotCommand, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    TypeHandler,
    filters,
)

import db
import handlers

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "default_secret")
PORT = int(os.getenv("PORT", "8080"))

if not BOT_TOKEN or not db.DATABASE_URL or not RENDER_EXTERNAL_URL:
    logger.error("Missing required environment variables.")

# Initialize DB Schema
db.init_db()

# Build Application
app = Application.builder().token(BOT_TOKEN).build()

# Middleware & Access Control
app.add_handler(TypeHandler(Update, handlers.access_control), group=-1)

# Register Command Handlers
app.add_handler(CommandHandler("start", handlers.start))
app.add_handler(CommandHandler("help", handlers.help_command))
app.add_handler(CommandHandler("menu", handlers.menu_command))
app.add_handler(CommandHandler("settings", handlers.settings_command))
app.add_handler(CommandHandler("collect", handlers.collect))
app.add_handler(CommandHandler("fav", handlers.fav_shortcut))
app.add_handler(CommandHandler("current", handlers.current))
app.add_handler(CommandHandler("finish", handlers.finish))
app.add_handler(CommandHandler("stop", handlers.stop_command))
app.add_handler(CommandHandler("minlength", handlers.minlength))
app.add_handler(CommandHandler("removemode", handlers.removemode))
app.add_handler(CommandHandler("remove", handlers.remove_video))
app.add_handler(CommandHandler("status", handlers.status))
app.add_handler(CommandHandler("count", handlers.count_collection))
app.add_handler(CommandHandler("info", handlers.collection_info))
app.add_handler(CommandHandler("setexpiry", handlers.set_expiry))
app.add_handler(CommandHandler("get", handlers.get_collection))
app.add_handler(CommandHandler("getbysize", handlers.get_by_size))
app.add_handler(CommandHandler("list", handlers.list_collections))
app.add_handler(CommandHandler("random", handlers.random_video))
app.add_handler(CommandHandler("search", handlers.search_videos))
app.add_handler(CommandHandler("retryfailed", handlers.retry_failed))
app.add_handler(CommandHandler("delete", handlers.delete_collection))
app.add_handler(CommandHandler("rename", handlers.rename_collection))
app.add_handler(CommandHandler("move", handlers.move_collection))
app.add_handler(CommandHandler("copy", handlers.copy_collection))
app.add_handler(CommandHandler("merge", handlers.merge_collections))
app.add_handler(CommandHandler("dups", handlers.dups_collection))
app.add_handler(CommandHandler("neardupes", handlers.near_duplicates_command))
app.add_handler(CommandHandler("export", handlers.export_collection))
app.add_handler(CommandHandler("exportjson", handlers.export_json))
app.add_handler(CommandHandler("importjson", handlers.import_json))
app.add_handler(CommandHandler("cleanup", handlers.cleanup_collection))
app.add_handler(CommandHandler("backup", handlers.backup_database))

# Register Callbacks
app.add_handler(CallbackQueryHandler(handlers.menu_callback, pattern="^menu_"))
app.add_handler(CallbackQueryHandler(handlers.menu_folder_callback, pattern="^menufolder:"))
app.add_handler(CallbackQueryHandler(handlers.menu_get_all_callback, pattern="^menugetall:"))
app.add_handler(CallbackQueryHandler(handlers.menu_rand_all_callback, pattern="^menurandall:"))
app.add_handler(CallbackQueryHandler(handlers.menu_set_callback, pattern="^menuset:"))
app.add_handler(CallbackQueryHandler(handlers.menu_view_callback, pattern="^menuview:"))
app.add_handler(CallbackQueryHandler(handlers.menu_random_callback, pattern="^menurandom:"))
app.add_handler(CallbackQueryHandler(handlers.menu_back_callback, pattern="^menu_back$"))
app.add_handler(CallbackQueryHandler(handlers.settings_callback, pattern="^settings:"))

app.add_handler(CallbackQueryHandler(handlers.list_page_callback, pattern="^listpage:"))
app.add_handler(CallbackQueryHandler(handlers.list_folder_callback, pattern="^listfolder:"))
app.add_handler(CallbackQueryHandler(handlers.list_choice_callback, pattern="^listchoice:"))
app.add_handler(CallbackQueryHandler(handlers.list_delete_callback, pattern="^listdelete:"))
app.add_handler(CallbackQueryHandler(handlers.list_set_callback, pattern="^listset:"))
app.add_handler(CallbackQueryHandler(handlers.list_get_callback, pattern="^listget:"))
app.add_handler(CallbackQueryHandler(handlers.list_random_callback, pattern="^listrandom:"))

app.add_handler(CallbackQueryHandler(handlers.random_next_callback, pattern="^random_next:"))
app.add_handler(CallbackQueryHandler(handlers.random_next_recursive_callback, pattern="^randomnextr:"))

app.add_handler(CallbackQueryHandler(handlers.get_page_callback, pattern="^getpage:"))
app.add_handler(CallbackQueryHandler(handlers.get_page_callback, pattern="^getstop:"))
app.add_handler(CallbackQueryHandler(handlers.get_jump_callback, pattern="^getjump:"))
app.add_handler(CallbackQueryHandler(handlers.get_send_callback, pattern="^getsend:"))
app.add_handler(CallbackQueryHandler(handlers.get_cancel_callback, pattern="^getcancel:"))
app.add_handler(CallbackQueryHandler(handlers.cleanupnow_callback, pattern="^cleanupnow:"))

app.add_handler(CallbackQueryHandler(handlers.find_page_callback, pattern="^findpage:"))
app.add_handler(CallbackQueryHandler(handlers.find_video_callback, pattern="^findvideo:"))
app.add_handler(CallbackQueryHandler(handlers.find_all_callback, pattern="^findall:"))
app.add_handler(CallbackQueryHandler(handlers.find_close_callback, pattern="^findclose:"))

app.add_handler(CallbackQueryHandler(handlers.confirm_delete_callback, pattern="^confirmdelete:"))
app.add_handler(CallbackQueryHandler(handlers.cancel_delete_callback, pattern="^canceldelete$"))

app.add_handler(CallbackQueryHandler(handlers.neardup_page_callback, pattern="^ndpage:"))
app.add_handler(CallbackQueryHandler(handlers.neardup_del_callback, pattern="^nddel:"))
app.add_handler(CallbackQueryHandler(handlers.neardup_delboth_callback, pattern="^nddelboth:"))
app.add_handler(CallbackQueryHandler(handlers.neardup_keep_callback, pattern="^ndkeep$"))
app.add_handler(CallbackQueryHandler(handlers.neardup_close_callback, pattern="^ndclose:"))

# Message Handlers
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.handle_page_jump_input))
app.add_handler(MessageHandler(filters.VIDEO, handlers.handle_video))
app.add_handler(MessageHandler(filters.Document.ALL, handlers.handle_document))
app.add_handler(MessageHandler(filters.PHOTO, handlers.handle_non_video))


# Web Server Routes
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


starlette_app = Starlette(
    routes=[
        Route("/health", health_check, methods=["GET"]),
        Route("/telegram-webhook", telegram_webhook, methods=["POST"]),
    ]
)


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
        BotCommand("search", "Search videos by filename"),
        BotCommand("retryfailed", "Retry sending failed videos"),
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
