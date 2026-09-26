"""
main.py - Application assembly, webhook server, and startup.

This is the Render entrypoint (Start Command: python main.py).
"""
import asyncio
import logging

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route
import uvicorn

from telegram import BotCommand, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    TypeHandler,
    filters,
)

from db import init_db
from utils import BOT_TOKEN, PORT, RENDER_EXTERNAL_URL, WEBHOOK_SECRET
from handlers import (
    access_control,
    autodelete,
    backup_database,
    cancel_delete_callback,
    cleanup_collection,
    collect,
    collection_info,
    confirm_delete_callback,
    copy_collection,
    count_collection,
    current,
    delete_collection,
    dups_collection,
    export_collection,
    export_json,
    fav_shortcut,
    finish,
    get_batch_toggle_callback,
    get_cancel_callback,
    get_collection,
    get_jump_callback,
    get_next_callback,
    get_prev_callback,
    handle_document,
    handle_get_page_jump_text,
    handle_non_video,
    handle_video,
    help_command,
    import_json,
    list_collections,
    list_delete_callback,
    list_folder_callback,
    list_get_callback,
    list_random_callback,
    list_set_callback,
    menu_back_callback,
    menu_callback,
    menu_command,
    menu_folder_callback,
    menu_get_all_callback,
    menu_rand_all_callback,
    menu_set_callback,
    merge_collections,
    minlength,
    move_collection,
    near_duplicates_command,
    neardup_close_callback,
    neardup_del_callback,
    neardup_delboth_callback,
    neardup_keep_callback,
    neardup_page_callback,
    random_next_callback,
    random_video,
    remove_video,
    removemode,
    rename_collection,
    search_videos,
    search_view_callback,
    set_expiry,
    settings_callback,
    settings_command,
    start,
    status,
    stop_command,
)

logger = logging.getLogger(__name__)

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
app.add_handler(CommandHandler("autodelete", autodelete))
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
app.add_handler(CallbackQueryHandler(neardup_keep_callback, pattern="^ndkeep:"))
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
        BotCommand("neardupes", "Visual near-duplicate cleanup"),
        BotCommand("autodelete", "Auto-delete forwarded videos after saving"),
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
