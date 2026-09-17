import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response
from telegram import Update
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, TypeHandler

import db
import handlers

# Configure Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Config & Environment Variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
PORT = int(os.getenv("PORT", 10000))
WEBHOOK_PATH = "/telegram-webhook"

# Parse Extra Render Environment Variables
ALLOWED_USER_IDS = [
    int(x.strip()) for x in os.getenv("ALLOWED_USER_IDS", "").split(",") if x.strip()
]
ADMIN_LOG_CHANNEL = os.getenv("ADMIN_LOG_CHANNEL")
BACKUP_CHANNEL_ID = os.getenv("BACKUP_CHANNEL_ID")

if not BOT_TOKEN or not db.DATABASE_URL or not RENDER_EXTERNAL_URL:
    raise ValueError("Missing essential environment variables (BOT_TOKEN, DATABASE_URL, RENDER_EXTERNAL_URL)")

# Initialize Telegram Bot Application
ptb_app = Application.builder().token(BOT_TOKEN).build()

# Store global environment context in bot_data for handlers
ptb_app.bot_data["allowed_user_ids"] = ALLOWED_USER_IDS
ptb_app.bot_data["admin_log_channel"] = ADMIN_LOG_CHANNEL
ptb_app.bot_data["backup_channel_id"] = BACKUP_CHANNEL_ID

# Register Middleware (Access Control check runs first on every update)
ptb_app.add_handler(TypeHandler(Update, handlers.access_control), group=-1)

# Register Command & Callback Handlers
ptb_app.add_handler(CommandHandler("start", handlers.start_command))
ptb_app.add_handler(CommandHandler("help", handlers.help_command))
ptb_app.add_handler(CommandHandler("search", handlers.search_command))
ptb_app.add_handler(CallbackQueryHandler(handlers.handle_callback))

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize Database & Register Webhook
    db.init_db()
    logger.info("Database schema initialized.")
    
    webhook_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}{WEBHOOK_PATH}"
    logger.info(f"Setting webhook to {webhook_url}")
    
    await ptb_app.initialize()
    await ptb_app.bot.set_webhook(url=webhook_url)
    await ptb_app.start()
    logger.info("Bot application started and webhook registered.")
    
    yield
    
    # Shutdown
    logger.info("Shutting down bot application...")
    await ptb_app.stop()
    await ptb_app.shutdown()

# Initialize FastAPI App
app = FastAPI(lifespan=lifespan)
from fastapi import FastAPI, Response, status

app = FastAPI()

# Add a health check endpoint for GET and HEAD requests
@app.api_route("/", methods=["GET", "HEAD"])
async def health_check():
    return Response(status_code=status.HTTP_200_OK)

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    """Processes incoming Telegram updates."""
    data = await request.json()
    update = Update.de_json(data, ptb_app.bot)
    await ptb_app.process_update(update)
    return Response(status_code=200)

@app.get("/")
async def health_check():
    """Health check endpoint for Render."""
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")
