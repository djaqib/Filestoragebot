import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response
from telegram import Update
from telegram.ext import Application, CommandHandler, TypeHandler

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

if not BOT_TOKEN or not db.DATABASE_URL or not RENDER_EXTERNAL_URL:
    raise ValueError("Missing essential environment variables (BOT_TOKEN, DATABASE_URL, RENDER_EXTERNAL_URL)")

# Initialize Telegram Bot Application
ptb_app = Application.builder().token(BOT_TOKEN).build()

# Register Handlers
if hasattr(handlers, 'access_control'):
    ptb_app.add_handler(TypeHandler(Update, handlers.access_control), group=-1)

ptb_app.add_handler(CommandHandler("search", handlers.search_command))
ptb_app.add_handler(CommandHandler("help", handlers.help_command))

# Lifespan context manager to handle Telegram Bot startup/shutdown safely with FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    db.init_db()
    logger.info("Database schema initialized.")
    
    webhook_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}{WEBHOOK_PATH}"
    logger.info(f"Setting webhook to {webhook_url}")
    
    await ptb_app.initialize()
    await ptb_app.bot.set_webhook(url=webhook_url)
    await ptb_app.start()
    logger.info("Bot application started and webhook registered.")
    
    yield  # Server runs here
    
    # Shutdown logic
    logger.info("Shutting down bot application...")
    await ptb_app.stop()
    await ptb_app.shutdown()

# Initialize FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    """Processes incoming Telegram updates."""
    data = await request.json()
    update = Update.de_json(data, ptb_app.bot)
    await ptb_app.process_update(update)
    return Response(status_code=200)

@app.get("/")
async def health_check():
    """Health check endpoint for Render pinging."""
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    # Blocking Uvicorn call keeps the container alive
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")
