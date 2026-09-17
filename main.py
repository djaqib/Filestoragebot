import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, status
import uvicorn
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    TypeHandler,
)

import handlers
import db

# Setup Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "8787101536:AAExICL-rdGfF7loVCeieqBFUKnyrMFwR4s")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://filestoragebot-vul6.onrender.com/telegram-webhook")
PORT = int(os.getenv("PORT", 10000))

# Build python-telegram-bot Application instance
ptb_app = Application.builder().token(BOT_TOKEN).build()

# 1. Access Control Middleware (group=-1 runs before all other handlers)
ptb_app.add_handler(TypeHandler(Update, handlers.access_control), group=-1)

# 2. Command Handlers
ptb_app.add_handler(CommandHandler("start", handlers.start_command))
ptb_app.add_handler(CommandHandler("help", handlers.help_command))
ptb_app.add_handler(CommandHandler("search", handlers.search_command))

# 3. Callback Query Handlers
ptb_app.add_handler(CallbackQueryHandler(handlers.handle_callback))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles async startup and shutdown tasks for FastAPI and Telegram Bot."""
    # Database initialization
    if hasattr(db, "init_db"):
        db.init_db()
        logger.info("Database schema initialized.")

    # Initialize and start PTB Application instance
    logger.info("Initializing Telegram bot application...")
    await ptb_app.initialize()
    await ptb_app.start()

    # Register Webhook with Telegram
    if WEBHOOK_URL:
        logger.info(f"Setting webhook to {WEBHOOK_URL}")
        await ptb_app.bot.set_webhook(url=WEBHOOK_URL)

    yield  # Application runs here

    # Shutdown sequence
    logger.info("Shutting down bot application...")
    await ptb_app.stop()
    await ptb_app.shutdown()


# Initialize FastAPI with the lifespan context manager
app = FastAPI(lifespan=lifespan)


@app.api_route("/", methods=["GET", "HEAD"])
async def health_check():
    """Health check endpoint to satisfy Render pings."""
    return Response(status_code=status.HTTP_200_OK)


@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    """Processes incoming updates sent by Telegram's webhook service."""
    try:
        data = await request.json()
        update = Update.de_json(data, ptb_app.bot)
        await ptb_app.process_update(update)
        return Response(status_code=status.HTTP_200_OK)
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")
