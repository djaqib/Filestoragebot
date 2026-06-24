import os
import asyncio
import logging
import psycopg2
from psycopg2 import pool
from telegram import Update, BotCommand, InputMediaVideo
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"]
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]
RENDER_EXTERNAL_URL = os.environ["RENDER_EXTERNAL_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]
PORT = int(os.environ.get("PORT", 10000))

# Default collection name used until the user sets one with /collect
DEFAULT_COLLECTION = "default"

# Simple connection pool so we don't open a fresh TCP/TLS handshake to Neon on
# every single message (Neon cold starts are fast, but no need to pay it twice).
db_pool = psycopg2.pool.SimpleConnectionPool(1, 5, DATABASE_URL, sslmode="require")

# Tracks which collection each chat is currently adding videos to.
# Resets to DEFAULT_COLLECTION on bot restart; persisted videos themselves
# are NOT affected by a restart, only this "current pointer" in memory.
active_collection: dict[int, str] = {}


def init_db():
    conn = db_pool.getconn()
    try:
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
        conn.commit()
    finally:
        db_pool.putconn(conn)


def get_active_collection(chat_id: int) -> str:
    return active_collection.get(chat_id, DEFAULT_COLLECTION)


HELP_TEXT = (
    "🎬 *Video Collector Bot*\n\n"
    "Send or forward videos and I'll save them into named collections\\. "
    "Photos and other file types are ignored\\. Duplicate videos within "
    "the same collection are skipped automatically\\.\n\n"
    "*Commands:*\n"
    "/collect `<name>` \\- Set the active collection \\(new videos go here\\)\n"
    "/finish \\- Stop adding to the active collection \\(resets to default\\)\n"
    "/current \\- Show which collection is active\n"
    "/list \\- List all collections and how many videos each has\n"
    "/get `<name>` \\- Send back every video in a collection, in albums of 10\n"
    "/delete `<name>` \\- Permanently delete a collection and its videos\n"
    "/status \\- Show video count in the active collection\n"
    "/help \\- Show this help message"
)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Send me videos and I'll collect them. Use /collect <name> to start "
        "a named collection, then /help to see everything I can do."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode="MarkdownV2")


async def collect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text(
            f"Usage: /collect <name>\nCurrently active: *{get_active_collection(chat_id)}*",
            parse_mode="MarkdownV2",
        )
        return

    name = " ".join(context.args).strip()
    active_collection[chat_id] = name
    await update.message.reply_text(f"📁 Active collection set to: {name}")


async def current(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(f"📁 Active collection: {get_active_collection(chat_id)}")


async def finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    previous = get_active_collection(chat_id)
    active_collection.pop(chat_id, None)
    await update.message.reply_text(
        f"✅ Finished with '{previous}'. Active collection reset to '{DEFAULT_COLLECTION}'.\n"
        f"Use /collect <name> before sending more videos to start a new one."
    )


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    video = update.message.video
    collection = get_active_collection(chat_id)

    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO videos (collection, file_id, file_unique_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (collection, file_unique_id) DO NOTHING
                RETURNING id
                """,
                (collection, video.file_id, video.file_unique_id),
            )
            inserted = cur.fetchone() is not None
        conn.commit()
    finally:
        db_pool.putconn(conn)

    if inserted:
        await update.message.reply_text(f"✅ Saved to '{collection}'")
    else:
        await update.message.reply_text(f"⚠️ Already in '{collection}', skipping duplicate.")


async def handle_non_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Photos, regular documents, audio, voice notes land here and are ignored.
    return


async def list_collections(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT collection, COUNT(*) FROM videos GROUP BY collection ORDER BY collection"
            )
            rows = cur.fetchall()
    finally:
        db_pool.putconn(conn)

    if not rows:
        await update.message.reply_text("No collections yet. Send a video to start one.")
        return

    lines = [f"• {name} — {count} video(s)" for name, count in rows]
    await update.message.reply_text("📚 Collections:\n" + "\n".join(lines))


async def get_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /get <name>")
        return

    name = " ".join(context.args).strip()
    chat_id = update.effective_chat.id

    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_id FROM videos WHERE collection = %s ORDER BY added_at",
                (name,),
            )
            rows = cur.fetchall()
    finally:
        db_pool.putconn(conn)

    if not rows:
        await update.message.reply_text(f"No videos found in '{name}' ❌")
        return

    file_ids = [r[0] for r in rows]
    ALBUM_SIZE = 10  # Telegram's hard cap per media group
    total_batches = (len(file_ids) + ALBUM_SIZE - 1) // ALBUM_SIZE

    await update.message.reply_text(
        f"📤 Sending {len(file_ids)} video(s) from '{name}' in {total_batches} album(s)..."
    )

    for i in range(0, len(file_ids), ALBUM_SIZE):
        batch = file_ids[i:i + ALBUM_SIZE]
        media_group = [InputMediaVideo(media=fid) for fid in batch]
        await context.bot.send_media_group(chat_id=chat_id, media=media_group)
        # Small pause between albums to stay well clear of Telegram's rate limits
        # on large batches (e.g. 300 videos = 30 albums).
        await asyncio.sleep(1)

    await update.message.reply_text("✅ Done")


async def delete_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /delete <name>")
        return

    name = " ".join(context.args).strip()

    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM videos WHERE collection = %s", (name,))
            deleted_count = cur.rowcount
        conn.commit()
    finally:
        db_pool.putconn(conn)

    if deleted_count == 0:
        await update.message.reply_text(f"No collection named '{name}' found.")
    else:
        await update.message.reply_text(f"🗑️ Deleted '{name}' ({deleted_count} video(s)).")


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    collection = get_active_collection(chat_id)

    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM videos WHERE collection = %s", (collection,))
            count = cur.fetchone()[0]
    finally:
        db_pool.putconn(conn)

    await update.message.reply_text(f"📦 '{collection}' has {count} video(s).")


async def post_init(application: Application):
    init_db()
    await application.bot.set_my_commands([
        BotCommand("collect", "Set the active collection name"),
        BotCommand("finish", "Stop adding to active collection"),
        BotCommand("current", "Show the active collection"),
        BotCommand("list", "List all collections"),
        BotCommand("get", "Send back a collection's videos"),
        BotCommand("delete", "Delete a collection"),
        BotCommand("status", "Show count in active collection"),
        BotCommand("help", "Show help and command list"),
    ])


def main():
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("collect", collect))
    application.add_handler(CommandHandler("finish", finish))
    application.add_handler(CommandHandler("current", current))
    application.add_handler(CommandHandler("list", list_collections))
    application.add_handler(CommandHandler("get", get_collection))
    application.add_handler(CommandHandler("delete", delete_collection))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(MessageHandler(filters.VIDEO, handle_video))
    application.add_handler(MessageHandler(
        filters.PHOTO | filters.Document.ALL | filters.AUDIO | filters.VOICE,
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
