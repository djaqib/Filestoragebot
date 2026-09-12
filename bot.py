import os
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from starlette.applications import Starlette
from starlette.responses import Response
from starlette.routing import Route
import uvicorn

# Environment variables from Render
TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
RENDER_EXTERNAL_URL = os.environ["RENDER_EXTERNAL_URL"]
PORT = int(os.environ.get("PORT", 10000))

# Connection pool
pg_pool = SimpleConnectionPool(1, 10, dsn=DATABASE_URL, sslmode="require")

def get_db():
    return pg_pool.getconn()

def release_db(conn):
    pg_pool.putconn(conn)

# --- Safe Database Initialization (Does NOT drop tables) ---
def init_db():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS folders (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    user_id BIGINT NOT NULL,
                    parent_id INT REFERENCES folders(id) ON DELETE CASCADE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS files (
                    id SERIAL PRIMARY KEY,
                    file_name VARCHAR(255) NOT NULL,
                    telegram_file_id VARCHAR(255) NOT NULL,
                    folder_id INT REFERENCES folders(id) ON DELETE CASCADE,
                    user_id BIGINT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
    finally:
        release_db(conn)

# --- Database Queries ---
def db_get_contents(user_id, folder_id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            if folder_id is None:
                cur.execute("SELECT id, name FROM folders WHERE user_id = %s AND parent_id IS NULL", (user_id,))
                folders = cur.fetchall()
                cur.execute("SELECT id, file_name FROM files WHERE user_id = %s AND folder_id IS NULL", (user_id,))
                files = cur.fetchall()
            else:
                cur.execute("SELECT id, name FROM folders WHERE user_id = %s AND parent_id = %s", (user_id, folder_id))
                folders = cur.fetchall()
                cur.execute("SELECT id, file_name FROM files WHERE user_id = %s AND folder_id = %s", (user_id, folder_id))
                files = cur.fetchall()
            return folders, files
    finally:
        release_db(conn)

def db_get_parent(folder_id):
    if folder_id is None:
        return None
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT parent_id FROM folders WHERE id = %s", (folder_id,))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        release_db(conn)

def db_create_folder(user_id, name, parent_id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO folders (name, user_id, parent_id) VALUES (%s, %s, %s)",
                (name, user_id, parent_id)
            )
            conn.commit()
    finally:
        release_db(conn)

def db_save_file(user_id, file_name, telegram_file_id, folder_id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO files (file_name, telegram_file_id, folder_id, user_id) VALUES (%s, %s, %s, %s)",
                (file_name, telegram_file_id, folder_id, user_id)
            )
            conn.commit()
    finally:
        release_db(conn)

def db_get_file(file_id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT file_name, telegram_file_id FROM files WHERE id = %s", (file_id,))
            return cur.fetchone()
    finally:
        release_db(conn)

# --- Dynamic Keyboard Generator ---
def build_menu(user_id, current_folder_id):
    folders, files = db_get_contents(user_id, current_folder_id)
    parent_id = db_get_parent(current_folder_id) if current_folder_id else None

    keyboard = []
    for f_id, f_name in folders:
        keyboard.append([InlineKeyboardButton(f"📁 {f_name}", callback_data=f"f_{f_id}")])

    for file_id, file_name in files:
        keyboard.append([InlineKeyboardButton(f"📄 {file_name}", callback_data=f"getfile_{file_id}")])

    controls = []
    if current_folder_id is not None:
        back_target = f"f_{parent_id}" if parent_id else "f_root"
        controls.append(InlineKeyboardButton("⬆️ Back", callback_data=back_target))

    folder_target = current_folder_id if current_folder_id else "root"
    controls.append(InlineKeyboardButton("➕ New Subfolder", callback_data=f"add_{folder_target}"))
    
    keyboard.append(controls)
    return InlineKeyboardMarkup(keyboard)

# --- Telegram Command & Message Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["current_folder"] = None
    markup = build_menu(update.effective_user.id, None)
    await update.message.reply_text("📁 **Your File Explorer**\n\nYou are in the Root directory.", reply_markup=markup, parse_mode="Markdown")

async def list_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Alias /list to open the file menu
    await start(update, context)

async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚙️ **Bot Settings**\n\n"
        "• **Hosting:** Render\n"
        "• **Database:** Neon PostgreSQL\n"
        "• **Storage Mode:** Subfolder Hierarchy Active\n\n"
        "Use /start or /list to view your folders.",
        parse_mode="Markdown"
    )

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id

    if data.startswith("f_"):
        target = data.split("_")[1]
        folder_id = None if target == "root" else int(target)
        context.user_data["current_folder"] = folder_id
        
        markup = build_menu(user_id, folder_id)
        text = f"📁 **Folder ID #{folder_id}**" if folder_id else "📁 **Your File Explorer (Root)**"
        await query.edit_message_text(text=text, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("add_"):
        target = data.split("_")[1]
        context.user_data["creating_in"] = None if target == "root" else int(target)
        await query.message.reply_text("Please reply with the name for your new subfolder:")

    elif data.startswith("getfile_"):
        file_id = int(data.split("_")[1])
        file_info = db_get_file(file_id)
        if file_info:
            file_name, telegram_id = file_info
            await query.message.reply_document(document=telegram_id, caption=f"📄 {file_name}")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "creating_in" in context.user_data:
        parent_id = context.user_data.pop("creating_in")
        folder_name = update.message.text.strip()
        user_id = update.effective_user.id

        db_create_folder(user_id, folder_name, parent_id)
        await update.message.reply_text(f"✅ Created folder **{folder_name}**! Send /start to view.", parse_mode="Markdown")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    current_folder = context.user_data.get("current_folder", None)
    
    document = update.message.document
    file_name = document.file_name or "Untitled File"
    file_id = document.file_id

    db_save_file(user_id, file_name, file_id, current_folder)
    await update.message.reply_text(f"✅ Saved **{file_name}** to your current folder! Send /start to view.", parse_mode="Markdown")

# --- App Build & Routes ---
ptb_app = Application.builder().token(TOKEN).build()

# Registered commands
ptb_app.add_handler(CommandHandler("start", start))
ptb_app.add_handler(CommandHandler("list", list_files))      # Added /list
ptb_app.add_handler(CommandHandler("settings", settings))  # Added /settings
ptb_app.add_handler(CallbackQueryHandler(handle_callback))
ptb_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
ptb_app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

async def telegram_webhook(request):
    data = await request.json()
    update = Update.de_json(data, ptb_app.bot)
    await ptb_app.process_update(update)
    return Response(status_code=200)

async def startup():
    init_db()
    await ptb_app.initialize()
    await ptb_app.start()
    webhook_url = f"{RENDER_EXTERNAL_URL}/webhook"
    await ptb_app.bot.set_webhook(webhook_url)

app = Starlette(
    routes=[Route("/webhook", telegram_webhook, methods=["POST"])],
    on_startup=[startup]
)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
