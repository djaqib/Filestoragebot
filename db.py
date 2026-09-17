import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Read connection string from Render environment
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    """Initializes the database schema if tables do not exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            id SERIAL PRIMARY KEY,
            title TEXT,
            tags TEXT,
            duration INTEGER,
            file_size BIGINT,
            vault_message_id BIGINT,
            added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()

def search_videos(query=None, min_duration=None, max_duration=None, min_size_mb=None, max_size_mb=None):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    sql = "SELECT * FROM videos WHERE 1=1"
    params = []

    if query:
        sql += " AND (title ILIKE %s OR tags ILIKE %s)"
        params.extend([f"%{query}%", f"%{query}%"])

    if min_duration is not None:
        sql += " AND duration >= %s"
        params.append(min_duration)

    if max_duration is not None:
        sql += " AND duration <= %s"
        params.append(max_duration)

    if min_size_mb is not None:
        sql += " AND file_size >= %s"
        params.append(int(min_size_mb * 1024 * 1024))

    if max_size_mb is not None:
        sql += " AND file_size <= %s"
        params.append(int(max_size_mb * 1024 * 1024))

    sql += " ORDER BY added_at DESC LIMIT 20;"

    cursor.execute(sql, params)
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    return results
