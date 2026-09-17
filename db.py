import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_connection():
    # Uses standard Postgres connection string (e.g., from Neon console)
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def search_videos(query=None, min_duration=None, max_duration=None, min_size_mb=None, max_size_mb=None):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    sql = "SELECT * FROM videos WHERE 1=1"
    params = []

    # Optional keyword search across titles or tags if provided
    if query:
        sql += " AND (title ILIKE %s OR tags ILIKE %s)"
        params.extend([f"%{query}%", f"%{query}%"])

    # Filter by duration (seconds)
    if min_duration is not None:
        sql += " AND duration >= %s"
        params.append(min_duration)

    if max_duration is not None:
        sql += " AND duration <= %s"
        params.append(max_duration)

    # Convert MB to Bytes for Neon's file_size bigint column
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
