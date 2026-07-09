import sqlite3

DB_NAME = "files.db"


def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT NOT NULL,
            unique_id TEXT NOT NULL UNIQUE
        )
    """)

    conn.commit()
    conn.close()


def save_video(file_id, unique_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    try:
        cur.execute(
            "INSERT INTO videos (file_id, unique_id) VALUES (?, ?)",
            (file_id, unique_id),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def get_all_videos():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("SELECT file_id FROM videos")
    rows = cur.fetchall()

    conn.close()

    return [row[0] for row in rows]


def clear_videos():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("DELETE FROM videos")

    conn.commit()
    conn.close()


def count_videos():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM videos")
    count = cur.fetchone()[0]

    conn.close()

    return count
