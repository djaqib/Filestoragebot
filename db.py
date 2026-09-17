import os
import asyncio
import logging
from typing import Optional, Tuple, List

import psycopg2

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
NEAR_DUP_DURATION_TOLERANCE_SECONDS = 2
NEAR_DUP_SIZE_TOLERANCE_FRACTION = 0.05
NEAR_DUP_SIZE_ONLY_TOLERANCE_FRACTION = 0.02


def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def _db_call(fn):
    conn = get_db_connection()
    try:
        res = fn(conn)
        conn.commit()
        return res
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


async def db_run(fn):
    return await asyncio.to_thread(_db_call, fn)


def init_db():
    def _schema(conn):
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS videos (
                    id SERIAL PRIMARY KEY,
                    collection TEXT NOT NULL,
                    file_id TEXT NOT NULL,
                    file_unique_id TEXT NOT NULL,
                    duration INTEGER,
                    file_size BIGINT,
                    file_name TEXT,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(collection, file_unique_id)
                );
                CREATE INDEX IF NOT EXISTS idx_videos_col ON videos(collection);
                CREATE INDEX IF NOT EXISTS idx_videos_fuid ON videos(file_unique_id);

                CREATE TABLE IF NOT EXISTS sent_videos (
                    chat_id BIGINT NOT NULL,
                    collection TEXT NOT NULL,
                    file_unique_id TEXT NOT NULL,
                    sent_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(chat_id, collection, file_unique_id)
                );

                CREATE TABLE IF NOT EXISTS dead_files (
                    file_unique_id TEXT PRIMARY KEY,
                    detected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS collection_settings (
                    collection TEXT PRIMARY KEY,
                    expiry_days INTEGER DEFAULT 0
                );
                """
            )

    _db_call(_schema)
    logger.info("Database schema initialized.")


async def save_video_to_db(
    collection: str,
    file_id: str,
    file_unique_id: str,
    duration: Optional[int],
    file_size: Optional[int],
    file_name: Optional[str],
) -> bool:
    def _insert(conn):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO videos (collection, file_id, file_unique_id, duration, file_size, file_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (collection, file_unique_id) DO NOTHING
                """,
                (collection, file_id, file_unique_id, duration, file_size, file_name),
            )
            return cur.rowcount > 0

    return await db_run(_insert)


async def delete_video_from_collection(collection: str, file_unique_id: str) -> bool:
    def _delete(conn):
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM videos WHERE collection = %s AND file_unique_id = %s",
                (collection, file_unique_id),
            )
            return cur.rowcount > 0

    return await db_run(_delete)


def fetch_near_duplicates(collection: str) -> List[Tuple[Tuple[str, str, int, int], Tuple[str, str, int, int]]]:
    def _query(conn):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT v1.file_id, v1.file_unique_id, v1.duration, v1.file_size,
                       v2.file_id, v2.file_unique_id, v2.duration, v2.file_size
                FROM videos v1
                JOIN videos v2 ON v1.collection = v2.collection AND v1.id < v2.id
                WHERE v1.collection = %s
                  AND (
                    (
                      v1.duration IS NOT NULL AND v2.duration IS NOT NULL
                      AND ABS(v1.duration - v2.duration) <= %s
                      AND v1.file_size IS NOT NULL AND v2.file_size IS NOT NULL
                      AND v2.file_size BETWEEN v1.file_size * (1 - %s) AND v1.file_size * (1 + %s)
                    )
                    OR
                    (
                      (v1.duration IS NULL OR v2.duration IS NULL)
                      AND v1.file_size IS NOT NULL AND v2.file_size IS NOT NULL
                      AND v2.file_size BETWEEN v1.file_size * (1 - %s) AND v1.file_size * (1 + %s)
                    )
                  )
                ORDER BY v1.id
                """,
                (
                    collection,
                    NEAR_DUP_DURATION_TOLERANCE_SECONDS,
                    NEAR_DUP_SIZE_TOLERANCE_FRACTION,
                    NEAR_DUP_SIZE_TOLERANCE_FRACTION,
                    NEAR_DUP_SIZE_ONLY_TOLERANCE_FRACTION,
                    NEAR_DUP_SIZE_ONLY_TOLERANCE_FRACTION,
                ),
            )
            rows = cur.fetchall()
            pairs = []
            for r in rows:
                v1 = (r[0], r[1], r[2], r[3])
                v2 = (r[4], r[5], r[6], r[7])
                pairs.append((v1, v2))
            return pairs

    return _db_call(_query)
