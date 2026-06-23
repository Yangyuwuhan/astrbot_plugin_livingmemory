"""SQLite-backed storage for archived conversation history."""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import aiosqlite


class ArchiveStore:
    """Persist original conversation text keyed by document ID."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    @asynccontextmanager
    async def _connect(self):
        db = await aiosqlite.connect(self.db_path)
        try:
            await db.execute("PRAGMA journal_mode = WAL")
            await db.execute("PRAGMA busy_timeout = 10000")
            yield db
        finally:
            await db.close()

    @staticmethod
    def _now() -> float:
        return time.time()

    async def initialize(self) -> None:
        """Create the conversation_archive table."""
        async with self._connect() as db:
            await db.execute("PRAGMA foreign_keys = ON")
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS conversation_archive (
                    id INTEGER PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    persona_id TEXT,
                    conversation_text TEXT NOT NULL,
                    is_group_chat INTEGER NOT NULL DEFAULT 0,
                    message_count INTEGER NOT NULL DEFAULT 0,
                    source_start INTEGER,
                    source_end INTEGER,
                    stored_at REAL NOT NULL
                )
                """
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_archive_session ON conversation_archive(session_id)"
            )
            await db.commit()

    async def archive(
        self,
        id: int,
        session_id: str,
        persona_id: str | None,
        conversation_text: str,
        message_count: int = 0,
        is_group_chat: bool = False,
        source_start: int | None = None,
        source_end: int | None = None,
    ) -> None:
        """Insert or replace an archive entry keyed by document ID."""
        async with self._connect() as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO conversation_archive (
                    id, session_id, persona_id, conversation_text,
                    is_group_chat, message_count, source_start, source_end, stored_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    id,
                    session_id,
                    persona_id,
                    conversation_text,
                    int(is_group_chat),
                    message_count,
                    source_start,
                    source_end,
                    self._now(),
                ),
            )
            await db.commit()

    async def get(self, memory_id: int) -> dict[str, Any] | None:
        """Retrieve an archive entry by document ID."""
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM conversation_archive WHERE id = ?",
                (memory_id,),
            )
            row = await cursor.fetchone()
        if not row:
            return None
        return {
            "id": int(row["id"]),
            "session_id": row["session_id"],
            "persona_id": row["persona_id"],
            "conversation_text": row["conversation_text"],
            "is_group_chat": bool(row["is_group_chat"]),
            "message_count": int(row["message_count"]),
            "source_start": row["source_start"],
            "source_end": row["source_end"],
            "stored_at": float(row["stored_at"]),
        }

    async def delete(self, memory_id: int) -> bool:
        """Delete a single archive entry. Returns True if a row was deleted."""
        async with self._connect() as db:
            cursor = await db.execute(
                "DELETE FROM conversation_archive WHERE id = ?",
                (memory_id,),
            )
            await db.commit()
            return cursor.rowcount > 0

    async def delete_by_ids(self, ids: list[int]) -> int:
        """Batch-delete archive entries. Returns count of deleted rows."""
        if not ids:
            return 0
        async with self._connect() as db:
            placeholders = ",".join("?" * len(ids))
            cursor = await db.execute(
                f"DELETE FROM conversation_archive WHERE id IN ({placeholders})",
                ids,
            )
            await db.commit()
            return cursor.rowcount

    async def copy(self, src_id: int, dst_id: int) -> None:
        """Copy an archive entry to a new ID (used during rebuild)."""
        async with self._connect() as db:
            # Check source exists before copying
            cursor = await db.execute(
                "SELECT COUNT(*) FROM conversation_archive WHERE id = ?",
                (src_id,),
            )
            row = await cursor.fetchone()
            if not row or int(row[0]) == 0:
                return

            await db.execute(
                """
                INSERT OR REPLACE INTO conversation_archive (
                    id, session_id, persona_id, conversation_text,
                    is_group_chat, message_count, source_start, source_end, stored_at
                )
                SELECT
                    ?, session_id, persona_id, conversation_text,
                    is_group_chat, message_count, source_start, source_end, stored_at
                FROM conversation_archive
                WHERE id = ?
                """,
                (dst_id, src_id),
            )
            await db.commit()

    async def count(self) -> int:
        """Return total number of archive entries."""
        async with self._connect() as db:
            cursor = await db.execute("SELECT COUNT(*) FROM conversation_archive")
            row = await cursor.fetchone()
        return int(row[0]) if row else 0


__all__ = ["ArchiveStore"]
