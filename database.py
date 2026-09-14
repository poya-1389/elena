"""
database.py — لایه‌ی دیتابیس النا (Elena)

از PostgreSQL روی Railway استفاده می‌کند (asyncpg، بدون ORM سنگین).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import asyncpg

logger = logging.getLogger("elena.database")

DAILY_LIMIT = 75
WINDOW_4H_LIMIT = 30
PREMIUM_WINDOW_4H_LIMIT = 500  # طبق تصمیم کاربر: مشترک‌ها 500 پیام در 4 ساعت
PREMIUM_DAILY_LIMIT = 3000     # متناسب با سقف 4 ساعته (500 در هر بازه)، تا سقف روزانه محدودکننده نباشه
HISTORY_LIMIT = 40  # تعداد پیام‌های اخیر هر چت که در Context نگه داشته می‌شود (تقویت‌شده)
GROUP_COOLDOWN_SECONDS = 240  # فاصله‌ی حداقلی بین دو ورود خودکار النا به بحث یک گروه

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id     BIGINT PRIMARY KEY,
    first_name  TEXT,
    username    TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    premium     BOOLEAN NOT NULL DEFAULT false
);

-- فقط پیام‌هایی که AI واقعاً با موفقیت جوابشون رو داد اینجا ثبت می‌شن
-- (تا وقتی خطا می‌خوریم، مصرف کاربر بی‌خودی کم نشه)
CREATE TABLE IF NOT EXISTS message_log (
    id          BIGSERIAL PRIMARY KEY,
    chat_id     BIGINT NOT NULL,
    user_id     BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_message_log_lookup
    ON message_log (chat_id, user_id, created_at);

CREATE TABLE IF NOT EXISTS conversation (
    id          BIGSERIAL PRIMARY KEY,
    chat_id     BIGINT NOT NULL,
    role        TEXT NOT NULL,          -- 'user' | 'model'
    author_name TEXT,
    content     TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_conversation_chat
    ON conversation (chat_id, created_at);

CREATE TABLE IF NOT EXISTS group_cooldown (
    chat_id             BIGINT PRIMARY KEY,
    last_ambient_reply  TIMESTAMPTZ
);
"""


class Database:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=10)
        async with self.pool.acquire() as conn:
            await conn.execute(SCHEMA)
        logger.info("Database connected and schema ensured.")

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    # ---------- کاربران ----------

    async def upsert_user(self, user_id: int, first_name: str, username: str | None) -> None:
        await self.pool.execute(
            """
            INSERT INTO users (user_id, first_name, username)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE
                SET first_name = EXCLUDED.first_name,
                    username = EXCLUDED.username
            """,
            user_id, first_name, username,
        )

    async def is_premium(self, user_id: int) -> bool:
        row = await self.pool.fetchrow("SELECT premium FROM users WHERE user_id=$1", user_id)
        return bool(row and row["premium"])

    async def get_user(self, user_id: int) -> dict | None:
        row = await self.pool.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)
        return dict(row) if row else None

    async def set_premium(self, user_id: int, value: bool) -> None:
        await self.pool.execute("UPDATE users SET premium=$2 WHERE user_id=$1", user_id, value)

    async def count_users(self) -> int:
        return await self.pool.fetchval("SELECT count(*) FROM users")

    async def count_premium(self) -> int:
        return await self.pool.fetchval("SELECT count(*) FROM users WHERE premium=true")

    async def list_users(self, limit: int, offset: int) -> list[dict]:
        rows = await self.pool.fetch(
            "SELECT user_id, first_name, username, premium, created_at FROM users "
            "ORDER BY created_at DESC LIMIT $1 OFFSET $2",
            limit, offset,
        )
        return [dict(r) for r in rows]

    # ---------- محدودیت پیام (فقط پیام‌های موفق شمرده می‌شوند) ----------

    async def check_limit(self, chat_id: int, user_id: int) -> tuple[bool, int, int, int, int]:
        """فقط بررسی می‌کند، چیزی ثبت نمی‌کند. خروجی: (allowed, used_today, limit_today, used_4h, limit_4h)."""
        premium = await self.is_premium(user_id)
        daily_limit = PREMIUM_DAILY_LIMIT if premium else DAILY_LIMIT
        window_limit = PREMIUM_WINDOW_4H_LIMIT if premium else WINDOW_4H_LIMIT
        used_today, used_4h = await self.get_usage(chat_id, user_id)
        allowed = used_today < daily_limit and used_4h < window_limit
        return allowed, used_today, daily_limit, used_4h, window_limit

    async def log_success(self, chat_id: int, user_id: int) -> None:
        """فقط وقتی صدا زده شود که AI واقعاً پاسخ موفق تولید کرده باشد."""
        await self.pool.execute(
            "INSERT INTO message_log (chat_id, user_id) VALUES ($1, $2)", chat_id, user_id
        )

    async def get_usage(self, chat_id: int, user_id: int) -> tuple[int, int]:
        now = datetime.now(timezone.utc)
        day_ago = now - timedelta(hours=24)
        four_h_ago = now - timedelta(hours=4)
        used_today = await self.pool.fetchval(
            "SELECT count(*) FROM message_log WHERE chat_id=$1 AND user_id=$2 AND created_at > $3",
            chat_id, user_id, day_ago,
        )
        used_4h = await self.pool.fetchval(
            "SELECT count(*) FROM message_log WHERE chat_id=$1 AND user_id=$2 AND created_at > $3",
            chat_id, user_id, four_h_ago,
        )
        return used_today, used_4h

    # ---------- تاریخچه‌ی گفتگو ----------

    async def add_turn(self, chat_id: int, role: str, content: str, author_name: str | None = None) -> None:
        await self.pool.execute(
            "INSERT INTO conversation (chat_id, role, author_name, content) VALUES ($1, $2, $3, $4)",
            chat_id, role, author_name, content,
        )
        await self.pool.execute(
            """
            DELETE FROM conversation
            WHERE id IN (
                SELECT id FROM conversation
                WHERE chat_id=$1
                ORDER BY created_at DESC
                OFFSET $2
            )
            """,
            chat_id, HISTORY_LIMIT,
        )

    async def get_history(self, chat_id: int, limit: int = HISTORY_LIMIT) -> list[dict]:
        rows = await self.pool.fetch(
            "SELECT role, author_name, content FROM conversation WHERE chat_id=$1 ORDER BY created_at DESC LIMIT $2",
            chat_id, limit,
        )
        return [dict(r) for r in reversed(rows)]

    async def clear_history(self, chat_id: int) -> None:
        await self.pool.execute("DELETE FROM conversation WHERE chat_id=$1", chat_id)

    # ---------- Cooldown حضور خودکار در گروه ----------

    async def ambient_cooldown_ok(self, chat_id: int) -> bool:
        row = await self.pool.fetchrow(
            "SELECT last_ambient_reply FROM group_cooldown WHERE chat_id=$1", chat_id
        )
        if not row or not row["last_ambient_reply"]:
            return True
        elapsed = (datetime.now(timezone.utc) - row["last_ambient_reply"]).total_seconds()
        return elapsed >= GROUP_COOLDOWN_SECONDS

    async def mark_ambient_reply(self, chat_id: int) -> None:
        await self.pool.execute(
            """
            INSERT INTO group_cooldown (chat_id, last_ambient_reply)
            VALUES ($1, now())
            ON CONFLICT (chat_id) DO UPDATE SET last_ambient_reply = now()
            """,
            chat_id,
        )
