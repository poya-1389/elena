"""
database.py — لایه‌ی دیتابیس النا (Elena)

از PostgreSQL روی Railway استفاده می‌کند (asyncpg، بدون ORM سنگین).

سیستم اشتراک سه‌سطحی:
    free   : رایگان — محدودیت معمولی، حافظه‌ی کمتر، فقط حالت "default"
    pro    : محدودیت بالاتر، حافظه‌ی بهتر، دسترسی به حالت "warm" هم
    promax : همون محدودیت pro، حافظه‌ی عالی، دسترسی به حالت "custom" هم

تعیین سطح فقط دست ادمینه (از پنل /admin). انتخاب "حالت" (mode) بین
حالت‌های مجاز همون سطح، دست خود کاربره (از /premium).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import asyncpg

logger = logging.getLogger("elena.database")

TIERS = ("free", "pro", "promax")
MODES_BY_TIER = {
    "free": ("default",),
    "pro": ("default", "warm"),
    "promax": ("default", "warm", "custom"),
}

# (سقف روزانه, سقف 4 ساعته, تعداد پیام‌های اخیر در Context)
TIER_LIMITS = {
    "free": (75, 30, 20),
    "pro": (3000, 500, 50),
    "promax": (3000, 500, 80),
}

GROUP_COOLDOWN_SECONDS = 240  # فاصله‌ی حداقلی بین دو ورود خودکار النا به بحث یک گروه

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id     BIGINT PRIMARY KEY,
    first_name  TEXT,
    username    TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    tier        TEXT NOT NULL DEFAULT 'free',
    mode        TEXT NOT NULL DEFAULT 'default'
);
ALTER TABLE users ADD COLUMN IF NOT EXISTS tier TEXT NOT NULL DEFAULT 'free';
ALTER TABLE users ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'default';

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
    id              BIGSERIAL PRIMARY KEY,
    chat_id         BIGINT NOT NULL,
    role            TEXT NOT NULL,          -- 'user' | 'model'
    author_name     TEXT,
    author_username TEXT,
    content         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE conversation ADD COLUMN IF NOT EXISTS author_username TEXT;
CREATE INDEX IF NOT EXISTS idx_conversation_chat
    ON conversation (chat_id, created_at);

CREATE TABLE IF NOT EXISTS group_cooldown (
    chat_id             BIGINT PRIMARY KEY,
    last_ambient_reply  TIMESTAMPTZ
);
"""

# بزرگ‌ترین HISTORY_LIMIT بین تمام سطوح — برای اینکه add_turn هیچ‌وقت پیامی
# رو زودتر از لازم برای بالاترین سطح پاک نکنه (هر کاربر با limit خودش از
# get_history می‌خونه؛ این فقط سقف نگه‌داریِ خودِ جدول رو تعیین می‌کنه)
_MAX_HISTORY_KEEP = max(v[2] for v in TIER_LIMITS.values())


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

    async def get_user(self, user_id: int) -> dict | None:
        row = await self.pool.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)
        return dict(row) if row else None

    async def get_tier(self, user_id: int) -> str:
        row = await self.pool.fetchrow("SELECT tier FROM users WHERE user_id=$1", user_id)
        return row["tier"] if row else "free"

    async def get_mode(self, user_id: int) -> str:
        row = await self.pool.fetchrow("SELECT tier, mode FROM users WHERE user_id=$1", user_id)
        if not row:
            return "default"
        allowed = MODES_BY_TIER.get(row["tier"], ("default",))
        return row["mode"] if row["mode"] in allowed else "default"

    async def set_tier(self, user_id: int, tier: str) -> None:
        assert tier in TIERS
        # اگه سطح جدید دیگه از حالت فعلی پشتیبانی نمی‌کنه (مثلاً از promax به free)، ریست به default
        allowed = MODES_BY_TIER[tier]
        await self.pool.execute(
            """
            UPDATE users SET tier=$2,
                mode = CASE WHEN mode = ANY($3::text[]) THEN mode ELSE 'default' END
            WHERE user_id=$1
            """,
            user_id, tier, list(allowed),
        )

    async def set_mode(self, user_id: int, mode: str) -> bool:
        """اگه حالت برای سطح فعلی کاربر مجاز باشه ست می‌کنه و True برمی‌گردونه."""
        tier = await self.get_tier(user_id)
        if mode not in MODES_BY_TIER.get(tier, ("default",)):
            return False
        await self.pool.execute("UPDATE users SET mode=$2 WHERE user_id=$1", user_id, mode)
        return True

    async def count_users(self) -> int:
        return await self.pool.fetchval("SELECT count(*) FROM users")

    async def count_by_tier(self) -> dict[str, int]:
        rows = await self.pool.fetch("SELECT tier, count(*) c FROM users GROUP BY tier")
        result = {t: 0 for t in TIERS}
        for r in rows:
            result[r["tier"]] = r["c"]
        return result

    async def list_users(self, limit: int, offset: int) -> list[dict]:
        rows = await self.pool.fetch(
            "SELECT user_id, first_name, username, tier, created_at FROM users "
            "ORDER BY created_at DESC LIMIT $1 OFFSET $2",
            limit, offset,
        )
        return [dict(r) for r in rows]

    # ---------- محدودیت پیام (فقط پیام‌های موفق شمرده می‌شوند) ----------

    async def get_tier_limits(self, user_id: int) -> tuple[int, int, int]:
        tier = await self.get_tier(user_id)
        return TIER_LIMITS[tier]

    async def check_limit(self, chat_id: int, user_id: int) -> tuple[bool, int, int, int, int, int]:
        """فقط بررسی می‌کند. خروجی: (allowed, used_today, limit_today, used_4h, limit_4h, history_limit)."""
        daily_limit, window_limit, history_limit = await self.get_tier_limits(user_id)
        used_today, used_4h = await self.get_usage(chat_id, user_id)
        allowed = used_today < daily_limit and used_4h < window_limit
        return allowed, used_today, daily_limit, used_4h, window_limit, history_limit

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

    async def add_turn(
        self, chat_id: int, role: str, content: str,
        author_name: str | None = None, author_username: str | None = None,
    ) -> None:
        await self.pool.execute(
            "INSERT INTO conversation (chat_id, role, author_name, author_username, content) "
            "VALUES ($1, $2, $3, $4, $5)",
            chat_id, role, author_name, author_username, content,
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
            chat_id, _MAX_HISTORY_KEEP,
        )

    async def get_history(self, chat_id: int, limit: int) -> list[dict]:
        rows = await self.pool.fetch(
            "SELECT role, author_name, author_username, content FROM conversation "
            "WHERE chat_id=$1 ORDER BY created_at DESC LIMIT $2",
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
