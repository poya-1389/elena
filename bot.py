"""
bot.py — نقطه‌ی ورود ربات النا (Elena)

Stack: Python 3.12+ / aiogram >= 3.31 (Bot API 10.3) / PostgreSQL (Railway) / Gemini 3.5 Flash-Lite

تصمیم Polling در برابر Webhook: توضیح در نسخه‌های قبلی این فایل — همچنان Polling.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
from collections import deque

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatAction, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommandScopeChat,
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ReactionTypeEmoji,
    Update,
)

import ai
from database import Database, TIERS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("elena.bot")

AMBIENT_PROBABILITY = 0.3
AMBIENT_KEYWORDS = ("نظرت", "به نظر", "نظر شما", "نظرتون", "فکر می‌کنی", "فکر میکنی", "نظرتو")
EDIT_THROTTLE_CHARS = 48
TYPING_REFRESH_SECONDS = 4
ADMIN_PAGE_SIZE = 6

TIER_LABELS = {"free": "معمولی (رایگان)", "pro": "Pro", "promax": "Pro Max"}
TIER_BADGES = {"free": "F", "pro": "P", "promax": "PM"}
MODE_LABELS = {"default": "پیش‌فرض", "warm": "نزدیک‌تر", "custom": "ویژه (Pro Max)"}

REACT_RE = re.compile(r"^\s*REACT:\s*(\S+)\s*\n?")
GROUP_JOIN_TEXT = (
    "سلام من النا - Elena هستم. اینجا می‌تونی با صدازدن، ریپلای و منشن کردن بهم پیام بدی.\n\n"
    "من پیام‌های صوتی، عکس‌ها و فایل‌های PDF رو می‌فهمم. درباره هر چیزی که می‌خوای بنویس."
)

db: Database
bot_id: int = 0
bot_username: str = ""
admin_id: int | None = None

# ---------------------------------------------------------------------------
# محافظ در برابر پردازش دوبرابر یک Update
# ---------------------------------------------------------------------------
_MAX_SEEN_UPDATES = 2000
_seen_update_ids: deque[int] = deque()
_seen_update_ids_set: set[int] = set()


def _mark_seen(update_id: int) -> bool:
    if update_id in _seen_update_ids_set:
        return False
    _seen_update_ids.append(update_id)
    _seen_update_ids_set.add(update_id)
    if len(_seen_update_ids) > _MAX_SEEN_UPDATES:
        old = _seen_update_ids.popleft()
        _seen_update_ids_set.discard(old)
    return True


async def dedup_outer_middleware(handler, event: Update, data: dict):
    if not _mark_seen(event.update_id):
        logger.warning("Duplicate update_id=%s ignored", event.update_id)
        return None
    return await handler(event, data)


def is_admin(user_id: int | None) -> bool:
    return admin_id is not None and user_id == admin_id


def group_button_kb() -> InlineKeyboardMarkup:
    url = f"https://t.me/{bot_username}?startgroup=true"
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="+ افزودن به گروه", url=url, style="primary")]]
    )


# ---------------------------------------------------------------------------
# تبدیل خروجی Markdown-ایِ مدل به HTML موردقبول تلگرام
# ---------------------------------------------------------------------------
def markdown_to_telegram_html(text: str) -> str:
    out = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    out = re.sub(r"```(\w*)\n(.*?)```", lambda m: f"<pre><code>{m.group(2)}</code></pre>", out, flags=re.DOTALL)
    out = re.sub(r"`([^`\n]+)`", r"<code>\1</code>", out)
    out = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", out)
    out = re.sub(r"(?<!\*)\*(?!\*)(.+?)\*(?!\*)", r"<i>\1</i>", out)
    out = re.sub(r"__(.+?)__", r"<u>\1</u>", out)
    out = re.sub(r"~~(.+?)~~", r"<s>\1</s>", out)
    out = re.sub(r"\|\|(.+?)\|\|", r"<tg-spoiler>\1</tg-spoiler>", out)
    out = re.sub(r"(?m)^&gt;\s?(.+)$", r"<blockquote>\1</blockquote>", out)
    return out


async def download_bytes(bot: Bot, file_id: str) -> bytes:
    tg_file = await bot.get_file(file_id)
    buf = await bot.download_file(tg_file.file_path)
    return buf.read()


def is_direct_mode(message: Message) -> bool:
    if message.chat.type == "private":
        return True
    text = message.text or message.caption or ""
    if message.reply_to_message and message.reply_to_message.from_user and (
        message.reply_to_message.from_user.id == bot_id
    ):
        return True
    if bot_username and f"@{bot_username.lower()}" in text.lower():
        return True
    if ai.message_calls_elena(text):
        return True
    return False


def is_ambient_worthy(text: str) -> bool:
    t = (text or "").strip()
    if len(t) < 8:
        return False
    if t.endswith("?") or t.endswith("؟"):
        return True
    return any(k in t for k in AMBIENT_KEYWORDS)


def author_info(message: Message) -> tuple[str | None, str | None]:
    """برمی‌گردونه (author_name, author_username) — فقط برای گروه‌ها؛ تو پیوی None."""
    if message.chat.type == "private":
        return None, None
    user = message.from_user
    name = user.first_name or (user.username or "کاربر")
    if user.is_bot:
        name = f"{name} [ربات]"
    return name, user.username


async def _typing_loop(bot: Bot, chat_id: int, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            await bot.send_chat_action(chat_id, ChatAction.TYPING)
        except Exception:  # noqa: BLE001
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=TYPING_REFRESH_SECONDS)
        except asyncio.TimeoutError:
            pass


async def _try_react(message: Message, emoji: str) -> None:
    try:
        await message.bot.set_message_reaction(
            chat_id=message.chat.id,
            message_id=message.message_id,
            reaction=[ReactionTypeEmoji(type="emoji", emoji=emoji)],
        )
    except (TelegramBadRequest, TelegramForbiddenError) as exc:
        logger.info("Reaction %r rejected: %s", emoji, exc)


async def stream_reply(
    message: Message, user_text: str, media: list[tuple[bytes, str]] | None = None, is_ambient: bool = False
) -> None:
    chat_id = message.chat.id
    user = message.from_user
    is_group = message.chat.type != "private"
    author_name, author_username = author_info(message)

    allowed, used_today, limit_today, used_4h, limit_4h, history_limit = await db.check_limit(chat_id, user.id)
    if not allowed:
        if is_group:
            return  # در گروه بی‌سروصدا رد شو
        await message.answer(
            "امروز به سقف پیام‌هات رسیدی. یکم بعد دوباره بیا 🙂\n"
            f"({used_today}/{limit_today} امروز — {used_4h}/{limit_4h} این ۴ ساعت اخیر)"
        )
        return

    await db.upsert_user(user.id, user.first_name, user.username)
    history = await db.get_history(chat_id, limit=history_limit)
    mode = await db.get_mode(user.id)

    stop_typing = asyncio.Event()
    typing_task = asyncio.create_task(_typing_loop(message.bot, chat_id, stop_typing))

    buffer = ""
    last_edit_len = 0
    had_error = False
    quota_exhausted = False
    placeholder: Message | None = None
    reaction_checked = False
    reaction_applied = False

    send = message.reply if is_group else message.answer

    try:
        async for chunk in ai.generate_reply_stream(
            history=history,
            user_text=user_text,
            author_name=author_name,
            author_username=author_username,
            chat_kind="group" if is_group else "private",
            mode=mode,
        ):
            if chunk == "__ELENA_AI_QUOTA__":
                quota_exhausted = True
                break
            if chunk == "__ELENA_AI_ERROR__":
                had_error = True
                break
            buffer += chunk

            if not reaction_checked:
                m = REACT_RE.match(buffer)
                if m:
                    await _try_react(message, m.group(1))
                    reaction_applied = True
                    buffer = buffer[m.end():]
                    reaction_checked = True
                elif len(buffer) > 24 or "\n" in buffer:
                    reaction_checked = True

            if not reaction_checked:
                continue
            if not buffer.strip():
                continue

            if placeholder is None:
                placeholder = await send("…")
            elif len(buffer) - last_edit_len >= EDIT_THROTTLE_CHARS:
                try:
                    await placeholder.edit_text(buffer)
                    last_edit_len = len(buffer)
                except TelegramRetryAfter as e:
                    await asyncio.sleep(e.retry_after)
                except TelegramBadRequest:
                    pass
    finally:
        stop_typing.set()
        typing_task.cancel()

    if quota_exhausted:
        if not is_group:
            msg = "الان ظرفیت رایگانم برای امروز پر شده. چند دقیقه دیگه یا فردا دوباره امتحان کن."
            if placeholder is not None:
                try:
                    await placeholder.edit_text(msg)
                except TelegramBadRequest:
                    pass
            else:
                await send(msg)
        elif placeholder is not None:
            try:
                await placeholder.delete()
            except TelegramBadRequest:
                pass
        return

    if had_error:
        if placeholder is not None:
            try:
                await placeholder.edit_text("یه مشکلی پیش اومد. یه‌بار دیگه امتحان کن.")
            except TelegramBadRequest:
                pass
        else:
            await send("یه مشکلی پیش اومد. یه‌بار دیگه امتحان کن.")
        return

    if not buffer.strip():
        if not reaction_applied and not is_ambient:
            # پاسخ کاملاً خالی بود و هیچ ری‌اکشنی هم نزدیم — این یعنی مدل رفتار
            # درستی نداشته؛ تو یه پیام مستقیم هیچ‌وقت نباید کاربر رو ساکت بذاریم
            logger.warning("Empty AI response with no reaction for chat_id=%s", chat_id)
            fallback = "چیزی نگفتم؟ یه‌بار دیگه بگو."
            if placeholder is not None:
                try:
                    await placeholder.edit_text(fallback)
                except TelegramBadRequest:
                    pass
            else:
                await send(fallback)
            return
        if placeholder is not None:
            try:
                await placeholder.delete()
            except TelegramBadRequest:
                pass
        await db.add_turn(chat_id, "user", user_text, author_name=author_name, author_username=author_username)
        await db.log_success(chat_id, user.id)
        if is_group:
            await db.mark_ambient_reply(chat_id)
        return

    final_html = markdown_to_telegram_html(buffer)
    try:
        if placeholder is not None:
            await placeholder.edit_text(final_html, parse_mode=ParseMode.HTML)
        else:
            placeholder = await send(final_html, parse_mode=ParseMode.HTML)
    except TelegramBadRequest:
        try:
            if placeholder is not None:
                await placeholder.edit_text(buffer)
            else:
                await send(buffer)
        except TelegramBadRequest:
            pass

    await db.add_turn(chat_id, "user", user_text, author_name=author_name, author_username=author_username)
    await db.add_turn(chat_id, "model", buffer)
    await db.log_success(chat_id, user.id)

    if is_group:
        await db.mark_ambient_reply(chat_id)


# ---------------------------------------------------------------------------
# /premium — وضعیت اشتراک + انتخاب حالت (فقط برای مشترک‌ها)
# ---------------------------------------------------------------------------
async def _render_premium(user_id: int) -> tuple[str, InlineKeyboardMarkup | None]:
    tier = await db.get_tier(user_id)
    mode = await db.get_mode(user_id)
    daily, window, _ = await db.get_tier_limits(user_id)

    lines = [
        f"وضعیت اشتراک: <b>{TIER_LABELS[tier]}</b>",
        f"محدودیت پیام: {window} در ۴ ساعت اخیر ({daily} در روز)",
    ]

    if tier == "free":
        lines.append("\nفعلاً فروش Pro و Pro Max در دسترس نیست.")
        return "\n".join(lines), None

    lines.append("\nحالت رفتاری النا با خودت رو انتخاب کن:")
    rows = []
    for m in ("default", "warm", "custom"):
        label = MODE_LABELS[m] + (" ✓" if m == mode else "")
        rows.append([InlineKeyboardButton(text=label, callback_data=f"pmset:{m}", style="primary")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)


# ---------------------------------------------------------------------------
# پنل ادمین
# ---------------------------------------------------------------------------
def _admin_list_kb(users: list[dict], page: int, total: int) -> InlineKeyboardMarkup:
    rows = []
    for u in users:
        label = f"[{TIER_BADGES[u['tier']]}] {u['first_name'] or u['user_id']}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"adm_u:{u['user_id']}:{page}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ قبلی", callback_data=f"adm_p:{page-1}"))
    if (page + 1) * ADMIN_PAGE_SIZE < total:
        nav.append(InlineKeyboardButton(text="بعدی ▶️", callback_data=f"adm_p:{page+1}"))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_admin_list(page: int) -> tuple[str, InlineKeyboardMarkup]:
    total = await db.count_users()
    by_tier = await db.count_by_tier()
    users = await db.list_users(ADMIN_PAGE_SIZE, page * ADMIN_PAGE_SIZE)
    text = (
        "🛠 <b>پنل مدیریت Elena</b>\n\n"
        f"کل کاربران: <b>{total}</b>\n"
        f"Free: {by_tier['free']} — Pro: {by_tier['pro']} — Pro Max: {by_tier['promax']}\n\n"
        f"صفحه‌ی {page + 1} — روی هر کاربر بزن برای مدیریت:"
    )
    return text, _admin_list_kb(users, page, total)


async def _render_admin_user(user_id: int, page: int) -> tuple[str, InlineKeyboardMarkup]:
    u = await db.get_user(user_id)
    if not u:
        return "کاربر پیدا نشد.", _admin_list_kb([], page, 0)
    text = (
        f"👤 <b>{u['first_name'] or '—'}</b>\n"
        + (f"یوزرنیم: @{u['username']}\n" if u["username"] else "")
        + f"آیدی: <code>{u['user_id']}</code>\n"
        + f"عضویت از: {u['created_at'].strftime('%Y-%m-%d')}\n"
        + f"سطح فعلی: <b>{TIER_LABELS[u['tier']]}</b>"
    )
    tier_buttons = [
        InlineKeyboardButton(
            text=("✓ " if u["tier"] == t else "") + TIER_LABELS[t],
            callback_data=f"adm_st:{user_id}:{t}:{page}",
        )
        for t in TIERS
    ]
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            tier_buttons,
            [InlineKeyboardButton(text="◀️ بازگشت به لیست", callback_data=f"adm_p:{page}")],
        ]
    )
    return text, kb


def register_handlers(dp: Dispatcher) -> None:

    @dp.message(CommandStart())
    async def cmd_start(message: Message) -> None:
        await db.upsert_user(message.from_user.id, message.from_user.first_name, message.from_user.username)
        if message.chat.type == "private":
            name = message.from_user.first_name
            text = (
                f"سلام، {name}\n\n"
                "من النا - Elena هستم. می‌تونی اینجا بهم پیام بدی یا من رو به یک گروه اضافه کنی.\n\n"
                "من پیام‌های صوتی، عکس‌ها و فایل‌های PDF رو می‌فهمم. درباره هر چیزی که می‌خوای بنویس."
            )
        else:
            text = GROUP_JOIN_TEXT
        await message.answer(text, reply_markup=group_button_kb())

    @dp.my_chat_member()
    async def on_bot_membership_changed(event: ChatMemberUpdated) -> None:
        if event.chat.type not in ("group", "supergroup"):
            return
        old_status = event.old_chat_member.status
        new_status = event.new_chat_member.status
        if old_status in ("left", "kicked") and new_status in ("member", "administrator"):
            try:
                await event.bot.send_message(event.chat.id, GROUP_JOIN_TEXT)
            except (TelegramBadRequest, TelegramForbiddenError):
                pass

    @dp.message(Command("stats"))
    async def cmd_stats(message: Message) -> None:
        _, used_today, limit_today, used_4h, limit_4h, _ = await db.check_limit(message.chat.id, message.from_user.id)
        text = f"این چت:\n- امروز: {used_today} از {limit_today}\n- این ۴ ساعت اخیر: {used_4h} از {limit_4h}"
        await message.answer(text)

    @dp.message(Command("premium"))
    async def cmd_premium(message: Message) -> None:
        await db.upsert_user(message.from_user.id, message.from_user.first_name, message.from_user.username)
        text, kb = await _render_premium(message.from_user.id)
        await message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)

    @dp.callback_query(F.data.startswith("pmset:"))
    async def cb_premium_mode(call: CallbackQuery) -> None:
        mode = call.data.split(":", 1)[1]
        ok = await db.set_mode(call.from_user.id, mode)
        if not ok:
            await call.answer("این حالت برای سطح اشتراک فعلیت باز نیست.", show_alert=True)
            return
        text, kb = await _render_premium(call.from_user.id)
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except TelegramBadRequest:
            pass
        await call.answer("حالت عوض شد ✓")

    @dp.message(Command("admin"))
    async def cmd_admin(message: Message) -> None:
        if not is_admin(message.from_user.id):
            return  # برای بقیه کاملاً بی‌صداست
        text, kb = await _render_admin_list(0)
        await message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)

    @dp.callback_query(F.data.startswith("adm_"))
    async def cb_admin(call: CallbackQuery) -> None:
        if not is_admin(call.from_user.id):
            await call.answer()
            return
        action, _, rest = call.data.partition(":")
        if action == "adm_p":
            page = int(rest)
            text, kb = await _render_admin_list(page)
        elif action == "adm_u":
            uid_str, _, page_str = rest.partition(":")
            text, kb = await _render_admin_user(int(uid_str), int(page_str))
        elif action == "adm_st":
            uid_str, tier, page_str = rest.split(":")
            await db.set_tier(int(uid_str), tier)
            text, kb = await _render_admin_user(int(uid_str), int(page_str))
        else:
            await call.answer()
            return
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except TelegramBadRequest:
            pass
        await call.answer()

    @dp.message(F.voice)
    async def on_voice(message: Message) -> None:
        if message.chat.type != "private" and not is_direct_mode(message):
            return
        data = await download_bytes(message.bot, message.voice.file_id)
        await stream_reply(message, message.caption or "به این پیام صوتی پاسخ بده.", media=[(data, "audio/ogg")])

    @dp.message(F.photo)
    async def on_photo(message: Message) -> None:
        if message.chat.type != "private" and not is_direct_mode(message):
            return
        data = await download_bytes(message.bot, message.photo[-1].file_id)
        await stream_reply(message, message.caption or "این عکس رو ببین و نظرت رو بگو.", media=[(data, "image/jpeg")])

    @dp.message(F.document)
    async def on_document(message: Message) -> None:
        if message.chat.type != "private" and not is_direct_mode(message):
            return
        doc = message.document
        if doc.mime_type != "application/pdf":
            if message.chat.type == "private":
                await message.answer("فعلاً فقط PDF رو می‌فهمم، نه این نوع فایل رو.")
            return
        data = await download_bytes(message.bot, doc.file_id)
        await stream_reply(
            message, message.caption or "این PDF رو بخون و خلاصه‌اش کن.", media=[(data, "application/pdf")]
        )

    @dp.message(F.text)
    async def on_text(message: Message) -> None:
        text = message.text
        chat_type = message.chat.type

        if chat_type == "private":
            await stream_reply(message, text)
            return

        if is_direct_mode(message):
            await stream_reply(message, text)
            return

        author_name, author_username = author_info(message)
        await db.add_turn(message.chat.id, "user", text, author_name=author_name, author_username=author_username)

        if not is_ambient_worthy(text):
            return
        if not await db.ambient_cooldown_ok(message.chat.id):
            return
        if random.random() > AMBIENT_PROBABILITY:
            return

        await db.pool.execute(
            """
            DELETE FROM conversation WHERE id = (
                SELECT id FROM conversation WHERE chat_id=$1 ORDER BY created_at DESC LIMIT 1
            )
            """,
            message.chat.id,
        )
        await stream_reply(message, text, is_ambient=True)


async def main() -> None:
    global db, bot_id, bot_username, admin_id

    bot_token = os.environ["BOT_TOKEN"]
    database_url = os.environ["DATABASE_URL"]
    admin_id_raw = os.environ.get("ADMIN_ID")
    admin_id = int(admin_id_raw) if admin_id_raw else None

    ai.init_client()
    db = Database(database_url)
    await db.connect()

    bot = Bot(token=bot_token, default=DefaultBotProperties(parse_mode=None))
    await bot.delete_webhook(drop_pending_updates=True)
    me = await bot.get_me()
    bot_id, bot_username = me.id, (me.username or "")

    await bot.set_my_commands(
        [
            {"command": "start", "description": "Start — Restart Elna"},
            {"command": "stats", "description": "Consumption status"},
            {"command": "premium", "description": "Upgrade subscription"},
        ]
    )
    if admin_id:
        await bot.set_my_commands(
            [
                {"command": "start", "description": "Start — Restart Eelna"},
                {"command": "stats", "description": "Consumption status"},
                {"command": "premium", "description": "Upgrade subscription"},
                {"command": "admin", "description": "ADMIN PANEL"},
            ],
            scope=BotCommandScopeChat(chat_id=admin_id),
        )

    dp = Dispatcher()
    dp.update.outer_middleware(dedup_outer_middleware)
    register_handlers(dp)

    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query", "my_chat_member"])
    finally:
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
