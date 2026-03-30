import os
import re
import sqlite3
import logging
import asyncio
from datetime import datetime, timedelta
from anthropic import Anthropic
from telegram import Update
from telegram.ext import (
    Application, MessageHandler, filters,
    ContextTypes, CommandHandler
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── ENV ───────────────────────────────────────────────────────────────────────
BOT_TOKEN        = os.environ.get("BOT_TOKEN")
SOURCE_CHAT_ID   = int(os.environ.get("SOURCE_CHAT_ID", "0"))   # канал с трейдами
REPORT_CHAT_ID   = int(os.environ.get("REPORT_CHAT_ID", "0"))   # куда слать отчёты
DIALOG_CHAT_ID   = int(os.environ.get("DIALOG_CHAT_ID", "0"))   # чат для AI-диалога
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
BOT_USERNAME     = os.environ.get("BOT_USERNAME", "")           # без @, напр. mybot

DB_PATH = "trades.db"
anthropic_client = Anthropic(api_key=ANTHROPIC_API_KEY)

# ─── Хранилище истории диалогов: {user_id: [{"role":..,"content":..}, ...]} ──
conversation_history: dict[int, list] = {}
MAX_HISTORY = 20  # максимум сообщений на пользователя

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp   TEXT,
        trader      TEXT,
        exchange    TEXT,
        side        TEXT,
        coin        TEXT,
        profit_usd  REAL,
        profit_pct  REAL,
        is_profit   INTEGER,
        raw_message TEXT
    )''')
    conn.commit()
    conn.close()

# ═══════════════════════════════════════════════════════════════════════════════
# PARSE & SAVE TRADE
# ═══════════════════════════════════════════════════════════════════════════════

def parse_trade(text: str) -> dict | None:
    try:
        is_profit = 1 if '✅' in text else (0 if '❌' in text else None)

        trader_match = re.search(
            r'[✅❌]?\s*([A-Za-zА-Яа-я0-9\-]+)\(([^)]+)\)', text
        )
        if trader_match:
            trader   = trader_match.group(1).strip()
            exchange = trader_match.group(2).strip()
        else:
            m        = re.search(r'[✅❌]?\s*([A-Za-zА-Яа-я0-9\-]+),', text)
            trader   = m.group(1).strip() if m else "Unknown"
            exchange = "Unknown"

        side_match = re.search(r'\b(BUY|SELL)\b', text, re.IGNORECASE)
        side       = side_match.group(1).upper() if side_match else "Unknown"

        pnl_match  = re.search(r'(?:Profit|Loss)\s*([+-]?\d+\.?\d*)\$', text, re.IGNORECASE)
        profit_usd = float(pnl_match.group(1)) if pnl_match else None

        pct_match  = re.search(r'\$\s*\(([+-]?\d+\.?\d*)%\)', text)
        profit_pct = float(pct_match.group(1)) if pct_match else None

        coin_match = re.search(r'#([A-Z0-9]+)', text)
        coin       = coin_match.group(1) if coin_match else None

        if not coin or profit_usd is None:
            return None
        if is_profit is None:
            is_profit = 1 if profit_usd >= 0 else 0

        return {
            "trader": trader, "exchange": exchange, "side": side,
            "coin": coin, "profit_usd": profit_usd, "profit_pct": profit_pct,
            "is_profit": is_profit, "raw_message": text[:500]
        }
    except Exception as e:
        logger.error(f"parse_trade error: {e}")
        return None


def save_trade(trade: dict):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        '''INSERT INTO trades
           (timestamp,trader,exchange,side,coin,profit_usd,profit_pct,is_profit,raw_message)
           VALUES (?,?,?,?,?,?,?,?,?)''',
        (
            datetime.now().isoformat(),
            trade["trader"], trade["exchange"], trade["side"],
            trade["coin"],   trade["profit_usd"], trade["profit_pct"],
            trade["is_profit"], trade["raw_message"]
        )
    )
    conn.commit()
    conn.close()

# ═══════════════════════════════════════════════════════════════════════════════
# REPORTS
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_report_data(days: int = 7) -> dict:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    since = (datetime.now() - timedelta(days=days)).isoformat()

    c.execute(
        'SELECT COUNT(*),SUM(profit_usd),SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END) '
        'FROM trades WHERE timestamp>?', (since,)
    )
    total, total_pnl, wins = c.fetchone()
    total = total or 0; total_pnl = total_pnl or 0.0; wins = wins or 0

    c.execute(
        'SELECT coin,SUM(profit_usd),COUNT(*) FROM trades WHERE timestamp>? '
        'GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 5', (since,)
    )
    top = c.fetchall()

    c.execute(
        'SELECT coin,SUM(profit_usd),COUNT(*) FROM trades WHERE timestamp>? '
        'GROUP BY coin ORDER BY SUM(profit_usd) ASC LIMIT 5', (since,)
    )
    worst = c.fetchall()

    c.execute(
        'SELECT trader,COUNT(*),SUM(profit_usd),SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END) '
        'FROM trades WHERE timestamp>? GROUP BY trader ORDER BY SUM(profit_usd) DESC', (since,)
    )
    traders = c.fetchall()
    conn.close()

    return dict(total=total, total_pnl=total_pnl, wins=wins,
                top=top, worst=worst, traders=traders, days=days)


def build_report(days: int = 7) -> str:
    d = _fetch_report_data(days)
    total, total_pnl, wins = d["total"], d["total_pnl"], d["wins"]
    wr   = round(wins / total * 100 if total > 0 else 0, 1)
    now  = datetime.now()
    sign = '+' if total_pnl >= 0 else ''

    label = "день" if days == 1 else f"{days} дней"
    r  = f"📊 *Отчёт за {label}* ({(now-timedelta(days=days)).strftime('%d.%m')}–{now.strftime('%d.%m')})\n\n"
    r += f"📈 Сделок: *{total}* | PnL: *{sign}{total_pnl:.2f}$* | WR: *{wr}%*\n\n"

    r += "🏆 *Топ-5 монет:*\n"
    for i, (coin, pnl, cnt) in enumerate(d["top"], 1):
        r += f"{i}. #{coin} {'+' if pnl>=0 else ''}{pnl:.2f}$ ({cnt} сд.)\n"

    r += "\n💀 *Худшие 5 монет:*\n"
    for i, (coin, pnl, cnt) in enumerate(d["worst"], 1):
        r += f"{i}. #{coin} {'+' if pnl>=0 else ''}{pnl:.2f}$ ({cnt} сд.)\n"

    r += "\n👤 *По трейдерам:*\n"
    for trader, cnt, pnl, w in d["traders"]:
        wr2  = round(w / cnt * 100 if cnt > 0 else 0, 1)
        icon = '🟢' if pnl >= 0 else '🔴'
        r += f"{icon} *{trader}*: {'+' if pnl>=0 else ''}{pnl:.2f}$ | {cnt} сд. | WR {wr2}%\n"

    return r


def build_top_report(limit: int = 10) -> str:
    """Топ трейдеров за всё время."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        'SELECT trader, COUNT(*), SUM(profit_usd), '
        'SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END) '
        'FROM trades GROUP BY trader ORDER BY SUM(profit_usd) DESC LIMIT ?',
        (limit,)
    )
    rows = c.fetchall()
    conn.close()

    if not rows:
        return "📭 База пустая — трейдов ещё нет."

    r = f"🥇 *Топ-{limit} трейдеров (за всё время)*\n\n"
    medals = ["🥇","🥈","🥉"] + ["🔹"] * (limit - 3)
    for i, (trader, cnt, pnl, w) in enumerate(rows):
        wr  = round(w / cnt * 100 if cnt > 0 else 0, 1)
        sgn = '+' if pnl >= 0 else ''
        r += f"{medals[i]} *{trader}*: {sgn}{pnl:.2f}$ | {cnt} сд. | WR {wr}%\n"
    return r

# ═══════════════════════════════════════════════════════════════════════════════
# CLAUDE AI DIALOG
# ═══════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """Ты — торговый ассистент бота, который анализирует крипто-трейды.
У тебя есть доступ к статистике трейдеров. Отвечай коротко, по делу, на том языке,
на котором тебя спрашивают. Используй эмодзи умеренно."""


def get_db_context() -> str:
    """Краткая сводка из БД для контекста Claude."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT COUNT(*), SUM(profit_usd) FROM trades')
    total, pnl = c.fetchone()
    c.execute(
        'SELECT trader, SUM(profit_usd) FROM trades '
        'GROUP BY trader ORDER BY SUM(profit_usd) DESC LIMIT 5'
    )
    top_traders = c.fetchall()
    conn.close()

    ctx = f"[DB] Всего сделок: {total or 0}, суммарный PnL: {(pnl or 0):.2f}$. "
    if top_traders:
        ctx += "Топ трейдеры: " + ", ".join(
            f"{t}={'+' if p>=0 else ''}{p:.2f}$" for t, p in top_traders
        )
    return ctx


async def claude_reply(user_id: int, user_text: str) -> str:
    history = conversation_history.setdefault(user_id, [])

    # Обрезаем историю
    if len(history) >= MAX_HISTORY:
        history[:] = history[-(MAX_HISTORY - 2):]

    history.append({"role": "user", "content": user_text})

    db_ctx = get_db_context()
    system = f"{SYSTEM_PROMPT}\n\nТекущая статистика бота: {db_ctx}"

    try:
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            system=system,
            messages=history
        )
        reply = response.content[0].text
    except Exception as e:
        logger.error(f"Claude API error: {e}")
        reply = "⚠️ Ошибка при обращении к Claude. Попробуй позже."

    history.append({"role": "assistant", "content": reply})
    return reply

# ═══════════════════════════════════════════════════════════════════════════════
# HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

async def handle_trade_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Парсим трейды из SOURCE_CHAT_ID."""
    msg = update.channel_post or update.message
    if not msg or msg.chat.id != SOURCE_CHAT_ID:
        return
    text  = msg.text or ""
    trade = parse_trade(text)
    if trade:
        save_trade(trade)
        logger.info(f"Saved: {trade['trader']} | {trade['coin']} | {trade['profit_usd']}$")


async def handle_dialog_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    AI-диалог в DIALOG_CHAT_ID.
    Бот отвечает ТОЛЬКО на:
      1. Упоминания @BOT_USERNAME
      2. Реплаи на сообщения бота
    """
    msg = update.message
    if not msg or msg.chat.id != DIALOG_CHAT_ID:
        return

    text = msg.text or msg.caption or ""
    bot_id = context.bot.id

    # Проверяем: это реплай на сообщение бота?
    is_reply_to_bot = (
        msg.reply_to_message is not None
        and msg.reply_to_message.from_user is not None
        and msg.reply_to_message.from_user.id == bot_id
    )

    # Проверяем: есть упоминание бота?
    is_mention = False
    if BOT_USERNAME and f"@{BOT_USERNAME}" in text:
        is_mention = True
    if msg.entities:
        for ent in msg.entities:
            if ent.type == "mention":
                mention_text = text[ent.offset: ent.offset + ent.length]
                if mention_text.lstrip("@").lower() == BOT_USERNAME.lower():
                    is_mention = True

    if not is_mention and not is_reply_to_bot:
        return  # игнорируем

    # Убираем упоминание из текста
    clean_text = text.replace(f"@{BOT_USERNAME}", "").strip()
    if not clean_text:
        clean_text = "Привет!"

    user_id = msg.from_user.id
    await msg.chat.send_action("typing")
    reply = await claude_reply(user_id, clean_text)
    await msg.reply_text(reply, parse_mode="Markdown")


# ─── COMMANDS ──────────────────────────────────────────────────────────────────

async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Недельный отчёт."""
    await update.message.reply_text(build_report(7), parse_mode="Markdown")


async def cmd_report_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Дневной отчёт."""
    await update.message.reply_text(build_report(1), parse_mode="Markdown")


async def cmd_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Топ трейдеров за всё время."""
    args  = context.args
    limit = 10
    if args and args[0].isdigit():
        limit = min(int(args[0]), 50)
    await update.message.reply_text(build_top_report(limit), parse_mode="Markdown")


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT COUNT(*), SUM(profit_usd) FROM trades')
    total, pnl = c.fetchone()
    conn.close()
    pnl = pnl or 0
    await update.message.reply_text(
        f"📊 Сделок в базе: *{total}*\n"
        f"💰 Суммарный PnL: *{'+' if pnl>=0 else ''}{pnl:.2f}$*",
        parse_mode="Markdown"
    )


async def cmd_clear_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Очистить историю диалога с Claude."""
    user_id = update.message.from_user.id
    conversation_history.pop(user_id, None)
    await update.message.reply_text("🧹 История диалога очищена.")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📖 *Команды бота:*\n\n"
        "/report — отчёт за 7 дней\n"
        "/report_daily — отчёт за сегодня\n"
        "/top N — топ трейдеров (по умолч. 10)\n"
        "/stats — общая статистика\n"
        "/clear — очистить историю AI-диалога\n"
        "/help — это сообщение\n\n"
        f"💬 Упомяни меня (@{BOT_USERNAME}) или ответь на моё сообщение, "
        "чтобы поговорить с AI."
    )
    await update.message.reply_text(text, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULED TASKS
# ═══════════════════════════════════════════════════════════════════════════════

async def scheduled_reports_loop(app: Application):
    """
    Ежедневный отчёт в 20:00 (каждый день).
    Еженедельный расширенный — воскресенье 20:00.
    """
    sent_daily  = None
    sent_weekly = None

    while True:
        now = datetime.now()
        day_key  = now.date()
        week_key = (now.isocalendar()[1], now.year)  # (week_number, year)

        if now.hour == 20 and now.minute == 0:
            # Ежедневный
            if sent_daily != day_key:
                try:
                    report = build_report(1)
                    await app.bot.send_message(
                        chat_id=REPORT_CHAT_ID, text=report, parse_mode="Markdown"
                    )
                    sent_daily = day_key
                    logger.info("Daily report sent.")
                except Exception as e:
                    logger.error(f"Daily report error: {e}")

            # Еженедельный (воскресенье)
            if now.weekday() == 6 and sent_weekly != week_key:
                try:
                    report = build_report(7)
                    await app.bot.send_message(
                        chat_id=REPORT_CHAT_ID,
                        text="🗓 *НЕДЕЛЬНЫЙ ОТЧЁТ*\n\n" + report,
                        parse_mode="Markdown"
                    )
                    sent_weekly = week_key
                    logger.info("Weekly report sent.")
                except Exception as e:
                    logger.error(f"Weekly report error: {e}")

        await asyncio.sleep(60)

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    # Команды
    app.add_handler(CommandHandler("report",       cmd_report))
    app.add_handler(CommandHandler("report_daily", cmd_report_daily))
    app.add_handler(CommandHandler("top",          cmd_top))
    app.add_handler(CommandHandler("stats",        cmd_stats))
    app.add_handler(CommandHandler("clear",        cmd_clear_history))
    app.add_handler(CommandHandler("help",         cmd_help))
    app.add_handler(CommandHandler("start",        cmd_help))

    # Сообщения из канала трейдов
    app.add_handler(MessageHandler(
        filters.Chat(SOURCE_CHAT_ID) & filters.ALL,
        handle_trade_message
    ))

    # AI-диалог (упоминания и реплаи) в чате
    app.add_handler(MessageHandler(
        filters.Chat(DIALOG_CHAT_ID) & filters.TEXT & ~filters.COMMAND,
        handle_dialog_message
    ))

    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        logger.info("✅ Bot started!")
        await scheduled_reports_loop(app)


if __name__ == "__main__":
    asyncio.run(main())
