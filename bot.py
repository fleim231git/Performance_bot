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

BOT_TOKEN         = os.environ.get("BOT_TOKEN")
SOURCE_CHAT_ID    = int(os.environ.get("SOURCE_CHAT_ID", "0"))
REPORT_CHAT_ID    = int(os.environ.get("REPORT_CHAT_ID", "0"))
DIALOG_CHAT_ID    = int(os.environ.get("DIALOG_CHAT_ID", "0"))
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
BOT_USERNAME      = os.environ.get("BOT_USERNAME", "")

DB_PATH = "/data/trades.db"
anthropic_client = Anthropic(api_key=ANTHROPIC_API_KEY)
conversation_history: dict[int, list] = {}
MAX_HISTORY = 20

# ─── МАППИНГ АББРЕВИАТУР К ИМЕНАМ ТРЕЙДЕРОВ ───────────────────────────────
TRADER_MAP = {
    'A7B': 'Рубен',
    'BOR': 'Борис',
    'D3N': 'Денис',
    'D3S': 'Дэн',
    'D4Y': 'Денис',
    'DMR': 'Дима',
    'GOR': 'Егор',
    'GRI': 'Гриша',
    'K0A': 'Костя',
    'K0L': 'Коля',
    'KRI': 'Кирилл',
    'M7R': 'Мирча',
    'MIK': 'Майк',
    'MKS': 'Максим',
    'MRC': 'Макар',
    'MVD': 'Дмитрий',
    'PSH': 'Паша',
    'R7B': 'Рубик',
    'ROD': 'Родион',
    'S3G': 'Сергей',
    'S3R': 'Саня',
    'V1N': 'Ваня',
    'VIK': 'Виктор',
    'VIT': 'Виктор',
    'VLA': 'Влад',
}

EXCHANGE_MAP = {
    'bnc': 'Binance',
    'bbt': 'Bybit',
    'okx': 'OKX',
    'mnl': 'Manual',
    'ang': 'Binance',
    'dmr': 'Binance',
    'dns': 'Binance',
    'mur': 'Binance',
    'r7b': 'Binance',
    'rub': 'Binance',
    'ser': 'Binance',
}

def normalize_trader(name: str) -> str:
    """Приводит аббревиатуру к имени трейдера."""
    if not name:
        return name
    prefix = name[:3].upper()
    return TRADER_MAP.get(prefix, name)

def normalize_exchange(trader_raw: str, exchange: str) -> str:
    """Определяет биржу из аббревиатуры трейдера или из названия."""
    # Если уже нормальное название — вернуть
    if exchange.upper() in ['BINANCE', 'BYBIT', 'OKX']:
        return exchange.capitalize() if exchange.upper() != 'BYBIT' else 'Bybit'
    # Парсим из аббревиатуры типа GRI-bnc-01
    parts = trader_raw.lower().split('-')
    if len(parts) >= 2:
        exch_code = parts[1]
        return EXCHANGE_MAP.get(exch_code, exchange)
    return exchange



MONTH_NAMES = {
    "январь": 1, "января": 1, "january": 1, "jan": 1,
    "февраль": 2, "февраля": 2, "february": 2, "feb": 2,
    "март": 3, "марта": 3, "march": 3, "mar": 3,
    "апрель": 4, "апреля": 4, "april": 4, "apr": 4,
    "май": 5, "мая": 5, "may": 5,
    "июнь": 6, "июня": 6, "june": 6, "jun": 6,
    "июль": 7, "июля": 7, "july": 7, "jul": 7,
    "август": 8, "августа": 8, "august": 8, "aug": 8,
    "сентябрь": 9, "сентября": 9, "september": 9, "sep": 9,
    "октябрь": 10, "октября": 10, "october": 10, "oct": 10,
    "ноябрь": 11, "ноября": 11, "november": 11, "nov": 11,
    "декабрь": 12, "декабря": 12, "december": 12, "dec": 12,
}

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp    TEXT,
        trader       TEXT,
        exchange     TEXT,
        side         TEXT,
        coin         TEXT,
        distance     REAL,
        buffer       REAL,
        take_profit  REAL,
        profit_usd   REAL,
        profit_pct   REAL,
        is_profit    INTEGER,
        raw_message  TEXT
    )''')
    for col, typ in [("distance","REAL"),("buffer","REAL"),("take_profit","REAL")]:
        try:
            c.execute(f'ALTER TABLE trades ADD COLUMN {col} {typ}')
        except Exception:
            pass
    conn.commit()
    conn.close()

def parse_trade(text: str) -> dict | None:
    try:
        is_profit = 1 if '✅' in text else (0 if '❌' in text else None)
        trader_match = re.search(r'[✅❌]?\s*([A-Za-zА-Яа-я0-9\-]+)\(([^)]+)\)', text)
        if trader_match:
            trader   = trader_match.group(1).strip()
            exchange = trader_match.group(2).strip()
        else:
            m        = re.search(r'[✅❌]?\s*([A-Za-zА-Яа-я0-9\-]+),', text)
            trader   = m.group(1).strip() if m else "Unknown"
            exchange = "Unknown"

        side_match = re.search(r'\b(BUY|SELL)', text, re.IGNORECASE)
        side       = side_match.group(1).upper() if side_match else "Unknown"

        # Формат 1: BUY 5.00 1.50 — дистанс и буфер через пробел
        dist_buf_match = re.search(r'\b(?:BUY|SELL)\s+([\d.]+)\s+([\d.]+)', text, re.IGNORECASE)
        if dist_buf_match:
            distance = float(dist_buf_match.group(1))
            buffer   = float(dist_buf_match.group(2))
        else:
            # Формат 2: BUY-0.55-0.20-(K0A) — дистанс и буфер через дефис
            dist_buf_dash = re.search(r'\b(?:BUY|SELL)-([\d.]+)-([\d.]+)-', text, re.IGNORECASE)
            if dist_buf_dash:
                distance = float(dist_buf_dash.group(1))
                buffer   = float(dist_buf_dash.group(2))
            else:
                # Формат 3: BUY_0.75 — только дистанс
                dist_match3 = re.search(r'\b(?:BUY|SELL)_([\d.]+)\b', text, re.IGNORECASE)
                distance = float(dist_match3.group(1)) if dist_match3 else None
                buffer   = None

        pnl_match  = re.search(r'(?:Profit|Loss)\s*([+-]?\d+\.?\d*)\$', text, re.IGNORECASE)
        profit_usd = float(pnl_match.group(1)) if pnl_match else None

        pct_match  = re.search(r'\$\s*\(([+-]?\d+\.?\d*)%\)', text)
        profit_pct = float(pct_match.group(1)) if pct_match else None

        coin_match = re.search(r'#([A-Z0-9]+)', text)
        coin       = coin_match.group(1) if coin_match else None

        tp_matches  = re.findall(r'\(([+-]?\d+\.?\d*)%\)', text)
        take_profit = float(tp_matches[-1]) if tp_matches else None

        if not coin or profit_usd is None:
            return None

        # Кэп на оверликвидации:
        # Профит > 110% -> обрезаем до 110%
        # Лосс < -106% -> обрезаем до -106%
        if profit_pct is not None and profit_usd != 0:
            if profit_pct > 110:
                profit_usd = round(profit_usd * (110 / profit_pct), 4)
                profit_pct = 110.0
            elif profit_pct < -106:
                profit_usd = round(profit_usd * (-106 / profit_pct), 4)
                profit_pct = -106.0

        if is_profit is None:
            is_profit = 1 if profit_usd >= 0 else 0

        trader_raw = trader  # сохраняем оригинал для определения биржи
        trader = normalize_trader(trader)
        exchange = normalize_exchange(trader_raw, exchange)
        return {
            "trader": trader, "exchange": exchange, "side": side,
            "coin": coin, "distance": distance, "buffer": buffer,
            "take_profit": take_profit, "profit_usd": profit_usd,
            "profit_pct": profit_pct, "is_profit": is_profit,
            "raw_message": text[:500]
        }
    except Exception as e:
        logger.error(f"parse_trade error: {e}")
        return None

def save_trade(trade: dict):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        '''INSERT INTO trades
           (timestamp,trader,exchange,side,coin,distance,buffer,take_profit,
            profit_usd,profit_pct,is_profit,raw_message)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
        (datetime.now().isoformat(),
         trade["trader"], trade["exchange"], trade["side"], trade["coin"],
         trade.get("distance"), trade.get("buffer"), trade.get("take_profit"),
         trade["profit_usd"], trade["profit_pct"],
         trade["is_profit"], trade["raw_message"])
    )
    conn.commit()
    conn.close()

def get_exchange_stats_for_period(since: str, until: str = None) -> str:
    """Разбивка по биржам за период."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if until:
        params = (since, until)
        where = "WHERE timestamp>=? AND timestamp<=?"
    else:
        params = (since,)
        where = "WHERE timestamp>=?"

    c.execute(f'''SELECT exchange, COUNT(*), SUM(profit_usd),
              SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
              FROM trades {where} GROUP BY exchange
              ORDER BY SUM(profit_usd) DESC''', params)
    exchanges = c.fetchall()

    # Топ монеты по каждой бирже
    c.execute(f'''SELECT exchange, coin, SUM(profit_usd), COUNT(*)
              FROM trades {where}
              GROUP BY exchange, coin
              ORDER BY exchange, SUM(profit_usd) DESC''', params)
    exch_coins = c.fetchall()
    conn.close()

    from collections import defaultdict
    exch_top = defaultdict(list)
    for exch, coin, p, cnt in exch_coins:
        if len(exch_top[exch]) < 3:
            exch_top[exch].append((coin, p, cnt))

    r = "\n🏦 *По биржам:*\n"
    for exch, cnt, pnl, wins in exchanges:
        wr = round((wins or 0)/cnt*100 if cnt > 0 else 0, 1)
        r += f"\n*{exch}*: {'+' if pnl>=0 else ''}{(pnl or 0):.2f}$ | {cnt} сделок | WR {wr}%\n"
        tops = exch_top.get(exch, [])
        if tops:
            r += "  🏆 " + ", ".join([f"#{c} {'+' if p>=0 else ''}{p:.1f}$" for c,p,_ in tops]) + "\n"
    return r


def get_stats_for_period(since: str, until: str = None) -> str:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if until:
        where = "WHERE timestamp>=? AND timestamp<=?"
        params = (since, until)
    else:
        where = "WHERE timestamp>=?"
        params = (since,)

    c.execute(f'SELECT COUNT(*), SUM(profit_usd), SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END) FROM trades {where}', params)
    total, pnl, wins = c.fetchone()
    total = total or 0; pnl = pnl or 0; wins = wins or 0

    c.execute(f'SELECT coin, SUM(profit_usd), COUNT(*) FROM trades {where} GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 5', params)
    top_coins = c.fetchall()

    c.execute(f'SELECT coin, SUM(profit_usd), COUNT(*) FROM trades {where} GROUP BY coin ORDER BY SUM(profit_usd) ASC LIMIT 5', params)
    worst_coins = c.fetchall()

    c.execute(f'SELECT trader, COUNT(*), SUM(profit_usd), SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END) FROM trades {where} GROUP BY trader ORDER BY SUM(profit_usd) DESC LIMIT 10', params)
    traders = c.fetchall()
    conn.close()

    wr = round(wins/total*100 if total > 0 else 0, 1)
    r  = f"📊 Сделок: *{total}* | PnL: *{'+' if pnl>=0 else ''}{pnl:.2f}$* | WR: *{wr}%*\n\n"

    r += "🏆 *Топ-5 монет:*\n"
    for i, (coin, p, cnt) in enumerate(top_coins, 1):
        r += f"{i}. #{coin} {'+' if p>=0 else ''}{p:.2f}$ ({cnt} сделок)\n"

    r += "\n💀 *Худшие 5 монет:*\n"
    for i, (coin, p, cnt) in enumerate(worst_coins, 1):
        r += f"{i}. #{coin} {'+' if p>=0 else ''}{p:.2f}$ ({cnt} сделок)\n"

    r += get_exchange_stats_for_period(since, until) + "\n"
    r += "\n👤 *Трейдеры:*\n"
    for trader, cnt, p, w in traders:
        wr2 = round(w/cnt*100 if cnt > 0 else 0, 1)
        icon = '🟢' if p >= 0 else '🔴'
        r += f"{icon} *{trader}*: {'+' if p>=0 else ''}{p:.2f}$ | {cnt} сделок | WR {wr2}%\n"

    return r

def get_db_context() -> str:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute('SELECT MIN(timestamp), MAX(timestamp), COUNT(*), SUM(profit_usd) FROM trades')
    min_date, max_date, total, pnl = c.fetchone()

    c.execute('SELECT trader, COUNT(*), SUM(profit_usd), SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END) FROM trades GROUP BY trader ORDER BY SUM(profit_usd) DESC LIMIT 10')
    top_traders = c.fetchall()

    week_ago = (datetime.now() - timedelta(days=7)).isoformat()
    c.execute('SELECT COUNT(*), SUM(profit_usd), SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END) FROM trades WHERE timestamp>?', (week_ago,))
    w_total, w_pnl, w_wins = c.fetchone()

    c.execute('SELECT coin, SUM(profit_usd), COUNT(*) FROM trades WHERE timestamp>? GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 5', (week_ago,))
    top_coins_week = c.fetchall()

    c.execute('SELECT coin, SUM(profit_usd), COUNT(*) FROM trades WHERE timestamp>? GROUP BY coin ORDER BY SUM(profit_usd) ASC LIMIT 5', (week_ago,))
    worst_coins_week = c.fetchall()

    today = datetime.now().replace(hour=0, minute=0, second=0).isoformat()
    c.execute('SELECT COUNT(*), SUM(profit_usd), SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END) FROM trades WHERE timestamp>?', (today,))
    t_total, t_pnl, t_wins = c.fetchone()

    c.execute('SELECT coin, SUM(profit_usd), COUNT(*) FROM trades WHERE timestamp>? GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 5', (today,))
    top_coins_today = c.fetchall()

    conn.close()

    ctx  = f"[ЖИВАЯ БАЗА ДАННЫХ]\n"
    ctx += f"Период данных: {min_date[:10] if min_date else 'н/д'} — {max_date[:10] if max_date else 'н/д'}\n"
    ctx += f"Всего сделок: {total or 0} | Суммарный PnL: {(pnl or 0):.2f}$\n\n"

    ctx += "=== СЕГОДНЯ ===\n"
    t_wr = round((t_wins or 0)/(t_total or 1)*100, 1)
    ctx += f"Сделок: {t_total or 0} | PnL: {(t_pnl or 0):.2f}$ | WR: {t_wr}%\n"
    for coin, p, cnt in (top_coins_today or []):
        ctx += f"  #{coin}: {'+' if p>=0 else ''}{p:.2f}$ ({cnt} сделок)\n"

    ctx += "\n=== ПОСЛЕДНИЕ 7 ДНЕЙ ===\n"
    w_wr = round((w_wins or 0)/(w_total or 1)*100, 1)
    ctx += f"Сделок: {w_total or 0} | PnL: {(w_pnl or 0):.2f}$ | WR: {w_wr}%\n"
    ctx += "Топ монет:\n"
    for coin, p, cnt in (top_coins_week or []):
        ctx += f"  #{coin}: {'+' if p>=0 else ''}{p:.2f}$ ({cnt} сделок)\n"
    ctx += "Худшие монеты:\n"
    for coin, p, cnt in (worst_coins_week or []):
        ctx += f"  #{coin}: {'+' if p>=0 else ''}{p:.2f}$ ({cnt} сделок)\n"

    ctx += "\n=== ТОП-10 ТРЕЙДЕРОВ (всё время) ===\n"
    for t, cnt, p, w in (top_traders or []):
        wr = round((w or 0)/cnt*100 if cnt > 0 else 0, 1)
        ctx += f"  {t}: {'+' if p>=0 else ''}{p:.2f}$ | {cnt} сделок | WR {wr}%\n"

    return ctx

SYSTEM_PROMPT = """Ты — аналитический ассистент трейдинговой группы с ПРЯМЫМ ДОСТУПОМ к живой базе сделок.

ВАЖНО: Данные которые ты получаешь — это АКТУАЛЬНАЯ статистика прямо сейчас. База обновляется в реальном времени. НЕ говори что у тебя нет доступа к live данным — они у тебя ЕСТЬ и они свежие.

СТРОГОЕ ПРАВИЛО 1 — ДАННЫЕ ЗА ПЕРИОДЫ:
Когда в сообщении пользователя есть блок [ДАННЫЕ ИЗ БАЗЫ ДЛЯ ОТВЕТА:] — используй ТОЛЬКО эти данные для ответа про период. Это реальные данные из базы. Никогда не говори что нет данных за период если они есть в сообщении.

СТРОГОЕ ПРАВИЛО 2 — НИКОГДА НЕ ВЫДУМЫВАЙ ЦИФРЫ:
- Если данных нет в базе — прямо говори "нет данных" или "недостаточно данных"
- Дистанс и буфер есть только у части сделок. Если в данных они есть — используй их. Если нет — скажи что недостаточно данных по дистансам, не придумывай цифры
- Никогда не генерируй примерные или предполагаемые цифры — только реальные данные из базы
- Если в контексте нет нужных данных — так и скажи честно

Ты знаешь из базы:
- Результаты каждого трейдера (PnL, WR, количество сделок)
- Топ и худшие монеты за любой период
- Биржи каждого трейдера (Binance, Bybit, OKX)
- Статистику за сегодня, неделю, любой месяц
- Дистанс и буфер только для новых сделок

Отвечай на том языке на котором спрашивают. Будь честным и точным. В конце каждого ответа добавляй эмодзи 🦀"""

def get_distance_stats(since: str, until: str = None) -> str:
    """Дистансы по монетам за период."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if until:
        c.execute(
            """SELECT coin, AVG(distance), AVG(buffer), AVG(take_profit), COUNT(*), SUM(profit_usd)
               FROM trades WHERE distance IS NOT NULL AND timestamp>=? AND timestamp<=?
               GROUP BY coin HAVING COUNT(*) >= 3
               ORDER BY SUM(profit_usd) DESC LIMIT 10""",
            (since, until)
        )
    else:
        c.execute(
            """SELECT coin, AVG(distance), AVG(buffer), AVG(take_profit), COUNT(*), SUM(profit_usd)
               FROM trades WHERE distance IS NOT NULL AND timestamp>=?
               GROUP BY coin HAVING COUNT(*) >= 3
               ORDER BY SUM(profit_usd) DESC LIMIT 10""",
            (since,)
        )
    rows = c.fetchall()
    conn.close()
    if not rows:
        return ""
    r = "\nДистансы по монетам:\n"
    for coin, dist, buf, tp, cnt, pnl in rows:
        d = f"{dist:.2f}" if dist else "н/д"
        b = f"{buf:.2f}" if buf else "н/д"
        t = f"{tp:.1f}%" if tp else "н/д"
        r += f"  #{coin}: dist={d} buf={b} tp={t} | {'+' if pnl>=0 else ''}{pnl:.1f}$ ({cnt} сделок)\n"
    return r


def get_period_context(user_text: str) -> str:
    """Если в тексте есть месяц/период — добавляем данные за этот период."""
    text_lower = user_text.lower()
    
    month_num = None
    year = None
    for name, num in MONTH_NAMES.items():
        if name in text_lower:
            month_num = num
            break
    
    year_match = re.search(r'\b(202[0-9])\b', user_text)
    if year_match:
        year = int(year_match.group(1))
    elif month_num:
        year = datetime.now().year

    if month_num and year:
        since = f"{year}-{month_num:02d}-01T00:00:00"
        if month_num == 12:
            until = f"{year+1}-01-01T00:00:00"
        else:
            until = f"{year}-{month_num+1:02d}-01T00:00:00"
        stats = get_stats_for_period(since, until)
        dist_stats = get_distance_stats(since, until)
        month_name = [k for k, v in MONTH_NAMES.items() if v == month_num and len(k) > 3][0].capitalize()
        return f"\n\n=== ДАННЫЕ ЗА {month_name.upper()} {year} ===\n{stats}{dist_stats}"

    # Ищем "за неделю" или "за 7 дней"
    if any(w in text_lower for w in ['за неделю', 'недел', '7 дней', 'семь дней']):
        since = (datetime.now() - timedelta(days=7)).isoformat()
        stats = get_stats_for_period(since)
        dist_stats = get_distance_stats(since)
        return f"\n\n=== ДАННЫЕ ЗА НЕДЕЛЮ ===\n{stats}{dist_stats}"

    # Ищем "за сегодня" или "сегодняшн"
    if any(w in text_lower for w in ['сегодня', 'сегодняшн']):
        since = datetime.now().replace(hour=0, minute=0, second=0).isoformat()
        stats = get_stats_for_period(since)
        dist_stats = get_distance_stats(since)
        return f"\n\n=== ДАННЫЕ ЗА СЕГОДНЯ ===\n{stats}{dist_stats}"

    # Ищем "за N дней"
    days_match = re.search(r'за\s+(\d+)\s*дн', text_lower)
    if days_match:
        days = int(days_match.group(1))
        since = (datetime.now() - timedelta(days=days)).isoformat()
        stats = get_stats_for_period(since)
        dist_stats = get_distance_stats(since)
        return f"\n\n=== ДАННЫЕ ЗА {days} ДНЕЙ ===\n{stats}{dist_stats}"

    return ""


async def claude_reply(user_id: int, user_text: str) -> str:
    history = conversation_history.setdefault(user_id, [])
    if len(history) >= MAX_HISTORY:
        history[:] = history[-(MAX_HISTORY - 2):]
    history.append({"role": "user", "content": user_text})
    db_ctx = get_db_context()
    period_ctx = get_period_context(user_text)
    system = f"{SYSTEM_PROMPT}\n\n{db_ctx}{period_ctx}"
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

async def handle_trade_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if not msg or msg.chat.id != SOURCE_CHAT_ID:
        return
    text  = msg.text or ""
    trade = parse_trade(text)
    if trade:
        save_trade(trade)
        logger.info(f"Saved: {trade['trader']} | {trade['coin']} | {trade['profit_usd']}$")

async def handle_dialog_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or msg.chat.id != DIALOG_CHAT_ID:
        return
    text   = msg.text or msg.caption or ""
    bot_id = context.bot.id
    is_reply_to_bot = (msg.reply_to_message is not None and msg.reply_to_message.from_user is not None and msg.reply_to_message.from_user.id == bot_id)
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
        return
    clean_text = text.replace(f"@{BOT_USERNAME}", "").strip()
    if not clean_text:
        clean_text = "Привет!"
    user_id = msg.from_user.id
    await msg.chat.send_action("typing")
    reply = await claude_reply(user_id, clean_text)
    await msg.reply_text(reply, parse_mode="Markdown")

async def cmd_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /period январь 2026
    /period 2026-01
    /period 7  (последние N дней)
    """
    args = context.args
    if not args:
        await update.message.reply_text("Использование:\n/period январь 2026\n/period 2026-01\n/period 7")
        return

    now = datetime.now()
    since = None
    until = None
    label = ""

    # /period 7 — последние N дней
    if len(args) == 1 and args[0].isdigit():
        days  = int(args[0])
        since = (now - timedelta(days=days)).isoformat()
        label = f"последние {days} дней"

    # /period 2026-01
    elif len(args) == 1 and re.match(r'\d{4}-\d{2}', args[0]):
        year, month = map(int, args[0].split('-'))
        since = f"{year}-{month:02d}-01T00:00:00"
        if month == 12:
            until = f"{year+1}-01-01T00:00:00"
        else:
            until = f"{year}-{month+1:02d}-01T00:00:00"
        label = f"{args[0]}"

    # /period январь 2026
    elif len(args) == 2:
        month_name = args[0].lower()
        month_num  = MONTH_NAMES.get(month_name)
        year_str   = args[1] if args[1].isdigit() else str(now.year)
        year       = int(year_str)
        if month_num:
            since = f"{year}-{month_num:02d}-01T00:00:00"
            if month_num == 12:
                until = f"{year+1}-01-01T00:00:00"
            else:
                until = f"{year}-{month_num+1:02d}-01T00:00:00"
            label = f"{args[0].capitalize()} {year}"

    if not since:
        await update.message.reply_text("Не понял период. Попробуй: /period январь 2026 или /period 7")
        return

    await update.message.reply_text(f"📅 *Статистика за {label}*\n\n" + get_stats_for_period(since, until), parse_mode="Markdown")

async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args  = context.args
    days  = int(args[0]) if args and args[0].isdigit() else 7
    since = (datetime.now() - timedelta(days=days)).isoformat()
    label = f"последние {days} дней"
    await update.message.reply_text(f"📅 *Отчёт за {label}*\n\n" + get_stats_for_period(since), parse_mode="Markdown")

async def cmd_report_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    since = datetime.now().replace(hour=0, minute=0, second=0).isoformat()
    await update.message.reply_text("📅 *Отчёт за сегодня*\n\n" + get_stats_for_period(since), parse_mode="Markdown")

async def cmd_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args  = context.args
    limit = int(args[0]) if args and args[0].isdigit() else 10
    limit = min(limit, 50)
    conn  = sqlite3.connect(DB_PATH)
    c     = conn.cursor()
    c.execute('SELECT trader, COUNT(*), SUM(profit_usd), SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END) FROM trades GROUP BY trader ORDER BY SUM(profit_usd) DESC LIMIT ?', (limit,))
    rows  = c.fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text("📭 База пустая.")
        return
    r = f"🥇 *Топ-{limit} трейдеров (за всё время)*\n\n"
    medals = ["🥇","🥈","🥉"] + ["🔹"] * (limit - 3)
    for i, (trader, cnt, pnl, w) in enumerate(rows):
        wr  = round(w/cnt*100 if cnt > 0 else 0, 1)
        sgn = '+' if pnl >= 0 else ''
        r  += f"{medals[i]} *{trader}*: {sgn}{pnl:.2f}$ | {cnt} сделок | WR {wr}%\n"
    await update.message.reply_text(r, parse_mode="Markdown")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute('SELECT COUNT(*), SUM(profit_usd), MIN(timestamp), MAX(timestamp) FROM trades')
    total, pnl, min_d, max_d = c.fetchone()
    conn.close()
    pnl = pnl or 0
    await update.message.reply_text(
        f"📊 Сделок в базе: *{total}*\n"
        f"💰 Суммарный PnL: *{'+' if pnl>=0 else ''}{pnl:.2f}$*\n"
        f"📅 Период: *{min_d[:10] if min_d else 'н/д'}* — *{max_d[:10] if max_d else 'н/д'}*",
        parse_mode="Markdown"
    )

async def cmd_clear_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conversation_history.pop(update.message.from_user.id, None)
    await update.message.reply_text("🧹 История диалога очищена.")

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📖 *Команды бота:*\n\n"
        "/report — отчёт за 7 дней\n"
        "/report 30 — отчёт за 30 дней\n"
        "/report_daily — отчёт за сегодня\n"
        "/period январь 2026 — статистика за месяц\n"
        "/period 2026-01 — статистика за месяц\n"
        "/period 30 — статистика за N дней\n"
        "/top N — топ трейдеров\n"
        "/stats — общая статистика\n"
        "/clear — очистить историю AI-диалога\n\n"
        f"💬 Упомяни @{BOT_USERNAME} или ответь на моё сообщение для AI-диалога."
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def scheduled_reports_loop(app: Application):
    sent_daily = None; sent_weekly = None
    while True:
        now      = datetime.now()
        day_key  = now.date()
        week_key = (now.isocalendar()[1], now.year)
        if now.hour == 20 and now.minute == 0:
            if sent_daily != day_key:
                try:
                    since = now.replace(hour=0, minute=0, second=0).isoformat()
                    await app.bot.send_message(chat_id=REPORT_CHAT_ID, text="📅 *Дневной отчёт*\n\n" + get_stats_for_period(since), parse_mode="Markdown")
                    sent_daily = day_key
                except Exception as e:
                    logger.error(f"Daily report error: {e}")
            if now.weekday() == 6 and sent_weekly != week_key:
                try:
                    since = (now - timedelta(days=7)).isoformat()
                    await app.bot.send_message(chat_id=REPORT_CHAT_ID, text="🗓 *Недельный отчёт*\n\n" + get_stats_for_period(since), parse_mode="Markdown")
                    sent_weekly = week_key
                except Exception as e:
                    logger.error(f"Weekly report error: {e}")
        await asyncio.sleep(60)

async def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("report",       cmd_report))
    app.add_handler(CommandHandler("report_daily", cmd_report_daily))
    app.add_handler(CommandHandler("period",       cmd_period))
    app.add_handler(CommandHandler("top",          cmd_top))
    app.add_handler(CommandHandler("stats",        cmd_stats))
    app.add_handler(CommandHandler("clear",        cmd_clear_history))
    app.add_handler(CommandHandler("help",         cmd_help))
    app.add_handler(CommandHandler("start",        cmd_help))
    app.add_handler(MessageHandler(filters.Chat(SOURCE_CHAT_ID) & filters.ALL, handle_trade_message))
    app.add_handler(MessageHandler(filters.Chat(DIALOG_CHAT_ID) & filters.TEXT & ~filters.COMMAND, handle_dialog_message))
    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        logger.info("✅ Bot started!")
        await scheduled_reports_loop(app)

if __name__ == "__main__":
    asyncio.run(main())
