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
MAX_HISTORY = 10

# Счётчик стоимости API запросов
# Sonnet claude-sonnet-4: input $3/MTok, output $15/MTok
COST_INPUT_PER_TOKEN  = 0.80 / 1_000_000
COST_OUTPUT_PER_TOKEN = 4.0 / 1_000_000
total_api_cost = 0.0
total_api_calls = 0

# ─── МАППИНГ АББРЕВИАТУР К ИМЕНАМ ТРЕЙДЕРОВ ───────────────────────────────
# Маппинг для dcc- префиксов
DCC_MAP = {
    'dns': 'Денис',
    'dmr': 'Дима',
    'ang': 'Андрей',
    'mur': 'Мирча',
    'r7b': 'Рубик',
    'rub': 'Рубен',
    'ser': 'Сергей',
}

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
    # Обработка dcc- префиксов: dcc-dns06 -> Денис
    if name.lower().startswith('dcc-'):
        parts = name.lower().split('-')
        if len(parts) >= 2:
            sub = parts[1][:3]
            mapped = DCC_MAP.get(sub)
            if mapped:
                return mapped
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

    VALID_EXCHANGES = {'Binance', 'Bybit', 'OKX'}
    r = "\n🏦 *По биржам:*\n"
    found_any = False
    for exch, cnt, pnl, wins in exchanges:
        if exch not in VALID_EXCHANGES:
            continue  # пропускаем аббревиатуры трейдеров и Unknown
        found_any = True
        wr = round((wins or 0)/cnt*100 if cnt > 0 else 0, 1)
        icon = "🥇" if exch == "Binance" else ("🥈" if exch == "Bybit" else "🥉")
        r += f"\n{icon} *{exch}*: {'+' if pnl>=0 else ''}{(pnl or 0):.2f}$ | {cnt} сделок | WR {wr}%\n"
        tops = exch_top.get(exch, [])
        if tops:
            r += "  🏆 " + ", ".join([f"#{c} {'+' if p>=0 else ''}{p:.1f}$" for c,p,_ in tops]) + "\n"
    if not found_any:
        r += "  Нет данных по биржам\n"
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
    # Группируем трейдеров по реальным именам
    from collections import defaultdict
    grouped = defaultdict(lambda: [0, 0.0, 0])
    for trader, cnt, p, w in traders:
        real = normalize_trader(trader)
        grouped[real][0] += cnt
        grouped[real][1] += (p or 0)
        grouped[real][2] += (w or 0)

    r += "\n👤 *Трейдеры:*\n"
    for name, (cnt, p, w) in sorted(grouped.items(), key=lambda x: x[1][1], reverse=True):
        wr2 = round(w/cnt*100 if cnt > 0 else 0, 1)
        icon = '🟢' if p >= 0 else '🔴'
        r += f"{icon} *{name}*: {'+' if p>=0 else ''}{p:.2f}$ | {cnt} сделок | WR {wr2}%\n"

    return r

def get_db_context() -> str:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute('SELECT MIN(timestamp), MAX(timestamp), COUNT(*), SUM(profit_usd) FROM trades')
    min_date, max_date, total, pnl = c.fetchone()

    c.execute('SELECT trader, COUNT(*), SUM(profit_usd), SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END) FROM trades GROUP BY trader ORDER BY SUM(profit_usd) DESC LIMIT 30')
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

    now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    ctx  = f"[ЖИВАЯ БАЗА ДАННЫХ]\n"
    ctx += f"Текущее время: {now_str}\n"
    ctx += f"Период данных: {min_date[:10] if min_date else 'н/д'} — {max_date[:10] if max_date else 'н/д'}\n"
    ctx += f"Всего сделок: {total or 0} | Суммарный PnL: {(pnl or 0):.2f}$\n\n"
    ctx += "=== СЕГОДНЯ ===\n"
    t_wr = round((t_wins or 0)/(t_total or 1)*100, 1)
    ctx += f"Сделок: {t_total or 0} | PnL: {(t_pnl or 0):.2f}$ | WR: {t_wr}%\n"
    ctx += "\n=== ПОСЛЕДНИЕ 7 ДНЕЙ ===\n"
    w_wr = round((w_wins or 0)/(w_total or 1)*100, 1)
    ctx += f"Сделок: {w_total or 0} | PnL: {(w_pnl or 0):.2f}$ | WR: {w_wr}%\n"

    # Топ трейдеры сегодня
    c2 = sqlite3.connect(DB_PATH)
    c2_cur = c2.cursor()
    c2_cur.execute('''SELECT trader, COUNT(*), SUM(profit_usd),
                 SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
                 FROM trades WHERE timestamp>?
                 GROUP BY trader ORDER BY SUM(profit_usd) DESC LIMIT 5''', (today,))
    traders_today = c2_cur.fetchall()
    c2.close()

    ctx += "\n[Для деталей по трейдерам, монетам и периодам — используй tools]\n"
    return ctx

SYSTEM_PROMPT = """Ты — аналитический ассистент трейдинговой группы с ПРЯМЫМ ДОСТУПОМ к живой базе сделок.
База обновляется в реальном времени. Данные которые ты получаешь — актуальны прямо сейчас.

━━━ ПРАВИЛО 1 — НЕ ВЫДУМЫВАЙ ━━━
- Только цифры из базы. Никаких "примерно", "около", "предположительно"
- Нет данных → говори "нет данных", не придумывай
- Дистанс/буфер есть только у части сделок — если нет, так и скажи

━━━ ПРАВИЛО 2 — ТОЧНЫЙ СЧЁТ, БЕЗ ДУБЛЕЙ ━━━
- Tool пишет "Монет в выборке: N" — используй ТОЛЬКО это число N в заголовке
- НЕ копируй строки "Монет в выборке: N" и "ВАЖНО: это полный список" в ответ
- Tool сам разделил монеты на 🟢 прибыльные и 🔴 убыточные — выводи ТОЛЬКО их
- ЗАПРЕЩЕНО добавлять секции "худшие монеты", "проблемные монеты" и т.п. от себя
- Каждая монета в ответе РОВНО ОДИН РАЗ — если монета есть в 🟢, её не будет в 🔴
- Не добавляй монеты которых нет в данных от tool

━━━ ПРАВИЛО 3 — ДИСТАНСЫ ━━━
- Дистанс и буфер — это ПРОЦЕНТЫ. Всегда пиши % : "1.93%", "0.77%"
- ⚡️ = рабочий дистанс (медиана прибыльных сделок) — где реально работает
- 🛡 = страховочный дистанс (90-й перцентиль) — выше ставить нет смысла
- ⚡️/🛡 показываются только когда разброс > 3% (данные уже рассчитаны в tool)
- Если в данных просто "dist=X%" — разброс маленький, всё стабильно
- КРИТИЧНО: Когда tool возвращает [ДАННЫЕ ДЛЯ ОТВЕТА] — копируй дистансы ТОЧНО как они там написаны
- Если tool показал ⚡️2.50% 🛡5.00% — пиши именно это, НЕ заменяй на свой avg
- НЕ пересчитывай и НЕ переформатируй дистансы из tool — они уже правильные

━━━ ПРАВИЛО 8 — ФОРМАТ ТАБЛИЦ ТРЕЙДЕРА ━━━
- При показе монет трейдера используй ТОЛЬКО: профит, сделки, дистанс (мин–макс или ⚡️рабочий), тейк профит
- НЕ показывай WR и буфер в таблице монет — это лишняя информация
- Формат строки: #МОНЕТА: +X.XX$ (N сделок) ⚡️X% (min–max) tp=X%
- Таблицы делай компактными — меньше колонок, больше читаемости

━━━ ПРАВИЛО 4 — ПЕРИОДЫ И ВРЕМЯ ━━━
- Timestamp в базе хранится с точностью до секунды
- Можешь запрашивать данные за любой час: since="2026-04-01T18:00:00"
- "за последний час" → get_period_stats с since = [ТЕКУЩЕЕ ВРЕМЯ] - 1 час
- "с 18:00" → since = сегодня + T18:00:00
- НИКОГДА не говори "нет доступа к данным по часам" — доступ есть

━━━ ПРАВИЛО 5 — РЕЙТИНГ И ДИСТАНСЫ ━━━
- Лучшая монета = profit_usd × win_rate (эффективность), не размер дистанса
- ВАЖНО — логика дистансов:
  - Высокий дистанс = нужен большой импульс чтобы снести ордер = БЕЗОПАСНЕЕ
  - Низкий дистанс = ордер сносится маленьким движением = РИСКОВАННЕЕ
  - Но низкий дистанс = меньше тейк-профит = меньше профит в $
  - Высокий дистанс = больше тейк-профит = больше профит но нужен сильный импульс
- НИКОГДА не говори "низкий дистанс — безопаснее" — это неверно
- НИКОГДА не говори "высокий дистанс — рискованнее" — это неверно
- Оптимальный дистанс = баланс между безопасностью и размером профита

━━━ ПРАВИЛО 7 — БИРЖИ ━━━
- Три биржи: Binance, Bybit, OKX
- Дистансы могут СИЛЬНО отличаться по биржам для одной монеты — это важная информация
- Когда показываешь дистанс монеты — всегда отмечай если есть разница между биржами
- get_top_coins поддерживает параметр exchange: "Binance", "Bybit", "OKX"
- get_coin_stats тоже поддерживает exchange для данных по конкретной бирже

━━━ ПРАВИЛО 6 — ДАННЫЕ ЗА ПЕРИОД ━━━
- Если в сообщении есть блок [ДАННЫЕ ИЗ БАЗЫ ДЛЯ ОТВЕТА:] — используй ТОЛЬКО их
- Никогда не говори "нет данных за период" если они есть в блоке

━━━ ПРАВИЛО 8 — ФОРМАТ ОТВЕТА ━━━
- Пиши нативным текстом, БЕЗ таблиц и markdown таблиц (| col | col |)
- Для монет используй простой список:
  1. #STOUSDT — +330$ | ⚡️10.50% 🛡12.00% (8.00%–14.00%) | tp=2.1%
  2. #ZECUSDT — +83$ | 🎯0.72% (0.60%–1.00%) | tp=0.4%
- Показывай ТОЛЬКО: профит, дистанс, тейк профит
- НЕ показывай WR и буфер — только если пользователь прямо просит
- ⚡️ = рабочий дистанс (разброс >3%), 🎯 = стабильный (разброс ≤3%), 🛡 = страховочный
- Анализ — 2-3 строки максимум, без лишних заголовков
- СЧЁТЧИК: tool пишет "Показано монет: N" — это точное число строк в списке. Напиши сам и убедись: если после слова "Показано монет: N" идёт ровно N строк с # — используй N. Никогда не пиши число больше чем реально строк в данных

Отвечай на том языке на котором спрашивают. В конце каждого ответа добавляй 🦀"""


EXCHANGE_ALIASES = {
    'binance': 'Binance', 'bnc': 'Binance', 'бинанс': 'Binance',
    'bybit': 'Bybit', 'bbt': 'Bybit', 'байбит': 'Bybit',
    'okx': 'OKX', 'okex': 'OKX', 'окекс': 'OKX', 'окх': 'OKX',
}

def parse_exchange_arg(arg: str) -> str | None:
    """Парсит аргумент биржи из команды. Возвращает нормализованное имя или None."""
    return EXCHANGE_ALIASES.get(arg.lower())

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
        d = fmt_dist(dist)
        b = fmt_dist(buf)
        t = f"{tp:.1f}%" if tp else "н/д"
        r += f"  #{coin}: dist={d} buf={b} tp={t} | {'+' if pnl>=0 else ''}{pnl:.1f}$ ({cnt} сделок)\n"
    return r


def get_smart_distance(conn, where: str, params: list, coin: str) -> dict | None:
    """
    Умная логика дистансов для монеты.
    Возвращает dict с рабочим и страховочным дистансом если разброс > 3%.
    Рабочий = медиана дистансов прибыльных сделок.
    Страховочный = 90-й перцентиль всех дистансов.
    Эффективность = profit_usd * win_rate для сортировки.
    """
    c2 = conn.cursor()
    coin_where = where + " AND coin=? AND distance>0 AND distance IS NOT NULL"
    coin_params = params + [coin]

    c2.execute(f"SELECT distance FROM trades {coin_where} AND is_profit=1 ORDER BY distance", coin_params)
    profit_dists = [r[0] for r in c2.fetchall()]

    c2.execute(f"SELECT distance FROM trades {coin_where} ORDER BY distance", coin_params)
    all_dists = [r[0] for r in c2.fetchall()]

    if not all_dists or len(all_dists) < 3:
        return None

    dmin = min(all_dists)
    dmax = max(all_dists)
    spread = dmax - dmin

    if spread <= 3:
        avg = sum(all_dists) / len(all_dists)
        return {"avg": avg, "min": dmin, "max": dmax, "spread": spread, "smart": False}

    # Медиана прибыльных сделок
    if profit_dists:
        n = len(profit_dists)
        if n % 2 == 1:
            working = profit_dists[n // 2]
        else:
            working = (profit_dists[n // 2 - 1] + profit_dists[n // 2]) / 2
    else:
        working = sum(all_dists) / len(all_dists)

    # 90-й перцентиль
    idx = int(len(all_dists) * 0.9)
    insurance = all_dists[min(idx, len(all_dists) - 1)]

    return {
        "avg": sum(all_dists) / len(all_dists),
        "min": dmin, "max": dmax, "spread": spread,
        "working": working, "insurance": insurance,
        "smart": True
    }


def fmt_dist(d: float | None) -> str:
    """Форматирует дистанс/буфер с символом %."""
    if d is None:
        return "н/д"
    return f"{d:.2f}%"


def fmt_dist_info(dist_data: dict | None) -> str:
    """Форматирует блок дистанса: умный или обычный."""
    if not dist_data:
        return ""
    if dist_data.get("smart"):
        return (f" ⚡️{fmt_dist(dist_data['working'])} 🛡{fmt_dist(dist_data['insurance'])}"
                f" ({fmt_dist(dist_data['min'])}–{fmt_dist(dist_data['max'])})")
    else:
        return f" 🎯{fmt_dist(dist_data['avg'])} ({fmt_dist(dist_data['min'])}–{fmt_dist(dist_data['max'])})"


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

    # Ищем "за последний час", "за 2 часа", "за N часов"
    hour_match = re.search(r'за\s+(\d+)\s*час|последни[йе]\s+час', text_lower)
    if hour_match:
        grp = hour_match.group(1)
        hours = int(grp) if grp else 1
        since = (datetime.now() - timedelta(hours=hours)).isoformat()
        stats = get_stats_for_period(since)
        dist_stats = get_distance_stats(since)
        if hours == 1:
            label = "час"
        elif 2 <= hours <= 4:
            label = f"{hours} часа"
        else:
            label = f"{hours} часов"
        return f"\n\n=== ДАННЫЕ ЗА ПОСЛЕДНИЕ {label.upper()} ===\n{stats}{dist_stats}"

    # Ищем "с 18:00", "с 15:00" и т.д.
    from_time_match = re.search(r'с\s+(\d{1,2})[\:\.](\d{2})|с\s+(\d{1,2})\s*час', text_lower)
    if from_time_match:
        if from_time_match.group(1):
            hour = int(from_time_match.group(1))
            minute = int(from_time_match.group(2))
        else:
            hour = int(from_time_match.group(3))
            minute = 0
        now = datetime.now()
        since = now.replace(hour=hour, minute=minute, second=0, microsecond=0).isoformat()
        stats = get_stats_for_period(since)
        dist_stats = get_distance_stats(since)
        return f"\n\n=== ДАННЫЕ С {hour:02d}:{minute:02d} ===\n{stats}{dist_stats}"

    # Ищем "за последние 30 минут", "за N минут"
    min_match = re.search(r'за\s+(\d+)\s*мин|последни[ехй]\s+(\d+)\s*мин', text_lower)
    if min_match:
        minutes = int(min_match.group(1) or min_match.group(2))
        since = (datetime.now() - timedelta(minutes=minutes)).isoformat()
        stats = get_stats_for_period(since)
        dist_stats = get_distance_stats(since)
        return f"\n\n=== ДАННЫЕ ЗА ПОСЛЕДНИЕ {minutes} МИНУТ ===\n{stats}{dist_stats}"

    return ""


# ─── TOOLS для Claude ─────────────────────────────────────────────────────────
TOOLS = [
    {
        "name": "get_trader_stats",
        "description": "Получить статистику трейдера по монетам с дистансами за период. Можно фильтровать по бирже и сортировать по прибыли или убытку.",
        "input_schema": {
            "type": "object",
            "properties": {
                "trader": {"type": "string", "description": "Имя трейдера"},
                "since": {"type": "string", "description": "Дата/время начала. Форматы: YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS для запросов по часам/минутам. Примеры: 2026-04-01 или 2026-04-01T18:00:00"},
                "until": {"type": "string", "description": "Дата/время конца. Форматы: YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS (опционально)"},
                "exchange": {"type": "string", "description": "Биржа: Binance, Bybit, OKX (опционально)"},
                "sort_by": {"type": "string", "description": "Сортировка: profit (лучшие) или loss (худшие/убыточные)"},
                "limit": {"type": "integer", "description": "Количество монет (по умолчанию 10)"}
            },
            "required": ["trader"]
        }
    },
    {
        "name": "get_coin_stats",
        "description": "Получить статистику по конкретной монете за период",
        "input_schema": {
            "type": "object",
            "properties": {
                "coin": {"type": "string", "description": "Название монеты например BTCUSDT"},
                "since": {"type": "string", "description": "Дата/время начала. Форматы: YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS для запросов по часам/минутам. Примеры: 2026-04-01 или 2026-04-01T18:00:00"},
                "until": {"type": "string", "description": "Дата/время конца. Форматы: YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS (опционально)"}
            },
            "required": ["coin"]
        }
    },
    {
        "name": "get_top_coins",
        "description": "Получить топ монет с дистансами за период. Фильтр по бирже через параметр exchange. ВАЖНО: если нужны данные по нескольким биржам — вызывай этот tool ОДИН РАЗ БЕЗ фильтра биржи, не делай отдельный вызов для каждой биржи.",
        "input_schema": {
            "type": "object",
            "properties": {
                "since": {"type": "string", "description": "Дата/время начала. Форматы: YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS для запросов по часам/минутам. Примеры: 2026-04-01 или 2026-04-01T18:00:00"},
                "until": {"type": "string", "description": "Дата/время конца. Форматы: YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS (опционально)"},
                "limit": {"type": "integer", "description": "Количество монет (по умолчанию 10)"},
                "min_distance": {"type": "number", "description": "Минимальный дистанс для фильтрации"},
                "max_distance": {"type": "number", "description": "Максимальный дистанс для фильтрации"},
                "sort_by": {"type": "string", "description": "Сортировка: profit (лучшие) или loss (худшие)"},
                "exchange": {"type": "string", "description": "Фильтр по бирже: Binance, Bybit, OKX (опционально)"}
            }
        }
    },
    {
        "name": "get_all_traders",
        "description": "Получить полный список всех трейдеров в базе с их общей статистикой",
        "input_schema": {
            "type": "object",
            "properties": {
                "since": {"type": "string", "description": "Дата начала в формате YYYY-MM-DD (опционально)"},
                "until": {"type": "string", "description": "Дата/время конца. Форматы: YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS (опционально)"}
            }
        }
    },
    {
        "name": "get_period_stats",
        "description": "Получить общую статистику за период. Поддерживает запросы по часам: since=2026-04-01T18:00:00. Используй для запросов 'за последний час', 'с 18:00', 'за 2 часа'",
        "input_schema": {
            "type": "object",
            "properties": {
                "since": {"type": "string", "description": "Дата/время начала. Форматы: YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS для запросов по часам/минутам. Примеры: 2026-04-01 или 2026-04-01T18:00:00"},
                "until": {"type": "string", "description": "Дата/время конца. Форматы: YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS (опционально)"}
            },
            "required": ["since"]
        }
    }
]


def parse_dt(dt_str: str, end_of_day: bool = False) -> str:
    """Парсит дату или datetime строку. Если уже содержит время — оставляет как есть."""
    if not dt_str:
        return dt_str
    if "T" in dt_str:
        return dt_str  # уже с временем, не трогаем
    if end_of_day:
        return dt_str + "T23:59:59"
    return dt_str + "T00:00:00"


def execute_tool(tool_name: str, tool_input: dict) -> str:
    """Выполняет SQL запрос на основе вызова инструмента."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    result = ""

    try:
        if tool_name == "get_trader_stats":
            trader = tool_input["trader"]
            since = tool_input.get("since")
            until = tool_input.get("until")
            exchange = tool_input.get("exchange")
            sort_by = tool_input.get("sort_by", "profit")
            limit = tool_input.get("limit", 10)

            where = "WHERE trader=?"
            params = [trader]
            if since:
                where += " AND timestamp>=?"
                params.append(parse_dt(since))
            if until:
                where += " AND timestamp<=?"
                params.append(parse_dt(until, end_of_day=True))
            if exchange:
                where += " AND exchange=?"
                params.append(exchange)

            c.execute(f"""SELECT COUNT(*), SUM(profit_usd),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
                         FROM trades {where}""", params)
            row = c.fetchone()
            if not row or not row[0]:
                return f"Трейдер {trader} не найден."

            cnt, pnl, wins = row
            wr = round((wins or 0)/cnt*100 if cnt > 0 else 0, 1)

            order = "ASC" if sort_by == "loss" else "DESC"
            c.execute(f"""SELECT coin, SUM(profit_usd), COUNT(*),
                         MIN(CASE WHEN distance>0 THEN distance END),
                         MAX(CASE WHEN distance>0 THEN distance END),
                         AVG(CASE WHEN take_profit!=0 THEN take_profit END),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
                         FROM trades {where}
                         GROUP BY coin ORDER BY SUM(profit_usd) {order} LIMIT {limit}""", params)
            top_coins = c.fetchall()

            c.execute(f"""SELECT exchange, COUNT(*), SUM(profit_usd) FROM trades {where}
                         GROUP BY exchange ORDER BY COUNT(*) DESC""", params)
            exchanges = c.fetchall()

            exch_filter = f" [{exchange}]" if exchange else ""
            sort_label = "убыточные" if sort_by == "loss" else "прибыльные"
            result = f"Трейдер: {trader}{exch_filter}\n"
            result += f"Сделок: {cnt} | PnL: {'+' if pnl>=0 else ''}{(pnl or 0):.2f}$ | WR: {wr}%\n"
            result += f"Биржи: {', '.join([f'{e}({n}) {'+' if p>=0 else ''}{(p or 0):.1f}$' for e,n,p in exchanges if e])}\n\n"
            coin_lines = []
            for coin, p, n, dmin, dmax, tp, wins in top_coins:
                dist_data = get_smart_distance(conn, where, params, coin)
                line = f"#{coin}: {'+' if p>=0 else ''}{(p or 0):.2f}$"
                if dmin and dmax:
                    if dist_data and dist_data.get("smart"):
                        line += f" | ⚡️{fmt_dist(dist_data['working'])} 🛡{fmt_dist(dist_data['insurance'])} ({fmt_dist(dmin)}–{fmt_dist(dmax)})"
                    else:
                        line += f" | 🎯{fmt_dist(dmin)}–{fmt_dist(dmax)}"
                if tp: line += f" | tp={tp:.1f}%"
                line += f" ({n} сделок)"
                coin_lines.append(line)
            result += f"Список содержит ровно {len(coin_lines)} монет (не больше, не меньше):\n"
            for i, line in enumerate(coin_lines, 1):
                result += f"{i}. {line}\n"
            result += f"ИТОГО В СПИСКЕ: {len(coin_lines)} монет\n"

        elif tool_name == "get_coin_stats":
            coin = tool_input["coin"].upper()
            if not coin.endswith("USDT"): coin += "USDT"
            since = tool_input.get("since")
            until = tool_input.get("until")

            where = "WHERE coin=?"
            params = [coin]
            if since:
                where += " AND timestamp>=?"
                params.append(parse_dt(since))
            if until:
                where += " AND timestamp<=?"
                params.append(parse_dt(until, end_of_day=True))

            c.execute(f"""SELECT COUNT(*), SUM(profit_usd),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END),
                         AVG(distance), MIN(distance), MAX(distance), AVG(buffer)
                         FROM trades {where}""", params)
            row = c.fetchone()
            if not row or not row[0]:
                return f"Монета {coin} не найдена."

            cnt, pnl, wins, dist, dmin, dmax, buf = row
            wr = round((wins or 0)/cnt*100 if cnt > 0 else 0, 1)

            c.execute(f"""SELECT trader, SUM(profit_usd), COUNT(*), AVG(distance)
                         FROM trades {where}
                         GROUP BY trader ORDER BY SUM(profit_usd) DESC LIMIT 5""", params)
            traders = c.fetchall()

            result = f"Монета: #{coin}\n"
            result += f"Сделок: {cnt} | PnL: {'+' if pnl>=0 else ''}{(pnl or 0):.2f}$ | WR: {wr}%\n"
            if dist:
                dmin_s = fmt_dist(dmin) if dmin and dmin > 0 else "н/д"
                dist_data = get_smart_distance(conn, where, params, coin)
                if dist_data and dist_data.get("smart"):
                    result += f"Дистанс: ⚡️рабочий {fmt_dist(dist_data['working'])} | 🛡страховочный {fmt_dist(dist_data['insurance'])} | диапазон {dmin_s}–{fmt_dist(dmax)}\n"
                else:
                    result += f"Дистанс: avg={fmt_dist(dist)} min={dmin_s} max={fmt_dist(dmax)}\n"
            if buf: result += f"Буфер: avg={fmt_dist(buf)}\n"
            result += "\nТоп трейдеры:\n"
            for t, p, n, d in traders:
                result += f"{t}: {'+' if p>=0 else ''}{(p or 0):.2f}$ ({n} сделок)"
                if d: result += f" dist={fmt_dist(d)}"
                result += "\n"

        elif tool_name == "get_top_coins":
            since = tool_input.get("since")
            until = tool_input.get("until")
            limit = tool_input.get("limit", 10)
            min_dist = tool_input.get("min_distance")
            max_dist = tool_input.get("max_distance")
            sort_by = tool_input.get("sort_by", "profit")

            # Базовый where без фильтра по дистансу — чтобы считать все монеты
            exchange = tool_input.get("exchange")
            where_base = "WHERE 1=1"
            params = []
            if since:
                where_base += " AND timestamp>=?"
                params.append(parse_dt(since))
            if until:
                where_base += " AND timestamp<=?"
                params.append(parse_dt(until, end_of_day=True))
            if exchange:
                where_base += " AND exchange=?"
                params.append(exchange)

            # where для дистансов (с фильтром)
            where_dist = where_base + " AND distance IS NOT NULL AND distance > 0"
            params_dist = list(params)
            if min_dist:
                where_dist += " AND distance>=?"
                params_dist.append(min_dist)
            if max_dist:
                where_dist += " AND distance<=?"
                params_dist.append(max_dist)

            order = "ASC" if sort_by == "loss" else "DESC"

            # Основной запрос — все монеты (не только с дистансом)
            c.execute(f"""SELECT coin, SUM(profit_usd), COUNT(*),
                         AVG(CASE WHEN distance>0 THEN distance END),
                         MIN(CASE WHEN distance>0 THEN distance END),
                         MAX(CASE WHEN distance>0 THEN distance END),
                         AVG(CASE WHEN buffer>0 THEN buffer END),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
                         FROM trades {where_base}
                         GROUP BY coin HAVING COUNT(*) >= 3
                         ORDER BY SUM(profit_usd) {order} LIMIT ?""",
                     params + [limit])
            rows = c.fetchall()

            dist_filter = ""
            if min_dist and max_dist:
                dist_filter = f" (дистанс {min_dist}%-{max_dist}%)"
            elif min_dist:
                dist_filter = f" (дистанс от {min_dist}%)"
            elif max_dist:
                dist_filter = f" (дистанс до {max_dist}%)"
            if exchange:
                dist_filter += f" | 🏦{exchange}"

            def fmt_coin_row(coin, pnl, cnt, dist, dmin, dmax, buf, wins):
                wr = round((wins or 0)/cnt*100 if cnt > 0 else 0, 1)
                sign = "🟢" if (pnl or 0) >= 0 else "🔴"
                line = f"{sign} #{coin}: {'+' if pnl>=0 else ''}{(pnl or 0):.2f}$ WR={wr}%"
                if dist:
                    dist_data = get_smart_distance(conn, where_dist, params_dist, coin)
                    line += fmt_dist_info(dist_data)
                if buf:
                    line += f" buf={fmt_dist(buf)}"
                line += f" ({cnt} сделок)"
                return line

            # Разделяем прибыльные и убыточные
            profit_rows = [(i, r) for i, r in enumerate(rows, 1) if (r[1] or 0) >= 0]
            loss_rows   = [(i, r) for i, r in enumerate(rows, 1) if (r[1] or 0) < 0]

            profit_lines = [fmt_coin_row(*row) for _, row in profit_rows]
            loss_lines   = [fmt_coin_row(*row) for _, row in loss_rows]
            total_coins  = len(profit_lines) + len(loss_lines)

            result = f"[ДАННЫЕ ДЛЯ ОТВЕТА]\n"
            result += f"Список содержит ровно {total_coins} монет{dist_filter}. Пиши {total_coins} в заголовке.\n\n"

            if profit_lines:
                result += f"ПРИБЫЛЬНЫЕ ({len(profit_lines)}):\n"
                for i, line in enumerate(profit_lines, 1):
                    result += f"{i}. {line}\n"

            if loss_lines:
                result += f"\nУБЫТОЧНЫЕ ({len(loss_lines)}):\n"
                for i, line in enumerate(loss_lines, 1):
                    result += f"{i}. {line}\n"

            result += f"\nИТОГО В СПИСКЕ: {total_coins} монет\n"

            # Анализ дистансов
            all_dist_data = []
            for coin, pnl, cnt, dist, dmin, dmax, buf, wins in rows:
                if dist:
                    dd = get_smart_distance(conn, where_dist, params_dist, coin)
                    if dd:
                        all_dist_data.append((coin, dd, pnl))

            if all_dist_data:
                min_entry = min(all_dist_data, key=lambda x: x[1]["min"])
                max_entry = max(all_dist_data, key=lambda x: x[1]["max"])
                stable = sorted([x for x in all_dist_data if x[1]["spread"] <= 3],
                                 key=lambda x: x[1]["spread"])

                result += "\nАНАЛИЗ ДИСТАНСОВ:\n"
                result += f"Мин. дистанс: #{min_entry[0]} от {fmt_dist(min_entry[1]['min'])}\n"
                result += f"Макс. дистанс: #{max_entry[0]} до {fmt_dist(max_entry[1]['max'])}\n"
                if stable:
                    result += "Стабильные дистансы (разброс ≤3%):\n"
                    for coin, dd, _ in stable[:3]:
                        result += f"  #{coin}: {fmt_dist(dd['min'])}—{fmt_dist(dd['max'])} (разброс {dd['spread']:.2f}%)\n"


        elif tool_name == "get_all_traders":
            since = tool_input.get("since")
            until = tool_input.get("until")

            where = "WHERE 1=1"
            params = []
            if since:
                where += " AND timestamp>=?"
                params.append(parse_dt(since))
            if until:
                where += " AND timestamp<=?"
                params.append(parse_dt(until, end_of_day=True))

            c.execute(f"""SELECT trader, COUNT(*), SUM(profit_usd),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
                         FROM trades {where}
                         GROUP BY trader ORDER BY SUM(profit_usd) DESC""", params)
            rows = c.fetchall()

            # Группируем по реальным именам из TRADER_MAP
            from collections import defaultdict
            grouped = defaultdict(lambda: [0, 0.0, 0])
            ungrouped = []

            for t, cnt, pnl, wins in rows:
                # Handle dcc- prefixes
                if t.lower().startswith('dcc-'):
                    parts = t.lower().split('-')
                    sub = parts[1][:3] if len(parts) >= 2 else ''
                    real_name = DCC_MAP.get(sub)
                else:
                    prefix = t[:3].upper()
                    real_name = TRADER_MAP.get(prefix)
                if real_name:
                    grouped[real_name][0] += cnt
                    grouped[real_name][1] += (pnl or 0)
                    grouped[real_name][2] += (wins or 0)
                else:
                    ungrouped.append((t, cnt, pnl or 0, wins or 0))

            result = f"Трейдеры группы ({len(grouped)} человек):\n\n"
            result += "✅ С реальными именами:\n"
            sorted_grouped = sorted(grouped.items(), key=lambda x: x[1][1], reverse=True)
            for name, (cnt, pnl, wins) in sorted_grouped:
                wr = round(wins/cnt*100 if cnt > 0 else 0, 1)
                icon = "🟢" if pnl >= 0 else "🔴"
                result += f"{icon} {name}: {'+' if pnl>=0 else ''}{pnl:.2f}$ | {cnt} сделок | WR {wr}%\n"

            if ungrouped:
                result += f"\n❓ Неопознанные ({len(ungrouped)}):\n"
                for t, cnt, pnl, wins in sorted(ungrouped, key=lambda x: x[2], reverse=True)[:10]:
                    wr = round(wins/cnt*100 if cnt > 0 else 0, 1)
                    result += f"  {t}: {'+' if pnl>=0 else ''}{pnl:.2f}$ | {cnt} сделок | WR {wr}%\n"

        elif tool_name == "get_period_stats":
            since = tool_input["since"]
            until = tool_input.get("until")

            where = "WHERE timestamp>=?"
            params = [parse_dt(since)]
            if until:
                where += " AND timestamp<=?"
                params.append(parse_dt(until, end_of_day=True))

            c.execute(f"""SELECT COUNT(*), SUM(profit_usd),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
                         FROM trades {where}""", params)
            cnt, pnl, wins = c.fetchone()
            wr = round((wins or 0)/(cnt or 1)*100, 1)

            c.execute(f"""SELECT trader, SUM(profit_usd), COUNT(*),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
                         FROM trades {where}
                         GROUP BY trader ORDER BY SUM(profit_usd) DESC LIMIT 10""", params)
            traders = c.fetchall()

            # Топ монеты за период
            c.execute(f"""SELECT coin, SUM(profit_usd), COUNT(*),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
                         FROM trades {where}
                         GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 5""", params)
            top_coins_p = c.fetchall()

            c.execute(f"""SELECT coin, SUM(profit_usd), COUNT(*),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
                         FROM trades {where}
                         GROUP BY coin ORDER BY SUM(profit_usd) ASC LIMIT 3""", params)
            worst_coins_p = c.fetchall()

            result = f"Период: {since} — {until or 'сейчас'}\n"
            result += f"Сделок: {cnt} | PnL: {'+' if pnl>=0 else ''}{(pnl or 0):.2f}$ | WR: {wr}%\n\n"
            result += "Топ-5 монет:\n"
            for coin_r, p_r, n_r, w_r in top_coins_p:
                cwr = round((w_r or 0)/n_r*100 if n_r > 0 else 0, 1)
                result += f"  #{coin_r}: {'+' if p_r>=0 else ''}{(p_r or 0):.2f}$ ({n_r} сделок WR={cwr}%)\n"
            result += "Худшие монеты:\n"
            for coin_r, p_r, n_r, w_r in worst_coins_p:
                cwr = round((w_r or 0)/n_r*100 if n_r > 0 else 0, 1)
                result += f"  #{coin_r}: {'+' if p_r>=0 else ''}{(p_r or 0):.2f}$ ({n_r} сделок WR={cwr}%)\n"
            result += "\nТоп трейдеры:\n"
            for t, p, n, w in traders:
                twr = round((w or 0)/n*100 if n > 0 else 0, 1)
                result += f"  {t}: {'+' if p>=0 else ''}{(p or 0):.2f}$ ({n} сделок WR={twr}%)\n"

    except Exception as e:
        result = f"Ошибка запроса: {e}"
    finally:
        conn.close()

    return result


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
        until = f"{year+1}-01-01T00:00:00" if month_num == 12 else f"{year}-{month_num+1:02d}-01T00:00:00"
        stats = get_stats_for_period(since, until)
        dist_stats = get_distance_stats(since, until)
        month_name = [k for k, v in MONTH_NAMES.items() if v == month_num and len(k) > 3][0].capitalize()
        return f"\n\n=== ДАННЫЕ ЗА {month_name.upper()} {year} ===\n{stats}{dist_stats}"

    if any(w in text_lower for w in ['за неделю', 'недел', '7 дней', 'семь дней']):
        since = (datetime.now() - timedelta(days=7)).isoformat()
        stats = get_stats_for_period(since)
        dist_stats = get_distance_stats(since)
        return f"\n\n=== ДАННЫЕ ЗА НЕДЕЛЮ ===\n{stats}{dist_stats}"

    if any(w in text_lower for w in ['сегодня', 'сегодняшн']):
        since = datetime.now().replace(hour=0, minute=0, second=0).isoformat()
        stats = get_stats_for_period(since)
        dist_stats = get_distance_stats(since)
        return f"\n\n=== ДАННЫЕ ЗА СЕГОДНЯ ===\n{stats}{dist_stats}"

    days_match = re.search(r'за\s+(\d+)\s*дн', text_lower)
    if days_match:
        days = int(days_match.group(1))
        since = (datetime.now() - timedelta(days=days)).isoformat()
        stats = get_stats_for_period(since)
        dist_stats = get_distance_stats(since)
        return f"\n\n=== ДАННЫЕ ЗА {days} ДНЕЙ ===\n{stats}{dist_stats}"

    hour_match = re.search(r'за\s+(\d+)\s*час|последни[йе]\s+час', text_lower)
    if hour_match:
        grp = hour_match.group(1)
        hours = int(grp) if grp else 1
        since = (datetime.now() - timedelta(hours=hours)).isoformat()
        stats = get_stats_for_period(since)
        dist_stats = get_distance_stats(since)
        if hours == 1:
            label = "час"
        elif 2 <= hours <= 4:
            label = f"{hours} часа"
        else:
            label = f"{hours} часов"
        return f"\n\n=== ДАННЫЕ ЗА ПОСЛЕДНИЕ {label.upper()} ===\n{stats}{dist_stats}"

    from_time_match = re.search(r'с\s+(\d{1,2})[\:\.](\d{2})|с\s+(\d{1,2})\s*час', text_lower)
    if from_time_match:
        if from_time_match.group(1):
            hour = int(from_time_match.group(1))
            minute = int(from_time_match.group(2))
        else:
            hour = int(from_time_match.group(3))
            minute = 0
        now = datetime.now()
        since = now.replace(hour=hour, minute=minute, second=0, microsecond=0).isoformat()
        stats = get_stats_for_period(since)
        dist_stats = get_distance_stats(since)
        return f"\n\n=== ДАННЫЕ С {hour:02d}:{minute:02d} ===\n{stats}{dist_stats}"

    min_match = re.search(r'за\s+(\d+)\s*мин|последни[ехй]\s+(\d+)\s*мин', text_lower)
    if min_match:
        minutes = int(min_match.group(1) or min_match.group(2))
        since = (datetime.now() - timedelta(minutes=minutes)).isoformat()
        stats = get_stats_for_period(since)
        dist_stats = get_distance_stats(since)
        return f"\n\n=== ДАННЫЕ ЗА ПОСЛЕДНИЕ {minutes} МИНУТ ===\n{stats}{dist_stats}"

    return ""


async def claude_reply(user_id: int, user_text: str) -> str:
    global total_api_cost, total_api_calls

    history = conversation_history.setdefault(user_id, [])
    if len(history) >= MAX_HISTORY:
        history[:] = history[-(MAX_HISTORY - 2):]
    period_ctx = get_period_context(user_text)
    enriched_text = user_text + period_ctx if period_ctx else user_text
    history.append({"role": "user", "content": enriched_text})

    db_ctx = get_db_context()
    system = f"{SYSTEM_PROMPT}\n\n{db_ctx}"

    # Логируем размер контекста
    system_chars = len(system)
    history_chars = sum(len(str(m.get("content", ""))) for m in history)
    logger.info(f"💬 Request | user={user_id} | system={system_chars}c | history={history_chars}c | msgs={len(history)}")

    request_cost = 0.0
    request_input = 0
    request_output = 0
    tool_calls_count = 0

    try:
        # Первый запрос с инструментами
        response = anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2048,
            system=system,
            tools=TOOLS,
            messages=history
        )

        # Считаем токены первого запроса
        if hasattr(response, "usage"):
            request_input  += response.usage.input_tokens
            request_output += response.usage.output_tokens

        # Обрабатываем вызовы инструментов — максимум 3 tool calls
        max_tool_calls = 3
        while response.stop_reason == "tool_use" and tool_calls_count < max_tool_calls:
            tool_results = []
            assistant_content = response.content
            tool_calls_count += 1

            for block in response.content:
                if block.type == "tool_use":
                    tool_result = execute_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": tool_result
                    })
                    logger.info(f"🔧 Tool: {block.name} → {len(tool_result)} chars")

            messages_with_tools = history + [
                {"role": "assistant", "content": assistant_content},
                {"role": "user", "content": tool_results}
            ]

            response = anthropic_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2048,
                system=system,
                tools=TOOLS,
                messages=messages_with_tools
            )

            if hasattr(response, "usage"):
                request_input  += response.usage.input_tokens
                request_output += response.usage.output_tokens

        reply = ""
        for block in response.content:
            if hasattr(block, "text"):
                reply += block.text

        if not reply:
            reply = "⚠️ Нет ответа от Claude."

    except Exception as e:
        logger.error(f"Claude API error: {e}")
        reply = "⚠️ Ошибка при обращении к Claude. Попробуй позже."
        history.append({"role": "assistant", "content": reply})
        return reply

    # Считаем и логируем стоимость
    request_cost = (request_input * COST_INPUT_PER_TOKEN) + (request_output * COST_OUTPUT_PER_TOKEN)
    total_api_cost += request_cost
    total_api_calls += 1

    logger.info(
        f"💰 Cost: ${request_cost:.4f} | "
        f"in={request_input} out={request_output} tokens | "
        f"tools={tool_calls_count} | "
        f"total=${total_api_cost:.4f} ({total_api_calls} calls)"
    )

    # Предупреждение если запрос дорогой
    if request_cost > 0.50:
        logger.warning(f"⚠️ EXPENSIVE REQUEST: ${request_cost:.4f} | user={user_id}")

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


async def cmd_trader(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /trader Денис
    /trader Денис январь 2026
    /trader Денис 2026-01
    """
    args = context.args
    if not args:
        await update.message.reply_text("Использование: /trader Денис\n/trader Денис январь 2026")
        return

    trader_name = args[0]
    since = None
    until = None
    label = "за всё время"

    if len(args) >= 2:
        month_name = args[1].lower()
        month_num = MONTH_NAMES.get(month_name)
        year = int(args[2]) if len(args) >= 3 and args[2].isdigit() else datetime.now().year
        if month_num:
            since = f"{year}-{month_num:02d}-01T00:00:00"
            until = f"{year}-{month_num+1:02d}-01T00:00:00" if month_num < 12 else f"{year+1}-01-01T00:00:00"
            label = f"{args[1].capitalize()} {year}"
        elif re.match(r'\d{4}-\d{2}', args[1]):
            y, m = map(int, args[1].split('-'))
            since = f"{y}-{m:02d}-01T00:00:00"
            until = f"{y}-{m+1:02d}-01T00:00:00" if m < 12 else f"{y+1}-01-01T00:00:00"
            label = args[1]

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    where = "WHERE trader=?"
    params = [trader_name]
    if since:
        where += " AND timestamp>=?"
        params.append(since)
    if until:
        where += " AND timestamp<=?"
        params.append(until)

    c.execute(f'''SELECT COUNT(*), SUM(profit_usd), SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
               FROM trades {where}''', params)
    total, pnl, wins = c.fetchone()
    total = total or 0; pnl = pnl or 0; wins = wins or 0

    if total == 0:
        await update.message.reply_text(f"❌ Трейдер *{trader_name}* не найден или нет сделок за период.", parse_mode="Markdown")
        conn.close()
        return

    wr = round(wins/total*100 if total > 0 else 0, 1)

    # Топ монет
    c.execute(f'''SELECT coin, SUM(profit_usd), COUNT(*), SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
               FROM trades {where} GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 7''', params)
    top_coins = c.fetchall()

    # Худшие монеты
    c.execute(f'''SELECT coin, SUM(profit_usd), COUNT(*), SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
               FROM trades {where} GROUP BY coin ORDER BY SUM(profit_usd) ASC LIMIT 5''', params)
    worst_coins = c.fetchall()

    # Дистансы по монетам
    c.execute(f'''SELECT coin, AVG(distance), AVG(buffer), AVG(take_profit), COUNT(*), SUM(profit_usd)
               FROM trades {where} AND distance IS NOT NULL
               GROUP BY coin HAVING COUNT(*) >= 3
               ORDER BY SUM(profit_usd) DESC LIMIT 7''', params)
    dist_coins = c.fetchall()

    # Биржи
    c.execute(f'''SELECT exchange, COUNT(*), SUM(profit_usd) FROM trades {where} GROUP BY exchange ORDER BY COUNT(*) DESC''', params)
    exchanges = c.fetchall()

    conn.close()

    sgn = '+' if pnl >= 0 else ''
    r  = f"👤 *{trader_name}* — {label}\n\n"
    r += f"📊 Сделок: *{total}* | PnL: *{sgn}{pnl:.2f}$* | WR: *{wr}%*\n"

    if exchanges:
        exch_str = ", ".join([f"{e} ({cnt} сделок)" for e, cnt, _ in exchanges])
        r += f"🏦 Биржи: {exch_str}\n"

    r += "\n🏆 *Топ монет:*\n"
    for coin, p, cnt, w in top_coins:
        wr2 = round(w/cnt*100 if cnt > 0 else 0, 1)
        r += f"  #{coin}: {'+' if p>=0 else ''}{p:.2f}$ | {cnt} сделок | WR {wr2}%\n"

    r += "\n💀 *Худшие монеты:*\n"
    for coin, p, cnt, w in worst_coins:
        r += f"  #{coin}: {'+' if p>=0 else ''}{p:.2f}$ | {cnt} сделок\n"

    if dist_coins:
        r += "\n📐 *Дистансы по монетам:*\n"
        conn2 = sqlite3.connect(DB_PATH)
        w2 = "WHERE trader=?"
        p2 = [trader_name]
        if since:
            w2 += " AND timestamp>=?"; p2.append(since)
        if until:
            w2 += " AND timestamp<=?"; p2.append(until)
        for coin, dist, buf, tp, cnt, p in dist_coins:
            dist_data = get_smart_distance(conn2, w2, p2, coin)
            t = f"{tp:.1f}%" if tp else "н/д"
            r += f"  #{coin}:{fmt_dist_info(dist_data)} buf={fmt_dist(buf)} tp={t} | {cnt} сделок\n"
        conn2.close()

    await update.message.reply_text(r, parse_mode="Markdown")


async def cmd_coins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /coins — топ монет за всё время
    /coins binance — только Binance
    /coins январь 2026
    /coins январь 2026 bybit
    /coins 30 okx
    """
    args = context.args
    since = None
    until = None
    label = "за всё время"
    exchange = None

    # Ищем биржу в любом аргументе
    clean_args = []
    for a in (args or []):
        exch = parse_exchange_arg(a)
        if exch:
            exchange = exch
        else:
            clean_args.append(a)

    if clean_args:
        first = clean_args[0].lower()
        if first.isdigit():
            days = int(first)
            since = (datetime.now() - timedelta(days=days)).isoformat()
            label = f"последние {days} дней"
        elif first in ['неделя', 'неделю', 'week']:
            since = (datetime.now() - timedelta(days=7)).isoformat()
            label = "последние 7 дней"
        elif first in ['сегодня', 'today']:
            since = datetime.now().replace(hour=0,minute=0,second=0).isoformat()
            label = "сегодня"
        else:
            month_num = MONTH_NAMES.get(first)
            year = int(clean_args[1]) if len(clean_args) >= 2 and clean_args[1].isdigit() else datetime.now().year
            if month_num:
                since = f"{year}-{month_num:02d}-01T00:00:00"
                until = f"{year}-{month_num+1:02d}-01T00:00:00" if month_num < 12 else f"{year+1}-01-01T00:00:00"
                label = f"{clean_args[0].capitalize()} {year}"

    if exchange:
        label += f" | {exchange}"

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    where = "WHERE 1=1"
    params = []
    if since:
        where += " AND timestamp>=?"
        params.append(since)
    if until:
        where += " AND timestamp<=?"
        params.append(until)
    if exchange:
        where += " AND exchange=?"
        params.append(exchange)

    c.execute(f'''SELECT coin, SUM(profit_usd), COUNT(*),
               SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
               FROM trades {where}
               GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 10''', params)
    top = c.fetchall()

    c.execute(f'''SELECT coin, SUM(profit_usd), COUNT(*)
               FROM trades {where}
               GROUP BY coin ORDER BY SUM(profit_usd) ASC LIMIT 5''', params)
    worst = c.fetchall()

    c.execute(f'''SELECT coin, AVG(distance), AVG(buffer), AVG(take_profit), COUNT(*), SUM(profit_usd)
               FROM trades {where} AND distance IS NOT NULL
               GROUP BY coin HAVING COUNT(*) >= 3
               ORDER BY SUM(profit_usd) DESC LIMIT 10''', params)
    dists = c.fetchall()

    conn.close()

    exch_label = f" 🏦{exchange}" if exchange else ""
    r  = f"📊 *Монеты — {label}*{exch_label}\n\n"

    # Топ монеты с умными дистансами
    conn2 = sqlite3.connect(DB_PATH)
    r += f"🏆 *Топ-{len(top)}:*\n"
    for i, (coin, p, cnt, w) in enumerate(top, 1):
        wr = round(w/cnt*100 if cnt > 0 else 0, 1)
        dist_data = get_smart_distance(conn2, where, params, coin)
        dist_str = fmt_dist_info(dist_data) if dist_data else ""
        r += f"{i}. *#{coin}*: {'+' if p>=0 else ''}{p:.2f}$ | WR {wr}%{dist_str} | {cnt} сделок\n"

    r += "\n💀 *Худшие 5:*\n"
    for coin, p, cnt in worst:
        r += f"  #{coin}: {'+' if p>=0 else ''}{p:.2f}$ | {cnt} сделок\n"

    # Анализ дистансов — только монеты с дистансами из топа
    dist_coins_data = []
    min_dist_coin = None
    max_dist_coin = None
    stable_coins = []

    for coin, p, cnt, w in top:
        dd = get_smart_distance(conn2, where, params, coin)
        if dd:
            dist_coins_data.append((coin, dd, p))
            if min_dist_coin is None or dd["min"] < min_dist_coin[1]:
                min_dist_coin = (coin, dd["min"])
            if max_dist_coin is None or dd["max"] > max_dist_coin[1]:
                max_dist_coin = (coin, dd["max"])
            if dd["spread"] <= 3:
                stable_coins.append((coin, dd["min"], dd["max"], dd["spread"]))

    if dist_coins_data:
        r += "\n📊 *Анализ дистансов:*\n"
        if min_dist_coin:
            r += f"  🔹 Мин. дистанс: *#{min_dist_coin[0]}* ({fmt_dist(min_dist_coin[1])})\n"
        if max_dist_coin:
            r += f"  🔸 Макс. дистанс: *#{max_dist_coin[0]}* ({fmt_dist(max_dist_coin[1])})\n"
        if stable_coins:
            r += "  📐 Стабильные (разброс ≤3%):\n"
            for coin, dmin, dmax, spread in sorted(stable_coins, key=lambda x: x[3])[:3]:
                r += f"    *#{coin}*: {fmt_dist(dmin)}—{fmt_dist(dmax)} (разброс {spread:.2f}%)\n"

    conn2.close()
    await update.message.reply_text(r, parse_mode="Markdown")

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

async def cmd_cost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статистику расходов на API."""
    r = (
        f"💰 *Расходы на Claude API*\n\n"
        f"Запросов: *{total_api_calls}*\n"
        f"Суммарно: *${total_api_cost:.4f}*\n"
        f"Средний запрос: *${(total_api_cost/total_api_calls):.4f}*\n\n"
        f"_(с момента последнего запуска бота)_"
    ) if total_api_calls > 0 else "💰 Запросов ещё не было."
    await update.message.reply_text(r, parse_mode="Markdown")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📖 *Команды бота:*\n\n"
        "/report — отчёт за 7 дней\n"
        "/report 30 — отчёт за 30 дней\n"
        "/report_daily — отчёт за сегодня\n"
        "/period январь 2026 — статистика за месяц\n"
        "/period 30 — статистика за N дней\n"
        "/top N — топ трейдеров\n"
        "/trader Денис март 2026 — статистика трейдера\n"
        "/coins — топ монет\n"
        "/coins binance — монеты только Binance\n"
        "/coins март 2026 bybit — монеты Bybit за март\n"
        "/coin BTCUSDT — статистика монеты по биржам\n"
        "/coin BTCUSDT март 2026 okx — монета на OKX\n"
        "/stats — общая статистика\n"
        "/cost — расходы на AI API\n"
        "/clear — очистить историю AI-диалога\n\n"
        f"💬 Упомяни @{BOT_USERNAME} или ответь на моё сообщение для AI-диалога."
    )
    await update.message.reply_text(text, parse_mode="Markdown")

def build_daily_report() -> str:
    now = datetime.now()
    today = now.replace(hour=0, minute=0, second=0).isoformat()
    week_ago = (now - timedelta(days=7)).isoformat()

    r = f"📊 *ДНЕВНОЙ ОТЧЁТ* | {now.strftime('%d.%m.%Y')}\n{'─'*25}\n\n"

    # Сегодня
    r += "📅 *СЕГОДНЯ*\n"
    r += get_stats_for_period(today)

    r += f"\n{'─'*25}\n📅 *НЕДЕЛЯ*\n"
    r += get_stats_for_period(week_ago)
    return r


async def scheduled_reports_loop(app: Application):
    sent_daily = None; sent_weekly = None
    while True:
        now      = datetime.now()
        day_key  = now.date()
        week_key = (now.isocalendar()[1], now.year)
        if now.hour == 20 and now.minute == 0:
            if sent_daily != day_key:
                try:
                    await app.bot.send_message(chat_id=REPORT_CHAT_ID, text=build_daily_report(), parse_mode="Markdown")
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

async def cmd_trader(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /trader Денис
    /trader Денис март 2026
    /trader Денис 2026-03
    """
    args = context.args
    if not args:
        await update.message.reply_text("Использование: /trader Денис или /trader Денис март 2026")
        return

    trader = args[0]
    since, until, label = None, None, "всё время"

    if len(args) >= 2:
        month_name = args[1].lower()
        month_num = MONTH_NAMES.get(month_name)
        year = int(args[2]) if len(args) >= 3 and args[2].isdigit() else datetime.now().year
        if month_num:
            since = f"{year}-{month_num:02d}-01T00:00:00"
            until = f"{year}-{month_num+1:02d}-01T00:00:00" if month_num < 12 else f"{year+1}-01-01T00:00:00"
            label = f"{args[1].capitalize()} {year}"
        elif re.match(r'\d{4}-\d{2}', args[1]):
            y, m = map(int, args[1].split('-'))
            since = f"{y}-{m:02d}-01T00:00:00"
            until = f"{y}-{m+1:02d}-01T00:00:00" if m < 12 else f"{y+1}-01-01T00:00:00"
            label = args[1]

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    where = "WHERE trader=?"
    params = [trader]
    if since:
        where += " AND timestamp>=?"
        params.append(since)
    if until:
        where += " AND timestamp<=?"
        params.append(until)

    # Общая статистика
    c.execute(f"SELECT COUNT(*), SUM(profit_usd), SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END) FROM trades {where}", params)
    total, pnl, wins = c.fetchone()
    if not total:
        await update.message.reply_text(f"❌ Трейдер *{trader}* не найден.", parse_mode="Markdown")
        conn.close()
        return

    wr = round((wins or 0) / total * 100 if total > 0 else 0, 1)
    pnl = pnl or 0

    # Топ монеты
    c.execute(f"""SELECT coin, SUM(profit_usd), COUNT(*),
                 AVG(distance), AVG(buffer), AVG(take_profit),
                 MIN(distance), MAX(distance)
                 FROM trades {where}
                 GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 10""", params)
    top_coins = c.fetchall()

    # Худшие монеты
    c.execute(f"""SELECT coin, SUM(profit_usd), COUNT(*),
                 AVG(distance), AVG(buffer), AVG(take_profit),
                 MIN(distance), MAX(distance)
                 FROM trades {where}
                 GROUP BY coin ORDER BY SUM(profit_usd) ASC LIMIT 5""", params)
    worst_coins = c.fetchall()

    # Биржи
    c.execute(f"SELECT exchange, COUNT(*) FROM trades {where} GROUP BY exchange ORDER BY COUNT(*) DESC", params)
    exchanges = c.fetchall()
    conn.close()

    exch_str = ", ".join([f"{e}({cnt})" for e, cnt in exchanges if e])

    r  = f"👤 *{trader}* | {label}\n"
    r += f"🏦 Биржи: {exch_str}\n"
    r += f"📊 Сделок: *{total}* | PnL: *{'+' if pnl>=0 else ''}{pnl:.2f}$* | WR: *{wr}%*\n\n"

    r += "🏆 *Топ-10 монет:*\n"
    for coin, p, cnt, dist, buf, tp, min_d, max_d in top_coins:
        d = fmt_dist(dist)
        b = fmt_dist(buf) if buf else ""
        t = f" tp={tp:.1f}%" if tp else ""
        b_str = f" buf={b}" if buf else ""
        r += f"  #{coin}: {'+' if p>=0 else ''}{p:.2f}$ ({cnt} сделок) dist={d}{b_str}{t}\n"

    r += "\n💀 *Худшие 5 монет:*\n"
    for coin, p, cnt, dist, buf, tp in worst_coins:
        d = f" dist={fmt_dist(dist)}" if dist else ""
        r += f"  #{coin}: {'+' if p>=0 else ''}{p:.2f}$ ({cnt} сделок){d}\n"

    await update.message.reply_text(r, parse_mode="Markdown")


async def cmd_coins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /coins — топ монет с дистансами за всё время
    /coins март 2026
    /coins 7 — за последние 7 дней
    """
    args = context.args
    since, until, label = None, None, "всё время"

    if args:
        if args[0].isdigit():
            days = int(args[0])
            since = (datetime.now() - timedelta(days=days)).isoformat()
            label = f"последние {days} дней"
        else:
            month_name = args[0].lower()
            month_num = MONTH_NAMES.get(month_name)
            year = int(args[1]) if len(args) >= 2 and args[1].isdigit() else datetime.now().year
            if month_num:
                since = f"{year}-{month_num:02d}-01T00:00:00"
                until = f"{year}-{month_num+1:02d}-01T00:00:00" if month_num < 12 else f"{year+1}-01-01T00:00:00"
                label = f"{args[0].capitalize()} {year}"

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    where = "WHERE distance IS NOT NULL"
    params = []
    if since:
        where += " AND timestamp>=?"
        params.append(since)
    if until:
        where += " AND timestamp<=?"
        params.append(until)

    c.execute(f"""SELECT coin, SUM(profit_usd), COUNT(*),
                 AVG(distance), AVG(buffer), AVG(take_profit),
                 SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
                 FROM trades {where}
                 GROUP BY coin HAVING COUNT(*) >= 3
                 ORDER BY SUM(profit_usd) DESC LIMIT 15""", params)
    rows = c.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("❌ Нет данных за этот период.")
        return

    r = f"📊 *Монеты с дистансами | {label}*\n\n"
    for i, (coin, pnl, cnt, dist, buf, tp, wins) in enumerate(rows, 1):
        wr = round((wins or 0)/cnt*100 if cnt > 0 else 0, 1)
        d = f"{dist:.2f}" if dist else "н/д"
        b = f"{buf:.2f}" if buf else "н/д"
        t = f"{tp:.1f}%" if tp else "н/д"
        r += f"{i}. *#{coin}*: {'+' if pnl>=0 else ''}{pnl:.2f}$ | WR {wr}%\n"
        r += f"   dist={d} buf={b} tp={t} | {cnt} сделок\n"

    await update.message.reply_text(r, parse_mode="Markdown")


async def cmd_coin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /coin NOMUSDT
    /coin NOMUSDT март 2026
    /coin NOMUSDT 7
    /coin NOMUSDT binance
    /coin NOMUSDT март 2026 bybit
    """
    args = context.args
    if not args:
        await update.message.reply_text("Использование: /coin NOMUSDT или /coin NOMUSDT март 2026 [binance|bybit|okx]")
        return

    coin = args[0].upper()
    if not coin.endswith('USDT'):
        coin += 'USDT'

    since, until, label = None, None, "всё время"
    exchange = None

    # Ищем биржу в оставшихся аргументах
    rest_args = []
    for a in args[1:]:
        exch = parse_exchange_arg(a)
        if exch:
            exchange = exch
        else:
            rest_args.append(a)

    if rest_args:
        if rest_args[0].isdigit():
            days = int(rest_args[0])
            since = (datetime.now() - timedelta(days=days)).isoformat()
            label = f"последние {days} дней"
        else:
            month_name = rest_args[0].lower()
            month_num = MONTH_NAMES.get(month_name)
            year = int(rest_args[1]) if len(rest_args) >= 2 and rest_args[1].isdigit() else datetime.now().year
            if month_num:
                since = f"{year}-{month_num:02d}-01T00:00:00"
                until = f"{year}-{month_num+1:02d}-01T00:00:00" if month_num < 12 else f"{year+1}-01-01T00:00:00"
                label = f"{rest_args[0].capitalize()} {year}"

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    where = "WHERE coin=?"
    params = [coin]
    if since:
        where += " AND timestamp>=?"
        params.append(since)
    if until:
        where += " AND timestamp<=?"
        params.append(until)
    if exchange:
        where += " AND exchange=?"
        params.append(exchange)

    # Общая статистика по монете
    c.execute(f"""SELECT COUNT(*), SUM(profit_usd),
                 SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END),
                 AVG(CASE WHEN distance>0 THEN distance END),
                 MIN(CASE WHEN distance>0 THEN distance END),
                 MAX(distance), AVG(CASE WHEN buffer>0 THEN buffer END),
                 AVG(take_profit)
                 FROM trades {where}""", params)
    row = c.fetchone()

    if not row or not row[0]:
        await update.message.reply_text(f"❌ Монета *#{coin}* не найдена.", parse_mode="Markdown")
        conn.close()
        return

    cnt, pnl, wins, avg_d, min_d, max_d, avg_b, avg_tp = row
    wr = round((wins or 0)/cnt*100 if cnt > 0 else 0, 1)
    pnl = pnl or 0

    # Статистика по каждой бирже с дистансами
    c.execute(f"""SELECT exchange,
                 COUNT(*), SUM(profit_usd),
                 SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END),
                 AVG(CASE WHEN distance>0 THEN distance END),
                 MIN(CASE WHEN distance>0 THEN distance END),
                 MAX(CASE WHEN distance>0 THEN distance END),
                 AVG(CASE WHEN buffer>0 THEN buffer END)
                 FROM trades {where}
                 GROUP BY exchange ORDER BY SUM(profit_usd) DESC""", params)
    exchanges = c.fetchall()

    # Топ трейдеров
    c.execute(f"""SELECT trader, COUNT(*), SUM(profit_usd),
                 SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END),
                 AVG(CASE WHEN distance>0 THEN distance END), exchange
                 FROM trades {where}
                 GROUP BY trader ORDER BY SUM(profit_usd) DESC LIMIT 8""", params)
    traders = c.fetchall()
    conn.close()

    exch_label = f" 🏦{exchange}" if exchange else ""
    r  = f"🪙 *#{coin}* | {label}{exch_label}\n"
    r += f"📊 Сделок: *{cnt}* | PnL: *{'+' if pnl>=0 else ''}{pnl:.2f}$* | WR: *{wr}%*\n"

    # Общий дистанс (если не фильтруем по бирже)
    if avg_d and not exchange:
        dist_data = get_smart_distance(conn if False else sqlite3.connect(DB_PATH), where, params, coin)
        if dist_data and dist_data.get("smart"):
            r += f"📐 Дистанс: ⚡️{fmt_dist(dist_data['working'])} 🛡{fmt_dist(dist_data['insurance'])} ({fmt_dist(min_d)}–{fmt_dist(max_d)})\n"
        else:
            r += f"📐 Дистанс: avg={fmt_dist(avg_d)} ({fmt_dist(min_d)}–{fmt_dist(max_d)})\n"
    if avg_b and not exchange:
        r += f"📏 Буфер: avg={fmt_dist(avg_b)}\n"
    if avg_tp:
        r += f"🎯 Тейк: avg={avg_tp:.1f}%\n"

    # Разбивка по биржам с дистансами
    if exchanges and not exchange:
        r += "\n🏦 *По биржам:*\n"
        conn3 = sqlite3.connect(DB_PATH)
        for exch, ecnt, epnl, ewins, edist, edmin, edmax, ebuf in exchanges:
            if not exch:
                continue
            ewr = round((ewins or 0)/ecnt*100 if ecnt > 0 else 0, 1)
            r += f"  *{exch}*: {'+' if epnl>=0 else ''}{(epnl or 0):.2f}$ | {ecnt} сделок | WR {ewr}%\n"
            if edist:
                # Умный дистанс по конкретной бирже
                w_exch = "WHERE coin=? AND exchange=?"
                p_exch = [coin, exch]
                if since:
                    w_exch += " AND timestamp>=?"; p_exch.append(since)
                if until:
                    w_exch += " AND timestamp<=?"; p_exch.append(until)
                dd = get_smart_distance(conn3, w_exch, p_exch, coin)
                if dd and dd.get("smart"):
                    r += f"    📐 ⚡️{fmt_dist(dd['working'])} 🛡{fmt_dist(dd['insurance'])} ({fmt_dist(edmin)}–{fmt_dist(edmax)})\n"
                else:
                    r += f"    📐 dist={fmt_dist(edist)} ({fmt_dist(edmin)}–{fmt_dist(edmax)})\n"
                if ebuf:
                    r += f"    📏 buf={fmt_dist(ebuf)}\n"
        conn3.close()
    elif exchange and avg_d:
        # Показываем дистанс для выбранной биржи
        conn3 = sqlite3.connect(DB_PATH)
        w_exch = "WHERE coin=? AND exchange=?"
        p_exch = [coin, exchange]
        if since:
            w_exch += " AND timestamp>=?"; p_exch.append(since)
        if until:
            w_exch += " AND timestamp<=?"; p_exch.append(until)
        dd = get_smart_distance(conn3, w_exch, p_exch, coin)
        conn3.close()
        if dd and dd.get("smart"):
            r += f"📐 Дистанс: ⚡️{fmt_dist(dd['working'])} 🛡{fmt_dist(dd['insurance'])} ({fmt_dist(dd['min'])}–{fmt_dist(dd['max'])})\n"
        else:
            r += f"📐 Дистанс: avg={fmt_dist(avg_d)} ({fmt_dist(min_d)}–{fmt_dist(max_d)})\n"
        if avg_b:
            r += f"📏 Буфер: avg={fmt_dist(avg_b)}\n"

    r += "\n👤 *Трейдеры:*\n"
    for trader, tcnt, tpnl, twins, tdist, texch in traders:
        twr = round((twins or 0)/tcnt*100 if tcnt > 0 else 0, 1)
        d = f" dist={fmt_dist(tdist)}" if tdist else ""
        exch_tag = f" [{texch}]" if texch and not exchange else ""
        r += f"  *{trader}*{exch_tag}: {'+' if tpnl>=0 else ''}{(tpnl or 0):.2f}$ | {tcnt} сделок | WR {twr}%{d}\n"

    await update.message.reply_text(r, parse_mode="Markdown")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Глобальный обработчик ошибок."""
    logger.error("Exception while handling update:", exc_info=context.error)
    # Пробуем уведомить пользователя если возможно
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("⚠️ Произошла ошибка. Попробуй ещё раз.")
        except Exception:
            pass


async def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("trader",       cmd_trader))
    app.add_handler(CommandHandler("coins",        cmd_coins))
    app.add_handler(CommandHandler("coin",         cmd_coin))
    app.add_handler(CommandHandler("report",       cmd_report))
    app.add_handler(CommandHandler("report_daily", cmd_report_daily))
    app.add_handler(CommandHandler("period",       cmd_period))
    app.add_handler(CommandHandler("top",          cmd_top))
    app.add_handler(CommandHandler("stats",        cmd_stats))
    app.add_handler(CommandHandler("cost",         cmd_cost))
    app.add_handler(CommandHandler("clear",        cmd_clear_history))
    app.add_handler(CommandHandler("help",         cmd_help))
    app.add_handler(CommandHandler("start",        cmd_help))
    app.add_handler(MessageHandler(filters.Chat(SOURCE_CHAT_ID) & filters.ALL, handle_trade_message))
    app.add_handler(MessageHandler(filters.Chat(DIALOG_CHAT_ID) & filters.TEXT & ~filters.COMMAND, handle_dialog_message))
    app.add_error_handler(error_handler)
    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        logger.info("✅ Bot started!")
        await scheduled_reports_loop(app)

if __name__ == "__main__":
    asyncio.run(main())
