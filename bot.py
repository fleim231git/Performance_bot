import os
import re
import sqlite3
import logging
import asyncio
from datetime import datetime, timedelta
from anthropic import Anthropic
import tempfile
import os as _os
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
REPORT_THREAD_ID  = int(os.environ.get("REPORT_THREAD_ID", "0")) or None
DIALOG_CHAT_ID    = int(os.environ.get("DIALOG_CHAT_ID", "0"))
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
BOT_USERNAME      = os.environ.get("BOT_USERNAME", "")

STAT_CHAT_IDS_RAW = os.environ.get("STAT_CHAT_IDS", "")
STAT_CHAT_IDS     = set(int(x.strip()) for x in STAT_CHAT_IDS_RAW.split(",") if x.strip())

DB_PATH = "/data/trades.db"
anthropic_client = Anthropic(api_key=ANTHROPIC_API_KEY)
OPENAI_API_KEY = _os.environ.get("OPENAI_API_KEY")
openai_client  = None  # Whisper disabled — install openai package to enable
conversation_history: dict[int, list] = {}
MAX_HISTORY = 10

# Счётчик стоимости API запросов
COST_INPUT_PER_TOKEN  = 0.80 / 1_000_000
COST_OUTPUT_PER_TOKEN = 4.0 / 1_000_000
total_api_cost = 0.0
total_api_calls = 0

# ─── МАППИНГ АББРЕВИАТУР К ИМЕНАМ ТРЕЙДЕРОВ ───────────────────────────────
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
    if not name:
        return name
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
    if exchange.upper() in ['BINANCE', 'BYBIT', 'OKX']:
        return exchange.capitalize() if exchange.upper() != 'BYBIT' else 'Bybit'
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
    for col, typ in [("distance","REAL"),("buffer","REAL"),("take_profit","REAL"),
                      ("source","TEXT DEFAULT 'live'"),
                      ("delta_min","REAL"),("delta_max","REAL"),("is_observ","INTEGER DEFAULT 0")]:
        try:
            c.execute(f'ALTER TABLE trades ADD COLUMN {col} {typ}')
        except Exception:
            pass

    # Knowledge base — заметки, правила, наблюдения
    c.execute('''CREATE TABLE IF NOT EXISTS knowledge (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        created   TEXT DEFAULT (datetime('now')),
        category  TEXT,
        content   TEXT,
        source    TEXT DEFAULT 'user'
    )''')

    # Feedback — оценки ответов бота
    c.execute('''CREATE TABLE IF NOT EXISTS feedback (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        created   TEXT DEFAULT (datetime('now')),
        user_id   INTEGER,
        question  TEXT,
        answer    TEXT,
        rating    INTEGER,
        comment   TEXT
    )''')

    # Query log — что спрашивают (для аналитики и обучения)
    c.execute('''CREATE TABLE IF NOT EXISTS query_log (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        created   TEXT DEFAULT (datetime('now')),
        user_id   INTEGER,
        question  TEXT,
        tools_used TEXT,
        tokens_in  INTEGER,
        tokens_out INTEGER,
        cost       REAL
    )''')
    conn.commit()

    c.execute("SELECT COUNT(*) FROM trades WHERE source='stat'")
    stat_count = c.fetchone()[0]
    if stat_count == 0:
        import glob as _glob
        import gzip as _gzip
        import csv as _csv
        csv_files = _glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)), "stat_*.csv.gz"))
        for csv_file in csv_files:
            logger.info(f"📦 Importing stat data from {csv_file}...")
            try:
                imported = 0
                with _gzip.open(csv_file, 'rt', encoding='utf-8', newline='') as f:
                    reader = _csv.reader(f)
                    batch = []
                    for row in reader:
                        if len(row) < 13:
                            continue
                        ts, trader, exch, side, coin, dist, buf, tp, pnl, pct, is_p, raw, src = row[:13]
                        d_min = float(row[13]) if len(row) > 13 and row[13] else None
                        d_max = float(row[14]) if len(row) > 14 and row[14] else None
                        obs = int(row[15]) if len(row) > 15 and row[15] else 0
                        batch.append((
                            ts, trader, exch, side, coin,
                            float(dist) if dist else None,
                            float(buf) if buf else None,
                            float(tp) if tp else None,
                            float(pnl) if pnl else None,
                            float(pct) if pct else None,
                            int(is_p) if is_p else 0,
                            raw, src, d_min, d_max, obs
                        ))
                        if len(batch) >= 5000:
                            c.executemany(
                                '''INSERT INTO trades(timestamp,trader,exchange,side,coin,
                                   distance,buffer,take_profit,profit_usd,profit_pct,
                                   is_profit,raw_message,source,delta_min,delta_max,is_observ)
                                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', batch
                            )
                            conn.commit()
                            imported += len(batch)
                            batch = []
                            if imported % 50000 == 0:
                                logger.info(f"  ... {imported} trades imported")
                    if batch:
                        c.executemany(
                            '''INSERT INTO trades(timestamp,trader,exchange,side,coin,
                               distance,buffer,take_profit,profit_usd,profit_pct,
                               is_profit,raw_message,source,delta_min,delta_max,is_observ)
                               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', batch
                        )
                        conn.commit()
                        imported += len(batch)
                logger.info(f"✅ Imported {csv_file}: {imported} trades")
            except Exception as e:
                logger.error(f"❌ Failed to import {csv_file}: {e}")

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

        dist_buf_match = re.search(r'\b(?:BUY|SELL)\s+([\d.]+)\s+([\d.]+)', text, re.IGNORECASE)
        if dist_buf_match:
            distance = float(dist_buf_match.group(1))
            buffer   = float(dist_buf_match.group(2))
        else:
            dist_buf_dash = re.search(r'\b(?:BUY|SELL)-([\d.]+)-([\d.]+)-', text, re.IGNORECASE)
            if dist_buf_dash:
                distance = float(dist_buf_dash.group(1))
                buffer   = float(dist_buf_dash.group(2))
            else:
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

        if profit_pct is not None and profit_usd != 0:
            if profit_pct > 110:
                profit_usd = round(profit_usd * (110 / profit_pct), 4)
                profit_pct = 110.0
            elif profit_pct < -106:
                profit_usd = round(profit_usd * (-106 / profit_pct), 4)
                profit_pct = -106.0

        if is_profit is None:
            is_profit = 1 if profit_usd >= 0 else 0

        trader_raw = trader
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
            profit_usd,profit_pct,is_profit,raw_message,source,
            delta_min,delta_max,is_observ)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (datetime.now().isoformat(),
         trade["trader"], trade["exchange"], trade["side"], trade["coin"],
         trade.get("distance"), trade.get("buffer"), trade.get("take_profit"),
         trade["profit_usd"], trade["profit_pct"],
         trade["is_profit"], trade["raw_message"],
         trade.get("source", "live"),
         trade.get("delta_min"), trade.get("delta_max"),
         trade.get("is_observ", 0))
    )
    conn.commit()
    conn.close()


STAT_EXCHANGE_MAP = {
    'bnc': 'Binance',
    'bbt': 'Bybit',
    'okx': 'OKX',
}

def parse_stat_trade(text: str) -> dict | None:
    try:
        is_profit = 1 if '⬆' in text or 'Profit' in text else (0 if '⬇' in text or 'Loss' in text else None)

        trader_match = re.search(r'(STATS-[a-z]+-\d+|stat\d+)', text, re.IGNORECASE)
        trader = trader_match.group(1) if trader_match else "Unknown"

        exch_match = re.search(r'STATS-([a-z]+)-', trader, re.IGNORECASE)
        if exch_match:
            exchange = STAT_EXCHANGE_MAP.get(exch_match.group(1).lower(), "Unknown")
        else:
            if 'Binance' in text:
                exchange = 'Binance'
            elif 'Bybit' in text or 'ByBit' in text:
                exchange = 'Bybit'
            elif 'OKX' in text:
                exchange = 'OKX'
            else:
                exchange = 'Unknown'

        side_match = re.search(r'\b(BUY|SELL)\b', text, re.IGNORECASE)
        side = side_match.group(1).upper() if side_match else "Unknown"

        dist_match = re.search(r'\b(?:BUY|SELL)\s+([\d.]+)\s+', text, re.IGNORECASE)
        distance = float(dist_match.group(1)) if dist_match else None

        delta_match = re.search(r'\b(?:BUY|SELL)\s+[\d.]+\s+([\d.]+)-([\d.]+)', text, re.IGNORECASE)
        delta_min = float(delta_match.group(1)) if delta_match else None
        delta_max = float(delta_match.group(2)) if delta_match else None

        pnl_match = re.search(r'(?:Profit|Loss)\s*([+-]?\d+\.?\d*)[$]', text, re.IGNORECASE)
        profit_usd = float(pnl_match.group(1)) if pnl_match else None

        pct_match = re.search(r'[$]\s*\(([+-]?\d+\.?\d*)%\)', text)
        profit_pct = float(pct_match.group(1)) if pct_match else None

        coin_match = re.search(r'#([^\s]+USDT)', text)
        coin = coin_match.group(1) if coin_match else None

        tp_matches = re.findall(r'\(([+-]?\d+\.?\d*)%\)', text)
        take_profit = float(tp_matches[-1]) if tp_matches else None

        if not coin or profit_usd is None:
            return None

        if profit_pct is not None and profit_usd != 0:
            if profit_pct > 110:
                profit_usd = round(profit_usd * (110 / profit_pct), 4)
                profit_pct = 110.0
            elif profit_pct < -106:
                profit_usd = round(profit_usd * (-106 / profit_pct), 4)
                profit_pct = -106.0

        if is_profit is None:
            is_profit = 1 if profit_usd >= 0 else 0

        is_observ = 'OBSERV' in text

        return {
            "trader": trader, "exchange": exchange, "side": side,
            "coin": coin, "distance": distance, "buffer": None,
            "take_profit": take_profit, "profit_usd": profit_usd,
            "profit_pct": profit_pct, "is_profit": is_profit,
            "raw_message": text[:500], "source": "stat",
            "delta_min": delta_min, "delta_max": delta_max,
            "is_observ": 1 if is_observ else 0,
        }
    except Exception as e:
        logger.error(f"parse_stat_trade error: {e}")
        return None

def get_exchange_stats_for_period(since: str, until: str = None) -> str:
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
            continue
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

    c.execute(f'''SELECT coin, SUM(profit_usd), COUNT(*),
                 MIN(CASE WHEN distance>0 THEN distance END),
                 MAX(CASE WHEN distance>0 THEN distance END),
                 AVG(CASE WHEN take_profit!=0 AND take_profit IS NOT NULL THEN take_profit END)
                 FROM trades {where} GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 5''', params)
    top_coins = c.fetchall()

    c.execute(f'SELECT coin, SUM(profit_usd), COUNT(*) FROM trades {where} GROUP BY coin ORDER BY SUM(profit_usd) ASC LIMIT 5', params)
    worst_coins = c.fetchall()

    c.execute(f'SELECT trader, COUNT(*), SUM(profit_usd), SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END) FROM trades {where} GROUP BY trader ORDER BY SUM(profit_usd) DESC LIMIT 10', params)
    traders = c.fetchall()

    wr = round(wins/total*100 if total > 0 else 0, 1)
    r  = f"📊 Сделок: *{total}* | PnL: *{'+' if pnl>=0 else ''}{pnl:.2f}$* | WR: *{wr}%*\n\n"

    VALID_EXCHANGES = ["Binance", "Bybit", "OKX"]
    for exch in VALID_EXCHANGES:
        exch_where = where + " AND exchange=?"
        exch_params = list(params) + [exch]
        c.execute(f'''SELECT coin, SUM(profit_usd), COUNT(*),
                     MIN(CASE WHEN distance>0 THEN distance END),
                     MAX(CASE WHEN distance>0 THEN distance END),
                     AVG(CASE WHEN take_profit!=0 AND take_profit IS NOT NULL THEN take_profit END)
                     FROM trades {exch_where}
                     GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 5''', exch_params)
        exch_coins = c.fetchall()
        if not exch_coins:
            continue
        top_positive = [(coin, p, cnt, dmin, dmax, tp) for coin, p, cnt, dmin, dmax, tp in exch_coins if (p or 0) > 0]
        if not top_positive:
            continue
        r += f"🏆 *Топ-5 монет {exch}:*\n"
        for i, (coin, p, cnt, dmin, dmax, tp) in enumerate(top_positive, 1):
            dist_data = get_smart_distance(conn, exch_where, exch_params, coin)
            dist_str = fmt_dist_info(dist_data) if dist_data else ""
            tp_str = f" | tp={tp:.1f}%" if tp else ""
            r += f"{i}. #{coin} {'+' if p>=0 else ''}{(p or 0):.2f}$ ({cnt} сделок){dist_str}{tp_str}\n"
        r += "\n"

    conn.close()
    r += "💀 *Худшие 5 монет:*\n"
    for i, (coin, p, cnt) in enumerate(worst_coins, 1):
        r += f"{i}. #{coin} {'+' if p>=0 else ''}{p:.2f}$ ({cnt} сделок)\n"

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
    ctx += "\n[Для деталей по трейдерам, монетам и периодам — используй tools]\n"
    return ctx

SYSTEM_PROMPT = """Ты — аналитический ассистент трейдинговой группы с прямым доступом к живой базе сделок.
База обновляется в реальном времени.

━━━ ПРАВИЛО 1 — ТОЛЬКО ФАКТЫ ━━━
- Только цифры из базы. Нет данных → говори "нет данных"
- Анализ строго по цифрам: "WR 58%, средний по группе 70%" — не интерпретируй эмоционально
- Запрещены домыслы без данных: "биржа мертва", "трейдишь против себя", "фатальная ошибка"
- Запрещены оценочные слова: "мертва", "король", "идеальный", "мусор", "катастрофа"
- Не объясняй почему биржа "работает" или "не работает" — в базе только PnL и количество сделок

━━━ ПРАВИЛО 2 — ТОЧНЫЙ СЧЁТ ━━━
- Tool пишет "Список содержит ровно N монет" и "ИТОГО: N монет" — используй это число в заголовке
- Каждая монета в ответе ровно один раз
- Не добавляй монеты которых нет в данных от tool

━━━ ПРАВИЛО 3 — ДИСТАНСЫ ━━━
- Дистанс и буфер — проценты. Всегда пиши % : "1.93%", "0.77%"
- ⚡️ = рабочий дистанс (медиана прибыльных сделок)
- 🛡 = страховочный дистанс (90-й перцентиль) — выше нет смысла ставить
- 🎯 = стабильный дистанс (разброс ≤3%)
- Копируй дистансы из tool точно как написаны — не пересчитывай

━━━ ПРАВИЛО 4 — ФОРМАТ ОТВЕТА ━━━
- Пиши нативным текстом, без markdown таблиц (| col | col |)
- Для монет — простой нумерованный список:
  1. #STOUSDT — +330$ | ⚡️10.50% 🛡12.00% (8.00%–14.00%)
  2. #ZECUSDT — +83$ | 🎯0.72% (0.60%–1.00%)
- Приоритет отображения: профит → рабочий дистанс → диапазон мин/макс
- Тейк профит показывай только если пользователь прямо просит
- WR и буфер — только если пользователь прямо просит
- Анализ — 2-3 строки, без лишних заголовков

━━━ ПРАВИЛО 5 — ЛОГИКА ДИСТАНСОВ ━━━
- Высокий дистанс = нужен большой импульс = БЕЗОПАСНЕЕ от случайного сноса
- Низкий дистанс = ордер сносится маленьким движением = РИСКОВАННЕЕ
- Низкий дистанс = меньше тейк-профит, высокий = больше тейк-профит
- Лучшая монета = profit_usd × win_rate, не размер дистанса

━━━ ПРАВИЛО 6 — БИРЖИ ━━━
- Три биржи: Binance, Bybit, OKX
- Дистансы могут сильно отличаться по биржам для одной монеты
- get_top_coins и get_coin_stats поддерживают параметр exchange
- Если просят топ по биржам — показывай ТОЛЬКО по биржам, без общего топа
- Общий топ показывай только если прямо просят "общий" или "по всем биржам"

━━━ ПРАВИЛО 7 — ИСТОЧНИКИ ДАННЫХ (source) ━━━
- В базе два типа данных: live (реальные трейдеры) и stat (статистические аккаунты)
- По умолчанию все tools показывают только live данные
- Если пользователь говорит "стат", "статистические", "тестовые", "stat" — передай source="stat"
- Если говорит "все данные", "общие", "оба" — передай source="all"
- Stat-аккаунты торгуют на маленьком ордер-сайзе — PnL в $ НЕ показывать для stat
- Для stat показывай profit_pct (%) вместо profit_usd ($) — это процент к ордеру
- Формат для stat: #COIN +29.8% | dist=0.85% | 50 сделок
- Для сравнения stat и live используй проценты (WR, profit_pct), не доллары

━━━ ПРАВИЛО 8 — ДЕЛЬТЫ (15-мин абсолютная дельта) ━━━
- Дельта — процент изменения цены за 15-минутный период в момент входа в сделку
- Абсолютная дельта всегда положительная (мин-макс движения цены)
- Хранится как диапазон: delta_min и delta_max (например 3-5 = дельта была 3-5%)
- Используй get_delta_analysis для вопросов типа "при какой дельте лучше входить"
- Можно фильтровать по монете, дистансу, бирже, стороне (BUY/SELL)
- Формат: δ0-1% (дельта 0-1%), δ3-5% (дельта 3-5%)

━━━ ПРАВИЛО 8.1 — ВРЕМЯ ━━━
- Timestamp хранится с точностью до секунды
- "за последний час" → get_period_stats с since = текущее время − 1 час
- "с 18:00" → since = сегодня T18:00:00

━━━ ПРАВИЛО 9 — ПАМЯТЬ И ОБУЧЕНИЕ ━━━
- manage_knowledge — сохраняй заметки когда пользователь говорит "запомни", "правило:", "заметка:"
- Категории: правило, монета, трейдер, стратегия, наблюдение, важно
- Перед ответом проверяй БАЗА ЗНАНИЙ в контексте — там могут быть подсказки
- Если в ФИДБЕК есть замечания — учитывай их
- Пользователь может оценить ответ: 👍/👎/+/- или "плохо: причина"

━━━ ПРАВИЛО 10 — ПОИСК В ИНТЕРНЕТЕ ━━━
- Используй web_search когда пользователь просит найти что-то в интернете
- Например: "найди новости по BTC", "что случилось с AAVE", "поищи эксплойт KelpDAO"
- Если монеты НЕТ в базе и пользователь спрашивает про неё — используй web_search чтобы найти информацию
- Для вопросов о монетах из базы — сначала ищи в базе через tools
- Не используй web_search для аналитики по базе данных — только SQL tools
- После поиска кратко резюмируй найденное, не цитируй большие куски текста

Отвечай на том языке на котором спрашивают. В конце каждого ответа добавляй 🦇"""


EXCHANGE_ALIASES = {
    'binance': 'Binance', 'bnc': 'Binance', 'бинанс': 'Binance',
    'bybit': 'Bybit', 'bbt': 'Bybit', 'байбит': 'Bybit',
    'okx': 'OKX', 'okex': 'OKX', 'окекс': 'OKX', 'окх': 'OKX',
}

def parse_exchange_arg(arg: str) -> str | None:
    return EXCHANGE_ALIASES.get(arg.lower())

def get_distance_stats(since: str, until: str = None) -> str:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if until:
        c.execute(
            """SELECT coin, AVG(distance), AVG(buffer), AVG(take_profit), COUNT(*), SUM(profit_usd)
               FROM trades WHERE distance IS NOT NULL AND timestamp>=? AND timestamp<=?
               GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 10""",
            (since, until)
        )
    else:
        c.execute(
            """SELECT coin, AVG(distance), AVG(buffer), AVG(take_profit), COUNT(*), SUM(profit_usd)
               FROM trades WHERE distance IS NOT NULL AND timestamp>=?
               GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 10""",
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

    if profit_dists:
        n = len(profit_dists)
        if n % 2 == 1:
            working = profit_dists[n // 2]
        else:
            working = (profit_dists[n // 2 - 1] + profit_dists[n // 2]) / 2
    else:
        working = sum(all_dists) / len(all_dists)

    idx = int(len(all_dists) * 0.9)
    insurance = all_dists[min(idx, len(all_dists) - 1)]
    insurance = max(insurance, working)

    return {
        "avg": sum(all_dists) / len(all_dists),
        "min": dmin, "max": dmax, "spread": spread,
        "working": working, "insurance": insurance,
        "smart": True
    }


def fmt_dist(d: float | None) -> str:
    if d is None:
        return "н/д"
    return f"{d:.2f}%"


def fmt_dist_info(dist_data: dict | None) -> str:
    if not dist_data:
        return ""
    if dist_data.get("smart"):
        return (f" ⚡️{fmt_dist(dist_data['working'])} 🛡{fmt_dist(dist_data['insurance'])}"
                f" ({fmt_dist(dist_data['min'])}–{fmt_dist(dist_data['max'])})")
    else:
        return f" 🎯{fmt_dist(dist_data['avg'])} ({fmt_dist(dist_data['min'])}–{fmt_dist(dist_data['max'])})"


# ─── TOOLS для Claude ─────────────────────────────────────────────────────────
SOURCE_PARAM = {"type": "string", "description": "Источник данных: live (реальные трейдеры, по умолчанию), stat (статистические аккаунты), all (оба)"}

TOOLS = [
    {
        "name": "get_trader_stats",
        "description": "Получить статистику трейдера по монетам с дистансами за период. Можно фильтровать по бирже и сортировать по прибыли или убытку.",
        "input_schema": {
            "type": "object",
            "properties": {
                "trader": {"type": "string", "description": "Имя трейдера"},
                "since": {"type": "string", "description": "Дата/время начала. Форматы: YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS"},
                "until": {"type": "string", "description": "Дата/время конца (опционально)"},
                "exchange": {"type": "string", "description": "Биржа: Binance, Bybit, OKX (опционально)"},
                "sort_by": {"type": "string", "description": "Сортировка: profit (лучшие) или loss (худшие/убыточные)"},
                "limit": {"type": "integer", "description": "Количество монет (по умолчанию 10)"},
                "source": SOURCE_PARAM
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
                "since": {"type": "string", "description": "Дата/время начала (опционально)"},
                "until": {"type": "string", "description": "Дата/время конца (опционально)"},
                "source": SOURCE_PARAM
            },
            "required": ["coin"]
        }
    },
    {
        "name": "get_top_coins",
        "description": "Получить топ монет с дистансами за период. Фильтр по бирже через параметр exchange.",
        "input_schema": {
            "type": "object",
            "properties": {
                "since": {"type": "string", "description": "Дата/время начала (опционально)"},
                "until": {"type": "string", "description": "Дата/время конца (опционально)"},
                "limit": {"type": "integer", "description": "Количество монет (по умолчанию 10)"},
                "min_distance": {"type": "number", "description": "Минимальный дистанс для фильтрации"},
                "max_distance": {"type": "number", "description": "Максимальный дистанс для фильтрации"},
                "sort_by": {"type": "string", "description": "Сортировка: profit (лучшие) или loss (худшие)"},
                "exchange": {"type": "string", "description": "Фильтр по бирже: Binance, Bybit, OKX (опционально)"},
                "source": SOURCE_PARAM
            }
        }
    },
    {
        "name": "get_all_traders",
        "description": "Получить полный список всех трейдеров в базе с их общей статистикой",
        "input_schema": {
            "type": "object",
            "properties": {
                "since": {"type": "string", "description": "Дата начала (опционально)"},
                "until": {"type": "string", "description": "Дата конца (опционально)"},
                "source": SOURCE_PARAM
            }
        }
    },
    {
        "name": "get_period_stats",
        "description": "Получить общую статистику за период. Поддерживает запросы по часам.",
        "input_schema": {
            "type": "object",
            "properties": {
                "since": {"type": "string", "description": "Дата/время начала. Форматы: YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS"},
                "until": {"type": "string", "description": "Дата/время конца (опционально)"},
                "source": SOURCE_PARAM
            },
            "required": ["since"]
        }
    },
    {
        "name": "get_delta_analysis",
        "description": "Универсальный анализ по 15-минутной абсолютной дельте. group_by определяет разбивку: 'delta' (ROE по каждой дельте), 'coin' (ROE по монетам с лучшей дельтой), 'distance' (ROE по дистансам), 'combo' (топ комбинаций монета+дистанс+дельта). Используй для: при какой дельте лучше входить, лучшая комбинация для монеты, сравнение дельт по дистансам.",
        "input_schema": {
            "type": "object",
            "properties": {
                "group_by": {"type": "string", "description": "Группировка: delta (по дельтам), coin (по монетам), distance (по дистансам), combo (монета+дистанс+дельта). По умолчанию delta"},
                "coin": {"type": "string", "description": "Монета (опционально)"},
                "distance": {"type": "number", "description": "Точный дистанс (опционально)"},
                "min_distance": {"type": "number", "description": "Минимальный дистанс (опционально)"},
                "max_distance": {"type": "number", "description": "Максимальный дистанс (опционально)"},
                "delta_range": {"type": "string", "description": "Диапазон дельты, напр. '3-5' (опционально)"},
                "side": {"type": "string", "description": "BUY или SELL (опционально)"},
                "exchange": {"type": "string", "description": "Биржа: Binance, Bybit, OKX (опционально)"},
                "since": {"type": "string", "description": "Дата начала (опционально)"},
                "until": {"type": "string", "description": "Дата конца (опционально)"},
                "limit": {"type": "integer", "description": "Кол-во результатов (по умолчанию 15)"},
                "min_trades": {"type": "integer", "description": "Минимум сделок для включения (по умолчанию 10)"},
                "source": SOURCE_PARAM
            }
        }
    },
    {
        "name": "manage_knowledge",
        "description": "Управление базой знаний: добавить заметку, найти по ключевому слову, или получить все записи категории. Используй когда пользователь говорит 'запомни', 'добавь правило', 'заметка:' или спрашивает что ты помнишь.",
        "input_schema": {
            "type": "object",
            "properties": {
                "action": {"type": "string", "description": "add (добавить), search (найти), list (все записи категории), delete (удалить по id)"},
                "category": {"type": "string", "description": "Категория: правило, монета, трейдер, стратегия, наблюдение, важно"},
                "content": {"type": "string", "description": "Текст заметки (для add)"},
                "query": {"type": "string", "description": "Поисковый запрос (для search)"},
                "id": {"type": "integer", "description": "ID записи (для delete)"}
            },
            "required": ["action"]
        }
    },
    # ─── WEB SEARCH ───────────────────────────────────────────────────────────
    {
        "type": "web_search_20250305",
        "name": "web_search",
        "max_uses": 3
    }
]


def parse_dt(dt_str: str, end_of_day: bool = False) -> str:
    if not dt_str:
        return dt_str
    if "T" in dt_str:
        return dt_str
    if end_of_day:
        return dt_str + "T23:59:59"
    return dt_str + "T00:00:00"


def apply_source_filter(where: str, params: list, source: str | None) -> tuple[str, list]:
    if not source or source == "live":
        where += " AND (source='live' OR source IS NULL)"
    elif source == "stat":
        where += " AND source='stat'"
    return where, params


def execute_tool(tool_name: str, tool_input: dict) -> str:
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
            source = tool_input.get("source")

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
            where, params = apply_source_filter(where, params, source)

            is_stat = source == "stat"

            c.execute(f"""SELECT COUNT(*), SUM(profit_usd),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END),
                         SUM(profit_pct)
                         FROM trades {where}""", params)
            row = c.fetchone()
            if not row or not row[0]:
                return f"Трейдер {trader} не найден."

            cnt, pnl, wins, sum_pct = row
            wr = round((wins or 0)/cnt*100 if cnt > 0 else 0, 1)

            order = "ASC" if sort_by == "loss" else "DESC"
            c.execute(f"""SELECT coin, SUM(profit_usd), COUNT(*),
                         MIN(CASE WHEN distance>0 THEN distance END),
                         MAX(CASE WHEN distance>0 THEN distance END),
                         AVG(CASE WHEN take_profit!=0 THEN take_profit END),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END),
                         SUM(profit_pct)
                         FROM trades {where}
                         GROUP BY coin ORDER BY SUM(profit_usd) {order} LIMIT {limit}""", params)
            top_coins = c.fetchall()

            c.execute(f"""SELECT exchange, COUNT(*), SUM(profit_usd), SUM(profit_pct) FROM trades {where}
                         GROUP BY exchange ORDER BY COUNT(*) DESC""", params)
            exchanges = c.fetchall()

            exch_filter = f" [{exchange}]" if exchange else ""
            result = f"Трейдер: {trader}{exch_filter}\n"
            if is_stat:
                result += f"Сделок: {cnt} | ROE {'+' if (sum_pct or 0)>=0 else ''}{(sum_pct or 0):.1f}% | WR: {wr}%\n"
                exch_parts = [f"{e}({n}) ROE {'+' if (sp or 0)>=0 else ''}{(sp or 0):.1f}%" for e,n,p,sp in exchanges if e]
            else:
                result += f"Сделок: {cnt} | PnL: {'+' if pnl>=0 else ''}{(pnl or 0):.2f}$ | WR: {wr}%\n"
                exch_parts = [f"{e}({n}) {'+' if p>=0 else ''}{(p or 0):.1f}$" for e,n,p,sp in exchanges if e]
            result += f"Биржи: {', '.join(exch_parts)}\n\n"
            coin_lines = []
            for coin, p, n, dmin, dmax, tp, wins, coin_sum_pct in top_coins:
                dist_data = get_smart_distance(conn, where, params, coin)
                if is_stat:
                    coin_wr = round((wins or 0)/n*100 if n > 0 else 0, 1)
                    line = f"#{coin}: ROE {'+' if (coin_sum_pct or 0)>=0 else ''}{(coin_sum_pct or 0):.1f}% | WR {coin_wr}%"
                else:
                    line = f"#{coin}: {'+' if p>=0 else ''}{(p or 0):.2f}$"
                if dmin and dmax:
                    if dist_data and dist_data.get("smart"):
                        line += f" | ⚡️{fmt_dist(dist_data['working'])} 🛡{fmt_dist(dist_data['insurance'])} ({fmt_dist(dmin)}–{fmt_dist(dmax)})"
                    else:
                        line += f" | 🎯{fmt_dist(dmin)}–{fmt_dist(dmax)}"
                if tp: line += f" | tp={tp:.1f}%"
                line += f" ({n} сделок)"
                coin_lines.append(line)
            result += f"Список содержит ровно {len(coin_lines)} монет:\n"
            for i, line in enumerate(coin_lines, 1):
                result += f"{i}. {line}\n"
            result += f"ИТОГО В СПИСКЕ: {len(coin_lines)} монет\n"

        elif tool_name == "get_coin_stats":
            coin = tool_input["coin"].upper()
            if not coin.endswith("USDT"): coin += "USDT"
            since = tool_input.get("since")
            until = tool_input.get("until")
            source = tool_input.get("source")

            where = "WHERE coin=?"
            params = [coin]
            if since:
                where += " AND timestamp>=?"
                params.append(parse_dt(since))
            if until:
                where += " AND timestamp<=?"
                params.append(parse_dt(until, end_of_day=True))
            where, params = apply_source_filter(where, params, source)

            is_stat = source == "stat"

            c.execute(f"""SELECT COUNT(*), SUM(profit_usd),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END),
                         AVG(distance), MIN(distance), MAX(distance), AVG(buffer),
                         SUM(profit_pct)
                         FROM trades {where}""", params)
            row = c.fetchone()
            if not row or not row[0]:
                return f"Монета {coin} не найдена."

            cnt, pnl, wins, dist, dmin, dmax, buf, sum_pct = row
            wr = round((wins or 0)/cnt*100 if cnt > 0 else 0, 1)

            c.execute(f"""SELECT trader, SUM(profit_usd), COUNT(*), AVG(distance), SUM(profit_pct)
                         FROM trades {where}
                         GROUP BY trader ORDER BY SUM(profit_usd) DESC LIMIT 5""", params)
            traders = c.fetchall()

            result = f"Монета: #{coin}\n"
            if is_stat:
                result += f"Сделок: {cnt} | ROE {'+' if (sum_pct or 0)>=0 else ''}{(sum_pct or 0):.1f}% | WR: {wr}%\n"
            else:
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
            for t, p, n, d, t_sum_pct in traders:
                if is_stat:
                    result += f"{t}: ROE {'+' if (t_sum_pct or 0)>=0 else ''}{(t_sum_pct or 0):.1f}% ({n} сделок)"
                else:
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
            exchange = tool_input.get("exchange")
            source = tool_input.get("source")

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
            where_base, params = apply_source_filter(where_base, params, source)

            where_dist = where_base + " AND distance IS NOT NULL AND distance > 0"
            params_dist = list(params)
            if min_dist:
                where_dist += " AND distance>=?"
                params_dist.append(min_dist)
            if max_dist:
                where_dist += " AND distance<=?"
                params_dist.append(max_dist)

            order = "ASC" if sort_by == "loss" else "DESC"

            is_stat = source == "stat"

            c.execute(f"""SELECT coin, SUM(profit_usd), COUNT(*),
                         AVG(CASE WHEN distance>0 THEN distance END),
                         MIN(CASE WHEN distance>0 THEN distance END),
                         MAX(CASE WHEN distance>0 THEN distance END),
                         AVG(CASE WHEN buffer>0 THEN buffer END),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END),
                         SUM(profit_pct)
                         FROM trades {where_base}
                         GROUP BY coin ORDER BY SUM(profit_usd) {order} LIMIT ?""",
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

            def fmt_coin_row(coin, pnl, cnt, dist, dmin, dmax, buf, wins, sum_pct):
                sign = "🟢" if (pnl or 0) >= 0 else "🔴"
                if is_stat:
                    wr = round((wins or 0)/cnt*100 if cnt > 0 else 0, 1)
                    line = f"{sign} #{coin}: ROE {'+' if (sum_pct or 0)>=0 else ''}{(sum_pct or 0):.1f}% | WR {wr}%"
                else:
                    line = f"{sign} #{coin}: {'+' if pnl>=0 else ''}{(pnl or 0):.2f}$"
                if dist:
                    dist_data = get_smart_distance(conn, where_dist, params_dist, coin)
                    line += fmt_dist_info(dist_data)
                line += f" ({cnt} сделок)"
                return line

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

            all_dist_data = []
            for coin, pnl, cnt, dist, dmin, dmax, buf, wins, _avg_pct in rows:
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

            EXCHANGES = ["Binance", "Bybit", "OKX"]
            result += "\nДИСТАНСЫ ПО БИРЖАМ:\n"
            for coin, pnl, cnt, dist, dmin, dmax, buf, wins, _avg_pct in rows[:5]:
                result += f"#{coin}:\n"
                for exch in EXCHANGES:
                    w_exch = where_base + " AND exchange=?"
                    p_exch = list(params) + [exch]
                    dd = get_smart_distance(conn, w_exch, p_exch, coin)
                    if dd:
                        dist_str = fmt_dist_info(dd)
                        result += f"  {exch}:{dist_str}\n"

        elif tool_name == "get_all_traders":
            since = tool_input.get("since")
            until = tool_input.get("until")
            source = tool_input.get("source")

            where = "WHERE 1=1"
            params = []
            if since:
                where += " AND timestamp>=?"
                params.append(parse_dt(since))
            if until:
                where += " AND timestamp<=?"
                params.append(parse_dt(until, end_of_day=True))
            where, params = apply_source_filter(where, params, source)

            is_stat = source == "stat"

            c.execute(f"""SELECT trader, COUNT(*), SUM(profit_usd),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END),
                         SUM(profit_pct)
                         FROM trades {where}
                         GROUP BY trader ORDER BY SUM(profit_usd) DESC""", params)
            rows = c.fetchall()

            from collections import defaultdict
            grouped = defaultdict(lambda: [0, 0.0, 0, 0.0])
            ungrouped = []

            for t, cnt, pnl, wins, sum_pct in rows:
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
                    grouped[real_name][3] += (sum_pct or 0)
                else:
                    ungrouped.append((t, cnt, pnl or 0, wins or 0, sum_pct or 0))

            result = f"Трейдеры группы ({len(grouped)} человек):\n\n"
            result += "✅ С реальными именами:\n"
            sorted_grouped = sorted(grouped.items(), key=lambda x: x[1][1], reverse=True)
            for name, (cnt, pnl, wins, sum_pct) in sorted_grouped:
                wr = round(wins/cnt*100 if cnt > 0 else 0, 1)
                icon = "🟢" if pnl >= 0 else "🔴"
                if is_stat:
                    result += f"{icon} {name}: ROE {'+' if sum_pct>=0 else ''}{sum_pct:.1f}% | {cnt} сделок | WR {wr}%\n"
                else:
                    result += f"{icon} {name}: {'+' if pnl>=0 else ''}{pnl:.2f}$ | {cnt} сделок | WR {wr}%\n"

            if ungrouped:
                result += f"\n❓ Неопознанные ({len(ungrouped)}):\n"
                for t, cnt, pnl, wins, sum_pct in sorted(ungrouped, key=lambda x: x[2], reverse=True)[:10]:
                    wr = round(wins/cnt*100 if cnt > 0 else 0, 1)
                    if is_stat:
                        result += f"  {t}: ROE {'+' if sum_pct>=0 else ''}{sum_pct:.1f}% | {cnt} сделок | WR {wr}%\n"
                    else:
                        result += f"  {t}: {'+' if pnl>=0 else ''}{pnl:.2f}$ | {cnt} сделок | WR {wr}%\n"

        elif tool_name == "get_period_stats":
            since = tool_input["since"]
            until = tool_input.get("until")
            source = tool_input.get("source")

            where = "WHERE timestamp>=?"
            params = [parse_dt(since)]
            if until:
                where += " AND timestamp<=?"
                params.append(parse_dt(until, end_of_day=True))
            where, params = apply_source_filter(where, params, source)

            is_stat = source == "stat"

            c.execute(f"""SELECT COUNT(*), SUM(profit_usd),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END),
                         SUM(profit_pct)
                         FROM trades {where}""", params)
            cnt, pnl, wins, sum_pct = c.fetchone()
            wr = round((wins or 0)/(cnt or 1)*100, 1)

            c.execute(f"""SELECT trader, SUM(profit_usd), COUNT(*),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END),
                         SUM(profit_pct)
                         FROM trades {where}
                         GROUP BY trader ORDER BY SUM(profit_usd) DESC LIMIT 10""", params)
            traders = c.fetchall()

            c.execute(f"""SELECT coin, SUM(profit_usd), COUNT(*),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END),
                         SUM(profit_pct)
                         FROM trades {where}
                         GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 5""", params)
            top_coins_p = c.fetchall()

            c.execute(f"""SELECT coin, SUM(profit_usd), COUNT(*),
                         SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END),
                         SUM(profit_pct)
                         FROM trades {where}
                         GROUP BY coin ORDER BY SUM(profit_usd) ASC LIMIT 3""", params)
            worst_coins_p = c.fetchall()

            result = f"Период: {since} — {until or 'сейчас'}\n"
            if is_stat:
                result += f"Сделок: {cnt} | ROE {'+' if (sum_pct or 0)>=0 else ''}{(sum_pct or 0):.1f}% | WR: {wr}%\n\n"
            else:
                result += f"Сделок: {cnt} | PnL: {'+' if pnl>=0 else ''}{(pnl or 0):.2f}$ | WR: {wr}%\n\n"
            result += "Топ-5 монет:\n"
            for coin_r, p_r, n_r, w_r, cpct in top_coins_p:
                cwr = round((w_r or 0)/n_r*100 if n_r > 0 else 0, 1)
                if is_stat:
                    result += f"  #{coin_r}: ROE {'+' if (cpct or 0)>=0 else ''}{(cpct or 0):.1f}% ({n_r} сделок WR={cwr}%)\n"
                else:
                    result += f"  #{coin_r}: {'+' if p_r>=0 else ''}{(p_r or 0):.2f}$ ({n_r} сделок WR={cwr}%)\n"
            result += "Худшие монеты:\n"
            for coin_r, p_r, n_r, w_r, cpct in worst_coins_p:
                cwr = round((w_r or 0)/n_r*100 if n_r > 0 else 0, 1)
                if is_stat:
                    result += f"  #{coin_r}: ROE {'+' if (cpct or 0)>=0 else ''}{(cpct or 0):.1f}% ({n_r} сделок WR={cwr}%)\n"
                else:
                    result += f"  #{coin_r}: {'+' if p_r>=0 else ''}{(p_r or 0):.2f}$ ({n_r} сделок WR={cwr}%)\n"
            result += "\nТоп трейдеры:\n"
            for t, p, n, w, tpct in traders:
                twr = round((w or 0)/n*100 if n > 0 else 0, 1)
                if is_stat:
                    result += f"  {t}: ROE {'+' if (tpct or 0)>=0 else ''}{(tpct or 0):.1f}% ({n} сделок WR={twr}%)\n"
                else:
                    result += f"  {t}: {'+' if p>=0 else ''}{(p or 0):.2f}$ ({n} сделок WR={twr}%)\n"

        elif tool_name == "get_delta_analysis":
            group_by = tool_input.get("group_by", "delta")
            coin = tool_input.get("coin")
            distance = tool_input.get("distance")
            min_dist = tool_input.get("min_distance")
            max_dist = tool_input.get("max_distance")
            delta_range = tool_input.get("delta_range")
            side = tool_input.get("side")
            exchange = tool_input.get("exchange")
            since = tool_input.get("since")
            until = tool_input.get("until")
            limit = tool_input.get("limit", 15)
            min_trades = tool_input.get("min_trades", 10)
            source = tool_input.get("source")

            where = "WHERE delta_min IS NOT NULL"
            params = []
            if coin:
                coin = coin.upper()
                if not coin.endswith("USDT"): coin += "USDT"
                where += " AND coin=?"
                params.append(coin)
            if distance:
                where += " AND distance=?"
                params.append(distance)
            if min_dist:
                where += " AND distance>=?"
                params.append(min_dist)
            if max_dist:
                where += " AND distance<=?"
                params.append(max_dist)
            if delta_range:
                dr_parts = delta_range.split("-")
                if len(dr_parts) == 2:
                    where += " AND CAST(delta_min AS INTEGER)=? AND CAST(delta_max AS INTEGER)=?"
                    params.extend([int(dr_parts[0]), int(dr_parts[1])])
            if side:
                where += " AND side=?"
                params.append(side.upper())
            if exchange:
                where += " AND exchange=?"
                params.append(exchange)
            if since:
                where += " AND timestamp>=?"
                params.append(parse_dt(since))
            if until:
                where += " AND timestamp<=?"
                params.append(parse_dt(until, end_of_day=True))
            where, params = apply_source_filter(where, params, source)

            is_stat = source == "stat"

            filters = []
            if coin: filters.append(f"#{coin}")
            if distance: filters.append(f"dist={distance}")
            elif min_dist or max_dist:
                if min_dist and max_dist: filters.append(f"dist {min_dist}-{max_dist}")
                elif min_dist: filters.append(f"dist≥{min_dist}")
                else: filters.append(f"dist≤{max_dist}")
            if delta_range: filters.append(f"δ{delta_range}%")
            if side: filters.append(side.upper())
            if exchange: filters.append(exchange)
            filter_str = f" | {', '.join(filters)}" if filters else ""

            def fmt_val(pnl, pct):
                if is_stat:
                    return f"ROE {'+' if (pct or 0)>=0 else ''}{(pct or 0):.1f}%"
                return f"{'+' if (pnl or 0)>=0 else ''}{(pnl or 0):.2f}$"

            def fmt_wr(wins, cnt):
                return f"{round((wins or 0)/cnt*100 if cnt > 0 else 0, 1)}%"

            if group_by == "delta":
                c.execute(f"""SELECT
                    CAST(delta_min AS INTEGER) || '-' || CAST(delta_max AS INTEGER),
                    COUNT(*), SUM(profit_usd), SUM(profit_pct),
                    SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END), AVG(distance)
                    FROM trades {where}
                    GROUP BY CAST(delta_min AS INTEGER), CAST(delta_max AS INTEGER)
                    HAVING COUNT(*) >= ?
                    ORDER BY SUM(profit_pct) DESC""", params + [min_trades])
                rows = c.fetchall()
                if not rows:
                    return "Нет данных по дельтам."
                result = f"ДЕЛЬТЫ{filter_str}\n\n"
                for dr, cnt, pnl, pct, wins, avg_d in rows:
                    icon = "🟢" if (pct or 0) >= 0 else "🔴"
                    result += f"{icon} δ{dr}%: {fmt_val(pnl, pct)} | WR {fmt_wr(wins, cnt)} | dist avg {avg_d:.2f} | {cnt} сделок\n"
                best = rows[0]
                result += f"\n🏆 Лучшая: δ{best[0]}% ({fmt_val(best[2], best[3])}, WR {fmt_wr(best[4], best[1])}, {best[1]} сделок)\n"

            elif group_by == "coin":
                c.execute(f"""SELECT coin,
                    CAST(delta_min AS INTEGER) || '-' || CAST(delta_max AS INTEGER) as dr,
                    COUNT(*), SUM(profit_pct),
                    SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END), AVG(distance)
                    FROM trades {where}
                    GROUP BY coin, CAST(delta_min AS INTEGER), CAST(delta_max AS INTEGER)
                    HAVING COUNT(*) >= ?
                    ORDER BY SUM(profit_pct) DESC LIMIT ?""", params + [min_trades, limit * 5])
                all_rows = c.fetchall()
                if not all_rows:
                    return "Нет данных."
                # Для каждой монеты берём лучшую дельту
                seen_coins = {}
                for coin_r, dr, cnt, pct, wins, avg_d in all_rows:
                    if coin_r not in seen_coins:
                        seen_coins[coin_r] = (dr, cnt, pct, wins, avg_d)
                sorted_coins = sorted(seen_coins.items(), key=lambda x: x[1][2] or 0, reverse=True)[:limit]
                result = f"МОНЕТЫ + ЛУЧШАЯ ДЕЛЬТА{filter_str}\n\n"
                for i, (coin_r, (dr, cnt, pct, wins, avg_d)) in enumerate(sorted_coins, 1):
                    icon = "🟢" if (pct or 0) >= 0 else "🔴"
                    result += f"{i}. {icon} #{coin_r}: ROE {'+' if (pct or 0)>=0 else ''}{(pct or 0):.1f}% | δ{dr}% | WR {fmt_wr(wins, cnt)} | dist {avg_d:.2f} | {cnt} сделок\n"
                # Для каждой монеты из топа — все дельты
                result += f"\nДЕТАЛИ ПО ДЕЛЬТАМ:\n"
                for coin_r, _ in sorted_coins[:5]:
                    result += f"\n#{coin_r}:\n"
                    coin_deltas = [(dr, cnt, pct, wins, avg_d) for c, dr, cnt, pct, wins, avg_d in all_rows if c == coin_r]
                    coin_deltas.sort(key=lambda x: x[2] or 0, reverse=True)
                    for dr, cnt, pct, wins, avg_d in coin_deltas:
                        icon = "🟢" if (pct or 0) >= 0 else "🔴"
                        result += f"  {icon} δ{dr}%: ROE {'+' if (pct or 0)>=0 else ''}{(pct or 0):.1f}% | WR {fmt_wr(wins, cnt)} | {cnt} сделок\n"

            elif group_by == "distance":
                c.execute(f"""SELECT distance,
                    CAST(delta_min AS INTEGER) || '-' || CAST(delta_max AS INTEGER) as dr,
                    COUNT(*), SUM(profit_pct),
                    SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
                    FROM trades {where}
                    GROUP BY distance, CAST(delta_min AS INTEGER), CAST(delta_max AS INTEGER)
                    HAVING COUNT(*) >= ?
                    ORDER BY distance, SUM(profit_pct) DESC""", params + [min_trades])
                rows = c.fetchall()
                if not rows:
                    return "Нет данных."
                from itertools import groupby as igroupby
                result = f"ДИСТАНСЫ + ДЕЛЬТЫ{filter_str}\n\n"
                rows_sorted = sorted(rows, key=lambda x: x[0] or 0)
                for dist_val, group in igroupby(rows_sorted, key=lambda x: x[0]):
                    group_list = sorted(list(group), key=lambda x: x[3] or 0, reverse=True)
                    total_pct = sum(r[3] or 0 for r in group_list)
                    total_cnt = sum(r[2] for r in group_list)
                    icon = "🟢" if total_pct >= 0 else "🔴"
                    result += f"{icon} dist={fmt_dist(dist_val)}: ROE {'+' if total_pct>=0 else ''}{total_pct:.1f}% | {total_cnt} сделок\n"
                    for dist_r, dr, cnt, pct, wins in group_list[:4]:
                        sub_icon = "🟢" if (pct or 0) >= 0 else "🔴"
                        result += f"  {sub_icon} δ{dr}%: ROE {'+' if (pct or 0)>=0 else ''}{(pct or 0):.1f}% | WR {fmt_wr(wins, cnt)} | {cnt} сделок\n"

            elif group_by == "combo":
                c.execute(f"""SELECT coin, distance,
                    CAST(delta_min AS INTEGER) || '-' || CAST(delta_max AS INTEGER) as dr,
                    COUNT(*), SUM(profit_usd), SUM(profit_pct),
                    SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END)
                    FROM trades {where}
                    GROUP BY coin, distance, CAST(delta_min AS INTEGER), CAST(delta_max AS INTEGER)
                    HAVING COUNT(*) >= ?
                    ORDER BY SUM(profit_pct) DESC LIMIT ?""", params + [min_trades, limit])
                rows = c.fetchall()
                if not rows:
                    return "Нет данных для комбо-анализа."
                result = f"ТОП КОМБИНАЦИЙ монета+дистанс+дельта{filter_str}\n\n"
                for i, (coin_r, dist_r, dr, cnt, pnl, pct, wins) in enumerate(rows, 1):
                    icon = "🟢" if (pct or 0) >= 0 else "🔴"
                    result += f"{i}. {icon} #{coin_r} dist={fmt_dist(dist_r)} δ{dr}%: {fmt_val(pnl, pct)} | WR {fmt_wr(wins, cnt)} | {cnt} сделок\n"
                result += f"\n🏆 Лучшая: #{rows[0][0]} dist={fmt_dist(rows[0][1])} δ{rows[0][2]}%\n"

            else:
                result = f"Неизвестный group_by: {group_by}. Используй: delta, coin, distance, combo"

        elif tool_name == "manage_knowledge":
            action = tool_input.get("action", "list")
            category = tool_input.get("category", "")
            content = tool_input.get("content", "")
            query = tool_input.get("query", "")
            kb_id = tool_input.get("id")

            if action == "add" and content:
                c.execute("INSERT INTO knowledge(category, content) VALUES(?,?)",
                          (category or "заметка", content))
                conn.commit()
                result = f"Записано в категорию '{category or 'заметка'}': {content[:100]}"

            elif action == "search" and query:
                q_like = f"%{query}%"
                c.execute("SELECT id, category, content, created FROM knowledge WHERE content LIKE ? OR category LIKE ? ORDER BY id DESC LIMIT 10",
                          (q_like, q_like))
                rows = c.fetchall()
                if rows:
                    result = f"Найдено {len(rows)} записей:\n"
                    for kid, cat, txt, dt in rows:
                        result += f"  [{kid}] ({cat}) {txt[:100]} | {dt}\n"
                else:
                    result = f"Ничего не найдено по '{query}'."

            elif action == "list":
                if category:
                    c.execute("SELECT id, content, created FROM knowledge WHERE category=? ORDER BY id DESC LIMIT 20", (category,))
                else:
                    c.execute("SELECT id, category, content, created FROM knowledge ORDER BY id DESC LIMIT 20")
                rows = c.fetchall()
                if rows:
                    result = f"База знаний ({len(rows)} записей):\n"
                    for row in rows:
                        if category:
                            kid, txt, dt = row
                            result += f"  [{kid}] {txt[:100]} | {dt}\n"
                        else:
                            kid, cat, txt, dt = row
                            result += f"  [{kid}] ({cat}) {txt[:80]} | {dt}\n"
                else:
                    result = "База знаний пуста."

            elif action == "delete" and kb_id:
                c.execute("DELETE FROM knowledge WHERE id=?", (kb_id,))
                conn.commit()
                result = f"Запись #{kb_id} удалена." if c.rowcount > 0 else f"Запись #{kb_id} не найдена."

            else:
                result = "Неизвестное действие. Используй: add, search, list, delete"

    except Exception as e:
        result = f"Ошибка запроса: {e}"
    finally:
        conn.close()

    return result


def get_period_context(user_text: str) -> str:
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
        month_candidates = [k for k, v in MONTH_NAMES.items() if v == month_num and len(k) > 3]
        month_name = month_candidates[0].capitalize() if month_candidates else f"Месяц {month_num}"
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


def get_knowledge_context(user_text: str) -> str:
    """Подтягивает релевантные знания из knowledge base."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        # Берём последние 20 записей (лёгкий — без full-text search)
        c.execute("SELECT category, content FROM knowledge ORDER BY id DESC LIMIT 20")
        rows = c.fetchall()
        conn.close()
        if not rows:
            return ""
        text_lower = user_text.lower()
        relevant = []
        for cat, content in rows:
            # Простой keyword matching
            words = (cat or "").lower().split() + content.lower().split()[:10]
            if any(w in text_lower for w in words if len(w) > 3):
                relevant.append(f"[{cat}] {content}")
        if not relevant:
            # Всегда добавляем записи с категорией "правило" или "важно"
            for cat, content in rows:
                if cat and cat.lower() in ("правило", "важно", "rule", "важное"):
                    relevant.append(f"[{cat}] {content}")
        if not relevant:
            return ""
        return "\n\n=== БАЗА ЗНАНИЙ ===\n" + "\n".join(relevant[:5])
    except Exception:
        return ""


def save_query_log(user_id: int, question: str, tools_used: list, tokens_in: int, tokens_out: int, cost: float):
    """Сохраняет лог запроса для аналитики."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "INSERT INTO query_log(user_id, question, tools_used, tokens_in, tokens_out, cost) VALUES(?,?,?,?,?,?)",
            (user_id, question[:500], ",".join(tools_used), tokens_in, tokens_out, cost)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Query log error: {e}")


def save_feedback(user_id: int, question: str, answer: str, rating: int, comment: str = ""):
    """Сохраняет фидбек на ответ бота."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "INSERT INTO feedback(user_id, question, answer, rating, comment) VALUES(?,?,?,?,?)",
            (user_id, question[:500], answer[:1000], rating, comment[:500])
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Feedback save error: {e}")


def get_feedback_context() -> str:
    """Подтягивает паттерны из фидбека — что нравится, что нет."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM feedback WHERE rating <= 0")
        bad = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM feedback WHERE rating > 0")
        good = c.fetchone()[0]
        tips = []
        if bad > 0:
            c.execute("SELECT question, comment FROM feedback WHERE rating <= 0 ORDER BY id DESC LIMIT 3")
            for q, cm in c.fetchall():
                if cm:
                    tips.append(f"Плохой ответ на '{q[:50]}': {cm}")
        conn.close()
        if not tips:
            return ""
        return "\n\n=== ФИДБЕК (учитывай) ===\n" + "\n".join(tips)
    except Exception:
        return ""


async def claude_reply(user_id: int, user_text: str) -> str:
    global total_api_cost, total_api_calls

    # Обработка фидбека: 👍/👎/+/-
    text_stripped = user_text.strip()
    if text_stripped in ("👍", "+", "хорошо", "верно", "правильно", "отлично"):
        last_msgs = conversation_history.get(user_id, [])
        q = next((m["content"] for m in reversed(last_msgs) if m["role"] == "user" and m["content"] != text_stripped), "")
        a = next((m["content"] for m in reversed(last_msgs) if m["role"] == "assistant"), "")
        save_feedback(user_id, q, a, 1)
        return "Спасибо, запомнил! 🦇"
    if text_stripped in ("👎", "-", "плохо", "неправильно", "неверно"):
        last_msgs = conversation_history.get(user_id, [])
        q = next((m["content"] for m in reversed(last_msgs) if m["role"] == "user" and m["content"] != text_stripped), "")
        a = next((m["content"] for m in reversed(last_msgs) if m["role"] == "assistant"), "")
        save_feedback(user_id, q, a, -1)
        return "Понял, учту в будущем. Напиши что было не так — запомню. 🦇"
    # Фидбек с комментарием: "плохо: показал доллары вместо процентов"
    if text_stripped.startswith(("плохо:", "неправильно:", "не так:")):
        comment = text_stripped.split(":", 1)[1].strip()
        last_msgs = conversation_history.get(user_id, [])
        q = next((m["content"] for m in reversed(last_msgs) if m["role"] == "user"), "")
        a = next((m["content"] for m in reversed(last_msgs) if m["role"] == "assistant"), "")
        save_feedback(user_id, q, a, -1, comment)
        return f"Записал: {comment}. Буду учитывать! 🦇"

    history = conversation_history.setdefault(user_id, [])
    if len(history) >= MAX_HISTORY:
        history[:] = history[-(MAX_HISTORY - 2):]
    period_ctx = get_period_context(user_text)
    knowledge_ctx = get_knowledge_context(user_text)
    feedback_ctx = get_feedback_context()
    enriched_text = user_text + period_ctx if period_ctx else user_text
    history.append({"role": "user", "content": enriched_text})

    db_ctx = get_db_context()
    system = f"{SYSTEM_PROMPT}\n\n{db_ctx}{knowledge_ctx}{feedback_ctx}"

    system_chars = len(system)
    history_chars = sum(len(str(m.get("content", ""))) for m in history)
    logger.info(f"💬 Request | user={user_id} | system={system_chars}c | history={history_chars}c | msgs={len(history)}")

    request_cost = 0.0
    request_input = 0
    request_output = 0
    tool_calls_count = 0
    tools_used = []

    # Ключевые слова которые указывают что нужен поиск в интернете
    WEB_SEARCH_KEYWORDS = [
        'найди', 'поищи', 'погугли', 'найти', 'поиск',
        'новости', 'новость', 'что случилось', 'что произошло',
        'эксплойт', 'exploit', 'hack', 'хак', 'взлом',
        'internet', 'интернет', 'в сети', 'онлайн',
        'search', 'find', 'look up', 'google',
        'залистили', 'залистил', 'листинг', 'listing', 'делистинг',
        'что за монет', 'что это за монет', 'что за токен', 'что за проект',
        'когда добавили', 'новая монета', 'новый токен',
    ]
    needs_web_search = any(kw in user_text.lower() for kw in WEB_SEARCH_KEYWORDS)

    if needs_web_search:
        # Sonnet поддерживает web search через betas
        model_name = "claude-sonnet-4-6"
        extra_kwargs = {"extra_headers": {"anthropic-beta": "web-search-2025-03-05"}}
        tools_to_use = TOOLS  # включает web_search
        logger.info("🌐 Web search mode → using Sonnet")
    else:
        # Haiku быстрее и дешевле для обычных запросов
        model_name = "claude-haiku-4-5-20251001"
        extra_kwargs = {}
        # Убираем web_search tool из списка чтобы не было конфликта
        tools_to_use = [t for t in TOOLS if not (isinstance(t, dict) and t.get("type") == "web_search_20250305")]

    try:
        # ── Первый запрос ──
        response = anthropic_client.messages.create(
            model=model_name,
            max_tokens=2048,
            system=system,
            tools=tools_to_use,
            messages=history,
            **extra_kwargs
        )

        if hasattr(response, "usage"):
            request_input  += response.usage.input_tokens
            request_output += response.usage.output_tokens

        max_tool_calls = 5
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
                    tools_used.append(block.name)
                    logger.info(f"🔧 Tool: {block.name} → {len(tool_result)} chars")

            messages_with_tools = history + [
                {"role": "assistant", "content": assistant_content},
                {"role": "user", "content": tool_results}
            ]

            # ── Повторный запрос с той же моделью ──
            response = anthropic_client.messages.create(
                model=model_name,
                max_tokens=2048,
                system=system,
                tools=tools_to_use,
                messages=messages_with_tools,
                **extra_kwargs
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

        NO_DATA_MARKERS = [
            'не найден', 'нет данных', 'нет в базе', 'отсутствует в базе',
            'не найдена', 'не найдено', 'нет сделок', 'не торговалась',
            'no data', 'not found',
        ]
        if (not needs_web_search
                and any(m in reply.lower() for m in NO_DATA_MARKERS)
                and re.search(r'#?[A-Z]{2,}(?:USDT)?', user_text.upper())):
            logger.info("🔄 Fallback → web search (no data in DB, coin mentioned)")
            fallback_prompt = (
                f"В базе данных нет информации по этому запросу. "
                f"Используй web_search чтобы найти актуальную информацию в интернете и ответь пользователю.\n\n"
                f"Вопрос пользователя: {user_text}"
            )
            fallback_history = history[:-1] + [{"role": "user", "content": fallback_prompt}]
            try:
                fb_response = anthropic_client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=2048,
                    system=system,
                    tools=TOOLS,
                    messages=fallback_history,
                    extra_headers={"anthropic-beta": "web-search-2025-03-05"}
                )
                if hasattr(fb_response, "usage"):
                    request_input += fb_response.usage.input_tokens
                    request_output += fb_response.usage.output_tokens

                fb_tool_calls = 0
                while fb_response.stop_reason == "tool_use" and fb_tool_calls < 3:
                    fb_tool_results = []
                    fb_assistant_content = fb_response.content
                    fb_tool_calls += 1
                    for block in fb_response.content:
                        if block.type == "tool_use":
                            fb_tool_result = execute_tool(block.name, block.input)
                            fb_tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": fb_tool_result
                            })
                            logger.info(f"🔧 Fallback tool: {block.name} → {len(fb_tool_result)} chars")
                    fb_messages = fallback_history + [
                        {"role": "assistant", "content": fb_assistant_content},
                        {"role": "user", "content": fb_tool_results}
                    ]
                    fb_response = anthropic_client.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=2048,
                        system=system,
                        tools=TOOLS,
                        messages=fb_messages,
                        extra_headers={"anthropic-beta": "web-search-2025-03-05"}
                    )
                    if hasattr(fb_response, "usage"):
                        request_input += fb_response.usage.input_tokens
                        request_output += fb_response.usage.output_tokens

                fb_reply = ""
                for block in fb_response.content:
                    if hasattr(block, "text"):
                        fb_reply += block.text
                if fb_reply:
                    reply = fb_reply
                    logger.info("✅ Fallback web search succeeded")
            except Exception as fb_e:
                logger.error(f"Fallback web search error: {fb_e}")

    except Exception as e:
        logger.error(f"Claude API error: {e}")
        reply = "⚠️ Ошибка при обращении к Claude. Попробуй позже."
        history.append({"role": "assistant", "content": reply})
        return reply

    request_cost = (request_input * COST_INPUT_PER_TOKEN) + (request_output * COST_OUTPUT_PER_TOKEN)
    total_api_cost += request_cost
    total_api_calls += 1

    logger.info(
        f"💰 Cost: ${request_cost:.4f} | "
        f"in={request_input} out={request_output} tokens | "
        f"tools={tool_calls_count} | "
        f"total=${total_api_cost:.4f} ({total_api_calls} calls)"
    )

    if request_cost > 0.50:
        logger.warning(f"⚠️ EXPENSIVE REQUEST: ${request_cost:.4f} | user={user_id}")

    save_query_log(user_id, user_text, tools_used, request_input, request_output, request_cost)

    history.append({"role": "assistant", "content": reply})
    return reply

async def handle_trade_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.message
    if not msg:
        return
    chat_id = msg.chat.id
    text = msg.text or ""

    if chat_id == SOURCE_CHAT_ID:
        trade = parse_trade(text)
        if trade:
            trade["source"] = "live"
            save_trade(trade)
            logger.info(f"Saved [live]: {trade['trader']} | {trade['coin']} | {trade['profit_usd']}$")
    elif chat_id in STAT_CHAT_IDS:
        trade = parse_stat_trade(text)
        if trade:
            save_trade(trade)
            logger.info(f"Saved [stat]: {trade['trader']} | {trade['coin']} | {trade['exchange']} | {trade['profit_usd']}$")
    else:
        if 'stat' in text.lower() or 'STATS' in text or 'Profit' in text or 'Loss' in text:
            logger.info(f"⚠️ Unknown chat_id={chat_id} | {text[:80]}")


async def transcribe_voice(file_path: str) -> str | None:
    if not openai_client:
        return None
    try:
        with open(file_path, "rb") as audio_file:
            transcript = await openai_client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                language="ru"
            )
        logger.info(f"🎤 Whisper: {len(transcript.text)} chars transcribed")
        return transcript.text
    except Exception as e:
        logger.error(f"Whisper error: {e}")
        return None


async def handle_dialog_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if msg:
        logger.info(f"📨 Message from chat_id={msg.chat.id} type={msg.chat.type} thread={msg.message_thread_id} | DIALOG={DIALOG_CHAT_ID} | text={repr((msg.text or '')[:50])}")
    if not msg or msg.chat.id != DIALOG_CHAT_ID:
        return

    bot_id = context.bot.id
    is_reply_to_bot = (msg.reply_to_message is not None and
                       msg.reply_to_message.from_user is not None and
                       msg.reply_to_message.from_user.id == bot_id)

    is_voice = msg.voice is not None or msg.audio is not None
    text = msg.text or msg.caption or ""

    is_mention = False
    if BOT_USERNAME and f"@{BOT_USERNAME}" in text:
        is_mention = True
    if msg.entities:
        for ent in msg.entities:
            if ent.type == "mention":
                mention_text = text[ent.offset: ent.offset + ent.length]
                if mention_text.lstrip("@").lower() == BOT_USERNAME.lower():
                    is_mention = True

    if is_voice and not (is_reply_to_bot or is_mention):
        return
    if not is_voice and not is_mention and not is_reply_to_bot:
        return

    user_id = msg.from_user.id

    if is_voice and openai_client:
        thread_id = msg.message_thread_id if msg.is_topic_message else None
        await msg.chat.send_action("typing", message_thread_id=thread_id)
        voice = msg.voice or msg.audio
        voice_file = await context.bot.get_file(voice.file_id)
        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            await voice_file.download_to_drive(tmp_path)
            transcribed = await transcribe_voice(tmp_path)
            if not transcribed:
                await msg.reply_text("⚠️ Не удалось распознать голосовое сообщение.")
                return
            await msg.reply_text(f"🎤 *Распознано:* _{transcribed}_", parse_mode="Markdown", message_thread_id=thread_id)
            clean_text = transcribed
        finally:
            try:
                _os.unlink(tmp_path)
            except Exception:
                pass
    else:
        clean_text = text.replace(f"@{BOT_USERNAME}", "").strip()
        if not clean_text:
            clean_text = "Привет!"

    thread_id = msg.message_thread_id if msg.is_topic_message else None

    await msg.chat.send_action("typing", message_thread_id=thread_id)
    reply = await claude_reply(user_id, clean_text)

    max_len = 4000
    if len(reply) <= max_len:
        parts = [reply]
    else:
        parts = []
        remaining = reply
        while remaining:
            if len(remaining) <= max_len:
                parts.append(remaining)
                break
            split_at = remaining.rfind("\n", 0, max_len)
            if split_at == -1:
                split_at = max_len
            parts.append(remaining[:split_at])
            remaining = remaining[split_at:].lstrip("\n")
    for part in parts:
        try:
            await msg.reply_text(part, parse_mode="Markdown", message_thread_id=thread_id)
        except Exception:
            await msg.reply_text(part, message_thread_id=thread_id)

async def cmd_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Использование:\n/period январь 2026\n/period 2026-01\n/period 7")
        return

    now = datetime.now()
    since = None
    until = None
    label = ""

    if len(args) == 1 and args[0].isdigit():
        days  = int(args[0])
        since = (now - timedelta(days=days)).isoformat()
        label = f"последние {days} дней"
    elif len(args) == 1 and re.match(r'\d{4}-\d{2}', args[0]):
        year, month = map(int, args[0].split('-'))
        since = f"{year}-{month:02d}-01T00:00:00"
        if month == 12:
            until = f"{year+1}-01-01T00:00:00"
        else:
            until = f"{year}-{month+1:02d}-01T00:00:00"
        label = f"{args[0]}"
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
    args = context.args
    if not args:
        await update.message.reply_text("Использование: /trader Денис\n/trader Денис январь 2026")
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

    c.execute(f"SELECT COUNT(*), SUM(profit_usd), SUM(CASE WHEN is_profit=1 THEN 1 ELSE 0 END) FROM trades {where}", params)
    total, pnl, wins = c.fetchone()
    if not total:
        await update.message.reply_text(f"❌ Трейдер *{trader}* не найден.", parse_mode="Markdown")
        conn.close()
        return

    wr = round((wins or 0) / total * 100 if total > 0 else 0, 1)
    pnl = pnl or 0

    c.execute(f"""SELECT coin, SUM(profit_usd), COUNT(*),
                 AVG(distance), AVG(buffer), AVG(take_profit),
                 MIN(distance), MAX(distance)
                 FROM trades {where}
                 GROUP BY coin ORDER BY SUM(profit_usd) DESC LIMIT 10""", params)
    top_coins = c.fetchall()

    c.execute(f"""SELECT coin, SUM(profit_usd), COUNT(*),
                 AVG(distance), AVG(buffer), AVG(take_profit),
                 MIN(distance), MAX(distance)
                 FROM trades {where}
                 GROUP BY coin ORDER BY SUM(profit_usd) ASC LIMIT 5""", params)
    worst_coins = c.fetchall()

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
    for coin, p, cnt, dist, buf, tp, min_d, max_d in worst_coins:
        d = f" dist={fmt_dist(dist)}" if dist else ""
        r += f"  #{coin}: {'+' if p>=0 else ''}{p:.2f}$ ({cnt} сделок){d}\n"

    await update.message.reply_text(r, parse_mode="Markdown")


async def cmd_coins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    since, until, label = None, None, "всё время"
    exchange = None

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

    conn2 = sqlite3.connect(DB_PATH)
    exch_label = f" 🏦{exchange}" if exchange else ""
    r  = f"📊 *Монеты — {label}*{exch_label}\n\n"

    r += f"🏆 *Топ-{len(top)}:*\n"
    for i, (coin, p, cnt, w) in enumerate(top, 1):
        wr = round(w/cnt*100 if cnt > 0 else 0, 1)
        dist_data = get_smart_distance(conn2, where, params, coin)
        dist_str = fmt_dist_info(dist_data) if dist_data else ""
        r += f"{i}. *#{coin}*: {'+' if p>=0 else ''}{p:.2f}$ | WR {wr}%{dist_str} | {cnt} сделок\n"

    r += "\n💀 *Худшие 5:*\n"
    for coin, p, cnt in worst:
        r += f"  #{coin}: {'+' if p>=0 else ''}{p:.2f}$ | {cnt} сделок\n"

    conn.close()
    conn2.close()
    await update.message.reply_text(r, parse_mode="Markdown")


async def cmd_coin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Использование: /coin NOMUSDT или /coin NOMUSDT март 2026 [binance|bybit|okx]")
        return

    coin = args[0].upper()
    if not coin.endswith('USDT'):
        coin += 'USDT'

    since, until, label = None, None, "всё время"
    exchange = None

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

    if avg_d and not exchange:
        conn3 = sqlite3.connect(DB_PATH)
        dist_data = get_smart_distance(conn3, where, params, coin)
        conn3.close()
        if dist_data and dist_data.get("smart"):
            r += f"📐 Дистанс: ⚡️{fmt_dist(dist_data['working'])} 🛡{fmt_dist(dist_data['insurance'])} ({fmt_dist(min_d)}–{fmt_dist(max_d)})\n"
        else:
            r += f"📐 Дистанс: avg={fmt_dist(avg_d)} ({fmt_dist(min_d)}–{fmt_dist(max_d)})\n"
    if avg_b and not exchange:
        r += f"📏 Буфер: avg={fmt_dist(avg_b)}\n"
    if avg_tp:
        r += f"🎯 Тейк: avg={avg_tp:.1f}%\n"

    if exchanges and not exchange:
        r += "\n🏦 *По биржам:*\n"
        conn3 = sqlite3.connect(DB_PATH)
        for exch, ecnt, epnl, ewins, edist, edmin, edmax, ebuf in exchanges:
            if not exch:
                continue
            ewr = round((ewins or 0)/ecnt*100 if ecnt > 0 else 0, 1)
            r += f"  *{exch}*: {'+' if epnl>=0 else ''}{(epnl or 0):.2f}$ | {ecnt} сделок | WR {ewr}%\n"
            if edist:
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

    r += "\n👤 *Трейдеры:*\n"
    for trader, tcnt, tpnl, twins, tdist, texch in traders:
        twr = round((twins or 0)/tcnt*100 if tcnt > 0 else 0, 1)
        d = f" dist={fmt_dist(tdist)}" if tdist else ""
        exch_tag = f" [{texch}]" if texch and not exchange else ""
        r += f"  *{trader}*{exch_tag}: {'+' if tpnl>=0 else ''}{(tpnl or 0):.2f}$ | {tcnt} сделок | WR {twr}%{d}\n"

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
    r = (
        f"💰 *Расходы на Claude API*\n\n"
        f"Запросов: *{total_api_calls}*\n"
        f"Суммарно: *${total_api_cost:.4f}*\n"
        f"Средний запрос: *${(total_api_cost/total_api_calls):.4f}*\n\n"
        f"_(с момента последнего запуска бота)_"
    ) if total_api_calls > 0 else "💰 Запросов ещё не было."
    await update.message.reply_text(r, parse_mode="Markdown")


async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет БД файлом в Telegram."""
    if not os.path.exists(DB_PATH):
        await update.message.reply_text("БД не найдена.")
        return
    size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    await update.message.reply_text(f"Отправляю БД ({size_mb:.1f} MB)...")
    try:
        await update.message.reply_document(
            document=open(DB_PATH, "rb"),
            filename=f"trades_{datetime.now().strftime('%Y%m%d_%H%M')}.db",
            caption=f"trades.db | {size_mb:.1f} MB | {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


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
        "/export — скачать БД (SQLite файл)\n"
        "/clear — очистить историю AI-диалога\n\n"
        f"💬 Упомяни @{BOT_USERNAME} или ответь на моё сообщение для AI-диалога.\n"
        f"🌐 Поиск в интернете: просто попроси 'найди новости по BTC' или 'поищи что случилось с AAVE'"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

def build_daily_report() -> str:
    now = datetime.now()
    today = now.replace(hour=0, minute=0, second=0).isoformat()
    week_ago = (now - timedelta(days=7)).isoformat()

    r = f"📊 *ДНЕВНОЙ ОТЧЁТ* | {now.strftime('%d.%m.%Y')}\n{'─'*25}\n\n"
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
                    await app.bot.send_message(chat_id=REPORT_CHAT_ID, text=build_daily_report(), parse_mode="Markdown", message_thread_id=REPORT_THREAD_ID)
                    sent_daily = day_key
                except Exception as e:
                    logger.error(f"Daily report error: {e}")
            if now.weekday() == 6 and sent_weekly != week_key:
                try:
                    since = (now - timedelta(days=7)).isoformat()
                    await app.bot.send_message(chat_id=REPORT_CHAT_ID, text="🗓 *Недельный отчёт*\n\n" + get_stats_for_period(since), parse_mode="Markdown", message_thread_id=REPORT_THREAD_ID)
                    sent_weekly = week_key
                except Exception as e:
                    logger.error(f"Weekly report error: {e}")
        await asyncio.sleep(60)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception while handling update:", exc_info=context.error)
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
    app.add_handler(CommandHandler("export",       cmd_export))
    app.add_handler(CommandHandler("clear",        cmd_clear_history))
    app.add_handler(CommandHandler("help",         cmd_help))
    app.add_handler(CommandHandler("start",        cmd_help))
    trade_chat_ids = [SOURCE_CHAT_ID] + list(STAT_CHAT_IDS)
    app.add_handler(MessageHandler(filters.Chat(trade_chat_ids) & filters.ALL, handle_trade_message))
    # Catch-all для обнаружения новых чатов (если STAT_CHAT_IDS ещё не настроен)
    if not STAT_CHAT_IDS:
        async def log_unknown(update, context):
            msg = update.channel_post or update.message
            if msg and msg.chat.id != SOURCE_CHAT_ID and msg.chat.id != DIALOG_CHAT_ID:
                text = msg.text or ""
                logger.info(f"⚠️ Unknown chat_id={msg.chat.id} type={msg.chat.type} | {text[:100]}")
        app.add_handler(MessageHandler(filters.ALL, log_unknown))
    app.add_handler(MessageHandler(filters.Chat(DIALOG_CHAT_ID) & filters.TEXT & ~filters.COMMAND, handle_dialog_message))
    app.add_handler(MessageHandler(filters.Chat(DIALOG_CHAT_ID) & (filters.VOICE | filters.AUDIO), handle_dialog_message))
    app.add_error_handler(error_handler)
    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        logger.info("✅ Bot started!")
        await scheduled_reports_loop(app)

if __name__ == "__main__":
    asyncio.run(main())
