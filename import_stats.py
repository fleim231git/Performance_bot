import ijson
import re
import sqlite3
import sys
import os

DB_PATH = os.environ.get("DB_PATH", "/data/trades.db")


def flatten_text(text_field) -> str:
    if isinstance(text_field, str):
        return text_field
    if isinstance(text_field, list):
        return ''.join(p if isinstance(p, str) else p.get('text', '') for p in text_field)
    return ''


def parse_stat_message(text: str, date: str, channel_exchange: str) -> dict | None:
    if ('Profit' not in text and 'Loss' not in text) or '$' not in text or '#' not in text:
        return None

    try:
        is_profit = 1 if 'Profit' in text else 0

        trader_match = re.match(r'(STATS-[a-zA-Z]+-\d+|stat\d+)', text)
        trader = trader_match.group(1) if trader_match else "Unknown"

        side_match = re.search(r'\b(BUY|SELL)\b', text, re.IGNORECASE)
        side = side_match.group(1).upper() if side_match else "Unknown"

        dist_match = re.search(r'\b(?:BUY|SELL)\s+([\d.]+)', text, re.IGNORECASE)
        distance = float(dist_match.group(1)) if dist_match else None

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

        return {
            "timestamp": date,
            "trader": trader,
            "exchange": channel_exchange,
            "side": side,
            "coin": coin,
            "distance": distance,
            "buffer": None,
            "take_profit": take_profit,
            "profit_usd": profit_usd,
            "profit_pct": profit_pct,
            "is_profit": is_profit,
            "raw_message": text[:500],
            "source": "stat",
        }
    except Exception as e:
        print(f"  Parse error: {e} | {text[:100]}")
        return None


def import_file(json_path: str, channel_exchange: str):
    print(f"Importing {json_path} as {channel_exchange} stat trades...")
    print(f"  DB: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM trades WHERE source='stat' AND exchange=?", (channel_exchange,))
    existing = c.fetchone()[0]
    if existing > 0:
        print(f"  WARNING: {existing} stat trades for {channel_exchange} already in DB.")
        answer = input("  Delete and re-import? (y/n): ").strip().lower()
        if answer == 'y':
            c.execute("DELETE FROM trades WHERE source='stat' AND exchange=?", (channel_exchange,))
            conn.commit()
            print(f"  Deleted {existing} records.")
        else:
            print("  Skipping.")
            conn.close()
            return

    parsed = 0
    skipped = 0
    total = 0
    batch = []

    with open(json_path, 'rb') as f:
        for msg in ijson.items(f, 'messages.item'):
            total += 1
            if msg.get('type') != 'message':
                skipped += 1
                continue

            text = flatten_text(msg.get('text', ''))
            date = msg.get('date', '')
            trade = parse_stat_message(text, date, channel_exchange)

            if trade:
                batch.append((
                    trade["timestamp"], trade["trader"], trade["exchange"],
                    trade["side"], trade["coin"], trade["distance"],
                    trade["buffer"], trade["take_profit"],
                    trade["profit_usd"], trade["profit_pct"],
                    trade["is_profit"], trade["raw_message"], trade["source"]
                ))
                parsed += 1
            else:
                skipped += 1

            if len(batch) >= 10000:
                c.executemany(
                    '''INSERT INTO trades
                       (timestamp,trader,exchange,side,coin,distance,buffer,take_profit,
                        profit_usd,profit_pct,is_profit,raw_message,source)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    batch
                )
                conn.commit()
                batch = []
                print(f"  Progress: {parsed} trades / {total} messages...")

    if batch:
        c.executemany(
            '''INSERT INTO trades
               (timestamp,trader,exchange,side,coin,distance,buffer,take_profit,
                profit_usd,profit_pct,is_profit,raw_message,source)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            batch
        )
        conn.commit()

    conn.close()
    print(f"\n  Done!")
    print(f"  Total messages: {total}")
    print(f"  Trades imported: {parsed}")
    print(f"  Skipped: {skipped}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python import_stats.py <result.json> <exchange>")
        print("  exchange: Binance, Bybit, OKX")
        print()
        print("Examples:")
        print("  python import_stats.py result_binance.json Binance")
        print("  python import_stats.py result_bybit.json Bybit")
        print("  python import_stats.py result_okx.json OKX")
        sys.exit(1)

    json_path = sys.argv[1]
    exchange = sys.argv[2]

    if exchange not in ('Binance', 'Bybit', 'OKX'):
        print(f"Unknown exchange: {exchange}. Use: Binance, Bybit, OKX")
        sys.exit(1)

    import_file(json_path, exchange)
