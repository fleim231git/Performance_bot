import ijson
import re
import sys
import gzip


def flatten_text(text_field) -> str:
    if isinstance(text_field, str):
        return text_field
    if isinstance(text_field, list):
        return ''.join(p if isinstance(p, str) else p.get('text', '') for p in text_field)
    return ''


def escape_sql(s: str) -> str:
    return s.replace("'", "''")


def parse_and_export(json_path: str, exchange: str, output_path: str):
    print(f"Parsing {json_path}...")
    parsed = 0
    total = 0

    with gzip.open(output_path, 'wt', encoding='utf-8') as out:
        out.write("BEGIN TRANSACTION;\n")

        with open(json_path, 'rb') as f:
            for msg in ijson.items(f, 'messages.item'):
                total += 1
                if msg.get('type') != 'message':
                    continue

                text = flatten_text(msg.get('text', ''))
                date = msg.get('date', '')

                if ('Profit' not in text and 'Loss' not in text) or '$' not in text or '#' not in text:
                    continue

                is_profit = 1 if 'Profit' in text else 0

                trader_match = re.match(r'(STATS-[a-zA-Z]+-\d+|stat\d+)', text)
                trader = trader_match.group(1) if trader_match else "Unknown"

                side_match = re.search(r'\b(BUY|SELL)\b', text, re.IGNORECASE)
                side = side_match.group(1).upper() if side_match else "Unknown"

                dist_match = re.search(r'\b(?:BUY|SELL)\s+([\d.]+)', text, re.IGNORECASE)
                distance = dist_match.group(1) if dist_match else "NULL"

                pnl_match = re.search(r'(?:Profit|Loss)\s*([+-]?\d+\.?\d*)[$]', text, re.IGNORECASE)
                if not pnl_match:
                    continue
                profit_usd = pnl_match.group(1)

                pct_match = re.search(r'[$]\s*\(([+-]?\d+\.?\d*)%\)', text)
                profit_pct = pct_match.group(1) if pct_match else "NULL"

                coin_match = re.search(r'#([^\s]+USDT)', text)
                if not coin_match:
                    continue
                coin = coin_match.group(1)

                tp_matches = re.findall(r'\(([+-]?\d+\.?\d*)%\)', text)
                take_profit = tp_matches[-1] if tp_matches else "NULL"

                pnl_f = float(profit_usd)
                pct_f = float(profit_pct) if profit_pct != "NULL" else None
                if pct_f is not None and pnl_f != 0:
                    if pct_f > 110:
                        pnl_f = round(pnl_f * (110 / pct_f), 4)
                        pct_f = 110.0
                    elif pct_f < -106:
                        pnl_f = round(pnl_f * (-106 / pct_f), 4)
                        pct_f = -106.0

                raw = escape_sql(text[:500])
                dist_val = distance if distance != "NULL" else "NULL"
                tp_val = take_profit if take_profit != "NULL" else "NULL"
                pct_val = pct_f if pct_f is not None else "NULL"

                out.write(
                    f"INSERT INTO trades(timestamp,trader,exchange,side,coin,distance,buffer,"
                    f"take_profit,profit_usd,profit_pct,is_profit,raw_message,source) VALUES("
                    f"'{date}','{escape_sql(trader)}','{exchange}','{side}','{escape_sql(coin)}',"
                    f"{dist_val},NULL,{tp_val},{pnl_f},{pct_val},{is_profit},'{raw}','stat');\n"
                )
                parsed += 1

                if total % 100000 == 0:
                    print(f"  {total} messages processed, {parsed} trades...")

        out.write("COMMIT;\n")

    print(f"\nDone! {parsed} trades from {total} messages")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python export_sql.py <result.json> <exchange>")
        sys.exit(1)
    parse_and_export(sys.argv[1], sys.argv[2], f"stat_{sys.argv[2].lower()}.sql.gz")
