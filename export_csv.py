import ijson
import re
import sys
import gzip
import csv
import io


def flatten_text(text_field) -> str:
    if isinstance(text_field, str):
        return text_field
    if isinstance(text_field, list):
        return ''.join(p if isinstance(p, str) else p.get('text', '') for p in text_field)
    return ''


def parse_and_export(json_path: str, exchange: str, output_path: str):
    print(f"Parsing {json_path}...")
    parsed = 0
    total = 0

    with gzip.open(output_path, 'wt', encoding='utf-8', newline='') as out:
        writer = csv.writer(out)

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
                distance = float(dist_match.group(1)) if dist_match else ""

                delta_match = re.search(r'\b(?:BUY|SELL)\s+[\d.]+\s+([\d.]+)-([\d.]+)', text, re.IGNORECASE)
                delta_min = float(delta_match.group(1)) if delta_match else ""
                delta_max = float(delta_match.group(2)) if delta_match else ""

                is_observ = 1 if 'OBSERV' in text else 0

                pnl_match = re.search(r'(?:Profit|Loss)\s*([+-]?\d+\.?\d*)[$]', text, re.IGNORECASE)
                if not pnl_match:
                    continue
                profit_usd = float(pnl_match.group(1))

                pct_match = re.search(r'[$]\s*\(([+-]?\d+\.?\d*)%\)', text)
                profit_pct = float(pct_match.group(1)) if pct_match else ""

                coin_match = re.search(r'#([^\s]+USDT)', text)
                if not coin_match:
                    continue
                coin = coin_match.group(1)

                tp_matches = re.findall(r'\(([+-]?\d+\.?\d*)%\)', text)
                take_profit = float(tp_matches[-1]) if tp_matches else ""

                if profit_pct != "" and profit_usd != 0:
                    if profit_pct > 110:
                        profit_usd = round(profit_usd * (110 / profit_pct), 4)
                        profit_pct = 110.0
                    elif profit_pct < -106:
                        profit_usd = round(profit_usd * (-106 / profit_pct), 4)
                        profit_pct = -106.0

                writer.writerow([
                    date, trader, exchange, side, coin,
                    distance, "",  # buffer
                    take_profit, profit_usd, profit_pct,
                    is_profit, text[:500], "stat",
                    delta_min, delta_max, is_observ
                ])
                parsed += 1

                if total % 100000 == 0:
                    print(f"  {total} messages processed, {parsed} trades...")

    print(f"\nDone! {parsed} trades from {total} messages")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python export_csv.py <result.json> <exchange>")
        sys.exit(1)
    parse_and_export(sys.argv[1], sys.argv[2], f"stat_{sys.argv[2].lower()}.csv.gz")
