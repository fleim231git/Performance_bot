# BatWeek (BotVik) — Telegram Trading Performance Bot

## О проекте
- Тип: Telegram-бот для аналитики трейдинговой группы
- Стек: Python 3.11+ / python-telegram-bot 21.5 / Anthropic Claude API / SQLite
- Основной файл: `bot.py` (~2350 строк)
- Деплой: Render Background Worker + Persistent Disk (`/data/trades.db`)

## Структура
```
bot.py              — весь код бота (парсер, БД, AI tools, команды, scheduled reports)
export_csv.py       — конвертер Telegram JSON export → CSV.gz (для загрузки stat данных)
import_stats.py     — прямой импорт JSON в SQLite (альтернатива CSV)
stat_binance.csv.gz — 526к stat-сделок Binance (автоимпорт при старте)
requirements.txt    — зависимости (python-telegram-bot, anthropic)
render.yaml         — конфигурация деплоя на Render
```

## Env-переменные
- `BOT_TOKEN` — токен Telegram-бота
- `ANTHROPIC_API_KEY` — ключ Anthropic API
- `OPENAI_API_KEY` — (опц.) для Whisper voice transcription
- `SOURCE_CHAT_ID` — чат откуда парсятся live-сделки
- `STAT_CHAT_IDS` — чаты stat-аккаунтов (через запятую: `-100XXX,-100YYY`)
- `REPORT_CHAT_ID` / `REPORT_THREAD_ID` — куда шлются отчёты
- `DIALOG_CHAT_ID` — чат для AI-диалога
- `BOT_USERNAME` — username бота для обнаружения упоминаний

## Запуск
```bash
python bot.py
```

## Архитектура bot.py

### Два источника данных
- **live** (`SOURCE_CHAT_ID`) — реальные сделки трейдеров, PnL в долларах
- **stat** (`STAT_CHAT_IDS`) — статистические аккаунты, PnL в процентах (ROE)
- Разделение через колонку `source` в таблице `trades`
- По умолчанию tools показывают только live, `source="stat"` для стата

### Парсер сделок
- `parse_trade()` — live-сделки (4 формата: DCC, имя-биржа, стандартный)
- `parse_stat_trade()` — stat-сделки (форматы: `stat12, BUY 0.70 3-5 OBSERV:`, `STATS-bnc-31, BUY 0.70:`)
- Извлекает: трейдер, биржа, сторона, монета, дистанс, дельта (15-мин), PnL
- Кэпинг: profit > 110% → 110%, loss < -106% → -106%
- STAT_EXCHANGE_MAP: `bnc→Binance, bbt→Bybit, okx→OKX`

### База данных
- SQLite: таблица `trades` (16 колонок)
- Ключевые поля: timestamp, trader, exchange, side, coin, distance, profit_usd, profit_pct, source
- Дельты: delta_min, delta_max (15-минутная абсолютная дельта в момент входа)
- `init_db()` — создание + автомиграция + автоимпорт stat_*.csv.gz
- `save_trade()` — вставка одной сделки

### Дистансы
- `get_smart_distance()` — умная логика:
  - spread <= 3% → стабильный (среднее)
  - spread > 3% → рабочий (медиана прибыльных) + страховочный (P90)

### Claude AI Tools (7 штук)
1. `get_trader_stats` — статистика трейдера по монетам
2. `get_coin_stats` — статистика по монете
3. `get_top_coins` — топ монет с дистансами
4. `get_all_traders` — список всех трейдеров
5. `get_period_stats` — статистика за период
6. `get_delta_analysis` — анализ по дельтам (4 режима: delta/coin/distance/combo)
7. `web_search` — поиск в интернете

- `execute_tool()` — диспетчер вызовов tools
- `claude_reply()` — agentic loop (до 5 tool calls)
- Модели: Haiku 4.5 (обычные запросы), Sonnet 4.6 (web search)
- Stat данные: SUM(profit_pct) = ROE, live данные: SUM(profit_usd) = PnL

### Telegram-команды
- /report, /report_daily, /period, /top, /trader, /coins, /coin, /stats, /cost, /clear, /help

### Расписание отчётов
- Ежедневно в 20:00 — дневной + недельный
- По воскресеньям в 20:00 — расширенный недельный

## Важные правила
- `apply_source_filter()` — обязательно использовать во всех SQL-запросах
- Stat = проценты (ROE), Live = доллары (PnL) — никогда не смешивать
- Дельты есть только у ~35% старых stat данных (остальные NULL)
- CSV-импорт обратно совместим (старый формат 13 колонок, новый 16)
- Telegram Markdown fallback: try/except при отправке, fallback на plain text

## Известные проблемы / TODO
- Весь код в одном файле (2350 строк)
- `get_smart_distance()` N+1 queries в циклах
- `conversation_history` теряет контекст после tool use
- Knowledge base + feedback система (планируется)
