# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# BatWeek (BotVik) — Telegram Trading Performance Bot

## О проекте
- Тип: Telegram-бот для аналитики трейдинговой группы
- Стек: Python 3.11+ / python-telegram-bot 21.5 / Anthropic Claude API / SQLite
- Основной файл: `bot.py` (~2900 строк, весь код в одном файле)
- Деплой: Render Background Worker + Persistent Disk (`/data/trades.db`)

## Запуск
```bash
python bot.py
```
Тестов нет. Lint: `python -m py_compile bot.py`.

## Структура файлов
```
bot.py              — весь код бота
export_csv.py       — конвертер Telegram JSON export → CSV.gz
import_stats.py     — прямой импорт JSON в SQLite (альтернатива CSV)
stat_binance.csv.gz — 526к stat-сделок Binance (автоимпорт при старте)
requirements.txt    — python-telegram-bot==21.5, anthropic>=0.28.0
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
- `ADMIN_USER_ID` — числовой Telegram user ID (не @username!) для команд /backup и пр.

## Архитектура bot.py

### Два источника данных
- **live** (`SOURCE_CHAT_ID`) — реальные сделки трейдеров, PnL в долларах
- **stat** (`STAT_CHAT_IDS`) — статистические аккаунты, PnL в процентах (ROE)
- Разделение через колонку `source` в таблице `trades`
- По умолчанию tools показывают только live, `source="stat"` для стата, `source="all"` для обоих
- Stat = проценты (ROE), Live = доллары (PnL) — **никогда не смешивать**

### Нормализация имён
- `DCC_MAP` — аббревиатуры DCC-трейдеров (`dcc-dns` → `Денис`)
- `TRADER_MAP` — коды трейдеров (`D3N` → `Денис`, `DMR` → `Дима`)
- `EXCHANGE_MAP` — коды бирж в имени трейдера (`bnc` → `Binance`)
- `normalize_trader()` / `normalize_exchange()` — вызываются в обоих парсерах

### Парсер сделок
- `parse_trade()` — live-сделки (4 формата: с именем+биржей в скобках, через запятую, BUY/SELL с пробелом, с подчёркиванием)
- `parse_stat_trade()` — stat-сделки (форматы: `stat12, BUY 0.70 3-5 OBSERV:`, `STATS-bnc-31, BUY 0.70:`)
- Извлекает: трейдер, биржа, сторона, монета, дистанс, дельта (15-мин), PnL
- Кэпинг: profit > 110% → 110%, loss < -106% → -106% (пересчитывается и profit_usd)
- `STAT_EXCHANGE_MAP`: `bnc→Binance, bbt→Bybit, okx→OKX`

### База данных (4 таблицы)
- `trades` — основная таблица (16 колонок): timestamp, trader, exchange, side, coin, distance, buffer, take_profit, profit_usd, profit_pct, is_profit, raw_message, source, delta_min, delta_max, is_observ
- `knowledge` — база знаний: заметки пользователя (category, content)
- `feedback` — оценки ответов бота (question, answer, rating, comment)
- `query_log` — лог запросов (question, tools_used, tokens, cost)
- `init_db()` — создание + автомиграция `ALTER TABLE ADD COLUMN` + автоимпорт stat_*.csv.gz при первом запуске
- `save_trade()` — вставка одной сделки

### Дистансы
- `get_smart_distance(conn, where, params, coin)` — ключевая функция:
  - spread <= 3% → стабильный (среднее, ключ `smart=False`)
  - spread > 3% → рабочий (MAX SUM(profit)) + страховочный (P90)
  - Для stat: сортирует по `profit_pct` если `profit_usd` близок к 0
- `fmt_dist_info(dist_data)` — форматирует dict → строку с иконками ⚡️/🛡/🎯
- `apply_source_filter(where, params, source)` — **обязательно** во всех SQL-запросах

### Claude AI Tools (9 штук)
1. `get_trader_stats` — статистика трейдера по монетам
2. `get_coin_stats` — статистика по монете (stat: ROE + дельты, live: PnL + трейдеры)
3. `get_top_coins` — топ монет с дистансами (stat: ROE + дельта breakdown, live: PnL)
4. `get_all_traders` — список всех трейдеров
5. `get_period_stats` — статистика за период (stat: ROE + топ дельты, live: PnL + трейдеры)
6. `get_delta_analysis` — анализ по дельтам (4 режима: delta/coin/distance/combo)
7. `get_exchange_listings` — сравнение фьючерсных листингов Binance/Bybit/OKX (CoinGecko API)
8. `web_search` — поиск в интернете (модель Sonnet 4.6, остальные — Haiku 4.5)
9. `manage_knowledge` — CRUD по таблице `knowledge` (save/get/delete)

- `execute_tool()` — диспетчер вызовов tools
- `claude_reply()` — agentic loop (до 5 tool calls)
- `get_db_context()` — инжектируется в system prompt перед каждым AI-вызовом (сводка за сегодня + 7 дней)
- `conversation_history: dict[int, list]` — история диалога per chat_id, MAX_HISTORY=10 сообщений

### Telegram-команды
`/report`, `/report_daily`, `/period`, `/top`, `/trader`, `/coins`, `/coin`, `/stats`, `/cost`, `/clear`, `/help`

### Расписание отчётов
- Ежедневно в 20:00 — дневной + недельный
- По воскресеньям в 20:00 — расширенный недельный

## Важные правила
- `apply_source_filter()` — **обязательно** во всех SQL-запросах tools
- Tool result limit: 15K chars max (truncate чтобы Claude API не упал)
- Adaptive HAVING threshold: `max(1, min(5, cnt // 20))` для малых выборок
- Дельты есть только у ~35% старых stat данных (delta_min/delta_max = NULL)
- CSV-импорт обратно совместим (старый формат 13 колонок, новый 16)
- Telegram Markdown fallback: try/except при отправке, fallback на plain text
- CoinGecko derivatives API (бесплатный, работает из US IP) для exchange listings
- Автодетект stat: inject `[SYSTEM: source="stat"]` когда user message содержит stat-keywords

## Известные проблемы / TODO
- Весь код в одном файле (~2900 строк)
- `get_smart_distance()` N+1 queries в циклах
- `conversation_history` теряет контекст после tool use
- `ADMIN_USER_ID` на Render нужно числовой Telegram user ID (не "@username")
- Bybit/OKX stat channel исторические данные — нужен экспорт и импорт
- Автобэкап trades.db (нужен Telegram user ID для отправки)
