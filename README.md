# Trade Bot — деплой на Render

## Файлы
| Файл | Назначение |
|---|---|
| `bot.py` | Основной код бота |
| `requirements.txt` | Зависимости Python |
| `render.yaml` | Конфиг автодеплоя на Render |

---

## Переменные окружения (Render → Environment)

| Переменная | Что вписать |
|---|---|
| `BOT_TOKEN` | Токен от @BotFather |
| `ANTHROPIC_API_KEY` | Ключ с console.anthropic.com |
| `SOURCE_CHAT_ID` | ID канала/чата с трейдами |
| `REPORT_CHAT_ID` | ID чата куда слать авто-отчёты |
| `DIALOG_CHAT_ID` | ID чата для AI-диалога |
| `BOT_USERNAME` | Username бота **без @** (напр. `mytradebot`) |

### Как узнать ID чата
1. Добавь бота в чат
2. Напиши любое сообщение
3. Открой: `https://api.telegram.org/bot<TOKEN>/getUpdates`
4. Найди поле `"chat": {"id": ...}`

---

## Деплой пошагово

### 1. GitHub
```bash
git init
git add .
git commit -m "init"
git remote add origin https://github.com/YOU/trade-bot.git
git push -u origin main
```

### 2. Render
1. Зайди на **render.com** → New → **Blueprint** → подключи репозиторий
2. Render подхватит `render.yaml` автоматически
3. Перейди в **Environment** и заполни все переменные из таблицы выше
4. Нажми **Deploy**

> ⚠️ Бесплатный план Render засыпает после 15 мин без активности.
> Для worker-процесса (не web) это не проблема — он не засыпает.

---

## Команды бота

| Команда | Описание |
|---|---|
| `/report` | Отчёт за 7 дней |
| `/report_daily` | Отчёт за сегодня |
| `/top [N]` | Топ-N трейдеров за всё время (по умолч. 10) |
| `/stats` | Кол-во сделок и суммарный PnL |
| `/clear` | Очистить историю AI-диалога |
| `/help` | Справка |

## AI-диалог
Бот отвечает **только** если:
- Упомянуть `@BOT_USERNAME` в сообщении
- Ответить (Reply) на сообщение бота

История диалога хранится в памяти (до 20 сообщений на пользователя).

## Авто-отчёты
- **Ежедневно** в 20:00 → `REPORT_CHAT_ID`
- **Воскресенье** в 20:00 → расширенный недельный отчёт

---

## Если БД хранится в /data (Render Disk)
В `bot.py` замени:
```python
DB_PATH = "trades.db"
```
на:
```python
DB_PATH = "/data/trades.db"
```
Это обеспечит сохранность данных при перезапуске.
