# Avito Monitor — PoC система мониторинга объявлений

Система для автоматического поиска товаров ниже рыночной цены на Avito.
Собирает объявления, анализирует цены и отправляет уведомления в Telegram о недооценённых предложениях.

---

## Возможности

- Мониторинг до 3 поисковых запросов одновременно
- Сбор данных через Playwright + Chromium (без официального API)
- Парсинг поисковых страниц и карточек объявлений из HTML
- Хранение в PostgreSQL с дедупликацией по `avito_id`
- Анализ цен: медиана, Q1, определение недооценённых товаров (undervalued)
- Telegram уведомления о товарах ниже рынка
- Резервные email-уведомления при недоступности Telegram (SMTP)
- Low-traffic режим: случайные задержки между действиями, headful браузер
- Сохранение сырого HTML для повторного анализа

---

## Архитектура

Проект состоит из следующих модулей:

| Модуль | Назначение |
|---|---|
| [`app/config/`](app/config/) | Конфигурация на основе pydantic-settings |
| [`app/collector/`](app/collector/) | Сбор данных через Playwright + Chromium |
| [`app/parser/`](app/parser/) | HTML-парсеры поисковых страниц и карточек |
| [`app/storage/`](app/storage/) | PostgreSQL через SQLAlchemy (модели, репозиторий) |
| [`app/analysis/`](app/analysis/) | Ценовой анализатор (медиана, undervalued) |
| [`app/notifier/`](app/notifier/) | Telegram и Email уведомления (fallback) |
| [`app/scheduler/`](app/scheduler/) | Оркестрация пайплайна и CLI (Typer) |
| [`app/utils/`](app/utils/) | Утилиты и кастомные исключения |

Подробное описание архитектуры — в [`docs/architecture.md`](docs/architecture.md).

---

## Требования

- **Python** 3.11+
- **PostgreSQL** 14+
- **Windows** / **Linux** / **macOS**

---

## Быстрый старт

### Шаг 1: Клонирование и установка зависимостей

```bash
cd avito
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

### Шаг 2: Настройка PostgreSQL

```bash
psql -U postgres
```

```sql
CREATE USER avito WITH PASSWORD 'avito';
CREATE DATABASE avito_monitor OWNER avito;
\q
```

### Шаг 3: Настройка `.env`

```bash
copy .env.example .env
```

Отредактируйте [`.env`](.env.example): укажите `SEARCH_URLS`, `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID`.

### Шаг 4: Инициализация БД

```bash
python -m app.main init-db
```

### Шаг 5: Проверка Telegram

```bash
python -m app.main test-telegram
```

### Шаг 6: Запуск одного цикла

```bash
python -m app.main run
```

---

## Конфигурация

Все параметры настраиваются через файл [`.env`](.env.example). Полный список:

| Параметр | Описание | По умолчанию |
|---|---|---|
| `SEARCH_URLS` | URL поисков Avito (через запятую, до 3) | — |
| `MAX_SEARCH_PAGES_PER_RUN` | Макс. страниц поиска за запуск | `3` |
| `MAX_ADS_PER_SEARCH_PER_RUN` | Макс. новых объявлений на поиск за запуск | `3` |
| `MIN_DELAY_SECONDS` | Мин. задержка между действиями (сек) | `5.0` |
| `MAX_DELAY_SECONDS` | Макс. задержка между действиями (сек) | `15.0` |
| `STARTUP_DELAY_MIN` | Мин. задержка перед стартом (сек) | `0.0` |
| `STARTUP_DELAY_MAX` | Макс. задержка перед стартом (сек) | `30.0` |
| `HEADLESS` | Безголовый режим браузера | `false` |
| `USE_PROXY` | Использовать прокси | `false` |
| `PROXY_URL` | URL прокси-сервера | — |
| `TELEGRAM_BOT_TOKEN` | Токен Telegram бота | — |
| `TELEGRAM_CHAT_ID` | ID чата Telegram | — |
| `SMTP_HOST` | SMTP сервер для email-уведомлений | — |
| `SMTP_PORT` | Порт SMTP сервера | `587` |
| `SMTP_USER` | Логин SMTP | — |
| `SMTP_PASSWORD` | Пароль SMTP | — |
| `SMTP_USE_TLS` | Использовать TLS | `true` |
| `EMAIL_FROM` | Email отправителя | — |
| `EMAIL_TO` | Email получателей (через запятую) | — |
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://avito:avito@localhost:5432/avito_monitor` |
| `UNDERVALUE_THRESHOLD` | Порог недооценённости (0–1) | `0.8` |
| `RAW_HTML_PATH` | Путь к каталогу с HTML-файлами | `data/raw_html` |
| `LOG_LEVEL` | Уровень логирования | `INFO` |

Конфигурация загружается через [`Settings`](app/config/settings.py:9) (pydantic-settings).

---

## CLI команды

| Команда | Описание |
|---|---|
| `python -m app.main run` | Запустить один цикл сбора и анализа |
| `python -m app.main init-db` | Создать таблицы в БД |
| `python -m app.main test-telegram` | Проверить подключение к Telegram |

Точка входа — [`app/main.py`](app/main.py), CLI реализован на [Typer](app/scheduler/cli.py).

---

## Где хранятся данные

| Данные | Путь |
|---|---|
| HTML поисковых страниц | [`data/raw_html/search/`](data/raw_html/search/) |
| HTML карточек объявлений | [`data/raw_html/ad/`](data/raw_html/ad/) |
| Структурированные данные | PostgreSQL (таблицы `tracked_searches`, `search_runs`, `ads`, `ad_snapshots`, `notifications_sent`) |

Схема БД определена в [`app/storage/models.py`](app/storage/models.py).

---

## Как работает анализ цен

1. Для каждого поискового запроса собираются все известные цены из базы
2. Рассчитывается медиана цен
3. Если цена объявления < медиана × `UNDERVALUE_THRESHOLD` → товар помечается как **undervalued**
4. Пользователь получает Telegram уведомление
5. Если Telegram недоступен — уведомление отправляется на email (fallback)

**Пример:** медиана = 100 000 ₽, порог = 0.8 → товар за 75 000 ₽ = undervalued

Реализация — в [`app/analysis/analyzer.py`](app/analysis/analyzer.py).

---

## Как работает fallback уведомлений

1. Сначала система пытается отправить уведомление через **Telegram**
2. Если Telegram заблокирован, вернул ошибку или отправил 0 уведомлений — автоматически переключается на **Email** (SMTP)
3. Email отправляется на все адреса из `EMAIL_TO` (можно указать несколько через запятую)
4. Для Gmail нужно использовать [пароль приложения](https://support.google.com/accounts/answer/185833), а не обычный пароль

Реализация — в [`app/notifier/email_notifier.py`](app/notifier/email_notifier.py).

---

## Как получить Telegram Bot Token

1. Откройте [@BotFather](https://t.me/BotFather) в Telegram
2. Отправьте `/newbot`
3. Следуйте инструкциям бота
4. Скопируйте полученный токен в `TELEGRAM_BOT_TOKEN` в [`.env`](.env.example)

---

## Как получить Telegram Chat ID

1. Добавьте бота в группу или напишите ему напрямую
2. Отправьте любое сообщение
3. Откройте в браузере: `https://api.telegram.org/bot<TOKEN>/getUpdates`
4. Найдите `chat_id` в JSON-ответе
5. Укажите его в `TELEGRAM_CHAT_ID` в [`.env`](.env.example)

---

## Ограничения PoC

- Максимум 3 поисковых URL
- 2–3 новых объявления на поиск за запуск
- Без пагинации глубже 1 страницы
- Без параллельного сбора (последовательная обработка)
- Без автоматического планировщика (запуск вручную через CLI)

---

## Планы развития

- Добавление proxy provider для ротации IP
- Планировщик (cron / systemd / встроенный scheduler)
- Больше стратегий анализа цен (Z-score, IQR, ML)
- Веб-интерфейс для управления и мониторинга
- Исторические графики цен
- Docker-контейнеризация
