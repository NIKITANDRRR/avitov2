# Avito Monitor — Система мониторинга объявлений

Система для автоматического поиска товаров ниже рыночной цены на Avito.
Собирает объявления, анализирует цены с помощью продвинутых статистических методов и отправляет уведомления в Telegram о недооценённых предложениях.

---

## Возможности

- **Продвинутый анализ цен v2**: сегментация по состоянию/локации/типу продавца, IQR-фильтрация выбросов, Z-score анализ, составной score недооценённости
- **Циклический планировщик**: автоматический запуск каждые 5 минут с проверкой просроченных поисков
- **Масштабирование поиска**: батчевая обработка до 20+ поисковых запросов с `asyncio.Semaphore` для параллельности
- Мониторинг поисковых запросов из БД с настраиваемым расписанием и приоритетами
- Сбор данных через Playwright + Chromium (без официального API)
- Парсинг поисковых страниц и карточек объявлений из HTML
- Хранение в PostgreSQL с дедупликацией по `avito_id`
- Telegram уведомления о товарах ниже рынка
- Резервные email-уведомления при недоступности Telegram (SMTP)
- Low-traffic режим: случайные задержки между действиями, headful браузер
- Сохранение сырого HTML для повторного анализа
- Автоматическая миграция схемы БД при обновлении

---

## Архитектура

Проект состоит из следующих модулей:

| Модуль | Назначение |
|---|---|
| [`app/config/`](app/config/) | Конфигурация на основе pydantic-settings |
| [`app/collector/`](app/collector/) | Сбор данных через Playwright + Chromium |
| [`app/parser/`](app/parser/) | HTML-парсеры поисковых страниц и карточек |
| [`app/storage/`](app/storage/) | PostgreSQL через SQLAlchemy (модели, репозиторий) |
| [`app/analysis/`](app/analysis/) | Продвинутый ценовой анализатор v2 (сегментация, IQR, Z-score, составной score) |
| [`app/notifier/`](app/notifier/) | Telegram и Email уведомления (fallback) |
| [`app/scheduler/`](app/scheduler/) | Оркестрация пайплайна, планировщик и CLI (Typer) |
| [`app/utils/`](app/utils/) | Утилиты, вспомогательные функции и кастомные исключения |

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

### Шаг 5: Заполнение поисковых запросов (опционально)

```bash
python scripts/seed_searches.py
```

### Шаг 6: Проверка Telegram

```bash
python -m app.main test-telegram
```

### Шаг 7: Запуск

**Циклический планировщик** (автоматический запуск каждые 5 минут):
```bash
python -m app.main run
```

**Один цикл** (разовый запуск):
```bash
python -m app.main run-once
```

---

## Конфигурация

Все параметры настраиваются через файл [`.env`](.env.example). Полный список:

### Основные параметры

| Параметр | Описание | По умолчанию |
|---|---|---|
| `SEARCH_URLS` | URL поисков Avito (через запятую) | — |
| `MAX_SEARCH_PAGES_PER_RUN` | Макс. страниц поиска за запуск | `3` |
| `MAX_ADS_PER_SEARCH_PER_RUN` | Макс. новых объявлений на поиск за запуск | `3` |
| `MIN_DELAY_SECONDS` | Мин. задержка между действиями (сек) | `5.0` |
| `MAX_DELAY_SECONDS` | Макс. задержка между действиями (сек) | `15.0` |
| `STARTUP_DELAY_MIN` | Мин. задержка перед стартом (сек) | `0.0` |
| `STARTUP_DELAY_MAX` | Макс. задержка перед стартом (сек) | `30.0` |
| `HEADLESS` | Безголовый режим браузера | `false` |
| `USE_PROXY` | Использовать прокси | `false` |
| `PROXY_URL` | URL прокси-сервера | — |

### Уведомления

| Параметр | Описание | По умолчанию |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | Токен Telegram бота | — |
| `TELEGRAM_CHAT_ID` | ID чата Telegram | — |
| `TELEGRAM_API_ID` | API ID для MTProto | `0` |
| `TELEGRAM_API_HASH` | API Hash для MTProto | — |
| `MTPROXY_ENABLED` | Использовать MTProto proxy | `true` |
| `MTPROXY_ADDRESS` | Адрес MTProxy сервера | `135.136.188.80` |
| `MTPROXY_PORT` | Порт MTProxy | `15871` |
| `MTPROXY_SECRET` | Секрет MTProxy | — |
| `SMTP_HOST` | SMTP сервер для email-уведомлений | — |
| `SMTP_PORT` | Порт SMTP сервера | `587` |
| `SMTP_USER` | Логин SMTP | — |
| `SMTP_PASSWORD` | Пароль SMTP | — |
| `SMTP_USE_TLS` | Использовать TLS | `true` |
| `EMAIL_FROM` | Email отправителя | — |
| `EMAIL_TO` | Email получателей (через запятую) | — |

### Анализ цен

| Параметр | Описание | По умолчанию |
|---|---|---|
| `UNDERVALUE_THRESHOLD` | Порог недооценённости v1 (0–1) | `0.8` |
| `TRIM_PERCENT` | % отбрасывания с каждого края распределения | `0.05` |
| `IQR_MULTIPLIER` | Множитель для IQR fences | `1.5` |
| `TEMPORAL_WINDOW_DAYS` | Окно анализа в днях | `14` |
| `MIN_SEGMENT_SIZE` | Минимальный размер сегмента для анализа | `3` |
| `UNDERVALUED_THRESHOLD` | Порог составного score для недооценённости v2 | `0.3` |
| `ZSCORE_THRESHOLD` | Порог Z-score для аномалий | `1.5` |
| `MEDIAN_DISCOUNT_THRESHOLD` | Порог % от медианы для недооценённости | `0.85` |

### Масштабирование поиска

| Параметр | Описание | По умолчанию |
|---|---|---|
| `MAX_CONCURRENT_SEARCHES` | Макс. параллельных поисков в батче | `3` |
| `DEFAULT_SCHEDULE_INTERVAL_HOURS` | Интервал запуска по умолчанию (часы) | `2` |
| `DEFAULT_MAX_ADS_TO_PARSE` | Карточек на поиск за запуск по умолчанию | `3` |
| `BATCH_DELAY_SECONDS` | Задержка между батчами поисков (сек) | `30` |
| `SEARCH_DELAY_SECONDS` | Задержка между поисками в батче (сек) | `5` |

### Хранение и логирование

| Параметр | Описание | По умолчанию |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://avito:avito@localhost:5432/avito_monitor` |
| `RAW_HTML_PATH` | Путь к каталогу с HTML-файлами | `data/raw_html` |
| `LOG_LEVEL` | Уровень логирования | `INFO` |

Конфигурация загружается через [`Settings`](app/config/settings.py) (pydantic-settings).

---

## CLI команды

| Команда | Описание |
|---|---|
| `python -m app.main run` | Запустить циклический планировщик (каждые 5 минут) |
| `python -m app.main run-once` | Запустить один цикл сбора и анализа |
| `python -m app.main init-db` | Создать таблицы в БД + миграции |
| `python -m app.main test-telegram` | Проверить подключение к Telegram |

Точка входа — [`app/main.py`](app/main.py), CLI реализован на [Typer](app/scheduler/cli.py).

---

## Скрипты

| Скрипт | Описание |
|---|---|
| [`scripts/init_db.py`](scripts/init_db.py) | Инициализация БД — создание таблиц + миграция новых колонок |
| [`scripts/seed_searches.py`](scripts/seed_searches.py) | Начальное заполнение поисковых запросов из `SEARCH_URLS` в БД |
| [`scripts/cleanup_duplicates.py`](scripts/cleanup_duplicates.py) | Очистка дубликатов объявлений в БД |

---

## Где хранятся данные

| Данные | Путь |
|---|---|
| HTML поисковых страниц | [`data/raw_html/search/`](data/raw_html/search/) |
| HTML карточек объявлений | [`data/raw_html/ad/`](data/raw_html/ad/) |
| Структурированные данные | PostgreSQL (таблицы `tracked_searches`, `search_runs`, `ads`, `ad_snapshots`, `notifications_sent`) |

Схема БД определена в [`app/storage/models.py`](app/storage/models.py).

---

## Как работает анализ цен v2

### Алгоритм

1. **Временной фильтр**: оставляются только объявления не старше `TEMPORAL_WINDOW_DAYS` дней
2. **Сегментация**: объявления группируются по ключу `{состояние}_{локация}_{тип продавца}`
3. **Trim**: отбрасываются `TRIM_PERCENT` самых дешёвых и дорогих (по 5% с каждого края)
4. **IQR-фильтрация**: удаляются выбросы за пределами `[Q1 - k×IQR, Q3 + k×IQR]`
5. **Расчёт статистики**: медиана, Q1, Q3, IQR, std, trimmed_mean, Z-score
6. **Составной score недооценённости**: взвешенная сумма IQR-score, Z-score и отклонения от медианы

### Составной критерий недооценённости

Объявление считается недооценённым, если `undervalue_score > UNDERVALUED_THRESHOLD`:

```
undervalue_score = 0.4 × iqr_score + 0.3 × zscore_score + 0.3 × median_score
```

Где:
- `iqr_score` — нормализованное отклонение от Q1
- `zscore_score` — нормализованный Z-score
- `median_score` — отклонение от медианы

**Пример:** медиана = 100 000 ₽, Q1 = 85 000 ₽, товар за 60 000 ₽ → IQR-аномалия + высокий Z-score → undervalued

Реализация — в [`app/analysis/analyzer.py`](app/analysis/analyzer.py).

---

## Как работает планировщик

Система использует двухуровневую архитектуру:

1. **[`Scheduler`](app/scheduler/scheduler.py)** — циклический планировщик, проверяет каждые 5 минут наличие просроченных поисков
2. **[`Pipeline`](app/scheduler/pipeline.py)** — обрабатывает поиски батчами с `asyncio.Semaphore` для параллельности

Каждый поиск в БД имеет настраиваемый `schedule_interval_hours` и `priority`. Pipeline выбирает поиски, у которых `last_run_at` превышает интервал, и обрабатывает их параллельно с ограничением `MAX_CONCURRENT_SEARCHES`.

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

## Структура проекта

```
avito/
├── app/
│   ├── __init__.py
│   ├── main.py                     # Точка входа CLI
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py             # Pydantic Settings (все параметры)
│   ├── collector/
│   │   ├── __init__.py
│   │   ├── browser.py              # Playwright collector
│   │   └── collector.py            # Сборщик данных (извлечение seller_type)
│   ├── parser/
│   │   ├── __init__.py
│   │   ├── search_parser.py        # Парсер поисковых страниц
│   │   └── ad_parser.py            # Парсер карточек объявлений
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── database.py             # Подключение к БД, session manager
│   │   ├── models.py               # SQLAlchemy ORM-модели (расширенные)
│   │   └── repository.py           # Repository pattern — CRUD + аналитические запросы
│   ├── analysis/
│   │   ├── __init__.py
│   │   └── analyzer.py             # Продвинутый анализ v2 (сегментация, IQR, Z-score)
│   ├── notifier/
│   │   ├── __init__.py
│   │   ├── telegram_notifier.py    # Telegram уведомления
│   │   └── email_notifier.py       # Email уведомления (fallback)
│   ├── scheduler/
│   │   ├── __init__.py
│   │   ├── cli.py                  # CLI команды (run, run-once, init-db, test-telegram)
│   │   ├── pipeline.py             # Пайплайн обработки поисков (батчевая обработка)
│   │   └── scheduler.py            # Циклический планировщик (5-минутный цикл)
│   └── utils/
│       ├── __init__.py
│       ├── exceptions.py           # Кастомные исключения
│       └── helpers.py              # Утилиты (задержки, URL, HTML, build_avito_url)
├── data/
│   └── raw_html/
│       ├── search/                 # HTML поисковых страниц
│       └── ad/                     # HTML карточек объявлений
├── scripts/
│   ├── init_db.py                  # Инициализация БД + миграции
│   ├── seed_searches.py            # Заполнение поисковых запросов
│   └── cleanup_duplicates.py       # Очистка дубликатов
├── tests/
│   └── __init__.py
├── docs/
│   ├── architecture.md             # Архитектурная документация
│   └── improvements_plan.md        # План улучшений
├── .env.example                    # Пример файла конфигурации
├── requirements.txt                # Зависимости Python
└── README.md                       # Этот файл
```

---

## Планы развития

- Добавление proxy provider для ротации IP
- Больше стратегий анализа цен (ML-модели)
- Веб-интерфейс для управления и мониторинга
- Исторические графики цен
- Docker-контейнеризация
- Alembic для управления миграциями
