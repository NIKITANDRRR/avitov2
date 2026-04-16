# Avito Price Monitor

Система мониторинга цен на объявления Avito с автоматическим обнаружением недооценённых товаров.

## Как работает проект (шаг за шагом)

1. **Сбор данных** — Playwright открывает Avito, собирает HTML страниц поиска
2. **Парсинг** — BeautifulSoup извлекает структурированные данные (цена, название, фото, параметры)
3. **Сохранение** — данные записываются в PostgreSQL, отслеживается история цен
4. **Анализ сегментов** — объявления группируются по бренду/модели, рассчитываются медианы за 7/30/90 дней
5. **Трекинг оборачиваемости** — отслеживается сколько дней объявление на рынке, быстро ли исчезло
6. **Детекция недооценённых** — составной критерий: IQR + Z-score + % от медианы
7. **Детекция «бриллиантов»** — редкие товары с ценой << ликвидной оценки
8. **Уведомления** — Telegram и/или Email о найденных недооценённых товарах

## Планировщик

В проекте есть планировщик ([`app/scheduler/scheduler.py`](app/scheduler/scheduler.py)). Он работает как бесконечный цикл:

- Запускает цикл сбора и анализа каждые 50 минут
- Каждый поисковый запрос имеет свой интервал (по умолчанию 2 часа)
- Ошибки не прерывают работу планировщика
- Поиски обрабатываются батчами по 3 параллельно через `asyncio.Semaphore`
- **Пагинация**: обход до 50 страниц поисковой выдачи за один запуск поиска
- **Параллельный сбор карточек**: до 5 вкладок открываются одновременно
- Пагинация прекращается досрочно, если на странице нет новых объявлений

## Быстрый старт

### 1. Установка

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

### 2. Настройка PostgreSQL

```sql
CREATE USER avito WITH PASSWORD 'avito';
CREATE DATABASE avito_monitor OWNER avito;
```

Скопируй `.env.example` в `.env` и заполни:

- `DATABASE_URL` — подключение к PostgreSQL
- `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID` — для Telegram-уведомлений
- `SEARCH_URLS` — URL поисковых запросов Avito (для legacy-режима)

### 3. Инициализация БД

```bash
python scripts/init_db.py
python scripts/migrate_segment_analysis.py --up
```

### 4. Запуск на постоянную работу

```bash
.venv\Scripts\activate

# Запуск планировщика (бесконечный цикл)
python -m app.main start

# Или один прогон без планировщика
python -m app.main run-once
```

## Режимы работы

| Команда | Описание |
|---------|----------|
| `python -m app.main start` | Полный запуск: инициализация + заполнение поисков + планировщик |
| `python -m app.main run-once` | Один цикл сбора и анализа |
| `python -m app.main run` | Legacy-режим по SEARCH_URLS из .env |
| `python -m app.main run-scheduler` | Планировщик по поисковым запросам из БД |

### Управление поисками

```bash
python -m app.main add-search "iPhone 15 Pro 128GB" --location "Москва" --interval 2 --max-ads 3 --priority 1
python -m app.main remove-search 5
python -m app.main list-searches
```

## Система анализа цен

### Многоуровневые медианы

- `median_7d` — свежая медиана за 7 дней
- `median_30d` — основная медиана за 30 дней
- `median_90d` — справочная медиана за 90 дней

Если `median_7d > median_30d` → рынок растёт, используются свежие данные.

### Двухуровневая цена

- `listing_price_median` — медиана по активным объявлениям
- `fast_sale_price_median` — медиана по объявлениям, которые быстро исчезли
- `liquid_market_estimate` — оценка реальной ликвидной цены

### Составной критерий недооценённости

```
undervalue_score = 0.4 × iqr_score + 0.3 × zscore_score + 0.3 × median_score
```

Объявление считается недооценённым при `score >= 0.3` и цене ниже порога.

### Детекция «бриллиантов»

Товар считается «бриллиантом» если:

- Цена < 70% от медианы активных объявлений
- И/или цена < 80% от медианы быстрых продаж
- И сегмент редкий + цена < 85% от лучшей медианы

### Редкие сегменты

Для товаров с малым количеством объявлений используется fallback:

1. Исторические данные сегмента
2. Родительский сегмент (brand вместо brand:model)
3. Текущая медиана с пониженной уверенностью

## Структура проекта

```
app/
├── config/          # Конфигурация (pydantic-settings)
├── collector/       # Сбор данных через Playwright + Chromium
├── parser/          # HTML-парсеры (BeautifulSoup + lxml)
├── storage/         # PostgreSQL через SQLAlchemy (модели, репозиторий)
├── analysis/        # Ценовой анализатор v2 + сегментный анализ
├── notifier/        # Telegram и Email уведомления
├── scheduler/       # Пайплайн, планировщик и CLI (Typer)
└── utils/           # Утилиты и исключения
scripts/             # Скрипты инициализации и миграций
data/raw_html/       # Сохранённый HTML (search/ и ad/)
```

## Скрипты

| Скрипт | Назначение |
|--------|-----------|
| [`scripts/init_db.py`](scripts/init_db.py) | Инициализация таблиц в БД |
| [`scripts/migrate_segment_analysis.py`](scripts/migrate_segment_analysis.py) | Миграция сегментного анализа |
| [`scripts/seed_searches.py`](scripts/seed_searches.py) | Заполнение поисковых запросов |
| [`scripts/seed_category_searches.py`](scripts/seed_category_searches.py) | Заполнение категорийных поисков |
| [`scripts/migrate_category_monitoring.py`](scripts/migrate_category_monitoring.py) | Миграция категорийного мониторинга |
| [`scripts/cleanup_duplicates.py`](scripts/cleanup_duplicates.py) | Очистка дубликатов |

## Переменные окружения

Основные параметры (полный список в [`.env.example`](.env.example)):

| Переменная | По умолчанию | Описание |
|---|---|---|
| `DATABASE_URL` | `postgresql://avito:avito@localhost:5432/avito_monitor` | Подключение к PostgreSQL |
| `TELEGRAM_BOT_TOKEN` | — | Токен Telegram-бота |
| `TELEGRAM_CHAT_ID` | — | ID чата Telegram |
| `SEARCH_URLS` | — | URL поиска Avito (через запятую, legacy) |
| `HEADLESS` | `false` | Headless-режим браузера |
| `MAX_CONCURRENT_SEARCHES` | `3` | Макс. параллельных поисков |
| `MAX_CONCURRENT_AD_PAGES` | `5` | Макс. параллельно открываемых карточек |
| `MAX_SEARCH_PAGES_PER_RUN` | `50` | Макс. страниц пагинации за поиск |
| `DEFAULT_SCHEDULE_INTERVAL_HOURS` | `2` | Интервал запуска поисков (часы) |
| `TEMPORAL_WINDOW_DAYS` | `14` | Окно анализа цен (дни) |
| `UNDERVALUED_THRESHOLD` | `0.3` | Порог composite score |
| `LOG_LEVEL` | `INFO` | Уровень логирования |

## Требования

- **Python** 3.11+
- **PostgreSQL** 14+
