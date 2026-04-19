# Avito Price Monitor

Система мониторинга цен на объявления Avito с автоматическим обнаружением недооценённых товаров («бриллиантов»).

## Возможности

- **24/7 мониторинг (constant-режим)** — непрерывный циклический сбор данных с настраиваемым интервалом
- **Парсинг поисковой выдачи** — обход до 50 страниц пагинации, параллельный сбор карточек
- **Парсинг профилей продавцов** — автоматический сбор данных о проданных товарах (sold items)
- **Сегментный анализ категорий** — группировка объявлений по бренду/модели, медианы за 7/30/90 дней
- **Нормализация продуктов** — приведение названий к единому формату, определение бренда и модели
- **Детекция «бриллиантов»** — выявление недооценённых объявлений через IQR + Z-score + медианный анализ
- **Уведомления** — Telegram и Email о найденных недооценённых товарах

## Как работает проект (шаг за шагом)

1. **Сбор данных** — Playwright открывает Avito, собирает HTML страниц поиска
2. **Парсинг** — BeautifulSoup извлекает структурированные данные (цена, название, фото, параметры)
3. **Сохранение** — данные записываются в PostgreSQL, отслеживается история цен
4. **Нормализация** — названия приводятся к стандарту, определяется продукт (бренд + модель)
5. **Анализ сегментов** — объявления группируются по продукту, рассчитываются медианы за 7/30/90 дней
6. **Детекция недооценённых** — составной критерий: IQR + Z-score + % от медианы
7. **Детекция «бриллиантов»** — редкие товары с ценой << ликвидной оценки
8. **Парсинг профилей продавцов** — автоматический сбор данных о проданных товарах со страниц пользователей
9. **Уведомления** — Telegram и/или Email о найденных недооценённых товарах

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
python -m scripts.migrate_seller_sold_items
python scripts/migrate_products.py
```

### 4. Запуск

```bash
.venv\Scripts\activate

# Постоянный режим 24/7 (рекомендуется)
python -m app.main start --constant

# Обычный планировщик
python -m app.main start

# Один прогон без планировщика
python -m app.main run-once
```

## Режимы работы

| Команда | Описание |
|---------|----------|
| `python -m app.main start --constant` | **Постоянный режим 24/7**: init-db + seed + ConstantScheduler |
| `python -m app.main start` | Полный запуск: init-db + seed + планировщик |
| `python -m app.main run-once` | Один цикл сбора и анализа (по умолчанию — принудительно все поиски) |
| `python -m app.main run-once --no-force` | Один цикл — только просроченные по расписанию |
| `python -m app.main run` | Legacy-режим по SEARCH_URLS из .env |
| `python -m app.main run-scheduler` | Планировщик по поисковым запросам из БД |
| `python -m app.main force-parse` | Принудительный парсинг: товары сразу, затем категории по очереди |
| `python -m app.main force-pending` | Дообработка pending объявлений с ожиданием капчи (браузер видимый) |
| `python -m app.main init-db` | Инициализация таблиц в PostgreSQL |
| `python -m app.main test-telegram` | Проверка подключения к Telegram боту |

### Constant-режим (24/7 мониторинг)

Постоянный режим (`start --constant`) запускает [`ConstantScheduler`](app/scheduler/scheduler.py) — бесконечный цикл с настраиваемым интервалом:

- **Первый цикл** запускает все поиски принудительно (`force_all=True`)
- **Последующие циклы** запускают только просроченные по расписанию
- После каждого поиска автоматически дообрабатывает pending-объявления
- Поддерживает graceful shutdown по Ctrl+C
- Настраивается через переменные окружения:
  - `CONSTANT_CYCLE_INTERVAL_SECONDS` — интервал между циклами (по умолчанию 300 сек)
  - `CONSTANT_FORCE_PENDING_AFTER_SEARCH` — запускать ли force-pending после поиска
  - `CONSTANT_BROWSER_HEADLESS` — headless-режим браузера

### Режим force-pending

Команда `force-pending` предназначена для дообработки объявлений, которые были обнаружены в поисковой выдаче, но не удалось спарсить их карточки (статус `parse_status='pending'`). Особенности:

- **Браузер открывается в видимом режиме** (`headless=False`) — пользователь может вручную пройти капчу
- **Автоматическая детекция капчи** — Cloudflare, reCAPTCHA, hCaptcha, Bitrix CAPTCHA
- **Ожидание ввода капчи** — до 120 секунд на каждую попытку
- **Повторные попытки** — до 3 попыток на каждое объявление
- **Паузы между объявлениями** — 3–8 сек, каждые 5 объявлений — 15–25 сек

```bash
python -m app.main force-pending
```

### Управление поисками

```bash
python -m app.main add-search "iPhone 15 Pro 128GB" --location "Москва" --interval 0.5 --max-ads 3 --priority 1
python -m app.main remove-search 5
python -m app.main list-searches
```

## Планировщик

В проекте есть два планировщика:

### Scheduler (обычный)

[`Scheduler`](app/scheduler/scheduler.py) работает как бесконечный цикл:

- Каждый поисковый запрос имеет свой интервал (по умолчанию 0.5 часа / 30 минут)
- Ошибки не прерывают работу планировщика
- Поиски обрабатываются батчами по 3 параллельно через `asyncio.Semaphore`
- **Пагинация**: обход до 50 страниц поисковой выдачи за один запуск поиска
- **Параллельный сбор карточек**: до 5 вкладок открываются одновременно
- **Изоляция контекста**: каждый поиск работает в отдельном BrowserContext
- **Раздельные rate limiter'ы**: 6 запросов/мин для поиска, 8 для карточек
- **Retry с exponential backoff**: до 3 попыток при ошибках навигации

### ConstantScheduler (24/7)

[`ConstantScheduler`](app/scheduler/scheduler.py) — постоянный режим мониторинга:

- Цикл с фиксированным интервалом (по умолчанию 300 сек)
- Первый цикл запускает все поиски принудительно
- Автоматическая дообработка pending-объявлений после каждого поиска
- Graceful shutdown по SIGINT/SIGTERM

## Система анализа цен

### Нормализация продуктов

Система приводит названия объявлений к единому формату:

- Определение бренда и модели из названия
- Фильтрация аксессуаров и запчастей
- Конфигурация нормализации в [`config/products.json`](config/products.json)
- Реализация в [`product_normalizer.py`](app/analysis/product_normalizer.py)

### Сегментный анализ

Объявления группируются по продукту (бренд + модель) в сегменты:

- `median_7d` — свежая медиана за 7 дней
- `median_30d` — основная медиана за 30 дней
- `median_90d` — справочная медиана за 90 дней
- `listing_price_median` — медиана по активным объявлениям
- `fast_sale_price_median` — медиана по объявлениям, которые быстро исчезли
- `liquid_market_estimate` — оценка реальной ликвидной цены

Если `median_7d > median_30d` → рынок растёт, используются свежие данные.

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

## Парсинг профилей продавцов

Система автоматически собирает данные о проданных товарах со страниц продавцов Avito:

- **Модель `Seller`** — реестр продавцов с рейтингом, отзывами, статистикой продаж
- **Модель `SoldItem`** — проданные товары с ценой, категорией, датой продажи
- **Парсер `seller_parser.py`** — извлечение данных из HTML профиля продавца
- **Интеграция в пайплайн** — автоматический сбор после обработки объявлений

Настройки: `SELLER_PROFILE_ENABLED`, `SELLER_RATE_LIMIT_PER_MINUTE`, `SELLER_MAX_PROFILES_PER_CYCLE`, `SELLER_SCRAPE_INTERVAL_HOURS`.

## Структура проекта

```
app/
├── config/          # Конфигурация (pydantic-settings)
├── collector/       # Сбор данных через Playwright + Chromium
├── parser/          # HTML-парсеры (BeautifulSoup + lxml)
│   ├── ad_parser.py       # Парсинг карточки объявления
│   ├── search_parser.py   # Парсинг поисковой выдачи
│   └── seller_parser.py   # Парсинг профиля продавца
├── storage/         # PostgreSQL через SQLAlchemy (модели, репозиторий)
├── analysis/        # Ценовой анализатор + сегментный анализ + нормализация
├── notifier/        # Telegram и Email уведомления
├── scheduler/       # Пайплайн, планировщик и CLI (Typer)
└── utils/           # Утилиты и исключения
config/              # Конфигурационные файлы (products.json, categories.json)
docs/                # Документация (архитектура, release notes)
scripts/             # Скрипты инициализации и миграций
data/raw_html/       # Сохранённый HTML (search/ и ad/)
```

## Скрипты

| Скрипт | Назначение |
|--------|-----------|
| [`scripts/init_db.py`](scripts/init_db.py) | Инициализация таблиц в БД |
| [`scripts/migrate_segment_analysis.py`](scripts/migrate_segment_analysis.py) | Миграция сегментного анализа |
| [`scripts/migrate_seller_sold_items.py`](scripts/migrate_seller_sold_items.py) | Миграция таблиц продавцов и проданных товаров |
| [`scripts/migrate_products.py`](scripts/migrate_products.py) | Миграция таблицы продуктов |
| [`scripts/migrate_category_monitoring.py`](scripts/migrate_category_monitoring.py) | Миграция категорийного мониторинга |
| [`scripts/seed_searches.py`](scripts/seed_searches.py) | Заполнение поисковых запросов |
| [`scripts/seed_category_searches.py`](scripts/seed_category_searches.py) | Заполнение категорийных поисков |
| [`scripts/db_stats.py`](scripts/db_stats.py) | Статистика базы данных |

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
| `DEFAULT_SCHEDULE_INTERVAL_HOURS` | `0.5` | Интервал запуска поисков (часы) |
| `MIN_DELAY_SECONDS` / `MAX_DELAY_SECONDS` | `3.0` / `8.0` | Диапазон задержек между действиями (сек) |
| `SEARCH_RATE_LIMIT_PER_MINUTE` | `6` | Максимум запросов поиска в минуту |
| `AD_RATE_LIMIT_PER_MINUTE` | `8` | Максимум запросов карточек в минуту |
| `RETRY_MAX_ATTEMPTS` | `3` | Максимум попыток при ошибке загрузки |
| `CONSTANT_CYCLE_INTERVAL_SECONDS` | `300` | Интервал циклов в constant-режиме (сек) |
| `CONSTANT_FORCE_PENDING_AFTER_SEARCH` | `true` | Force-pending после каждого поиска |
| `CONSTANT_BROWSER_HEADLESS` | `true` | Headless-режим в constant-режиме |
| `SELLER_PROFILE_ENABLED` | `true` | Включить парсинг профилей продавцов |
| `SELLER_RATE_LIMIT_PER_MINUTE` | `3` | Rate limit для запросов к профилям |
| `SELLER_MAX_PROFILES_PER_CYCLE` | `5` | Макс. профилей за цикл |
| `SELLER_SCRAPE_INTERVAL_HOURS` | `24` | Интервал повторного парсинга профиля (часы) |
| `LOG_LEVEL` | `INFO` | Уровень логирования |

## Требования

- **Python** 3.11+
- **PostgreSQL** 14+
