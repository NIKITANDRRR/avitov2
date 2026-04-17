# Архитектура Avito Price Monitor

> **Версия:** 3.1
> **Дата:** 2026-04-17

---

## 1. Обзор системы

**Назначение:** автоматический мониторинг цен на Avito, обнаружение недооценённых товаров и отправка уведомлений через Telegram/Email.

**Стек технологий:**

| Компонент | Технология |
|---|---|
| Язык | Python 3.11+ |
| Сбор данных | Playwright + Chromium |
| Парсинг | BeautifulSoup4 + lxml |
| База данных | PostgreSQL (через SQLAlchemy 2.x) |
| CLI | Typer |
| Уведомления | Telethon (Telegram), aiosmtplib (Email) |
| Настройки | pydantic-settings + .env |

---

## 2. Компоненты системы

### 2.1 Точки входа

- [`app/main.py`](app/main.py) — делегирует вызов CLI на базе Typer
- [`app/scheduler/cli.py`](app/scheduler/cli.py) — все CLI-команды

**Команды:**

| Команда | Функция | Описание |
|---|---|---|
| `start` | [`start()`](app/scheduler/cli.py:153) | Полный запуск: init-db + seed + scheduler |
| `run-scheduler` | [`run_scheduler()`](app/scheduler/cli.py:175) | Циклический планировщик |
| `run-once` | [`run_once()`](app/scheduler/cli.py:181) | Один цикл по просроченным поискам |
| `run` | [`run()`](app/scheduler/cli.py:169) | Legacy: один цикл по SEARCH_URLS из .env |
| `add-search` | [`add_search()`](app/scheduler/cli.py:21) | Добавить поисковый запрос |
| `list-searches` | [`list_searches()`](app/scheduler/cli.py:103) | Список всех поисков |
| `remove-search` | [`remove_search()`](app/scheduler/cli.py:69) | Удалить поиск |
| `init-db` | [`init_db()`](app/scheduler/cli.py:191) | Создать таблицы в PostgreSQL |
| `test-telegram` | [`test_telegram()`](app/scheduler/cli.py:204) | Проверить подключение к Telegram |

### 2.2 Планировщик (Scheduler)

- [`app/scheduler/scheduler.py`](app/scheduler/scheduler.py) — [`Scheduler`](app/scheduler/scheduler.py:14): бесконечный цикл с интервалом 3000 сек (50 мин)
- [`app/scheduler/pipeline.py`](app/scheduler/pipeline.py) — [`Pipeline`](app/scheduler/pipeline.py:60): основная логика обработки поисков
- [`app/scheduler/cli.py`](app/scheduler/cli.py) — CLI-интерфейс (Typer)

**Ключевые параметры:**

| Параметр | Значение | Источник |
|---|---|---|
| Интервал проверки | 50 мин | Хардкод в [`scheduler.py`](app/scheduler/scheduler.py:53) |
| Интервал запуска поиска | 0.5 ч (по умолч.) | `schedule_interval_hours` в БД (Float) |
| Параллельность | 3 поиска одновременно | `MAX_CONCURRENT_SEARCHES` |
| Параллельные карточки | 5 вкладок одновременно | `MAX_CONCURRENT_AD_PAGES` |
| Пагинация поиска | до 50 страниц | `MAX_SEARCH_PAGES_PER_RUN` |
| Задержка между батчами | 30 сек | `BATCH_DELAY_SECONDS` |
| Задержки между действиями | 3–8 сек | `MIN_DELAY_SECONDS` / `MAX_DELAY_SECONDS` |
| Rate limiter поиска | 6 запросов/мин | `SEARCH_RATE_LIMIT_PER_MINUTE` |
| Rate limiter карточек | 8 запросов/мин | `AD_RATE_LIMIT_PER_MINUTE` |
| Retry при ошибках | до 3 попыток | `RETRY_MAX_ATTEMPTS` |
| Изоляция контекста | отдельный контекст на поиск | `USE_ISOLATED_CONTEXTS` |

### 2.3 Сборщик (Collector)

- [`app/collector/browser.py`](app/collector/browser.py) — [`BrowserManager`](app/collector/browser.py): Playwright, загрузка страниц через Chromium
- [`app/collector/collector.py`](app/collector/collector.py) — [`AvitoCollector`](app/collector/collector.py): координация сбора

**Методы:**
- [`collect_search_page()`](app/collector/collector.py:46) — загружает поисковую выдачу, имитирует скролл, сохраняет HTML в `data/raw_html/search/`
- [`collect_ad_page()`](app/collector/collector.py:133) — загружает карточку объявления, сохраняет HTML в `data/raw_html/ad/`

### 2.4 Парсер (Parser)

- [`app/parser/search_parser.py`](app/parser/search_parser.py) — [`parse_search_page()`](app/parser/search_parser.py:38): парсинг списка объявлений, три стратегии селекторов с fallback
- [`app/parser/ad_parser.py`](app/parser/ad_parser.py) — [`parse_ad_page()`](app/parser/ad_parser.py:47): парсинг детальной страницы объявления

**Извлекаемые данные:**
- Поиск: `ad_id`, `url`, `title`, `price_str`, `location`
- Карточка: `title`, `price`, `location`, `seller_name`, `condition`, `publication_date`, `description`

### 2.5 Анализатор (Analysis)

- [`app/analysis/analyzer.py`](app/analysis/analyzer.py) — [`PriceAnalyzer`](app/analysis/analyzer.py:149): составной критерий недооценённости v2
- [`app/analysis/segment_analyzer.py`](app/analysis/segment_analyzer.py) — [`SegmentAnalyzer`](app/analysis/segment_analyzer.py): сегментный анализ для категорийных поисков
- [`app/analysis/attribute_extractor.py`](app/analysis/attribute_extractor.py) — извлечение бренда/модели из названия
- [`app/analysis/accessory_filter.py`](app/analysis/accessory_filter.py) — [`AccessoryFilter`](app/analysis/accessory_filter.py): фильтрация аксессуаров и мелочёвки

### 2.6 Хранилище (Storage)

- [`app/storage/models.py`](app/storage/models.py) — SQLAlchemy-модели (TrackedSearch, SearchRun, Ad, AdSnapshot, NotificationSent, SegmentStats, SegmentPriceHistory)
- [`app/storage/repository.py`](app/storage/repository.py) — CRUD-операции с БД
- [`app/storage/database.py`](app/storage/database.py) — подключение к PostgreSQL, `engine`, `Session`, `Base`

### 2.7 Уведомления (Notifier)

- [`app/notifier/telegram_notifier.py`](app/notifier/telegram_notifier.py) — [`TelegramNotifier`](app/notifier/telegram_notifier.py:40): Telethon + MTProto-прокси
- [`app/notifier/email_notifier.py`](app/notifier/email_notifier.py) — [`EmailNotifier`](app/notifier/email_notifier.py): SMTP (fallback-канал)

---

## 3. Модели данных

### 3.1 TrackedSearch

Поисковый запрос: [`TrackedSearch`](app/storage/models.py:33)

| Поле | Тип | Описание |
|---|---|---|
| `search_url` | String(2048) | URL поисковой выдачи Avito (unique) |
| `search_phrase` | String(512) | Человекочитаемое название |
| `is_active` | Boolean | Флаг активности |
| `schedule_interval_hours` | Float | Интервал запуска (по умолч. 0.5 ч., может быть дробным) |
| `last_run_at` | DateTime | Время последнего запуска |
| `priority` | Integer | Приоритет (1–10, ниже = важнее) |
| `max_ads_to_parse` | Integer | Карточек за запуск (по умолч. 3) |
| `category` | String(256) | Категория поиска |
| `is_category_search` | Boolean | Флаг категорийного поиска |

**Связи:** `runs` (→ SearchRun), `segment_stats` (→ SegmentStats)

### 3.2 Ad

Объявление: [`Ad`](app/storage/models.py:152)

| Поле | Тип | Описание |
|---|---|---|
| `ad_id` | String(64) | Идентификатор Avito (unique) |
| `url` | String(2048) | URL объявления |
| `title` | String(512) | Заголовок |
| `price` | Float | Цена |
| `location` | String(256) | Местоположение |
| `seller_name` / `seller_type` | String | Продавец |
| `condition` | String(128) | Состояние |
| `first_seen_at` | DateTime | Первое обнаружение |
| `last_seen_at` | DateTime | Последнее обнаружение |
| `days_on_market` | Integer | Дней на рынке |
| `is_disappeared_quickly` | Boolean | Быстро исчезло (≤ 3 дней) |
| `is_undervalued` | Boolean | Недооценённое |
| `undervalue_score` | Float | Отклонение от медианы |
| `z_score` | Float | Z-score относительно сегмента |
| `iqr_outlier` | Boolean | Выброс по IQR |
| `segment_key` | String(512) | Ключ сегмента |
| `ad_category` | String(256) | Категория |
| `brand` / `extracted_model` | String(256) | Бренд и модель |

**Связи:** `snapshots` (→ AdSnapshot), `notifications` (→ NotificationSent)

### 3.3 SegmentStats

Статистика сегмента: [`SegmentStats`](app/storage/models.py:341)

| Поле | Тип | Описание |
|---|---|---|
| `search_id` | Integer (FK) | Связь с TrackedSearch |
| `segment_key` | String | Ключ сегмента (brand:model) |
| `median_7d` / `median_30d` / `median_90d` | Float | Медианы за периоды |
| `price_trend_slope` | Float | Наклон тренда цены |
| `sample_size` / `listing_count` | Integer | Размер выборки / активных |
| `appearance_count_90d` | Integer | Появлений за 90 дней |
| `median_days_on_market` | Float | Медиана дней на рынке |
| `listing_price_median` | Float | Медиана по активным объявлениям |
| `fast_sale_price_median` | Float | Медиана цен быстрых продаж |
| `liquid_market_estimate` | Float | Оценка ликвидной цены |
| `is_rare_segment` | Boolean | Признак редкого сегмента |

**Связи:** `search` (→ TrackedSearch), `price_history` (→ SegmentPriceHistory)

### 3.4 SegmentPriceHistory

История цен сегмента по дням: [`SegmentPriceHistory`](app/storage/models.py:438)

| Поле | Тип | Описание |
|---|---|---|
| `segment_stats_id` | Integer (FK) | Связь с SegmentStats |
| `snapshot_date` | Date | Дата снапшота |
| `median_price` / `mean_price` / `min_price` / `max_price` | Float | Цены на дату |
| `sample_size` / `listing_count` / `fast_sale_count` | Integer | Объёмы |
| `median_days_on_market` | Float | Медиана дней на рынке |

Уникальный индекс: один снапшот в день на сегмент.

### 3.5 AdSnapshot и NotificationSent

- [`AdSnapshot`](app/storage/models.py:254) — снимок цены: `ad_id`, `price`, `scraped_at`, `html_path`
- [`NotificationSent`](app/storage/models.py:294) — отправленное уведомление: `ad_id`, `notification_type`, `sent_at`, `telegram_message_id`

### 3.6 SearchRun

- [`SearchRun`](app/storage/models.py:98) — запись о запуске: `tracked_search_id`, `started_at`, `completed_at`, `status`, `ads_found`, `ads_new`, `errors_count`

---

## 4. Поток данных

```
Avito.ru
  │
  ▼
Collector (Playwright/Chromium)
  ├── collect_search_page() → HTML поисковой страницы
  └── collect_ad_page()     → HTML карточки объявления
  │                           Сохраняется в data/raw_html/
  ▼
Parser (BeautifulSoup + lxml)
  ├── parse_search_page() → [SearchResultItem]
  └── parse_ad_page()     → AdData (title, price, location, seller, ...)
  │
  ▼
PostgreSQL (SQLAlchemy)
  ├── tracked_searches, search_runs
  ├── ads, ad_snapshots, notifications_sent
  ├── segment_stats, segment_price_history
  │
  ▼
Analysis
  ├── AccessoryFilter — фильтрация аксессуаров
  ├── PriceAnalyzer   — стандартный анализ (сегментация + составной критерий v2)
  └── SegmentAnalyzer — сегментный анализ (категорийные поиски) + детекция «бриллиантов»
  │
  ▼
Notifier
  ├── TelegramNotifier (Telethon + MTProto) — основной канал
  └── EmailNotifier (SMTP) — fallback
```

---

## 5. Алгоритмы анализа

### 5.1 Составной критерий недооценённости (v2)

Реализован в [`PriceAnalyzer`](app/analysis/analyzer.py:149).

**Сегментация:** группировка по ключу `{condition}_{location}_{seller_type}`, объединение мелких сегментов (< 3 объявлений).

**Фильтрация выбросов:** trim-percent (5%) + IQR.

**Составной score:**

```
score = 0.4 × IQR_компонент + 0.3 × Z-score_компонент + 0.3 × процент_от_медианы
```

- **IQR-компонент** (вес 0.4): цена ниже `lower_fence = Q1 - 1.5 × IQR`
- **Z-score компонент** (вес 0.3): z < −1.5
- **Процент от медианы** (вес 0.3): цена < медиана × 0.85

**Условие недооценённости:** `score ≥ 0.3` И цена < 85% медианы.

### 5.2 Сегментный анализ

Реализован в [`SegmentAnalyzer`](app/analysis/segment_analyzer.py).

- Группировка по `brand:model` (извлечение через [`attribute_extractor.py`](app/analysis/attribute_extractor.py))
- Медианы за 7 / 30 / 90 дней
- **Правило роста рынка:** если `median_7d > median_30d` → использовать `median_7d` (с весом 1.5)
- Расчёт тренда: `price_trend_slope` за окно 30 дней
- Сохранение снапшотов в `segment_price_history` (раз в 7 дней)

### 5.3 Детекция «бриллиантов»

Редкий сегмент + цена значительно ниже ликвидной оценки.

**Пороги:**
- 70% от `listing_price_median`
- 80% от `fast_sale_price_median`
- 85% от лучшей медианы (`best_median`)

Редкость определяется по `is_rare_segment` (< 5 объявлений) с премией за ликвидность × 1.2.

### 5.4 Оборачиваемость

Трекинг жизненного цикла объявлений:

- `first_seen_at` → `last_seen_at` → `days_on_market`
- [`_detect_disappeared_ads()`](app/scheduler/pipeline.py:893): сравнение текущих ad_id с известными в БД
- `is_disappeared_quickly` = True, если `days_on_market ≤ segment_fast_sale_days` (3 дня)
- Быстрое исчезновение дешёвых объявлений = сигнал, что цена ниже рыночной

---

## 6. Конфигурация

Все настройки загружаются через [`app/config/settings.py`](app/config/settings.py) (pydantic-settings) из переменных окружения `.env`.

### Ключевые параметры

| Категория | Переменная | По умолч. | Описание |
|---|---|---|---|
| **БД** | `DATABASE_URL` | `postgresql://avito:avito@localhost:5432/avito_monitor` | URL PostgreSQL |
| **Сбор** | `MAX_ADS_PER_SEARCH_PER_RUN` | `3` | Карточек за поиск за запуск |
| **Задержки** | `MIN_DELAY_SECONDS` / `MAX_DELAY_SECONDS` | `3.0` / `8.0` | Антибан-задержки (сек) |
| **Браузер** | `HEADLESS` | `false` | Headless-режим |
| **Rate limit** | `SEARCH_RATE_LIMIT_PER_MINUTE` | `6` | Максимум запросов поиска в минуту |
| **Rate limit** | `AD_RATE_LIMIT_PER_MINUTE` | `8` | Максимум запросов карточек в минуту |
| **Retry** | `RETRY_MAX_ATTEMPTS` | `3` | Максимум попыток при ошибке загрузки |
| **Retry** | `RETRY_BACKOFF_BASE` | `5.0` | Базовая задержка exponential backoff (сек) |
| **Контекст** | `USE_ISOLATED_CONTEXTS` | `true` | Отдельный контекст браузера на каждый поиск |
| **Интервал** | `DEFAULT_SCHEDULE_INTERVAL_HOURS` | `0.5` | Интервал запуска поисков (часы, float) |
| **Анализ** | `TEMPORAL_WINDOW_DAYS` | `14` | Окно анализа (дни) |
| **Анализ** | `UNDERVALUED_THRESHOLD` | `0.3` | Порог composite score |
| **Анализ** | `MEDIAN_DISCOUNT_THRESHOLD` | `0.85` | Порог % от медианы |
| **Анализ** | `ZSCORE_THRESHOLD` | `1.5` | Порог z-score |
| **Сегменты** | `segment_rare_threshold` | `5` | Мин. объявлений для не-редкого сегмента |
| **Сегменты** | `segment_fast_sale_days` | `3` | Дней для быстрой продажи |
| **Telegram** | `TELEGRAM_BOT_TOKEN` | `""` | Токен бота |
| **Email** | `SMTP_HOST` | `smtp.gmail.com` | SMTP-сервер |

Параметры каждого поискового запроса хранятся **в БД** (таблица `tracked_searches`): `schedule_interval_hours`, `max_ads_to_parse`, `priority`, `is_active`.

---

## 7. Запуск и эксплуатация

### Установка

```bash
pip install -r requirements.txt
playwright install chromium
```

### Инициализация

```bash
python -m app.main init-db        # Создание таблиц
python scripts/seed_searches.py   # Заполнение поисковыми запросами
```

### Запуск

```bash
# Полный запуск (рекомендуемый)
python -m app.main start

# Только планировщик (если БД уже инициализирована)
python -m app.main run-scheduler

# Однократный запуск
python -m app.main run-once
```

### Управление поисками

```bash
python -m app.main add-search "iPhone 15 Pro 128GB" --location "Москва" --interval 0.5
python -m app.main list-searches
python -m app.main remove-search 5
```

### Мониторинг

Логирование через стандартный `logging` (уровень настраивается через `LOG_LEVEL` в .env). HTML-файлы сохраняются в `data/raw_html/`.

### Зависимости

| Пакет | Версия | Назначение |
|---|---|---|
| `playwright` | 1.51.0 | Браузерная автоматизация |
| `beautifulsoup4` | 4.13.3 | Парсинг HTML |
| `lxml` | 5.3.0 | Быстрый HTML-парсер |
| `sqlalchemy` | 2.0.40 | ORM для PostgreSQL |
| `psycopg2-binary` | 2.9.9 | Драйвер PostgreSQL |
| `alembic` | 1.15.2 | Миграции БД |
| `pydantic` / `pydantic-settings` | 2.11.3 / 2.8.1 | Валидация настроек |
| `telethon` | ≥1.34.0 | Telegram-клиент (MTProto) |
| `typer` | 0.15.2 | CLI-интерфейс |
| `numpy` | 2.2.4 | Математические вычисления |
| `aiosmtplib` | ≥3.0.0 | Асинхронная отправка email |
