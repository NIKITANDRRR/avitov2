# Анализ проекта Avito Monitor

> Дата анализа: 2026-04-14

---

## Содержание

1. [Пошаговое описание работы проекта](#1-пошаговое-описание-работы-проекта)
2. [Планировщик](#2-планировщик)
3. [Команды запуска](#3-команды-запуска)
4. [Все точки входа](#4-все-точки-входа)
5. [Конфигурация](#5-конфигурация)
6. [Поток данных](#6-поток-данных)

---

## 1. Пошаговое описание работы проекта

### Общая схема

```
Запуск CLI → Scheduler (цикл каждые 50 мин) → Pipeline → Collector → Parser → Analyzer → Notifier
```

### Детальный алгоритм (масштабированный режим — основной)

#### Шаг 1. Запуск

Пользователь выполняет команду `python -m app.main run-scheduler` (или `start`). Точка входа — [`app/main.py`](app/main.py:4), которая делегирует вызов CLI на базе **Typer**.

#### Шаг 2. Инициализация планировщика

[`Scheduler.run()`](app/scheduler/scheduler.py:30) запускает бесконечный цикл:
- Вызывает [`Pipeline.run_search_cycle()`](app/scheduler/pipeline.py:166)
- Спит **3000 секунд (50 минут)** до следующей проверки
- Исключения в одном цикле **не прерывают** работу планировщика

#### Шаг 3. Определение просроченных поисков

[`Pipeline.run_search_cycle()`](app/scheduler/pipeline.py:166):
1. Автосоздаёт таблицы в БД через [`ensure_tables()`](app/storage/database.py:47)
2. Получает поиски, которые пора запускать: `repo.get_searches_due_for_run()` — выбирает активные поиски, у которых `last_run_at` старше чем `schedule_interval_hours` (по умолчанию 2 часа)
3. Если просроченных поисков нет — цикл завершается

#### Шаг 4. Запуск браузера

Создаётся [`BrowserManager`](app/collector/browser.py) (Playwright) с настройками:
- `headless` / видимый режим
- опциональный прокси
- Запускается Chromium через `playwright.chromium.launch()`

#### Шаг 5. Обработка батчами

Поиски разбиваются на батчи по `MAX_CONCURRENT_SEARCHES` (по умолчанию 3). Каждый батч обрабатывается **параллельно** через `asyncio.Semaphore` + `asyncio.gather`. Между батчами — задержка `BATCH_DELAY_SECONDS` (30 сек).

#### Шаг 6. Обработка одного поиска ([`_process_search()`](app/scheduler/pipeline.py:351))

Для каждого поискового URL:

1. **Регистрация запуска** — создаётся запись `SearchRun` в БД
2. **Случайная задержка** — `MIN_DELAY_SECONDS` (5 сек) … `MAX_DELAY_SECONDS` (15 сек)
3. **Сбор поисковой страницы** — [`AvitoCollector.collect_search_page()`](app/collector/collector.py:46):
   - Открывает страницу через Playwright
   - Ждёт появления селекторов объявлений
   - Имитирует скролл
   - Сохраняет HTML в `data/raw_html/search/`
4. **Парсинг поисковой страницы** — [`parse_search_page()`](app/parser/search_parser.py:38):
   - BeautifulSoup + lxml
   - Извлекает список `SearchResultItem` (ad_id, url, title, price_str, location)
   - Три стратегии селекторов с fallback
5. **Ранняя фильтрация аксессуаров** — [`_early_filter_search_items()`](app/scheduler/pipeline.py:1183):
   - Проверка по чёрному списку слов в названии
   - Проверка по минимальной цене (5000₽)
6. **Фильтрация уже известных** — исключает объявления, уже собранные ранее
7. **Лимит новых** — берёт до `max_ads_to_parse` (по умолчанию 3) новых объявлений

#### Шаг 7. Обработка карточки объявления ([`_process_ad()`](app/scheduler/pipeline.py:511))

Для каждого нового объявления:

1. **Случайная задержка** (5–15 сек)
2. **Сбор карточки** — [`AvitoCollector.collect_ad_page()`](app/collector/collector.py:133):
   - Открывает страницу объявления
   - Ждёт загрузки, имитирует скролл
   - Сохраняет HTML в `data/raw_html/ad/`
3. **Парсинг карточки** — [`parse_ad_page()`](app/parser/ad_parser.py:47):
   - Извлекает: title, price, location, seller_name, condition, publication_date, description
   - Нормализует цену (убирает символы, приводит к числу)
   - Парсит дату публикации (относительные форматы: «сегодня», «вчера», «N минут назад»)
4. **Обновление БД** — `repo.update_ad()` с распарсенными данными
5. **Трекинг оборачиваемости** — обновляет `last_seen_at`, `days_on_market`
6. **Создание снимка цены** — `repo.create_snapshot()` для отслеживания изменения цен

#### Шаг 8. Обнаружение исчезнувших объявлений

[`_detect_disappeared_ads()`](app/scheduler/pipeline.py:893):
- Сравнивает текущие ad_id с известными в БД
- Помечает исчезнувшие как `is_disappeared_quickly` (если были на рынке < `segment_fast_sale_days` дней)

#### Шаг 9. Анализ цен и уведомления ([`_analyze_and_notify_searches()`](app/scheduler/pipeline.py:711))

Для каждого поиска:

1. **Получение объявлений** за `TEMPORAL_WINDOW_DAYS` (14 дней)
2. **Фильтрация аксессуаров** через [`AccessoryFilter`](app/analysis/accessory_filter.py)
3. **Два пути анализа**:
   - **Категорийный поиск** (`is_category_search=True`) → сегментный анализ через [`SegmentAnalyzer`](app/analysis/segment_analyzer.py) с детекцией «бриллиантов»
   - **Стандартный поиск** → анализ через [`PriceAnalyzer.analyze_ad()`](app/analysis/analyzer.py:699)

**Стандартный анализ** ([`PriceAnalyzer`](app/analysis/analyzer.py:149)):
- Временной фильтр (14 дней)
- Сегментация по ключу `{condition}_{location}_{seller_type}`
- Объединение мелких сегментов (< 3 объявлений)
- Расчёт статистики: медиана, среднее, Q1, Q3, IQR, std_dev, trimmed_mean
- Фильтрация выбросов: trim-percent (5%) + IQR
- Составной критерий недооценённости (v2):
  - **IQR-компонент** (вес 0.4): цена ниже lower_fence
  - **Z-score компонент** (вес 0.3): z < -1.5
  - **Процент от медианы** (вес 0.3): цена < медиана × 0.85
  - Итог: `score >= 0.3` И цена ниже порога → **undervalued**

4. **Обновление аналитических полей** в БД (z_score, iqr_outlier, segment_key, is_undervalued)

#### Шаг 10. Отправка уведомлений ([`_send_notifications()`](app/scheduler/pipeline.py:1264))

1. **Telegram** (основной канал) — [`TelegramNotifier`](app/notifier/telegram_notifier.py:40):
   - Telethon с поддержкой MTProto-прокси
   - Проверка: было ли уже отправлено уведомление для данного объявления
   - HTML-формат сообщения: название, цена, медиана, отклонение %, ссылка, локация
   - Случайная задержка 1–3 сек между сообщениями
2. **Email** (fallback) — если Telegram не сработал, пробует отправить через SMTP

#### Шаг 11. Фиксация транзакции

`repo.commit()` — все изменения в БД (новые объявления, снимки цен, аналитика, уведомления) фиксируются одной транзакцией.

---

## 2. Планировщик

### Есть ли планировщик?

**Да.** Планировщик реализован в [`Scheduler`](app/scheduler/scheduler.py:14).

### Как работает

```
Scheduler.run()
  │
  ├── while self._running:
  │     ├── Pipeline.run_search_cycle()  ← обработка всех просроченных поисков
  │     └── asyncio.sleep(3000)          ← 50 минут до следующей проверки
  │
  └── scheduler_stopped
```

### Ключевые характеристики

| Параметр | Значение | Источник |
|---|---|---|
| Интервал проверки | **3000 сек (50 мин)** | Хардкод в [`scheduler.py:53`](app/scheduler/scheduler.py:53) |
| Интервал запуска поиска | **2 часа** (по умолчанию) | `schedule_interval_hours` в БД, `DEFAULT_SCHEDULE_INTERVAL_HOURS` в настройках |
| Параллельность | **3 поиска** одновременно | `MAX_CONCURRENT_SEARCHES` |
| Задержка между батчами | **30 сек** | `BATCH_DELAY_SECONDS` |
| Задержка между поисками | **5 сек** | `SEARCH_DELAY_SECONDS` |
| Обработка ошибок | Исключения логируются, не прерывают цикл | `try/except` в цикле |

### Как определяются просроченные поиски

Метод `repo.get_searches_due_for_run()` выбирает из таблицы `tracked_searches` записи, где:
- `is_active = True`
- `last_run_at IS NULL` (никогда не запускался) **ИЛИ** `last_run_at + schedule_interval_hours <= NOW()`

### Остановка планировщика

Вызов `scheduler.stop()` устанавливает `self._running = False`. Текущий цикл завершается, после чего планировщик выходит из цикла.

---

## 3. Команды запуска

### Основные команды для постоянной работы

#### Вариант 1: Полный запуск (рекомендуемый)

```bash
python -m app.main start
```

Выполняет последовательно:
1. Создание таблиц в БД (`init-db`)
2. Заполнение 14 поисковыми запросами (seed)
3. Запуск планировщика

#### Вариант 2: Только планировщик (если БД уже инициализирована)

```bash
python -m app.main run-scheduler
```

Запускает циклический планировщик без инициализации.

#### Вариант 3: Однократный запуск

```bash
python -m app.main run-once
```

Обрабатывает все просроченные поиски один раз и завершается.

### Управление поисками

```bash
# Добавить поисковый запрос
python -m app.main add-search "iPhone 15 Pro 128GB" --location "Москва" --interval 2 --max-ads 3 --priority 1

# Удалить поиск
python -m app.main remove-search 5

# Список всех поисков
python -m app.main list-searches
```

### Служебные команды

```bash
# Инициализация БД (создание таблиц)
python -m app.main init-db

# Проверка подключения к Telegram
python -m app.main test-telegram

# Legacy: один цикл по SEARCH_URLS из .env
python -m app.main run
```

### Запуск через скрипты

```bash
# Инициализация БД с миграциями
python scripts/init_db.py

# Заполнение 14 поисковыми запросами
python scripts/seed_searches.py
```

### Запуск на постоянную работу (production)

```bash
# Способ 1: через CLI (рекомендуемый)
python -m app.main start

# Способ 2: через nohup (Linux)
nohup python -m app.main run-scheduler &

# Способ 3: через screen/tmux
screen -S avito-monitor
python -m app.main run-scheduler
# Ctrl+A, D — отключиться от screen
```

---

## 4. Все точки входа

### CLI команды (Typer)

| Команда | Функция | Описание |
|---|---|---|
| `start` | [`start()`](app/scheduler/cli.py:153) | Полный запуск: init-db + seed + scheduler |
| `run` | [`run()`](app/scheduler/cli.py:169) | Один цикл по SEARCH_URLS из .env (legacy) |
| `run-scheduler` | [`run_scheduler()`](app/scheduler/cli.py:175) | Циклический планировщик |
| `run-once` | [`run_once()`](app/scheduler/cli.py:181) | Один цикл по просроченным поискам из БД |
| `add-search` | [`add_search()`](app/scheduler/cli.py:21) | Добавить поисковый запрос |
| `remove-search` | [`remove_search()`](app/scheduler/cli.py:69) | Удалить поисковый запрос |
| `list-searches` | [`list_searches()`](app/scheduler/cli.py:103) | Показать список поисков |
| `init-db` | [`init_db()`](app/scheduler/cli.py:191) | Создать таблицы в PostgreSQL |
| `test-telegram` | [`test_telegram()`](app/scheduler/cli.py:204) | Проверить подключение к Telegram |

### Автономные скрипты

| Скрипт | Описание |
|---|---|
| [`scripts/init_db.py`](scripts/init_db.py) | Создание таблиц + миграция новых колонок |
| [`scripts/seed_searches.py`](scripts/seed_searches.py) | Заполнение 14 поисковыми запросами |
| [`scripts/seed_category_searches.py`](scripts/seed_category_searches.py) | Заполнение категорийных поисков |
| [`scripts/migrate_category_monitoring.py`](scripts/migrate_category_monitoring.py) | Миграция для категорийного мониторинга |
| [`scripts/migrate_segment_analysis.py`](scripts/migrate_segment_analysis.py) | Миграция для сегментного анализа |
| [`scripts/cleanup_duplicates.py`](scripts/cleanup_duplicates.py) | Очистка дубликатов |

### Внутренние точки входа

| Модуль | Метод | Описание |
|---|---|---|
| [`Pipeline.run()`](app/scheduler/pipeline.py:60) | Legacy-режим | Один цикл по SEARCH_URLS из конфига |
| [`Pipeline.run_search_cycle()`](app/scheduler/pipeline.py:166) | Основной режим | Обработка просроченных поисков из БД |
| [`Scheduler.run()`](app/scheduler/scheduler.py:30) | Планировщик | Бесконечный цикл с вызовом Pipeline |

---

## 5. Конфигурация

### Переменные окружения (.env)

#### Поиск и сбор

| Переменная | По умолчанию | Описание |
|---|---|---|
| `SEARCH_URLS` | `[]` | URL поиска Avito (через запятую, до 3). Только для legacy-режима |
| `MAX_SEARCH_PAGES_PER_RUN` | `3` | Максимум страниц за запуск (1–10) |
| `MAX_ADS_PER_SEARCH_PER_RUN` | `3` | Максимум объявлений за поиск за запуск (1–10) |

#### Задержки (антибан)

| Переменная | По умолчанию | Описание |
|---|---|---|
| `MIN_DELAY_SECONDS` | `5.0` | Минимальная задержка между запросами (сек) |
| `MAX_DELAY_SECONDS` | `15.0` | Максимальная задержка между запросами (сек) |
| `STARTUP_DELAY_MIN` | `0.0` | Минимальная задержка при старте (сек) |
| `STARTUP_DELAY_MAX` | `30.0` | Максимальная задержка при старте (сек) |

#### Браузер

| Переменная | По умолчанию | Описание |
|---|---|---|
| `HEADLESS` | `false` | Запуск браузера в headless-режиме |
| `USE_PROXY` | `false` | Использовать прокси |
| `PROXY_URL` | `None` | URL прокси-сервера |

#### Telegram

| Переменная | По умолчанию | Описание |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | `""` | Токен Telegram-бота |
| `TELEGRAM_CHAT_ID` | `""` | ID чата Telegram |
| `TELEGRAM_API_ID` | `0` | API ID с my.telegram.org |
| `TELEGRAM_API_HASH` | `""` | API hash с my.telegram.org |

#### MTProto Proxy

| Переменная | По умолчанию | Описание |
|---|---|---|
| `MTPROXY_ENABLED` | `false` | Использовать MTProto-прокси |
| `MTPROXY_ADDRESS` | `""` | Адрес MTProto-прокси |
| `MTPROXY_PORT` | `0` | Порт MTProto-прокси |
| `MTPROXY_SECRET` | `""` | Секрет MTProto-прокси |

#### Email (fallback-уведомления)

| Переменная | По умолчанию | Описание |
|---|---|---|
| `SMTP_HOST` | `smtp.gmail.com` | SMTP-сервер |
| `SMTP_PORT` | `587` | Порт SMTP |
| `SMTP_USER` | `""` | Логин SMTP |
| `SMTP_PASSWORD` | `""` | Пароль SMTP |
| `SMTP_USE_TLS` | `true` | Использовать TLS |
| `EMAIL_FROM` | `""` | Адрес отправителя |
| `EMAIL_TO` | `[]` | Список получателей (через запятую) |

#### База данных

| Переменная | По умолчанию | Описание |
|---|---|---|
| `DATABASE_URL` | `postgresql://avito:avito@localhost:5432/avito_monitor` | URL подключения к PostgreSQL |

#### Анализ цен

| Переменная | По умолчанию | Описание |
|---|---|---|
| `UNDERVALUE_THRESHOLD` | `0.8` | Порог недооценённости v1 (0; 1) |
| `TRIM_PERCENT` | `0.05` | Доля отбрасываемых выбросов с каждого края |
| `IQR_MULTIPLIER` | `1.5` | Множитель для IQR fences |
| `TEMPORAL_WINDOW_DAYS` | `14` | Окно анализа в днях |
| `MIN_SEGMENT_SIZE` | `3` | Минимальный размер сегмента |
| `UNDERVALUED_THRESHOLD` | `0.3` | Порог composite score для недооценённости |
| `ZSCORE_THRESHOLD` | `1.5` | Порог z-score для аномалий |
| `MEDIAN_DISCOUNT_THRESHOLD` | `0.85` | Порог % от медианы |

#### Фильтрация аксессуаров

| Переменная | По умолчанию | Описание |
|---|---|---|
| `ENABLE_ACCESSORY_FILTER` | `true` | Включить фильтрацию |
| `MIN_PRICE_FILTER` | `5000` | Минимальная цена (руб.) |
| `ACCESSORY_BLACKLIST` | `[чехол, case, ...]` | Чёрный список слов |
| `ACCESSORY_PRICE_RATIO_THRESHOLD` | `0.3` | Порог отношения цены к медиане |

#### Масштабирование поиска

| Переменная | По умолчанию | Описание |
|---|---|---|
| `MAX_CONCURRENT_SEARCHES` | `3` | Макс. параллельных поисков в батче (1–20) |
| `DEFAULT_SCHEDULE_INTERVAL_HOURS` | `2` | Интервал запуска по умолчанию (часы, 1–48) |
| `DEFAULT_MAX_ADS_TO_PARSE` | `3` | Карточек на поиск за запуск (1–50) |
| `BATCH_DELAY_SECONDS` | `30` | Задержка между батчами (сек) |
| `SEARCH_DELAY_SECONDS` | `5` | Задержка между поисками в батче (сек) |

#### Сегментный анализ

| Переменная | По умолчанию | Описание |
|---|---|---|
| `segment_rare_threshold` | `5` | Мин. объявлений, чтобы сегмент не считался редким |
| `segment_fast_sale_days` | `3` | Дней для определения быстрой продажи |
| `segment_7d_weight` | `1.5` | Вес 7-дневной медианы при росте рынка |
| `segment_trend_window_days` | `30` | Окно расчёта тренда цены |
| `segment_history_snapshot_days` | `7` | Периодичность сохранения снапшотов |
| `segment_min_samples_for_stats` | `3` | Минимум объявлений для статистики |
| `segment_liquidity_premium` | `1.2` | Премия за ликвидность для редких товаров |
| `segment_price_outlier_percentile` | `0.05` | Процентиль отсечения выбросов |

#### Прочее

| Переменная | По умолчанию | Описание |
|---|---|---|
| `RAW_HTML_PATH` | `data/raw_html` | Путь к каталогу с HTML-файлами |
| `LOG_LEVEL` | `INFO` | Уровень логирования |

### Настройки из Settings (не в .env)

Параметры каждого поискового запроса хранятся **в БД** (таблица `tracked_searches`):
- `schedule_interval_hours` — интервал запуска (по умолчанию 2 ч.)
- `max_ads_to_parse` — карточек за запуск (по умолчанию 3)
- `priority` — приоритет (1–10, ниже = важнее)
- `is_active` — активен ли поиск

---

## 6. Поток данных

### Схема потока данных

```
Avito.ru
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ Collector (Playwright)                                          │
│   ├── collect_search_page() → HTML поисковой страницы           │
│   └── collect_ad_page()     → HTML карточки объявления          │
│   Сохраняет HTML в: data/raw_html/search/ и data/raw_html/ad/   │
└───────────────────────────┬─────────────────────────────────────┘
                            │ HTML
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Parser (BeautifulSoup + lxml)                                   │
│   ├── parse_search_page() → [SearchResultItem]                  │
│   │   ad_id, url, title, price_str, location, metadata          │
│   └── parse_ad_page()      → AdData                             │
│       ad_id, url, title, price, location, seller_name,          │
│       condition, publication_date, description                   │
└───────────────────────────┬─────────────────────────────────────┘
                            │ Structured Data
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ PostgreSQL Database                                             │
│   ├── tracked_searches — поисковые запросы                      │
│   ├── search_runs      — история запусков                       │
│   ├── ads              — объявления (ad_id, title, price, ...)  │
│   ├── ad_snapshots     — снимки цен (история изменения)         │
│   └── notifications_sent — отправленные уведомления             │
└───────────────────────────┬─────────────────────────────────────┘
                            │ Ad objects
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Analyzer                                                        │
│   ├── AccessoryFilter — фильтрация аксессуаров и мелочёвки      │
│   ├── PriceAnalyzer   — ценовой анализ:                         │
│   │   ├── Сегментация: {condition}_{location}_{seller_type}     │
│   │   ├── Статистика: медиана, IQR, z-score, trimmed_mean      │
│   │   └── Критерий v2: score = 0.4*IQR + 0.3*Z + 0.3*%        │
│   └── SegmentAnalyzer — сегментный анализ (категорийные поиски) │
│       └── Детекция «бриллиантов»                                │
└───────────────────────────┬─────────────────────────────────────┘
                            │ Undervalued Ads
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Notifier                                                        │
│   ├── TelegramNotifier (Telethon + MTProto proxy) — основной    │
│   └── EmailNotifier (SMTP) — fallback                           │
│                                                                  │
│   Проверка: было ли уже отправлено уведомление для данного Ad   │
└─────────────────────────────────────────────────────────────────┘
```

### Откуда данные

- **Источник**: сайт Avito (avito.ru)
- **Метод сбора**: Playwright (Chromium) — имитация реального пользователя
- **Антибан**: случайные задержки 5–15 сек, имитация скролла, стартовая задержка до 30 сек

### Как обрабатываются

1. **HTML** → парсинг через BeautifulSoup с fallback-селекторами
2. **Цены** → нормализация (удаление символов, приведение к числу)
3. **Даты** → парсинг относительных форматов («сегодня», «вчера», «N минут назад»)
4. **Сегментация** → группировка по `{condition}_{location}_{seller_type}` с объединением мелких сегментов
5. **Фильтрация** → trim-percent (5%) + IQR для удаления выбросов
6. **Анализ** → составной критерий: IQR (0.4) + z-score (0.3) + % от медианы (0.3)

### Куда сохраняются

| Данные | Таблица | Описание |
|---|---|---|
| Поисковые запросы | `tracked_searches` | URL, интервал, приоритет, последний запуск |
| Запуски | `search_runs` | Время, кол-во найденных/новых/ошибок |
| Объявления | `ads` | ad_id, title, price, location, seller, condition, аналитика |
| Снимки цен | `ad_snapshots` | price, html_path, timestamp |
| Уведомления | `notifications_sent` | ad_id, telegram_message_id, timestamp |
| HTML-файлы | Файловая система | `data/raw_html/search/` и `data/raw_html/ad/` |

### Зависимости

| Пакет | Версия | Назначение |
|---|---|---|
| `playwright` | 1.51.0 | Сбор данных с Avito (браузерная автоматизация) |
| `beautifulsoup4` | 4.13.3 | Парсинг HTML |
| `lxml` | 5.3.0 | Быстрый HTML-парсер |
| `sqlalchemy` | 2.0.40 | ORM для PostgreSQL |
| `psycopg2-binary` | 2.9.9 | Драйвер PostgreSQL |
| `alembic` | 1.15.2 | Миграции БД |
| `pydantic` | 2.11.3 | Валидация настроек |
| `pydantic-settings` | 2.8.1 | Загрузка конфигурации из .env |
| `python-dotenv` | 1.1.0 | Чтение .env файлов |
| `telethon` | ≥1.34.0 | Telegram-клиент (MTProto) |
| `typer` | 0.15.2 | CLI-интерфейс |
| `structlog` | 24.4.0 | Структурированное логирование |
| `numpy` | 2.2.4 | Математические вычисления (статистика) |
| `aiosmtplib` | ≥3.0.0 | Асинхронная отправка email |
