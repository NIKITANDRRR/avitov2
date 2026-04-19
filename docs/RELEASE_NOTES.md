# Release Notes — Avito Price Monitor

## [0.8.0] - 2026-04-19

### Fixed
- **Constant mode first cycle**: первый цикл теперь запускает все поиски принудительно (force_all), а не ждёт расписания
- **segment_price_history NotNullViolation**: поле `segment_key` добавлено в SQLAlchemy-модель и передаётся при INSERT
- **get_or_create_product() session rollback**: заменён полный `session.rollback()` на savepoint (`begin_nested()`), аналогично другим `get_or_create_*` методам
- **ensure_tables()**: теперь импортирует все модели (ранее только 5 из 10), что гарантирует создание всех таблиц

### Changed
- `run_constant_cycle()` принимает параметр `force_all: bool` для принудительного запуска всех поисков
- `ConstantScheduler.run()` передаёт `force_all=True` на первом цикле

### Removed
- Устаревшие скрипты: `_fix_rollback.py`, `cleanup_duplicates.py`, `renormalize_products.py`
- Устаревшие документы: `fix_plan.md`, `implementation_plan.md`, `improvements_plan.md`, `project_analysis.md`
- Устаревшие планы: `diamond_detection_product_first.md`, `normalizer_v2_plan.md`
- `project_start.txt`

## [2026-04-17] — Улучшения парсинга и режим force-pending

### Новое
- **QW-2: Сохранение цены из поисковой выдачи** — цена и заголовок теперь сохраняются при первом обнаружении объявления в поисковой выдаче, до парсинга карточки
  - [`SearchResultItem`](app/parser/search_parser.py:18) — добавлено вычисляемое поле `price` (нормализованное из `price_str`)
  - [`batch_get_or_create_ads()`](app/storage/repository.py:354) — принимает `title` и `price` из поисковых результатов
  - [`_process_search()`](app/scheduler/pipeline.py) — передаёт `item.title` и `item.price` при создании Ad
- **P1-1: Извлечение seller_type из карточки** — определение типа продавца (частный, компания, магазин)
  - Поле [`seller_type`](app/parser/ad_parser.py:47) добавлено в `AdData`
  - Функция [`_extract_seller_type()`](app/parser/ad_parser.py:432) — извлечение из HTML-блока продавца
  - Функция [`_extract_seller_data_from_next_data()`](app/parser/ad_parser.py:281) — fallback через `__NEXT_DATA__` (React/Next)
  - Функция [`_extract_seller_type_from_json()`](app/parser/ad_parser.py:369) — рекурсивный поиск в JSON-структуре
- **Force-pending: режим дообработки pending объявлений** — новая команда для повторной обработки объявлений, карточки которых не были спарсены
  - Команда [`force-pending`](app/scheduler/cli.py:202) — CLI-точка входа
  - Метод [`run_force_pending_cycle()`](app/scheduler/pipeline.py:535) — основная логика: загрузка, детекция капчи, парсинг, обновление
  - Метод [`_detect_captcha()`](app/collector/collector.py:439) — обнаружение Cloudflare, reCAPTCHA, hCaptcha, Bitrix CAPTCHA
  - Метод [`get_pending_ads()`](app/storage/repository.py:250) — запрос объявлений со статусом `parse_status='pending'`
  - Браузер запускается в видимом режиме (`headless=False`) для ручного ввода капчи
  - Ожидание ввода капчи — до 120 секунд, до 3 попыток на объявление
  - Паузы: 3–8 сек между объявлениями, 15–25 сек каждые 5 объявлений

### Изменения
- **P0-2: Валидация цены при создании Ad** — отрицательные цены обнуляются (`None`) в [`batch_get_or_create_ads()`](app/storage/repository.py:398)

### Документация
- Добавлен [`docs/fix_plan.md`](docs/fix_plan.md) — анализ проблем и план исправлений

### Изменённые файлы
- `app/parser/search_parser.py` — поле `price` в `SearchResultItem`, автонормализация
- `app/parser/ad_parser.py` — поле `seller_type`, расширенные селекторы, fallback `__NEXT_DATA__`
- `app/collector/collector.py` — метод `_detect_captcha()`
- `app/storage/repository.py` — метод `get_pending_ads()`, `title`/`price` в `batch_get_or_create_ads()`
- `app/scheduler/pipeline.py` — метод `run_force_pending_cycle()`, передача `title`/`price` из поиска
- `app/scheduler/cli.py` — команда `force-pending`
- `docs/fix_plan.md` — новый файл

---

## [2026-04-17] — Парсинг проданных товаров продавцов

### Новое
- **Парсинг профилей продавцов** — автоматический сбор данных о проданных товарах со страниц пользователей Avito
  - Новая модель `Seller` — реестр продавцов с рейтингом, отзывами, статистикой продаж
  - Новая модель `SoldItem` — проданные товары с ценой, категорией, датой продажи
  - Парсер `seller_parser.py` — извлечение данных из HTML профиля продавца
  - Метод `collect_seller_page()` в коллекторе — загрузка страниц профилей
  - Интеграция в пайплайн — автоматический сбор после обработки объявлений
- **Команда `force-parse`** — принудительный парсинг: сначала все товарные поиски, затем категории по очереди с интервалом
- **Warm-up режим** — при первом запуске сниженная параллельность и увеличенные задержки для предотвращения бана
- **Скрипт `db_stats.py`** — расширенная статистика базы данных
- **structlog** — структурированное логирование вместо стандартного logging

### Настройки
- `SELLER_PROFILE_ENABLED` — включение/выключение парсинга профилей (по умолчанию: True)
- `SELLER_RATE_LIMIT_PER_MINUTE` — rate limit для запросов к профилям (по умолчанию: 3/мин)
- `SELLER_MAX_PROFILES_PER_CYCLE` — макс. количество профилей за цикл (по умолчанию: 5)
- `SELLER_SCRAPE_INTERVAL_HOURS` — интервал повторного парсинга (по умолчанию: 24ч)
- `SELLER_PAGE_DELAY_MIN` / `SELLER_PAGE_DELAY_MAX` — задержки между страницами профиля (5–12 сек)
- `WARMUP_ENABLED` — режим разогрева при первом запуске (по умолчанию: True)
- `WARMUP_INITIAL_DELAY` — начальная задержка перед первым запросом (60 сек)
- `WARMUP_SEARCH_DELAY` — задержка между поисками при warm-up (30 сек)
- `WARMUP_AD_DELAY_MIN` / `WARMUP_AD_DELAY_MAX` — задержки между карточками при warm-up (10–20 сек)
- `WARMUP_MAX_CONCURRENT_SEARCHES` — параллельность поисков при warm-up (1)
- `WARMUP_MAX_CONCURRENT_ADS` — параллельность карточек при warm-up (1)
- `FORCE_PARSE_PRODUCT_DELAY_SECONDS` — задержка после парсинга товаров перед категориями (60 сек)
- `FORCE_PARSE_CATEGORY_INTERVAL_SECONDS` — интервал между категориями при force-parse (60 сек)

### Миграция
- Запустить `python -m scripts.migrate_seller_sold_items` для создания таблиц `sellers`, `sold_items` и добавления FK в `ads`

### Изменённые файлы
- `app/storage/models.py` — модели `Seller`, `SoldItem`, поле `seller_id_fk` в `Ad`
- `app/config/settings.py` — блоки настроек `SELLER_*`, `WARMUP_*`, `FORCE_PARSE_*`
- `app/parser/ad_parser.py` — извлечение `seller_id`, `seller_url` из карточки
- `app/parser/seller_parser.py` — новый парсер профиля продавца
- `app/parser/__init__.py` — экспорт `SellerProfileData`, `SoldItemData`, `parse_seller_profile`
- `app/collector/collector.py` — метод `collect_seller_page()`, селекторы для профиля продавца
- `app/storage/repository.py` — CRUD для Seller/SoldItem (9 новых методов)
- `app/scheduler/pipeline.py` — метод `_collect_seller_profiles()`, привязка продавца в `_process_ad()`
- `app/scheduler/cli.py` — команда `force-parse`, warm-up логика
- `scripts/migrate_seller_sold_items.py` — миграция БД
- `scripts/db_stats.py` — расширенная статистика БД

---

## [2026-04-17] — Оптимизация производительности парсера

### Added
- **Изоляция контекста браузера**: каждый поиск работает в отдельном `BrowserContext` (настройка `USE_ISOLATED_CONTEXTS`)
- **Раздельные rate limiter'ы**: независимые лимиты для поиска (6/мин) и карточек (8/мин) (`SEARCH_RATE_LIMIT_PER_MINUTE`, `AD_RATE_LIMIT_PER_MINUTE`)
- **Retry с exponential backoff**: до 3 попыток при ошибках навигации с задержкой 5→10→20 сек (`RETRY_MAX_ATTEMPTS`, `RETRY_BACKOFF_BASE`, `RETRY_BACKOFF_MAX`)
- **Batch-операции с БД**: `batch_get_or_create_ads()` для массового создания/обновления объявлений
- **Асинхронная запись HTML**: `asyncio.to_thread()` для неблокирующего сохранения HTML-файлов

### Changed
- **Задержки уменьшены**: 3–8 сек вместо 5–15 сек (`MIN_DELAY_SECONDS`: 5→3, `MAX_DELAY_SECONDS`: 15→8)
- **`DEFAULT_SCHEDULE_INTERVAL_HOURS`**: тип изменён с `int` на `float`, default 2→0.5 (30 мин)
- **`schedule_interval_hours` в БД**: тип колонки изменён с `INTEGER` на `FLOAT` (миграция в `init_db.py`)
- **`make_interval` в SQL**: использует секунды (`hours * 3600`) для поддержки дробных часов
- **CLI `add-search`**: параметр `--interval` теперь `float` (default 0.5)

### New Settings
| Параметр | По умолчанию | Описание |
|---|---|---|
| `SEARCH_RATE_LIMIT_PER_MINUTE` | `6` | Максимум запросов поиска в минуту |
| `AD_RATE_LIMIT_PER_MINUTE` | `8` | Максимум запросов карточек в минуту |
| `RETRY_MAX_ATTEMPTS` | `3` | Максимум попыток при ошибке загрузки |
| `RETRY_BACKOFF_BASE` | `5.0` | Базовая задержка exponential backoff (сек) |
| `RETRY_BACKOFF_MAX` | `60.0` | Максимальная задержка retry (сек) |
| `USE_ISOLATED_CONTEXTS` | `true` | Создавать отдельный контекст браузера на каждый поиск |

---

## [2026-04-15]

### Added
- **Параллельный сбор карточек**: до 5 вкладок открываются одновременно через `asyncio.Semaphore` (настройка `MAX_CONCURRENT_AD_PAGES`)
- **Пагинация поисковой выдачи**: обход до 50 страниц за один запуск поиска (настройка `MAX_SEARCH_PAGES_PER_RUN`)
- **Досрочная остановка пагинации**: при отсутствии новых объявлений на странице или пустом результате
- **Функция `build_page_url()`**: добавление параметра `&p=N` к URL Avito для пагинации

### Changed
- `_process_search()` полностью переписан: вместо одной страницы обходит до 50 с пагинацией
- Карточки объявлений обрабатываются параллельно через `asyncio.gather` вместо последовательного `for`
- `MAX_SEARCH_PAGES_PER_RUN` default изменён с 3 на 50, лимит увеличен с 10 до 100
- `recent_ids` загружается один раз перед циклом страниц и пополняется динамически

### New Settings
| Параметр | По умолчанию | Описание |
|---|---|---|
| `MAX_CONCURRENT_AD_PAGES` | `5` | Макс. параллельно открываемых карточек объявлений |
| `MAX_SEARCH_PAGES_PER_RUN` | `50` | Макс. страниц пагинации за запуск поиска |

---

## [2026-04-14]

### Added
- **Расширенная статистика сегментов**: median_7d, median_30d, median_90d, price_trend_slope
- **Двухуровневая цена**: listing_price_median (активные) и fast_sale_price_median (быстрые продажи)
- **Оценка ликвидной цены**: liquid_market_estimate с премией для редких товаров
- **Трекинг оборачиваемости**: first_seen_at, last_seen_at, days_on_market, is_disappeared_quickly
- **Детекция "бриллиантов"**: автоматическое обнаружение недооценённых редких товаров
- **Режим для редких сегментов**: fallback-логика с использованием истории и иерархии сегментов
- **История цен сегментов**: ежедневные/еженедельные снапшоты медианы
- **Правило роста рынка**: если median_7d > median_30d, используется более свежая метрика
- **Миграция БД**: скрипт `scripts/migrate_segment_analysis.py`
- **Настройки**: 8 новых параметров сегментного анализа в settings.py

### Changed
- `get_best_median()` теперь возвращает tuple (медиана, описание) с учётом роста рынка
- Пайплайн обновлён для трекинга оборачиваемости при каждом запуске
- Категорийные поисковые запросы обрабатываются через расширенный сегментный анализ

### New Files
- `scripts/migrate_segment_analysis.py` — миграция для новых таблиц и колонок
- `docs/implementation_plan.md` — детальный план реализации

---

## Категорийный мониторинг

### 1. Категорийный мониторинг (category search)
Система умеет искать товары не по конкретным моделям, а по **целым категориям** (например, «все iPhone» или «все MacBook»). Из заголовков объявлений автоматически извлекаются:
- **Категория** (телефон, ноутбук, планшет)
- **Бренд** (Apple, Samsung, Xiaomi)
- **Модель** (iPhone 15 Pro Max, MacBook Air M3)

Объявления разбиваются на сегменты, и для каждого сегмента считается медиана, IQR, стандартное отклонение. Товары с ценой аномально ниже сегмента помечаются как «бриллианты».

### 2. Детекция бриллиантов (diamond detection)
Составной score учитывает:
- **Скидку** от медианы сегмента (30%)
- **Редкость** товара в сегменте (30%)
- **Ликвидность** — сколько объявлений быстро продаются (20%)
- **Дефицит предложения** — изменение объёма за 30 дней (20%)

Если score > 0.6 — алерт.

### 3. Фильтрация аксессуаров
Автоматически отсекаются:
- Чехлы, кабели, защитные стёкла
- Запчасти (матрицы, шлейфы, аккумуляторы)
- Товары дешевле 5000₽
- Товары дешевле 30% медианы сегмента

### 4. Извлечение атрибутов из заголовков
Регулярные выражения + словари брендов/моделей для автоматического парсинга:
- «iPhone 15 Pro Max 256GB» → brand=Apple, model=iPhone 15 Pro Max, storage=256GB
- «MacBook Air M3 13 8/256» → brand=Apple, model=MacBook Air M3, screen=13", ram=8GB, storage=256GB

### 5. История цен по сегментам
Ежедневные снимки медианы, среднего и количества объявлений по каждому сегменту. Позволяет отслеживать тренды цен.

### 6. Уведомления
- **Telegram** через Telethon + MTProto-прокси
- **Email** через SMTP (Gmail) — fallback если Telegram недоступен

---

## Таблицы в БД

| Таблица | Назначение |
|---|---|
| `tracked_searches` | Поисковые запросы |
| `search_runs` | Записи о запусках сбора |
| `ads` | Объявления Avito |
| `ad_snapshots` | Снимки цен объявлений |
| `notifications_sent` | Отправленные уведомления |
| `sellers` | Профили продавцов |
| `sold_items` | Проданные товары продавцов |
| `segment_stats` | Предрасчитанная статистика по сегментам |
| `segment_price_history` | Ежедневные снимки цен по сегментам |
