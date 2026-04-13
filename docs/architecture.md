# Архитектура системы мониторинга Avito

> **Версия:** 2.0
> **Статус:** Actual
> **Дата:** 2026-04-12

---

## Содержание

1. [Обзор системы](#1-обзор-системы)
2. [Структура проекта](#2-структура-проекта)
3. [Модели данных PostgreSQL](#3-модели-данных-postgresql)
4. [Интерфейсы модулей](#4-интерфейсы-модулей)
5. [Конфигурация](#5-конфигурация)
6. [Алгоритм одного запуска](#6-алгоритм-одного-запуска)
7. [Поток данных](#7-поток-данных)
8. [Аналитический движок v2](#8-аналитический-движок-v2)
9. [Архитектура планировщика](#9-архитектура-планировщика)
10. [Обработка ошибок](#10-обработка-ошибок)
11. [Логирование](#11-логирование)
12. [CLI — точка входа](#12-cli--точка-входа)
13. [Скрипты утилиты](#13-скрипты-утилиты)
14. [Зависимости](#14-зависимости)
15. [Ограничения и допущения](#15-ограничения-и-допущения)
16. [Ключевые паттерны](#16-ключевые-паттерны)

---

## 1. Обзор системы

Система мониторинга объявлений Avito с расширенным анализом цен и циклическим планировщиком. Работает в **асинхронном режиме** с **больши́ми случайными задержками**, используя Playwright + Chromium для сбора данных.

### Принципы

- **Low-traffic:** случайные задержки между действиями, ограничение карточек на поиск
- **Headful по умолчанию:** на этапе PoC используется видимый браузер
- **Без API Avito:** только веб-скрапинг через Playwright
- **Без прокси:** но архитектура готова к добавлению
- **Циклический планировщик:** автоматический запуск каждые 5 минут с проверкой просроченных поисков
- **Масштабирование:** батчевая обработка до 20+ поисков с `asyncio.Semaphore`

### Стек технологий

| Компонент       | Технология              |
|-----------------|-------------------------|
| Язык            | Python 3.12+            |
| Сбор данных     | Playwright + Chromium   |
| Парсинг         | BeautifulSoup4 + lxml   |
| База данных     | PostgreSQL 16+          |
| ORM             | SQLAlchemy 2.0 async    |
| Уведомления     | python-telegram-bot v20+, aiosmtplib (fallback) |
| Конфигурация    | pydantic-settings v2    |
| CLI             | Typer                   |
| Логирование     | structlog               |
| Анализ цен      | numpy (IQR, Z-score, сегментация) |

---

## 2. Структура проекта

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
│   ├── architecture.md             # Этот файл
│   └── improvements_plan.md        # План улучшений
├── .env.example                    # Пример файла конфигурации
├── requirements.txt                # Зависимости Python
├── README.md                       # Инструкция по запуску
└── .gitignore
```

### Назначение директорий

| Директория         | Назначение                                                |
|--------------------|-----------------------------------------------------------|
| `app/config/`      | Конфигурация приложения через pydantic-settings           |
| `app/collector/`   | Управление браузером Playwright, сбор HTML                |
| `app/parser/`      | Извлечение структурированных данных из HTML               |
| `app/storage/`     | Модели БД, подключение, CRUD-операции, аналитические запросы |
| `app/analysis/`    | Продвинутый анализ цен v2: сегментация, IQR, Z-score, составной score |
| `app/notifier/`    | Отправка уведомлений в Telegram и Email (fallback)        |
| `app/scheduler/`   | Оркестрация пайплайна, циклический планировщик, CLI       |
| `app/utils/`       | Общие утилиты — задержки, нормализация, HTML, URL         |
| `data/raw_html/`   | Сырые HTML-файлы, организованные по типу страницы         |
| `scripts/`         | Скрипты администрирования (миграции, заполнение, очистка) |
| `tests/`           | Модульные и интеграционные тесты                          |

---

## 3. Модели данных PostgreSQL

### ER-диаграмма

```mermaid
erDiagram
    tracked_searches ||--o{ search_runs : has
    tracked_searches ||--o{ ads : contains
    ads ||--o{ ad_snapshots : has
    ads ||--o{ notifications_sent : triggers

    tracked_searches {
        bigserial id PK
        varchar search_url UK
        varchar search_phrase
        boolean is_active
        integer schedule_interval_hours
        timestamptz last_run_at
        integer priority
        integer max_ads_to_parse
        timestamptz created_at
        timestamptz updated_at
    }

    search_runs {
        bigserial id PK
        bigint tracked_search_id FK
        timestamptz started_at
        timestamptz completed_at
        integer pages_fetched
        integer ads_found
        integer ads_new
        integer ads_opened
        integer errors_count
        varchar status
        text error_message
    }

    ads {
        bigserial id PK
        bigint ad_id UK
        varchar url
        varchar title
        integer price
        varchar location
        varchar seller_name
        varchar seller_type
        varchar condition
        timestamptz publication_date
        varchar search_url
        bigint tracked_search_id FK
        timestamptz first_seen_at
        timestamptz last_scraped_at
        varchar parse_status
        text last_error
        boolean is_undervalued
        float undervalue_score
        float z_score
        boolean iqr_outlier
        varchar segment_key
    }

    ad_snapshots {
        bigserial id PK
        bigint ad_id FK
        integer price
        timestamptz scraped_at
        varchar html_path
    }

    notifications_sent {
        bigserial id PK
        bigint ad_id FK
        varchar notification_type
        timestamptz sent_at
        integer telegram_message_id
    }
```

### DDL — SQL-схема

#### `tracked_searches` — Отслеживаемые поисковые запросы

```sql
CREATE TABLE tracked_searches (
    id                      BIGSERIAL       PRIMARY KEY,
    search_url              VARCHAR(1024)   NOT NULL UNIQUE,
    search_phrase           VARCHAR(512)    NOT NULL,
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE,
    schedule_interval_hours INTEGER         NOT NULL DEFAULT 2,       -- Интервал запуска (часы)
    last_run_at             TIMESTAMPTZ,                               -- Время последнего запуска
    priority                INTEGER         NOT NULL DEFAULT 5,       -- Приоритет (1-10, ниже = важнее)
    max_ads_to_parse        INTEGER         NOT NULL DEFAULT 3,       -- Карточек за запуск
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tracked_searches_is_active ON tracked_searches (is_active);
CREATE INDEX idx_tracked_searches_last_run_at ON tracked_searches (last_run_at);
```

#### `search_runs` — Запуски сбора

```sql
CREATE TABLE search_runs (
    id                  BIGSERIAL       PRIMARY KEY,
    tracked_search_id   BIGINT          NOT NULL REFERENCES tracked_searches(id) ON DELETE CASCADE,
    started_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    pages_fetched       INTEGER         NOT NULL DEFAULT 0,
    ads_found           INTEGER         NOT NULL DEFAULT 0,
    ads_new             INTEGER         NOT NULL DEFAULT 0,
    ads_opened          INTEGER         NOT NULL DEFAULT 0,
    errors_count        INTEGER         NOT NULL DEFAULT 0,
    status              VARCHAR(32)     NOT NULL DEFAULT 'running',  -- running | completed | failed
    error_message       TEXT
);

CREATE INDEX idx_search_runs_tracked_search_id ON search_runs (tracked_search_id);
CREATE INDEX idx_search_runs_status ON search_runs (status);
```

#### `ads` — Объявления

```sql
CREATE TABLE ads (
    id                  BIGSERIAL       PRIMARY KEY,
    ad_id               BIGINT          NOT NULL UNIQUE,
    url                 VARCHAR(1024)   NOT NULL,
    title               VARCHAR(512),
    price               INTEGER,
    location            VARCHAR(256),
    seller_name         VARCHAR(256),
    seller_type         VARCHAR(32),                               -- Тип продавца: «частное», «магазин», «компания»
    condition           VARCHAR(64),
    publication_date    TIMESTAMPTZ,
    search_url          VARCHAR(1024)   NOT NULL,
    tracked_search_id   BIGINT          NOT NULL REFERENCES tracked_searches(id) ON DELETE CASCADE,
    first_seen_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    last_scraped_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    parse_status        VARCHAR(32)     NOT NULL DEFAULT 'pending',
    last_error          TEXT,
    is_undervalued      BOOLEAN         NOT NULL DEFAULT FALSE,
    undervalue_score    FLOAT,
    z_score             FLOAT,                                     -- Z-score цены относительно сегмента
    iqr_outlier         BOOLEAN         NOT NULL DEFAULT FALSE,      -- Является ли цена IQR-выбросом
    segment_key         VARCHAR(256)                                -- Ключ сегмента анализа
);

CREATE INDEX idx_ads_ad_id ON ads (ad_id);
CREATE INDEX idx_ads_search_url ON ads (search_url);
CREATE INDEX idx_ads_is_undervalued ON ads (is_undervalued);
CREATE INDEX idx_ads_first_seen_at ON ads (first_seen_at);
CREATE INDEX idx_ads_tracked_search_id ON ads (tracked_search_id);
CREATE INDEX idx_ads_parse_status ON ads (parse_status);
CREATE INDEX idx_ads_segment_key ON ads (segment_key);
```

#### `ad_snapshots` — Снимки цен

```sql
CREATE TABLE ad_snapshots (
    id          BIGSERIAL       PRIMARY KEY,
    ad_id       BIGINT          NOT NULL REFERENCES ads(id) ON DELETE CASCADE,
    price       INTEGER         NOT NULL,
    scraped_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    html_path   VARCHAR(512)    NOT NULL
);

CREATE INDEX idx_ad_snapshots_ad_id ON ad_snapshots (ad_id);
CREATE INDEX idx_ad_snapshots_scraped_at ON ad_snapshots (scraped_at);
```

#### `notifications_sent` — Отправленные уведомления

```sql
CREATE TABLE notifications_sent (
    id                      BIGSERIAL       PRIMARY KEY,
    ad_id                   BIGINT          NOT NULL REFERENCES ads(id) ON DELETE CASCADE,
    notification_type       VARCHAR(64)     NOT NULL DEFAULT 'undervalued',
    sent_at                 TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    telegram_message_id     BIGINT
);

CREATE INDEX idx_notifications_sent_ad_id ON notifications_sent (ad_id);
CREATE UNIQUE INDEX idx_notifications_sent_unique ON notifications_sent (ad_id, notification_type);
```

### SQLAlchemy-модели — расположение

Модели определяются в [`app/storage/models.py`](app/storage/models.py) с использованием `DeclarativeBase` и `MappedAsColumn` из SQLAlchemy 2.0. Все модели используют аннотации типов `Mapped[type]`.

---

## 4. Интерфейсы модулей

### 4.1 Data Classes / DTO

Определяются в [`app/analysis/analyzer.py`](app/analysis/analyzer.py) и соответствующих модулях.

```python
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class SearchResult:
    """Результат парсинга одной записи из поисковой выдачи."""
    ad_id: int
    url: str
    title: str | None = None
    price: int | None = None
    location: str | None = None

@dataclass
class AdData:
    """Полные данные объявления из карточки."""
    ad_id: int
    url: str
    title: str | None = None
    price: int | None = None
    location: str | None = None
    seller_name: str | None = None
    seller_type: str | None = None       # Тип продавца
    condition: str | None = None
    publication_date: datetime | None = None
    search_url: str | None = None

@dataclass
class MarketStats:
    """Расширенная статистика рынка для поискового запроса/сегмента."""
    search_url: str
    median_price: float
    mean_price: float
    q1_price: float                       # Первый квартиль
    q3_price: float | None = None         # Третий квартиль (75-й перцентиль)
    iqr: float | None = None              # Межквартильный размах: Q3 - Q1
    std_dev: float | None = None          # Стандартное отклонение
    lower_fence: float | None = None      # Нижняя граница IQR: Q1 - 1.5 * IQR
    upper_fence: float | None = None      # Верхняя граница IQR: Q3 + 1.5 * IQR
    trimmed_mean: float | None = None     # Среднее после удаления 5% выбросов
    segment_key: str = ""                 # Ключ сегмента
    sample_size: int = 0                  # Размер выборки
    calculated_at: datetime = field(default_factory=datetime.now)

@dataclass
class UndervaluedAd:
    """Объявление ниже рыночной цены."""
    ad: AdData
    stats: MarketStats
    undervalue_score: float               # price / median_price
    price_diff_percent: float             # Отклонение в %

@dataclass
class UndervaluedResult:
    """Результат анализа недооценённости v2."""
    ad_id: int
    is_undervalued: bool
    undervalue_score: float
    iqr_score: float
    zscore_score: float
    median_score: float
    segment_key: str

@dataclass
class AdAnalysisResult:
    """Полный результат анализа одного объявления."""
    ad_id: int
    price: int
    segment_key: str
    market_stats: MarketStats
    z_score: float | None
    iqr_outlier: bool
    is_undervalued: bool
    undervalue_score: float
```

### 4.2 Collector — `app/collector/`

#### `app/collector/browser.py`

```python
from playwright.async_api import async_playwright, Browser, Page

class AvitoCollector:
    """Сборщик HTML-страниц Avito через Playwright."""

    def __init__(self, settings: "Settings") -> None:
        """
        Инициализация коллектора с настройками.

        Args:
            settings: конфигурация приложения
        """
        ...

    async def start(self) -> None:
        """Запуск браузера. Создаёт Playwright instance и открывает Browser."""
        ...

    async def stop(self) -> None:
        """Закрытие браузера и Playwright."""
        ...

    async def collect_search_page(self, url: str) -> str:
        """
        Открыть поисковую страницу и вернуть HTML.

        Алгоритм:
        1. Случайная задержка перед открытием
        2. Создание нового контекста со случайным User-Agent
        3. Навигация по URL
        4. Ожидание загрузки списка объявлений (wait for selector)
        5. Прокрутка страницы для загрузки lazy-контента
        6. Получение HTML через page.content()
        7. Закрытие контекста

        Args:
            url: URL поисковой страницы Avito

        Returns:
            Полный HTML страницы в виде строки

        Raises:
            CollectorError: при ошибке загрузки страницы
        """
        ...

    async def collect_ad_page(self, url: str) -> str:
        """
        Открыть карточку объявления и вернуть HTML.

        Args:
            url: URL карточки объявления

        Returns:
            Полный HTML карточки в виде строки

        Raises:
            CollectorError: при ошибке загрузки страницы
        """
        ...
```

#### `app/collector/collector.py`

```python
class Collector:
    """Высокоуровневый сборщик данных Avito."""

    async def collect_ad_data(self, html: str, url: str) -> AdData:
        """
        Собрать данные объявления из HTML карточки.
        Извлекает все поля включая seller_type.
        """
        ...
```

**Примечание:** [`collector.py`](app/collector/collector.py) извлекает `seller_type` при парсинге карточки объявления.

### 4.3 Parser — `app/parser/`

#### `app/parser/search_parser.py`

```python
from bs4 import BeautifulSoup

def parse_search_page(html: str, search_url: str) -> list[SearchResult]:
    """
    Парсинг HTML поисковой страницы Avito.

    Алгоритм:
    1. Создать BeautifulSoup(html, 'lxml')
    2. Найти все элементы объявлений по fallback-селекторам
    3. Для каждого элемента извлечь: ad_id, url, title, price, location
    4. ad_id извлекается из data-marker атрибута или из URL
    5. URL формируется как полный https://www.avito.ru/... если он относительный
    6. Пропустить элементы, которые не удалось распарсить — не падать

    Args:
        html: HTML-код поисковой страницы
        search_url: URL поиска для контекста

    Returns:
        Список SearchResult. Может быть пустым.
    """
    ...

def extract_ad_id_from_url(url: str) -> int | None:
    """
    Извлечь числовой ID объявления из URL Avito.

    Пример: https://www.avito.ru/moskva/telefony/iphone_1234567890
    Результат: 1234567890
    """
    ...
```

#### `app/parser/ad_parser.py`

```python
def normalize_price(price_text: str | None) -> int | None:
    """
    Нормализация цены из текста Avito в целое число.

    Примеры:
        "12 345 ₽" → 12345
        "1\xa0234" → 1234
        None → None
    """
    ...

def parse_ad_page(html: str, url: str) -> AdData:
    """
    Парсинг HTML карточки объявления Avito.
    Извлекает все поля включая seller_type.
    """
    ...
```

### 4.4 Storage — `app/storage/`

#### `app/storage/database.py`

```python
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

def create_engine(database_url: str):
    """Создать async SQLAlchemy engine."""
    ...

def create_session_factory(engine) -> async_sessionmaker[AsyncSession]:
    """Создать фабрику async-сессий."""
    ...

async def init_db(engine) -> None:
    """Создать все таблицы по моделям SQLAlchemy."""
    ...
```

#### `app/storage/models.py`

SQLAlchemy 2.0 ORM-модели, соответствующие таблицам из раздела 3. Используют `Mapped[type]` и `mapped_column`. Модельный класс `Base` с `DeclarativeBase`.

Модели:
- `TrackedSearch` → `tracked_searches` (включая `schedule_interval_hours`, `last_run_at`, `priority`, `max_ads_to_parse`)
- `SearchRun` → `search_runs`
- `Ad` → `ads` (включая `seller_type`, `z_score`, `iqr_outlier`, `segment_key`)
- `AdSnapshot` → `ad_snapshots`
- `NotificationSent` → `notifications_sent`

#### `app/storage/repository.py`

```python
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

class Repository:
    """Репозиторий для работы с БД. Все методы — async."""

    def __init__(self, session: AsyncSession) -> None:
        ...

    # --- TrackedSearch ---

    async def get_active_searches(self) -> list[TrackedSearch]:
        """Получить все активные поисковые запросы."""
        ...

    async def get_or_create_tracked_search(
        self, search_url: str, search_phrase: str
    ) -> TrackedSearch:
        """Получить или создать запись отслеживаемого поиска."""
        ...

    async def get_searches_due_for_run(self) -> list[TrackedSearch]:
        """
        Получить поиски, готовые к запуску.

        Выбирает активные поиски, у которых:
        - last_run_at IS NULL (никогда не запускался), ИЛИ
        - last_run_at + schedule_interval_hours <= NOW()

        Сортировка по priority (ASC), затем по last_run_at (ASC).
        """
        ...

    async def update_search_last_run(self, search_id: int) -> None:
        """Обновить last_run_at = NOW() для указанного поиска."""
        ...

    # --- SearchRun ---

    async def create_search_run(
        self, tracked_search_id: int
    ) -> SearchRun:
        """Создать запись о запуске сбора со статусом 'running'."""
        ...

    async def complete_search_run(
        self,
        run_id: int,
        ads_found: int,
        ads_new: int,
        pages_fetched: int = 1,
        ads_opened: int = 0,
        errors_count: int = 0,
    ) -> None:
        """Завершить запуск — установить completed_at, метрики, status='completed'."""
        ...

    async def fail_search_run(
        self, run_id: int, error_message: str
    ) -> None:
        """Отметить запуск как failed с сообщением об ошибке."""
        ...

    # --- Ads ---

    async def get_or_create_ad(
        self,
        ad_id: int,
        url: str,
        search_url: str,
        tracked_search_id: int,
    ) -> tuple[Ad, bool]:
        """
        Получить или создать объявление.
        Обрабатывает IntegrityError при конкурентной вставке.

        Returns:
            Кортеж (модель Ad, флаг created_new)
        """
        ...

    async def update_ad_data(self, ad_id: int, data: AdData) -> None:
        """
        Обновить данные объявления из AdData.
        Устанавливает parse_status = 'parsed', last_scraped_at = NOW().
        """
        ...

    async def mark_ad_parse_failed(self, ad_id: int, error: str) -> None:
        """Отметить объявление как failed при парсинге."""
        ...

    async def mark_ad_undervalued(
        self, ad_id: int, score: float
    ) -> None:
        """Пометить объявление как undervalued с указанием score."""
        ...

    async def get_recent_ad_ids(
        self, search_url: str, hours: int = 24
    ) -> set[int]:
        """
        Получить множество ad_id объявлений, которые уже проверялись
        за последние N часов для данного search_url.
        """
        ...

    async def get_ads_for_search(
        self, search_url: str
    ) -> list[Ad]:
        """Получить все объявления для данного поискового запроса."""
        ...

    async def get_ads_by_segment(
        self, segment_key: str
    ) -> list[Ad]:
        """
        Получить все объявления в указанном сегменте.

        Args:
            segment_key: Ключ сегмента (например, "Новое_Москва_частное")
        """
        ...

    async def get_ads_for_analysis(
        self, tracked_search_id: int, max_age_days: int = 14
    ) -> list[Ad]:
        """
        Получить объявления для ценового анализа.

        Фильтрует по:
        - tracked_search_id
        - price IS NOT NULL
        - publication_date или first_seen_at не старше max_age_days

        Args:
            tracked_search_id: ID отслеживаемого поиска
            max_age_days: Максимальный возраст объявлений в днях
        """
        ...

    async def update_ad_analysis(
        self,
        ad_id: int,
        z_score: float | None = None,
        iqr_outlier: bool | None = None,
        segment_key: str | None = None,
        is_undervalued: bool | None = None,
        undervalue_score: float | None = None,
    ) -> None:
        """
        Обновить результаты анализа для объявления.

        Записывает: z_score, iqr_outlier, segment_key, is_undervalued, undervalue_score.
        """
        ...

    # --- AdSnapshots ---

    async def save_ad_snapshot(
        self, ad_id: int, price: int, html_path: str
    ) -> AdSnapshot:
        """Сохранить снимок цены и путь к HTML."""
        ...

    # --- NotificationsSent ---

    async def is_notification_sent(
        self, ad_id: int, notification_type: str = "undervalued"
    ) -> bool:
        """Проверить, было ли уже отправлено уведомление данного типа."""
        ...

    async def mark_notification_sent(
        self, ad_id: int, notification_type: str, telegram_message_id: int
    ) -> None:
        """Записать факт отправки уведомления."""
        ...
```

### 4.5 Analysis — `app/analysis/analyzer.py`

```python
import numpy as np

class PriceAnalyzer:
    """Продвинутый анализатор рыночных цен v2."""

    def __init__(self, repository: Repository, settings: "Settings") -> None:
        """
        Args:
            repository: репозиторий для доступа к данным
            settings: конфигурация приложения (параметры анализа)
        """
        ...

    # === Базовые методы v1 ===

    async def calculate_market_stats(
        self, search_url: str
    ) -> MarketStats | None:
        """
        Рассчитать базовую статистику рынка для поискового запроса.

        Returns:
            MarketStats или None если недостаточно данных
        """
        ...

    async def detect_undervalued(
        self, search_url: str
    ) -> list[UndervaluedAd]:
        """
        Найти объявления ниже рыночной цены (алгоритм v1).

        Критерий: price < median * threshold
        """
        ...

    # === Методы v2: сегментация и фильтрация ===

    def segment_ads(self, ads: list[Ad]) -> dict[str, list[Ad]]:
        """
        Сегментация объявлений по ключу {condition}_{location}_{seller_type}.

        Правила:
        - condition: значение из Ad.condition — «Новое», «Б/у», «Не указано»
        - location: город из Ad.location (первый компонент: «Москва» из «Москва, Арбат»)
        - seller_type: тип продавца из Ad.seller_type

        Returns:
            Словарь {segment_key: [list of Ad]}
        """
        ...

    def build_segment_key(self, ad: Ad) -> str:
        """
        Построить ключ сегмента для объявления.

        Returns:
            Строка вида "Новое_Москва_частное"
        """
        ...

    def filter_iqr(
        self, prices: np.ndarray, k: float = 1.5
    ) -> np.ndarray:
        """
        IQR-фильтрация выбросов.

        Алгоритм:
        1. Q1 = percentile(prices, 25)
        2. Q3 = percentile(prices, 75)
        3. IQR = Q3 - Q1
        4. lower = Q1 - k * IQR
        5. upper = Q3 + k * IQR
        6. Оставить prices[(prices >= lower) & (prices <= upper)]

        Args:
            prices: массив цен
            k: множитель IQR (по умолчанию 1.5)
        """
        ...

    def calculate_zscore(self, prices: np.ndarray) -> np.ndarray:
        """
        Расчёт Z-score для массива цен.

        Формула: z = (price - mean) / std
        """
        ...

    # === Методы v2: расширенный анализ ===

    async def detect_undervalued_v2(
        self, ads: list[Ad], market_stats: MarketStats
    ) -> list[UndervaluedResult]:
        """
        Определение недооценённых объявлений (алгоритм v2).

        Составной критерий:
        - IQR-аномалия: price < Q1 - k * IQR
        - Z-score: z < -z_threshold
        - Процент от медианы: price < median * threshold

        Комбинированный score:
        score = 0.4 * iqr_score + 0.3 * zscore_score + 0.3 * median_score

        Returns:
            Список UndervaluedResult для каждого объявления
        """
        ...

    async def analyze_ad(
        self, ad: Ad, market_stats: MarketStats
    ) -> AdAnalysisResult:
        """
        Полный анализ одного объявления.

        Включает:
        - Расчёт Z-score
        - Проверку IQR-выброса
        - Определение недооценённости
        - Расчёт составного score

        Returns:
            AdAnalysisResult со всеми метриками
        """
        ...
```

### 4.6 Notifier — `app/notifier/`

#### `app/notifier/telegram_notifier.py`

```python
class TelegramNotifier:
    """Отправка уведомлений в Telegram."""

    def __init__(self, bot_token: str, chat_id: str) -> None: ...

    async def send_undervalued_notification(
        self, ad: AdData, stats: MarketStats, undervalue_score: float
    ) -> int | None:
        """
        Отправить уведомление о заниженном объявлении.
        Возвращает telegram_message_id при успехе, None при ошибке.
        """
        ...

    async def send_cycle_summary(
        self, total_ads: int, new_ads: int, undervalued_count: int
    ) -> None:
        """Отправить краткое резюме цикла сбора."""
        ...
```

#### `app/notifier/email_notifier.py`

```python
class EmailNotifier:
    """Резервный email-нотификатор (fallback при недоступности Telegram)."""

    async def send_notification(self, subject: str, body: str) -> bool:
        """Отправить email-уведомление. Возвращает True при успехе."""
        ...

    async def send_undervalued_notifications(
        self, undervalued_ads: list[UndervaluedAd]
    ) -> int:
        """Отправить уведомления о всех недооценённых товарах по email."""
        ...
```

### 4.7 Scheduler — `app/scheduler/`

#### `app/scheduler/scheduler.py`

```python
class Scheduler:
    """Циклический планировщик запуска поисков.

    Периодически проверяет, какие поиски пора запускать,
    и передаёт их в Pipeline для обработки.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        """
        Args:
            settings: Конфигурация приложения.
                Если None — создаётся экземпляр по умолчанию.
        """
        ...

    async def run(self) -> None:
        """
        Основной цикл: проверяет каждые 5 минут, какие поиски пора запустить.

        Цикл работает до вызова stop(). Каждый цикл:
            1. Вызывает Pipeline.run_search_cycle() для обработки
               всех просроченных поисков.
            2. Спит 5 минут до следующей проверки.

        Исключения в одном цикле не прерывают работу планировщика.
        """
        ...

    def stop(self) -> None:
        """Остановить планировщик. Текущий цикл завершится корректно."""
        ...
```

#### `app/scheduler/pipeline.py`

```python
class Pipeline:
    """Пайплайн обработки поисковых запросов с батчевой обработкой."""

    def __init__(self, settings: Settings) -> None: ...

    async def run_search_cycle(self) -> dict:
        """
        Основной цикл обработки поисков из БД.

        Алгоритм:
        1. Получить список поисков, готовых к запуску (get_searches_due_for_run)
        2. Разбить на батчи по MAX_CONCURRENT_SEARCHES
        3. Для каждого батча:
           - Запустить обработки параллельно через asyncio.Semaphore
           - Задержка SEARCH_DELAY_SECONDS между поисками
           - Задержка BATCH_DELAY_SECONDS между батчами
        4. Анализ и уведомления (_analyze_and_notify_searches)

        Returns:
            Словарь со статистикой: total_searches, total_ads, etc.
        """
        ...

    async def _process_tracked_search(self, search: TrackedSearch) -> dict:
        """
        Обработка одного поискового запроса.

        Включает: сбор поисковой страницы, парсинг, дедупликация,
        сбор карточек, обновление last_run_at.
        """
        ...

    async def _analyze_and_notify_searches(
        self, searches: list[TrackedSearch]
    ) -> None:
        """
        Анализ цен и отправка уведомлений для обработанных поисков.

        Для каждого поиска:
        1. Получить объявления для анализа (get_ads_for_analysis)
        2. Сегментировать (segment_ads)
        3. Рассчитать статистику по сегментам
        4. Определить недооценённые (detect_undervalued_v2)
        5. Обновить результаты анализа (update_ad_analysis)
        6. Отправить уведомления (_send_notifications)
        """
        ...

    async def _send_notifications(
        self, undervalued_ads: list[UndervaluedResult]
    ) -> None:
        """
        Отправка уведомлений: Telegram → Email fallback.
        """
        ...
```

### 4.8 Utils — `app/utils/`

#### `app/utils/helpers.py`

```python
async def random_delay(min_sec: float, max_sec: float) -> None:
    """Асинхронная случайная задержка с логированием."""
    ...

def normalize_url(url: str) -> str:
    """Нормализация URL: удаление query-параметров, fragment, trailing slash."""
    ...

def extract_ad_id_from_url(url: str) -> str:
    """Извлечение ID объявления из URL Avito."""
    ...

def save_html(html: str, directory: str, filename: str) -> str:
    """Сохранение HTML-контента на диск. Возвращает полный путь к файлу."""
    ...

def normalize_price(price_str: str) -> float | None:
    """Парсинг строки цены в число."""
    ...

def setup_logging(level: str) -> None:
    """Настройка structlog для приложения."""
    ...

def build_avito_url(query: str, location: str = "Москва") -> str:
    """
    Построить URL поиска Avito по запросу и локации.

    Поддерживаемые локации: Москва, Санкт-Петербург, Екатеринбург,
    Новосибирск, Россия (и транслитерации).

    Returns:
        URL вида https://www.avito.ru/{location_slug}?q={query}&s=104
    """
    ...
```

#### `app/utils/exceptions.py`

```python
class AvitoMonitorError(Exception):
    """Базовое исключение системы."""
    pass

class CollectorError(AvitoMonitorError):
    """Ошибка при сборе HTML через Playwright."""
    pass

class ParserError(AvitoMonitorError):
    """Ошибка при парсинге HTML."""
    pass

class StorageError(AvitoMonitorError):
    """Ошибка при работе с БД."""
    pass

class NotificationError(AvitoMonitorError):
    """Ошибка при отправке уведомления."""
    pass
```

---

## 5. Конфигурация

### `app/config/settings.py`

```python
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Конфигурация приложения.
    Значения загружаются из .env файла и/или переменных окружения.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # --- Поисковые запросы ---
    search_urls: list[str]                           # Список URL для мониторинга
    search_phrases: list[str] = []                   # Человекочитаемые названия запросов

    # --- Лимиты сбора ---
    max_search_pages_per_run: int = 3                # Максимум страниц пагинации на один поиск
    max_ads_per_search_per_run: int = 3              # Максимум новых карточек на один поиск

    # --- Задержки ---
    min_delay_seconds: float = 5.0                   # Минимальная задержка между действиями (сек)
    max_delay_seconds: float = 15.0                  # Максимальная задержка между действиями (сек)
    startup_delay_min: float = 0.0                   # Минимальная задержка перед стартом цикла
    startup_delay_max: float = 30.0                  # Максимальная задержка перед стартом цикла

    # --- Браузер ---
    headless: bool = False                           # Headless-режим
    use_proxy: bool = False                          # Использовать прокси
    proxy_url: str | None = None                     # URL прокси-сервера

    # --- Telegram ---
    telegram_bot_token: str                          # Токен Telegram бота
    telegram_chat_id: str                            # ID чата для уведомлений

    # --- Telegram MTProto ---
    telegram_api_id: int = 0                         # API ID для MTProto
    telegram_api_hash: str = ""                      # API Hash для MTProto
    mtproxy_enabled: bool = True                     # Использовать MTProto proxy
    mtproxy_address: str = "135.136.188.80"          # Адрес MTProxy сервера
    mtproxy_port: int = 15871                        # Порт MTProxy
    mtproxy_secret: str = ""                         # Секрет MTProxy

    # --- Email (fallback) ---
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_user: str | None = None
    smtp_password: str | None = None
    smtp_use_tls: bool = True
    email_from: str | None = None
    email_to: str | None = None

    # --- База данных ---
    database_url: str = "postgresql+asyncpg://avito:avito@localhost:5432/avito_monitor"

    # --- Анализ цен (v1) ---
    undervalue_threshold: float = 0.8                # Порог: price < median * threshold → undervalued

    # --- Фильтрация аномалий (v2) ---
    trim_percent: float = 0.05                       # % отбрасывания с каждого края
    iqr_multiplier: float = 1.5                      # Множитель для IQR fences
    temporal_window_days: int = 14                   # Окно анализа в днях
    min_segment_size: int = 3                        # Минимальный размер сегмента
    undervalued_threshold: float = 0.3               # Порог составного score для недооценённости
    zscore_threshold: float = 1.5                    # Порог Z-score для аномалий
    median_discount_threshold: float = 0.85          # Порог % от медианы

    # --- Масштабирование поиска ---
    max_concurrent_searches: int = 3                 # Макс. параллельных поисков в батче
    default_schedule_interval_hours: int = 2         # Интервал запуска по умолчанию (часы)
    default_max_ads_to_parse: int = 3                # Карточек на поиск за запуск по умолчанию
    batch_delay_seconds: float = 30                  # Задержка между батчами поисков (сек)
    search_delay_seconds: float = 5                  # Задержка между поисками в батче (сек)

    # --- Хранение ---
    raw_html_path: str = "data/raw_html"

    # --- Логирование ---
    log_level: str = "INFO"
```

### Назначение параметров фильтрации аномалий

| Параметр | Назначение |
|---|---|
| `TRIM_PERCENT` | Доля отбрасываемых выбросов с каждого края распределения (5% = 0.05) |
| `IQR_MULTIPLIER` | Множитель k для расчёта IQR-границ: `[Q1 - k×IQR, Q3 + k×IQR]` |
| `TEMPORAL_WINDOW_DAYS` | Временное окно: в анализе участвуют только объявления не старше N дней |
| `MIN_SEGMENT_SIZE` | Минимальное количество объявлений в сегменте для статистической значимости |
| `UNDERVALUED_THRESHOLD` | Порог составного undervalue_score: если score > threshold → недооценён |
| `ZSCORE_THRESHOLD` | Порог Z-score: если z < -threshold → аномально дешёвый |
| `MEDIAN_DISCOUNT_THRESHOLD` | Порог % от медианы: если price < median × threshold → недооценён |

### Назначение параметров масштабирования

| Параметр | Назначение |
|---|---|
| `MAX_CONCURRENT_SEARCHES` | Количество параллельных поисков в одном батче (ограничение Semaphore) |
| `DEFAULT_SCHEDULE_INTERVAL_HOURS` | Интервал запуска по умолчанию для новых поисков в БД |
| `DEFAULT_MAX_ADS_TO_PARSE` | Карточек на поиск за запуск по умолчанию для новых поисков |
| `BATCH_DELAY_SECONDS` | Пауза между обработкой батчей поисков |
| `SEARCH_DELAY_SECONDS` | Пауза между запусками поисков внутри батча |

---

## 6. Алгоритм одного запуска

### Общий алгоритм `run_search_cycle()`

```
┌─────────────────────────────────────────────────────────────┐
│              START: Pipeline.run_search_cycle()               │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
                ┌──────────────────────────┐
                │ 1. Получить поиски,      │
                │    готовые к запуску     │
                │    (get_searches_due)    │
                └──────────┬───────────────┘
                           │
                ┌──────────┴───────────────┐
                │ Есть поиски для запуска? │
                └──────────┬───────────────┘
                     Нет   │   Да
                    ───────┘   │
                              ▼
                ┌──────────────────────────┐
                │ 2. Разбить на батчи по   │
                │    MAX_CONCURRENT_       │
                │    SEARCHES             │
                └──────────┬───────────────┘
                           │
              ┌────────────┴────────────┐
              │ ДЛЯ КАЖДОГО БАТЧА       │
              └────────────┬────────────┘
                           │
                           ▼
              ┌─────────────────────────────────────┐
              │ 3. Запустить поиски параллельно      │
              │    через asyncio.Semaphore            │
              │    с задержкой SEARCH_DELAY_SECONDS   │
              └────────────┬─────────────────────────┘
                           │
                           ▼
              ┌─────────────────────────────────────┐
              │ 4. Задержка BATCH_DELAY_SECONDS      │
              │    перед следующим батчем             │
              └────────────┬─────────────────────────┘
                           │
                           ▼
              ┌─────────────────────────────────────┐
              │ === Фаза анализа v2 ===               │
              │ 5. Для каждого поиска:                │
              │    a. get_ads_for_analysis()          │
              │    b. segment_ads()                   │
              │    c. Для каждого сегмента:           │
              │       - filter_iqr()                  │
              │       - calculate_market_stats()      │
              │       - detect_undervalued_v2()       │
              │    d. update_ad_analysis()             │
              └────────────┬─────────────────────────┘
                           │
                           ▼
              ┌─────────────────────────────────────┐
              │ === Фаза уведомлений ===              │
              │ 6. Для каждого undervalued ad:        │
              │    - Проверить is_notification_sent   │
              │    - Попытка отправить в Telegram     │
              │    - Fallback на Email (SMTP)         │
              │    - Записать notification_sent в БД  │
              └────────────┬─────────────────────────┘
                           │
                           ▼
              ┌─────────────────────────────────────┐
              │ 7. Обновить last_run_at для поисков   │
              │ 8. Вернуть статистику                 │
              └─────────────────────────────────────┘
```

### Обработка одного поискового запроса `_process_tracked_search()`

```
┌─────────────────────────────────────────────────────────────┐
│         _process_tracked_search(search)                       │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
               ┌─────────────────────────────┐
               │ 1. Случайная задержка        │
               │ 2. Открыть поисковую страницу│
               │ 3. Сохранить HTML на диск    │
               │ 4. Создать SearchRun в БД    │
               └────────────┬────────────────┘
                            │
                            ▼
               ┌─────────────────────────────────────┐
               │ 5.  Парсинг HTML → list[SearchResult]│
               │ 6. Получить recent_ad_ids из БД      │
               │ 7. Отфильтровать уже известные       │
               │ 8. Выбрать max_ads новых             │
               └────────────┬────────────────────────┘
                            │
               ┌────────────┴────────────┐
               │ ДЛЯ КАЖДОГО нового ad   │
               └────────────┬────────────┘
                            │
                            ▼
               ┌──────────────────────────────────────┐
               │ 9.  Случайная задержка                │
               │ 10. Открыть карточку объявления       │
               │ 11. Сохранить HTML на диск            │
               │ 12. Парсинг HTML → AdData             │
               │ 13. Сохранить/обновить Ad в БД        │
               │ 14. Сохранить AdSnapshot в БД         │
               └────────────┬─────────────────────────┘
                            │
                            ▼
               ┌──────────────────────────────────────┐
               │ 15. Обновить SearchRun: completed     │
               │ 16. Обновить search.last_run_at       │
               └──────────────────────────────────────┘
```

---

## 7. Поток данных

### Диаграмма потока данных при масштабированном поиске

```mermaid
flowchart TD
    A[Scheduler: каждые 5 мин] --> B[Pipeline.run_search_cycle]
    B --> C[Repository: get_searches_due_for_run]
    C --> D{Есть просроченные поиски?}
    D -- Нет --> E[Sleep 5 мин]
    D -- Да --> F[Разбить на батчи по MAX_CONCURRENT_SEARCHES]

    F --> G[Батч 1: asyncio.gather + Semaphore]
    F --> H[Батч 2: ...]
    F --> I[Батч N: ...]

    G --> J[_process_tracked_search]
    J --> K[Collector: Playwright]
    K --> L[HTML поисковых страниц]
    L --> M[SearchParser: извлечение ссылок]
    M --> N[Repository: дедупликация]
    N --> O[Collector: открытие карточек]
    O --> P[HTML карточек]
    P --> Q[AdParser: извлечение данных + seller_type]
    Q --> R[Repository: сохранение в PostgreSQL]

    R --> S[_analyze_and_notify_searches]
    S --> T[Repository: get_ads_for_analysis]
    T --> U[PriceAnalyzer: segment_ads]
    U --> V[Для каждого сегмента: filter_iqr + calculate_stats]
    V --> W[detect_undervalued_v2: составной score]
    W --> X[Repository: update_ad_analysis]
    X --> Y{Telegram доступен?}
    Y -->|Да| Z[TelegramNotifier]
    Y -->|Нет| AA[EmailNotifier: fallback]
    Z --> AB[Repository: notification_sent]
    AA --> AB
```

### Подробное описание потока

| Этап | Вход | Модуль | Выход | Хранилище |
|------|------|--------|-------|-----------|
| 1. Выбор поисков | `schedule_interval_hours`, `last_run_at` | `Repository.get_searches_due_for_run()` | `list[TrackedSearch]` | PostgreSQL |
| 2. Батчевая обработка | `list[TrackedSearch]` | `Pipeline` (Semaphore) | Результаты по поисков | — |
| 3. Сбор поисковой страницы | `search_url: str` | `AvitoCollector.collect_search_page()` | `html: str` | `data/raw_html/search/` |
| 4. Парсинг поиска | `html: str` | `parse_search_page()` | `list[SearchResult]` | — |
| 5. Дедупликация | `list[SearchResult]` | `Repository.get_recent_ad_ids()` | Отфильтрованный список | PostgreSQL |
| 6. Сбор карточки | `url: str` | `AvitoCollector.collect_ad_page()` | `html: str` | `data/raw_html/ad/` |
| 7. Парсинг карточки | `html: str` | `parse_ad_page()` | `AdData` (с `seller_type`) | — |
| 8. Сохранение данных | `AdData` | `Repository` | Записи в таблицах | PostgreSQL + диск |
| 9. Ценовой анализ v2 | `tracked_search_id` | `PriceAnalyzer` (сегментация, IQR, Z-score) | `list[UndervaluedResult]` | PostgreSQL |
| 10. Обновление анализа | `UndervaluedResult` | `Repository.update_ad_analysis()` | z_score, iqr_outlier, segment_key | PostgreSQL |
| 11. Уведомление | `UndervaluedResult` | `TelegramNotifier` / `EmailNotifier` | `telegram_message_id` или email | PostgreSQL + Telegram/SMTP |

---

## 8. Аналитический движок v2

### Обзор

Аналитический движок v2 в [`app/analysis/analyzer.py`](app/analysis/analyzer.py) реализует продвинутый статистический анализ цен с сегментацией, фильтрацией выбросов и составным критерием недооценённости.

### Архитектура анализа

```mermaid
flowchart TD
    A[Все объявления для tracked_search] --> B[Временной фильтр: TEMPORAL_WINDOW_DAYS]
    B --> C[Сегментация: condition + location + seller_type]
    C --> D{Размер сегмента >= MIN_SEGMENT_SIZE?}
    D -- Нет --> E[Объединение сегментов / fallback]
    E --> D
    D -- Да --> F[Trim: убрать TRIM_PERCENT дешёвых и дорогих]
    F --> G[IQR-фильтрация: IQR_MULTIPLIER]
    G --> H[Расчёт статистики: median, Q1, Q3, IQR, std, trimmed_mean]
    H --> I[Z-score для каждого объявления]
    I --> J[Составной undervalue_score]
    J --> K{score > UNDERVALUED_THRESHOLD?}
    K -- Да --> L[Объявление недооценено]
    K -- Нет --> M[Объявление в норме]
    L --> N[update_ad_analysis в БД]
    M --> N
```

### Сегментация

Объявления группируются по составному ключу `{condition}_{location}_{seller_type}`:

| Компонент | Источник | Примеры |
|---|---|---|
| condition | `Ad.condition` | «Новое», «Б/у», «Не указано» |
| location | `Ad.location` (первый компонент) | «Москва», «Екатеринбург» |
| seller_type | `Ad.seller_type` | «частное», «магазин», «компания» |

Пример ключа: `"Новое_Москва_частное"`

Если размер сегмента < `MIN_SEGMENT_SIZE`, сегмент объединяется с менее гранулярным.

### IQR-фильтрация

Метод [`filter_iqr()`](app/analysis/analyzer.py) удаляет статистические выбросы:

```
Q1 = percentile(prices, 25)
Q3 = percentile(prices, 75)
IQR = Q3 - Q1
Lower Fence = Q1 - IQR_MULTIPLIER × IQR
Upper Fence = Q3 + IQR_MULTIPLIER × IQR
Выброс: price < Lower Fence ИЛИ price > Upper Fence
```

### Z-score анализ

Метод [`calculate_zscore()`](app/analysis/analyzer.py) рассчитывает стандартное отклонение цены от среднего:

```
z = (price - μ) / σ
Аномалия: z < -ZSCORE_THRESHOLD
```

### Составной undervalue_score

Метод [`detect_undervalued_v2()`](app/analysis/analyzer.py) вычисляет взвешенный score:

```
iqr_score = max(0, (Q1 - price) / IQR)           если IQR > 0
zscore_score = max(0, -z_score / ZSCORE_THRESHOLD) если z_score < 0
median_score = max(0, (median - price) / median)  если price < median

undervalue_score = 0.4 × iqr_score + 0.3 × zscore_score + 0.3 × median_score
```

Объявление считается **недооценённым**, если:
- `undervalue_score > UNDERVALUED_THRESHOLD` (по умолчанию 0.3)
- И при этом `iqr_score > 0` ИЛИ `zscore_score > 0`

### Расширенная MarketStats

[`MarketStats`](app/analysis/analyzer.py) содержит расширенный набор метрик:

| Поле | Тип | Описание |
|------|-----|----------|
| `median_price` | `float` | Медиана цен |
| `mean_price` | `float` | Среднее цен |
| `q1_price` | `float` | Первый квартиль (25-й перцентиль) |
| `q3_price` | `float \| None` | Третий квартиль (75-й перцентиль) |
| `iqr` | `float \| None` | Межквартильный размах: Q3 - Q1 |
| `std_dev` | `float \| None` | Стандартное отклонение |
| `lower_fence` | `float \| None` | Нижняя граница IQR |
| `upper_fence` | `float \| None` | Верхняя граница IQR |
| `trimmed_mean` | `float \| None` | Среднее после удаления выбросов |
| `segment_key` | `str` | Ключ сегмента |
| `sample_size` | `int` | Размер выборки |

---

## 9. Архитектура планировщика

### Обзор

Двухуровневая архитектура: [`Scheduler`](app/scheduler/scheduler.py) → [`Pipeline`](app/scheduler/pipeline.py).

```mermaid
flowchart TD
    A[CLI: python -m app.main run] --> B[Scheduler.run]
    B --> C[Цикл: каждые 5 минут]
    C --> D[Pipeline.run_search_cycle]
    D --> E[Repository.get_searches_due_for_run]
    E --> F{Просроченные поиски?}
    F -- Да --> G[Батчевая обработка]
    F -- Нет --> H[Sleep 5 мин]
    G --> I[Анализ + уведомления]
    I --> H
    H --> C
```

### Scheduler

Класс [`Scheduler`](app/scheduler/scheduler.py:14) реализует циклический планировщик:

- **Интервал проверки**: 5 минут (3000 секунд)
- **Обработка**: делегирует [`Pipeline.run_search_cycle()`](app/scheduler/pipeline.py)
- **Отказоустойчивость**: исключения в одном цикле не прерывают работу
- **Остановка**: через `Scheduler.stop()` — текущий цикл завершается корректно

### Pipeline

Класс [`Pipeline`](app/scheduler/pipeline.py) реализует обработку поисков:

#### Батчевая обработка

Поиски разбиваются на батчи по `MAX_CONCURRENT_SEARCHES` и обрабатываются параллельно:

```
Батч 1: [Search 1, Search 2, Search 3]  ← asyncio.gather + Semaphore(3)
    ↓ BATCH_DELAY_SECONDS (30 сек)
Батч 2: [Search 4, Search 5, Search 6]
    ↓ BATCH_DELAY_SECONDS
...
```

Внутри каждого батча между запусками поисков задержка `SEARCH_DELAY_SECONDS` (5 сек).

#### Управление расписанием

Каждый `TrackedSearch` имеет:
- `schedule_interval_hours` — индивидуальный интервал запуска (по умолчанию 2 часа)
- `last_run_at` — время последнего запуска
- `priority` — приоритет (1–10, ниже = важнее)
- `max_ads_to_parse` — лимит карточек за запуск

Поиск считается готовым к запуску если:
```
last_run_at IS NULL
  ИЛИ
last_run_at + schedule_interval_hours <= NOW()
```

---

## 10. Обработка ошибок

### Стратегия

| Уровень | Обработка | Пример |
|---------|-----------|--------|
| **Отдельное объявление** | Логировать ошибку, продолжить со следующего | Не удалось распарсить цену |
| **Отдельный поиск** | Логировать ошибку, отметить SearchRun как failed, продолжить со следующего URL | Страница не загрузилась |
| **Батч поисков** | Ошибки отдельных поисков не прерывают батч | Один из 3 поисков упал |
| **Цикл планировщика** | Логировать ошибку, sleep до следующего цикла | Все поиски упали |
| **База данных** | Логировать и завершить цикл с ошибкой | Нет подключения к PostgreSQL |
| **Браузер** | Логировать и завершить цикл с ошибкой | Chromium не установлен |

### Кастомные исключения

Определены в [`app/utils/exceptions.py`](app/utils/exceptions.py):

```python
class AvitoMonitorError(Exception):
    """Базовое исключение системы."""
    pass

class CollectorError(AvitoMonitorError):
    """Ошибка при сборе HTML через Playwright."""
    pass

class ParserError(AvitoMonitorError):
    """Ошибка при парсинге HTML."""
    pass

class StorageError(AvitoMonitorError):
    """Ошибка при работе с БД."""
    pass

class NotificationError(AvitoMonitorError):
    """Ошибка при отправке уведомления."""
    pass
```

### IntegrityError в get_or_create_ad

[`Repository.get_or_create_ad()`](app/storage/repository.py) обрабатывает `IntegrityError` при конкурентной вставке: если два процесса одновременно пытаются создать одно объявление, второй получит уже существующую запись вместо ошибки.

---

## 11. Логирование

### Настройка

Используется `structlog` для структурированного логирования. Настройка в [`app/utils/helpers.py`](app/utils/helpers.py) через `setup_logging()`.

### Ключевые события для логирования

| Событие | Уровень | Поля |
|---------|---------|------|
| Старт планировщика | INFO | — |
| Начало цикла планировщика | INFO | — |
| Конец цикла планировщика | INFO | статистика цикла |
| Ошибка цикла планировщика | ERROR | error, exc_info |
| Открытие поисковой страницы | INFO | `search_url`, `delay_seconds` |
| Результат парсинга поиска | INFO | `search_url`, `ads_found`, `ads_new` |
| Открытие карточки | INFO | `ad_id`, `url` |
| Результат парсинга карточки | INFO | `ad_id`, `title`, `price` |
| Сегментация объявлений | INFO | `segment_count`, `ads_count` |
| Обнаружено undervalued v2 | WARNING | `ad_id`, `price`, `score`, `segment_key` |
| Уведомление отправлено | INFO | `ad_id`, `telegram_message_id` |
| Telegram недоступен, fallback на Email | WARNING | `error` |
| Ошибка парсинга объявления | ERROR | `ad_id`, `error` |
| Ошибка загрузки страницы | ERROR | `url`, `error` |
| Завершение цикла | INFO | `total_found`, `total_new`, `undervalued_count` |

---

## 12. CLI — точка входа

### `app/scheduler/cli.py`

CLI реализован на Typer в [`app/scheduler/cli.py`](app/scheduler/cli.py).

```python
import typer
import asyncio

app = typer.Typer(name="avito-monitor", help="Avito Monitor")

@app.command()
def run():
    """Запустить циклический планировщик (каждые 5 минут)."""
    settings = Settings()
    asyncio.run(_run_scheduler(settings))

@app.command()
def run_once():
    """Запустить один цикл сбора и анализа."""
    settings = Settings()
    asyncio.run(_run_once(settings))

@app.command()
def init_db():
    """Инициализировать базу данных — создать таблицы + миграции."""
    settings = Settings()
    asyncio.run(_init_db(settings))

@app.command()
def test_telegram():
    """Проверить подключение к Telegram."""
    settings = Settings()
    asyncio.run(_test_telegram(settings))
```

### Вспомогательные функции CLI

```python
async def _ensure_tables(settings: Settings) -> None:
    """Создать таблицы, если они не существуют."""
    ...

async def _seed_searches(settings: Settings) -> None:
    """Заполнить поисковые запросы из SEARCH_URLS в БД."""
    ...
```

### Запуск

```bash
# Циклический планировщик
python -m app.main run

# Один цикл
python -m app.main run-once

# Инициализация БД
python -m app.main init-db

# Проверка Telegram
python -m app.main test-telegram
```

---

## 13. Скрипты утилиты

### `scripts/init_db.py` — Инициализация БД + миграции

Создаёт таблицы и выполняет миграции новых колонок в существующие таблицы PostgreSQL.

Функция `_migrate_existing_db()` добавляет колонки, добавленные в v2:
- `tracked_searches`: `schedule_interval_hours`, `last_run_at`, `priority`, `max_ads_to_parse`
- `ads`: `seller_type`, `z_score`, `iqr_outlier`, `segment_key`

Миграция идемпотентна: использует `IF NOT EXISTS` для каждой колонки.

### `scripts/seed_searches.py` — Заполнение поисковых запросов

Читает `SEARCH_URLS` из `.env` и создаёт записи `TrackedSearch` в БД для каждого URL. Использует `get_or_create_tracked_search()` для дедупликации.

### `scripts/cleanup_duplicates.py` — Очистка дубликатов

Удаляет дубликаты объявлений из таблицы `ads`, оставляя самую свежую запись для каждого `avito_id`.

---

## 14. Зависимости

### `requirements.txt`

```
playwright>=1.40.0
beautifulsoup4>=4.12.0
lxml>=5.0.0
sqlalchemy[asyncio]>=2.0.0
asyncpg>=0.29.0
pydantic-settings>=2.0.0
httpx>=0.25.0
typer>=0.9.0
structlog>=23.0.0
python-dotenv>=1.0.0
aiosmtplib>=3.0.0
numpy>=1.24.0
```

### Установка Playwright

```bash
pip install playwright
playwright install chromium
```

---

## 15. Ограничения и допущения

### Текущие ограничения

1. **Нет пагинации:** обрабатывается только первая страница поиска
2. **Нет авторизации:** не обрабатываются объявления требующие входа
3. **Нет ротации прокси:** `USE_PROXY = False` по умолчанию
4. **Нет CAPTCHA-handling:** при появлении CAPTCHA цикл завершится с ошибкой
5. **Миграции без Alembic:** миграции выполняются скриптом `init_db.py` с `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`

### Допущения

1. Селекторы Avito могут меняться — для этого предусмотрен fallback-механизм в парсерах
2. Формат URL объявлений: `https://www.avito.ru/{city}/{category}/{title}-{ad_id}`
3. Цена указана в рублях, целое число
4. Достаточно одной инстанции PostgreSQL на локальной машине
5. Планировщик работает в одном процессе (нет горизонтального масштабирования)

### Точки расширения

| Что | Как расширить |
|-----|---------------|
| Прокси | Установить `USE_PROXY=True`, указать `PROXY_URL` — `AvitoCollector` автоматически использует его |
| Пагинация | Добавить параметр `?page=N` к URL, цикл по страницам в `_process_search` |
| Новый канал уведомлений | Реализовать новый класс с методом `send_undervalued_notification` |
| ML-анализ | Расширить `PriceAnalyzer` или создать новый класс рядом |
| Больше поисков | Добавить через `seed_searches.py` или напрямую в БД |
| Alembic | Настроить Alembic для полноценного управления миграциями |

---

## 16. Ключевые паттерны

### 16.1 Разделение сырых HTML и структурированных данных

Система строго разделяет два слоя хранения:

| Слой | Хранилище | Содержимое | Назначение |
|------|-----------|------------|------------|
| Сырые данные | Файловая система `data/raw_html/` | Полный HTML-код страниц | Возможность перепарсинга без повторного сбора |
| Структурированные данные | PostgreSQL | Извлечённые поля, метаданные | Анализ, поиск, уведомления |

**Преимущества:**
- Можно перепарсить HTML без обращения к Avito
- Можно отладить парсер на сохранённых файлах
- Исторические данные не теряются при изменении селекторов

### 16.2 Дедупликация объявлений

**Уникальность объявления** определяется по `ad_id` (числовой ID из URL Avito).

**Алгоритм дедупликации при обработке поиска:**

1. `parse_search_page()` извлекает `list[SearchResult]` с `ad_id` для каждого
2. `Repository.get_recent_ad_ids(search_url, hours=24)` возвращает множество `ad_id`
3. Фильтрация: `new_results = [r for r in search_results if r.ad_id not in recent_ids]`
4. Ограничение: `new_results = new_results[:max_ads_per_search_per_run]`

**Статусы объявления:**

| `parse_status` | Значение |
|-----------------|----------|
| `pending` | Объявление обнаружено, карточка ещё не открывалась |
| `parsed` | Карточка открыта и успешно распарсена |
| `failed` | При парсинге произошла ошибка (см. `last_error`) |
| `skipped` | Объявление пропущено — например, уже проверялось |

### 16.3 Нормализация цены

Цена всегда хранится в БД как `INTEGER` (рубли, без копеек). Функция `normalize_price()` обрабатывает все варианты формата Avito:

- Удаляет неразрывные пробелы (`\xa0`)
- Удаляет обычные пробелы и запятые
- Удаляет символы `₽`, `руб.`
- Извлекает числовую часть через regex
- Возвращает `int` или `None`

### 16.4 Дедупликация уведомлений

Таблица `notifications_sent` имеет `UNIQUE` индекс на `(ad_id, notification_type)`. Это гарантирует:

- Уведомление типа `undervalued` отправляется **не более одного раза** для каждого объявления
- Перед отправкой проверяется `Repository.is_notification_sent(ad_id, notification_type)`
- После успешной отправки записывается `Repository.mark_notification_sent(ad_id, type, msg_id)`
- Повторный запуск цикла не вызовет дублирование уведомлений

### 16.5 Сегментация рынка

Объявления сегментируются перед анализом для корректного сравнения:

- «Новый iPhone в Москве» сравнивается только с «новыми iPhone в Москве от частных продавцов»
- Ключ сегмента: `{condition}_{location}_{seller_type}`
- Если сегмент слишком мал (< `MIN_SEGMENT_SIZE`), происходит объединение

### 16.6 Пакетная обработка с Semaphore

Пайплайн использует `asyncio.Semaphore` для ограничения параллельности:

- `MAX_CONCURRENT_SEARCHES` — максимальное количество одновременных поисков
- Между батчами задержка `BATCH_DELAY_SECONDS`
- Между поисками внутри батча задержка `SEARCH_DELAY_SECONDS`
- Ошибка одного поиска не прерывает обработку батча

### 16.7 Идемпотентные миграции

Скрипт [`scripts/init_db.py`](scripts/init_db.py) выполняет миграции через `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, что обеспечивает:

- Безопасный повторный запуск
- Добавление новых колонок без потери данных
- Совместимость с существующими инсталляциями
