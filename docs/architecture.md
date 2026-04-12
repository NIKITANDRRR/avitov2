# Архитектура PoC системы мониторинга Avito

> **Версия:** 1.1
> **Статус:** Draft
> **Дата:** 2026-04-10

---

## Содержание

1. [Обзор системы](#1-обзор-системы)
2. [Структура проекта](#2-структура-проекта)
3. [Модели данных PostgreSQL](#3-модели-данных-postgresql)
4. [Интерфейсы модулей](#4-интерфейсы-модулей)
5. [Конфигурация](#5-конфигурация)
6. [Алгоритм одного запуска](#6-алгоритм-одного-запуска)
7. [Поток данных](#7-поток-данных)
8. [Обработка ошибок](#8-обработка-ошибок)
9. [Логирование](#9-логирование)
10. [CLI — точка входа](#10-cli--точка-входа)
11. [Зависимости](#11-зависимости)
12. [Ограничения и допущения](#12-ограничения-и-допущения)
13. [Ключевые паттерны](#13-ключевые-паттерны)

---

## 1. Обзор системы

PoC/MVP система для мониторинга объявлений Avito с целью поиска товаров ниже рыночной цены. Система работает в **однопоточном режиме** с **больши́ми случайными задержками**, используя Playwright + Chromium для сбора данных.

### Принципы

- **Low-traffic:** максимум 3 поисковых URL, 2–3 карточки на поиск, не более 6–9 объявлений за запуск
- **Headful по умолчанию:** на этапе PoC используется видимый браузер
- **Без API Avito:** только веб-скрапинг через Playwright
- **Без прокси:** но архитектура готова к добавлению
- **Одноразовый запуск:** нет фоновых процессов, один цикл — один запуск

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

---

## 2. Структура проекта

```
avito/
├── app/
│   ├── __init__.py
│   ├── main.py                     # Точка входа CLI
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py             # Pydantic Settings
│   ├── collector/
│   │   ├── __init__.py
│   │   └── browser.py              # Playwright collector
│   ├── parser/
│   │   ├── __init__.py
│   │   ├── search_parser.py        # Парсер поисковых страниц
│   │   ├── ad_parser.py            # Парсер карточек объявлений
│   │   └── selectors.py            # CSS-селекторы Avito с fallback
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── database.py             # Подключение к БД, session manager
│   │   ├── models.py               # SQLAlchemy ORM-модели
│   │   └── repository.py           # Repository pattern — CRUD операции
│   ├── analysis/
│   │   ├── __init__.py
│   │   └── price_analyzer.py       # Ценовой анализ
│   ├── notifier/
│   │   ├── __init__.py
│   │   ├── telegram_notifier.py    # Telegram уведомления
│   │   └── email_notifier.py       # Email уведомления (fallback)
│   ├── scheduler/
│   │   ├── __init__.py
│   │   └── cycle.py                # Оркестрация одного цикла
│   └── utils/
│       ├── __init__.py
│       ├── html_storage.py         # Сохранение/чтение HTML файлов
│       └── delays.py               # Случайные задержки
├── data/
│   └── raw_html/
│       ├── search/                 # HTML поисковых страниц
│       └── ad/                     # HTML карточек объявлений
├── scripts/
│   └── init_db.py                  # Инициализация БД — создание таблиц
├── tests/
│   ├── __init__.py
│   ├── test_parser.py
│   ├── test_price_analyzer.py
│   ├── test_repository.py
│   └── fixtures/
│       └── *.html                  # Тестовые HTML-файлы
├── docs/
│   └── architecture.md             # Этот файл
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
| `app/storage/`     | Модели БД, подключение, CRUD-операции                    |
| `app/analysis/`    | Расчёт рыночных цен, определение заниженных               |
| `app/notifier/`    | Отправка уведомлений в Telegram и Email (fallback)        |
| `app/scheduler/`   | Оркестрация полного цикла сбора и анализа                 |
| `app/utils/`       | Общие утилиты — HTML-хранилище, задержки                 |
| `data/raw_html/`   | Сырые HTML-файлы, организованные по типу страницы         |
| `scripts/`         | Скрипты администрирования                                 |
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
    id              BIGSERIAL       PRIMARY KEY,
    search_url      VARCHAR(1024)   NOT NULL UNIQUE,
    search_phrase   VARCHAR(512)    NOT NULL,          -- Описание/название запроса
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tracked_searches_is_active ON tracked_searches (is_active);
```

#### `search_runs` — Запуски сбора

```sql
CREATE TABLE search_runs (
    id                  BIGSERIAL       PRIMARY KEY,
    tracked_search_id   BIGINT          NOT NULL REFERENCES tracked_searches(id) ON DELETE CASCADE,
    started_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    pages_fetched       INTEGER         NOT NULL DEFAULT 0,    -- Сколько страниц пагинации загружено
    ads_found           INTEGER         NOT NULL DEFAULT 0,    -- Всего найдено в выдаче
    ads_new             INTEGER         NOT NULL DEFAULT 0,    -- Новых, ранее не известных
    ads_opened          INTEGER         NOT NULL DEFAULT 0,    -- Сколько карточек открыто
    errors_count        INTEGER         NOT NULL DEFAULT 0,    -- Количество ошибок при обработке
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
    ad_id               BIGINT          NOT NULL UNIQUE,       -- Уникальный ID объявления Avito
    url                 VARCHAR(1024)   NOT NULL,
    title               VARCHAR(512),
    price               INTEGER,                                -- Цена в рублях (INTEGER, нормализованная)
    location            VARCHAR(256),
    seller_name         VARCHAR(256),
    condition           VARCHAR(64),                            -- new | used | и т.д.
    publication_date    TIMESTAMPTZ,
    search_url          VARCHAR(1024)   NOT NULL,               -- URL поиска, откуда найдено
    tracked_search_id   BIGINT          NOT NULL REFERENCES tracked_searches(id) ON DELETE CASCADE,
    first_seen_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    last_scraped_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    parse_status        VARCHAR(32)     NOT NULL DEFAULT 'pending',  -- pending | parsed | failed | skipped
    last_error          TEXT,                                    -- Последняя ошибка парсинга/сбора
    is_undervalued      BOOLEAN         NOT NULL DEFAULT FALSE,
    undervalue_score    FLOAT                                   -- Отклонение от медианы: price / median_price
);

CREATE INDEX idx_ads_ad_id ON ads (ad_id);
CREATE INDEX idx_ads_search_url ON ads (search_url);
CREATE INDEX idx_ads_is_undervalued ON ads (is_undervalued);
CREATE INDEX idx_ads_first_seen_at ON ads (first_seen_at);
CREATE INDEX idx_ads_tracked_search_id ON ads (tracked_search_id);
CREATE INDEX idx_ads_parse_status ON ads (parse_status);
```

#### `ad_snapshots` — Снимки цен

```sql
CREATE TABLE ad_snapshots (
    id          BIGSERIAL       PRIMARY KEY,
    ad_id       BIGINT          NOT NULL REFERENCES ads(id) ON DELETE CASCADE,
    price       INTEGER         NOT NULL,
    scraped_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    html_path   VARCHAR(512)    NOT NULL               -- Путь к файлу на диске
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
    telegram_message_id     BIGINT                                  -- ID сообщения в Telegram
);

CREATE INDEX idx_notifications_sent_ad_id ON notifications_sent (ad_id);
CREATE UNIQUE INDEX idx_notifications_sent_unique ON notifications_sent (ad_id, notification_type);
```

### SQLAlchemy-модели — расположение

Модели определяются в [`app/storage/models.py`](app/storage/models.py) с использованием `DeclarativeBase` и `MappedAsColumn` из SQLAlchemy 2.0. Все модели используют аннотации типов `Mapped[type]`.

---

## 4. Интерфейсы модулей

### 4.1 Data Classes / DTO

Определяются в соответствующих модулях или в `app/models.py` при необходимости.

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
    condition: str | None = None
    publication_date: datetime | None = None
    search_url: str | None = None

@dataclass
class MarketStats:
    """Статистика рынка для поискового запроса."""
    search_url: str
    median_price: float
    mean_price: float
    q1_price: float                    # Первый квартиль
    sample_size: int                   -- Размер выборки
    calculated_at: datetime = field(default_factory=datetime.now)

@dataclass
class UndervaluedAd:
    """Объявление ниже рыночной цены."""
    ad: AdData
    stats: MarketStats
    undervalue_score: float            # price / median_price
    price_diff_percent: float          # Отклонение в %
```

### 4.2 Collector — `app/collector/browser.py`

```python
from playwright.async_api import async_playwright, Browser, Page

class AvitoCollector:
    """Сборщик HTML-страниц Avito через Playwright."""

    def __init__(self, settings: "AppSettings") -> None:
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

        Алгоритм:
        1. Случайная задержка перед открытием
        2. Создание нового контекста со случайным User-Agent
        3. Навигация по URL
        4. Ожидание загрузки основных данных (price, title)
        5. Получение HTML через page.content()
        6. Закрытие контекста

        Args:
            url: URL карточки объявления

        Returns:
            Полный HTML карточки в виде строки

        Raises:
            CollectorError: при ошибке загрузки страницы
        """
        ...
```

**Примечание по User-Agent:** Используется список популярных десктопных User-Agent. При каждом вызове выбирается случайный.

**Примечание по прокси:** В `AvitoCollector.__init__` проверяется `settings.USE_PROXY`. Если `True`, браузерный контекст создаётся с параметром `proxy={"server": settings.PROXY_URL}`. По умолчанию `False`.

### 4.3 Parser — `app/parser/`

#### `app/parser/selectors.py`

```python
"""
CSS-селекторы для парсинга страниц Avito.

Каждый селектор определяется как список fallback-вариантов.
Парсер пробует селекторы по порядку и берёт результат первого совпадения.
"""

SEARCH_RESULT_ITEMS: list[str] = [
    "[data-marker='item']",
    "div[itemtype='http://schema.org/Product']",
    "div.iva-item-root--",
]

SEARCH_RESULT_TITLE: list[str] = [
    "[itemprop='name']",
    "a.iva-item-root--",
]

SEARCH_RESULT_PRICE: list[str] = [
    "[itemprop='price']",
    "span.price-text-",
]

SEARCH_RESULT_URL: list[str] = [
    "a[itemprop='url']",
    "a.iva-item-root--",
]

SEARCH_RESULT_LOCATION: list[str] = [
    "[class*='geo']",
]

AD_TITLE: list[str] = [
    "h1.title-root-",
    "h1[data-marker='item-view/title-info']",
]

AD_PRICE: list[str] = [
    "span.js-item-price",
    "[itemprop='price']",
    "span.price-text-",
]

AD_LOCATION: list[str] = [
    "[class*='style-item-address__content']",
    "[data-marker='item-view/item-address']",
]

AD_SELLER_NAME: list[str] = [
    "[data-marker='seller-info/name']",
    "[class*='style-seller-info-name']",
]

AD_CONDITION: list[str] = [
    "[class*='params-paramsList'] li",
]

AD_PUBLICATION_DATE: list[str] = [
    "[data-marker='item-view/item-date']",
    "[class*='style-item-header-date']",
]
```

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

    Args:
        url: URL объявления

    Returns:
        Числовой ID или None если не удалось извлечь
    """
    ...
```

#### `app/parser/ad_parser.py`

```python
from bs4 import BeautifulSoup
import re

def normalize_price(price_text: str | None) -> int | None:
    """
    Нормализация цены из текста Avito в целое число.

    Алгоритм:
    1. Если price_text is None — вернуть None
    2. Удалить символы: пробелы, неразрывные пробелы (\xa0), символ ₽, запятые
    3. Извлечь числовую часть через regex: r'[\d]+'
    4. Объединить все найденные группы цифр в одно число
    5. Привести к int

    Примеры:
        "12 345 ₽" → 12345
        "1\xa0234" → 1234
        "Цена: 50 000 руб." → 50000
        None → None
        "" → None

    Args:
        price_text: сырой текст цены из HTML

    Returns:
        Цена как int или None
    """
    if not price_text:
        return None
    cleaned = price_text.replace("\xa0", "").replace(" ", "").replace(",", "")
    digits = re.findall(r"\d+", cleaned)
    if not digits:
        return None
    return int("".join(digits))

def parse_ad_page(html: str, url: str) -> AdData:
    """
    Парсинг HTML карточки объявления Avito.

    Алгоритм:
    1. Создать BeautifulSoup(html, 'lxml')
    2. Извлечь поля по fallback-селекторам из selectors.py
    3. Цена: извлечь текст, пропустить через normalize_price()
    4. Дата публикации: распарсить относительную дату — "сегодня", "вчера" и т.д.
    5. Заполнить AdData, отсутствующие поля = None

    Args:
        html: HTML-код карточки объявления
        url: URL объявления

    Returns:
        AdData с извлечёнными полями. Поля могут быть None.

    Raises:
        ParserError: если не удалось извлечь критичные поля (ad_id)
    """
    ...
```

### 4.4 Storage — `app/storage/`

#### `app/storage/database.py`

```python
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

def create_engine(database_url: str):
    """
    Создать async SQLAlchemy engine.

    Args:
        database_url: PostgreSQL DSN, например:
            postgresql+asyncpg://user:pass@localhost:5432/avito_monitor

    Returns:
        AsyncEngine
    """
    ...

def create_session_factory(engine) -> async_sessionmaker[AsyncSession]:
    """
    Создать фабрику async-сессий.

    Returns:
        async_sessionmaker[AsyncSession]
    """
    ...

async def init_db(engine) -> None:
    """
    Создать все таблицы по моделям SQLAlchemy.
    Используется в scripts/init_db.py.
    """
    ...
```

#### `app/storage/models.py`

SQLAlchemy 2.0 ORM-модели, соответствующие таблицам из раздела 3. Используют `Mapped[type]` и `mapped_column`. Модельный класс `Base` с `DeclarativeBase`.

Модели:
- `TrackedSearch` → `tracked_searches`
- `SearchRun` → `search_runs`
- `Ad` → `ads`
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
        """
        Завершить запуск — установить completed_at, метрики, status='completed'.

        Args:
            run_id: ID запуска
            ads_found: всего найдено в выдаче
            ads_new: новых, ранее не известных
            pages_fetched: сколько страниц пагинации загружено
            ads_opened: сколько карточек открыто
            errors_count: количество ошибок при обработке
        """
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

        Returns:
            Кортеж (модель Ad, флаг created_new)
        """
        ...

    async def update_ad_data(self, ad_id: int, data: AdData) -> None:
        """
        Обновить данные объявления из AdData.

        Также устанавливает:
        - parse_status = 'parsed' при успехе
        - last_scraped_at = NOW()
        - last_error = None
        """
        ...

    async def mark_ad_parse_failed(self, ad_id: int, error: str) -> None:
        """
        Отметить объявление как failed при парсинге.

        Устанавливает:
        - parse_status = 'failed'
        - last_error = error
        - last_scraped_at = NOW()
        """
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

        Используется для дедупликации — не открывать повторно.
        """
        ...

    async def get_ads_for_search(
        self, search_url: str
    ) -> list[Ad]:
        """Получить все объявления для данного поискового запроса."""
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

### 4.5 Analysis — `app/analysis/price_analyzer.py`

```python
import statistics

class PriceAnalyzer:
    """Анализатор рыночных цен."""

    def __init__(self, repository: Repository, threshold: float = 0.8) -> None:
        """
        Args:
            repository: репозиторий для доступа к данным
            threshold: порог определения undervalued (price < median * threshold)
        """
        ...

    async def calculate_market_stats(
        self, search_url: str
    ) -> MarketStats | None:
        """
        Рассчитать статистику рынка для поискового запроса.

        Алгоритм:
        1. Получить все ads для search_url через repository
        2. Отфильтровать объявления с price is not None
        3. Если sample_size < 3 — вернуть None (недостаточно данных)
        4. Рассчитать: median_price, mean_price, q1_price
        5. Вернуть MarketStats

        Args:
            search_url: URL поискового запроса

        Returns:
            MarketStats или None если недостаточно данных
        """
        ...

    async def detect_undervalued(
        self, search_url: str
    ) -> list[UndervaluedAd]:
        """
        Найти объявления ниже рыночной цены.

        Алгоритм:
        1. Рассчитать MarketStats для search_url
        2. Если stats is None — вернуть пустой список
        3. Получить все ads для search_url
        4. Для каждого ad:
           - score = ad.price / stats.median_price
           - если score < threshold — это undervalued
           - price_diff_percent = (1 - score) * 100
        5. Вернуть список UndervaluedAd

        Args:
            search_url: URL поискового запроса

        Returns:
            Список UndervaluedAd, отсортированный по score (самые дешёвые первыми)
        """
        ...
```

### 4.6 Notifier — `app/notifier/`

#### `app/notifier/telegram_notifier.py`

```python
import httpx

class TelegramNotifier:
    """Отправка уведомлений в Telegram."""

    def __init__(self, bot_token: str, chat_id: str) -> None:
        """
        Args:
            bot_token: токен Telegram бота
            chat_id: ID чата/канала для отправки
        """
        ...

    async def send_undervalued_notification(
        self, ad: AdData, stats: MarketStats, undervalue_score: float
    ) -> int | None:
        """
        Отправить уведомление о заниженном объявлении.

        Формат сообщения:
        ```
        🔔 Товар ниже рынка!

        📦 {title}
        💰 Цена: {price:,} ₽
        📊 Медиана: {median_price:,.0f} ₽
        📉 Отклонение: {diff_percent:.1f}%
        📍 {location}
        🔗 {url}
        🕐 Обнаружено: {first_seen_at}
        ```

        Args:
            ad: данные объявления
            stats: статистика рынка
            undervalue_score: отношение цены к медиане

        Returns:
            telegram_message_id при успехе, None при ошибке
        """
        ...

    async def send_cycle_summary(
        self, total_ads: int, new_ads: int, undervalued_count: int
    ) -> None:
        """
        Отправить краткое резюме цикла сбора.

        Формат:
        ```
        ✅ Цикл завершён
        Обработано поисков: {search_count}
        Найдено объявлений: {total_ads}
        Новых: {new_ads}
        Ниже рынка: {undervalued_count}
        ```
        """
        ...
```

**Реализация:** Используется `httpx.AsyncClient` для прямых HTTP-запросов к Telegram Bot API. Это проще и легче, чем `python-telegram-bot`, для простых уведомлений.

#### `app/notifier/email_notifier.py`

```python
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class EmailNotifier:
    """Резервный email-нотификатор (fallback при недоступности Telegram)."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        smtp_user: str,
        smtp_password: str,
        email_from: str,
        email_to: list[str],
        use_tls: bool = True,
    ) -> None:
        """
        Args:
            smtp_host: SMTP сервер (например, smtp.gmail.com)
            smtp_port: Порт SMTP (обычно 587 для TLS)
            smtp_user: Логин SMTP
            smtp_password: Пароль SMTP (для Gmail — пароль приложения)
            email_from: Email отправителя
            email_to: Список email получателей
            use_tls: Использовать TLS (по умолчанию True)
        """
        ...

    async def send_notification(self, subject: str, body: str) -> bool:
        """
        Отправить email-уведомление.

        Args:
            subject: Тема письма
            body: Тело письма (HTML)

        Returns:
            True при успехе, False при ошибке
        """
        ...

    async def send_undervalued_notifications(
        self, undervalued_ads: list[UndervaluedAd]
    ) -> int:
        """
        Отправить уведомления о всех недооценённых товарах по email.

        Args:
            undervalued_ads: список недооценённых объявлений

        Returns:
            Количество успешно отправленных уведомлений
        """
        ...
```

**Реализация:** Используется `aiosmtplib` для асинхронной отправки email через SMTP. Является fallback-каналом: вызывается только если Telegram недоступен или вернул ошибку.

### 4.7 Scheduler — `app/scheduler/cycle.py`

```python
class CycleOrchestrator:
    """Оркестратор одного цикла сбора и анализа."""

    def __init__(
        self,
        settings: "AppSettings",
        collector: AvitoCollector,
        repository: Repository,
        price_analyzer: PriceAnalyzer,
        notifier: TelegramNotifier,
        email_notifier: EmailNotifier,
        html_storage: HtmlStorage,
    ) -> None:
        ...

    async def run_single_cycle(self) -> None:
        """
        Выполнить один полный цикл сбора и анализа.

        См. раздел 6 — Алгоритм одного запуска.
        """
        ...
```

### 4.8 Utils

#### `app/utils/html_storage.py`

```python
class HtmlStorage:
    """Сохранение и чтение HTML-файлов на диске."""

    def __init__(self, base_path: str = "data/raw_html") -> None:
        """
        Args:
            base_path: корневая директория для HTML-файлов
        """
        ...

    def save_search_html(self, html: str, search_url: str) -> str:
        """
        Сохранить HTML поисковой страницы.

        Имя файла: search/{search_url_hash}_{timestamp}.html

        Args:
            html: HTML-код страницы
            search_url: URL для генерации хеша

        Returns:
            Относительный путь к сохранённому файлу
        """
        ...

    def save_ad_html(self, html: str, ad_id: int) -> str:
        """
        Сохранить HTML карточки объявления.

        Имя файла: ad/{ad_id}_{timestamp}.html

        Args:
            html: HTML-код карточки
            ad_id: ID объявления

        Returns:
            Относительный путь к сохранённому файлу
        """
        ...
```

#### `app/utils/delays.py`

```python
import asyncio
import random

async def random_delay(
    min_seconds: float = 5.0,
    max_seconds: float = 15.0
) -> None:
    """
    Асинхронная случайная задержка.

    Args:
        min_seconds: минимальная задержка в секундах
        max_seconds: максимальная задержка в секундах
    """
    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)

def get_random_user_agent() -> str:
    """
    Вернуть случайный User-Agent из списка популярных десктопных браузеров.

    Returns:
        Строка User-Agent
    """
    ...
```

---

## 5. Конфигурация

### `app/config/settings.py`

```python
from pydantic_settings import BaseSettings, SettingsConfigDict

class AppSettings(BaseSettings):
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
    headless: bool = False                           # Headless-режим (False = видимый браузер)
    use_proxy: bool = False                          # Использовать прокси
    proxy_url: str | None = None                     # URL прокси-сервера

    # --- Telegram ---
    telegram_bot_token: str                          # Токен Telegram бота
    telegram_chat_id: str                            # ID чата для уведомлений

    # --- Email (fallback) ---
    smtp_host: str | None = None                     # SMTP сервер (например, smtp.gmail.com)
    smtp_port: int = 587                             # Порт SMTP
    smtp_user: str | None = None                     # Логин SMTP
    smtp_password: str | None = None                 # Пароль SMTP (для Gmail — пароль приложения)
    smtp_use_tls: bool = True                        # Использовать TLS
    email_from: str | None = None                    # Email отправителя
    email_to: str | None = None                      # Email получателей (через запятую)

    # --- База данных ---
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/avito_monitor"

    # --- Анализ цен ---
    undervalue_threshold: float = 0.8                # Порог: price < median * threshold → undervalued

    # --- Хранение ---
    raw_html_path: str = "data/raw_html"             # Путь к директории с HTML-файлами

    # --- Логирование ---
    log_level: str = "INFO"                          # Уровень логирования
```

### `.env.example`

```env
# Поисковые запросы (через запятую)
SEARCH_URLS=https://www.avito.ru/rossiya/telefony?q=iphone+15,https://www.avito.ru/rossiya/noutbuki?q=macbook
SEARCH_PHRASES=iPhone 15,MacBook

# Лимиты
MAX_SEARCH_PAGES_PER_RUN=3
MAX_ADS_PER_SEARCH_PER_RUN=3

# Задержки (секунды)
MIN_DELAY_SECONDS=5.0
MAX_DELAY_SECONDS=15.0
STARTUP_DELAY_MIN=0
STARTUP_DELAY_MAX=30

# Браузер
HEADLESS=false
USE_PROXY=false
PROXY_URL=

# Telegram
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here

# Email (fallback-уведомления)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password
SMTP_USE_TLS=true
EMAIL_FROM=your_email@gmail.com
EMAIL_TO=recipient1@example.com,recipient2@example.com

# База данных
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/avito_monitor

# Анализ
UNDERVALUE_THRESHOLD=0.8

# Хранение
RAW_HTML_PATH=data/raw_html

# Логирование
LOG_LEVEL=INFO
```

---

## 6. Алгоритм одного запуска

### Общий алгоритм `run_single_cycle()`

```
┌─────────────────────────────────────────────────────────────┐
│                    START: run_single_cycle                   │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │ 1. Загрузить конфиг  │
                │ 2. Случайная задержка │
                │    перед стартом      │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │ 3. Подключиться к БД │
                │ 4. Запустить браузер │
                └──────────┬───────────┘
                           │
              ┌────────────┴────────────┐
              │  ДЛЯ КАЖДОГО search_url │
              │  из search_urls         │
              └────────────┬────────────┘
                           │
                           ▼
              ┌─────────────────────────────┐
              │ 5. Случайная задержка        │
              │ 6. Открыть поисковую страницу│
              │ 7. Сохранить HTML на диск    │
              │ 8. Создать SearchRun в БД    │
              └────────────┬────────────────┘
                           │
                           ▼
              ┌─────────────────────────────────────┐
              │ 9.  Парсинг HTML → list[SearchResult]│
              │ 10. Получить recent_ad_ids из БД     │
              │ 11. Отфильтровать уже известные      │
              │ 12. Выбрать max_ads новых            │
              └────────────┬────────────────────────┘
                           │
              ┌────────────┴────────────┐
              │ ДЛЯ КАЖДОГО нового ad   │
              └────────────┬────────────┘
                           │
                           ▼
              ┌──────────────────────────────────────┐
              │ 13. Случайная задержка                │
              │ 14. Открыть карточку объявления       │
              │ 15. Сохранить HTML на диск            │
              │ 16. Парсинг HTML → AdData             │
              │ 17. Сохранить/обновить Ad в БД        │
              │ 18. Сохранить AdSnapshot в БД         │
              └────────────┬─────────────────────────┘
                           │
                           ▼
              ┌──────────────────────────────────────┐
              │ 19. Обновить SearchRun: completed     │
              │     ads_found, ads_new                │
              └────────────┬─────────────────────────┘
                           │
                           ▼
              ┌──────────────────────────────────────┐
              │ === Фаза анализа ===                  │
              │ 20. Для каждого search_url:           │
              │     - calculate_market_stats()        │
              │     - detect_undervalued()            │
              │ 21. Пометить undervalued ads в БД     │
              └────────────┬─────────────────────────┘
                           │
                           ▼
               ┌──────────────────────────────────────┐
               │ === Фаза уведомлений ===              │
               │ 22. Для каждого undervalued ad:       │
               │     - Проверить is_notification_sent  │
               │     - Попытка отправить в Telegram    │
               │     - Если Telegram недоступен —      │
               │       fallback на Email (SMTP)        │
               │     - Записать notification_sent в БД │
               └────────────┬─────────────────────────┘
                            │
                            ▼
               ┌──────────────────────────────────────┐
               │ 23. Отправить summary в Telegram      │
               │     (или fallback на Email)           │
               │ 24. Закрыть браузер                   │
               │ 25. Закрыть подключение к БД          │
               │ 26. Записать результаты в лог         │
              └────────────┬─────────────────────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │       END            │
                └──────────────────────┘
```

### Псевдокод

```python
async def run_single_cycle(self) -> None:
    logger.info("Starting collection cycle")

    # 1. Startup delay
    await random_delay(self.settings.startup_delay_min, self.settings.startup_delay_max)

    # 2. Init
    await self.collector.start()
    total_new = 0
    total_found = 0

    # 3. For each search URL
    for search_url in self.settings.search_urls:
        try:
            await self._process_search(search_url)
        except Exception as e:
            logger.error("Failed to process search", search_url=search_url, error=str(e))

    # 4. Analysis phase
    all_undervalued = []
    for search_url in self.settings.search_urls:
        undervalued = await self.price_analyzer.detect_undervalued(search_url)
        for uv in undervalued:
            await self.repository.mark_ad_undervalued(uv.ad.ad_id, uv.undervalue_score)
            all_undervalued.append(uv)

    # 5. Notification phase (Telegram → Email fallback)
    telegram_failed = False
    for uv in all_undervalued:
        already_sent = await self.repository.is_notification_sent(uv.ad.ad_id)
        if not already_sent:
            msg_id = await self.notifier.send_undervalued_notification(
                uv.ad, uv.stats, uv.undervalue_score
            )
            if msg_id:
                await self.repository.mark_notification_sent(
                    uv.ad.ad_id, "undervalued", msg_id
                )
            else:
                telegram_failed = True

    # Fallback: отправить через Email, если Telegram не смог
    if telegram_failed and all_undervalued:
        unsent = [uv for uv in all_undervalued
                  if not await self.repository.is_notification_sent(uv.ad.ad_id)]
        if unsent:
            await self.email_notifier.send_undervalued_notifications(unsent)

    # 6. Summary
    await self.notifier.send_cycle_summary(total_found, total_new, len(all_undervalued))
    await self.collector.stop()
    logger.info("Collection cycle completed")

async def _process_search(self, search_url: str) -> None:
    """Обработка одного поискового запроса."""
    # Delay
    await random_delay(self.settings.min_delay_seconds, self.settings.max_delay_seconds)

    # Get or create tracked search
    tracked_search = await self.repository.get_or_create_tracked_search(search_url, ...)
    search_run = await self.repository.create_search_run(tracked_search.id)

    try:
        # Collect search page HTML
        html = await self.collector.collect_search_page(search_url)
        html_path = self.html_storage.save_search_html(html, search_url)

        # Parse search results
        search_results = parse_search_page(html, search_url)
        logger.info("Parsed search results", url=search_url, count=len(search_results))

        # Deduplication
        recent_ids = await self.repository.get_recent_ad_ids(search_url)
        new_results = [r for r in search_results if r.ad_id not in recent_ids]
        new_results = new_results[:self.settings.max_ads_per_search_per_run]

        # Process each new ad
        for result in new_results:
            await self._process_ad(result, search_url, tracked_search.id)

        # Complete run
        await self.repository.complete_search_run(
            search_run.id,
            ads_found=len(search_results),
            ads_new=len(new_results),
        )

    except Exception as e:
        await self.repository.fail_search_run(search_run.id, str(e))
        raise

async def _process_ad(self, result: SearchResult, search_url: str, tracked_search_id: int) -> None:
    """Обработка одного объявления."""
    # Delay
    await random_delay(self.settings.min_delay_seconds, self.settings.max_delay_seconds)

    # Get or create ad record
    ad, created = await self.repository.get_or_create_ad(
        result.ad_id, result.url, search_url, tracked_search_id
    )

    try:
        # Collect ad page HTML
        html = await self.collector.collect_ad_page(result.url)
        html_path = self.html_storage.save_ad_html(html, result.ad_id)

        # Parse ad data
        ad_data = parse_ad_page(html, result.url)
        ad_data.search_url = search_url

        # Update ad in DB (sets parse_status='parsed')
        await self.repository.update_ad_data(result.ad_id, ad_data)

        # Save snapshot
        if ad_data.price is not None:
            await self.repository.save_ad_snapshot(result.ad_id, ad_data.price, html_path)

    except Exception as e:
        logger.error("Failed to process ad", ad_id=result.ad_id, error=str(e))
        await self.repository.mark_ad_parse_failed(result.ad_id, str(e))
```

---

## 7. Поток данных

### Диаграмма потока данных

```mermaid
flowchart LR
    A[search_urls из конфига] --> B[Collector: Playwright]
    B --> C[HTML поисковых страниц]
    C --> D[HtmlStorage: сохранение на диск]
    C --> E[SearchParser: извлечение ссылок]
    E --> F[list of SearchResult]
    F --> G[Repository: дедупликация по recent_ad_ids]
    G --> H[Отфильтрованные новые объявления]
    H --> I[Collector: открытие карточек]
    I --> J[HTML карточек]
    J --> K[HtmlStorage: сохранение на диск]
    J --> L[AdParser: извлечение данных]
    L --> M[AdData - структурированные данные]
    M --> N[Repository: сохранение в PostgreSQL]
    N --> O[PriceAnalyzer: расчёт медианы и Q1]
    O --> P[Список UndervaluedAd]
    P --> Q{Telegram доступен?}
    Q -->|Да| R[TelegramNotifier: отправка в Telegram]
    Q -->|Нет| S[EmailNotifier: отправка по SMTP]
    R --> T[Repository: запись notification_sent]
    S --> T
```

### Подробное описание потока

| Этап | Вход | Модуль | Выход | Хранилище |
|------|------|--------|-------|-----------|
| 1. Сбор поисковой страницы | `search_url: str` | `AvitoCollector.collect_search_page()` | `html: str` | `data/raw_html/search/` |
| 2. Парсинг поиска | `html: str` | `parse_search_page()` | `list[SearchResult]` | — |
| 3. Дедупликация | `list[SearchResult]` | `Repository.get_recent_ad_ids()` | Отфильтрованный список | PostgreSQL |
| 4. Сбор карточки | `url: str` | `AvitoCollector.collect_ad_page()` | `html: str` | `data/raw_html/ad/` |
| 5. Парсинг карточки | `html: str` | `parse_ad_page()` | `AdData` | — |
| 6. Сохранение данных | `AdData` | `Repository` | Записи в таблицах | PostgreSQL + диск |
| 7. Ценовой анализ | `search_url` | `PriceAnalyzer` | `list[UndervaluedAd]` | PostgreSQL |
| 8. Уведомление | `UndervaluedAd` | `TelegramNotifier` / `EmailNotifier` (fallback) | `telegram_message_id` или email | PostgreSQL + Telegram/SMTP |

---

## 8. Обработка ошибок

### Стратегия

| Уровень | Обработка | Пример |
|---------|-----------|--------|
| **Отдельное объявление** | Логировать ошибку, продолжить со следующего | Не удалось распарсить цену |
| **Отдельный поиск** | Логировать ошибку, отметить SearchRun как failed, продолжить со следующего URL | Страница не загрузилась |
| **База данных** | Логировать и завершить цикл с ошибкой | Нет подключения к PostgreSQL |
| **Браузер** | Логировать и завершить цикл с ошибкой | Chromium не установлен |

### Кастомные исключения

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

## 9. Логирование

### Настройка

Используется `structlog` для структурированного логирования. Настройка в `app/main.py`.

```python
import structlog

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
```

### Ключевые события для логирования

| Событие | Уровень | Поля |
|---------|---------|------|
| Старт цикла | INFO | `search_urls_count` |
| Открытие поисковой страницы | INFO | `search_url`, `delay_seconds` |
| Результат парсинга поиска | INFO | `search_url`, `ads_found`, `ads_new` |
| Открытие карточки | INFO | `ad_id`, `url` |
| Результат парсинга карточки | INFO | `ad_id`, `title`, `price` |
| Обнаружено undervalued | WARNING | `ad_id`, `price`, `median`, `score` |
| Уведомление отправлено | INFO | `ad_id`, `telegram_message_id` |
| Telegram недоступен, fallback на Email | WARNING | `error` |
| Email-уведомление отправлено | INFO | `ad_id`, `email_to` |
| Ошибка отправки email | ERROR | `error` |
| Ошибка парсинга объявления | ERROR | `ad_id`, `error` |
| Ошибка загрузки страницы | ERROR | `url`, `error` |
| Завершение цикла | INFO | `total_found`, `total_new`, `undervalued_count` |

---

## 10. CLI — точка входа

### `app/main.py`

```python
import typer
import asyncio
from app.config.settings import AppSettings

app = typer.Typer(name="avito-monitor", help="Avito Monitor PoC")

@app.command()
def run():
    """Запустить один цикл сбора и анализа."""
    settings = AppSettings()
    asyncio.run(_run_cycle(settings))

@app.command()
def init_db():
    """Инициализировать базу данных — создать таблицы."""
    settings = AppSettings()
    asyncio.run(_init_db(settings))

async def _run_cycle(settings: AppSettings) -> None:
    """Асинхронный запуск полного цикла."""
    ...

async def _init_db(settings: AppSettings) -> None:
    """Создание таблиц в БД."""
    ...

if __name__ == "__main__":
    app()
```

### Запуск

```bash
# Инициализация БД
python -m app.main init-db

# Запуск одного цикла
python -m app.main run
```

---

## 11. Зависимости

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
```

### Установка Playwright

```bash
pip install playwright
playwright install chromium
```

---

## 12. Ограничения и допущения

### Текущие ограничения PoC

1. **Нет пагинации:** обрабатывается только первая страница поиска
2. **Нет авторизации:** не обрабатываются объявления требующие входа
3. **Нет ротации прокси:** `USE_PROXY = False` по умолчанию
4. **Нет расписания:** запуск только вручную через CLI
5. **Простой анализ:** только медиана и Q1, без ML
6. **Нет CAPTCHA-handling:** при появлении CAPTCHA цикл завершится с ошибкой

### Допущения

1. Селекторы Avito могут меняться — для этого предусмотрен `selectors.py` с fallback-селекторами
2. Формат URL объявлений: `https://www.avito.ru/{city}/{category}/{title}-{ad_id}`
3. Цена указана в рублях, целое число
4. Достаточно одной инстанции PostgreSQL на локальной машине

### Точки расширения

| Что | Как расширить |
|-----|---------------|
| Прокси | Установить `USE_PROXY=True`, указать `PROXY_URL` — `AvitoCollector` автоматически использует его |
| Расписание | Добавить cron/systemd/Task Scheduler на `python -m app.main run` |
| Пагинация | Добавить параметр `?page=N` к URL, цикл по страницам в `_process_search` |
| Новый канал уведомлений | Реализовать новый класс с методом `send_undervalued_notification` (пример: [`EmailNotifier`](app/notifier/email_notifier.py)) |
| ML-анализ | Расширить `PriceAnalyzer` или создать новый класс рядом |
| Больше поисков | Добавить URL в `SEARCH_URLS` в `.env` |

---

## 13. Ключевые паттерны

### 13.1 Разделение сырых HTML и структурированных данных

Система строго разделяет два слоя хранения:

| Слой | Хранилище | Содержимое | Назначение |
|------|-----------|------------|------------|
| Сырые данные | Файловая система `data/raw_html/` | Полный HTML-код страниц | Возможность перепарсинга без повторного сбора |
| Структурированные данные | PostgreSQL | Извлечённые поля, метаданные | Анализ, поиск, уведомления |

**Поток:**

1. `Collector` получает HTML через Playwright
2. `HtmlStorage.save_*()` записывает файл на диск, возвращает путь
3. `Parser` извлекает структурированные данные из HTML
4. `Repository` сохраняет структурированные данные в БД, включая `html_path`
5. `ad_snapshots.html_path` хранит ссылку на файл для повторного парсинга

**Преимущества:**
- Можно перепарсить HTML без обращения к Avito
- Можно отладить парсер на сохранённых файлах
- Исторические данные не теряются при изменении селекторов

### 13.2 Дедупликация объявлений

**Уникальность объявления** определяется по `ad_id` (числовой ID из URL Avito).

**Алгоритм дедупликации при обработке поиска:**

1. `parse_search_page()` извлекает `list[SearchResult]` с `ad_id` для каждого
2. `Repository.get_recent_ad_ids(search_url, hours=24)` возвращает множество `ad_id`, которые уже проверялись за последние 24 часа
3. Фильтрация: `new_results = [r for r in search_results if r.ad_id not in recent_ids]`
4. Ограничение: `new_results = new_results[:max_ads_per_search_per_run]`

**Статусы объявления:**

| `parse_status` | Значение |
|-----------------|----------|
| `pending` | Объявление обнаружено, карточка ещё не открывалась |
| `parsed` | Карточка открыта и успешно распарсена |
| `failed` | При парсинге произошла ошибка (см. `last_error`) |
| `skipped` | Объявление пропущено — например, уже проверялось |

### 13.3 Нормализация цены

Цена всегда хранится в БД как `INTEGER` (рубли, без копеек). Функция `normalize_price()` в `ad_parser.py` обрабатывает все варианты формата Avito:

- Удаляет неразрывные пробелы (`\xa0`)
- Удаляет обычные пробелы и запятые
- Удаляет символы `₽`, `руб.`
- Извлекает числовую часть через regex
- Возвращает `int` или `None`

### 13.4 Дедупликация уведомлений

Таблица `notifications_sent` имеет `UNIQUE` индекс на `(ad_id, notification_type)`. Это гарантирует:

- Уведомление типа `undervalued` отправляется **не более одного раза** для каждого объявления
- Перед отправкой проверяется `Repository.is_notification_sent(ad_id, notification_type)`
- После успешной отправки записывается `Repository.mark_notification_sent(ad_id, type, msg_id)`
- Повторный запуск цикла не вызовет дублирование уведомлений

### 13.5 Слой адаптации селекторов

Все CSS-селекторы вынесены в отдельный файл `app/parser/selectors.py`. Каждый селектор — это `list[str]` с fallback-вариантами. Парсер пробует селекторы по порядку:

```python
def _extract_with_fallback(soup: BeautifulSoup, selectors: list[str]) -> str | None:
    for selector in selectors:
        element = soup.select_one(selector)
        if element:
            return element.get_text(strip=True)
    return None
```

При изменении вёрстки Avito достаточно обновить `selectors.py` — бизнес-логика парсера не меняется.
