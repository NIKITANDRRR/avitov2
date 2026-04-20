"""Конфигурация приложения на основе pydantic-settings."""

from __future__ import annotations

import json
from typing import Any, ClassVar, Tuple, Type

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource
from pydantic_settings.sources import DotEnvSettingsSource, EnvSettingsSource


# Поля, которые передаются как comma-separated строки, а не JSON
_COMMA_LIST_FIELDS: set[str] = {"SEARCH_URLS", "EMAIL_TO", "USER_AGENTS"}


class _CommaListDotEnvSource(DotEnvSettingsSource):
    """Кастомный DotEnv-источник, который для comma-list полей
    не пытается декодировать значение как JSON, а передаёт
    сырую строку дальше — field_validator разберёт её по запятым.
    """

    def decode_complex_value(
        self,
        field_name: str,
        field: Any,
        value: Any,
    ) -> Any:
        if field_name in _COMMA_LIST_FIELDS and isinstance(value, str):
            return value
        return super().decode_complex_value(field_name, field, value)


class _CommaListEnvSource(EnvSettingsSource):
    """Кастомный Env-источник (os.environ), который для comma-list полей
    не пытается декодировать значение как JSON.

    Это нужно, когда .env загружается через python-dotenv (load_dotenv),
    и значения попадают в os.environ как обычные строки.
    """

    def decode_complex_value(
        self,
        field_name: str,
        field: Any,
        value: Any,
    ) -> Any:
        if field_name in _COMMA_LIST_FIELDS and isinstance(value, str):
            return value
        return super().decode_complex_value(field_name, field, value)


class Settings(BaseSettings):
    """Application configuration loaded from environment variables.

    Attributes:
        SEARCH_URLS: Список URL поиска Avito (до 3).
        MAX_SEARCH_PAGES_PER_RUN: Максимум страниц за запуск.
        MAX_ADS_PER_SEARCH_PER_RUN: Максимум объявлений за поиск за запуск.
        MIN_DELAY_SECONDS: Минимальная задержка между запросами (сек).
        MAX_DELAY_SECONDS: Максимальная задержка между запросами (сек).
        STARTUP_DELAY_MIN: Минимальная задержка при старте (сек).
        STARTUP_DELAY_MAX: Максимальная задержка при старте (сек).
        HEADLESS: Запуск браузера в headless-режиме.
        USE_PROXY: Использовать прокси.
        PROXY_URL: URL прокси-сервера.
        TELEGRAM_BOT_TOKEN: Токен Telegram-бота.
        TELEGRAM_CHAT_ID: ID чата Telegram.
        DATABASE_URL: URL подключения к PostgreSQL.
        UNDERVALUE_THRESHOLD: Порог недооценённости (0; 1).
        RAW_HTML_PATH: Путь к каталогу с HTML-файлами.
        LOG_LEVEL: Уровень логирования.
    """

    # Search URLs
    SEARCH_URLS: list[str] = Field(
        default=[],
        description="Comma-separated list of Avito search URLs (max 3)",
    )

    # Collection limits
    MAX_SEARCH_PAGES_PER_RUN: int = Field(default=50, ge=1, le=100)
    MAX_ADS_PER_SEARCH_PER_RUN: int = Field(default=10, ge=1, le=10)

    # Delays (seconds)
    MIN_DELAY_SECONDS: float = Field(
        default=4.0, ge=1.0,
        description="Минимальная задержка между запросами (сек)",
    )
    MAX_DELAY_SECONDS: float = Field(
        default=10.0, ge=1.0,
        description="Максимальная задержка между запросами (сек)",
    )
    STARTUP_DELAY_MIN: float = Field(default=0.0, ge=0.0)
    STARTUP_DELAY_MAX: float = Field(default=30.0, ge=0.0)

    # Browser settings
    HEADLESS: bool = Field(default=False)
    USE_PROXY: bool = Field(default=False)
    PROXY_URL: str | None = Field(default=None)

    # Telegram
    TELEGRAM_BOT_TOKEN: str = Field(default="")
    TELEGRAM_CHAT_ID: str = Field(default="")

    # Telegram MTProto settings
    TELEGRAM_API_ID: int = Field(
        default=0, description="Telegram API ID from my.telegram.org"
    )
    TELEGRAM_API_HASH: str = Field(
        default="", description="Telegram API hash from my.telegram.org"
    )

    # MTProto Proxy
    MTPROXY_ENABLED: bool = Field(
        default=False, description="Use MTProto proxy for Telegram"
    )
    MTPROXY_ADDRESS: str = Field(default="", description="MTProto proxy address")
    MTPROXY_PORT: int = Field(default=0, description="MTProto proxy port")
    MTPROXY_SECRET: str = Field(default="", description="MTProto proxy secret")

    # Email notification settings
    SMTP_HOST: str = Field(default="", description="SMTP server host")
    SMTP_PORT: int = Field(default=587, description="SMTP server port")
    SMTP_USER: str = Field(default="", description="SMTP login")
    SMTP_PASSWORD: str = Field(default="", description="SMTP password")
    SMTP_USE_TLS: bool = Field(default=True, description="Use TLS")
    EMAIL_FROM: str = Field(default="", description="Sender email address")
    EMAIL_TO: list[str] = Field(
        default=[], description="Comma-separated recipient emails"
    )

    # PostgreSQL
    DATABASE_URL: str = Field(
        default="postgresql://avito:avito@localhost:5432/avito_monitor"
    )

    # Analysis
    UNDERVALUE_THRESHOLD: float = Field(default=0.8, gt=0.0, lt=1.0)

    # === Параметры фильтрации аномалий ===
    TRIM_PERCENT: float = Field(
        default=0.05,
        ge=0.0,
        le=0.5,
        description="Доля отбрасываемых выбросов с каждого края (5%)",
    )
    IQR_MULTIPLIER: float = Field(
        default=1.5,
        ge=0.0,
        description="Множитель для IQR fences",
    )
    TEMPORAL_WINDOW_DAYS: int = Field(
        default=14,
        ge=1,
        description="Окно анализа в днях",
    )
    MIN_SEGMENT_SIZE: int = Field(
        default=3,
        ge=1,
        description="Минимальный размер сегмента для анализа",
    )
    UNDERVALUED_THRESHOLD: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="Порог composite score для недооценённости",
    )
    ZSCORE_THRESHOLD: float = Field(
        default=1.5,
        ge=0.0,
        description="Порог z-score для аномалий",
    )
    MEDIAN_DISCOUNT_THRESHOLD: float = Field(
        default=0.85,
        gt=0.0,
        le=1.0,
        description="Порог % от медианы для недооценённости",
    )

    # === Настройки детекции бриллиантов ===
    DIAMOND_DISCOUNT_THRESHOLD: float = Field(
        default=0.70,
        gt=0.5,
        le=1.0,
        description="Порог price/median для детекции бриллиантов (0.70 = 30% ниже медианы)",
    )
    DIAMOND_MIN_PRICE: float = Field(
        default=10000.0,
        ge=0.0,
        description="Минимальная цена товара для уведомления о бриллианте (руб.)",
    )
    DIAMOND_MIN_SNAPSHOTS: int = Field(
        default=3,
        ge=2,
        le=20,
        description="Минимум снапшотов продукта для использования product-level медианы",
    )
    DIAMOND_FAST_SALE_THRESHOLD: float = Field(
        default=0.8,
        gt=0.5,
        le=1.0,
        description="Порог для медианы быстрых продаж (segment fallback)",
    )

    # === Фильтрация аксессуаров ===
    ENABLE_ACCESSORY_FILTER: bool = Field(
        default=True,
        description="Включить фильтрацию аксессуаров и мелочевки"
    )
    MIN_PRICE_FILTER: int = Field(
        default=10000,
        description="Минимальная цена товара для анализа (руб.)"
    )
    ACCESSORY_BLACKLIST: list[str] = Field(
        default=[
            "чехол", "case", "кейс", "сумка",
            "шлейф", "кабель", "провод",
            "матрица", "экран", "дисплей",
            "блок питания", "зарядк", "адаптер питания", "power adapter",
            "клавиатура", "keyboard",
            "игр", "game",
            "стилус", "pen", "pencil",
            "подставк", "держатель", "mount",
            "защитн", "плёнк", "стекло", "protector",
            "мышь", "mouse", "трекпад",
            "ремешок", "браслет",
            "наушник", "airpods pro",
            "аккумулятор", "battery",
            "винт", "болт", "креплени",
            "комплект", "набор", "bundle", "лот", "штук", "пара", "набором", "комплектом",
        ],
        description="Чёрный список слов в названии для фильтрации аксессуаров"
    )
    ACCESSORY_PRICE_RATIO_THRESHOLD: float = Field(
        default=0.3,
        description="Если цена < 30% медианы сегмента — вероятный аксессуар"
    )

    # === Параметры масштабирования поиска ===
    MAX_CONCURRENT_SEARCHES: int = Field(
        default=3,
        ge=1,
        le=20,
        description="Макс. параллельных поисков в батче",
    )
    MAX_CONCURRENT_AD_PAGES: int = Field(
        default=5,
        ge=1,
        le=10,
        description="Макс. параллельно открываемых карточек объявлений",
    )
    DEFAULT_SCHEDULE_INTERVAL_HOURS: float = Field(
        default=0.5,
        ge=0.5,
        le=48,
        description="Интервал запуска по умолчанию (часы)",
    )
    DEFAULT_MAX_ADS_TO_PARSE: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Карточек на поиск за запуск по умолчанию",
    )
    BATCH_DELAY_SECONDS: int = Field(
        default=30,
        ge=0,
        description="Задержка между батчами поисков (сек)",
    )
    SEARCH_DELAY_SECONDS: int = Field(
        default=5,
        ge=0,
        description="Задержка между поисками в батче (сек)",
    )

    # === Warm-up режим (первый запуск) ===
    WARMUP_ENABLED: bool = Field(
        default=True,
        description="Включить режим разогрева при первом запуске (все поиски 'new')",
    )
    WARMUP_INITIAL_DELAY: float = Field(
        default=60.0,
        ge=10.0,
        description="Начальная задержка перед первым запросом при warm-up (сек)",
    )
    WARMUP_SEARCH_DELAY: float = Field(
        default=30.0,
        ge=5.0,
        description="Задержка между поисками при warm-up (сек)",
    )
    WARMUP_AD_DELAY_MIN: float = Field(
        default=10.0,
        ge=3.0,
        description="Мин. задержка между карточками при warm-up (сек)",
    )
    WARMUP_AD_DELAY_MAX: float = Field(
        default=20.0,
        ge=5.0,
        description="Макс. задержка между карточками при warm-up (сек)",
    )
    WARMUP_MAX_CONCURRENT_SEARCHES: int = Field(
        default=1,
        ge=1,
        le=5,
        description="Макс. параллельных поисков при warm-up",
    )
    WARMUP_MAX_CONCURRENT_ADS: int = Field(
        default=1,
        ge=1,
        le=3,
        description="Макс. параллельных карточек при warm-up",
    )
    REQUEST_RATE_LIMIT_PER_MINUTE: int = Field(
        default=10,
        ge=1,
        le=30,
        description="Макс. запросов в минуту глобально",
    )

    # === Раздельные rate limiter'ы ===
    SEARCH_RATE_LIMIT_PER_MINUTE: int = Field(
        default=8,
        ge=1,
        le=30,
        description="Максимум запросов поиска в минуту",
    )
    AD_RATE_LIMIT_PER_MINUTE: int = Field(
        default=10,
        ge=1,
        le=30,
        description="Максимум запросов карточек в минуту",
    )

    # --- Seller Profile Parsing ---
    SELLER_PROFILE_ENABLED: bool = Field(
        default=True,
        description="Включить/выключить парсинг профилей продавцов",
    )
    SELLER_RATE_LIMIT_PER_MINUTE: int = Field(
        default=3,
        ge=1,
        le=30,
        description="Rate limit для запросов к профилям (запросов/мин)",
    )
    SELLER_MAX_PROFILES_PER_CYCLE: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Макс. кол-во профилей за один цикл",
    )
    SELLER_MAX_PAGES_PER_PROFILE: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Максимальное количество страниц профиля продавца для парсинга",
    )
    SELLER_SCRAPE_INTERVAL_HOURS: float = Field(
        default=24.0,
        ge=1.0,
        le=168.0,
        description="Интервал повторного парсинга профиля (часы)",
    )
    SELLER_PAGE_DELAY_MIN: float = Field(
        default=5.0,
        ge=1.0,
        description="Мин. задержка между страницами профиля (сек)",
    )
    SELLER_PAGE_DELAY_MAX: float = Field(
        default=12.0,
        ge=1.0,
        description="Макс. задержка между страницами профиля (сек)",
    )

    # === Настройки retry ===
    RETRY_MAX_ATTEMPTS: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Максимум попыток при ошибке загрузки",
    )
    RETRY_BACKOFF_BASE: float = Field(
        default=5.0,
        ge=1.0,
        description="Базовая задержка для exponential backoff (сек)",
    )
    RETRY_BACKOFF_MAX: float = Field(
        default=60.0,
        ge=10.0,
        description="Максимальная задержка retry (сек)",
    )

    # === Настройки изоляции контекста ===
    USE_ISOLATED_CONTEXTS: bool = Field(
        default=True,
        description="Создавать отдельный контекст браузера на каждый поиск",
    )

    # === Force parse mode settings ===
    FORCE_PARSE_PRODUCT_DELAY_SECONDS: int = Field(
        default=60,
        ge=0,
        description="Задержка после парсинга товаров перед категориями (сек)",
    )
    FORCE_PARSE_CATEGORY_INTERVAL_SECONDS: int = Field(
        default=60,
        ge=0,
        description="Интервал между категориями при force-parse (сек)",
    )

    # --- Segment Analysis Settings ---
    segment_rare_threshold: int = Field(
        default=5,
        description="Минимальное кол-во объявлений, чтобы сегмент не считался редким",
    )
    segment_fast_sale_days: int = Field(
        default=3,
        description="Кол-во дней, за которое продажа считается быстрой",
    )
    segment_7d_weight: float = Field(
        default=1.5,
        description="Вес 7d медианы при росте рынка",
    )
    segment_trend_window_days: int = Field(
        default=30,
        description="Окно для расчёта тренда цены",
    )
    segment_history_snapshot_days: int = Field(
        default=7,
        description="Периодичность сохранения снапшотов (дни)",
    )
    segment_min_samples_for_stats: int = Field(
        default=3,
        description="Минимум объявлений для расчёта статистики",
    )
    segment_liquidity_premium: float = Field(
        default=1.2,
        description="Премия за ликвидность для редких товаров",
    )
    segment_price_outlier_percentile: float = Field(
        default=0.05,
        description="Процентиль для отсечения выбросов",
    )

    # Storage
    RAW_HTML_PATH: str = Field(default="data/raw_html")

    # Logging
    LOG_LEVEL: str = Field(default="INFO")

    # === Avito base URL ===
    AVITO_BASE_URL: str = Field(
        default="https://www.avito.ru",
        description="Базовый URL сайта Avito",
    )

    # === Database connection pool ===
    DB_POOL_SIZE: int = Field(
        default=5, ge=1, le=50,
        description="Размер пула подключений к БД",
    )
    DB_MAX_OVERFLOW: int = Field(
        default=10, ge=0, le=50,
        description="Макс. дополнительных подключений сверх pool_size",
    )
    DB_POOL_RECYCLE: int = Field(
        default=1800, ge=60,
        description="Время жизни подключения в пуле (сек)",
    )
    DB_CONNECT_TIMEOUT: int = Field(
        default=10, ge=1, le=60,
        description="Таймаут подключения к БД (сек)",
    )

    # === Telegram session path ===
    TELEGRAM_SESSION_PATH: str = Field(
        default="data/bot_session",
        description="Путь к файлу сессии Telethon (без расширения)",
    )

    # === Browser fingerprint settings ===
    BROWSER_LOCALE: str = Field(
        default="ru-RU",
        description="Локаль браузера",
    )
    BROWSER_TIMEZONE: str = Field(
        default="Europe/Moscow",
        description="Часовой пояс браузера",
    )
    USER_AGENTS: list[str] = Field(
        default=[
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
        ],
        description="Список User-Agent строк для ротации (JSON или comma-separated)",
    )
    BROWSER_VIEWPORTS: list[dict[str, int]] = Field(
        default=[
            {"width": 1920, "height": 1080},
            {"width": 1536, "height": 864},
            {"width": 1440, "height": 900},
        ],
        description="Список viewport размеров для ротации (JSON)",
    )

    # === Page timeouts ===
    PAGE_NAVIGATION_TIMEOUT_MS: int = Field(
        default=30000, ge=5000, le=120000,
        description="Таймаут навигации страницы (мс)",
    )
    SELECTOR_WAIT_TIMEOUT_MS: int = Field(
        default=15000, ge=1000, le=60000,
        description="Таймаут ожидания селекторов (мс)",
    )

    # === Captcha settings ===
    MAX_CONSECUTIVE_CAPTCHAS: int = Field(
        default=3, ge=1, le=10,
        description="Порог последовательных капч для паузы",
    )
    CAPTCHA_DELAY_MIN: float = Field(
        default=30.0, ge=5.0,
        description="Мин. задержка при обнаружении капчи (сек)",
    )
    CAPTCHA_DELAY_MAX: float = Field(
        default=60.0, ge=10.0,
        description="Макс. задержка при обнаружении капчи (сек)",
    )
    CAPTCHA_MANUAL_INPUT_WAIT: int = Field(
        default=120, ge=30, le=600,
        description="Ожидание ручного ввода капчи (сек)",
    )

    # === Scheduler ===
    SCHEDULER_CYCLE_INTERVAL_SECONDS: int = Field(
        default=300, ge=60, le=3600,
        description="Интервал цикла планировщика (сек)",
    )

    # --- Constant mode settings ---
    CONSTANT_MODE_ENABLED: bool = Field(
        default=False,
        description="Включить режим 24/7 постоянной работы",
    )
    CONSTANT_CYCLE_INTERVAL_SECONDS: int = Field(
        default=300, ge=60, le=3600,
        description="Интервал между полными циклами в constant режиме (секунд)",
    )
    CONSTANT_FORCE_PENDING_AFTER_SEARCH: bool = Field(
        default=True,
        description="Запускать force-pending после каждого цикла поиска",
    )
    CONSTANT_BROWSER_HEADLESS: bool = Field(
        default=False,
        description="Headless режим браузера в constant режиме",
    )

    # === Config file paths ===
    PRODUCTS_CONFIG_PATH: str = Field(
        default="config/products.json",
        description="Путь к файлу конфигурации продуктов",
    )
    CATEGORIES_CONFIG_PATH: str = Field(
        default="config/categories.json",
        description="Путь к файлу конфигурации категорий",
    )
    NOTIFICATIONS_LOG_PATH: str = Field(
        default="data/notifications.jsonl",
        description="Путь к файлу лога уведомлений",
    )

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """Заменяет стандартные EnvSettingsSource и DotEnvSettingsSource
        на кастомные, которые не JSON-декодируют comma-separated поля."""
        custom_env = _CommaListEnvSource(settings_cls)
        custom_dotenv = _CommaListDotEnvSource(settings_cls)
        return (
            init_settings,
            custom_env,
            custom_dotenv,
            file_secret_settings,
        )

    @field_validator("SEARCH_URLS", mode="before")
    @classmethod
    def _split_urls(cls, v: str | list[str]) -> list[str]:
        """Разбивает строку с URL, разделёнными запятыми, в список."""
        if isinstance(v, str):
            # Сначала пробуем JSON (на случай если передали ["url1","url2"])
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, ValueError):
                pass
            # Fallback — разбиваем по запятым
            return [url.strip() for url in v.split(",") if url.strip()]
        return v

    @field_validator("EMAIL_TO", mode="before")
    @classmethod
    def _split_emails(cls, v: str | list[str]) -> list[str]:
        """Разбивает строку с email-адресами, разделёнными запятыми, в список."""
        if isinstance(v, str):
            return [email.strip() for email in v.split(",") if email.strip()]
        return v

    @field_validator("USER_AGENTS", mode="before")
    @classmethod
    def _split_user_agents(cls, v: str | list[str]) -> list[str]:
        """Разбивает строку с User-Agent'ами, разделёнными запятыми, в список."""
        if isinstance(v, str):
            return [ua.strip() for ua in v.split(",") if ua.strip()]
        return v


_settings: Settings | None = None


def get_settings() -> Settings:
    """Возвращает singleton-экземпляр конфигурации.

    Returns:
        Settings: Экземпляр конфигурации приложения.
    """
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
