"""Конфигурация приложения на основе pydantic-settings."""

from __future__ import annotations

import json
from typing import Any, ClassVar, Tuple, Type

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource
from pydantic_settings.sources import DotEnvSettingsSource


class _CommaListDotEnvSource(DotEnvSettingsSource):
    """Кастомный DotEnv-источник, который для поля SEARCH_URLS
    не пытается декодировать значение как JSON, а передаёт
    сырую строку дальше — field_validator разберёт её по запятым.
    """

    # Имя поля, которое нужно обрабатывать особым образом
    _COMMA_LIST_FIELDS: ClassVar[set[str]] = {"SEARCH_URLS", "EMAIL_TO"}

    def decode_complex_value(
        self,
        field_name: str,
        field: Any,
        value: Any,
    ) -> Any:
        if field_name in self._COMMA_LIST_FIELDS and isinstance(value, str):
            # Не пытаемся json.loads — возвращаем как есть,
            # field_validator потом разобьёт по запятым.
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
    MAX_SEARCH_PAGES_PER_RUN: int = Field(default=3, ge=1, le=10)
    MAX_ADS_PER_SEARCH_PER_RUN: int = Field(default=3, ge=1, le=10)

    # Delays (seconds)
    MIN_DELAY_SECONDS: float = Field(default=5.0, ge=1.0)
    MAX_DELAY_SECONDS: float = Field(default=15.0, ge=1.0)
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

    # Storage
    RAW_HTML_PATH: str = Field(default="data/raw_html")

    # Logging
    LOG_LEVEL: str = Field(default="INFO")

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
        """Заменяет стандартный DotEnvSettingsSource на кастомный."""
        # Подменяем dotenv-источник на наш, который не JSON-декодирует
        # comma-separated поля.
        custom_dotenv = _CommaListDotEnvSource(settings_cls)
        return (
            init_settings,
            env_settings,
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
