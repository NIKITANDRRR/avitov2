"""Модуль утилит приложения."""

from app.utils.exceptions import (
    AvitoMonitorError,
    CollectorError,
    ConfigurationError,
    NotifierError,
    ParserError,
    StorageError,
)
from app.utils.helpers import (
    build_page_url,
    extract_ad_id_from_url,
    normalize_price,
    normalize_url,
    random_delay,
    save_html,
    setup_logging,
)

__all__ = [
    "AvitoMonitorError",
    "CollectorError",
    "ConfigurationError",
    "NotifierError",
    "ParserError",
    "StorageError",
    "build_page_url",
    "extract_ad_id_from_url",
    "normalize_price",
    "normalize_url",
    "random_delay",
    "save_html",
    "setup_logging",
]
