"""Вспомогательные утилиты Avito Monitor."""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

import structlog


async def random_delay(min_sec: float, max_sec: float) -> None:
    """Асинхронная случайная задержка с логированием.

    Args:
        min_sec: Минимальная задержка в секундах.
        max_sec: Максимальная задержка в секундах.
    """
    import random

    delay = random.uniform(min_sec, max_sec)
    logger = structlog.get_logger()
    logger.debug("random_delay", delay_sec=round(delay, 2))
    await asyncio.sleep(delay)


def normalize_url(url: str) -> str:
    """Нормализация URL объявления Avito.

    Удаляет query-параметры и приводит URL к каноническому виду.

    Args:
        url: Исходный URL объявления.

    Returns:
        str: Нормализованный URL без query-параметров.
    """
    # Удаляем fragment и query
    url = url.split("?")[0].split("#")[0]
    # Удаляем trailing slash
    url = url.rstrip("/")
    return url


def extract_ad_id_from_url(url: str) -> str:
    """Извлечение идентификатора объявления из URL Avito.

    Avito URL format: ``https://www.avito.ru/{city}/{slug}_{ad_id}``.

    Args:
        url: URL объявления Avito.

    Returns:
        str: Идентификатор объявления.

    Raises:
        ValueError: Если не удалось извлечь ad_id из URL.
    """
    normalized = normalize_url(url)
    # Последний сегмент пути после '/'
    last_segment = normalized.split("/")[-1]
    # ad_id — это суффикс после последнего подчёркивания
    match = re.search(r"_(\d+)$", last_segment)
    if match:
        return match.group(1)
    raise ValueError(f"Cannot extract ad_id from URL: {url}")


def save_html(html: str, directory: str, filename: str) -> str:
    """Сохранение HTML-контента на диск.

    Args:
        html: HTML-контент для сохранения.
        directory: Целевой каталог.
        filename: Имя файла (без расширения, добавляется ``.html``).

    Returns:
        str: Полный путь к сохранённому файлу.
    """
    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)

    if not filename.endswith(".html"):
        filename = f"{filename}.html"

    file_path = dir_path / filename
    file_path.write_text(html, encoding="utf-8")

    return str(file_path)


def normalize_price(price_str: str) -> float | None:
    """Парсинг строки цены в число.

    Примеры:
        ``"125 000 ₽"`` → ``125000.0``
        ``"3 500 $"`` → ``3500.0``

    Args:
        price_str: Строка с ценой.

    Returns:
        float | None: Числовое значение цены или ``None``, если не удалось распарсить.
    """
    if not price_str:
        return None

    # Удаляем все символы, кроме цифр, точек, запятых и минусов
    cleaned = re.sub(r"[^\d.,\-]", "", price_str)

    if not cleaned:
        return None

    # Заменяем запятые на точки (для десятичных разделителей)
    cleaned = cleaned.replace(",", ".")

    # Удаляем точки, используемые как разделители тысяч (если после точки идёт 3 цифры)
    cleaned = re.sub(r"\.(?=\d{3})", "", cleaned)

    try:
        return float(cleaned)
    except ValueError:
        return None


def setup_logging(level: str) -> None:
    """Настройка structlog для приложения.

    Args:
        level: Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
    )


def build_avito_url(query: str, location: str = "Москва") -> str:
    """Построить URL поиска Avito по запросу и локации.

    Args:
        query: Поисковый запрос.
        location: Город/регион.

    Returns:
        URL для поиска на Avito.
    """
    location_map = {
        "москва": "moskva",
        "санкт-петербург": "sankt-peterburg",
        "петербург": "sankt-peterburg",
        "екатеринбург": "ekaterinburg",
        "новосибирск": "novosibirsk",
        "россия": "rossiya",
    }
    location_slug = location_map.get(location.lower(), location.lower())

    encoded_query = query.replace(" ", "+")

    return (
        f"https://www.avito.ru/{location_slug}"
        f"?q={encoded_query}&s=104"
    )
