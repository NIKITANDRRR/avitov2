"""Вспомогательные утилиты Avito Monitor."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path

import structlog


def get_avito_base_url() -> str:
    """Возвращает базовый URL Avito из настроек (lazy import)."""
    from app.config.settings import get_settings
    return get_settings().AVITO_BASE_URL


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


async def save_html(html: str, directory: str, filename: str) -> str:
    """Асинхронное сохранение HTML-контента на диск.

    Использует ``asyncio.to_thread`` для неблокирующей записи файла,
    чтобы не приостанавливать event loop при I/O операциях.

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
    await asyncio.to_thread(file_path.write_text, html, "utf-8")

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


def build_page_url(base_url: str, page: int) -> str:
    """Добавить или обновить параметр пагинации ``p`` в URL Avito.

    Для страницы 1 возвращает исходный URL без параметра ``p``.
    Для последующих — добавляет ``&p=N`` (или обновляет, если уже есть).

    Args:
        base_url: Базовый URL поисковой выдачи Avito.
        page: Номер страницы (начиная с 1).

    Returns:
        str: URL с параметром пагинации.
    """
    if page <= 1:
        return base_url

    # Удаляем существующий параметр p если есть
    parts = base_url.split("?")
    path = parts[0]
    query = parts[1] if len(parts) > 1 else ""

    params = [p for p in query.split("&") if p and not p.startswith("p=")]
    params.append(f"p={page}")

    return f"{path}?{'&'.join(params)}"


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
        "челябинск": "chelyabinsk",
        "россия": "rossiya",
    }
    location_slug = location_map.get(location.lower(), location.lower())

    encoded_query = query.replace(" ", "+")

    return (
        f"{get_avito_base_url()}/{location_slug}"
        f"?q={encoded_query}&s=104"
    )


class RateLimiter:
    """Ограничение частоты запросов к Avito (скользящее окно).

    Потокобезопасный (asyncio.Lock) ограничитель частоты запросов.
    Использует алгоритм скользящего окна: хранит таймстемпы запросов
    и блокирует новые запросы, если лимит за период исчерпан.

    Attributes:
        _max: Максимальное количество запросов за период.
        _window: Длина окна в секундах.
        _timestamps: Список таймстемпов последних запросов.
        _lock: Асинхронный мьютекс для потокобезопасности.
    """

    def __init__(self, max_requests: int, per_seconds: int = 60) -> None:
        self._max = max_requests
        self._window = per_seconds
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Подождать, пока не будет безопасно сделать запрос.

        Если лимит запросов за окно исчерпан — ожидает освобождения слота.
        """
        async with self._lock:
            now = time.monotonic()
            # Удаляем старые таймстемпы за пределами окна
            self._timestamps = [
                t for t in self._timestamps if now - t < self._window
            ]

            if len(self._timestamps) >= self._max:
                # Нужно подождать до освобождения слота
                wait = self._window - (now - self._timestamps[0]) + 0.1
                if wait > 0:
                    logger = structlog.get_logger()
                    logger.debug(
                        "rate_limiter_waiting",
                        wait_sec=round(wait, 2),
                        max_requests=self._max,
                        window_sec=self._window,
                    )
                    await asyncio.sleep(wait)

            self._timestamps.append(time.monotonic())
