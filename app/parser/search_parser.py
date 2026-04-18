"""Парсер поисковой страницы Avito."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import structlog
from bs4 import BeautifulSoup

from app.utils.exceptions import ParserError
from app.utils.helpers import extract_ad_id_from_url, get_avito_base_url, normalize_url, normalize_price

log = structlog.get_logger()


@dataclass
class SearchResultItem:
    """Результат парсинга одной записи из поисковой выдачи.

    Attributes:
        ad_id: Идентификатор объявления.
        url: Полный URL объявления.
        title: Заголовок объявления.
        price_str: Сырая строка цены.
        price: Числовая цена (нормализованная) или None.
        location: Местоположение / адрес.
        metadata: Дополнительные данные парсинга.
    """

    ad_id: str
    url: str
    title: str
    price_str: str | None
    location: str | None
    metadata: dict = field(default_factory=dict)
    price: float | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        """Вычисляет числовую цену из строки."""
        if self.price_str is not None:
            self.price = normalize_price(self.price_str)


def parse_search_page(html: str, search_url: str) -> list[SearchResultItem]:
    """Парсит HTML поисковой страницы Avito.

    Использует BeautifulSoup + lxml для извлечения объявлений
    из HTML-контента поисковой выдачи.

    Селекторы (с fallback):

    1. Основной: ``[data-marker="item"]`` или ``div[data-item-id]``
    2. Fallback: ``a[href*="/rossiya/"]`` с классом ``*item*``

    Args:
        html: HTML-контент поисковой страницы.
        search_url: Исходный URL поиска (для логирования).

    Returns:
        list[SearchResultItem]: Список найденных объявлений.

    Raises:
        ParserError: При критической ошибке парсинга.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
        items: list[SearchResultItem] = []
        skipped = 0

        # Стратегия 1: data-marker="item"
        elements = soup.select('[data-marker="item"]')

        # Стратегия 2: div[data-item-id]
        if not elements:
            log.debug("fallback_data_item_id", search_url=search_url)
            elements = soup.select("div[data-item-id]")

        # Стратегия 3: a[href*="/rossiya/"] с классом *item*
        if not elements:
            log.debug("fallback_link_items", search_url=search_url)
            elements = [
                a for a in soup.select("a[href*='/rossiya/']")
                if any("item" in cls for cls in a.get("class", []))
            ]

        if not elements:
            log.warning(
                "no_search_items_found",
                search_url=search_url,
            )
            return []

        log.debug(
            "search_elements_found",
            count=len(elements),
            search_url=search_url,
        )

        for element in elements:
            try:
                item = _parse_search_item(element)
                if item is not None:
                    items.append(item)
                else:
                    skipped += 1
            except Exception as exc:
                skipped += 1
                log.warning(
                    "search_item_parse_error",
                    error=str(exc),
                )

        log.info(
            "search_page_parsed",
            search_url=search_url,
            found=len(items),
            skipped=skipped,
        )

        return items

    except Exception as exc:
        raise ParserError(
            f"Failed to parse search page {search_url}: {exc}"
        ) from exc


def _parse_search_item(element: "Tag") -> SearchResultItem | None:  # noqa: F821
    """Парсинг одного элемента объявления из поисковой выдачи.

    Извлекает ad_id, url, title, price_str, location из элемента.
    Если не удаётся извлечь ad_id или url — возвращает ``None``.

    Args:
        element: BeautifulSoup Tag элемента объявления.

    Returns:
        SearchResultItem | None: Распарсенный элемент или ``None``.
    """
    # --- ad_id ---
    ad_id = element.get("data-item-id")
    if not ad_id:
        # Попытка извлечь из вложенного href
        link = element.select_one("a[href]")
        if link:
            href = link.get("href", "")
            try:
                ad_id = extract_ad_id_from_url(href)
            except ValueError:
                log.debug("cannot_extract_ad_id", href=href)
                return None
    if not ad_id:
        return None

    # --- url ---
    url: str | None = None
    link = element.select_one("a[href]")
    if link:
        href = link.get("href", "")
        if href.startswith("/"):
            url = f"{get_avito_base_url()}{href}"
        elif href.startswith("http"):
            url = href
        if url:
            url = normalize_url(url)

    if not url:
        return None

    # --- title ---
    title = _extract_text(element, [
        '[itemprop="name"]',
        '[data-marker="item/title"]',
        "a[itemprop]",
        "a",
    ])

    # --- price_str ---
    price_str = _extract_text(element, [
        '[itemprop="price"]',
        '[data-marker="item/price"]',
        'meta[itemprop="price"]',
    ])
    # Для meta-тега price — берём content
    if not price_str:
        price_meta = element.select_one('meta[itemprop="price"]')
        if price_meta:
            price_str = price_meta.get("content")

    # --- location ---
    location = _extract_text(element, [
        '[data-marker="item/address"]',
        '[class*="geo"]',
    ])

    # --- metadata ---
    metadata: dict = {}
    # Извлекаем дополнительные data-marker атрибуты
    for el in element.select("[data-marker]"):
        marker = el.get("data-marker", "")
        if marker.startswith("item/"):
            key = marker.replace("item/", "")
            text = el.get_text(strip=True)
            if text:
                metadata[key] = text

    return SearchResultItem(
        ad_id=str(ad_id),
        url=url,
        title=title or "",
        price_str=price_str,
        location=location,
        metadata=metadata,
    )


def _extract_text(
    element: "Tag",  # noqa: F821
    selectors: list[str],
) -> str | None:
    """Извлечь текст из первого найденного селектора.

    Args:
        element: Родительский элемент для поиска.
        selectors: Список CSS-селекторов (по приоритету).

    Returns:
        str | None: Текст элемента или ``None``.
    """
    for selector in selectors:
        try:
            found = element.select_one(selector)
            if found:
                text = found.get_text(strip=True)
                if text:
                    return text
        except Exception:
            continue
    return None
