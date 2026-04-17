# -*- coding: utf-8 -*-
"""Parser for Avito seller profile page (sold items tab)."""

from __future__ import annotations

import datetime
import re
from dataclasses import dataclass, field

import structlog
from bs4 import BeautifulSoup, Tag

from app.utils.exceptions import ParserError
from app.utils.helpers import normalize_price

log = structlog.get_logger()


# ------------------------------------------------------------------
# Data classes
# ------------------------------------------------------------------


@dataclass
class SoldItemData:
    """Sold item data.

    Attributes:
        item_id: Item ID.
        title: Item title.
        price: Normalized price (number).
        price_str: Raw price string.
        category: Item category.
        sold_date: Sale date.
        item_url: Item URL.
    """

    item_id: str | None = None
    title: str | None = None
    price: float | None = None
    price_str: str | None = None
    category: str | None = None
    sold_date: datetime.datetime | None = None
    item_url: str | None = None


@dataclass
class SellerProfileData:
    """Seller profile data.

    Attributes:
        seller_id: Seller ID (from URL).
        seller_url: Profile URL.
        seller_name: Seller name.
        rating: Seller rating.
        reviews_count: Number of reviews.
        total_sold_items: Total number of sold items.
        sold_items: List of sold items on the current page.
    """

    seller_id: str | None = None
    seller_url: str | None = None
    seller_name: str | None = None
    rating: float | None = None
    reviews_count: int | None = None
    total_sold_items: int | None = None
    sold_items: list[SoldItemData] = field(default_factory=list)


# ------------------------------------------------------------------
# Main parsing function
# ------------------------------------------------------------------


def parse_seller_profile(html: str, url: str) -> SellerProfileData:
    """Parse seller profile HTML page (sold items tab).

    Extracts seller data and list of sold items.
    Each field is parsed in a separate try/except block so that
    a missing field does not break the entire parsing process.

    Args:
        html: HTML content of the seller profile page.
        url: URL of the seller profile page.

    Returns:
        SellerProfileData: Parsed seller profile data.

    Raises:
        ParserError: On critical parsing error.
    """
    try:
        soup = BeautifulSoup(html, "lxml")

        # --- seller_id from URL ---
        seller_id = _extract_seller_id(url)

        # --- seller_name ---
        seller_name = _safe_extract(soup, [
            '[data-marker="profile/name"]',
            ".seller-name",
            "h1",
        ])

        # --- rating ---
        rating_raw = _safe_extract(soup, [
            '[data-marker="profile/rating"]',
            ".rating-value",
            '[class*="rating"]',
        ])
        rating: float | None = None
        if rating_raw:
            try:
                match = re.search(r"[\d]+[.,][\d]+", rating_raw)
                if match:
                    rating = float(match.group().replace(",", "."))
                else:
                    match = re.search(r"[\d]+", rating_raw)
                    if match:
                        rating = float(match.group())
            except (ValueError, TypeError):
                log.debug("rating_parse_failed", raw=rating_raw)

        # --- reviews_count ---
        reviews_raw = _safe_extract(soup, [
            '[data-marker="profile/reviews-count"]',
            '[class*="reviews-count"]',
        ])
        reviews_count: int | None = None
        if reviews_raw:
            try:
                match = re.search(r"[\d]+", reviews_raw.replace(" ", ""))
                if match:
                    reviews_count = int(match.group())
            except (ValueError, TypeError):
                log.debug("reviews_count_parse_failed", raw=reviews_raw)

        # --- total_sold_items ---
        total_sold_raw = _safe_extract(soup, [
            '[data-marker="profile/sold-count"]',
            '[class*="sold-count"]',
        ])
        total_sold_items: int | None = None
        if total_sold_raw:
            try:
                match = re.search(r"[\d]+", total_sold_raw.replace(" ", ""))
                if match:
                    total_sold_items = int(match.group())
            except (ValueError, TypeError):
                log.debug("total_sold_parse_failed", raw=total_sold_raw)

        # --- sold_items ---
        sold_items = _parse_sold_items(soup)

        profile = SellerProfileData(
            seller_id=seller_id,
            seller_url=url,
            seller_name=seller_name,
            rating=rating,
            reviews_count=reviews_count,
            total_sold_items=total_sold_items,
            sold_items=sold_items,
        )

        log.info(
            "seller_profile_parsed",
            url=url,
            seller_id=seller_id,
            seller_name=seller_name,
            sold_items_count=len(sold_items),
        )

        return profile

    except Exception as exc:
        raise ParserError(
            f"Failed to parse seller profile {url}: {exc}"
        ) from exc


# ------------------------------------------------------------------
# Sold items list parsing
# ------------------------------------------------------------------


def _parse_sold_items(soup: BeautifulSoup) -> list[SoldItemData]:
    """Extract list of sold items from profile HTML.

    Searches for item cards using multiple fallback selectors
    and extracts data from each card.

    Args:
        soup: BeautifulSoup object of the profile page.

    Returns:
        list[SoldItemData]: List of sold items.
    """
    items: list[SoldItemData] = []

    # Selectors for sold item cards
    item_selectors = [
        '[data-marker="sold-item"]',
        '.sold-items-list [data-item-id]',
        '.items-list-item',
        '[data-marker="catalog-serp"] [data-item-id]',
        'div[data-item-id]',
    ]

    # Find all item cards
    elements: list[Tag] = []
    for selector in item_selectors:
        try:
            found = soup.select(selector)
            if found:
                elements = found
                log.debug(
                    "sold_items_selector_matched",
                    selector=selector,
                    count=len(found),
                )
                break
        except Exception:
            continue

    if not elements:
        log.debug("no_sold_items_found")
        return items

    for element in elements:
        try:
            item = _parse_single_sold_item(element)
            if item:
                items.append(item)
        except Exception:
            log.debug("sold_item_parse_error", element=str(element)[:200])
            continue

    return items


def _parse_single_sold_item(element: Tag) -> SoldItemData | None:
    """Parse a single sold item card.

    Args:
        element: BeautifulSoup Tag of the item card.

    Returns:
        SoldItemData | None: Item data or ``None`` if extraction failed.
    """
    # --- item_id ---
    item_id = element.get("data-item-id")
    if not item_id:
        link = element.select_one("a[href*='/']")
        if link:
            href = link.get("href", "")
            match = re.search(r"_(\d+)(?:\?|$)", str(href))
            if match:
                item_id = match.group(1)

    # --- title ---
    title = _safe_extract_from(element, [
        '[data-marker="item-title"]',
        "a.item-title",
        "[itemprop='name']",
        "a",
    ])

    # --- price_str ---
    price_str = _safe_extract_from(element, [
        '[data-marker="item-price"]',
        "span.price",
        "[class*='price']",
        "meta[itemprop='price']",
    ])
    if not price_str:
        price_meta = element.select_one("meta[itemprop='price']")
        if price_meta:
            price_str = price_meta.get("content")

    # --- price (normalized) ---
    price: float | None = None
    if price_str:
        price = normalize_price(price_str)

    # --- category ---
    category = _safe_extract_from(element, [
        "[class*='category']",
        "[class*='subcategory']",
        "[class*='CategoryPath']",
        "[class*='breadcrumbs']",
        "nav a",
    ])

    # --- item_url ---
    item_url: str | None = None
    link = element.select_one("a[href]")
    if link:
        href = link.get("href", "")
        if href:
            item_url = str(href)
            if item_url.startswith("/"):
                item_url = "https://www.avito.ru" + item_url

    # Fallback: извлечь категорию из URL товара
    # Формат Avito: /ГОРОД/КАТЕГОРИЯ/ПОДКАТЕГОРИЯ/название_ID
    # Например: /moskva/telefony/iphone_15_pro_max_256gb_3456789012
    if not category and item_url:
        category = _extract_category_from_url(item_url)

    # --- sold_date ---
    sold_date_raw = _safe_extract_from(element, [
        "[class*='date']",
        "[class*='sold-date']",
        "time",
    ])
    sold_date: datetime.datetime | None = None
    if sold_date_raw:
        sold_date = _parse_date(sold_date_raw)

    result = SoldItemData(
        item_id=str(item_id) if item_id else None,
        title=title,
        price=price,
        price_str=price_str,
        category=category,
        sold_date=sold_date,
        item_url=item_url,
    )

    if category is None:
        log.debug(
            "sold_item_no_category",
            item_id=item_id,
            title=title[:50] if title else None,
            item_url=item_url,
        )

    return result


# ------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------


def _safe_extract(
    soup: BeautifulSoup,
    selectors: list[str],
) -> str | None:
    """Safely extract text using a list of fallback selectors.

    Iterates through selectors by priority. Logs debug if no
    selector matched.

    Args:
        soup: BeautifulSoup object of the page.
        selectors: List of CSS selectors by priority.

    Returns:
        str | None: Text of the first matched element or ``None``.
    """
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                if element.name == "meta":
                    text = element.get("content", "")
                else:
                    text = element.get_text(strip=True)
                if text:
                    return text
        except Exception:
            continue

    return None


def _safe_extract_from(
    element: Tag,
    selectors: list[str],
) -> str | None:
    """Safely extract text from a nested element.

    Iterates through selectors by priority within the given element.

    Args:
        element: BeautifulSoup Tag to search within.
        selectors: List of CSS selectors by priority.

    Returns:
        str | None: Text of the first matched element or ``None``.
    """
    for selector in selectors:
        try:
            found = element.select_one(selector)
            if found:
                if found.name == "meta":
                    text = found.get("content", "")
                else:
                    text = found.get_text(strip=True)
                if text:
                    return text
        except Exception:
            continue

    return None


def _extract_category_from_url(url: str) -> str | None:
    """Extract category from Avito item URL.

    Avito URL format::

        https://www.avito.ru/CITY/CATEGORY/SUBCATEGORY/item_NAME_ID
        https://www.avito.ru/CITY/CATEGORY/item_NAME_ID
        /moskva/telefony/iphone_15_pro_max_256gb_3456789012

    The category is the second path segment (after city).

    Args:
        url: Item URL.

    Returns:
        str | None: Category string or ``None``.
    """
    try:
        # Убираем домен и query/fragment
        path = re.sub(r"^https?://(?:www\.)?avito\.ru", "", url)
        path = path.split("?")[0].split("#")[0]
        parts = [p for p in path.strip("/").split("/") if p]

        # parts[0] = город, parts[1] = категория, parts[2] = подкатегория (опционально)
        if len(parts) >= 2:
            category = parts[1]
            # Декодируем URL-encoding и заменяем подчёркивания на пробелы
            from urllib.parse import unquote
            category = unquote(category).replace("_", " ")
            # Пропускаем служебные пути
            if category in ("user", "u", "account", "basket", "favorites"):
                return None
            return category
    except Exception:
        log.debug("category_extract_from_url_failed", url=url)
    return None


def _extract_seller_id(url: str) -> str | None:
    """Extract seller ID from profile URL.

    Supported URL formats::

        https://www.avito.ru/user/SELLER_ID/profile
        https://www.avito.ru/user/SELLER_ID
        https://www.avito.ru/u/SELLER_ID
        /user/SELLER_ID/profile

    Args:
        url: Seller profile URL.

    Returns:
        str | None: Seller ID or ``None``.
    """
    match = re.search(r"/user/([^/?#]+)", url)
    if match:
        return match.group(1)
    # Fallback: новый формат /u/SELLER_ID
    match = re.search(r"/u/([^/?#]+)", url)
    if match:
        return match.group(1)
    return None


def _parse_date(raw: str) -> datetime.datetime | None:
    """Parse date from string.

    Supported formats::

        DD month YYYY (Russian)
        YYYY-MM-DD
        DD.MM.YYYY

    Args:
        raw: Raw date string.

    Returns:
        datetime.datetime | None: Parsed date or ``None``.
    """
    if not raw:
        return None

    cleaned = raw.strip()

    months_map: dict[str, int] = {}
    _months_src = {
        1: ["\u044f\u043d\u0432\u0430\u0440\u044f", "\u044f\u043d\u0432\u0430\u0440\u044c"],
        2: ["\u0444\u0435\u0432\u0440\u0430\u043b\u044f", "\u0444\u0435\u0432\u0440\u0430\u043b\u044c"],
        3: ["\u043c\u0430\u0440\u0442\u0430", "\u043c\u0430\u0440\u0442"],
        4: ["\u0430\u043f\u0440\u0435\u043b\u044f", "\u0430\u043f\u0440\u0435\u043b\u044c"],
        5: ["\u043c\u0430\u044f", "\u043c\u0430\u0439"],
        6: ["\u0438\u044e\u043d\u044f", "\u0438\u044e\u043d\u044c"],
        7: ["\u0438\u044e\u043b\u044f", "\u0438\u044e\u043b\u044c"],
        8: ["\u0430\u0432\u0433\u0443\u0441\u0442\u0430", "\u0430\u0432\u0433\u0443\u0441\u0442"],
        9: ["\u0441\u0435\u043d\u0442\u044f\u0431\u0440\u044f", "\u0441\u0435\u043d\u0442\u044f\u0431\u0440\u044c"],
        10: ["\u043e\u043a\u0442\u044f\u0431\u0440\u044f", "\u043e\u043a\u0442\u044f\u0431\u0440\u044c"],
        11: ["\u043d\u043e\u044f\u0431\u0440\u044f", "\u043d\u043e\u044f\u0431\u0440\u044c"],
        12: ["\u0434\u0435\u043a\u0430\u0431\u0440\u044f", "\u0434\u0435\u043a\u0430\u0431\u0440\u044c"],
    }
    for _m, _names in _months_src.items():
        for _n in _names:
            months_map[_n] = _m

    # DD month YYYY (Russian)
    m = re.match(r"(\d{1,2})\s+(\S+)\s+(\d{4})", cleaned)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        month = months_map.get(month_name)
        if month:
            try:
                return datetime.datetime(year, month, day)
            except ValueError:
                return None

    # YYYY-MM-DD
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", cleaned)
    if m:
        try:
            return datetime.datetime(
                int(m.group(1)), int(m.group(2)), int(m.group(3))
            )
        except ValueError:
            return None

    # DD.MM.YYYY
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", cleaned)
    if m:
        try:
            return datetime.datetime(
                int(m.group(3)), int(m.group(2)), int(m.group(1))
            )
        except ValueError:
            return None

    return None
