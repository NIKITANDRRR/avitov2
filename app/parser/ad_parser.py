"""Парсер карточки объявления Avito."""

from __future__ import annotations

import datetime
import re
from dataclasses import dataclass

import structlog
from bs4 import BeautifulSoup

from app.utils.exceptions import ParserError
from app.utils.helpers import extract_ad_id_from_url, normalize_price, normalize_url

log = structlog.get_logger()


@dataclass
class AdData:
    """Результат парсинга карточки объявления.

    Attributes:
        ad_id: Идентификатор объявления.
        url: URL объявления.
        title: Заголовок объявления.
        price: Нормализованная цена (число).
        price_str: Сырая строка цены.
        location: Местоположение / адрес.
        seller_name: Имя продавца.
        condition: Состояние товара (Новое / Б/у).
        publication_date: Дата публикации (datetime или None).
        description: Описание объявления.
    """

    ad_id: str
    url: str
    title: str | None
    price: float | None
    price_str: str | None
    location: str | None
    seller_name: str | None
    condition: str | None
    publication_date: datetime.datetime | None
    description: str | None


def parse_ad_page(html: str, url: str) -> AdData:
    """Парсит HTML карточки объявления Avito.

    Извлекает данные объявления из HTML-контента карточки.
    Каждое поле парсится отдельно в try/except — отсутствие поля
    не приводит к падению всего парсинга.

    Args:
        html: HTML-контент карточки объявления.
        url: URL карточки объявления.

    Returns:
        AdData: Распарсенные данные объявления.

    Raises:
        ParserError: При критической ошибке парсинга.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
        normalized_url = normalize_url(url)

        # --- ad_id ---
        try:
            ad_id = extract_ad_id_from_url(normalized_url)
        except ValueError:
            log.warning("cannot_extract_ad_id", url=url)
            ad_id = ""

        # --- title ---
        title = _safe_extract(soup, "title", [
            '[data-marker="item-view/title-info"]',
            '[data-marker="item-view/item-title"]',
            'h1[itemprop="name"]',
            "h1",
        ])

        # --- price_str ---
        price_str = _safe_extract(soup, "price_str", [
            '[data-marker="item-view/item-price"]',
            '[itemprop="price"]',
            "span.price-value",
            'meta[itemprop="price"]',
        ])
        # Для meta-тега — берём content
        if not price_str:
            price_meta = soup.select_one('meta[itemprop="price"]')
            if price_meta:
                price_str = price_meta.get("content")

        # --- price (нормализованная) ---
        price: float | None = None
        if price_str:
            price = normalize_price(price_str)

        # --- location ---
        location = _safe_extract(soup, "location", [
            '[data-marker="item-view/item-address"]',
            '[itemprop="address"]',
        ])

        # --- seller_name ---
        seller_name = _safe_extract(soup, "seller_name", [
            '[data-marker="seller-info/name"]',
            "a.seller-info-name",
            '[class*="seller-info"] a',
        ])

        # --- condition ---
        condition = _extract_condition(soup)

        # --- publication_date ---
        publication_date_raw = _safe_extract(soup, "publication_date", [
            '[data-marker="item-view/item-date"]',
            "div.date-text",
        ])
        publication_date = normalize_publication_date(publication_date_raw)

        # --- description ---
        description = _safe_extract(soup, "description", [
            '[itemprop="description"]',
            '[data-marker="item-view/item-description"]',
        ])

        ad_data = AdData(
            ad_id=ad_id,
            url=normalized_url,
            title=title,
            price=price,
            price_str=price_str,
            location=location,
            seller_name=seller_name,
            condition=condition,
            publication_date=publication_date,
            description=description,
        )

        log.info(
            "ad_page_parsed",
            url=url,
            ad_id=ad_id,
            has_title=title is not None,
            has_price=price is not None,
            has_location=location is not None,
        )

        return ad_data

    except Exception as exc:
        raise ParserError(
            f"Failed to parse ad page {url}: {exc}"
        ) from exc


def _safe_extract(
    soup: BeautifulSoup,
    field_name: str,
    selectors: list[str],
) -> str | None:
    """Безопасно извлечь текст по списку fallback-селекторов.

    Перебирает селекторы по приоритету. Логирует warning, если ни один
    селектор не сработал.

    Args:
        soup: BeautifulSoup объект страницы.
        field_name: Имя поля (для логирования).
        selectors: Список CSS-селекторов по приоритету.

    Returns:
        str | None: Текст первого найденного элемента или ``None``.
    """
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                # Для meta-тегов берём атрибут content
                if element.name == "meta":
                    text = element.get("content", "")
                else:
                    text = element.get_text(strip=True)
                if text:
                    return text
        except Exception:
            continue

    log.debug("field_not_found", field=field_name)
    return None


def _extract_condition(soup: BeautifulSoup) -> str | None:
    """Извлечь состояние товара из параметров объявления.

    Ищет в блоке параметров строку «Состояние» и извлекает значение.

    Args:
        soup: BeautifulSoup объект страницы.

    Returns:
        str | None: «Новое», «Б/у» или ``None``.
    """
    # Стратегия 1: поиск по data-marker параметров
    params_block = soup.select_one('[data-marker="item-view/item-params"]')
    if params_block:
        try:
            items = params_block.select("li")
            for item in items:
                text = item.get_text(strip=True)
                if "Состояние" in text:
                    # Обычно формат «Состояние: Новое» или «СостояниеНовое»
                    condition = text.replace("Состояние", "").strip(": ").strip()
                    if condition:
                        return condition
        except Exception:
            pass

    # Стратегия 2: поиск по тексту в параметрах
    try:
        for li in soup.select("li"):
            text = li.get_text(strip=True)
            if text.startswith("Состояние"):
                condition = text.replace("Состояние", "").strip(": ").strip()
                if condition:
                    return condition
    except Exception:
        pass

    return None


# ------------------------------------------------------------------
# Нормализация даты публикации
# ------------------------------------------------------------------

# Словарь месяцев: русское название → номер (именительный / родительный)
_MONTHS_MAP: dict[str, int] = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "мая": 5, "июня": 6, "июля": 7, "августа": 8,
    "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
    "январь": 1, "февраль": 2, "март": 3, "апрель": 4,
    "май": 5, "июнь": 6, "июль": 7, "август": 8,
    "сентябрь": 9, "октябрь": 10, "ноябрь": 11, "декабрь": 12,
}


def normalize_publication_date(raw: str | None) -> datetime.datetime | None:
    """Преобразует сырую строку даты Авито в :class:`datetime.datetime`.

    Поддерживаемые форматы::

        · сегодня в HH:MM
        сегодня в HH:MM
        вчера в HH:MM
        N минут/минуты/минуту назад
        N час/часа/часов назад
        DD месяц(я)

    Если строку не удалось распознать, возвращается ``None``.

    Args:
        raw: Сырая строка даты с Авито.

    Returns:
        datetime.datetime | None: Распарсенная дата или ``None``.
    """
    if not raw:
        return None

    # Очистка от ведущих символов ·, —, пробелов
    cleaned = raw.strip().lstrip("·— ").strip()
    now = datetime.datetime.now(datetime.timezone.utc)

    # --- сегодня в HH:MM ---
    m = re.match(r"сегодня\s+в\s+(\d{1,2}):(\d{2})", cleaned)
    if m:
        return now.replace(
            hour=int(m.group(1)), minute=int(m.group(2)),
            second=0, microsecond=0,
        )

    # --- вчера в HH:MM ---
    m = re.match(r"вчера\s+в\s+(\d{1,2}):(\d{2})", cleaned)
    if m:
        yesterday = now - datetime.timedelta(days=1)
        return yesterday.replace(
            hour=int(m.group(1)), minute=int(m.group(2)),
            second=0, microsecond=0,
        )

    # --- N минут/минуты/минуту назад ---
    m = re.match(r"(\d+)\s+(?:минут|минуты|минуту)\s+назад", cleaned)
    if m:
        return now - datetime.timedelta(minutes=int(m.group(1)))

    # --- N час/часа/часов назад ---
    m = re.match(r"(\d+)\s+(?:час|часа|часов)\s+назад", cleaned)
    if m:
        return now - datetime.timedelta(hours=int(m.group(1)))

    # --- DD месяц(я) — например «15 марта» ---
    m = re.match(r"(\d{1,2})\s+(\S+)", cleaned)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        month = _MONTHS_MAP.get(month_name)
        if month:
            try:
                return now.replace(
                    month=month, day=day,
                    hour=0, minute=0, second=0, microsecond=0,
                )
            except ValueError:
                return None

    # Неизвестный формат — не падаем
    return None
