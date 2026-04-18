"""Парсер карточки объявления Avito."""

from __future__ import annotations

import datetime
import re
from dataclasses import dataclass

import structlog
from bs4 import BeautifulSoup

from app.utils.exceptions import ParserError
from app.utils.helpers import extract_ad_id_from_url, get_avito_base_url, normalize_price, normalize_url

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
        seller_id: Avito seller ID (из URL профиля).
        seller_url: URL профиля продавца.
        seller_type: Тип продавца (частный, компания, магазин).
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
    seller_id: str | None = None
    seller_url: str | None = None
    seller_type: str | None = None
    condition: str | None = None
    publication_date: datetime.datetime | None = None
    description: str | None = None


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

        # --- seller_url ---
        seller_url = _safe_extract_attr(soup, "seller_url", [
            '[data-marker="seller-info/link"]',
            'a[href*="/user/"]',
            'a[href*="/u/"]',
            '[data-marker*="seller"] a[href]',
            'a[href*="/profile/"]',
            '[class*="seller-info"] a[href]',
            '[class*="SellerInfo"] a[href]',
            '[data-marker="seller-info/name"]',
        ], attr="href")

        # Fallback: поиск seller URL в JSON-LD или скриптах страницы
        if not seller_url:
            seller_url = _extract_seller_url_from_scripts(soup)

        # Fallback: парсинг __NEXT_DATA__ (React/Next)
        next_data_seller = _extract_seller_data_from_next_data(soup)

        # Если seller_url всё ещё не найден — попробовать из __NEXT_DATA__
        if not seller_url:
            seller_url = next_data_seller.get("seller_url")

        # Если найдена относительная ссылка, добавить домен
        if seller_url and seller_url.startswith("/"):
            seller_url = get_avito_base_url() + seller_url

        # --- seller_id ---
        seller_id: str | None = None
        if seller_url:
            # Паттерн /user/SELLER_ID
            match = re.search(r"/user/([^/?#]+)", seller_url)
            if match:
                seller_id = match.group(1)
            else:
                # Паттерн /u/SELLER_ID (новый формат Avito)
                match = re.search(r"/u/([^/?#]+)", seller_url)
                if match:
                    seller_id = match.group(1)

        # Если seller_id не найден через URL — попробовать из __NEXT_DATA__
        if not seller_id:
            seller_id = next_data_seller.get("seller_id")

        # --- seller_type ---
        seller_type: str | None = None
        try:
            # Сначала из HTML-блока продавца
            seller_type = _extract_seller_type(soup)
        except Exception:
            pass
        if not seller_type:
            # Fallback из __NEXT_DATA__
            seller_type = next_data_seller.get("seller_type")

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
            seller_id=seller_id,
            seller_url=seller_url,
            seller_type=seller_type,
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
            seller_id=seller_id,
            seller_url=seller_url,
            seller_type=seller_type,
        )

        if seller_id is None:
            log.debug(
                "ad_page_parsed_no_seller",
                url=url,
                ad_id=ad_id,
                seller_name=seller_name,
            )

        return ad_data

    except Exception as exc:
        raise ParserError(
            f"Failed to parse ad page {url}: {exc}"
        ) from exc


def _extract_seller_url_from_scripts(soup: BeautifulSoup) -> str | None:
    """Извлечь URL профиля продавца из встроенных скриптов страницы.

    Avito часто встраивает данные о продавце в JSON внутри <script> тегов.
    Ищет URL профиля продавца в формате /user/... или /u/... .

    Args:
        soup: BeautifulSoup объект страницы.

    Returns:
        str | None: URL профиля продавца или ``None``.
    """
    import json

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            text = script.get_text(strip=True)
            if not text:
                continue
            data = json.loads(text)
            # Поиск seller URL в JSON-LD
            if isinstance(data, dict):
                seller = data.get("seller", {})
                if isinstance(seller, dict):
                    url_val = seller.get("url") or seller.get("@id")
                    if url_val and ("/user/" in str(url_val) or "/u/" in str(url_val)):
                        return str(url_val)
        except (json.JSONDecodeError, AttributeError, TypeError):
            continue

    # Поиск в обычных <script> тегах
    for script in soup.find_all("script"):
        try:
            text = script.get_text()
            if not text:
                continue
            # Ищем паттерн /user/HASH или /u/HASH в JavaScript
            match = re.search(r'["\'](?:https?://(?:www\.)?avito\.ru)?(/(?:user|u)/[^"\'/?#]+)', text)
            if match:
                return get_avito_base_url() + match.group(1)
        except Exception:
            continue

    return None


def _extract_seller_data_from_next_data(
    soup: BeautifulSoup,
) -> dict[str, str | None]:
    """Извлечь данные продавца из ``<script id=\"__NEXT_DATA__\">``.

    Avito (React/Next) встраивает начальный state в JSON внутри тега
    ``<script id="__NEXT_DATA__">``.  Функция рекурсивно ищет в этом JSON
    поля ``seller``, ``owner``, ``userId``, ``profileUrl``, ``ownerId``.

    Args:
        soup: BeautifulSoup объект страницы.

    Returns:
        dict: Словарь с ключами ``seller_id``, ``seller_url``, ``seller_type``.
              Значения — строки или ``None``.
    """
    import json

    result: dict[str, str | None] = {
        "seller_id": None,
        "seller_url": None,
        "seller_type": None,
    }

    try:
        script_tag = soup.select_one('script[id="__NEXT_DATA__"]')
        if script_tag is None:
            log.debug("next_data_script_not_found")
            return result

        text = script_tag.get_text(strip=True)
        if not text:
            return result

        data = json.loads(text)
    except (json.JSONDecodeError, AttributeError, TypeError) as exc:
        log.debug("next_data_parse_error", error=str(exc))
        return result

    # Рекурсивный поиск нужных полей в JSON
    _found: dict[str, str] = {}

    def _walk(obj: object, depth: int = 0) -> None:
        """Рекурсивно обойти JSON и собрать seller-поля."""
        if depth > 15 or len(_found) >= 5:
            return
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, str):
                    low = key.lower()
                    if low in ("userid", "ownerid", "sellerid") and "id" not in _found:
                        _found["id"] = value
                    elif low in ("profileurl", "sellerurl", "ownerurl") and "url" not in _found:
                        if "/user/" in value or "/u/" in value or "/profile/" in value:
                            _found["url"] = value
                elif isinstance(value, (dict, list)):
                    _walk(value, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item, depth + 1)

    try:
        _walk(data)
    except Exception as exc:
        log.debug("next_data_walk_error", error=str(exc))

    # Извлечь seller_id
    if "id" in _found:
        result["seller_id"] = _found["id"]

    # Извлечь seller_url
    if "url" in _found:
        url_val = _found["url"]
        if url_val.startswith("/"):
            url_val = get_avito_base_url() + url_val
        result["seller_url"] = url_val

    # Попытка извлечь seller_type из __NEXT_DATA__
    try:
        _type = _extract_seller_type_from_json(data)
        if _type:
            result["seller_type"] = _type
    except Exception:
        pass

    return result


def _extract_seller_type_from_json(data: object) -> str | None:
    """Попытаться извлечь тип продавца из JSON-структуры __NEXT_DATA__.

    Ищет поля ``sellerType``, ``type``, ``accountType`` и нормализует.

    Args:
        data: Распарсенный JSON.

    Returns:
        str | None: ``private``, ``company``, ``shop`` или ``None``.
    """
    if not isinstance(data, dict):
        return None

    # Прямой поиск в seller-подобъекте
    for key in ("seller", "owner"):
        obj = data.get(key)
        if isinstance(obj, dict):
            for field in ("sellerType", "type", "accountType"):
                val = obj.get(field)
                if isinstance(val, str):
                    normalized = _normalize_seller_type(val)
                    if normalized:
                        return normalized

    # Рекурсивный поиск
    for value in data.values():
        if isinstance(value, dict):
            result = _extract_seller_type_from_json(value)
            if result:
                return result

    return None


def _normalize_seller_type(raw: str) -> str | None:
    """Нормализовать строку типа продавца к enum-значению.

    Args:
        raw: Сырая строка (например «Частное лицо», «Компания»).

    Returns:
        str | None: ``private``, ``company``, ``shop`` или ``None``.
    """
    if not raw:
        return None
    low = raw.strip().lower()
    if low in ("частное лицо", "private", "личное"):
        return "private"
    if low in ("компания", "company", "юридическое лицо", "юридическое"):
        return "company"
    if low in ("магазин", "shop", "store", "торговая площадка"):
        return "shop"
    # Частичные совпадения
    if "частн" in low or "private" in low:
        return "private"
    if "компан" in low or "коммерч" in low or "company" in low:
        return "company"
    if "магазин" in low or "shop" in low or "store" in low:
        return "shop"
    return None


def _extract_seller_type(soup: BeautifulSoup) -> str | None:
    """Извлечь тип продавца из HTML-блока продавца.

    Ищет текст в блоках с data-marker или class, содержащими «seller»,
    и нормализует к ``private``, ``company`` или ``shop``.

    Args:
        soup: BeautifulSoup объект страницы.

    Returns:
        str | None: ``private``, ``company``, ``shop`` или ``None``.
    """
    selectors = [
        '[data-marker*="seller"]',
        '[class*="seller"]',
        '[class*="Seller"]',
    ]
    for selector in selectors:
        try:
            elements = soup.select(selector)
            for el in elements:
                text = el.get_text(separator=" ", strip=True)
                if not text:
                    continue
                seller_type = _normalize_seller_type(text)
                if seller_type:
                    return seller_type
        except Exception:
            continue

    return None


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


def _safe_extract_attr(
    soup: BeautifulSoup,
    field_name: str,
    selectors: list[str],
    attr: str = "href",
) -> str | None:
    """Безопасно извлечь атрибут элемента по списку fallback-селекторов.

    Перебирает селекторы по приоритету. Логирует warning, если ни один
    селектор не сработал.

    Args:
        soup: BeautifulSoup объект страницы.
        field_name: Имя поля (для логирования).
        selectors: Список CSS-селекторов по приоритету.
        attr: Имя атрибута для извлечения (по умолчанию ``href``).

    Returns:
        str | None: Значение атрибута первого найденного элемента или ``None``.
    """
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                value = element.get(attr, "")
                if value:
                    return str(value)
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
