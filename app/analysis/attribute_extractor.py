"""Извлечение атрибутов (бренд, модель, категория, состояние) из заголовка объявления.

Без ML — на основе словарей и regex-паттернов.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import structlog


# ---------------------------------------------------------------------------
# Ключевые слова для определения состояния
# ---------------------------------------------------------------------------

CONDITION_KEYWORDS: dict[str, list[str]] = {
    "new": [
        "новый", "новая", "новое", "новые", "new",
        "в упаковке", "заводская упаковка", "нераспакованный",
        "запечатанный", "с завода",
    ],
    "used": [
        "б/у", "бу", "б/у.", "подержанный", "used",
        "с пробегом", "не новый", "бывший в употреблении",
    ],
}


# ---------------------------------------------------------------------------
# Словари брендов/моделей для стартовых категорий
# ---------------------------------------------------------------------------

DEFAULT_BRAND_DICTIONARIES: dict[str, dict[str, dict]] = {
    "телефоны": {
        "apple": {
            "patterns": ["iphone", "айфон"],
            "models": [
                "iphone 16 pro max", "iphone 16 pro", "iphone 16 plus", "iphone 16",
                "iphone 15 pro max", "iphone 15 pro", "iphone 15 plus", "iphone 15",
                "iphone 14 pro max", "iphone 14 pro", "iphone 14 plus", "iphone 14",
                "iphone 13 pro max", "iphone 13 pro", "iphone 13 mini", "iphone 13",
                "iphone 12 pro max", "iphone 12 pro", "iphone 12 mini", "iphone 12",
                "iphone 11 pro max", "iphone 11 pro", "iphone 11",
                "iphone se", "iphone xs", "iphone xr", "iphone x",
            ],
        },
        "samsung": {
            "patterns": ["samsung", "самсунг", "galaxy"],
            "models": [
                "galaxy s24 ultra", "galaxy s24+", "galaxy s24",
                "galaxy s23 ultra", "galaxy s23+", "galaxy s23",
                "galaxy s22 ultra", "galaxy s22+", "galaxy s22",
                "galaxy a55", "galaxy a54", "galaxy a53",
                "galaxy a35", "galaxy a34", "galaxy a33",
                "galaxy z flip", "galaxy z fold",
            ],
        },
        "xiaomi": {
            "patterns": ["xiaomi", "сяоми", "redmi", "poco"],
            "models": [
                "redmi note 13 pro+", "redmi note 13 pro", "redmi note 13",
                "redmi 13c", "redmi 13",
                "poco x6 pro", "poco x6", "poco m6 pro",
                "xiaomi 14", "xiaomi 13t", "xiaomi 13",
            ],
        },
        "google": {
            "patterns": ["pixel", "google pixel"],
            "models": [
                "pixel 9 pro", "pixel 9",
                "pixel 8 pro", "pixel 8",
                "pixel 7 pro", "pixel 7",
                "pixel 6 pro", "pixel 6",
            ],
        },
    },
    "ноутбуки": {
        "apple": {
            "patterns": ["macbook", "макбук"],
            "models": [
                "macbook pro 16 m3", "macbook pro 16 m2", "macbook pro 16 m1",
                "macbook pro 14 m3", "macbook pro 14 m2", "macbook pro 14 m1",
                "macbook air 15 m3", "macbook air 15 m2",
                "macbook air 13 m3", "macbook air 13 m2", "macbook air 13 m1",
            ],
        },
        "lenovo": {
            "patterns": ["lenovo", "thinkpad", "legion", "ideapad"],
            "models": [
                "thinkpad x1 carbon", "thinkpad t14", "thinkpad t16",
                "legion 5 pro", "legion 5", "legion 7",
                "ideapad 5", "ideapad 3", "ideapad gaming",
            ],
        },
        "asus": {
            "patterns": ["asus", "rog", "zenbook", "vivobook"],
            "models": [
                "rog strix g16", "rog strix g18", "rog zephyrus g14", "rog zephyrus g16",
                "zenbook 14", "zenbook 15", "vivobook 15", "vivobook 16",
            ],
        },
    },
    "велосипеды": {
        "stels": {
            "patterns": ["stels", "стелс"],
            "models": [
                "navigator 800", "navigator 760", "navigator 630",
                "pilot 950", "pilot 710",
                "aggressor", "challenge", "energy",
            ],
        },
        "merida": {
            "patterns": ["merida", "мерида"],
            "models": [
                "big nine 15", "big nine 20", "big seven 15",
                "matts 6.5", "matts 6.2",
                "scultura 4000", "reacto 4000",
            ],
        },
        "trek": {
            "patterns": ["trek"],
            "models": [
                "marlin 7", "marlin 6", "marlin 5",
                "x-caliber 8", "x-caliber 7",
                "domane al 2", "emonda alr",
            ],
        },
        "forward": {
            "patterns": ["forward", "форвард"],
            "models": [
                "impulse x", "impulse 29", "apex",
                "sport 2.0", "sport 3.0", "trail",
            ],
        },
        "altair": {
            "patterns": ["altair", "альтаир"],
            "models": [
                "mtb ht 27.5", "mtb ht 29", "city 26",
            ],
        },
    },
    "шины": {
        "michelin": {
            "patterns": ["michelin", "мишлен"],
            "models": [
                "x-ice north 4", "x-ice north 3", "x-ice 3",
                "pilot sport 4", "pilot sport 5",
                "primacy 4", "energy saver",
                "crossclimate 2",
            ],
        },
        "nokian": {
            "patterns": ["nokian", "хакка", "hakka"],
            "models": [
                "hakkapeliitta r5", "hakkapeliitta r4",
                "hakkapeliitta 9", "hakkapeliitta 8",
                "nordman rs2", "nordman 7",
                "wr d4", "wr a4",
            ],
        },
        "continental": {
            "patterns": ["continental", "континенталь"],
            "models": [
                "wintercontact ts870", "wintercontact ts860",
                "premiumcontact 7", "premiumcontact 6",
                "ecocontact 6",
            ],
        },
        "pirelli": {
            "patterns": ["pirelli", "пирелли"],
            "models": [
                "winter sottozero 3", "ice zero",
                "cinturato p7", "scorpion verde",
            ],
        },
        "kama": {
            "patterns": ["kama", "кама"],
            "models": [
                "euro-519", "euro-129", "breeze-131",
                "hk-131", "flame-131",
            ],
        },
    },
}


# ---------------------------------------------------------------------------
# Dataclass результата
# ---------------------------------------------------------------------------

@dataclass
class ExtractedAttributes:
    """Результат извлечения атрибутов из заголовка объявления."""

    category: str | None = None
    brand: str | None = None
    model: str | None = None
    condition: str | None = None
    confidence: float = 0.0
    raw: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# AttributeExtractor
# ---------------------------------------------------------------------------

class AttributeExtractor:
    """Извлечение атрибутов из заголовка объявления без ML.

    Использует словари брендов/моделей и regex-паттерны для определения
    категории, бренда, модели и состояния товара из текста заголовка.

    Args:
        brand_dicts: Словарь категорий → бренды → модели.
            Если ``None`` — используется ``DEFAULT_BRAND_DICTIONARIES``.
    """

    def __init__(
        self,
        brand_dicts: dict[str, dict[str, dict]] | None = None,
    ) -> None:
        self._log = structlog.get_logger("attribute_extractor")
        self._brand_dicts = brand_dicts or DEFAULT_BRAND_DICTIONARIES
        self._compiled_patterns = self._compile_patterns()

    def _compile_patterns(self) -> dict[str, dict[str, list[re.Pattern]]]:
        """Скомпилировать regex-паттерны для каждого бренда."""
        compiled: dict[str, dict[str, list[re.Pattern]]] = {}
        for category, brands in self._brand_dicts.items():
            compiled[category] = {}
            for brand_key, brand_data in brands.items():
                patterns = [
                    re.compile(r"\b" + re.escape(p) + r"\b", re.IGNORECASE)
                    for p in brand_data.get("patterns", [])
                ]
                compiled[category][brand_key] = patterns
        return compiled

    def extract(
        self,
        title: str,
        search_category: str | None = None,
    ) -> ExtractedAttributes:
        """Извлечь атрибуты из заголовка объявления.

        Args:
            title: Заголовок объявления.
            search_category: Категория поиска (если известна).

        Returns:
            :class:`ExtractedAttributes` с извлечёнными атрибутами.
        """
        if not title:
            return ExtractedAttributes()

        title_lower = title.strip().lower()
        raw: dict = {"title": title, "title_lower": title_lower}

        # Шаг 1: Определяем бренд
        brand, matched_category = self._detect_brand(title_lower, search_category)
        raw["brand_detected"] = brand
        raw["category_detected"] = matched_category

        # Шаг 2: Извлекаем модель
        model = None
        if brand and matched_category:
            model = self._detect_model(title_lower, matched_category, brand)
        raw["model_detected"] = model

        # Шаг 3: Определяем состояние
        condition = self._detect_condition(title_lower)
        raw["condition_detected"] = condition

        # Рассчитываем уверенность
        confidence = 0.0
        if brand:
            confidence += 0.4
        if model:
            confidence += 0.4
        if condition:
            confidence += 0.2

        result = ExtractedAttributes(
            category=matched_category,
            brand=brand,
            model=model,
            condition=condition,
            confidence=confidence,
            raw=raw,
        )

        self._log.debug(
            "attributes_extracted",
            title=title[:80],
            category=result.category,
            brand=result.brand,
            model=result.model,
            condition=result.condition,
            confidence=result.confidence,
        )
        return result

    def _detect_brand(
        self,
        title_lower: str,
        search_category: str | None = None,
    ) -> tuple[str | None, str | None]:
        """Определить бренд и категорию по словарю.

        Args:
            title_lower: Нормализованный заголовок (lowercase).
            search_category: Категория поиска (если известна).

        Returns:
            Кортеж ``(brand_key, category)`` или ``(None, None)``.
        """
        # Если категория известна — ищем только в ней
        if search_category and search_category in self._compiled_patterns:
            brand = self._match_brand_in_category(title_lower, search_category)
            if brand:
                return brand, search_category

        # Ищем по всем категориям
        for category, brands in self._compiled_patterns.items():
            if category == search_category:
                continue  # Уже проверили
            brand = self._match_brand_in_category(title_lower, category)
            if brand:
                return brand, category

        return None, None

    def _match_brand_in_category(
        self,
        title_lower: str,
        category: str,
    ) -> str | None:
        """Проверить совпадение бренда в конкретной категории.

        Args:
            title_lower: Нормализованный заголовок.
            category: Категория для поиска.

        Returns:
            Ключ бренда или ``None``.
        """
        brands = self._compiled_patterns.get(category, {})
        for brand_key, patterns in brands.items():
            for pattern in patterns:
                if pattern.search(title_lower):
                    return brand_key
        return None

    def _detect_model(
        self,
        title_lower: str,
        category: str,
        brand: str,
    ) -> str | None:
        """Извлечь модель из заголовка.

        Сначала проверяет по словарю моделей (от длинных к коротким),
        затем пытается извлечь остаток после бренда.

        Args:
            title_lower: Нормализованный заголовок.
            category: Категория.
            brand: Ключ бренда.

        Returns:
            Строка модели или ``None``.
        """
        brand_data = self._brand_dicts.get(category, {}).get(brand, {})
        models = brand_data.get("models", [])

        # Сортируем модели по длине (от длинных к коротким) для точного match
        sorted_models = sorted(models, key=len, reverse=True)

        for model_str in sorted_models:
            model_lower = model_str.lower()
            if model_lower in title_lower:
                return model_lower

        # Fallback: извлечь остаток после бренда
        brand_patterns = brand_data.get("patterns", [])
        for bp in brand_patterns:
            bp_lower = bp.lower()
            idx = title_lower.find(bp_lower)
            if idx >= 0:
                # Берём текст после бренда
                after_brand = title_lower[idx + len(bp_lower):].strip()
                # Очищаем от лишних символов
                after_brand = re.sub(r"^[^\w]+", "", after_brand).strip()
                if after_brand:
                    # Ограничиваем длину и убираем спецсимволы
                    after_brand = after_brand[:100]
                    after_brand = re.sub(r"[^\w\s\-\+\.]", "", after_brand).strip()
                    if after_brand:
                        return after_brand

        return None

    def _detect_condition(self, title_lower: str) -> str | None:
        """Определить состояние товара по ключевым словам.

        Args:
            title_lower: Нормализованный заголовок.

        Returns:
            ``"new"``, ``"used"`` или ``None``.
        """
        for condition, keywords in CONDITION_KEYWORDS.items():
            for keyword in keywords:
                if keyword in title_lower:
                    return condition
        return None
