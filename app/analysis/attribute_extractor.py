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
    "телевизоры": {
        "samsung": {
            "patterns": ["samsung", "самсунг"],
            "models": [
                "qe55qn90b", "qe65qn90b", "ue55au7100", "ue43tu7097",
            ],
        },
        "lg": {
            "patterns": ["lg", "элджи"],
            "models": [
                "oled55c16la", "oled55b16la", "55nano776pa", "43up75006lf",
            ],
        },
        "sony": {
            "patterns": ["sony", "сони"],
            "models": [
                "kd55xr70", "kd65xr70", "kd55x80j", "kd65x90j",
            ],
        },
        "xiaomi": {
            "patterns": ["xiaomi", "сяоми", "mi tv", "redmi tv"],
            "models": [
                "mi tv p1 55", "mi tv q1e 55", "redmi tv 55",
            ],
        },
        "hisense": {
            "patterns": ["hisense", "хайсенс"],
            "models": [
                "55u7kqf", "65u7kqf", "55e7kqf",
            ],
        },
    },
    "велосипеды": {
        "stels": {
            "patterns": ["stels", "стелс"],
            "models": [
                "navigator", "pilot", "jet", "challenge",
            ],
        },
        "merida": {
            "patterns": ["merida", "мерида"],
            "models": [
                "big seven", "big nine", "duke", "matts",
            ],
        },
        "trek": {
            "patterns": ["trek"],
            "models": [
                "marlin", "x-caliber", "fuel ex", "domane",
            ],
        },
        "forward": {
            "patterns": ["forward", "форвард"],
            "models": [
                "impulse", "apex", "sport", "next",
            ],
        },
        "altair": {
            "patterns": ["altair", "альтаир"],
            "models": [
                "mtb", "city", "teen",
            ],
        },
    },
    "саундбары": {
        "samsung": {
            "patterns": ["samsung", "самсунг"],
            "models": [
                "hw q990b", "hw q930b", "hw q800b", "hw q700b", "hw s60b", "hw b650",
            ],
        },
        "lg": {
            "patterns": ["lg", "элджи"],
            "models": [
                "s95qr", "s80qy", "sp8ya", "sn11rg",
            ],
        },
        "sony": {
            "patterns": ["sony", "сони"],
            "models": [
                "ht a7000", "ht a5000", "ht g700", "ht st5000",
            ],
        },
        "jbl": {
            "patterns": ["jbl"],
            "models": [
                "bar 9.1", "bar 5.1", "bar 800", "bar 500",
            ],
        },
        "yamaha": {
            "patterns": ["yamaha", "ямаха"],
            "models": [
                "sr c30a", "yas 209", "music cast 220",
            ],
        },
    },
    "тв приставки": {
        "nvidia": {
            "patterns": ["nvidia", "нвидиа", "shield"],
            "models": [
                "shield tv 2019", "shield tv pro 2019", "shield tv 2017", "shield tv pro 2017",
            ],
        },
        "apple": {
            "patterns": ["apple", "apple tv", "эпл"],
            "models": [
                "apple tv 4k", "apple tv hd",
            ],
        },
        "xiaomi": {
            "patterns": ["xiaomi", "сяоми", "mi box", "mi tv stick"],
            "models": [
                "mi box s", "mi tv stick", "mi box 4k",
            ],
        },
        "google": {
            "patterns": ["google", "chromecast"],
            "models": [
                "chromecast with google tv", "chromecast 4k",
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
