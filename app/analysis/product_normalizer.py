"""Нормализатор названий товаров v2 для группировки объявлений.

Превращает сырой заголовок объявления (title) в стабильный ключ
``normalized_key``, по которому можно группировать однородные товары
и строить историю цен.

Алгоритм:
    1. Очистка и токенизация
    2. Удаление шумовых слов
    3. Product-specific правила (приоритетный путь)
    4. Извлечение бренда
    5. Извлечение модели (бренд-специфичные + общие паттерны)
    6. Извлечение атрибутов (год, объём, размер экрана)
    7. Сборка canonical key

Пример:
    "iPhone 13 128GB Черный Новый" → "apple_iphone_13_128"
    "Айфон 13 128 гб"             → "apple_iphone_13_128"
    "NVIDIA Shield TV Pro 2019"    → "nvidia_shield_tv_pro_2019"
    "JBL PartyBox 520"             → "jbl_partybox_520"
"""

from __future__ import annotations

import re
import structlog
from dataclasses import dataclass

from app.analysis.normalizer_data import (
    BRAND_ALIASES,
    BRAND_MODEL_PATTERNS,
    MODEL_MODIFIERS,
    NOISE_WORDS_SET,
    PRODUCT_RULES,
)

logger = structlog.get_logger("product_normalizer")

# ---------------------------------------------------------------------------
# Regex-паттерны
# ---------------------------------------------------------------------------

# Спецсимволы (кроме дефисов и точек в числах)
_CLEANUP_RE = re.compile(r"[^\w\s.\-]")

# Множественные пробелы
_MULTI_SPACE_RE = re.compile(r"\s+")

# Ёмкость / объём: 128GB, 256 ГБ, 1TB, 512гб
_CAPACITY_RE = re.compile(r"(\d+)\s*(gb|гб|tb|тб|mb|мб)\b", re.I)

# Размер экрана: 55", 65 дюймов, 55.5 inch
_SCREEN_RE = re.compile(r"(\d+[\.,]?\d*)\s*(?:inch|дюйм|дюймов|\"|'')\b", re.I)

# Год: 2017-2026
_YEAR_RE = re.compile(r"\b(20[12]\d)\b")

# Alpha-numeric токен (буквы + цифры в одном слове)
_ALPHA_NUM_RE = re.compile(r"^[a-z]*\d+[a-z\d]*$", re.I)

# Подчёркивания в ключе
_MULTI_UNDERSCORE_RE = re.compile(r"_+")

# Максимальная длина ключа
_MAX_KEY_LENGTH = 120


# ---------------------------------------------------------------------------
# Результат нормализации
# ---------------------------------------------------------------------------


@dataclass
class NormalizationResult:
    """Результат нормализации названия товара."""

    normalized_key: str
    brand: str | None
    model: str | None
    capacity: str | None
    screen_size: str | None


# ---------------------------------------------------------------------------
# Публичный API
# ---------------------------------------------------------------------------


def normalize_title(title: str) -> NormalizationResult:
    """Нормализовать заголовок объявления в ключ товара.

    Args:
        title: Сырой заголовок объявления Avito.

    Returns:
        NormalizationResult с normalized_key и извлечёнными атрибутами.
    """
    if not title:
        return NormalizationResult(
            normalized_key="unknown",
            brand=None,
            model=None,
            capacity=None,
            screen_size=None,
        )

    # --- Этап 1: Очистка и токенизация ---
    cleaned = _CLEANUP_RE.sub(" ", title)
    cleaned = _MULTI_SPACE_RE.sub(" ", cleaned).strip()
    tokens = cleaned.lower().split()

    if not tokens:
        return NormalizationResult(
            normalized_key="unknown",
            brand=None,
            model=None,
            capacity=None,
            screen_size=None,
        )

    # --- Этап 2: Извлечение атрибутов ДО удаления шума ---
    capacity = _extract_capacity(title)
    screen_size = _extract_screen_size(title)
    year = _extract_year(tokens)

    # --- Этап 3: Удаление шумовых слов ---
    clean_tokens = _remove_noise(tokens)

    # --- Этап 4: Product-specific правила ---
    text_for_rules = " ".join(clean_tokens)
    rule_result = _apply_product_rules(text_for_rules)
    if rule_result is not None:
        # Добавляем год/ёмкость к product rule ключу
        key = rule_result.canonical_key
        if year and year not in key:
            key = f"{key}_{year}"
        if capacity and capacity not in key:
            key = f"{key}_{capacity}"
        return NormalizationResult(
            normalized_key=_finalize_key(key),
            brand=rule_result.brand,
            model=rule_result.model,
            capacity=capacity,
            screen_size=screen_size,
        )

    # --- Этап 5: Извлечение бренда ---
    brand, remaining_tokens = _extract_brand(clean_tokens)

    # --- Этап 6: Извлечение модели ---
    # Передаём clean_tokens (до удаления бренда) для бренд-специфичных паттернов,
    # и remaining_tokens — для fallback
    model = _extract_model(clean_tokens, remaining_tokens, brand)

    # --- Этап 7: Сборка canonical key ---
    parts: list[str] = []

    if brand:
        parts.append(brand)
    if model:
        parts.append(model)
    if year and model and year not in model:
        parts.append(year)
    if capacity and model and capacity not in model:
        parts.append(capacity)
    elif screen_size and not capacity:
        parts.append(f"{screen_size}in")

    if not parts:
        # Fallback: используем очищенные токены (обрезанные)
        key = "_".join(clean_tokens[:10])[:_MAX_KEY_LENGTH]
        return NormalizationResult(
            normalized_key=_finalize_key(key) or "unknown",
            brand=brand,
            model=model,
            capacity=capacity,
            screen_size=screen_size,
        )

    key = "_".join(parts)
    return NormalizationResult(
        normalized_key=_finalize_key(key),
        brand=brand,
        model=model,
        capacity=capacity,
        screen_size=screen_size,
    )


# ---------------------------------------------------------------------------
# Внутренние функции
# ---------------------------------------------------------------------------


def _remove_noise(tokens: list[str]) -> list[str]:
    """Удалить шумовые слова из списка токенов."""
    return [t for t in tokens if t not in NOISE_WORDS_SET and len(t) > 0]


def _extract_capacity(text: str) -> str | None:
    """Извлечь объём памяти из исходного текста."""
    match = _CAPACITY_RE.search(text)
    if match:
        return match.group(0).lower().replace(" ", "")
    return None


def _extract_screen_size(text: str) -> str | None:
    """Извлечь размер экрана из исходного текста."""
    match = _SCREEN_RE.search(text)
    if match:
        return match.group(1).replace(",", ".")
    return None


def _extract_year(tokens: list[str]) -> str | None:
    """Извлечь год из токенов (2017-2026)."""
    for token in tokens:
        if _YEAR_RE.match(token):
            return token
    return None


def _apply_product_rules(text: str):
    """Проверить, подходит ли заголовок под известный продукт.

    Returns:
        ProductRule или None.
    """
    for rule in PRODUCT_RULES:
        if rule.pattern.search(text):
            return rule
    return None


def _extract_brand(tokens: list[str]) -> tuple[str | None, list[str]]:
    """Извлечь бренд и вернуть оставшиеся токены.

    Returns:
        (brand_canonical_name | None, remaining_tokens)
    """
    remaining = list(tokens)
    found_brand = None

    # Проходим по токенам и ищем бренд
    for i, token in enumerate(remaining):
        clean = token.strip("()-,.")
        if clean in BRAND_ALIASES:
            found_brand = BRAND_ALIASES[clean]
            # Удаляем бренд-токен(ы) из remaining
            remaining.pop(i)
            return found_brand, remaining

    return None, remaining


def _extract_model(
    all_tokens: list[str],
    remaining_tokens: list[str],
    brand: str | None,
) -> str | None:
    """Извлечь модель из токенов.

    Args:
        all_tokens: Все токены (до удаления бренда) — для бренд-специфичных паттернов.
        remaining_tokens: Токены после удаления бренда — для fallback.
        brand: Каноническое имя бренда.
    """
    if not remaining_tokens and not all_tokens:
        return None

    # --- Приоритет 1: Бренд-специфичные паттерны (ищем в полном тексте) ---
    if brand and brand in BRAND_MODEL_PATTERNS:
        full_text = " ".join(all_tokens)
        for pattern, normalizer in BRAND_MODEL_PATTERNS[brand]:
            match = pattern.search(full_text)
            if match:
                try:
                    return normalizer(match)
                except Exception:
                    return match.group(0).lower().replace(" ", "_")

    # --- Приоритет 2: Жадный сбор значимых токенов ---
    model_parts: list[str] = []
    brand_values = set(BRAND_ALIASES.values())

    for token in remaining_tokens:
        clean = token.strip("()-,.")
        # Пропускаем бренды
        if clean in BRAND_ALIASES or clean in brand_values:
            continue
        # Пропускаем шум
        if clean in NOISE_WORDS_SET:
            continue
        # Alpha-numeric (модельный номер)
        if _ALPHA_NUM_RE.match(clean):
            model_parts.append(clean)
        # Известный модификатор
        elif clean in MODEL_MODIFIERS:
            model_parts.append(clean)
        # Год
        elif _YEAR_RE.match(clean):
            model_parts.append(clean)

    if model_parts:
        return "_".join(model_parts)

    # --- Приоритет 3: Fallback — первый значимый токен ---
    for token in remaining_tokens:
        clean = token.strip("()-,.")
        if clean not in NOISE_WORDS_SET and len(clean) >= 2:
            return clean

    return None


def _finalize_key(key: str) -> str:
    """Очистить и ограничить длину ключа."""
    key = key.lower().strip()
    key = _MULTI_UNDERSCORE_RE.sub("_", key)
    key = key.strip("_")
    if len(key) > _MAX_KEY_LENGTH:
        key = key[:_MAX_KEY_LENGTH]
        # Обрезаем по последнее подчёркивание, чтобы не обрывать слово
        last_underscore = key.rfind("_")
        if last_underscore > _MAX_KEY_LENGTH // 2:
            key = key[:last_underscore]
    return key
