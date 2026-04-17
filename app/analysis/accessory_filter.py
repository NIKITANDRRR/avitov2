"""
Модуль фильтрации аксессуаров и мелочевки.

Отсеивает товары, которые не являются целевыми для мониторинга:
- Аксессуары (чехлы, кабели, клавиатуры и т.д.)
- Запчасти (матрицы, шлейфы, аккумуляторы)
- Мелочёвку с аномально низкой ценой относительно рынка
- Комплекты/наборы (bundle) — несколько товаров по цене одного
"""

import logging
import re
from dataclasses import dataclass, field

from app.storage.models import Ad

logger = logging.getLogger(__name__)


@dataclass
class FilterResult:
    """Результат проверки фильтра аксессуаров."""
    is_filtered: bool
    reason: str = ""


class AccessoryFilter:
    """Комбинированный фильтр аксессуаров и мелочёвки."""

    def __init__(
        self,
        blacklist: list[str] | None = None,
        min_price: int = 5000,
        price_ratio: float = 0.3,
        enabled: bool = True,
    ):
        self.enabled = enabled
        self.blacklist = [w.lower().strip() for w in (blacklist or []) if w.strip()]
        self.min_price = min_price
        self.price_ratio = price_ratio

    def _check_min_price(self, ad: Ad) -> FilterResult:
        """Проверка по минимальной цене."""
        if ad.price is not None and ad.price < self.min_price:
            return FilterResult(
                is_filtered=True,
                reason=f"Цена {ad.price}₽ ниже минимальной {self.min_price}₽"
            )
        return FilterResult(is_filtered=False)

    def _check_blacklist(self, ad: Ad) -> FilterResult:
        """Проверка по чёрному списку ключевых слов в названии."""
        if not ad.title:
            return FilterResult(is_filtered=False)

        title_lower = ad.title.lower()
        for word in self.blacklist:
            if word in title_lower:
                return FilterResult(
                    is_filtered=True,
                    reason=f"Стоп-слово '{word}' в названии: {ad.title}"
                )
        return FilterResult(is_filtered=False)

    def _check_bundle(self, ad: Ad, median_price: float | None = None) -> FilterResult:
        """Проверка на комплект (bundle) — несколько моделей в одном названии.

        Логика:
        1. Ищем в названии числовые идентификаторы (2-4 цифры).
        2. Исключаем числа, которые являются частью спецификаций:
           - объём памяти: ``256GB``, ``512 ГБ``, ``1TB``
           - диагональ: ``13.3``, ``15.6`` (дробные)
           - серия процессора: ``M2``, ``i5``, ``i7``
           - размеры: ``mm``, ``см``
        3. Если осталось ≥2 «модельных» чисел И цена ниже медианы — помечаем как bundle.
        """
        if not ad.title:
            return FilterResult(is_filtered=False)

        # Исключаем числа, являющиеся частью спецификаций (GB, TB, ГБ, ТБ, mm, см, M1-M9, i3-i9)
        title_cleaned = re.sub(
            r'\b\d{2,4}\s*(?:GB|TB|ГБ|ТБ|gb|tb|мм|mm|см|cm)\b',
            '', ad.title, flags=re.IGNORECASE,
        )
        title_cleaned = re.sub(
            r'\b(?:M\d|i\d|Ryzen\s*\d)\b',
            '', title_cleaned, flags=re.IGNORECASE,
        )
        # Исключаем дробные числа (диагонали: 13.3, 15.6, 27)
        title_cleaned = re.sub(r'\b\d+\.\d+\b', '', title_cleaned)

        # Паттерн: два и более чисел (2-4 цифры), разделённых пробелами/дефисами/запятыми
        numbers = re.findall(r'\b(\d{2,4})\b', title_cleaned)
        if len(numbers) < 2:
            return FilterResult(is_filtered=False)

        # Если есть медиана — проверяем, что цена значительно ниже
        if median_price is not None and median_price > 0 and ad.price is not None:
            ratio = ad.price / median_price
            if ratio < self.price_ratio:
                return FilterResult(
                    is_filtered=True,
                    reason=(
                        f"Bundle: {len(numbers)} числовых моделей в названии "
                        f"({', '.join(numbers)}), цена {ad.price}₽ = {ratio:.1%} "
                        f"от медианы {median_price:.0f}₽: {ad.title}"
                    )
                )

        # Без медианы — помечаем bundle только если чисел >= 4
        # (после очистки от спецификаций 3 числа ещё могут быть нормой:
        #  "MacBook Air 13 2022" → [13, 2022] — 2 числа, норма)
        if len(numbers) >= 4:
            return FilterResult(
                is_filtered=True,
                reason=(
                    f"Bundle: {len(numbers)} числовых моделей в названии "
                    f"({', '.join(numbers)}): {ad.title}"
                )
            )

        return FilterResult(is_filtered=False)

    def _check_price_ratio(self, ad: Ad, median_price: float | None = None) -> FilterResult:
        """Проверка по соотношению цены к медиане."""
        if median_price is None or median_price <= 0:
            return FilterResult(is_filtered=False)
        if ad.price is None or ad.price <= 0:
            return FilterResult(is_filtered=False)

        ratio = ad.price / median_price
        if ratio < self.price_ratio:
            return FilterResult(
                is_filtered=True,
                reason=f"Цена {ad.price}₽ составляет {ratio:.1%} от медианы {median_price:.0f}₽ (порог {self.price_ratio:.0%})"
            )
        return FilterResult(is_filtered=False)

    def is_accessory(
        self,
        ad: Ad,
        median_price: float | None = None,
    ) -> FilterResult:
        """
        Проверить, является ли товар аксессуаром/мелочёвкой/комплектом.

        Применяет последовательно четыре фильтра:
        1. Минимальная цена
        2. Чёрный список слов в названии
        3. Комплект (bundle) — несколько моделей в названии
        4. Соотношение цены к медиане (если медиана предоставлена)

        Returns:
            FilterResult с is_filtered=True если товар следует отфильтровать
        """
        if not self.enabled:
            return FilterResult(is_filtered=False)

        # Проверка 1: минимальная цена
        result = self._check_min_price(ad)
        if result.is_filtered:
            logger.debug(
                "ad_filtered_min_price",
                extra={"ad_id": ad.ad_id, "title": ad.title, "reason": result.reason}
            )
            return result

        # Проверка 2: чёрный список
        result = self._check_blacklist(ad)
        if result.is_filtered:
            logger.debug(
                "ad_filtered_blacklist",
                extra={"ad_id": ad.ad_id, "title": ad.title, "reason": result.reason}
            )
            return result

        # Проверка 3: комплект (bundle) — несколько моделей в названии
        result = self._check_bundle(ad, median_price)
        if result.is_filtered:
            logger.debug(
                "ad_filtered_bundle",
                extra={"ad_id": ad.ad_id, "title": ad.title, "reason": result.reason}
            )
            return result

        # Проверка 4: соотношение к медиане (только если медиана предоставлена)
        if median_price is not None:
            result = self._check_price_ratio(ad, median_price)
            if result.is_filtered:
                logger.debug(
                    "ad_filtered_price_ratio",
                    extra={"ad_id": ad.ad_id, "title": ad.title, "reason": result.reason}
                )
                return result

        return FilterResult(is_filtered=False)
