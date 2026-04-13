"""
Модуль фильтрации аксессуаров и мелочевки.

Отсеивает товары, которые не являются целевыми для мониторинга:
- Аксессуары (чехлы, кабели, клавиатуры и т.д.)
- Запчасти (матрицы, шлейфы, аккумуляторы)
- Мелочёвку с аномально низкой ценой относительно рынка
"""

import logging
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
        Проверить, является ли товар аксессуаром/мелочёвкой.

        Применяет последовательно три фильтра:
        1. Минимальная цена
        2. Чёрный список слов в названии
        3. Соотношение цены к медиане (если медиана предоставлена)

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

        # Проверка 3: соотношение к медиане (только если медиана предоставлена)
        if median_price is not None:
            result = self._check_price_ratio(ad, median_price)
            if result.is_filtered:
                logger.debug(
                    "ad_filtered_price_ratio",
                    extra={"ad_id": ad.ad_id, "title": ad.title, "reason": result.reason}
                )
                return result

        return FilterResult(is_filtered=False)
