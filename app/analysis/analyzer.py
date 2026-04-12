"""Ценовой анализатор для определения недооценённых объявлений."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import structlog

from app.config import get_settings
from app.storage.models import Ad


@dataclass
class MarketStats:
    """Статистика рынка для одного поискового запроса.

    Attributes:
        search_url: URL поискового запроса.
        count: Количество объявлений в выборке.
        median_price: Медианная цена.
        mean_price: Средняя цена.
        q1_price: Первый квартиль (25-й перцентиль).
        min_price: Минимальная цена.
        max_price: Максимальная цена.
    """

    search_url: str
    count: int
    median_price: float | None
    mean_price: float | None
    q1_price: float | None
    min_price: float | None
    max_price: float | None


@dataclass
class UndervaluedAd:
    """Объявление, определённое как недооценённое.

    Attributes:
        ad: Объект объявления.
        market_stats: Статистика рынка для данного поиска.
        deviation_percent: Отклонение от медианы в % (отрицательное = ниже).
        threshold_used: Применённый порог недооценённости.
    """

    ad: Ad
    market_stats: MarketStats
    deviation_percent: float
    threshold_used: float


class PriceAnalyzer:
    """Анализатор цен для определения недооценённых объявлений.

    Сравнивает цены объявлений с медианной ценой по рынку (поисковому
    запросу) и выявляет объявления, цена которых ниже медианы на заданный
    порог ``undervalue_threshold``.

    Args:
        undervalue_threshold: Порог недооценённости (0.0–1.0).
            Объявление считается undervalued, если
            ``price < median * threshold``.
            Если ``None`` — берётся из конфига.
    """

    def __init__(self, undervalue_threshold: float | None = None) -> None:
        self.logger = structlog.get_logger()
        if undervalue_threshold is None:
            settings = get_settings()
            undervalue_threshold = settings.UNDERVALUE_THRESHOLD
        self.undervalue_threshold = undervalue_threshold

    def calculate_market_stats(self, ads: Sequence[Ad]) -> MarketStats:
        """Рассчитать статистику рынка по списку объявлений.

        Фильтрует объявления с ``price is not None`` и ``price > 0``.
        Если после фильтрации 0 объявлений — все числовые поля ``None``,
        ``count=0``.
        Если 1 объявление — ``median = mean = q1 = min = max`` этому
        значению.

        Использует numpy для расчётов: ``np.median``, ``np.mean``,
        ``np.percentile(q=25)``, ``np.min``, ``np.max``.

        Args:
            ads: Список объявлений (модели :class:`Ad` из storage).

        Returns:
            :class:`MarketStats` с рассчитанной статистикой.
        """
        search_url = ads[0].search_url if ads else ""

        # Фильтруем объявления с валидной ценой
        valid_prices = [
            ad.price for ad in ads
            if ad.price is not None and ad.price > 0
        ]

        if not valid_prices:
            self.logger.warning(
                "no_valid_prices",
                search_url=search_url,
                total_ads=len(ads),
            )
            return MarketStats(
                search_url=search_url or "",
                count=0,
                median_price=None,
                mean_price=None,
                q1_price=None,
                min_price=None,
                max_price=None,
            )

        prices = np.array(valid_prices, dtype=np.float64)

        stats = MarketStats(
            search_url=search_url or "",
            count=len(valid_prices),
            median_price=float(np.median(prices)),
            mean_price=float(np.mean(prices)),
            q1_price=float(np.percentile(prices, 25)),
            min_price=float(np.min(prices)),
            max_price=float(np.max(prices)),
        )

        self.logger.info(
            "market_stats_calculated",
            search_url=stats.search_url,
            count=stats.count,
            median=stats.median_price,
            mean=stats.mean_price,
        )
        return stats

    def detect_undervalued(
        self,
        ads: Sequence[Ad],
        market_stats: MarketStats,
    ) -> list[UndervaluedAd]:
        """Определить недооценённые объявления.

        Критерий: ``ad.price < market_stats.median_price *
        self.undervalue_threshold``.

        Также рассчитывает:

        * ``deviation_percent = ((ad.price - median) / median) * 100``
        * ``threshold_used = self.undervalue_threshold``

        Если ``market_stats.median_price is None`` — вернуть пустой
        список. Если ``ad.price is None`` — пропустить.

        Args:
            ads: Список объявлений для проверки.
            market_stats: Статистика рынка.

        Returns:
            Список :class:`UndervaluedAd`.
        """
        if market_stats.median_price is None:
            self.logger.warning(
                "cannot_detect_undervalued_no_median",
                search_url=market_stats.search_url,
            )
            return []

        median = market_stats.median_price
        threshold_price = median * self.undervalue_threshold
        undervalued: list[UndervaluedAd] = []

        for ad in ads:
            if ad.price is None:
                continue

            if ad.price < threshold_price:
                deviation_percent = ((ad.price - median) / median) * 100
                undervalued.append(
                    UndervaluedAd(
                        ad=ad,
                        market_stats=market_stats,
                        deviation_percent=deviation_percent,
                        threshold_used=self.undervalue_threshold,
                    )
                )

        self.logger.info(
            "undervalued_detected",
            search_url=market_stats.search_url,
            total_ads=len(ads),
            undervalued_count=len(undervalued),
            median=median,
            threshold_price=threshold_price,
        )
        return undervalued

    def analyze_and_mark(
        self,
        ads: Sequence[Ad],
        repository: object,
    ) -> list[UndervaluedAd]:
        """Полный цикл: рассчитать статистику, найти undervalued, обновить БД.

        1. ``calculate_market_stats(ads)``
        2. ``detect_undervalued(ads, stats)``
        3. Для каждого undervalued:
           ``repository.update_ad(ad_id, is_undervalued=True,
           undervalue_score=deviation_percent/100)``
        4. Вернуть список :class:`UndervaluedAd`.

        Args:
            ads: Список объявлений.
            repository: Экземпляр :class:`Repository` для обновления БД.

        Returns:
            Список :class:`UndervaluedAd`.
        """
        stats = self.calculate_market_stats(ads)
        undervalued = self.detect_undervalued(ads, stats)

        for item in undervalued:
            repository.update_ad(
                item.ad.ad_id,
                is_undervalued=True,
                undervalue_score=item.deviation_percent / 100,
            )
            self.logger.info(
                "ad_marked_undervalued",
                ad_id=item.ad.ad_id,
                price=item.ad.price,
                deviation_percent=item.deviation_percent,
                undervalue_score=item.deviation_percent / 100,
            )

        self.logger.info(
            "analyze_and_mark_completed",
            search_url=stats.search_url,
            total_ads=len(ads),
            undervalued_count=len(undervalued),
        )
        return undervalued
