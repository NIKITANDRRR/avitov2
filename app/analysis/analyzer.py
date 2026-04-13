"""Ценовой анализатор для определения недооценённых объявлений."""

from __future__ import annotations

import datetime
import logging
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np
import structlog

from app.config import get_settings
from app.storage.models import Ad

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Вспомогательная функция для перцентилей (fallback без numpy)
# ---------------------------------------------------------------------------

def _percentile(sorted_values: list[float], pct: float) -> float:
    """Вычислить перцентиль методом линейной интерполяции.

    Args:
        sorted_values: Отсортированный список значений.
        pct: Процентиль (0–100).

    Returns:
        Значение перцентиля.
    """
    n = len(sorted_values)
    if n == 0:
        return 0.0
    if n == 1:
        return sorted_values[0]

    rank = (pct / 100.0) * (n - 1)
    lower_idx = int(math.floor(rank))
    upper_idx = int(math.ceil(rank))

    if lower_idx == upper_idx:
        return sorted_values[lower_idx]

    fraction = rank - lower_idx
    return sorted_values[lower_idx] + fraction * (sorted_values[upper_idx] - sorted_values[lower_idx])


@dataclass
class MarketStats:
    """Статистика рынка для одного поискового запроса / сегмента.

    Attributes:
        search_url: URL поискового запроса.
        count: Количество объявлений в выборке.
        median_price: Медианная цена.
        mean_price: Средняя цена.
        q1_price: Первый квартиль (25-й перцентиль).
        q3: Третий квартиль (75-й перцентиль).
        iqr: Межквартильный размах (Q3 - Q1).
        std_dev: Стандартное отклонение цен.
        lower_fence: Нижняя граница IQR (Q1 - 1.5 * IQR).
        upper_fence: Верхняя граница IQR (Q3 + 1.5 * IQR).
        trimmed_mean: Усечённое среднее (без выбросов).
        segment_key: Ключ сегмента вида «{condition}_{location}_{seller_type}».
        original_count: Исходное количество объявлений до фильтрации.
        filtered_count: Количество объявлений после фильтрации выбросов.
        min_price: Минимальная цена.
        max_price: Максимальная цена.
    """

    search_url: str
    count: int
    median_price: float | None
    mean_price: float | None
    q1_price: float | None
    q3: float | None = None
    iqr: float | None = None
    std_dev: float | None = None
    lower_fence: float | None = None
    upper_fence: float | None = None
    trimmed_mean: float | None = None
    segment_key: str | None = None
    original_count: int = 0
    filtered_count: int = 0
    min_price: float | None = None
    max_price: float | None = None


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


@dataclass
class UndervaluedResult:
    """Результат анализа объявления на недооценённость (v2).

    Attributes:
        is_undervalued: Является ли объявление недооценённым.
        score: Итоговый составной score (0.0–1.0).
        score_iqr: IQR-компонент (0.0 или 1.0).
        score_z: Z-score компонент (0.0–1.0).
        score_pct: Процентный компонент (0.0–1.0).
        z_score: Z-score цены относительно сегмента.
    """

    is_undervalued: bool
    score: float
    score_iqr: float
    score_z: float
    score_pct: float
    z_score: float


@dataclass
class AdAnalysisResult:
    """Полный результат анализа одного объявления.

    Attributes:
        ad: Анализируемое объявление.
        segment_key: Ключ сегмента.
        market_stats: Статистика рынка для сегмента.
        undervalued_result: Результат проверки на недооценённость.
    """

    ad: Ad
    segment_key: str
    market_stats: MarketStats
    undervalued_result: UndervaluedResult


class PriceAnalyzer:
    """Анализатор цен для определения недооценённых объявлений.

    Поддерживает два режима анализа:

    * **v1 (legacy)**: простой порог ``price < median * threshold`` через
      :meth:`detect_undervalued`.
    * **v2 (продвинутый)**: составной критерий с IQR, z-score и процентом
      от медианы через :meth:`detect_undervalued_v2` и :meth:`analyze_ad`.

    Args:
        undervalue_threshold: Порог недооценённости (0.0–1.0).
            Объявление считается undervalued (v1), если
            ``price < median * threshold``.
            Если ``None`` — берётся из конфига.
    """

    def __init__(self, undervalue_threshold: float | None = None) -> None:
        self._log = structlog.get_logger()
        if undervalue_threshold is None:
            settings = get_settings()
            undervalue_threshold = settings.UNDERVALUE_THRESHOLD
        self.undervalue_threshold = undervalue_threshold

    # ------------------------------------------------------------------
    # Временной фильтр
    # ------------------------------------------------------------------

    def filter_temporal(
        self,
        ads: Sequence[Ad],
        days: int = 14,
    ) -> list[Ad]:
        """Отфильтровать объявления по возрасту.

        Оставляет только объявления, у которых ``publication_date`` или
        ``first_seen_at`` попадают в последние ``days`` дней от текущего
        момента.

        Args:
            ads: Список объявлений.
            days: Максимальный возраст объявления в днях.

        Returns:
            Список объявлений за последние ``days`` дней.
        """
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days)
        filtered: list[Ad] = []

        for ad in ads:
            # Проверяем publication_date и first_seen_at
            pub = ad.publication_date
            first_seen = ad.first_seen_at

            # Приводим naive datetime к UTC-сравнению
            is_recent = False
            if pub is not None:
                # Если pub содержит tzinfo — приводим к UTC
                if pub.tzinfo is not None:
                    pub = pub.replace(tzinfo=None) - pub.utcoffset()  # type: ignore[operator]

                if pub >= cutoff:
                    is_recent = True

            if not is_recent and first_seen is not None:
                if first_seen.tzinfo is not None:
                    first_seen = first_seen.replace(tzinfo=None) - first_seen.utcoffset()  # type: ignore[operator]

                if first_seen >= cutoff:
                    is_recent = True

            if is_recent:
                filtered.append(ad)

        self._log.debug(
            "filter_temporal",
            total=len(ads),
            filtered=len(filtered),
            days=days,
        )
        return filtered

    # ------------------------------------------------------------------
    # Сегментация
    # ------------------------------------------------------------------

    @staticmethod
    def build_segment_key(ad: Ad) -> str:
        """Сформировать ключ сегмента из атрибутов объявления.

        Ключ формата: ``{condition}_{location}_{seller_type}``.

        Нормализация: lowercase, strip, ``None`` → ``"unknown"``.
        Из ``location`` берётся только первый компонент до запятой.

        Args:
            ad: Объявление.

        Returns:
            Строка-ключ сегмента, например ``"новый_москва_частный"``.
        """
        # condition
        condition = ad.condition
        if condition is not None:
            condition = str(condition).strip().lower()
        if not condition:
            condition = "unknown"

        # location — берём город (первый компонент до запятой)
        location = ad.location
        if location is not None:
            location = str(location).split(",")[0].strip().lower()
        if not location:
            location = "unknown"

        # seller_type
        seller_type = ad.seller_type
        if seller_type is not None:
            seller_type = str(seller_type).strip().lower()
        if not seller_type:
            seller_type = "unknown"

        return f"{condition}_{location}_{seller_type}"

    def segment_ads(self, ads: Sequence[Ad]) -> dict[str, list[Ad]]:
        """Разбить объявления на сегменты по ключу.

        Группирует по ``{condition}_{location}_{seller_type}``.
        Если сегмент содержит < 3 объявлений — объединяет с родительским
        сегментом (сначала убирая ``seller_type``, затем ``location``).

        Args:
            ads: Список объявлений.

        Returns:
            Словарь ``{segment_key: [Ad, ...]}``.
        """
        # Группировка по полному ключу
        segments: dict[str, list[Ad]] = defaultdict(list)
        for ad in ads:
            key = self.build_segment_key(ad)
            segments[key].append(ad)

        # Объединение мелких сегментов (< 3 объявлений)
        merged: dict[str, list[Ad]] = defaultdict(list)
        small_keys: list[str] = []

        for key, group in segments.items():
            if len(group) < 3:
                small_keys.append(key)
            else:
                merged[key] = group

        # Пытаемся объединить мелкие по родительскому ключу
        # Уровень 1: убираем seller_type (последний компонент)
        parent_pending: dict[str, list[Ad]] = defaultdict(list)
        for key in small_keys:
            parts = key.rsplit("_", 1)  # condition_location → без seller_type
            parent_key = parts[0] if len(parts) > 1 else key
            parent_pending[parent_key].extend(segments[key])

        for parent_key, group in parent_pending.items():
            if len(group) >= 3:
                merged[parent_key] = group
            else:
                # Уровень 2: убираем location — только condition
                condition_key = parent_key.split("_")[0]
                merged[f"{condition_key}_all_all"] = merged.get(
                    f"{condition_key}_all_all", []
                ) + group

        # Если после объединения остались сегменты < 3 — объединяем в "all_all_all"
        final: dict[str, list[Ad]] = {}
        overflow: list[Ad] = []
        for key, group in merged.items():
            if len(group) >= 3:
                final[key] = group
            else:
                overflow.extend(group)

        if overflow:
            if "all_all_all" in final:
                final["all_all_all"].extend(overflow)
            elif len(overflow) >= 3:
                final["all_all_all"] = overflow
            else:
                # Слишком мало данных — всё равно сохраняем
                final["all_all_all"] = overflow

        self._log.debug(
            "segment_ads",
            total_ads=len(ads),
            segment_count=len(final),
            segment_sizes={k: len(v) for k, v in final.items()},
        )
        return dict(final)

    # ------------------------------------------------------------------
    # Фильтрация выбросов
    # ------------------------------------------------------------------

    @staticmethod
    def filter_trim_percent(
        prices: list[float],
        trim_pct: float = 0.05,
    ) -> list[float]:
        """Отбросить trim_pct самых дешёвых и самых дорогих.

        Сортирует цены и удаляет ``trim_pct`` долю с каждого края.

        Args:
            prices: Список цен.
            trim_pct: Доля отбрасываемых цен с каждого края (0.0–0.5).

        Returns:
            Отфильтрованный список цен. Если после обрезки осталось < 3
            цен — возвращается исходный список.
        """
        if len(prices) < 3:
            return list(prices)

        sorted_prices = sorted(prices)
        n = len(sorted_prices)
        trim_count = int(n * trim_pct)

        # Если обрезка удалит всё — вернуть исходный список
        if n - 2 * trim_count < 3:
            return list(prices)

        return sorted_prices[trim_count : n - trim_count]

    @staticmethod
    def filter_iqr(
        prices: list[float],
    ) -> tuple[list[float], float, float]:
        """Фильтрация выбросов методом межквартильного размаха (IQR).

        Считает Q1 (25-й перцентиль), Q3 (75-й перцентиль),
        IQR = Q3 − Q1. Нижняя граница: Q1 − 1.5 × IQR.
        Верхняя граница: Q3 + 1.5 × IQR.

        Args:
            prices: Список цен.

        Returns:
            Кортеж ``(filtered_prices, lower_fence, upper_fence)``.
            Если список пуст — ``([], 0.0, 0.0)``.
        """
        if not prices:
            return [], 0.0, 0.0

        sorted_prices = sorted(prices)
        q1 = _percentile(sorted_prices, 25)
        q3 = _percentile(sorted_prices, 75)
        iqr = q3 - q1

        lower_fence = q1 - 1.5 * iqr
        upper_fence = q3 + 1.5 * iqr

        filtered = [p for p in prices if lower_fence <= p <= upper_fence]

        return filtered, lower_fence, upper_fence

    # ------------------------------------------------------------------
    # Z-score
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_zscore(
        price: float,
        mean: float,
        std_dev: float,
    ) -> float:
        """Рассчитать z-score для цены.

        ``z = (price − mean) / std_dev``.

        При ``std_dev == 0`` возвращает ``0.0`` (нет вариации в данных).

        Args:
            price: Цена объявления.
            mean: Среднее значение цен в сегменте.
            std_dev: Стандартное отклонение цен в сегменте.

        Returns:
            Z-score (float).
        """
        if std_dev == 0:
            return 0.0
        return (price - mean) / std_dev

    # ------------------------------------------------------------------
    # Усечённое среднее
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_trimmed_mean(
        prices: list[float],
        trim_pct: float = 0.1,
    ) -> float:
        """Рассчитать усечённое среднее (trimmed mean).

        Сортирует цены, отбрасывает ``trim_pct / 2`` с начала и с конца,
        возвращает среднее оставшихся.

        Args:
            prices: Список цен.
            trim_pct: Доля отбрасываемых цен (половина с начала,
                половина с конца). Например, ``0.1`` → по 5% с каждого края.

        Returns:
            Усечённое среднее. Если список пуст — ``0.0``.
        """
        if not prices:
            return 0.0

        sorted_prices = sorted(prices)
        n = len(sorted_prices)
        trim_count = max(1, int(n * trim_pct / 2))

        if n - 2 * trim_count < 1:
            # Не хватает данных для обрезки — вернуть обычное среднее
            return statistics.mean(sorted_prices)

        trimmed = sorted_prices[trim_count : n - trim_count]
        return statistics.mean(trimmed)

    # ------------------------------------------------------------------
    # Расчёт статистики рынка (обновлённый)
    # ------------------------------------------------------------------

    def calculate_market_stats(
        self,
        ads: Sequence[Ad],
        segment_key: str | None = None,
    ) -> MarketStats:
        """Рассчитать расширенную статистику рынка по списку объявлений.

        Фильтрует объявления с ``price is not None`` и ``price > 0``.
        Применяет :meth:`filter_trim_percent` для удаления выбросов,
        затем рассчитывает Q1, Q3, IQR, std_dev, lower_fence,
        upper_fence, trimmed_mean.

        Args:
            ads: Список объявлений (модели :class:`Ad`).
            segment_key: Ключ сегмента (если ``None`` — не заполняется).

        Returns:
            :class:`MarketStats` с рассчитанной статистикой.
        """
        search_url = ads[0].search_url if ads else ""

        # Фильтруем объявления с валидной ценой
        valid_prices = [
            ad.price for ad in ads
            if ad.price is not None and ad.price > 0
        ]
        original_count = len(valid_prices)

        if not valid_prices:
            self._log.warning(
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
                segment_key=segment_key,
                original_count=len(ads),
                filtered_count=0,
            )

        # Шаг 1: Trim-percent фильтрация
        trimmed_prices = self.filter_trim_percent(valid_prices, trim_pct=0.05)

        # Шаг 2: IQR фильтрация
        filtered_prices, lower_fence, upper_fence = self.filter_iqr(trimmed_prices)

        # Если после фильтрации ничего не осталось — используем trimmed
        if not filtered_prices:
            filtered_prices = trimmed_prices
        if not filtered_prices:
            filtered_prices = valid_prices

        prices_arr = np.array(filtered_prices, dtype=np.float64)

        # Основные статистики
        median_price = float(np.median(prices_arr))
        mean_price = float(np.mean(prices_arr))
        q1 = float(np.percentile(prices_arr, 25))
        q3 = float(np.percentile(prices_arr, 75))
        iqr = q3 - q1

        # Стандартное отклонение
        if len(filtered_prices) >= 2:
            std_dev = float(np.std(prices_arr, ddof=1))
        else:
            std_dev = 0.0

        # Усечённое среднее
        trimmed_mean = self.calculate_trimmed_mean(filtered_prices, trim_pct=0.1)

        stats = MarketStats(
            search_url=search_url or "",
            count=len(filtered_prices),
            median_price=median_price,
            mean_price=mean_price,
            q1_price=q1,
            q3=q3,
            iqr=iqr,
            std_dev=std_dev,
            lower_fence=lower_fence,
            upper_fence=upper_fence,
            trimmed_mean=trimmed_mean,
            segment_key=segment_key,
            original_count=original_count,
            filtered_count=len(filtered_prices),
            min_price=float(np.min(prices_arr)),
            max_price=float(np.max(prices_arr)),
        )

        self._log.info(
            "market_stats_calculated",
            search_url=stats.search_url,
            segment=segment_key,
            count=stats.count,
            original_count=original_count,
            median=stats.median_price,
            mean=stats.mean_price,
            q1=stats.q1_price,
            q3=stats.q3,
            iqr=stats.iqr,
            std_dev=stats.std_dev,
            lower_fence=stats.lower_fence,
            upper_fence=stats.upper_fence,
        )
        return stats

    # ------------------------------------------------------------------
    # Продвинутый критерий undervalued (v2)
    # ------------------------------------------------------------------

    def detect_undervalued_v2(
        self,
        ad: Ad,
        market_stats: MarketStats,
    ) -> UndervaluedResult:
        """Продвинутый составной критерий недооценённости.

        Компоненты и веса:

        * **IQR** (вес 0.4): цена ниже ``lower_fence`` → ``score_iqr = 1.0``
        * **Z-score** (вес 0.3): ``z_score < −1.5`` →
          ``score_z = min(1.0, abs(z_score) / 3.0)``
        * **Процент от медианы** (вес 0.3): ``price < median × 0.85`` →
          ``score_pct = (median − price) / median``

        Итоговый ``score = 0.4 × score_iqr + 0.3 × score_z + 0.3 × score_pct``.
        Объявление считается недооценённым при ``score >= 0.3``.

        Args:
            ad: Объявление для проверки.
            market_stats: Статистика рынка (сегмента).

        Returns:
            :class:`UndervaluedResult` с деталями анализа.
        """
        # Инициализация результата
        result = UndervaluedResult(
            is_undervalued=False,
            score=0.0,
            score_iqr=0.0,
            score_z=0.0,
            score_pct=0.0,
            z_score=0.0,
        )

        # Проверка базовых условий
        if ad.price is None or ad.price <= 0:
            return result

        if market_stats.median_price is None or market_stats.mean_price is None:
            return result

        price = ad.price
        median = market_stats.median_price
        mean = market_stats.mean_price
        std_dev = market_stats.std_dev or 0.0
        lower_fence = market_stats.lower_fence

        # Z-score
        z_score = self.calculate_zscore(price, mean, std_dev)
        result.z_score = z_score

        # IQR-компонент (вес 0.4)
        if lower_fence is not None and price < lower_fence:
            result.score_iqr = 1.0

        # Z-score компонент (вес 0.3)
        if z_score < -1.5:
            result.score_z = min(1.0, abs(z_score) / 3.0)

        # Процент от медианы (вес 0.3)
        if median > 0 and price < median * 0.85:
            result.score_pct = (median - price) / median

        # Итоговый score
        result.score = (
            0.4 * result.score_iqr
            + 0.3 * result.score_z
            + 0.3 * result.score_pct
        )

        # Порог
        result.is_undervalued = result.score >= 0.3

        self._log.debug(
            "detect_undervalued_v2",
            ad_id=ad.ad_id,
            price=price,
            median=median,
            z_score=z_score,
            score=result.score,
            score_iqr=result.score_iqr,
            score_z=result.score_z,
            score_pct=result.score_pct,
            is_undervalued=result.is_undervalued,
        )
        return result

    # ------------------------------------------------------------------
    # Главный метод анализа объявления
    # ------------------------------------------------------------------

    def analyze_ad(
        self,
        ad: Ad,
        all_ads: Sequence[Ad],
    ) -> AdAnalysisResult | None:
        """Полный анализ одного объявления против рынка.

        Алгоритм:

        1. Фильтрует ``all_ads`` по времени (14 дней).
        2. Группирует по сегментам.
        3. Находит сегмент текущего ``ad``.
        4. Считает :class:`MarketStats` для сегмента с фильтрацией.
        5. Вызывает :meth:`detect_undervalued_v2`.

        Args:
            ad: Анализируемое объявление.
            all_ads: Все объявления для сравнения.

        Returns:
            :class:`AdAnalysisResult` с полной информацией или ``None``,
            если не удалось определить сегмент или недостаточно данных.
        """
        # Шаг 1: временной фильтр
        recent_ads = self.filter_temporal(all_ads, days=14)

        if not recent_ads:
            self._log.warning(
                "analyze_ad_no_recent_ads",
                ad_id=ad.ad_id,
                total_ads=len(all_ads),
            )
            return None

        # Шаг 2: сегментация
        segments = self.segment_ads(recent_ads)

        # Шаг 3: находим сегмент текущего объявления
        ad_segment_key = self.build_segment_key(ad)

        # Ищем точное совпадение сегмента
        segment_ads_list: list[Ad] | None = segments.get(ad_segment_key)

        # Если точного совпадения нет — ищем по родительским ключам
        if segment_ads_list is None:
            # Пробуем без seller_type
            parts = ad_segment_key.rsplit("_", 1)
            parent_key = parts[0] if len(parts) > 1 else ad_segment_key
            segment_ads_list = segments.get(parent_key)

        if segment_ads_list is None:
            # Пробуем только condition
            condition = ad_segment_key.split("_")[0]
            fallback_key = f"{condition}_all_all"
            segment_ads_list = segments.get(fallback_key)

        if segment_ads_list is None:
            # Берём самый большой сегмент
            if segments:
                best_key = max(segments, key=lambda k: len(segments[k]))
                segment_ads_list = segments[best_key]
                ad_segment_key = best_key
            else:
                self._log.warning(
                    "analyze_ad_no_segment",
                    ad_id=ad.ad_id,
                )
                return None

        # Шаг 4: статистика сегмента
        market_stats = self.calculate_market_stats(
            segment_ads_list,
            segment_key=ad_segment_key,
        )

        # Шаг 5: detect_undervalued_v2
        undervalued_result = self.detect_undervalued_v2(ad, market_stats)

        result = AdAnalysisResult(
            ad=ad,
            segment_key=ad_segment_key,
            market_stats=market_stats,
            undervalued_result=undervalued_result,
        )

        self._log.info(
            "analyze_ad_completed",
            ad_id=ad.ad_id,
            price=ad.price,
            segment=ad_segment_key,
            segment_size=len(segment_ads_list),
            score=undervalued_result.score,
            is_undervalued=undervalued_result.is_undervalued,
        )
        return result

    # ------------------------------------------------------------------
    # Legacy-методы (обратная совместимость)
    # ------------------------------------------------------------------

    def detect_undervalued(
        self,
        ads: Sequence[Ad],
        market_stats: MarketStats,
    ) -> list[UndervaluedAd]:
        """Определить недооценённые объявления (v1, legacy).

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
            self._log.warning(
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

        self._log.info(
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
            self._log.info(
                "ad_marked_undervalued",
                ad_id=item.ad.ad_id,
                price=item.ad.price,
                deviation_percent=item.deviation_percent,
                undervalue_score=item.deviation_percent / 100,
            )

        self._log.info(
            "analyze_and_mark_completed",
            search_url=stats.search_url,
            total_ads=len(ads),
            undervalued_count=len(undervalued),
        )
        return undervalued
