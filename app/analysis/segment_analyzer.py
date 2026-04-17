"""Анализатор сегментов для категорийного мониторинга.

Сегментация по category|brand|model|condition|location,
расчёт статистики с временными окнами, метрики ликвидности,
детекция «бриллиантов».
"""

from __future__ import annotations

import datetime
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np
import structlog

from app.config import get_settings
from app.config.settings import Settings
from app.storage.models import Ad, SegmentStats, SegmentPriceHistory


logger = structlog.get_logger("segment_analyzer")


# ---------------------------------------------------------------------------
# Ключ сегмента
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CategorySegmentKey:
    """Ключ сегмента для категорийного мониторинга."""

    category: str = "unknown"
    brand: str = "unknown"
    model: str = "unknown"
    condition: str = "unknown"
    location: str = "unknown"

    def to_string(self) -> str:
        """Строковое представление: ``'телефоны|apple|iphone 15|used|moscow'``."""
        return "|".join([
            self.category,
            self.brand,
            self.model,
            self.condition,
            self.location,
        ])

    @classmethod
    def from_string(cls, key_str: str) -> CategorySegmentKey:
        """Создать ключ из строки."""
        parts = key_str.split("|")
        return cls(
            category=parts[0] if len(parts) > 0 else "unknown",
            brand=parts[1] if len(parts) > 1 else "unknown",
            model=parts[2] if len(parts) > 2 else "unknown",
            condition=parts[3] if len(parts) > 3 else "unknown",
            location=parts[4] if len(parts) > 4 else "unknown",
        )

    def parent(self, level: int = 1) -> CategorySegmentKey:
        """Получить родительский ключ (убрать level последних компонентов)."""
        parts = [self.category, self.brand, self.model, self.condition, self.location]
        for i in range(level):
            idx = len(parts) - 1 - i
            if idx >= 0:
                parts[idx] = "unknown"
        return CategorySegmentKey(*parts)


# ---------------------------------------------------------------------------
# Алерт о бриллианте
# ---------------------------------------------------------------------------

@dataclass
class DiamondAlert:
    """Алерт о «бриллианте» — товаре значительно ниже рынка."""

    ad: Ad
    segment_key: CategorySegmentKey
    segment_stats: SegmentStats | None
    price: float
    median_price: float
    discount_percent: float
    sample_size: int
    reason: str
    is_rare_segment: bool = False
    composite_score: float = 0.0
    rarity_score: float = 0.0
    discount_score: float = 0.0
    liquidity_score: float = 0.0
    supply_score: float = 0.0
    appearance_count_90d: int = 0
    supply_drop_percent: float | None = None


# ---------------------------------------------------------------------------
# SegmentAnalyzer
# ---------------------------------------------------------------------------

class SegmentAnalyzer:
    """Анализатор сегментов для категорийного мониторинга."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._log = structlog.get_logger("segment_analyzer")

    # ------------------------------------------------------------------
    # Вспомогательный: безопасное получение настройки
    # ------------------------------------------------------------------

    def _get_setting(self, name: str, default: Any = None) -> Any:
        """Получить настройку по имени (поддерживает оба регистра)."""
        value = getattr(self.settings, name, None)
        if value is not None:
            return value
        # Fallback: пробуем альтернативное имя
        if name.isupper():
            lower_name = name.lower()
            return getattr(self.settings, lower_name, default)
        return default

    # ------------------------------------------------------------------
    # Построение ключа сегмента
    # ------------------------------------------------------------------

    @staticmethod
    def build_segment_key(ad: Ad) -> CategorySegmentKey:
        """Построить ключ сегмента из атрибутов объявления."""
        cat = ad.ad_category
        if cat:
            cat = str(cat).strip().lower()
        if not cat:
            cat = "unknown"

        brand = ad.brand
        if brand:
            brand = str(brand).strip().lower()
        if not brand:
            brand = "unknown"

        model = ad.extracted_model
        if model:
            model = str(model).strip().lower()
        if not model:
            model = "unknown"

        condition = ad.condition
        if condition:
            condition = str(condition).strip().lower()
        if not condition:
            condition = "unknown"

        location = ad.location
        if location:
            location = str(location).split(",")[0].strip().lower()
        if not location:
            location = "unknown"

        return CategorySegmentKey(
            category=cat,
            brand=brand,
            model=model,
            condition=condition,
            location=location,
        )

    # ------------------------------------------------------------------
    # Сегментация
    # ------------------------------------------------------------------

    def segment_ads(self, ads: Sequence[Ad]) -> dict[str, list[Ad]]:
        """Разбить объявления на сегменты."""
        segments: dict[str, list[Ad]] = defaultdict(list)
        for ad in ads:
            key = self.build_segment_key(ad)
            segments[key.to_string()].append(ad)

        self._log.debug(
            "segment_ads",
            total_ads=len(ads),
            segment_count=len(segments),
        )
        return dict(segments)

    def merge_small_segments(
        self,
        segments: dict[str, list[Ad]],
        min_size: int | None = None,
    ) -> dict[str, list[Ad]]:
        """Объединить мелкие сегменты по иерархии."""
        if min_size is None:
            min_size = self._get_setting(
                "CATEGORY_MIN_SEGMENT_SIZE",
                self._get_setting("segment_min_samples_for_stats", 3),
            )

        merged: dict[str, list[Ad]] = {}
        small: list[tuple[str, list[Ad]]] = []

        for key_str, group in segments.items():
            if len(group) >= min_size:
                merged[key_str] = group
            else:
                small.append((key_str, group))

        if not small:
            return merged

        for level in range(1, 5):
            still_small: list[tuple[str, list[Ad]]] = []
            buckets: dict[str, list[Ad]] = defaultdict(list)

            for key_str, group in small:
                key = CategorySegmentKey.from_string(key_str)
                parent = key.parent(level=level)
                parent_str = parent.to_string()
                buckets[parent_str].extend(group)

            for parent_str, group in buckets.items():
                if len(group) >= min_size:
                    merged[parent_str] = group
                else:
                    still_small.append((parent_str, group))

            small = still_small
            if not small:
                break

        for key_str, group in small:
            if key_str in merged:
                merged[key_str].extend(group)
            else:
                merged[key_str] = group

        self._log.debug(
            "merge_small_segments",
            min_size=min_size,
            result_count=len(merged),
        )
        return merged

    # ------------------------------------------------------------------
    # Расчёт статистики сегмента
    # ------------------------------------------------------------------

    def calculate_segment_stats(
        self,
        segment_ads: list[Ad],
        segment_key: CategorySegmentKey,
        repo: Any = None,
        search_id: int | None = None,
    ) -> SegmentStats:
        """Рассчитать статистику для одного сегмента.

        Args:
            segment_ads: Объявления в сегменте.
            segment_key: Ключ сегмента.
            repo: Экземпляр репозитория (для расчёта временных окон).
            search_id: ID поиска (для запроса истории из БД).

        Returns:
            Объект :class:`SegmentStats` (не привязанный к сессии).
        """
        prices = [
            ad.price for ad in segment_ads
            if ad.price is not None and ad.price > 0
        ]

        if not prices:
            return SegmentStats(
                segment_key=segment_key.to_string(),
                segment_name=segment_key.to_string(),
                sample_size=0,
                listing_count=0,
                calculated_at=datetime.datetime.now(datetime.timezone.utc),
            )

        # IQR фильтрация выбросов
        if len(prices) >= 4:
            prices_arr_iqr = np.array(prices, dtype=np.float64)
            q1 = float(np.percentile(prices_arr_iqr, 25))
            q3 = float(np.percentile(prices_arr_iqr, 75))
            iqr = q3 - q1
            lower_fence = q1 - 1.5 * iqr
            upper_fence = q3 + 1.5 * iqr
            filtered = prices_arr_iqr[
                (prices_arr_iqr >= lower_fence) & (prices_arr_iqr <= upper_fence)
            ]
            if len(filtered) >= 3:
                self._log.debug(
                    "iqr_filter_applied",
                    segment=segment_key.to_string(),
                    before=len(prices),
                    after=len(filtered),
                    lower_fence=lower_fence,
                    upper_fence=upper_fence,
                )
                prices = filtered.tolist()
            else:
                self._log.debug(
                    "iqr_filter_skipped_too_few_remaining",
                    segment=segment_key.to_string(),
                    filtered_count=len(filtered),
                )

        prices_arr = np.array(prices, dtype=np.float64)

        median_price = float(np.median(prices_arr))
        mean_price = float(np.mean(prices_arr))

        stats = SegmentStats(
            segment_key=segment_key.to_string(),
            segment_name=segment_key.to_string(),
            sample_size=len(prices),
            listing_count=len(prices),
            mean_price=mean_price,
            min_price=float(np.min(prices_arr)),
            max_price=float(np.max(prices_arr)),
            listing_price_median=median_price,
            calculated_at=datetime.datetime.now(datetime.timezone.utc),
        )

        # Заполняем временные окна, если передан repo и search_id
        if repo is not None and search_id is not None:
            temporal = self._calculate_temporal_windows(
                segment_key.to_string(),
                repo,
                search_id,
                self._get_setting("CATEGORY_TEMPORAL_WINDOWS", [7, 30, 90]),
            )
            for attr, val in temporal.items():
                if hasattr(stats, attr):
                    setattr(stats, attr, val)

        self._log.debug(
            "segment_stats_calculated",
            segment=segment_key.to_string(),
            count=len(prices),
            median=median_price,
        )
        return stats

    # ------------------------------------------------------------------
    # Выбор лучшей медианы
    # ------------------------------------------------------------------

    def get_best_median(self, stats: dict) -> tuple[float, str]:
        """Возвращает (лучшую медиану, описание почему она выбрана).

        Логика:
        1. Если сегмент редкий (sample_size < segment_rare_threshold) и есть
           liquid_market_estimate — используем его
        2. Если median_7d > median_30d — рынок растёт, используем median_7d
           с весом segment_7d_weight
        3. Иначе — используем median_30d как основную метрику
        """
        sample_size = stats.get('sample_size', 0) or 0
        rare_threshold = self._get_setting(
            "CATEGORY_RARE_SEGMENT_THRESHOLD",
            self._get_setting("segment_rare_threshold", 5),
        )

        # 1. Редкий сегмент с ликвидной оценкой
        if sample_size < rare_threshold:
            liquid = stats.get('liquid_market_estimate')
            if liquid is not None and liquid > 0:
                return liquid, (
                    f"liquid_market_estimate ({liquid:,.0f}\u20bd) \u2014 "
                    f"\u0440\u0435\u0434\u043a\u0438\u0439 \u0441\u0435\u0433\u043c\u0435\u043d\u0442 "
                    f"(sample_size={sample_size})"
                )

        median_7d = stats.get('median_7d')
        median_30d = stats.get('median_30d')
        median_price = stats.get('median_price') or stats.get('listing_price_median', 0) or 0

        # 2. Рынок растёт
        weight = self._get_setting("segment_7d_weight", 1.5)
        if (median_7d is not None and median_7d > 0
                and median_30d is not None and median_30d > 0):
            if median_7d > median_30d:
                weighted = median_7d * weight
                return weighted, (
                    f"median_7d \u00d7 weight ({median_7d:,.0f}\u20bd \u00d7 "
                    f"{weight}) \u2014 \u0440\u044b\u043d\u043e\u043a \u0440\u0430\u0441\u0442\u0451\u0442"
                )

        # 3. Стандартная median_30d
        if median_30d is not None and median_30d > 0:
            return median_30d, f"median_30d ({median_30d:,.0f}\u20bd) \u2014 \u043e\u0441\u043d\u043e\u0432\u043d\u0430\u044f \u043c\u0435\u0442\u0440\u0438\u043a\u0430"

        # Fallback: median_7d
        if median_7d is not None and median_7d > 0:
            return median_7d, f"median_7d ({median_7d:,.0f}\u20bd) \u2014 fallback"

        return median_price, f"median_price ({median_price:,.0f}\u20bd) \u2014 fallback"

    # ------------------------------------------------------------------
    # Метрики ликвидности
    # ------------------------------------------------------------------

    def calculate_liquidity_metrics(
        self,
        ads: list[Ad],
        disappeared_ads: list[Ad],
    ) -> dict:
        """Рассчитывает метрики ликвидности для сегмента.

        Args:
            ads: Активные объявления сегмента.
            disappeared_ads: Исчезнувшие объявления сегмента.

        Returns:
            dict с ключами:
                - listing_price_median: медиана по активным объявлениям
                - fast_sale_price_median: медиана цен быстрых продаж
                - liquid_market_estimate: оценка ликвидной цены
                - median_days_on_market: медиана дней на рынке
                - appearance_count_90d: кол-во появлений за 90 дней
                - fast_sale_count: кол-во быстрых продаж
        """
        fast_sale_days = self._get_setting(
            "CATEGORY_FAST_SALE_DAYS",
            self._get_setting("segment_fast_sale_days", 3),
        )
        rare_threshold = self._get_setting(
            "CATEGORY_RARE_SEGMENT_THRESHOLD",
            self._get_setting("segment_rare_threshold", 5),
        )
        liquidity_premium = self._get_setting("segment_liquidity_premium", 1.2)

        # listing_price_median — медиана по активным объявлениям
        active_prices = [
            ad.price for ad in ads
            if ad.price is not None and ad.price > 0
        ]
        listing_price_median: float | None = None
        if active_prices:
            listing_price_median = float(statistics.median(active_prices))

        # Быстрые продажи — исчезнувшие с days_on_market <= порога
        fast_sale_prices: list[float] = []
        days_on_market_values: list[float] = []
        for ad in disappeared_ads:
            if ad.price is not None and ad.price > 0:
                if ad.days_on_market is not None:
                    days_on_market_values.append(float(ad.days_on_market))
                    if ad.days_on_market <= fast_sale_days:
                        fast_sale_prices.append(ad.price)

        fast_sale_price_median: float | None = None
        if fast_sale_prices:
            fast_sale_price_median = float(statistics.median(fast_sale_prices))

        median_days_on_market: float | None = None
        if days_on_market_values:
            median_days_on_market = float(statistics.median(days_on_market_values))

        # liquid_market_estimate
        liquid_market_estimate: float | None = None
        if fast_sale_price_median is not None and listing_price_median is not None:
            liquid_market_estimate = (fast_sale_price_median + listing_price_median) / 2
            # Премия для редких сегментов
            if len(ads) < rare_threshold:
                liquid_market_estimate *= liquidity_premium
        elif listing_price_median is not None:
            liquid_market_estimate = listing_price_median
        elif fast_sale_price_median is not None:
            liquid_market_estimate = fast_sale_price_median

        return {
            'listing_price_median': listing_price_median,
            'fast_sale_price_median': fast_sale_price_median,
            'liquid_market_estimate': liquid_market_estimate,
            'median_days_on_market': median_days_on_market,
            'appearance_count_90d': len(disappeared_ads),
            'fast_sale_count': len(fast_sale_prices),
        }

    # ------------------------------------------------------------------
    # Сохранение снапшота истории
    # ------------------------------------------------------------------

    def save_segment_snapshot(
        self,
        repo: Any,
        segment_stats_id: int,
        stats: dict,
    ) -> None:
        """Сохраняет ежедневный/еженедельный снапшот истории цен сегмента.

        Проверяет, есть ли уже снапшот на сегодня.
        Если нет — создаёт через repo.save_price_history_snapshot().
        Периодичность определяется настройкой segment_history_snapshot_days.

        Args:
            repo: Экземпляр репозитория.
            segment_stats_id: ID записи SegmentStats.
            stats: Словарь со статистикой сегмента.
        """
        snapshot_days = self._get_setting("segment_history_snapshot_days", 7)
        today = datetime.date.today()

        try:
            # Проверяем, нужно ли делать снапшот сегодня
            # (периодичность segment_history_snapshot_days)
            history = repo.get_price_history(segment_stats_id, days=snapshot_days)
            if history:
                last_snapshot_date = history[-1].snapshot_date
                days_since_last = (today - last_snapshot_date).days
                if days_since_last < snapshot_days:
                    self._log.debug(
                        "snapshot_skipped_recent",
                        segment_stats_id=segment_stats_id,
                        days_since_last=days_since_last,
                    )
                    return

            snapshot_data = {
                'median_price': stats.get('median_price') or stats.get('listing_price_median'),
                'mean_price': stats.get('mean_price'),
                'min_price': stats.get('min_price'),
                'max_price': stats.get('max_price'),
                'sample_size': stats.get('sample_size', 0) or 0,
                'listing_count': stats.get('listing_count', 0) or 0,
                'fast_sale_count': stats.get('fast_sale_count', 0) or 0,
                'median_days_on_market': stats.get('median_days_on_market'),
            }

            repo.save_price_history_snapshot(
                segment_stats_id=segment_stats_id,
                snapshot_date=today,
                data=snapshot_data,
            )
            self._log.info(
                "segment_snapshot_saved",
                segment_stats_id=segment_stats_id,
                date=str(today),
            )
        except Exception as exc:
            self._log.warning(
                "segment_snapshot_failed",
                segment_stats_id=segment_stats_id,
                error=str(exc),
            )

    # ------------------------------------------------------------------
    # Редкие сегменты
    # ------------------------------------------------------------------

    def calculate_rare_segment_stats(
        self,
        repo: Any,
        search_id: int,
        segment_key: str,
        current_ads: list[Ad],
    ) -> dict:
        """Специальная обработка редких сегментов (sample_size < threshold).

        Для редких товаров НЕ делаем жёсткий вывод по медиане текущей выдачи.
        Вместо этого:
        1. Получаем историю цен сегмента (get_price_history за 90 дней)
        2. Смотрим частоту появления (appearance_count_90d)
        3. Используем иерархию сегментов (если brand:model редкий, смотрим brand)
        4. Учитываем оборачиваемость

        Args:
            repo: Экземпляр репозитория.
            search_id: ID отслеживаемого поиска.
            segment_key: Ключ сегмента.
            current_ads: Текущие активные объявления.

        Returns:
            dict с полями:
                - is_rare_segment: True
                - confidence: 'low' | 'medium' | 'high'
                - recommended_price: float
                - price_source: str
        """
        rare_threshold = self._get_setting(
            "CATEGORY_RARE_SEGMENT_THRESHOLD",
            self._get_setting("segment_rare_threshold", 5),
        )
        result: dict[str, Any] = {
            'is_rare_segment': True,
            'confidence': 'low',
            'recommended_price': 0.0,
            'price_source': 'expert',
        }

        # Текущие цены
        current_prices = [
            ad.price for ad in current_ads
            if ad.price is not None and ad.price > 0
        ]

        # 1. Попытка получить историю из БД
        history_median: float | None = None
        try:
            seg_stats_obj = self._get_segment_stats_obj(repo, search_id, segment_key)

            if seg_stats_obj is not None and hasattr(seg_stats_obj, 'id'):
                history = repo.get_price_history(seg_stats_obj.id, days=90)
                if history:
                    history_prices = [
                        h.median_price for h in history
                        if h.median_price is not None and h.median_price > 0
                    ]
                    if history_prices:
                        history_median = float(statistics.median(history_prices))
        except Exception as exc:
            self._log.warning(
                "rare_segment_history_failed",
                segment_key=segment_key,
                error=str(exc),
            )

        # 2. Иерархия сегментов — пробуем родительские
        parent_median: float | None = None
        try:
            key = CategorySegmentKey.from_string(segment_key)
            for level in range(1, 4):
                parent = key.parent(level=level)
                parent_str = parent.to_string()
                parent_stats = self._get_segment_stats_obj(
                    repo, search_id, parent_str,
                )
                if parent_stats is not None:
                    if (parent_stats.listing_price_median is not None
                            and parent_stats.listing_price_median > 0):
                        parent_median = parent_stats.listing_price_median
                        break
                    if (parent_stats.sample_size is not None
                            and parent_stats.sample_size >= rare_threshold):
                        # Родительский сегмент достаточно большой
                        break
        except Exception as exc:
            self._log.warning(
                "rare_segment_hierarchy_failed",
                segment_key=segment_key,
                error=str(exc),
            )

        # 3. Определяем рекомендованную цену и источник
        current_median: float | None = None
        if current_prices:
            current_median = float(statistics.median(current_prices))

        if history_median is not None and history_median > 0:
            result['recommended_price'] = history_median
            result['price_source'] = 'history'
            if current_median is not None:
                # Усредняем с текущей медианой
                result['recommended_price'] = (history_median + current_median) / 2
            result['confidence'] = 'medium'
        elif parent_median is not None and parent_median > 0:
            result['recommended_price'] = parent_median
            result['price_source'] = 'hierarchy'
            result['confidence'] = 'medium'
        elif current_median is not None and current_median > 0:
            result['recommended_price'] = current_median
            result['price_source'] = 'current'
            result['confidence'] = 'low'
        else:
            # Нет данных — экспертная оценка невозможна
            result['recommended_price'] = 0.0
            result['price_source'] = 'expert'
            result['confidence'] = 'low'

        self._log.debug(
            "rare_segment_stats",
            segment_key=segment_key,
            recommended_price=result['recommended_price'],
            price_source=result['price_source'],
            confidence=result['confidence'],
        )
        return result

    @staticmethod
    def _get_segment_stats_obj(
        repo: Any,
        search_id: int,
        segment_key: str,
    ) -> SegmentStats | None:
        """Получить один SegmentStats из репозитория.

        Скрывает различия между сигнатурами, возвращающими
        один объект или список.
        """
        result = repo.get_segment_stats(search_id, segment_key)
        if result is None:
            return None
        if isinstance(result, list):
            return result[0] if result else None
        return result

    # ------------------------------------------------------------------
    # Тренд цены
    # ------------------------------------------------------------------

    def calculate_price_trend(
        self,
        price_history: list[SegmentPriceHistory],
    ) -> float:
        """Рассчитывает наклон тренда цены (price_trend_slope) по истории.

        Использует линейную регрессию по median_price от snapshot_date.
        Возвращает slope (положительный = рост, отрицательный = снижение).
        Если данных < 3 точек — возвращает 0.0.

        Args:
            price_history: Записи истории цен сегмента.

        Returns:
            float: Наклон тренда (slope).
        """
        prices = [
            (i, h.median_price)
            for i, h in enumerate(price_history)
            if h.median_price is not None and h.median_price > 0
        ]

        if len(prices) < 3:
            return 0.0

        n = len(prices)
        x_vals = [p[0] for p in prices]
        y_vals = [p[1] for p in prices]

        x_mean = sum(x_vals) / n
        y_mean = sum(y_vals) / n

        numerator = sum(
            (x - x_mean) * (y - y_mean)
            for x, y in zip(x_vals, y_vals)
        )
        denominator = sum((x - x_mean) ** 2 for x in x_vals)

        if denominator == 0:
            return 0.0

        slope = numerator / denominator
        return float(slope)

    # ------------------------------------------------------------------
    # Основной метод анализа сегментов
    # ------------------------------------------------------------------

    def analyze_segments(
        self,
        ads: list[Ad],
        repo: Any,
        search_id: int,
    ) -> dict[str, dict]:
        """Интегрирует все методы анализа в единый поток.

        Алгоритм:
        1. Сегментация объявлений
        2. Для каждого сегмента:
           a. Рассчитать медианы за 7d, 30d, 90d
           b. Рассчитать тренд цены
           c. Рассчитать метрики ликвидности
           d. Определить, является ли сегмент редким
           e. Если редкий — использовать calculate_rare_segment_stats()
           f. Сохранить статистику через repo.upsert_segment_stats()
           g. Сохранить снапшот истории через save_segment_snapshot()

        Args:
            ads: Список объявлений для анализа.
            repo: Экземпляр репозитория.
            search_id: ID отслеживаемого поиска.

        Returns:
            dict[str, dict]: Маппинг segment_key → статистика.
        """
        rare_threshold = self._get_setting(
            "CATEGORY_RARE_SEGMENT_THRESHOLD",
            self._get_setting("segment_rare_threshold", 5),
        )

        # 1. Сегментация
        segments = self.segment_ads(ads)
        merged = self.merge_small_segments(segments)

        results: dict[str, dict] = {}

        for segment_key_str, segment_ads in merged.items():
            segment_key = CategorySegmentKey.from_string(segment_key_str)

            try:
                # 2a. Рассчитать базовую статистику с временными окнами
                stats_obj = self.calculate_segment_stats(
                    segment_ads, segment_key, repo=repo, search_id=search_id,
                )

                # Собираем dict для анализа
                # Извлекаем компоненты сегмента из segment_key
                stats_dict: dict[str, Any] = {
                    'category': segment_key.category or 'unknown',
                    'brand': segment_key.brand or 'unknown',
                    'model': segment_key.model or 'unknown',
                    'condition': segment_key.condition or 'unknown',
                    'location': segment_key.location or 'unknown',
                    'seller_type': 'unknown',
                    'sample_size': stats_obj.sample_size or 0,
                    'listing_count': stats_obj.listing_count or 0,
                    'median_price': stats_obj.listing_price_median,
                    'mean_price': stats_obj.mean_price,
                    'min_price': stats_obj.min_price,
                    'max_price': stats_obj.max_price,
                    'median_7d': stats_obj.median_7d,
                    'median_30d': stats_obj.median_30d,
                    'median_90d': stats_obj.median_90d,
                    'price_trend_slope': stats_obj.price_trend_slope,
                    'listing_price_median': stats_obj.listing_price_median,
                    'fast_sale_price_median': stats_obj.fast_sale_price_median,
                    'liquid_market_estimate': stats_obj.liquid_market_estimate,
                    'median_days_on_market': stats_obj.median_days_on_market,
                    'appearance_count_90d': stats_obj.appearance_count_90d or 0,
                }

                # 2b. Рассчитать тренд цены
                seg_stats_record = self._get_segment_stats_obj(
                    repo, search_id, segment_key_str,
                )
                if seg_stats_record is not None and hasattr(seg_stats_record, 'id'):
                    history = repo.get_price_history(seg_stats_record.id, days=90)
                    trend_slope = self.calculate_price_trend(history)
                    stats_dict['price_trend_slope'] = trend_slope

                # 2c. Рассчитать метрики ликвидности
                disappeared_ads: list[Ad] = []
                try:
                    disappeared_ads = repo.get_disappeared_ads(
                        search_id, since_days=90,
                    )
                except Exception:
                    pass

                liquidity = self.calculate_liquidity_metrics(
                    segment_ads, disappeared_ads,
                )
                stats_dict.update(liquidity)

                # 2d. Определить, является ли сегмент редким
                is_rare = (stats_dict['sample_size'] < rare_threshold)
                stats_dict['is_rare_segment'] = is_rare

                # 2e. Если редкий — использовать calculate_rare_segment_stats()
                if is_rare:
                    rare_stats = self.calculate_rare_segment_stats(
                        repo, search_id, segment_key_str, segment_ads,
                    )
                    stats_dict.update(rare_stats)
                    if rare_stats.get('recommended_price', 0) > 0:
                        stats_dict['liquid_market_estimate'] = (
                            rare_stats['recommended_price']
                        )

                # 2f. Сохранить статистику через repo.upsert_segment_stats()
                upsert_data = {
                    k: v for k, v in stats_dict.items()
                    if hasattr(SegmentStats, k)
                }
                saved_stats = repo.upsert_segment_stats(
                    search_id, segment_key_str, upsert_data,
                )

                # 2g. Сохранить снапшот истории
                self.save_segment_snapshot(
                    repo, saved_stats.id, stats_dict,
                )

                results[segment_key_str] = stats_dict

            except Exception as exc:
                self._log.warning(
                    "segment_analysis_failed",
                    segment_key=segment_key_str,
                    error=str(exc),
                )

        self._log.info(
            "segments_analyzed",
            search_id=search_id,
            total_segments=len(merged),
            successful=len(results),
        )
        return results

    # ------------------------------------------------------------------
    # Временные окна медианы
    # ------------------------------------------------------------------

    def _calculate_temporal_windows(
        self,
        segment_key: str,
        repo: Any,
        search_id: int,
        temporal_windows: list[int] | None = None,
    ) -> dict:
        """Рассчитать временные окна медианы и тренды из истории."""
        if temporal_windows is None:
            temporal_windows = [7, 30, 90]

        result: dict[str, Any] = {
            'median_7d': None,
            'median_30d': None,
            'median_90d': None,
            'price_trend_slope': None,
            'appearance_count_90d': 0,
        }

        try:
            # Получаем SegmentStats по search_id + segment_key
            seg_stats = self._get_segment_stats_obj(repo, search_id, segment_key)
            if seg_stats is None or not hasattr(seg_stats, 'id'):
                return result

            history = repo.get_price_history(seg_stats.id, days=90)
            if not history:
                return result

            now = datetime.datetime.now(datetime.timezone.utc)

            for window in temporal_windows:
                cutoff = now - datetime.timedelta(days=window)
                window_prices = [
                    h.median_price for h in history
                    if h.snapshot_date >= cutoff.date() and h.median_price and h.median_price > 0
                ]
                if window_prices:
                    median_val = float(statistics.median(window_prices))
                    result[f'median_{window}d'] = median_val

            # Частота появления
            result['appearance_count_90d'] = sum(
                h.listing_count or 0 for h in history
            )

        except Exception as e:
            self._log.warning(
                "temporal_windows_calc_failed",
                segment_key=segment_key,
                error=str(e),
            )

        return result

    # ------------------------------------------------------------------
    # Составной score бриллианта
    # ------------------------------------------------------------------

    def _calculate_composite_score(
        self,
        ad_price: float,
        median_price: float,
        ad_count: int,
        appearance_count_90d: int,
        median_days_on_market: float | None,
        supply_change_percent: float | None,
        weights: dict[str, float] | None = None,
        rare_threshold: int | None = None,
    ) -> dict:
        """Рассчитать составной score бриллианта."""
        if weights is None:
            weights = {
                'rarity': self._get_setting("CATEGORY_SCORE_WEIGHT_RARITY", 0.3),
                'discount': self._get_setting("CATEGORY_SCORE_WEIGHT_DISCOUNT", 0.3),
                'liquidity': self._get_setting("CATEGORY_SCORE_WEIGHT_LIQUIDITY", 0.2),
                'supply': self._get_setting("CATEGORY_SCORE_WEIGHT_SUPPLY", 0.2),
            }

        if rare_threshold is None:
            rare_threshold = self._get_setting(
                "CATEGORY_RARE_SEGMENT_THRESHOLD",
                self._get_setting("segment_rare_threshold", 5),
            )

        # 1. Rarity score
        rarity = 0.0
        if ad_count > 0:
            rarity = min(1.0, rare_threshold / ad_count)
        if appearance_count_90d > 0:
            frequency_rarity = min(1.0, 10.0 / appearance_count_90d)
            rarity = (rarity + frequency_rarity) / 2

        # 2. Discount score
        discount = 0.0
        if median_price and median_price > 0:
            discount_percent = (median_price - ad_price) / median_price
            discount = min(1.0, max(0.0, discount_percent / 0.5))

        # 3. Liquidity score
        liquidity = 0.5
        if median_days_on_market is not None and median_days_on_market > 0:
            liquidity = max(0.1, min(1.0, 10.0 / median_days_on_market))

        # 4. Supply score
        supply = 0.5
        if supply_change_percent is not None:
            if supply_change_percent < 0:
                supply = min(1.0, abs(supply_change_percent) / 0.5)
            else:
                supply = max(0.0, 0.5 - supply_change_percent)

        # Composite
        w = weights
        composite = (
            w.get('rarity', 0.3) * rarity
            + w.get('discount', 0.3) * discount
            + w.get('liquidity', 0.2) * liquidity
            + w.get('supply', 0.2) * supply
        )

        return {
            'composite_score': round(composite, 3),
            'rarity_score': round(rarity, 3),
            'discount_score': round(discount, 3),
            'liquidity_score': round(liquidity, 3),
            'supply_score': round(supply, 3),
        }

    # ------------------------------------------------------------------
    # Детекция резкого падения цены
    # ------------------------------------------------------------------

    def detect_price_drop(
        self,
        ad: Ad,
        repo: Any,
        threshold: float | None = None,
    ) -> bool:
        """Детекция резкого падения цены относительно истории объявлений."""
        if threshold is None:
            threshold = self._get_setting("CATEGORY_PRICE_DROP_THRESHOLD", 0.8)

        try:
            from app.storage.models import AdSnapshot
            snapshots = repo.session.query(AdSnapshot).filter(
                AdSnapshot.ad_id == ad.id
            ).order_by(AdSnapshot.scraped_at.desc()).limit(5).all()

            if len(snapshots) < 2:
                return False

            previous_price = snapshots[1].price
            if previous_price and previous_price > 0 and ad.price is not None:
                return ad.price < previous_price * threshold
        except Exception as exc:
            self._log.warning(
                "price_drop_detection_failed",
                ad_id=getattr(ad, 'ad_id', None),
                error=str(exc),
            )
        return False

    # ------------------------------------------------------------------
    # Детекция бриллиантов
    # ------------------------------------------------------------------

    def detect_diamonds(
        self,
        ads: list[Ad],
        segments: dict[str, list[Ad]],
        segment_stats_map: dict[str, SegmentStats] | None = None,
    ) -> list[DiamondAlert]:
        """Детекция «бриллиантов» — товаров значительно ниже рынка."""
        if segment_stats_map is None:
            segment_stats_map = {}

        discount_threshold = self._get_setting("CATEGORY_DISCOUNT_THRESHOLD", 0.7)
        rare_threshold = self._get_setting(
            "CATEGORY_RARE_SEGMENT_THRESHOLD",
            self._get_setting("segment_rare_threshold", 5),
        )

        diamonds: list[DiamondAlert] = []

        for ad in ads:
            if ad.price is None or ad.price <= 0:
                continue

            seg_key = self.build_segment_key(ad)
            seg_key_str = seg_key.to_string()

            stats = self._find_stats(seg_key_str, segment_stats_map)
            if stats is None:
                continue

            is_rare = (stats.sample_size or 0) < rare_threshold

            if is_rare:
                diamond = self._detect_rare_diamond(ad, seg_key, stats)
            else:
                diamond = self._detect_frequent_diamond(
                    ad, seg_key, stats, discount_threshold,
                )

            if diamond is not None:
                diamonds.append(diamond)

        self._log.info(
            "diamonds_detected",
            total_ads=len(ads),
            diamonds=len(diamonds),
        )
        return diamonds

    def _find_stats(
        self,
        seg_key_str: str,
        stats_map: dict[str, SegmentStats],
    ) -> SegmentStats | None:
        """Найти статистику сегмента с fallback на родительские."""
        if seg_key_str in stats_map:
            return stats_map[seg_key_str]

        key = CategorySegmentKey.from_string(seg_key_str)
        for level in range(1, 5):
            parent = key.parent(level=level)
            parent_str = parent.to_string()
            if parent_str in stats_map:
                return stats_map[parent_str]

        return None

    @staticmethod
    def _stats_to_dict(stats: SegmentStats) -> dict:
        """Преобразовать SegmentStats в dict для get_best_median()."""
        return {
            'sample_size': stats.sample_size or 0,
            'listing_count': stats.listing_count or 0,
            'median_price': stats.listing_price_median,
            'mean_price': stats.mean_price,
            'min_price': stats.min_price,
            'max_price': stats.max_price,
            'median_7d': stats.median_7d,
            'median_30d': stats.median_30d,
            'median_90d': stats.median_90d,
            'price_trend_slope': stats.price_trend_slope,
            'listing_price_median': stats.listing_price_median,
            'fast_sale_price_median': stats.fast_sale_price_median,
            'liquid_market_estimate': stats.liquid_market_estimate,
            'median_days_on_market': stats.median_days_on_market,
            'appearance_count_90d': stats.appearance_count_90d or 0,
            'is_rare_segment': stats.is_rare_segment or False,
        }

    def _detect_frequent_diamond(
        self,
        ad: Ad,
        seg_key: CategorySegmentKey,
        stats: SegmentStats,
        discount_threshold: float,
    ) -> DiamondAlert | None:
        """Детекция бриллианта для частого сегмента."""
        stats_dict = self._stats_to_dict(stats)
        best_median, _reason = self.get_best_median(stats_dict)
        if best_median <= 0:
            return None

        threshold_price = best_median * discount_threshold

        if ad.price < threshold_price:
            discount_pct = (best_median - ad.price) / best_median * 100
            reason = (
                f"\u0426\u0435\u043d\u0430 {ad.price:,.0f}\u20bd "
                f"\u043f\u0440\u0438 \u043c\u0435\u0434\u0438\u0430\u043d\u0435 "
                f"{best_median:,.0f}\u20bd "
                f"(-{discount_pct:.1f}%). "
                f"\u0421\u0435\u0433\u043c\u0435\u043d\u0442: "
                f"{seg_key.to_string()}, "
                f"\u0432\u044b\u0431\u043e\u0440\u043a\u0430: "
                f"{stats.sample_size or 0} \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u0439"
            )
            return DiamondAlert(
                ad=ad,
                segment_key=seg_key,
                segment_stats=stats,
                price=ad.price,
                median_price=best_median,
                discount_percent=discount_pct,
                sample_size=stats.sample_size or 0,
                reason=reason,
                is_rare_segment=False,
            )

        return None

    def _detect_rare_diamond(
        self,
        ad: Ad,
        seg_key: CategorySegmentKey,
        stats: SegmentStats,
    ) -> DiamondAlert | None:
        """Детекция бриллианта для редкого сегмента.

        Использует исторические медианы и составной score.
        """
        discount_threshold = self._get_setting("CATEGORY_DISCOUNT_THRESHOLD", 0.7)

        # Выбираем референсную цену
        reference_price: float | None = None
        reference_label = ""

        if stats.median_90d is not None and stats.median_90d > 0:
            reference_price = stats.median_90d
            reference_label = (
                f"\u043c\u0435\u0434\u0438\u0430\u043d\u0430 90\u0434 "
                f"({reference_price:,.0f}\u20bd)"
            )
        elif stats.median_30d is not None and stats.median_30d > 0:
            reference_price = stats.median_30d
            reference_label = (
                f"\u043c\u0435\u0434\u0438\u0430\u043d\u0430 30\u0434 "
                f"({reference_price:,.0f}\u20bd)"
            )
        elif (stats.fast_sale_price_median is not None
              and stats.fast_sale_price_median > 0):
            reference_price = stats.fast_sale_price_median
            reference_label = (
                f"\u043c\u0435\u0434\u0438\u0430\u043d\u0430 \u0431\u044b\u0441\u0442\u0440\u044b\u0445 "
                f"\u043f\u0440\u043e\u0434\u0430\u0436 ({reference_price:,.0f}\u20bd)"
            )
        elif stats.listing_price_median is not None and stats.listing_price_median > 0:
            reference_price = stats.listing_price_median
            reference_label = (
                f"\u0442\u0435\u043a\u0443\u0449\u0430\u044f \u043c\u0435\u0434\u0438\u0430\u043d\u0430 "
                f"({reference_price:,.0f}\u20bd)"
            )

        if reference_price is None or reference_price <= 0:
            return None

        threshold_price = reference_price * discount_threshold

        if ad.price < threshold_price:
            discount_pct = (reference_price - ad.price) / reference_price * 100

            # Рассчитываем составной score
            score_result = self._calculate_composite_score(
                ad_price=ad.price,
                median_price=reference_price,
                ad_count=stats.sample_size or 0,
                appearance_count_90d=stats.appearance_count_90d or 0,
                median_days_on_market=stats.median_days_on_market,
                supply_change_percent=None,
            )

            reason = (
                f"\U0001f48e \u0420\u0415\u0414\u041a\u0418\u0419 \u0421\u0415\u0413\u041c\u0415\u041d\u0422: "
                f"\u0426\u0435\u043d\u0430 {ad.price:,.0f}\u20bd "
                f"\u043f\u0440\u0438 {reference_label} "
                f"(-{discount_pct:.1f}%). "
                f"\u0421\u0435\u0433\u043c\u0435\u043d\u0442: {seg_key.to_string()}, "
                f"\u0430\u043a\u0442\u0438\u0432\u043d\u044b\u0445: {stats.sample_size or 0}, "
                f"\u0437\u0430 90\u0434: {stats.appearance_count_90d or 0} "
                f"\u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u0439, "
                f"score: {score_result['composite_score']:.2f}"
            )
            return DiamondAlert(
                ad=ad,
                segment_key=seg_key,
                segment_stats=stats,
                price=ad.price,
                median_price=reference_price,
                discount_percent=discount_pct,
                sample_size=stats.sample_size or 0,
                reason=reason,
                is_rare_segment=True,
                composite_score=score_result['composite_score'],
                rarity_score=score_result['rarity_score'],
                discount_score=score_result['discount_score'],
                liquidity_score=score_result['liquidity_score'],
                supply_score=score_result['supply_score'],
                appearance_count_90d=stats.appearance_count_90d or 0,
                supply_drop_percent=None,
            )

        return None
