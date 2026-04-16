"""Patch pipeline.py: fix bugs + add category monitoring.

Run: .venv\\Scripts\\python.exe scripts\\_fix_rollback.py
"""

import os
import re
import sys
from pathlib import Path

# Fix Windows console encoding
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

PIPELINE_PATH = Path(__file__).resolve().parent.parent / "app" / "scheduler" / "pipeline.py"


def fix_rollback(code: str) -> tuple[str, int]:
    """Убрать repo.rollback() из error handler _process_ad."""
    pattern = r"(\n            try:\n)                repo\.rollback\(\)\n(                repo\.update_ad\(\n                    ad_id,\n                    parse_status=\"failed\",)"
    replacement = r"\1\2"
    new_code, count = re.subn(pattern, replacement, code)
    return new_code, count


def fix_extra_structlog(code: str) -> tuple[str, int]:
    """Исправить extra={} в structlog вызовах."""
    pattern = r'extra=\{"ad_id": ad\.ad_id, "title": getattr\(ad, \'title\', \'\'\), "reason": filter_result\.reason\}'
    replacement = "ad_id=ad.ad_id, title=getattr(ad, 'title', ''), reason=filter_result.reason"
    new_code, count = re.subn(pattern, replacement, code)
    return new_code, count


def add_category_imports(code: str) -> tuple[str, int]:
    """Добавить импорты AttributeExtractor и SegmentAnalyzer."""
    if "from app.analysis.attribute_extractor import AttributeExtractor" in code:
        return code, 0

    old = "from app.analysis.accessory_filter import AccessoryFilter"
    new = (
        "from app.analysis.accessory_filter import AccessoryFilter\n"
        "from app.analysis.attribute_extractor import AttributeExtractor\n"
        "from app.analysis.segment_analyzer import SegmentAnalyzer, DiamondAlert"
    )
    new_code = code.replace(old, new)
    return new_code, 1


def add_category_branching(code: str) -> tuple[str, int]:
    """Добавить ветвление по типу поиска в _analyze_and_notify_searches."""
    if "Ветвление по типу поиска" in code:
        return code, 0

    # Ищем начало цикла for search in searches
    old = (
        "        for search in searches:\n"
        "            try:\n"
        "                ads = repo.get_ads_for_analysis("
    )
    new = (
        '        for search in searches:\n'
        '            try:\n'
        '                # Ветвление по типу поиска\n'
        '                if getattr(search, "search_type", "model") == "category":\n'
        '                    # НОВЫЙ ПУТЬ: категорийный анализ\n'
        '                    category_diamonds = await self._analyze_category_search(\n'
        '                        search, repo,\n'
        '                    )\n'
        '                    # Конвертируем DiamondAlert → UndervaluedAd для уведомлений\n'
        '                    for diamond in category_diamonds:\n'
        '                        undervalued_item = UndervaluedAd(\n'
        '                            ad=diamond.ad,\n'
        '                            market_stats=None,  # type: ignore\n'
        '                            deviation_percent=-diamond.discount_percent,\n'
        '                            threshold_used=self.settings.CATEGORY_DISCOUNT_THRESHOLD,\n'
        '                        )\n'
        '                        all_undervalued.append(undervalued_item)\n'
        '                    continue\n'
        '\n'
        '                # СУЩЕСТВУЮЩИЙ ПУТЬ: анализ конкретных моделей\n'
        '                ads = repo.get_ads_for_analysis('
    )
    new_code = code.replace(old, new)
    return new_code, 1 if new_code != code else 0


def add_analyze_category_search(code: str) -> tuple[str, int]:
    """Добавить метод _analyze_category_search."""
    if "def _analyze_category_search" in code:
        return code, 0

    method_code = '''
    async def _analyze_category_search(
        self,
        search: TrackedSearch,
        repo: Repository,
    ) -> list:
        """Анализ для category-поиска с сегментацией."""
        try:
            ads = repo.get_ads_for_analysis(
                search.search_url,
                days=self.settings.TEMPORAL_WINDOW_DAYS,
            )
            if not ads:
                self.logger.info(
                    "no_ads_for_category_analysis",
                    search_url=search.search_url,
                )
                return []

            # Фильтрация аксессуаров
            accessory_filter = AccessoryFilter(
                blacklist=self.settings.ACCESSORY_BLACKLIST,
                min_price=self.settings.MIN_PRICE_FILTER,
                price_ratio=self.settings.ACCESSORY_PRICE_RATIO_THRESHOLD,
                enabled=self.settings.ENABLE_ACCESSORY_FILTER,
            )

            filtered_ads = []
            for ad in ads:
                if ad.price is None or ad.price <= 0:
                    continue
                filter_result = accessory_filter.is_accessory(ad, median_price=None)
                if filter_result.is_filtered:
                    self.stats["ads_filtered"] += 1
                    continue
                filtered_ads.append(ad)

            if not filtered_ads:
                self.logger.info(
                    "all_ads_filtered_category",
                    search_url=search.search_url,
                )
                return []

            # Извлечение атрибутов для объявлений без них
            extractor = AttributeExtractor()
            search_category = getattr(search, "category", None)
            for ad in filtered_ads:
                if ad.brand is None and ad.title:
                    try:
                        attrs = extractor.extract(ad.title, search_category)
                        repo.update_ad(
                            ad.ad_id,
                            ad_category=attrs.category,
                            brand=attrs.brand,
                            extracted_model=attrs.model,
                        )
                    except Exception as exc:
                        self.logger.warning(
                            "category_attr_extraction_failed",
                            ad_id=ad.ad_id,
                            error=str(exc),
                        )

            # Сегментация
            segment_analyzer = SegmentAnalyzer(self.settings)
            segments = segment_analyzer.segment_ads(filtered_ads)
            segments = segment_analyzer.merge_small_segments(
                segments,
                min_size=self.settings.CATEGORY_MIN_SEGMENT_SIZE,
            )

            # Расчёт статистики и кэширование
            stats_map = {}
            for seg_key_str, seg_ads in segments.items():
                from app.analysis.segment_analyzer import CategorySegmentKey
                seg_key = CategorySegmentKey.from_string(seg_key_str)
                stats = segment_analyzer.calculate_segment_stats(seg_ads, seg_key)
                saved = repo.upsert_segment_stats(stats)
                stats_map[seg_key_str] = saved

            # Сохраняем ежедневные снимки истории
            today = datetime.now(timezone.utc).date()
            for seg_key_str, seg_ads in segments.items():
                prices = [
                    a.price for a in seg_ads
                    if a.price is not None and a.price > 0
                ]
                if prices:
                    import numpy as np
                    repo.save_price_history(
                        segment_key=seg_key_str,
                        date=today,
                        median_price=float(np.median(prices)),
                        ad_count=len(prices),
                        mean_price=float(np.mean(prices)),
                    )

            # Детекция бриллиантов
            diamonds = segment_analyzer.detect_diamonds(
                filtered_ads, segments, stats_map,
            )

            # Обновление БД
            for diamond in diamonds:
                repo.update_ad(
                    diamond.ad.ad_id,
                    is_undervalued=True,
                    undervalue_score=diamond.discount_percent / 100,
                    segment_key=diamond.segment_key.to_string(),
                )
                self.stats["ads_undervalued"] += 1

            self.logger.info(
                "category_analysis_completed",
                search_url=search.search_url,
                total_ads=len(ads),
                filtered_ads=len(filtered_ads),
                segments=len(segments),
                diamonds=len(diamonds),
            )

            return diamonds

        except Exception as exc:
            self.stats["errors"] += 1
            self.logger.error(
                "category_analysis_failed",
                search_url=search.search_url,
                error=str(exc),
            )
            return []

'''

    # Try to insert before _early_filter_search_items if it exists
    anchor = "    def _early_filter_search_items("
    if anchor in code:
        new_code = code.replace(anchor, method_code + anchor)
        return new_code, 1

    # Otherwise append at the end of the file
    new_code = code.rstrip() + "\n\n" + method_code
    return new_code, 1


def add_attribute_extraction(code: str) -> tuple[str, int]:
    """Добавить извлечение атрибутов в _process_ad."""
    if "ATTRIBUTE_EXTRACTION_ENABLED" in code:
        return code, 0

    old = (
        '            # Обновление записи в БД\n'
        '            repo.update_ad(\n'
        '                ad_id,\n'
    )
    new = (
        '            # Обновление записи в БД\n'
        '            update_kwargs = dict(\n'
    )
    new_code = code.replace(old, new, 1)

    if new_code == code:
        return code, 0

    # Заменяем закрывающую скобку и добавляем attribute extraction
    old2 = (
        '                parse_status="parsed",\n'
        '            )\n'
        '\n'
        '            # Создание снимка'
    )
    new2 = (
        '                parse_status="parsed",\n'
        '            )\n'
        '\n'
        '            # Извлечение атрибутов из заголовка (категорийный мониторинг)\n'
        '            if self.settings.ATTRIBUTE_EXTRACTION_ENABLED and ad_data.title:\n'
        '                try:\n'
        '                    extractor = AttributeExtractor()\n'
        '                    attrs = extractor.extract(title=ad_data.title)\n'
        '                    update_kwargs["ad_category"] = attrs.category\n'
        '                    update_kwargs["brand"] = attrs.brand\n'
        '                    update_kwargs["extracted_model"] = attrs.model\n'
        '                    update_kwargs["attributes_raw"] = json.dumps(\n'
        '                        attrs.raw, ensure_ascii=False,\n'
        '                    )\n'
        '                except Exception as attr_exc:\n'
        '                    self.logger.warning(\n'
        '                        "attribute_extraction_failed",\n'
        '                        ad_id=ad_id,\n'
        '                        error=str(attr_exc),\n'
        '                    )\n'
        '\n'
        '            repo.update_ad(ad_id, **update_kwargs)\n'
        '\n'
        '            # Создание снимка'
    )
    new_code = new_code.replace(old2, new2, 1)
    return new_code, 1


def main() -> None:
    if not PIPELINE_PATH.exists():
        print(f"❌ Файл не найден: {PIPELINE_PATH}")
        return

    code = PIPELINE_PATH.read_text(encoding="utf-8")
    total_fixes = 0

    # 1. Убираем repo.rollback()
    code, count = fix_rollback(code)
    if count:
        print(f"✅ Убран repo.rollback() из error handler ({count} вхождений)")
        total_fixes += count
    else:
        print("ℹ️ repo.rollback() не найден — уже исправлено")

    # 2. Исправляем extra={} в structlog
    code, count = fix_extra_structlog(code)
    if count:
        print(f"✅ Исправлен extra={{}} в structlog ({count} вхождений)")
        total_fixes += count
    else:
        print("ℹ️ extra={{}} не найден — уже исправлен")

    # 3. Добавляем импорты
    code, count = add_category_imports(code)
    if count:
        print("✅ Добавлены импорты AttributeExtractor, SegmentAnalyzer, DiamondAlert")
        total_fixes += count
    else:
        print("ℹ️ Импорты уже присутствуют")

    # 4. Добавляем attribute extraction в _process_ad
    code, count = add_attribute_extraction(code)
    if count:
        print("✅ Добавлено извлечение атрибутов в _process_ad")
        total_fixes += count
    else:
        print("ℹ️ Извлечение атрибутов уже присутствует")

    # 5. Добавляем category branching
    code, count = add_category_branching(code)
    if count:
        print("✅ Добавлено ветвление по типу поиска")
        total_fixes += count
    else:
        print("ℹ️ Ветвление по типу поиска уже присутствует")

    # 6. Добавляем _analyze_category_search
    code, count = add_analyze_category_search(code)
    if count:
        print("✅ Добавлен метод _analyze_category_search")
        total_fixes += count
    else:
        print("ℹ️ Метод _analyze_category_search уже присутствует")

    if total_fixes > 0:
        PIPELINE_PATH.write_text(code, encoding="utf-8")
        print(f"\n🎉 Применено {total_fixes} исправлений. Файл сохранён.")
    else:
        print("\n✨ Все исправления уже применены.")


if __name__ == "__main__":
    main()
