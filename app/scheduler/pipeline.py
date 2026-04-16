"""Оркестратор одного цикла сбора и анализа данных Avito."""

from __future__ import annotations

import asyncio
import re
import time
from datetime import datetime, timezone

import psutil
import structlog

from app.config import get_settings
from app.config.settings import Settings
from app.collector import BrowserManager, AvitoCollector
from app.parser import parse_search_page, parse_ad_page, SearchResultItem, AdData
from app.storage import Repository, get_session
from app.storage.models import TrackedSearch
from app.analysis import PriceAnalyzer, UndervaluedAd, AdAnalysisResult, MarketStats
from app.analysis.accessory_filter import AccessoryFilter
from app.analysis.segment_analyzer import SegmentAnalyzer, DiamondAlert, CategorySegmentKey
from app.analysis.attribute_extractor import AttributeExtractor
from app.notifier import EmailNotifier, TelegramNotifier
from app.utils import random_delay, setup_logging, extract_ad_id_from_url, normalize_url, build_page_url


class Pipeline:
    """Оркестратор одного цикла сбора и анализа данных Avito.

    Выполняет полный пайплайн:
        1. Настройка логирования и стартовая задержка.
        2. Подключение к БД и запуск браузера.
        3. Сбор поисковых страниц и карточек объявлений.
        4. Парсинг и сохранение данных.
        5. Ценовой анализ и отправка уведомлений.

    Attributes:
        settings: Конфигурация приложения.
        logger: structlog-логгер.
        stats: Словарь со статистикой цикла.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings: Settings = settings or get_settings()
        self.logger = structlog.get_logger("pipeline")
        self.stats: dict[str, int] = {
            "searches_processed": 0,
            "ads_found": 0,
            "ads_new": 0,
            "ads_scraped": 0,
            "ads_parsed": 0,
            "ads_undervalued": 0,
            "ads_filtered": 0,
            "notifications_sent": 0,
            "errors": 0,
        }

    # ------------------------------------------------------------------
    # Публичные методы
    # ------------------------------------------------------------------

    async def run(self) -> dict[str, int]:
        """Запустить один полный цикл сбора и анализа (legacy-режим).

        Использует ``SEARCH_URLS`` из конфига — обратная совместимость.

        Алгоритм:
            1. Настроить логирование.
            2. Случайная задержка перед стартом.
            3. Подключиться к БД.
            4. Запустить браузер.
            5. Для каждого ``search_url`` — сбор и парсинг.
            6. Анализ цен и отправка уведомлений.
            7. Фиксация транзакции.
            8. Возврат статистики.

        Returns:
            dict[str, int]: Статистика выполненного цикла.
        """
        setup_logging(self.settings.LOG_LEVEL)
        self.logger.info("pipeline_starting")

        # Стартовая задержка
        await random_delay(
            self.settings.STARTUP_DELAY_MIN,
            self.settings.STARTUP_DELAY_MAX,
        )

        repo: Repository | None = None
        collector: AvitoCollector | None = None

        try:
            # Автосоздание таблиц при необходимости
            from app.storage.database import ensure_tables
            ensure_tables()

            # Подключение к БД
            session = get_session()
            repo = Repository(session)

            # Запуск браузера
            browser_manager = BrowserManager(
                headless=self.settings.HEADLESS,
                use_proxy=self.settings.USE_PROXY,
                proxy_url=self.settings.PROXY_URL,
            )
            await browser_manager.start()
            collector = AvitoCollector(browser_manager, self.settings)

            self.logger.info(
                "browser_ready",
                search_urls_count=len(self.settings.SEARCH_URLS),
            )

            # --- Цикл по поисковым URL ---
            for search_url in self.settings.SEARCH_URLS:
                try:
                    await self._process_search(search_url, collector, repo)
                    self.stats["searches_processed"] += 1
                except Exception as exc:
                    self.stats["errors"] += 1
                    self.logger.error(
                        "search_processing_failed",
                        search_url=search_url,
                        error=str(exc),
                    )

            # --- Анализ и уведомления ---
            await self._analyze_and_notify(repo)

            # Фиксация транзакции
            repo.commit()
            self.logger.info("pipeline_transaction_committed")

        except Exception as exc:
            self.stats["errors"] += 1
            self.logger.error("pipeline_fatal_error", error=str(exc))
            if repo is not None:
                try:
                    repo.rollback()
                    self.logger.warning("pipeline_transaction_rolled_back")
                except Exception as rb_exc:
                    self.logger.error(
                        "pipeline_rollback_failed", error=str(rb_exc),
                    )

        finally:
            # Закрытие ресурсов
            if collector is not None:
                try:
                    await collector.close()
                except Exception as exc:
                    self.logger.warning(
                        "collector_close_error", error=str(exc),
                    )

            if repo is not None:
                try:
                    repo.close()
                except Exception as exc:
                    self.logger.warning(
                        "repository_close_error", error=str(exc),
                    )

        self.logger.info("pipeline_completed", **self.stats)
        return self.stats

    async def run_search_cycle(
        self,
        searches: list[TrackedSearch] | None = None,
    ) -> dict[str, int]:
        """Основной цикл обработки поисков из БД (масштабированный режим).

        Алгоритм:
            1. Получить поиски, которые пора запускать:
               ``repo.get_searches_due_for_run()`` — либо использовать
               переданный список *searches* (принудительный запуск).
            2. Обрабатывать батчами по ``MAX_CONCURRENT_SEARCHES``
               с использованием ``asyncio.Semaphore``.
            3. Для каждого поиска: собрать страницу → парсить → взять
               первые N карточек → парсить карточки → сохранить →
               проанализировать.
            4. После обработки каждого поиска обновлять ``last_run_at``.
            5. Задержки между поисками и между батчами.

        Args:
            searches: Если передан — использовать этот список поисков
                (принудительный запуск). Иначе — получить просроченные
                из БД через ``repo.get_searches_due_for_run()``.

        Returns:
            dict[str, int]: Статистика выполненного цикла.
        """
        setup_logging(self.settings.LOG_LEVEL)
        self.logger.info("search_cycle_starting")
        cycle_start = time.monotonic()
        process = psutil.Process()
        self.logger.info("cycle_memory_start", 
            memory_mb=round(process.memory_info().rss / 1024 / 1024, 1))

        # Сброс статистики
        self.stats = {k: 0 for k in self.stats}

        session = get_session()
        repo = Repository(session)

        try:
            # Получаем поиски: либо переданные принудительно, либо просроченные
            if searches is not None:
                due_searches = searches
                self.logger.info(
                    "forced_searches_provided",
                    count=len(due_searches),
                )
            else:
                due_searches = repo.get_searches_due_for_run()

            if not due_searches:
                self.logger.info("no_searches_due_for_run")
                return self.stats

            self.logger.info(
                "searches_due_for_run",
                count=len(due_searches),
            )

            # Запуск браузера
            browser_manager = BrowserManager(
                headless=self.settings.HEADLESS,
                use_proxy=self.settings.USE_PROXY,
                proxy_url=self.settings.PROXY_URL,
            )
            await browser_manager.start()
            collector = AvitoCollector(browser_manager, self.settings)

            try:
                # Разбиваем на батчи
                batch_size = self.settings.MAX_CONCURRENT_SEARCHES
                semaphore = asyncio.Semaphore(batch_size)

                batches = [
                    due_searches[i:i + batch_size]
                    for i in range(0, len(due_searches), batch_size)
                ]

                analyzer = PriceAnalyzer()

                for batch_idx, batch in enumerate(batches):
                    self.logger.info(
                        "pipeline_heartbeat",
                        batch_num=batch_idx + 1,
                        total_batches=len(batches),
                        memory_mb=round(process.memory_info().rss / 1024 / 1024, 1),
                    )

                    # Обрабатываем батч параллельно с семафором
                    tasks = [
                        self._process_tracked_search(
                            search, collector, repo, analyzer, semaphore,
                        )
                        for search in batch
                    ]
                    await asyncio.gather(*tasks, return_exceptions=True)

                    # Задержка между батчами (кроме последнего)
                    if batch_idx < len(batches) - 1:
                        self.logger.info(
                            "batch_delay",
                            seconds=self.settings.BATCH_DELAY_SECONDS,
                        )
                        await asyncio.sleep(self.settings.BATCH_DELAY_SECONDS)

                # --- Анализ и уведомления для всех поисков ---
                await self._analyze_and_notify_searches(
                    repo, due_searches, analyzer,
                )

                # Фиксация транзакции
                repo.commit()
                self.logger.info("search_cycle_transaction_committed")

            finally:
                try:
                    await collector.close()
                except Exception as exc:
                    self.logger.warning(
                        "collector_close_error", error=str(exc),
                    )

        except Exception as exc:
            self.stats["errors"] += 1
            self.logger.error("search_cycle_fatal_error", error=str(exc))
            try:
                repo.rollback()
                self.logger.warning("search_cycle_transaction_rolled_back")
            except Exception as rb_exc:
                self.logger.error(
                    "search_cycle_rollback_failed", error=str(rb_exc),
                )

        finally:
            try:
                repo.close()
            except Exception as exc:
                self.logger.warning(
                    "repository_close_error", error=str(exc),
                )

        self.logger.info(
            "cycle_completed",
            duration_seconds=round(time.monotonic() - cycle_start, 1),
            memory_mb=round(process.memory_info().rss / 1024 / 1024, 1),
            **self.stats,
        )
        return self.stats

    # ------------------------------------------------------------------
    # Внутренние методы
    # ------------------------------------------------------------------

    async def _process_tracked_search(
        self,
        search: TrackedSearch,
        collector: AvitoCollector,
        repo: Repository,
        analyzer: PriceAnalyzer,
        semaphore: asyncio.Semaphore,
    ) -> None:
        """Обработать один отслеживаемый поиск с семафором.

        Args:
            search: Отслеживаемый поиск из БД.
            collector: Экземпляр сборщика.
            repo: Экземпляр репозитория.
            analyzer: Экземпляр анализатора цен.
            semaphore: Семафор для ограничения параллельности.
        """
        async with semaphore:
            search_start = time.monotonic()
            try:
                # Задержка между поисками в батче
                await asyncio.sleep(self.settings.SEARCH_DELAY_SECONDS)

                max_ads = (
                    search.max_ads_to_parse
                    or self.settings.DEFAULT_MAX_ADS_TO_PARSE
                )

                await self._process_search(
                    search.search_url,
                    collector,
                    repo,
                    max_ads=max_ads,
                )

                # Обновляем last_run_at
                repo.update_search_last_run(search.id)

                self.stats["searches_processed"] += 1
                self.logger.info(
                    "search_processed",
                    search_url=search.search_url,
                    duration_seconds=round(time.monotonic() - search_start, 1),
                )

            except Exception as exc:
                self.stats["errors"] += 1
                self.logger.error(
                    "tracked_search_failed",
                    search_id=search.id,
                    search_url=search.search_url,
                    error=str(exc),
                )

    async def _process_search(
        self,
        search_url: str,
        collector: AvitoCollector,
        repo: Repository,
        max_ads: int | None = None,
    ) -> list[str]:
        """Обработать один поисковый URL с пагинацией.

        Выполняет:
            1. Регистрацию поиска и запуск в БД.
            2. Сбор и парсинг поисковых страниц (до MAX_SEARCH_PAGES_PER_RUN).
            3. Фильтрацию уже известных объявлений.
            4. Обработку новых карточек (до лимита max_ads).
            5. Завершение записи запуска в БД.

        Пагинация прекращается досрочно, если на странице не найдено
        новых (ещё неизвестных) объявлений.

        Args:
            search_url: URL поисковой выдачи Avito.
            collector: Экземпляр сборщика.
            repo: Экземпляр репозитория.
            max_ads: Максимум объявлений для обработки.
                Если ``None`` — берётся из настроек.

        Returns:
            list[str]: Список ``ad_id`` новых обработанных объявлений.
        """
        if max_ads is None:
            max_ads = self.settings.MAX_ADS_PER_SEARCH_PER_RUN

        tracked_search = repo.get_or_create_tracked_search(search_url)
        search_run = repo.create_search_run(tracked_search.id)

        run_ads_found = 0
        run_ads_new = 0
        run_ads_opened = 0
        run_errors = 0
        pages_fetched = 0
        new_ad_ids: list[str] = []
        all_search_items: list[SearchResultItem] = []

        try:
            max_pages = self.settings.MAX_SEARCH_PAGES_PER_RUN

            # Получаем список известных ad_id один раз для всех страниц
            recent_ids = repo.get_recent_ad_ids(search_url)

            # Счётчик ошибок через список (mutable для замыкания)
            run_errors_counter = [0]

            # Семфор для параллельного сбора карточек
            ad_semaphore = asyncio.Semaphore(
                self.settings.MAX_CONCURRENT_AD_PAGES,
            )

            async def _process_ad_safe(
                item: SearchResultItem,
            ) -> str | None:
                """Обёртка для параллельной обработки с семафором."""
                async with ad_semaphore:
                    try:
                        return await self._process_ad(
                            item, search_url, collector, repo,
                        )
                    except Exception as exc:
                        run_errors_counter[0] += 1
                        self.stats["errors"] += 1
                        self.logger.error(
                            "ad_processing_failed",
                            url=item.url,
                            error=str(exc),
                        )
                        return None

            # --- Цикл по страницам пагинации ---
            all_new_items: list[SearchResultItem] = []

            for page_num in range(1, max_pages + 1):
                page_url = build_page_url(search_url, page_num)

                # Задержка перед сбором страницы
                await random_delay(
                    self.settings.MIN_DELAY_SECONDS,
                    self.settings.MAX_DELAY_SECONDS,
                )

                self.logger.info(
                    "collecting_search_page",
                    search_url=search_url,
                    page=page_num,
                    max_pages=max_pages,
                    page_url=page_url,
                )

                try:
                    html, _html_path = await collector.collect_search_page(
                        page_url,
                    )
                except Exception as exc:
                    self.logger.warning(
                        "search_page_collection_failed",
                        page=page_num,
                        page_url=page_url,
                        error=str(exc),
                    )
                    run_errors += 1
                    self.stats["errors"] += 1
                    # Прерываем пагинацию при ошибке загрузки
                    break

                pages_fetched += 1

                # Парсинг
                search_items: list[SearchResultItem] = parse_search_page(
                    html, page_url,
                )
                run_ads_found += len(search_items)
                self.stats["ads_found"] += len(search_items)

                self.logger.info(
                    "search_page_parsed",
                    search_url=search_url,
                    page=page_num,
                    items_found=len(search_items),
                )

                # Если страница пустая — прекращаем пагинацию
                if not search_items:
                    self.logger.info(
                        "search_page_empty_pagination_stop",
                        search_url=search_url,
                        page=page_num,
                    )
                    break

                # Ранняя фильтрация аксессуаров
                search_items, early_filtered = self._early_filter_search_items(
                    search_items,
                )
                if early_filtered > 0:
                    self.stats["ads_filtered"] += early_filtered

                all_search_items.extend(search_items)

                # Фильтрация уже известных
                new_on_page: list[SearchResultItem] = []
                for item in search_items:
                    ad_id = extract_ad_id_from_url(item.url)
                    if ad_id not in recent_ids:
                        _, created = repo.get_or_create_ad(
                            ad_id, normalize_url(item.url), search_url,
                        )
                        if created:
                            new_on_page.append(item)
                            recent_ids.add(ad_id)

                self.logger.info(
                    "search_page_new_items",
                    search_url=search_url,
                    page=page_num,
                    total_on_page=len(search_items),
                    new_on_page=len(new_on_page),
                )

                # Если новых нет — прекращаем пагинацию
                if not new_on_page:
                    self.logger.info(
                        "no_new_ads_pagination_stop",
                        search_url=search_url,
                        page=page_num,
                    )
                    break

                all_new_items.extend(new_on_page)

                # Если уже набрали достаточно — прекращаем
                if len(all_new_items) >= max_ads:
                    break

            # Лимит новых объявлений
            new_items = all_new_items[:max_ads]
            run_ads_new = len(new_items)
            self.stats["ads_new"] += run_ads_new

            self.logger.info(
                "pagination_summary",
                search_url=search_url,
                pages_fetched=pages_fetched,
                total_found=run_ads_found,
                new_ads=run_ads_new,
            )

            # Параллельная обработка карточек с семафором
            if new_items:
                ad_results = await asyncio.gather(
                    *[_process_ad_safe(item) for item in new_items],
                    return_exceptions=True,
                )

                for result in ad_results:
                    if isinstance(result, Exception):
                        run_errors += 1
                        self.stats["errors"] += 1
                    elif result is not None:
                        new_ad_ids.append(result)
                        run_ads_opened += 1

            run_errors += run_errors_counter[0]

            # Завершение запуска
            repo.complete_search_run(
                search_run.id,
                ads_found=run_ads_found,
                ads_new=run_ads_new,
                pages_fetched=pages_fetched,
                ads_opened=run_ads_opened,
                errors_count=run_errors,
            )

            # --- Обнаружение исчезнувших объявлений ---
            try:
                disappeared_ids = self._detect_disappeared_ads(
                    search_url, all_search_items, repo,
                )
                if disappeared_ids:
                    self.logger.info(
                        "ads_disappeared_detected",
                        search_url=search_url,
                        count=len(disappeared_ids),
                    )
            except Exception as disp_exc:
                self.logger.warning(
                    "disappeared_detection_failed",
                    search_url=search_url,
                    error=str(disp_exc),
                )

        except Exception as exc:
            run_errors += 1
            self.stats["errors"] += 1
            self.logger.error(
                "search_run_failed",
                search_url=search_url,
                error=str(exc),
            )
            try:
                repo.fail_search_run(search_run.id, error=str(exc))
            except Exception as fail_exc:
                self.logger.error(
                    "fail_search_run_error",
                    error=str(fail_exc),
                )

        return new_ad_ids

    async def _process_ad(
        self,
        search_item: SearchResultItem,
        search_url: str,
        collector: AvitoCollector,
        repo: Repository,
    ) -> str | None:
        """Обработать одну карточку объявления.

        Выполняет:
            1. Сбор HTML карточки.
            2. Парсинг данных.
            3. Обновление записи в БД.
            4. Создание снимка цены.

        Args:
            search_item: Элемент из поисковой выдачи.
            search_url: URL поискового запроса.
            collector: Экземпляр сборщика.
            repo: Экземпляр репозитория.

        Returns:
            str | None: ``ad_id`` при успехе или ``None`` при ошибке.
        """
        ad_id = extract_ad_id_from_url(search_item.url)
        normalized_url = normalize_url(search_item.url)

        savepoint = repo.session.begin_nested()
        try:
            # Задержка перед сбором
            await random_delay(
                self.settings.MIN_DELAY_SECONDS,
                self.settings.MAX_DELAY_SECONDS,
            )

            # Сбор карточки
            html, html_path = await collector.collect_ad_page(normalized_url)
            self.stats["ads_scraped"] += 1

            # Парсинг
            ad_data: AdData = parse_ad_page(html, normalized_url)
            self.stats["ads_parsed"] += 1

            # Обновление записи в БД
            repo.update_ad(
                ad_id,
                title=ad_data.title,
                price=ad_data.price,
                location=ad_data.location,
                seller_name=ad_data.seller_name,
                condition=ad_data.condition,
                publication_date=ad_data.publication_date,
                parse_status="parsed",
            )

            # Создание снимка
            ad_record, _ = repo.get_or_create_ad(
                ad_id, normalized_url, search_url,
            )

            # --- Трекинг оборачиваемости: обновить last_seen_at ---
            try:
                now = datetime.now(timezone.utc)
                days_on_market = None
                if (
                    hasattr(ad_record, "first_seen_at")
                    and ad_record.first_seen_at is not None
                ):
                    first_seen = ad_record.first_seen_at
                    if first_seen.tzinfo is None:
                        first_seen = first_seen.replace(tzinfo=timezone.utc)
                    delta = now - first_seen
                    days_on_market = delta.days
                repo.update_ad_tracking_fields(
                    ad_id=ad_record.id,
                    last_seen_at=now,
                    days_on_market=days_on_market,
                )
            except Exception as tracking_exc:
                self.logger.warning(
                    "ad_tracking_update_failed",
                    ad_id=ad_id,
                    error=str(tracking_exc),
                )
            repo.create_snapshot(
                ad_id=ad_record.id,
                price=ad_data.price,
                html_path=html_path,
            )

            savepoint.commit()

            self.logger.info(
                "ad_processed",
                ad_id=ad_id,
                title=ad_data.title,
                price=ad_data.price,
            )
            return ad_id

        except Exception as exc:
            savepoint.rollback()
            self.stats["errors"] += 1
            self.logger.error(
                "ad_card_failed",
                ad_id=ad_id,
                url=normalized_url,
                error=str(exc),
            )
            try:
                repo.update_ad(
                    ad_id,
                    parse_status="failed",
                    last_error=str(exc),
                )
            except Exception as update_exc:
                self.logger.error(
                    "ad_status_update_failed",
                    ad_id=ad_id,
                    error=str(update_exc),
                )
            return None

    async def _analyze_and_notify(self, repo: Repository) -> None:
        """Анализ цен и отправка уведомлений (legacy-режим).

        Для каждого поискового URL:
            1. Получить все объявления.
            2. Отфильтровать аксессуары.
            3. Запустить ценовой анализ.
            4. Собрать недооценённые объявления.

        Затем отправить уведомления через Telegram.

        Args:
            repo: Экземпляр репозитория.
        """
        all_undervalued: list[UndervaluedAd] = []

        price_analyzer = PriceAnalyzer()
        accessory_filter = AccessoryFilter(
            blacklist=self.settings.ACCESSORY_BLACKLIST,
            min_price=self.settings.MIN_PRICE_FILTER,
            price_ratio=self.settings.ACCESSORY_PRICE_RATIO_THRESHOLD,
            enabled=self.settings.ENABLE_ACCESSORY_FILTER,
        )

        for search_url in self.settings.SEARCH_URLS:
            try:
                ads = repo.get_ads_for_search(search_url)
                if not ads:
                    self.logger.info(
                        "no_ads_for_analysis",
                        search_url=search_url,
                    )
                    continue

                # Фильтрация аксессуаров
                filtered_count = 0
                filtered_ads = []
                for ad in ads:
                    filter_result = accessory_filter.is_accessory(ad, median_price=None)
                    if filter_result.is_filtered:
                        filtered_count += 1
                        self.logger.info(
                            "ad_filtered_as_accessory",
                            extra={"ad_id": ad.ad_id, "title": getattr(ad, 'title', ''), "reason": filter_result.reason},
                        )
                        continue
                    filtered_ads.append(ad)

                self.stats["ads_filtered"] += filtered_count

                if not filtered_ads:
                    self.logger.info(
                        "all_ads_filtered_as_accessories",
                        search_url=search_url,
                        filtered_count=filtered_count,
                    )
                    continue

                undervalued = price_analyzer.analyze_and_mark(filtered_ads, repo)
                all_undervalued.extend(undervalued)

                self.logger.info(
                    "analysis_completed",
                    search_url=search_url,
                    total_ads=len(ads),
                    filtered_as_accessory=filtered_count,
                    analyzed_ads=len(filtered_ads),
                    undervalued=len(undervalued),
                )

            except Exception as exc:
                self.stats["errors"] += 1
                self.logger.error(
                    "analysis_failed",
                    search_url=search_url,
                    error=str(exc),
                )

        self.stats["ads_undervalued"] = len(all_undervalued)

        # Отправка уведомлений
        await self._send_notifications(all_undervalued, repo)

    async def _analyze_and_notify_searches(
        self,
        repo: Repository,
        searches: list[TrackedSearch],
        analyzer: PriceAnalyzer,
    ) -> None:
        """Анализ цен и отправка уведомлений для масштабированного режима.

        Для каждого отслеживаемого поиска:
            1. Получить объявления за ``TEMPORAL_WINDOW_DAYS``.
            2. Отфильтровать аксессуары и мелочёвку.
            3. Проанализировать каждое объявление через ``analyzer.analyze_ad()``.
            4. Обновить аналитические поля в БД.
            5. Собрать недооценённые объявления.

        Args:
            repo: Экземпляр репозитория.
            searches: Список обработанных поисков.
            analyzer: Экземпляр анализатора цен.
        """
        analyze_start = time.monotonic()
        all_undervalued: list[UndervaluedAd] = []

        accessory_filter = AccessoryFilter(
            blacklist=self.settings.ACCESSORY_BLACKLIST,
            min_price=self.settings.MIN_PRICE_FILTER,
            price_ratio=self.settings.ACCESSORY_PRICE_RATIO_THRESHOLD,
            enabled=self.settings.ENABLE_ACCESSORY_FILTER,
        )

        for search in searches:
            try:
                # --- Ветвление по типу поиска ---
                if getattr(search, 'is_category_search', False):
                    # Категорийный поиск — сегментный анализ
                    try:
                        diamonds = await self._analyze_category_search(
                            search, repo,
                        )
                        for diamond in diamonds:
                            undervalued_item = UndervaluedAd(
                                ad=diamond.ad,
                                market_stats=MarketStats(
                                    search_url=search.search_url,
                                    count=diamond.sample_size,
                                    median_price=diamond.median_price,
                                    mean_price=diamond.median_price,
                                    q1_price=None,
                                ),
                                deviation_percent=-diamond.discount_percent,
                                threshold_used=self.settings.UNDERVALUE_THRESHOLD,
                            )
                            all_undervalued.append(undervalued_item)
                    except Exception as cat_exc:
                        self.logger.error(
                            "category_analysis_failed",
                            search_id=search.id,
                            search_url=search.search_url,
                            error=str(cat_exc),
                        )
                    continue

                # --- Стандартный путь: анализ конкретных моделей ---
                ads = repo.get_ads_for_analysis(
                    search.search_url,
                    days=self.settings.TEMPORAL_WINDOW_DAYS,
                )
                if not ads:
                    self.logger.info(
                        "no_ads_for_analysis",
                        search_url=search.search_url,
                    )
                    continue

                # Фильтрация аксессуаров
                filtered_count = 0
                filtered_ads = []
                for ad in ads:
                    if ad.price is None or ad.price <= 0:
                        continue

                    filter_result = accessory_filter.is_accessory(ad, median_price=None)
                    if filter_result.is_filtered:
                        filtered_count += 1
                        self.logger.info(
                            "ad_filtered_as_accessory",
                            ad_id=ad.ad_id,
                            title=getattr(ad, 'title', ''),
                            reason=filter_result.reason,
                        )
                        continue
                    filtered_ads.append(ad)

                self.stats["ads_filtered"] += filtered_count

                if not filtered_ads:
                    self.logger.info(
                        "all_ads_filtered_as_accessories",
                        search_url=search.search_url,
                        filtered_count=filtered_count,
                    )
                    continue

                # Анализируем каждое объявление через v2
                for ad in filtered_ads:
                    try:
                        result: AdAnalysisResult | None = analyzer.analyze_ad(
                            ad, filtered_ads,
                        )
                        if result is None:
                            continue

                        # Обновляем аналитические поля в БД
                        repo.update_ad_analysis(
                            ad_id=ad.id,
                            z_score=result.undervalued_result.z_score,
                            iqr_outlier=result.undervalued_result.score_iqr > 0,
                            segment_key=result.segment_key,
                        )

                        # Обновляем флаг undervalued
                        if result.undervalued_result.is_undervalued:
                            repo.update_ad(
                                ad.ad_id,
                                is_undervalued=True,
                                undervalue_score=result.undervalued_result.score,
                            )

                            # Создаём UndervaluedAd для уведомления
                            undervalued_item = UndervaluedAd(
                                ad=ad,
                                market_stats=result.market_stats,
                                deviation_percent=(
                                    (ad.price - (result.market_stats.median_price or ad.price))
                                    / (result.market_stats.median_price or ad.price)
                                    * 100
                                ),
                                threshold_used=self.settings.UNDERVALUE_THRESHOLD,
                            )
                            all_undervalued.append(undervalued_item)

                    except Exception as exc:
                        self.logger.error(
                            "ad_analysis_failed",
                            ad_id=ad.ad_id,
                            error=str(exc),
                        )

                self.logger.info(
                    "analysis_completed_for_search",
                    search_url=search.search_url,
                    total_ads=len(ads),
                    filtered_as_accessory=filtered_count,
                    analyzed_ads=len(filtered_ads),
                    undervalued=len(
                        [u for u in all_undervalued
                         if u.ad.search_url == search.search_url]
                    ),
                )

            except Exception as exc:
                self.stats["errors"] += 1
                self.logger.error(
                    "analysis_failed",
                    search_url=search.search_url,
                    error=str(exc),
                )

        self.stats["ads_undervalued"] = len(all_undervalued)

        self.logger.info(
            "analysis_completed",
            duration_seconds=round(time.monotonic() - analyze_start, 1),
            undervalued_count=len(all_undervalued),
        )

        # Фиксируем аналитические данные в БД до отправки уведомлений,
        # чтобы записи NotificationSent (из mark_notification_sent) были
        # закоммичены до фактической отправки через Telegram/Email.
        repo.commit()
        self.logger.info("analysis_committed_before_notifications")

        # Отправка уведомлений
        await self._send_notifications(all_undervalued, repo)

    # ------------------------------------------------------------------
    # Трекинг оборачиваемости и сегментный анализ
    # ------------------------------------------------------------------

    def _detect_disappeared_ads(
        self,
        search_url: str,
        current_items: list[SearchResultItem],
        repo: Repository,
    ) -> list[int]:
        """Обнаружить объявления, отсутствующие в текущем сборе.

        Сравнивает все известные объявления для ``search_url`` с текущими
        собранными ``ad_id``.  Те, что не найдены в текущей выдаче и
        ещё не помечены как исчезнувшие, отмечаются через
        ``repo.mark_ads_disappeared()``.

        Args:
            search_url: URL поисковой выдачи.
            current_items: Элементы, найденные в текущем сборе.
            repo: Экземпляр репозитория.

        Returns:
            list[int]: Список внутренних ID (``Ad.id``) исчезнувших
            объявлений.
        """
        current_ad_ids: set[str] = {
            extract_ad_id_from_url(item.url)
            for item in current_items
            if item.url
        }

        # Получаем все объявления для данного поиска
        all_ads = repo.get_ads_for_search(search_url)

        disappeared_db_ids: list[int] = []
        for ad in all_ads:
            if ad.ad_id not in current_ad_ids:
                # Проверяем, не помечено ли уже
                if getattr(ad, "is_disappeared_quickly", False):
                    continue
                disappeared_db_ids.append(ad.id)

        if disappeared_db_ids:
            days_threshold = getattr(
                self.settings, "segment_fast_sale_days", 3,
            )
            try:
                repo.mark_ads_disappeared(
                    disappeared_db_ids,
                    days_threshold=days_threshold,
                )
            except Exception as exc:
                self.logger.warning(
                    "mark_disappeared_failed",
                    search_url=search_url,
                    error=str(exc),
                )

        return disappeared_db_ids

    async def _analyze_category_search(
        self,
        search: TrackedSearch,
        repo: Repository,
    ) -> list[DiamondAlert]:
        """Анализ категорийного поиска с полным сегментным анализом.

        Алгоритм:
            1. Получить объявления за ``TEMPORAL_WINDOW_DAYS``.
            2. Отфильтровать аксессуары.
            3. Извлечь атрибуты для объявлений без них.
            4. Сегментация через ``SegmentAnalyzer``.
            5. Расчёт статистики с временными окнами и ликвидностью.
            6. Сохранение snapshot в ``segment_price_history``.
            7. Детекция «бриллиантов».
            8. Обновление БД.

        Args:
            search: Отслеживаемый категорийный поиск.
            repo: Экземпляр репозитория.

        Returns:
            list[DiamondAlert]: Найденные «бриллианты».
        """
        segment_analyzer = SegmentAnalyzer(settings=self.settings)
        attribute_extractor = AttributeExtractor()

        # 1. Получить объявления
        ads = repo.get_ads_for_analysis(
            search.search_url,
            days=self.settings.TEMPORAL_WINDOW_DAYS,
        )
        if not ads:
            self.logger.info(
                "category_search_no_ads",
                search_id=search.id,
                search_url=search.search_url,
            )
            return []

        # 2. Фильтрация аксессуаров
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
                continue
            filtered_ads.append(ad)

        if not filtered_ads:
            self.logger.info(
                "category_search_all_filtered",
                search_id=search.id,
            )
            return []

        # 3. Извлечь атрибуты для объявлений без них
        for ad in filtered_ads:
            if not getattr(ad, "ad_category", None) and ad.title:
                try:
                    search_category = getattr(search, "category", None)
                    attrs = attribute_extractor.extract(
                        ad.title, search_category=search_category,
                    )
                    if attrs.category or attrs.brand or attrs.model:
                        repo.update_ad_tracking_fields(
                            ad_id=ad.id,
                            ad_category=attrs.category,
                            brand=attrs.brand,
                            extracted_model=attrs.model,
                        )
                except Exception as attr_exc:
                    self.logger.warning(
                        "attribute_extraction_failed",
                        ad_id=ad.ad_id,
                        error=str(attr_exc),
                    )

        # 4-6. Сегментация и расчёт статистики
        segment_results = segment_analyzer.analyze_segments(
            ads=filtered_ads,
            repo=repo,
            search_id=search.id,
        )

        self.logger.info(
            "category_search_segments_analyzed",
            search_id=search.id,
            total_ads=len(filtered_ads),
            segments=len(segment_results),
        )

        # 7. Детекция «бриллиантов»
        diamonds: list[DiamondAlert] = []
        for segment_key_str, stats_dict in segment_results.items():
            best_median, reason = segment_analyzer.get_best_median(stats_dict)
            if best_median <= 0:
                continue

            # Ищем «бриллианты» среди объявлений этого сегмента
            for ad in filtered_ads:
                if ad.price is None or ad.price <= 0:
                    continue
                diamond = self._check_diamond_candidate(
                    ad, stats_dict, best_median, reason,
                )
                if diamond is not None:
                    diamonds.append(diamond)

        self.logger.info(
            "category_search_diamonds_detected",
            search_id=search.id,
            diamonds_count=len(diamonds),
        )

        return diamonds

    def _check_diamond_candidate(
        self,
        ad: "Ad",
        segment_stats: dict,
        best_median: float,
        median_reason: str,
    ) -> DiamondAlert | None:
        """Проверяет, является ли объявление «бриллиантом».

        Условия:
            - Сегмент редкий (``is_rare_segment = True``) **или**
              цена < ``listing_price_median * 0.7`` (на 30% ниже медианы).
            - И/или цена < ``fast_sale_price_median * 0.8``.
            - И/или исторически быстрые продажи были сильно выше текущего
              листинга.

        Args:
            ad: Объявление для проверки.
            segment_stats: Словарь статистики сегмента.
            best_median: Лучшая медиана сегмента.
            median_reason: Описание причины выбора медианы.

        Returns:
            DiamondAlert | None: Алерт если «бриллиант», иначе ``None``.
        """
        if ad.price is None or ad.price <= 0 or best_median <= 0:
            return None

        listing_price_median = segment_stats.get("listing_price_median")
        fast_sale_price_median = segment_stats.get("fast_sale_price_median")
        is_rare = segment_stats.get("is_rare_segment", False)
        sample_size = segment_stats.get("sample_size", 0)

        # Пороговые коэффициенты
        discount_threshold = 0.7   # 30% ниже медианы
        fast_sale_threshold = 0.8  # 20% ниже медианы быстрой продажи

        reasons: list[str] = []
        is_diamond = False

        # Проверка: цена значительно ниже медианы листинга
        if (
            listing_price_median is not None
            and listing_price_median > 0
            and ad.price < listing_price_median * discount_threshold
        ):
            is_diamond = True
            reasons.append(
                f"цена {ad.price:,.0f}₽ < listing_median "
                f"{listing_price_median:,.0f}₽ × {discount_threshold}"
            )

        # Проверка: цена ниже медианы быстрых продаж
        if (
            fast_sale_price_median is not None
            and fast_sale_price_median > 0
            and ad.price < fast_sale_price_median * fast_sale_threshold
        ):
            is_diamond = True
            reasons.append(
                f"цена {ad.price:,.0f}₽ < fast_sale_median "
                f"{fast_sale_price_median:,.0f}₽ × {fast_sale_threshold}"
            )

        # Редкий сегмент — дополнительный бонус
        if is_rare and ad.price < best_median * 0.85:
            is_diamond = True
            reasons.append(
                f"редкий сегмент + цена {ad.price:,.0f}₽ < "
                f"best_median {best_median:,.0f}₽ × 0.85"
            )

        if not is_diamond:
            return None

        discount_percent = (
            (best_median - ad.price) / best_median * 100
        )

        # Сегментный ключ
        segment_key = CategorySegmentKey(
            category=getattr(ad, "ad_category", "unknown") or "unknown",
            brand=getattr(ad, "brand", "unknown") or "unknown",
            model=getattr(ad, "extracted_model", "unknown") or "unknown",
            condition=getattr(ad, "condition", "unknown") or "unknown",
            location=getattr(ad, "location", "unknown") or "unknown",
        )

        self.logger.info(
            "diamond_detected",
            ad_id=ad.ad_id,
            price=ad.price,
            best_median=best_median,
            discount_percent=round(discount_percent, 1),
            reason="; ".join(reasons),
        )

        return DiamondAlert(
            ad=ad,
            segment_key=segment_key,
            segment_stats=None,
            price=ad.price,
            median_price=best_median,
            discount_percent=discount_percent,
            sample_size=sample_size,
            reason="; ".join(reasons),
            is_rare_segment=is_rare,
        )

    def _early_filter_search_items(
        self,
        items: list[SearchResultItem],
    ) -> tuple[list[SearchResultItem], int]:
        """Ранняя фильтрация элементов поиска по цене и стоп-словам.

        Фильтрует на этапе поисковой выдачи (до сбора карточек),
        чтобы сэкономить ресурсы браузера. Проверяет:
        1. Минимальную цену (по price_str, если удалось извлечь число).
        2. Чёрный список слов в названии.

        Args:
            items: Список элементов из поисковой выдачи.

        Returns:
            Кортеж (отфильтрованный список, количество отсеянных).
        """
        if not self.settings.ENABLE_ACCESSORY_FILTER:
            return items, 0

        blacklist = [w.lower().strip() for w in self.settings.ACCESSORY_BLACKLIST if w.strip()]
        min_price = self.settings.MIN_PRICE_FILTER

        kept: list[SearchResultItem] = []
        filtered = 0

        for item in items:
            # Проверка по стоп-словам в названии
            title_lower = (item.title or "").lower()
            skip = False
            for word in blacklist:
                if word in title_lower:
                    self.logger.info(
                        "search_item_filtered_blacklist",
                        ad_id=item.ad_id,
                        title=item.title,
                        reason=f"Стоп-слово '{word}' в названии",
                    )
                    skip = True
                    break

            if skip:
                filtered += 1
                continue

            # Проверка по минимальной цене (если удалось извлечь число)
            if item.price_str and min_price > 0:
                price_num = self._extract_price_number(item.price_str)
                if price_num is not None and price_num < min_price:
                    self.logger.info(
                        "search_item_filtered_min_price",
                        ad_id=item.ad_id,
                        title=item.title,
                        price=price_num,
                        reason=f"Цена {price_num}₽ ниже минимальной {min_price}₽",
                    )
                    filtered += 1
                    continue

            kept.append(item)

        return kept, filtered

    @staticmethod
    def _extract_price_number(price_str: str) -> int | None:
        """Извлечь числовое значение цены из строки.

        Args:
            price_str: Строка цены (например «15 000 ₽», «15000»).

        Returns:
            Числовое значение цены или None.
        """
        if not price_str:
            return None
        # Удаляем всё кроме цифр
        digits = re.sub(r"\D", "", price_str)
        if digits:
            return int(digits)
        return None

    async def _send_notifications(
        self,
        all_undervalued: list[UndervaluedAd],
        repo: Repository,
    ) -> None:
        """Отправить уведомления о недооценённых объявлениях.

        Сначала пробует Telegram, при неудаче — email (fallback).

        Args:
            all_undervalued: Список недооценённых объявлений.
            repo: Экземпляр репозитория.
        """
        if not all_undervalued:
            self.logger.info("no_undervalued_ads_to_notify")
            return

        # Сначала пробуем Telegram
        telegram_ok = False
        try:
            notifier = TelegramNotifier()
            results = await notifier.send_undervalued_notifications(
                all_undervalued, repo,
            )
            sent_count = sum(1 for r in results if r.success)
            self.stats["notifications_sent"] = sent_count

            self.logger.info(
                "notifications_sent",
                channel="telegram",
                total_undervalued=len(all_undervalued),
                sent=sent_count,
                failed=len(results) - sent_count,
            )

            if sent_count > 0:
                telegram_ok = True

        except Exception as exc:
            self.logger.error(
                "telegram_notifications_failed",
                error=str(exc),
            )

        # Если Telegram не сработал — пробуем email (fallback)
        if not telegram_ok:
            self.logger.info("falling_back_to_email")
            try:
                email_notifier = EmailNotifier()
                results = await email_notifier.send_undervalued_notifications(
                    all_undervalued, repo,
                )
                sent_count = sum(1 for r in results if r.success)
                self.stats["notifications_sent"] = sent_count

                self.logger.info(
                    "notifications_sent",
                    channel="email",
                    total_undervalued=len(all_undervalued),
                    sent=sent_count,
                    failed=len(results) - sent_count,
                )

            except Exception as exc:
                self.stats["errors"] += 1
                self.logger.error(
                    "email_notifications_failed",
                    error=str(exc),
                )
