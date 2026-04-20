"""Оркестратор одного цикла сбора и анализа данных Avito."""

from __future__ import annotations

import asyncio
import random
import re
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import numpy as np
import psutil
import structlog

from app.config import get_settings
from app.config.settings import Settings
from app.collector import BrowserManager, AvitoCollector
from app.parser import parse_search_page, parse_ad_page, SearchResultItem, AdData
from app.parser.seller_parser import parse_seller_profile, SellerProfileData, SoldItemData
from app.storage import Repository, get_session
from app.storage.models import TrackedSearch
from app.analysis import PriceAnalyzer, UndervaluedAd, AdAnalysisResult, MarketStats
from app.analysis.accessory_filter import AccessoryFilter
from app.analysis.segment_analyzer import SegmentAnalyzer, DiamondAlert, CategorySegmentKey
from app.analysis.attribute_extractor import AttributeExtractor
from app.analysis.product_normalizer import normalize_title
from app.notifier import EmailNotifier, TelegramNotifier
from app.utils import random_delay, setup_logging, extract_ad_id_from_url, normalize_url, build_page_url
from app.utils.helpers import RateLimiter

if TYPE_CHECKING:
    from playwright.async_api import BrowserContext

# Невалидные seller_id — артефакты парсинга Avito
# (например "review" из URL https://www.avito.ru/user/review)
_INVALID_SELLER_IDS: frozenset[str] = frozenset({"review", "favorites", "blocked"})


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

            # Создаём раздельные RateLimiter'ы для поиска и карточек
            search_rate_limiter = RateLimiter(
                max_requests=self.settings.SEARCH_RATE_LIMIT_PER_MINUTE,
                per_seconds=60,
            )
            ad_rate_limiter = RateLimiter(
                max_requests=self.settings.AD_RATE_LIMIT_PER_MINUTE,
                per_seconds=60,
            )
            rate_limiter = RateLimiter(
                max_requests=self.settings.REQUEST_RATE_LIMIT_PER_MINUTE,
                per_seconds=60,
            )
            collector = AvitoCollector(
                browser_manager,
                self.settings,
                rate_limiter=rate_limiter,
                search_rate_limiter=search_rate_limiter,
                ad_rate_limiter=ad_rate_limiter,
            )

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

            # === Детекция warm-up режима ===
            is_warmup = self._detect_warmup(due_searches)

            if is_warmup:
                effective_batch_size = self.settings.WARMUP_MAX_CONCURRENT_SEARCHES
                effective_ad_concurrency = self.settings.WARMUP_MAX_CONCURRENT_ADS
                search_delay = self.settings.WARMUP_SEARCH_DELAY

                self.logger.info(
                    "warmup_mode_detected",
                    total_searches=len(due_searches),
                    batch_size=effective_batch_size,
                    ad_concurrency=effective_ad_concurrency,
                    search_delay=search_delay,
                )

                # Начальная задержка при warm-up
                self.logger.info(
                    "warmup_initial_delay",
                    seconds=self.settings.WARMUP_INITIAL_DELAY,
                )
                await asyncio.sleep(self.settings.WARMUP_INITIAL_DELAY)
            else:
                effective_batch_size = self.settings.MAX_CONCURRENT_SEARCHES
                effective_ad_concurrency = self.settings.MAX_CONCURRENT_AD_PAGES
                search_delay = self.settings.SEARCH_DELAY_SECONDS

            # Запуск браузера
            browser_manager = BrowserManager(
                headless=self.settings.HEADLESS,
                use_proxy=self.settings.USE_PROXY,
                proxy_url=self.settings.PROXY_URL,
            )
            await browser_manager.start()

            # Создаём раздельные RateLimiter'ы для поиска и карточек
            search_rate_limiter = RateLimiter(
                max_requests=self.settings.SEARCH_RATE_LIMIT_PER_MINUTE,
                per_seconds=60,
            )
            ad_rate_limiter = RateLimiter(
                max_requests=self.settings.AD_RATE_LIMIT_PER_MINUTE,
                per_seconds=60,
            )
            seller_rate_limiter = RateLimiter(
                max_requests=self.settings.SELLER_RATE_LIMIT_PER_MINUTE,
                per_seconds=60,
            )
            # Общий rate_limiter для обратной совместимости (legacy run)
            rate_limiter = RateLimiter(
                max_requests=self.settings.REQUEST_RATE_LIMIT_PER_MINUTE,
                per_seconds=60,
            )
            collector = AvitoCollector(
                browser_manager, self.settings,
                rate_limiter=rate_limiter,
                search_rate_limiter=search_rate_limiter,
                ad_rate_limiter=ad_rate_limiter,
                seller_rate_limiter=seller_rate_limiter,
            )

            try:
                # Разбиваем на батчи
                semaphore = asyncio.Semaphore(effective_batch_size)

                batches = [
                    due_searches[i:i + effective_batch_size]
                    for i in range(0, len(due_searches), effective_batch_size)
                ]

                analyzer = PriceAnalyzer()

                for batch_idx, batch in enumerate(batches):
                    self.logger.info(
                        "pipeline_heartbeat",
                        batch_num=batch_idx + 1,
                        total_batches=len(batches),
                        warmup=is_warmup,
                        memory_mb=round(process.memory_info().rss / 1024 / 1024, 1),
                    )

                    # Обрабатываем батч параллельно с семафором
                    tasks = [
                        self._process_tracked_search(
                            search, collector, repo, analyzer, semaphore,
                            is_warmup=is_warmup,
                            ad_concurrency=effective_ad_concurrency,
                            search_delay=search_delay,
                        )
                        for search in batch
                    ]
                    await asyncio.gather(*tasks, return_exceptions=True)

                    # Задержка между батчами (кроме последнего)
                    if batch_idx < len(batches) - 1:
                        delay_sec = (
                            self.settings.WARMUP_SEARCH_DELAY
                            if is_warmup
                            else self.settings.BATCH_DELAY_SECONDS
                        )
                        self.logger.info(
                            "batch_delay",
                            seconds=delay_sec,
                            warmup=is_warmup,
                        )
                        await asyncio.sleep(delay_sec)

                # --- Анализ и уведомления для всех поисков ---
                self.logger.info(
                    "analyze_and_notify_starting",
                    searches_count=len(due_searches),
                    searches_processed=self.stats["searches_processed"],
                    ads_found=self.stats["ads_found"],
                    ads_new=self.stats["ads_new"],
                    errors=self.stats["errors"],
                )
                await self._analyze_and_notify_searches(
                    repo, due_searches, analyzer,
                )
                self.logger.info(
                    "analyze_and_notify_completed",
                    ads_undervalued=self.stats["ads_undervalued"],
                    notifications_sent=self.stats["notifications_sent"],
                )

                # --- Seller Profile Collection ---
                try:
                    sellers_processed = await self._collect_seller_profiles(collector, repo)
                    if sellers_processed > 0:
                        self.logger.info(
                            "seller_profiles_collected",
                            sellers_processed=sellers_processed,
                        )
                except Exception as e:
                    self.logger.error(
                        "seller_profile_collection_error",
                        error=str(e),
                        exc_info=True,
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

    async def run_constant_cycle(
        self,
        force_all: bool = False,
    ) -> dict[str, int]:
        """Один цикл constant режима: search → force-pending.

        Алгоритм:
            1. Сбросить статистику.
            2. Вызвать ``run_search_cycle()`` — сбор новых объявлений.
               Если *force_all* — передать все активные поиски
               принудительно (полезно для первого цикла).
            3. Если ``CONSTANT_FORCE_PENDING_AFTER_SEARCH=True`` и есть
               pending объявления — вызвать ``run_force_pending_cycle()``.
            4. Вернуть объединённую статистику.

        Args:
            force_all: Если ``True`` — принудительно запустить ВСЕ
                активные поиски, игнорируя расписание.  Используется
                на первом цикле constant-режима.

        Returns:
            dict[str, int]: Объединённая статистика цикла.
        """
        self.logger.info("constant_cycle_start", force_all=force_all)

        # 1. Сброс статистики
        self.stats = {k: 0 for k in self.stats}

        # 2. Сбор новых объявлений
        forced_searches = None
        if force_all:
            try:
                session = get_session()
                repo = Repository(session)
                try:
                    forced_searches = repo.get_active_searches()
                finally:
                    repo.close()
            except Exception as exc:
                self.logger.error(
                    "constant_cycle_force_all_failed",
                    error=str(exc),
                    exc_info=True,
                )

        search_stats = await self.run_search_cycle(searches=forced_searches)
        self.logger.info(
            "constant_cycle_search_done",
            searches_processed=search_stats.get("searches_processed", 0),
            ads_found=search_stats.get("ads_found", 0),
            ads_new=search_stats.get("ads_new", 0),
            errors=search_stats.get("errors", 0),
        )

        # 3. Дообработка pending (если включена)
        pending_stats: dict[str, int] = {}
        if self.settings.CONSTANT_FORCE_PENDING_AFTER_SEARCH:
            try:
                session = get_session()
                repo = Repository(session)
                try:
                    pending_ads = repo.get_pending_ads()
                finally:
                    repo.close()

                if pending_ads:
                    self.logger.info(
                        "constant_cycle_pending_found",
                        pending_count=len(pending_ads),
                    )
                    pending_stats = await self.run_force_pending_cycle()
                    self.logger.info(
                        "constant_cycle_pending_done",
                        pending_processed=pending_stats.get("pending_processed", 0),
                        pending_success=pending_stats.get("pending_success", 0),
                        pending_failed=pending_stats.get("pending_failed", 0),
                    )
                else:
                    self.logger.info("constant_cycle_no_pending")
            except Exception as exc:
                self.logger.error(
                    "constant_cycle_pending_error",
                    error=str(exc),
                    exc_info=True,
                )

        # 4. Объединённая статистика
        combined = {**search_stats}
        combined.update({
            f"pending_{k}": v for k, v in pending_stats.items()
        })

        self.logger.info("constant_cycle_complete", **combined)
        return combined

    async def run_force_parse_cycle(self) -> dict:
        """Принудительный запуск парсинга: сначала все товары, затем категории по очереди.

        Алгоритм:
            1. Получить все активные поиски из БД.
            2. Разделить на товарные (``is_category_search=False``)
               и категорийные (``is_category_search=True``).
            3. Запустить парсинг всех товарных поисков одновременно
               через ``run_search_cycle()``.
            4. Ждать ``FORCE_PARSE_PRODUCT_DELAY_SECONDS`` секунд.
            5. Парсить категории по очереди с интервалом
               ``FORCE_PARSE_CATEGORY_INTERVAL_SECONDS`` между ними.

        Returns:
            dict: Статистика ``{"products_parsed": int, "categories_parsed": int}``.
        """
        self.logger.info("force_parse_starting")

        # 1. Получить все активные поиски
        session = get_session()
        repo = Repository(session)
        try:
            all_searches = repo.get_active_searches()
        finally:
            repo.close()

        if not all_searches:
            self.logger.warning("force_parse_no_searches")
            return {"status": "no_searches", "products_parsed": 0, "categories_parsed": 0}

        # 2. Разделить на товары и категории
        product_searches = [s for s in all_searches if not s.is_category_search]
        category_searches = [s for s in all_searches if s.is_category_search]

        self.logger.info(
            "force_parse_searches_found",
            products=len(product_searches),
            categories=len(category_searches),
        )

        total_stats: dict[str, int] = {"products_parsed": 0, "categories_parsed": 0}

        # 3. Парсим все товары сразу
        if product_searches:
            self.logger.info("force_parse_products_start", count=len(product_searches))
            try:
                stats = await self.run_search_cycle(searches=product_searches)
                total_stats["products_parsed"] = stats.get("ads_found", 0) if stats else 0
                self.logger.info(
                    "force_parse_products_done",
                    ads_found=total_stats["products_parsed"],
                )
            except Exception as e:
                self.logger.error("force_parse_products_error", error=str(e))

        # 4. Ждём перед категориями
        if category_searches:
            delay = self.settings.FORCE_PARSE_PRODUCT_DELAY_SECONDS
            self.logger.info("force_parse_delay_before_categories", seconds=delay)
            await asyncio.sleep(delay)

            # 5. Парсим категории по очереди
            interval = self.settings.FORCE_PARSE_CATEGORY_INTERVAL_SECONDS
            for i, category in enumerate(category_searches):
                self.logger.info(
                    "force_parse_category_start",
                    index=i + 1,
                    total=len(category_searches),
                    query=category.search_phrase or category.search_url,
                )
                try:
                    stats = await self.run_search_cycle(searches=[category])
                    ads_count = stats.get("ads_found", 0) if stats else 0
                    total_stats["categories_parsed"] += 1
                    self.logger.info(
                        "force_parse_category_done",
                        query=category.search_phrase or category.search_url,
                        ads_found=ads_count,
                    )
                except Exception as e:
                    self.logger.error(
                        "force_parse_category_error",
                        query=category.search_phrase or category.search_url,
                        error=str(e),
                    )

                # Интервал между категориями (кроме последней)
                if i < len(category_searches) - 1:
                    self.logger.info(
                        "force_parse_category_interval",
                        seconds=interval,
                    )
                    await asyncio.sleep(interval)

        # Дообработка pending объявлений
        try:
            pending_stats = await self.run_force_pending_cycle()
            total_stats["pending_processed"] = pending_stats.get("pending_processed", 0)
            total_stats["pending_success"] = pending_stats.get("pending_success", 0)
            total_stats["pending_failed"] = pending_stats.get("pending_failed", 0)
        except Exception as e:
            self.logger.error("force_parse_pending_error", error=str(e))

        self.logger.info(
            "force_parse_completed",
            products_parsed=total_stats["products_parsed"],
            categories_parsed=total_stats["categories_parsed"],
            pending_processed=total_stats.get("pending_processed", 0),
            pending_success=total_stats.get("pending_success", 0),
            pending_failed=total_stats.get("pending_failed", 0),
        )
        return total_stats

    async def run_force_pending_cycle(self) -> dict[str, int]:
        """Принудительная дообработка всех pending объявлений.

        Алгоритм:
            1. Получить все объявления со статусом ``parse_status='pending'``.
            2. Запустить браузер (``headless=False`` — чтобы пользователь
               мог ввести капчу вручную).
            3. Для каждого pending объявления:
               a. Открыть карточку.
               b. Если обнаружена капча — ждать до 120 секунд
                  (пользователь вводит вручную).
               c. После прохождения капчи — повторить попытку (до 3 раз).
               d. Спарсить данные, обновить запись.
            4. Вернуть статистику.

        Returns:
            dict[str, int]: Статистика с ключами
            ``pending_processed``, ``pending_success``, ``pending_failed``,
            ``captcha_encountered``.
        """
        self.logger.info("force_pending_starting")

        # 1. Получить все pending объявления
        session = get_session()
        repo = Repository(session)
        try:
            pending_ads = repo.get_pending_ads()
        finally:
            repo.close()

        if not pending_ads:
            self.logger.info("force_pending_no_ads")
            return {
                "pending_processed": 0,
                "pending_success": 0,
                "pending_failed": 0,
                "captcha_encountered": 0,
            }

        self.logger.info(
            "force_pending_ads_found",
            count=len(pending_ads),
        )

        stats: dict[str, int] = {
            "pending_processed": 0,
            "pending_success": 0,
            "pending_failed": 0,
            "captcha_encountered": 0,
        }

        # 2. Запустить браузер headless=False
        browser = BrowserManager(headless=False)
        await browser.start()

        collector = AvitoCollector(
            browser_manager=browser,
            settings=self.settings,
        )

        try:
            for idx, ad in enumerate(pending_ads, start=1):
                self.logger.info(
                    "force_pending_processing",
                    index=idx,
                    total=len(pending_ads),
                    ad_id=ad.ad_id,
                    url=ad.url,
                )

                success = False
                captcha_detected = False

                for attempt in range(1, 4):  # до 3 попыток
                    try:
                        # Сбор карточки
                        html, html_path = await collector.collect_ad_page(ad.url)

                        # Проверка капчи
                        if collector._detect_captcha(html):
                            captcha_detected = True
                            stats["captcha_encountered"] += 1
                            self.logger.warning(
                                "CAPTCHA_DETECTED",
                                ad_id=ad.ad_id,
                                attempt=attempt,
                                msg="waiting for manual input (120s)...",
                            )
                            # Ждём CAPTCHA_MANUAL_INPUT_WAIT секунд — пользователь вводит капчу
                            await asyncio.sleep(self.settings.CAPTCHA_MANUAL_INPUT_WAIT)

                            # Повторная загрузка страницы после ввода капчи
                            html, html_path = await collector.collect_ad_page(ad.url)

                            # Снова проверяем
                            if collector._detect_captcha(html):
                                self.logger.warning(
                                    "CAPTCHA_STILL_PRESENT",
                                    ad_id=ad.ad_id,
                                    attempt=attempt,
                                )
                                continue  # следующая попытка

                        # Парсинг данных
                        ad_data: AdData = parse_ad_page(html, ad.url)

                        # Обновление записи в БД
                        session = get_session()
                        repo = Repository(session)
                        try:
                            repo.update_ad(
                                ad.ad_id,
                                title=ad_data.title,
                                price=ad_data.price,
                                location=ad_data.location,
                                seller_name=ad_data.seller_name,
                                seller_type=ad_data.seller_type,
                                condition=ad_data.condition,
                                publication_date=ad_data.publication_date,
                                parse_status="parsed",
                                last_error=None,
                            )
                            repo.commit()
                        except Exception as db_exc:
                            self.logger.error(
                                "force_pending_db_update_failed",
                                ad_id=ad.ad_id,
                                error=str(db_exc),
                            )
                        finally:
                            repo.close()

                        success = True
                        stats["pending_success"] += 1
                        self.logger.info(
                            "force_pending_ad_success",
                            ad_id=ad.ad_id,
                            title=ad_data.title,
                            price=ad_data.price,
                            attempt=attempt,
                        )
                        break  # выходим из цикла попыток

                    except Exception as exc:
                        self.logger.warning(
                            "force_pending_attempt_failed",
                            ad_id=ad.ad_id,
                            attempt=attempt,
                            error=str(exc),
                        )

                if not success:
                    stats["pending_failed"] += 1
                    self.logger.error(
                        "force_pending_ad_failed",
                        ad_id=ad.ad_id,
                        url=ad.url,
                    )
                    # Пометить как failed
                    try:
                        session = get_session()
                        repo = Repository(session)
                        repo.update_ad(
                            ad.ad_id,
                            parse_status="failed",
                            last_error="captcha_not_passed",
                        )
                        repo.commit()
                    except Exception as db_exc:
                        self.logger.error(
                            "force_pending_status_update_failed",
                            ad_id=ad.ad_id,
                            error=str(db_exc),
                        )
                    finally:
                        repo.close()

                stats["pending_processed"] += 1

                # Задержка между объявлениями: 3-8 секунд
                delay = random.uniform(3, 8)
                await asyncio.sleep(delay)

                # Пауза между группами по 5 объявлений: 15-25 секунд
                if idx % 5 == 0 and idx < len(pending_ads):
                    group_delay = random.uniform(15, 25)
                    self.logger.info(
                        "force_pending_group_pause",
                        processed=idx,
                        total=len(pending_ads),
                        pause_sec=round(group_delay, 1),
                    )
                    await asyncio.sleep(group_delay)

        finally:
            await collector.close()

        self.logger.info(
            "force_pending_completed",
            **stats,
        )
        return stats

    # ------------------------------------------------------------------
    # Внутренние методы
    # ------------------------------------------------------------------

    def _detect_warmup(self, searches: list[TrackedSearch]) -> bool:
        """Определить, является ли запуск «разогревочным» (первый запуск).

        Warm-up detected если ВСЕ запланированные поиски имеют
        ``last_run_at = None`` — то есть ни разу не выполнялись.

        Args:
            searches: Список отслеживаемых поисков.

        Returns:
            bool: ``True`` если нужен warm-up режим.
        """
        if not self.settings.WARMUP_ENABLED:
            return False
        if not searches:
            return False
        return all(s.last_run_at is None for s in searches)

    async def _process_tracked_search(
        self,
        search: TrackedSearch,
        collector: AvitoCollector,
        repo: Repository,
        analyzer: PriceAnalyzer,
        semaphore: asyncio.Semaphore,
        *,
        is_warmup: bool = False,
        ad_concurrency: int | None = None,
        search_delay: float | None = None,
    ) -> None:
        """Обработать один отслеживаемый поиск с семафором.

        Каждая параллельная задача создаёт свою собственную
        session / Repository, чтобы rollback при ошибке не
        закрывал транзакцию для остальных задач.

        Args:
            search: Отслеживаемый поиск из БД.
            collector: Экземпляр сборщика.
            repo: Экземпляр репозитория (не используется напрямую,
                сохранён для совместимости сигнатуры).
            analyzer: Экземпляр анализатора цен.
            semaphore: Семафор для ограничения параллельности.
            is_warmup: Флаг warm-up режима (последовательная обработка).
            ad_concurrency: Переопределение параллельности карточек.
            search_delay: Переопределение задержки между поисками.
        """
        async with semaphore:
            search_start = time.monotonic()
            context = None

            # Изолированная session/repo для каждого параллельного поиска
            search_session = get_session()
            search_repo = Repository(search_session)
            try:
                # Задержка между поисками в батче
                delay = (
                    search_delay
                    if search_delay is not None
                    else self.settings.SEARCH_DELAY_SECONDS
                )
                await asyncio.sleep(delay)

                max_ads = (
                    search.max_ads_to_parse
                    or self.settings.DEFAULT_MAX_ADS_TO_PARSE
                )

                # Изоляция контекста: отдельный BrowserContext на каждый поиск
                if self.settings.USE_ISOLATED_CONTEXTS:
                    context = await collector.browser.create_context()
                    self.logger.debug(
                        "isolated_context_created_for_search",
                        search_id=search.id,
                    )

                await self._process_search(
                    search.search_url,
                    collector,
                    search_repo,
                    max_ads=max_ads,
                    is_warmup=is_warmup,
                    ad_concurrency=ad_concurrency,
                    context=context,
                )

                # Обновляем last_run_at
                search_repo.update_search_last_run(search.id)

                search_session.commit()

                self.stats["searches_processed"] += 1
                self.logger.info(
                    "search_processed",
                    search_url=search.search_url,
                    warmup=is_warmup,
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
                try:
                    search_session.rollback()
                except Exception:
                    pass
            finally:
                if context is not None:
                    await collector.browser.close_context(context)
                search_session.close()

    async def _process_search(
        self,
        search_url: str,
        collector: AvitoCollector,
        repo: Repository,
        max_ads: int | None = None,
        *,
        is_warmup: bool = False,
        ad_concurrency: int | None = None,
        context: "BrowserContext | None" = None,
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
            is_warmup: Флаг warm-up режима (последовательная обработка карточек).
            ad_concurrency: Переопределение параллельности карточек.
            context: Опциональный изолированный контекст браузера.

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

            # Семафор для параллельного сбора карточек
            effective_ad_concurrency = (
                ad_concurrency
                if ad_concurrency is not None
                else self.settings.MAX_CONCURRENT_AD_PAGES
            )
            ad_semaphore = asyncio.Semaphore(effective_ad_concurrency)

            async def _process_ad_safe(
                item: SearchResultItem,
            ) -> str | None:
                """Обёртка для параллельной обработки с семафором.

                Каждая параллельная задача создаёт свою собственную
                session / Repository, чтобы rollback при ошибке не
                закрывал транзакцию для остальных задач.
                """
                async with ad_semaphore:
                    ad_session = get_session()
                    ad_repo = Repository(ad_session)
                    try:
                        result = await self._process_ad(
                            item, search_url, collector, ad_repo,
                            context=context,
                        )
                        ad_session.commit()
                        return result
                    except Exception as exc:
                        run_errors_counter[0] += 1
                        self.stats["errors"] += 1
                        try:
                            ad_session.rollback()
                        except Exception:
                            pass
                        self.logger.error(
                            "ad_processing_failed",
                            url=item.url,
                            error=str(exc),
                        )
                        return None
                    finally:
                        ad_session.close()

            # --- Цикл по страницам пагинации ---
            all_new_items: list[SearchResultItem] = []

            for page_num in range(1, max_pages + 1):
                page_url = build_page_url(search_url, page_num)

                # Задержка уже присутствует внутри collector.collect_search_page()

                self.logger.info(
                    "collecting_search_page",
                    search_url=search_url,
                    page=page_num,
                    max_pages=max_pages,
                    page_url=page_url,
                )

                try:
                    html, _html_path = await collector.collect_search_page(
                        page_url, context=context,
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
                    # Пропускаем страницу при ошибке загрузки, продолжаем пагинацию
                    continue

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

                # Фильтрация уже известных (batch-обработка)
                new_on_page: list[SearchResultItem] = []
                pending_items: list[tuple[SearchResultItem, str]] = []
                batch_items: list[dict] = []
                for item in search_items:
                    ad_id = extract_ad_id_from_url(item.url)
                    if ad_id not in recent_ids:
                        pending_items.append((item, ad_id))
                        batch_items.append({
                            "ad_id": ad_id,
                            "url": normalize_url(item.url),
                            "search_url": search_url,
                            "title": item.title,
                            "price": item.price,
                        })

                if batch_items:
                    batch_results = repo.batch_get_or_create_ads(batch_items)
                    for (item, ad_id), (_, created) in zip(
                        pending_items, batch_results,
                    ):
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

            # ✅ ВАЖНО: фикс видимости между session
            # Коммитим созданные объявления ДО параллельной обработки
            if new_items:
                repo.session.commit()
                self.logger.debug(
                    "ads_committed_before_processing",
                    search_url=search_url,
                    count=len(new_items),
                )

            self.logger.info(
                "pagination_summary",
                search_url=search_url,
                pages_fetched=pages_fetched,
                total_found=run_ads_found,
                new_ads=run_ads_new,
            )

            # Обработка карточек: warm-up → последовательно, иначе → параллельно
            if new_items:
                if is_warmup:
                    # Последовательная обработка карточек с задержками (warm-up)
                    self.logger.info(
                        "warmup_sequential_ad_processing",
                        search_url=search_url,
                        ad_count=len(new_items),
                    )
                    for item in new_items:
                        try:
                            result = await self._process_ad(
                                item, search_url, collector, repo,
                                context=context,
                            )
                            if result is not None:
                                new_ad_ids.append(result)
                                run_ads_opened += 1
                        except Exception as exc:
                            run_errors += 1
                            self.stats["errors"] += 1
                            self.logger.error(
                                "warmup_ad_processing_failed",
                                url=item.url,
                                error=str(exc),
                            )

                        # Задержка между карточками при warm-up
                        delay = random.uniform(
                            self.settings.WARMUP_AD_DELAY_MIN,
                            self.settings.WARMUP_AD_DELAY_MAX,
                        )
                        self.logger.debug(
                            "warmup_ad_delay",
                            delay_sec=round(delay, 2),
                        )
                        await asyncio.sleep(delay)
                else:
                    # Параллельная обработка (обычный режим)
                    ad_results = await asyncio.gather(
                        *[_process_ad_safe(item) for item in new_items],
                        return_exceptions=True,
                    )

                    for result in ad_results:
                        if isinstance(result, Exception):
                            # Недостижимо: _process_ad_safe ловит все
                            # исключения и возвращает None, но оставляем
                            # защиту на случай изменения поведения.
                            run_errors += 1
                            self.stats["errors"] += 1
                        elif result is not None:
                            new_ad_ids.append(result)
                            run_ads_opened += 1

            run_errors += run_errors_counter[0]

            # --- Product-нормализация на уровне search (ВСЕ найденные) ---
            # Это ключевой фикс: раньше Product создавался только при
            # открытии карточки (parse_ad_page), что давало ~3-5% coverage.
            # Теперь нормализуем и записываем snapshot для КАЖДОГО
            # объявления из поисковой выдачи с title и price.
            product_snapshot_count = 0
            product_ids_to_update: set[int] = set()
            for item in all_search_items:
                if not item.title or not item.price or item.price <= 0:
                    continue
                try:
                    norm = normalize_title(item.title)
                    product = repo.get_or_create_product(
                        normalized_key=norm.normalized_key,
                        brand=norm.brand,
                        model=norm.model,
                        category=getattr(tracked_search, "category", None),
                    )
                    # Привязываем snapshot к объявлению через ad_id
                    _ad_record = repo.get_ad_by_ad_id(item.ad_id)
                    _ad_internal_id = _ad_record.id if _ad_record else None
                    repo.add_product_price_snapshot(
                        product_id=product.id,
                        price=item.price,
                        ad_id=_ad_internal_id,
                    )
                    product_ids_to_update.add(product.id)
                    product_snapshot_count += 1
                except Exception as product_exc:
                    self.logger.debug(
                        "search_level_product_normalization_failed",
                        title=item.title[:60],
                        error=str(product_exc),
                    )

            # Батчевое обновление статистики продуктов (не в hot-path)
            for pid in product_ids_to_update:
                try:
                    repo.update_product_stats(pid)
                except Exception as stats_exc:
                    self.logger.debug(
                        "product_stats_update_failed",
                        product_id=pid,
                        error=str(stats_exc),
                    )

            if product_snapshot_count > 0:
                self.logger.info(
                    "search_level_product_snapshots_created",
                    search_url=search_url,
                    snapshots=product_snapshot_count,
                    products_updated=len(product_ids_to_update),
                )

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
        *,
        context: "BrowserContext | None" = None,
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
            context: Опциональный изолированный контекст браузера.

        Returns:
            str | None: ``ad_id`` при успехе или ``None`` при ошибке.
        """
        ad_id = extract_ad_id_from_url(search_item.url)
        normalized_url = normalize_url(search_item.url)

        # Восстановление сессии если она в failed состоянии
        if not repo.session.is_active:
            try:
                repo.session.rollback()
            except Exception:
                pass

        savepoint = repo.session.begin_nested()
        try:
            # Задержка уже присутствует внутри collector.collect_ad_page()

            # Сбор карточки
            html, html_path = await collector.collect_ad_page(
                normalized_url, context=context,
            )
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
                seller_type=ad_data.seller_type,
                condition=ad_data.condition,
                publication_date=ad_data.publication_date,
                parse_status="parsed",
            )

            # Создание снимка
            ad_record, _ = repo.get_or_create_ad(
                ad_id, normalized_url, search_url,
            )

            # --- Привязка объявления к продавцу ---
            if ad_data.seller_id is not None:
                if ad_data.seller_id.lower() in _INVALID_SELLER_IDS:
                    self.logger.debug(
                        "seller_link_skipped_invalid_id",
                        ad_id=ad_id,
                        seller_id=ad_data.seller_id,
                    )
                else:
                    seller_sp = repo.session.begin_nested()
                    try:
                        seller = repo.get_or_create_seller(
                            seller_id=ad_data.seller_id,
                            seller_url=ad_data.seller_url,
                            seller_name=ad_data.seller_name,
                        )
                        repo.link_ad_to_seller(
                            ad_id=ad_record.id,
                            seller_id_fk=seller.id,
                        )
                        seller_sp.commit()
                    except Exception as seller_exc:
                        seller_sp.rollback()
                        self.logger.warning(
                            "seller_link_failed",
                            ad_id=ad_id,
                            seller_id=ad_data.seller_id,
                            error=str(seller_exc),
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

            # --- Нормализация товара и запись в Product ---
            if ad_data.title and ad_data.price and ad_data.price > 0:
                product_sp = repo.session.begin_nested()
                try:
                    norm = normalize_title(ad_data.title)
                    product = repo.get_or_create_product(
                        normalized_key=norm.normalized_key,
                        brand=norm.brand,
                        model=norm.model,
                        category=getattr(ad_record, "ad_category", None),
                    )
                    repo.add_product_price_snapshot(
                        product_id=product.id,
                        price=ad_data.price,
                        ad_id=ad_record.id,
                    )
                    product_sp.commit()
                except Exception as product_exc:
                    product_sp.rollback()
                    self.logger.warning(
                        "product_normalization_failed",
                        ad_id=ad_id,
                        title=ad_data.title,
                        error=str(product_exc),
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
            # Полный rollback для восстановления сессии (иначе будет "transaction is closed")
            try:
                repo.session.rollback()
            except Exception:
                pass
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

    async def _collect_seller_profiles(
        self,
        collector: AvitoCollector,
        repo: Repository,
    ) -> int:
        """Собирает данные о профилях продавцов (проданные товары).

        Вызывается после обработки всех объявлений в цикле.
        Получает продавцов, которых нужно парсить, и собирает их профили.
        Использует переданный репозиторий для обеспечения атомарности
        транзакции вместе с основным циклом.

        Args:
            collector: Экземпляр сборщика.
            repo: Репозиторий текущей транзакции.

        Returns:
            Количество обработанных профилей продавцов.
        """
        settings = self.settings

        if not settings.SELLER_PROFILE_ENABLED:
            return 0

        sellers = repo.get_sellers_due_for_scrape(
            limit=settings.SELLER_MAX_PROFILES_PER_CYCLE,
            interval_hours=settings.SELLER_SCRAPE_INTERVAL_HOURS,
        )

        if not sellers:
            self.logger.info("no_sellers_due_for_scrape")
            return 0

        self.logger.info(
            "sellers_due_for_scrape",
            count=len(sellers),
        )

        processed = 0

        for seller in sellers:
            self.logger.info(
                "collecting_seller_profile",
                seller_id=seller.seller_id,
            )

            # Пропуск продавцов без URL профиля
            if not seller.seller_url:
                self.logger.warning(
                    "seller_url_missing",
                    seller_id=seller.seller_id,
                )
                continue

            try:
                max_pages = settings.SELLER_MAX_PAGES_PER_PROFILE
                all_sold_items: list[dict] = []
                profile_updated = False

                for page_num in range(1, max_pages + 1):
                    if page_num > 1:
                        await asyncio.sleep(
                            random.uniform(
                                settings.SELLER_PAGE_DELAY_MIN,
                                settings.SELLER_PAGE_DELAY_MAX,
                            )
                        )

                    html, final_url = await collector.collect_seller_page(
                        seller.seller_url, tab="sold", page_num=page_num,
                    )

                    if html is not None:
                        profile: SellerProfileData = parse_seller_profile(
                            html, url=final_url or seller.seller_url,
                        )

                        # Обновляем данные продавца только один раз (по первой странице)
                        if not profile_updated:
                            repo.update_seller(
                                seller.seller_id,
                                seller_name=profile.seller_name,
                                rating=profile.rating,
                                reviews_count=profile.reviews_count,
                                total_sold_items=profile.total_sold_items,
                                last_scraped_at=datetime.now(timezone.utc),
                                scrape_status="scraped",
                            )
                            profile_updated = True

                        if profile.sold_items:
                            page_items = [
                                {
                                    "item_id": item.item_id,
                                    "title": item.title,
                                    "price": item.price,
                                    "price_str": item.price_str,
                                    "category": item.category,
                                    "sold_date": item.sold_date,
                                    "item_url": item.item_url,
                                }
                                for item in profile.sold_items
                            ]
                            all_sold_items.extend(page_items)
                            self.logger.info(
                                "seller_profile_page_scraped",
                                seller_id=seller.seller_id,
                                page=page_num,
                                page_sold_items=len(profile.sold_items),
                            )
                        else:
                            self.logger.info(
                                "seller_profile_page_no_sold_items",
                                seller_id=seller.seller_id,
                                page=page_num,
                            )
                            # Если на странице нет проданных товаров — дальше нет смысла
                            break
                    else:
                        self.logger.warning(
                            "seller_page_html_empty",
                            seller_id=seller.seller_id,
                            page=page_num,
                        )
                        repo.update_seller(
                            seller.seller_id,
                            scrape_status="failed",
                        )
                        break

                # Сохраняем все собранные проданные товары
                if all_sold_items:
                    saved = repo.save_sold_items(
                        seller_db_id=seller.id,
                        items=all_sold_items,
                    )
                    self.logger.info(
                        "seller_profile_scraped",
                        seller_id=seller.seller_id,
                        total_pages_parsed=page_num,
                        total_sold_items=len(all_sold_items),
                        sold_items_saved=saved,
                    )
                elif profile_updated:
                    self.logger.info(
                        "seller_profile_scraped_no_sold_items",
                        seller_id=seller.seller_id,
                        total_pages_parsed=page_num,
                    )

            except Exception as exc:
                self.logger.error(
                    "seller_profile_failed",
                    seller_id=seller.seller_id,
                    error=str(exc),
                    exc_info=True,
                )
                try:
                    repo.update_seller(
                        seller.seller_id,
                        scrape_status="failed",
                    )
                except Exception as update_exc:
                    self.logger.error(
                        "seller_status_update_failed",
                        seller_id=seller.seller_id,
                        error=str(update_exc),
                    )

            processed += 1

            # Задержка между профилями продавцов
            await asyncio.sleep(
                random.uniform(
                    settings.SELLER_PAGE_DELAY_MIN,
                    settings.SELLER_PAGE_DELAY_MAX,
                )
            )

        return processed

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

        self.logger.info(
            "DEBUG_analyze_and_notify_searches_started",
            searches_count=len(searches),
            search_urls=[s.search_url for s in searches],
            is_category_flags=[getattr(s, 'is_category_search', False) for s in searches],
        )

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
                            # Устанавливаем флаг is_undervalued на модели Ad
                            try:
                                repo.update_ad(
                                    diamond.ad.ad_id,
                                    is_undervalued=True,
                                    undervalue_score=-diamond.discount_percent / 100,
                                )
                            except Exception as flag_exc:
                                self.logger.warning(
                                    "diamond_undervalued_flag_failed",
                                    ad_id=diamond.ad.ad_id,
                                    error=str(flag_exc),
                                )
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
                self.logger.info(
                    "DEBUG_ads_for_analysis_result",
                    search_url=search.search_url,
                    ads_count=len(ads),
                    temporal_window_days=self.settings.TEMPORAL_WINDOW_DAYS,
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

        # 2. Фильтрация аксессуаров (двухпроходная схема)
        accessory_filter = AccessoryFilter(
            blacklist=self.settings.ACCESSORY_BLACKLIST,
            min_price=self.settings.MIN_PRICE_FILTER,
            price_ratio=self.settings.ACCESSORY_PRICE_RATIO_THRESHOLD,
            enabled=self.settings.ENABLE_ACCESSORY_FILTER,
        )

        # Проход 1: фильтрация по blacklist, min_price и bundle (без медианы)
        pass1_ads = []
        for ad in ads:
            if ad.price is None or ad.price <= 0:
                continue
            filter_result = accessory_filter.is_accessory(ad, median_price=None)
            if filter_result.is_filtered:
                continue
            pass1_ads.append(ad)

        if not pass1_ads:
            self.logger.info(
                "category_search_all_filtered_pass1",
                search_id=search.id,
            )
            return []

        # Рассчитываем предварительную медиану по результатам прохода 1
        _prelim_prices = [ad.price for ad in pass1_ads if ad.price]
        _prelim_median = float(np.median(_prelim_prices)) if _prelim_prices else None

        # Проход 2: фильтрация по price_ratio с предварительной медианой
        if _prelim_median is not None and _prelim_median > 0:
            filtered_ads = []
            for ad in pass1_ads:
                filter_result = accessory_filter.is_accessory(ad, median_price=_prelim_median)
                if filter_result.is_filtered:
                    self.logger.debug(
                        "category_search_pass2_filtered",
                        ad_id=ad.ad_id,
                        title=ad.title,
                        price=ad.price,
                        prelim_median=_prelim_median,
                        reason=filter_result.reason,
                    )
                    continue
                filtered_ads.append(ad)

            self.logger.info(
                "category_search_two_pass_filter",
                search_id=search.id,
                total_ads=len(ads),
                pass1_ads=len(pass1_ads),
                pass2_ads=len(filtered_ads),
                prelim_median=_prelim_median,
            )
        else:
            filtered_ads = pass1_ads

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

        # 7. Детекция «бриллиантов» — product-first подход
        diamonds: list[DiamondAlert] = []
        discount_threshold = self.settings.DIAMOND_DISCOUNT_THRESHOLD
        min_snapshots = self.settings.DIAMOND_MIN_SNAPSHOTS
        fast_sale_threshold = self.settings.DIAMOND_FAST_SALE_THRESHOLD

        product_hits = 0
        segment_hits = 0
        no_data_count = 0

        for ad in filtered_ads:
            if ad.price is None or ad.price <= 0:
                continue

            effective_median: float | None = None
            effective_reason: str = "none"
            product_key: str | None = None
            sample_size = 0

            # --- ПРИОРИТЕТ 1: Product-level медиана ---
            if ad.title:
                try:
                    from app.analysis.product_normalizer import normalize_title
                    norm = normalize_title(ad.title)
                    product_key = norm.normalized_key
                    product = repo.get_product_by_key(product_key)
                    if product is not None:
                        stats = repo.get_product_price_stats(product.id)
                        p_count = stats.get("count", 0)
                        p_median = stats.get("median")
                        if p_count >= min_snapshots and p_median is not None and p_median > 0:
                            effective_median = float(p_median)
                            effective_reason = (
                                f"product_median ({p_median:,.0f}₽) "
                                f"по {p_count} записям, key={product_key}"
                            )
                            sample_size = p_count
                            product_hits += 1
                except Exception as exc:
                    self.logger.debug(
                        "diamond_product_lookup_failed",
                        ad_id=ad.ad_id,
                        error=str(exc),
                    )

            # --- ПРИОРИТЕТ 2: Segment-level fallback ---
            if effective_median is None:
                seg_key_str = segment_analyzer.build_segment_key(ad).to_string()
                seg_stats = segment_results.get(seg_key_str)
                if seg_stats is not None:
                    best_median, reason = segment_analyzer.get_best_median(seg_stats)
                    if best_median > 0:
                        effective_median = best_median
                        effective_reason = reason
                        sample_size = seg_stats.get("sample_size", 0)
                        segment_hits += 1

            # --- Нет данных — пропускаем ---
            if effective_median is None or effective_median <= 0:
                no_data_count += 1
                continue

            # --- Проверка порога ---
            ratio = ad.price / effective_median

            self.logger.debug(
                "diamond_candidate_ratio",
                ad_id=ad.ad_id,
                price=ad.price,
                median=effective_median,
                ratio=round(ratio, 3),
                threshold=discount_threshold,
                source=effective_reason,
                product_key=product_key,
            )

            if ratio >= discount_threshold:
                continue  # Не бриллиант

            # Фильтр по минимальной цене
            if ad.price < self.settings.DIAMOND_MIN_PRICE:
                self.logger.debug(
                    "diamond_skipped_min_price",
                    ad_id=ad.ad_id,
                    price=ad.price,
                    min_price=self.settings.DIAMOND_MIN_PRICE,
                )
                continue

            # --- Создание DiamondAlert ---
            discount_percent = (1 - ratio) * 100
            segment_key = CategorySegmentKey(
                category=getattr(ad, "ad_category", "unknown") or "unknown",
                brand=getattr(ad, "brand", "unknown") or "unknown",
                model=getattr(ad, "extracted_model", "unknown") or "unknown",
                condition=getattr(ad, "condition", "unknown") or "unknown",
                location=getattr(ad, "location", "unknown") or "unknown",
            )

            reason_msg = (
                f"цена {ad.price:,.0f}₽ < {effective_reason} "
                f"× {discount_threshold} (ratio={ratio:.2f}, "
                f"скидка={discount_percent:.1f}%)"
            )

            self.logger.info(
                "diamond_detected",
                ad_id=ad.ad_id,
                price=ad.price,
                effective_median=effective_median,
                ratio=round(ratio, 3),
                discount_percent=round(discount_percent, 1),
                product_key=product_key,
                source=effective_reason,
            )

            diamonds.append(DiamondAlert(
                ad=ad,
                segment_key=segment_key,
                segment_stats=None,
                price=ad.price,
                median_price=effective_median,
                discount_percent=discount_percent,
                sample_size=sample_size,
                reason=reason_msg,
                is_rare_segment=False,
            ))

        self.logger.info(
            "category_search_diamonds_detected",
            search_id=search.id,
            diamonds_count=len(diamonds),
            product_hits=product_hits,
            segment_hits=segment_hits,
            no_data_count=no_data_count,
            total_candidates=len(filtered_ads),
        )

        return diamonds

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

        Отправляет уведомления через два параллельных канала:
        Telegram и Email — независимо друг от друга.

        Args:
            all_undervalued: Список недооценённых объявлений.
            repo: Экземпляр репозитория.
        """
        self.logger.info(
            "DEBUG_send_notifications_called",
            undervalued_count=len(all_undervalued),
        )
        if not all_undervalued:
            self.logger.info("no_undervalued_ads_to_notify")
            return

        # Канал 1: Telegram
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

        except Exception as exc:
            self.logger.error(
                "telegram_notifications_failed",
                error=str(exc),
            )

        # Канал 2: Email — отправляем всегда (параллельный канал)
        try:
            email_notifier = EmailNotifier()
            results = await email_notifier.send_undervalued_notifications(
                all_undervalued, repo,
            )
            email_count = sum(1 for r in results if r.success)

            self.logger.info(
                "notifications_sent",
                channel="email",
                total_undervalued=len(all_undervalued),
                sent=email_count,
                failed=len(results) - email_count,
            )

        except Exception as exc:
            self.stats["errors"] += 1
            self.logger.error(
                "email_notifications_failed",
                error=str(exc),
            )
