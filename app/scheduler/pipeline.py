"""Оркестратор одного цикла сбора и анализа данных Avito."""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone

import structlog

from app.config import get_settings
from app.config.settings import Settings
from app.collector import BrowserManager, AvitoCollector
from app.parser import parse_search_page, parse_ad_page, SearchResultItem, AdData
from app.storage import Repository, get_session
from app.storage.models import TrackedSearch
from app.analysis import PriceAnalyzer, UndervaluedAd, AdAnalysisResult
from app.analysis.accessory_filter import AccessoryFilter
from app.notifier import EmailNotifier, TelegramNotifier
from app.utils import random_delay, setup_logging, extract_ad_id_from_url, normalize_url


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

    async def run_search_cycle(self) -> dict[str, int]:
        """Основной цикл обработки поисков из БД (масштабированный режим).

        Алгоритм:
            1. Получить поиски, которые пора запускать:
               ``repo.get_searches_due_for_run()``.
            2. Обрабатывать батчами по ``MAX_CONCURRENT_SEARCHES``
               с использованием ``asyncio.Semaphore``.
            3. Для каждого поиска: собрать страницу → парсить → взять
               первые N карточек → парсить карточки → сохранить →
               проанализировать.
            4. После обработки каждого поиска обновлять ``last_run_at``.
            5. Задержки между поисками и между батчами.

        Returns:
            dict[str, int]: Статистика выполненного цикла.
        """
        setup_logging(self.settings.LOG_LEVEL)
        self.logger.info("search_cycle_starting")

        # Сброс статистики
        self.stats = {k: 0 for k in self.stats}

        # Автосоздание таблиц при необходимости
        from app.storage.database import ensure_tables
        ensure_tables()

        session = get_session()
        repo = Repository(session)

        try:
            # Получаем поиски, которые пора запускать
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
                        "processing_batch",
                        batch_idx=batch_idx + 1,
                        total_batches=len(batches),
                        batch_size=len(batch),
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

        self.logger.info("search_cycle_completed", **self.stats)
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
                    "tracked_search_processed",
                    search_id=search.id,
                    search_url=search.search_url,
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
        """Обработать один поисковый URL.

        Выполняет:
            1. Регистрацию поиска и запуск в БД.
            2. Сбор и парсинг поисковой страницы.
            3. Фильтрацию уже известных объявлений.
            4. Обработку новых карточек (до лимита).
            5. Завершение записи запуска в БД.

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
        new_ad_ids: list[str] = []

        try:
            # Задержка перед сбором
            await random_delay(
                self.settings.MIN_DELAY_SECONDS,
                self.settings.MAX_DELAY_SECONDS,
            )

            # Сбор поисковой страницы
            html, _html_path = await collector.collect_search_page(search_url)

            # Парсинг
            search_items: list[SearchResultItem] = parse_search_page(
                html, search_url,
            )
            run_ads_found = len(search_items)
            self.stats["ads_found"] += run_ads_found

            self.logger.info(
                "search_page_parsed",
                search_url=search_url,
                items_found=len(search_items),
            )

            # Ранняя фильтрация аксессуаров (до сбора карточек)
            search_items, early_filtered = self._early_filter_search_items(search_items)
            if early_filtered > 0:
                self.stats["ads_filtered"] += early_filtered
                self.logger.info(
                    "search_items_early_filtered",
                    search_url=search_url,
                    filtered_count=early_filtered,
                    remaining=len(search_items),
                )

            # Фильтрация уже известных
            recent_ids = repo.get_recent_ad_ids(search_url)
            known_items: list[SearchResultItem] = []
            for item in search_items:
                ad_id = extract_ad_id_from_url(item.url)
                if ad_id not in recent_ids:
                    _, created = repo.get_or_create_ad(
                        ad_id, normalize_url(item.url), search_url,
                    )
                    if created:
                        known_items.append(item)

            # Лимит новых объявлений
            new_items = known_items[:max_ads]
            run_ads_new = len(new_items)
            self.stats["ads_new"] += run_ads_new

            self.logger.info(
                "new_ads_to_process",
                search_url=search_url,
                total_found=run_ads_found,
                new_ads=run_ads_new,
            )

            # Обработка каждой карточки
            for item in new_items:
                try:
                    ad_id = await self._process_ad(
                        item, search_url, collector, repo,
                    )
                    if ad_id is not None:
                        new_ad_ids.append(ad_id)
                        run_ads_opened += 1
                except Exception as exc:
                    run_errors += 1
                    self.stats["errors"] += 1
                    self.logger.error(
                        "ad_processing_failed",
                        url=item.url,
                        error=str(exc),
                    )

            # Завершение запуска
            repo.complete_search_run(
                search_run.id,
                ads_found=run_ads_found,
                ads_new=run_ads_new,
                pages_fetched=1,
                ads_opened=run_ads_opened,
                errors_count=run_errors,
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
            repo.create_snapshot(
                ad_id=ad_record.id,
                price=ad_data.price,
                html_path=html_path,
            )

            self.logger.info(
                "ad_processed",
                ad_id=ad_id,
                title=ad_data.title,
                price=ad_data.price,
            )
            return ad_id

        except Exception as exc:
            self.stats["errors"] += 1
            self.logger.error(
                "ad_card_failed",
                ad_id=ad_id,
                url=normalized_url,
                error=str(exc),
            )
            try:
                repo.rollback()
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
        all_undervalued: list[UndervaluedAd] = []

        accessory_filter = AccessoryFilter(
            blacklist=self.settings.ACCESSORY_BLACKLIST,
            min_price=self.settings.MIN_PRICE_FILTER,
            price_ratio=self.settings.ACCESSORY_PRICE_RATIO_THRESHOLD,
            enabled=self.settings.ENABLE_ACCESSORY_FILTER,
        )

        for search in searches:
            try:
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

        # Фиксируем аналитические данные в БД до отправки уведомлений,
        # чтобы записи NotificationSent (из mark_notification_sent) были
        # закоммичены до фактической отправки через Telegram/Email.
        repo.commit()
        self.logger.info("analysis_committed_before_notifications")

        # Отправка уведомлений
        await self._send_notifications(all_undervalued, repo)

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
