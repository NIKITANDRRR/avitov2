"""Оркестратор одного цикла сбора и анализа данных Avito."""

from __future__ import annotations

from datetime import datetime, timezone

import structlog

from app.config import get_settings
from app.config.settings import Settings
from app.collector import BrowserManager, AvitoCollector
from app.parser import parse_search_page, parse_ad_page, SearchResultItem, AdData
from app.storage import Repository, get_session
from app.analysis import PriceAnalyzer, UndervaluedAd
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

    def __init__(self) -> None:
        self.settings: Settings = get_settings()
        self.logger = structlog.get_logger("pipeline")
        self.stats: dict[str, int] = {
            "searches_processed": 0,
            "ads_found": 0,
            "ads_new": 0,
            "ads_scraped": 0,
            "ads_parsed": 0,
            "ads_undervalued": 0,
            "notifications_sent": 0,
            "errors": 0,
        }

    async def run(self) -> dict[str, int]:
        """Запустить один полный цикл сбора и анализа.

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

    # ------------------------------------------------------------------
    # Внутренние методы
    # ------------------------------------------------------------------

    async def _process_search(
        self,
        search_url: str,
        collector: AvitoCollector,
        repo: Repository,
    ) -> list[str]:
        """Обработать один поисковый URL.

        Выполняет:
            1. Регистрацию поиска и запуск в БД.
            2. Сбор и парсинг поисковой страницы.
            3. Фильтрацию уже известных объявлений.
            4. Обработку новых карточек (до ``MAX_ADS_PER_SEARCH_PER_RUN``).
            5. Завершение записи запуска в БД.

        Args:
            search_url: URL поисковой выдачи Avito.
            collector: Экземпляр сборщика.
            repo: Экземпляр репозитория.

        Returns:
            list[str]: Список ``ad_id`` новых обработанных объявлений.
        """
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
            new_items = known_items[: self.settings.MAX_ADS_PER_SEARCH_PER_RUN]
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
        """Анализ цен и отправка уведомлений.

        Для каждого поискового URL:
            1. Получить все объявления.
            2. Запустить ценовой анализ.
            3. Собрать недооценённые объявления.

        Затем отправить уведомления через Telegram.

        Args:
            repo: Экземпляр репозитория.
        """
        all_undervalued: list[UndervaluedAd] = []

        price_analyzer = PriceAnalyzer()

        for search_url in self.settings.SEARCH_URLS:
            try:
                ads = repo.get_ads_for_search(search_url)
                if not ads:
                    self.logger.info(
                        "no_ads_for_analysis",
                        search_url=search_url,
                    )
                    continue

                undervalued = price_analyzer.analyze_and_mark(ads, repo)
                all_undervalued.extend(undervalued)

                self.logger.info(
                    "analysis_completed",
                    search_url=search_url,
                    total_ads=len(ads),
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
        if all_undervalued:
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
        else:
            self.logger.info("no_undervalued_ads_to_notify")
