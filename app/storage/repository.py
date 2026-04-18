"""Repository pattern — доступ к данным Avito Monitor через SQLAlchemy."""

from __future__ import annotations

import datetime

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

import structlog

from app.utils.exceptions import StorageError
from app.storage.models import (
    Ad,
    AdSnapshot,
    NotificationSent,
    Product,
    ProductPriceSnapshot,
    SearchRun,
    Seller,
    SoldItem,
    TrackedSearch,
    SegmentStats,
    SegmentPriceHistory,
)

logger = structlog.get_logger(__name__)


class Repository:
    """Репозиторий для работы с данными Avito Monitor.

    Инкапсулирует все операции с базой данных, использует structlog
    для логирования и оборачивает ошибки БД в StorageError.

    Args:
        session: Экземпляр SQLAlchemy Session.
    """

    def __init__(self, session: Session) -> None:
        self.session = session

    # ------------------------------------------------------------------
    # TrackedSearch
    # ------------------------------------------------------------------

    def get_or_create_tracked_search(self, search_url: str) -> TrackedSearch:
        """Возвращает существующий или создаёт новый TrackedSearch.

        Args:
            search_url: URL поисковой выдачи Avito.

        Returns:
            TrackedSearch: Найденный или созданный объект.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = select(TrackedSearch).where(TrackedSearch.search_url == search_url)
            result = self.session.execute(stmt).scalar_one_or_none()
            if result is not None:
                logger.debug(
                    "tracked_search_found",
                    search_url=search_url,
                    tracked_search_id=result.id,
                )
                return result

            tracked = TrackedSearch(search_url=search_url)
            self.session.add(tracked)
            self.session.flush()
            logger.info(
                "tracked_search_created",
                search_url=search_url,
                tracked_search_id=tracked.id,
            )
            return tracked
        except SQLAlchemyError as exc:
            logger.error(
                "get_or_create_tracked_search_failed",
                search_url=search_url,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get or create tracked search: {exc}"
            ) from exc

    def get_active_searches(self) -> list[TrackedSearch]:
        """Возвращает список активных поисковых запросов.

        Returns:
            list[TrackedSearch]: Активные поисковые запросы.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = select(TrackedSearch).where(TrackedSearch.is_active.is_(True))
            results = self.session.execute(stmt).scalars().all()
            logger.debug("active_searches_fetched", count=len(results))
            return list(results)
        except SQLAlchemyError as exc:
            logger.error("get_active_searches_failed", error=str(exc))
            raise StorageError(
                f"Failed to get active searches: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # SearchRun
    # ------------------------------------------------------------------

    def create_search_run(self, tracked_search_id: int) -> SearchRun:
        """Создаёт запись о новом запуске сбора.

        Args:
            tracked_search_id: ID отслеживаемого поиска.

        Returns:
            SearchRun: Созданный объект запуска.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            run = SearchRun(
                tracked_search_id=tracked_search_id,
                started_at=datetime.datetime.now(datetime.timezone.utc),
                status="running",
            )
            self.session.add(run)
            self.session.flush()
            logger.info(
                "search_run_created",
                run_id=run.id,
                tracked_search_id=tracked_search_id,
            )
            return run
        except SQLAlchemyError as exc:
            logger.error(
                "create_search_run_failed",
                tracked_search_id=tracked_search_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to create search run: {exc}"
            ) from exc

    def complete_search_run(self, run_id: int, **kwargs) -> None:
        """Отмечает запуск сбора как завершённый.

        Args:
            run_id: ID запуска.
            **kwargs: Дополнительные поля для обновления
                (ads_found, ads_new, pages_fetched, ads_opened).

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            run = self.session.get(SearchRun, run_id)
            if run is None:
                logger.warning("search_run_not_found", run_id=run_id)
                return

            run.status = "completed"
            run.completed_at = datetime.datetime.now(datetime.timezone.utc)
            for key, value in kwargs.items():
                if hasattr(run, key):
                    setattr(run, key, value)

            self.session.flush()
            logger.info("search_run_completed", run_id=run_id, **kwargs)
        except SQLAlchemyError as exc:
            logger.error(
                "complete_search_run_failed",
                run_id=run_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to complete search run: {exc}"
            ) from exc

    def fail_search_run(self, run_id: int, error: str) -> None:
        """Отмечает запуск сбора как завершённый с ошибкой.

        Args:
            run_id: ID запуска.
            error: Текст ошибки.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            run = self.session.get(SearchRun, run_id)
            if run is None:
                logger.warning("search_run_not_found", run_id=run_id)
                return

            run.status = "failed"
            run.completed_at = datetime.datetime.now(datetime.timezone.utc)
            run.error_message = error
            run.errors_count = (run.errors_count or 0) + 1

            self.session.flush()
            logger.info(
                "search_run_failed",
                run_id=run_id,
                error=error,
            )
        except SQLAlchemyError as exc:
            logger.error(
                "fail_search_run_failed",
                run_id=run_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to fail search run: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Ad
    # ------------------------------------------------------------------

    def get_ad_by_ad_id(self, ad_id: str) -> Ad | None:
        """Возвращает объявление по Avito ad_id или None.

        Args:
            ad_id: Идентификатор объявления Avito.

        Returns:
            Ad | None: Найденное объявление или None.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = select(Ad).where(Ad.ad_id == ad_id)
            result = self.session.execute(stmt).scalar_one_or_none()
            if result is not None:
                logger.debug("ad_found_by_ad_id", ad_id=ad_id, db_id=result.id)
            else:
                logger.debug("ad_not_found_by_ad_id", ad_id=ad_id)
            return result
        except SQLAlchemyError as exc:
            logger.error("get_ad_by_ad_id_failed", ad_id=ad_id, error=str(exc))
            raise StorageError(
                f"Failed to get ad by ad_id: {exc}"
            ) from exc

    def get_pending_ads(self, limit: int | None = None) -> list[Ad]:
        """Получить объявления в статусе pending.

        Возвращает все объявления с ``parse_status='pending'``,
        отсортированные по времени первого обнаружения (от старых к новым).

        Args:
            limit: Максимум записей. Если ``None`` — без ограничения.

        Returns:
            list[Ad]: Список объявлений в статусе pending.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            query = self.session.query(Ad).filter(
                Ad.parse_status == "pending",
            ).order_by(Ad.first_seen_at.asc())
            if limit:
                query = query.limit(limit)
            return query.all()
        except Exception as exc:
            logger.error("get_pending_ads_failed", error=str(exc))
            raise StorageError(
                f"Failed to get pending ads: {exc}"
            ) from exc

    def get_or_create_ad(
        self, ad_id: str, url: str, search_url: str,
    ) -> tuple[Ad, bool]:
        """Возвращает существующее или создаёт новое объявление.

        Args:
            ad_id: Идентификатор объявления Avito.
            url: Полный URL объявления.
            search_url: URL поиска, откуда найдено объявление.

        Returns:
            tuple[Ad, bool]: Кортеж (объявление, флаг создания).

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = select(Ad).where(Ad.ad_id == ad_id)
            existing = self.session.execute(stmt).scalar_one_or_none()
            if existing is not None:
                logger.debug("ad_already_exists", ad_id=ad_id, db_id=existing.id)
                return existing, False

            ad = Ad(ad_id=ad_id, url=url, search_url=search_url)
            self.session.add(ad)
            savepoint = self.session.begin_nested()
            try:
                self.session.flush()
                savepoint.commit()
            except IntegrityError:
                # Race condition: другой параллельный запрос уже вставил эту запись.
                # Откатываем только SAVEPOINT, не трогая основную транзакцию.
                savepoint.rollback()
                logger.warning(
                    "get_or_create_ad_savepoint_rollback",
                    ad_id=ad_id,
                )
                self.session.expire_all()
                stmt = select(Ad).where(Ad.ad_id == ad_id)
                existing = self.session.execute(stmt).scalar_one_or_none()
                if existing is not None:
                    logger.info(
                        "ad_created_by_another_thread",
                        ad_id=ad_id,
                        db_id=existing.id,
                    )
                    return existing, False
                # Если всё ещё не найден — пробросим оригинальную ошибку
                raise

            logger.info(
                "ad_created",
                ad_id=ad_id,
                db_id=ad.id,
                search_url=search_url,
            )
            return ad, True
        except IntegrityError as exc:
            logger.error(
                "get_or_create_ad_integrity_error",
                ad_id=ad_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get or create ad (integrity): {exc}"
            ) from exc
        except SQLAlchemyError as exc:
            logger.error(
                "get_or_create_ad_failed",
                ad_id=ad_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get or create ad: {exc}"
            ) from exc

    def batch_get_or_create_ads(
        self,
        items: list[dict],
    ) -> list[tuple[Ad, bool]]:
        """Массовое создание/получение объявлений.

        Принимает список словарей с ключами ``ad_id``, ``url``, ``search_url``
        и возвращает список кортежей ``(Ad, created)`` в том же порядке.

        Использует ``IN``-запрос для получения существующих записей
        и ``flush`` для массовой вставки новых.

        Args:
            items: Список словарей с полями объявления.

        Returns:
            list[tuple[Ad, bool]]: Список кортежей (объявление, флаг создания).

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        if not items:
            return []

        try:
            # Собрать все ad_id
            ad_ids = [item["ad_id"] for item in items]

            # Получить существующие
            stmt = select(Ad).where(Ad.ad_id.in_(ad_ids))
            result = self.session.execute(stmt).scalars().all()
            existing = {ad.ad_id: ad for ad in result}

            # Создать недостающие
            new_ads: list[Ad] = []
            created_flags: dict[str, bool] = {}

            for item in items:
                aid = item["ad_id"]
                if aid in existing:
                    created_flags[aid] = False
                else:
                    title = item.get("title")
                    price = item.get("price")
                    if price is not None and price < 0:
                        price = None
                    ad = Ad(
                        ad_id=aid,
                        url=item["url"],
                        search_url=item["search_url"],
                        title=title,
                        price=price,
                    )
                    self.session.add(ad)
                    new_ads.append(ad)
                    created_flags[aid] = True

            if new_ads:
                savepoint = self.session.begin_nested()
                try:
                    self.session.flush()
                    savepoint.commit()
                    # Обновить existing новыми объектами после flush
                    for ad in new_ads:
                        existing[ad.ad_id] = ad
                except IntegrityError:
                    savepoint.rollback()
                    logger.warning(
                        "batch_get_or_create_ads_rollback",
                        count=len(new_ads),
                    )
                    # Перечитать из БД
                    self.session.expire_all()
                    stmt = select(Ad).where(Ad.ad_id.in_(ad_ids))
                    result = self.session.execute(stmt).scalars().all()
                    existing = {ad.ad_id: ad for ad in result}
                    # Сбросить флаги created для тех, что уже есть
                    for aid in ad_ids:
                        if aid in existing:
                            created_flags[aid] = False

            logger.info(
                "batch_get_or_create_ads",
                total=len(items),
                existing=len(items) - len(new_ads),
                new=len(new_ads),
            )

            return [
                (existing[item["ad_id"]], created_flags[item["ad_id"]])
                for item in items
            ]
        except IntegrityError as exc:
            logger.error(
                "batch_get_or_create_ads_integrity_error",
                error=str(exc),
            )
            raise StorageError(
                f"Failed to batch get or create ads (integrity): {exc}"
            ) from exc
        except SQLAlchemyError as exc:
            logger.error(
                "batch_get_or_create_ads_failed",
                error=str(exc),
            )
            raise StorageError(
                f"Failed to batch get or create ads: {exc}"
            ) from exc

    def update_ad(self, ad_id: str, **kwargs) -> None:
        """Обновляет поля объявления по Avito ad_id.

        Args:
            ad_id: Идентификатор объявления Avito.
            **kwargs: Поля для обновления.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = select(Ad).where(Ad.ad_id == ad_id)
            ad = self.session.execute(stmt).scalar_one_or_none()
            if ad is None:
                logger.warning("ad_not_found_for_update", ad_id=ad_id)
                return

            for key, value in kwargs.items():
                if hasattr(ad, key):
                    setattr(ad, key, value)
            ad.last_scraped_at = datetime.datetime.now(datetime.timezone.utc)

            self.session.flush()
            logger.debug("ad_updated", ad_id=ad_id, fields=list(kwargs.keys()))
        except SQLAlchemyError as exc:
            logger.error("update_ad_failed", ad_id=ad_id, error=str(exc))
            raise StorageError(
                f"Failed to update ad: {exc}"
            ) from exc

    def get_ads_for_search(self, search_url: str) -> list[Ad]:
        """Возвращает все объявления для заданного поискового URL.

        Args:
            search_url: URL поисковой выдачи.

        Returns:
            list[Ad]: Список объявлений.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = select(Ad).where(Ad.search_url == search_url)
            results = self.session.execute(stmt).scalars().all()
            logger.debug(
                "ads_for_search_fetched",
                search_url=search_url,
                count=len(results),
            )
            return list(results)
        except SQLAlchemyError as exc:
            logger.error(
                "get_ads_for_search_failed",
                search_url=search_url,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get ads for search: {exc}"
            ) from exc

    def get_recent_ad_ids(self, search_url: str, hours: int = 24) -> set[str]:
        """Возвращает множество ad_id объявлений за последние N часов.

        Args:
            search_url: URL поисковой выдачи.
            hours: Количество часов для поиска.

        Returns:
            set[str]: Множество ad_id.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=hours)
            stmt = (
                select(Ad.ad_id)
                .where(Ad.search_url == search_url)
                .where(Ad.first_seen_at >= cutoff)
            )
            results = self.session.execute(stmt).scalars().all()
            ad_ids = set(results)
            logger.debug(
                "recent_ad_ids_fetched",
                search_url=search_url,
                hours=hours,
                count=len(ad_ids),
            )
            return ad_ids
        except SQLAlchemyError as exc:
            logger.error(
                "get_recent_ad_ids_failed",
                search_url=search_url,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get recent ad ids: {exc}"
            ) from exc

    def mark_ad_parse_failed(self, ad_id: str, error: str) -> None:
        """Отмечает объявление как failed при парсинге.

        Args:
            ad_id: Идентификатор объявления Avito.
            error: Текст ошибки парсинга.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = select(Ad).where(Ad.ad_id == ad_id)
            ad = self.session.execute(stmt).scalar_one_or_none()
            if ad is None:
                logger.warning("ad_not_found_for_parse_fail", ad_id=ad_id)
                return

            ad.parse_status = "failed"
            ad.last_error = error
            self.session.flush()
            logger.info("ad_parse_failed", ad_id=ad_id, error=error)
        except SQLAlchemyError as exc:
            logger.error(
                "mark_ad_parse_failed_error",
                ad_id=ad_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to mark ad parse failed: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # AdSnapshot
    # ------------------------------------------------------------------

    def create_snapshot(
        self, ad_id: int, price: float | None, html_path: str,
    ) -> AdSnapshot:
        """Создаёт снимок цены объявления.

        Args:
            ad_id: Внутренний ID объявления (FK).
            price: Зафиксированная цена.
            html_path: Путь к файлу с HTML.

        Returns:
            AdSnapshot: Созданный снимок.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            snapshot = AdSnapshot(
                ad_id=ad_id,
                price=price,
                html_path=html_path,
            )
            self.session.add(snapshot)
            self.session.flush()
            logger.info(
                "snapshot_created",
                snapshot_id=snapshot.id,
                ad_id=ad_id,
                price=price,
            )
            return snapshot
        except SQLAlchemyError as exc:
            logger.error(
                "create_snapshot_failed",
                ad_id=ad_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to create snapshot: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Notifications
    # ------------------------------------------------------------------

    def is_notification_sent(
        self, ad_id: int, notification_type: str = "telegram_undervalued",
    ) -> bool:
        """Проверяет, было ли уже отправлено уведомление для объявления.

        Args:
            ad_id: Внутренний ID объявления (FK).
            notification_type: Тип уведомления.

        Returns:
            bool: True если уведомление уже отправлено.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = (
                select(NotificationSent)
                .where(
                    NotificationSent.ad_id == ad_id,
                    NotificationSent.notification_type == notification_type,
                )
                .limit(1)
            )
            result = self.session.scalar(stmt)
            sent = result is not None
            logger.debug(
                "notification_sent_check",
                ad_id=ad_id,
                notification_type=notification_type,
                sent=sent,
            )
            return sent
        except SQLAlchemyError as exc:
            logger.error(
                "is_notification_sent_failed",
                ad_id=ad_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to check notification sent: {exc}"
            ) from exc

    def mark_notification_sent(
        self,
        ad_id: int,
        notification_type: str = "telegram_undervalued",
        telegram_message_id: str | None = None,
    ) -> None:
        """Записывает факт отправки уведомления.

        Args:
            ad_id: Внутренний ID объявления (FK).
            notification_type: Тип уведомления.
            telegram_message_id: ID сообщения в Telegram (опционально).

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        # Проверяем, не отправлено ли уже
        if self.is_notification_sent(ad_id, notification_type):
            logger.debug(
                "notification_already_sent_skip",
                ad_id=ad_id,
                notification_type=notification_type,
            )
            return

        savepoint = self.session.begin_nested()
        try:
            notification = NotificationSent(
                ad_id=ad_id,
                notification_type=notification_type,
                telegram_message_id=telegram_message_id,
            )
            self.session.add(notification)
            self.session.flush()
            savepoint.commit()
            # flush() отправляет изменения в БД внутри транзакции.
            # Pipeline сам сделает commit в конце цикла.
            logger.info(
                "notification_marked_sent",
                ad_id=ad_id,
                notification_type=notification_type,
                telegram_message_id=telegram_message_id,
            )
        except IntegrityError:
            # Дубль на уровне БД — откатываем только SAVEPOINT
            savepoint.rollback()
            logger.warning(
                "notification_duplicate_integrity",
                ad_id=ad_id,
                notification_type=notification_type,
            )
        except SQLAlchemyError as exc:
            logger.error(
                "mark_notification_sent_failed",
                ad_id=ad_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to mark notification sent: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Сегментация и аналитика
    # ------------------------------------------------------------------

    def get_ads_by_segment(
        self, segment_key: str, days: int = 14,
    ) -> list[Ad]:
        """Возвращает объявления по сегменту за последние N дней.

        Args:
            segment_key: Ключ сегмента вида «{condition}_{location}_{seller_type}».
            days: Количество дней для поиска.

        Returns:
            list[Ad]: Список объявлений в сегменте.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
            stmt = (
                select(Ad)
                .where(Ad.segment_key == segment_key)
                .where(Ad.first_seen_at >= cutoff)
            )
            results = self.session.execute(stmt).scalars().all()
            logger.debug(
                "ads_by_segment_fetched",
                segment_key=segment_key,
                days=days,
                count=len(results),
            )
            return list(results)
        except SQLAlchemyError as exc:
            logger.error(
                "get_ads_by_segment_failed",
                segment_key=segment_key,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get ads by segment: {exc}"
            ) from exc

    def get_ads_for_analysis(
        self, search_url: str, days: int = 14,
    ) -> list[Ad]:
        """Возвращает все объявления для анализа с фильтром по давности.

        Выбирает объявления с валидной ценой (price IS NOT NULL AND price > 0)
        и датой первого обнаружения не старше N дней.

        Args:
            search_url: URL поисковой выдачи.
            days: Количество дней для поиска.

        Returns:
            list[Ad]: Список объявлений для анализа.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
            stmt = (
                select(Ad)
                .where(Ad.search_url == search_url)
                .where(Ad.first_seen_at >= cutoff)
                .where(Ad.price.is_not(None))
                .where(Ad.price > 0)
            )
            results = self.session.execute(stmt).scalars().all()
            logger.debug(
                "ads_for_analysis_fetched",
                search_url=search_url,
                days=days,
                count=len(results),
            )
            return list(results)
        except SQLAlchemyError as exc:
            logger.error(
                "get_ads_for_analysis_failed",
                search_url=search_url,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get ads for analysis: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Планирование запусков
    # ------------------------------------------------------------------

    def get_searches_due_for_run(self) -> list[TrackedSearch]:
        """Возвращает поиски, которые пора запускать.

        Выбирает активные поиски, у которых:
        - last_run_at IS NULL (никогда не запускался), либо
        - last_run_at + schedule_interval_hours <= now()

        Результаты отсортированы по приоритету (по убыванию).

        Returns:
            list[TrackedSearch]: Список поисков, готовых к запуску.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            from sqlalchemy import func, or_

            now = datetime.datetime.now(datetime.timezone.utc)

            # Фильтрация полностью в SQL:
            # last_run_at IS NULL (никогда не запускался)
            # OR last_run_at + interval <= now()
            stmt = (
                select(TrackedSearch)
                .where(
                    TrackedSearch.is_active.is_(True),
                    or_(
                        TrackedSearch.last_run_at.is_(None),
                        TrackedSearch.last_run_at + func.make_interval(
                            0, 0, 0, 0, 0, 0,
                            TrackedSearch.schedule_interval_hours * 3600,
                        ) <= now,
                    ),
                )
                .order_by(TrackedSearch.priority.desc())
            )
            due = list(self.session.execute(stmt).scalars().all())

            logger.debug(
                "searches_due_for_run_fetched",
                count=len(due),
            )
            return due
        except SQLAlchemyError as exc:
            logger.error(
                "get_searches_due_for_run_failed",
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get searches due for run: {exc}"
            ) from exc

    def update_search_last_run(self, search_id: int) -> None:
        """Обновляет last_run_at для поискового запроса.

        Args:
            search_id: ID поискового запроса (TrackedSearch.id).

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            tracked = self.session.get(TrackedSearch, search_id)
            if tracked is None:
                logger.warning(
                    "tracked_search_not_for_update_last_run",
                    search_id=search_id,
                )
                return

            tracked.last_run_at = datetime.datetime.now(datetime.timezone.utc)
            self.session.flush()
            logger.info(
                "search_last_run_updated",
                search_id=search_id,
            )
        except SQLAlchemyError as exc:
            logger.error(
                "update_search_last_run_failed",
                search_id=search_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to update search last run: {exc}"
            ) from exc

    def update_ad_analysis(
        self,
        ad_id: int,
        z_score: float,
        iqr_outlier: bool,
        segment_key: str,
    ) -> None:
        """Обновляет аналитические поля объявления.

        Args:
            ad_id: Внутренний ID объявления (Ad.id).
            z_score: Z-score цена относительно сегмента.
            iqr_outlier: Является ли выбросом по IQR.
            segment_key: Ключ сегмента.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            ad = self.session.get(Ad, ad_id)
            if ad is None:
                logger.warning(
                    "ad_not_found_for_analysis_update",
                    ad_id=ad_id,
                )
                return

            ad.z_score = z_score
            ad.iqr_outlier = iqr_outlier
            ad.segment_key = segment_key
            self.session.flush()
            logger.info(
                "ad_analysis_updated",
                ad_id=ad_id,
                z_score=z_score,
                iqr_outlier=iqr_outlier,
                segment_key=segment_key,
            )
        except SQLAlchemyError as exc:
            logger.error(
                "update_ad_analysis_failed",
                ad_id=ad_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to update ad analysis: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # SegmentStats / SegmentPriceHistory
    # ------------------------------------------------------------------

    def upsert_segment_stats(
        self, search_id: int, segment_key: str, stats: dict,
    ) -> SegmentStats:
        """Создаёт или обновляет запись статистики сегмента.

        Ищет существующую запись по ``search_id`` + ``segment_key``.
        Если найдена — обновляет переданные поля из ``stats``.
        Если нет — создаёт новую запись.

        Args:
            search_id: ID отслеживаемого поиска (TrackedSearch.id).
            segment_key: Ключ сегмента.
            stats: Словарь с полями: ``median_7d``, ``median_30d``,
                ``median_90d``, ``mean_price``, ``min_price``, ``max_price``,
                ``price_trend_slope``, ``sample_size``, ``listing_count``,
                ``appearance_count_90d``, ``median_days_on_market``,
                ``listing_price_median``, ``fast_sale_price_median``,
                ``liquid_market_estimate``, ``is_rare_segment``,
                ``segment_name``.

        Returns:
            SegmentStats: Привязанный к сессии объект.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            savepoint = self.session.begin_nested()
            try:
                stmt = (
                    select(SegmentStats)
                    .where(SegmentStats.search_id == search_id)
                    .where(SegmentStats.segment_key == segment_key)
                )
                existing = self.session.execute(stmt).scalar_one_or_none()

                if existing is not None:
                    for key, value in stats.items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                    existing.calculated_at = datetime.datetime.now(datetime.timezone.utc)
                    existing.updated_at = datetime.datetime.now(datetime.timezone.utc)
                    self.session.flush()
                    logger.debug(
                        "segment_stats_updated",
                        search_id=search_id,
                        segment_key=segment_key,
                    )
                    savepoint.commit()
                    return existing

                # Гарантируем, что NOT NULL колонки имеют значения
                _not_null_defaults = {
                    "category": "unknown",
                    "brand": "unknown",
                    "model": "unknown",
                    "condition": "unknown",
                    "location": "unknown",
                    "seller_type": "unknown",
                }
                filtered_stats = {
                    k: v for k, v in stats.items() if hasattr(SegmentStats, k)
                }
                for col, default in _not_null_defaults.items():
                    filtered_stats.setdefault(col, default)
                    if filtered_stats[col] is None:
                        filtered_stats[col] = default

                new_stats = SegmentStats(
                    search_id=search_id,
                    segment_key=segment_key,
                    **filtered_stats,
                )
                self.session.add(new_stats)
                self.session.flush()
                logger.info(
                    "segment_stats_created",
                    search_id=search_id,
                    segment_key=segment_key,
                    stats_id=new_stats.id,
                )
                savepoint.commit()
                return new_stats
            except Exception as inner_exc:
                savepoint.rollback()
                raise inner_exc
        except SQLAlchemyError as exc:
            logger.error(
                "upsert_segment_stats_failed",
                search_id=search_id,
                segment_key=segment_key,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to upsert segment stats: {exc}"
            ) from exc

    def get_segment_stats(
        self,
        search_id: int,
        segment_key: str | None = None,
    ) -> list[SegmentStats] | SegmentStats | None:
        """Возвращает статистику сегмента(ов) для поиска.

        Если ``segment_key`` задан — возвращает одну запись или ``None``.
        Если нет — возвращает все записи для ``search_id``.

        Args:
            search_id: ID отслеживаемого поиска (TrackedSearch.id).
            segment_key: Ключ сегмента (опционально).

        Returns:
            list[SegmentStats] | SegmentStats | None: Статистика сегмента(ов).

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            if segment_key is not None:
                stmt = (
                    select(SegmentStats)
                    .where(SegmentStats.search_id == search_id)
                    .where(SegmentStats.segment_key == segment_key)
                )
                result = self.session.execute(stmt).scalar_one_or_none()
                logger.debug(
                    "segment_stats_fetched",
                    search_id=search_id,
                    segment_key=segment_key,
                    found=result is not None,
                )
                return result

            stmt = select(SegmentStats).where(
                SegmentStats.search_id == search_id,
            )
            results = self.session.execute(stmt).scalars().all()
            logger.debug(
                "segment_stats_list_fetched",
                search_id=search_id,
                count=len(results),
            )
            return list(results)
        except SQLAlchemyError as exc:
            logger.error(
                "get_segment_stats_failed",
                search_id=search_id,
                segment_key=segment_key,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get segment stats: {exc}"
            ) from exc

    def save_price_history_snapshot(
        self,
        segment_stats_id: int,
        snapshot_date: datetime.date,
        data: dict,
    ) -> SegmentPriceHistory:
        """Создаёт или обновляет снапшот истории цен для сегмента.

        Использует UPSERT по ``(segment_stats_id, snapshot_date)``.

        Args:
            segment_stats_id: ID записи SegmentStats.
            snapshot_date: Дата снапшота.
            data: Словарь с полями: ``median_price``, ``mean_price``,
                ``min_price``, ``max_price``, ``sample_size``,
                ``listing_count``, ``fast_sale_count``,
                ``median_days_on_market``.

        Returns:
            SegmentPriceHistory: Привязанный к сессии объект.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = (
                select(SegmentPriceHistory)
                .where(SegmentPriceHistory.segment_stats_id == segment_stats_id)
                .where(SegmentPriceHistory.snapshot_date == snapshot_date)
            )
            existing = self.session.execute(stmt).scalar_one_or_none()

            if existing is not None:
                for key, value in data.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
                self.session.flush()
                logger.debug(
                    "price_history_snapshot_updated",
                    segment_stats_id=segment_stats_id,
                    snapshot_date=str(snapshot_date),
                )
                return existing

            snapshot = SegmentPriceHistory(
                segment_stats_id=segment_stats_id,
                snapshot_date=snapshot_date,
                **{k: v for k, v in data.items() if hasattr(SegmentPriceHistory, k)},
            )
            self.session.add(snapshot)
            self.session.flush()
            logger.info(
                "price_history_snapshot_created",
                segment_stats_id=segment_stats_id,
                snapshot_date=str(snapshot_date),
                snapshot_id=snapshot.id,
            )
            return snapshot
        except SQLAlchemyError as exc:
            logger.error(
                "save_price_history_snapshot_failed",
                segment_stats_id=segment_stats_id,
                snapshot_date=str(snapshot_date),
                error=str(exc),
            )
            raise StorageError(
                f"Failed to save price history snapshot: {exc}"
            ) from exc

    def get_price_history(
        self,
        segment_stats_id: int,
        days: int = 90,
    ) -> list[SegmentPriceHistory]:
        """Возвращает историю цен сегмента за последние ``days`` дней.

        Сортировка по ``snapshot_date`` ASC.

        Args:
            segment_stats_id: ID записи SegmentStats.
            days: Количество дней истории.

        Returns:
            list[SegmentPriceHistory]: Записи истории цен.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            cutoff = datetime.date.today() - datetime.timedelta(days=days)
            stmt = (
                select(SegmentPriceHistory)
                .where(SegmentPriceHistory.segment_stats_id == segment_stats_id)
                .where(SegmentPriceHistory.snapshot_date >= cutoff)
                .order_by(SegmentPriceHistory.snapshot_date.asc())
            )
            results = self.session.execute(stmt).scalars().all()
            logger.debug(
                "price_history_fetched",
                segment_stats_id=segment_stats_id,
                days=days,
                count=len(results),
            )
            return list(results)
        except SQLAlchemyError as exc:
            logger.error(
                "get_price_history_failed",
                segment_stats_id=segment_stats_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get price history: {exc}"
            ) from exc

    def mark_ads_disappeared(
        self,
        ad_ids: list[int],
        days_threshold: int = 3,
    ) -> None:
        """Помечает объявления как исчезнувшие.

        Для каждого объявления из ``ad_ids``:
        - Устанавливает ``is_disappeared_quickly = True`` если
          ``days_on_market <= days_threshold``.
        - Устанавливает ``is_active = False`` (если поле существует).

        Args:
            ad_ids: Список внутренних ID объявлений (Ad.id).
            days_threshold: Порог дней для признания быстрого исчезновения.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        if not ad_ids:
            return
        try:
            stmt = select(Ad).where(Ad.id.in_(ad_ids))
            ads = self.session.execute(stmt).scalars().all()

            now = datetime.datetime.now(datetime.timezone.utc)
            marked = 0
            for ad in ads:
                if ad.days_on_market is not None and ad.days_on_market <= days_threshold:
                    ad.is_disappeared_quickly = True
                if hasattr(ad, "is_active"):
                    ad.is_active = False
                marked += 1

            if marked:
                self.session.flush()
            logger.info(
                "ads_marked_disappeared",
                count=marked,
                days_threshold=days_threshold,
            )
        except SQLAlchemyError as exc:
            logger.error(
                "mark_ads_disappeared_failed",
                error=str(exc),
            )
            raise StorageError(
                f"Failed to mark ads disappeared: {exc}"
            ) from exc

    def update_ad_tracking_fields(
        self,
        ad_id: int,
        last_seen_at: datetime.datetime | None = None,
        days_on_market: int | None = None,
        ad_category: str | None = None,
        brand: str | None = None,
        extracted_model: str | None = None,
    ) -> None:
        """Обновляет поля трекинга объявления.

        Обновляет только переданные поля (не ``None``).

        Args:
            ad_id: Внутренний ID объявления (Ad.id).
            last_seen_at: Время последнего обнаружения.
            days_on_market: Количество дней на рынке.
            ad_category: Категория объявления.
            brand: Бренд товара.
            extracted_model: Извлечённая модель товара.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            ad = self.session.get(Ad, ad_id)
            if ad is None:
                logger.warning(
                    "ad_not_found_for_tracking_update",
                    ad_id=ad_id,
                )
                return

            updates = {
                "last_seen_at": last_seen_at,
                "days_on_market": days_on_market,
                "ad_category": ad_category,
                "brand": brand,
                "extracted_model": extracted_model,
            }
            for key, value in updates.items():
                if value is not None and hasattr(ad, key):
                    setattr(ad, key, value)

            self.session.flush()
            updated_fields = [k for k, v in updates.items() if v is not None]
            logger.debug(
                "ad_tracking_fields_updated",
                ad_id=ad_id,
                fields=updated_fields,
            )
        except SQLAlchemyError as exc:
            logger.error(
                "update_ad_tracking_fields_failed",
                ad_id=ad_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to update ad tracking fields: {exc}"
            ) from exc

    def get_disappeared_ads(
        self,
        search_id: int,
        since_days: int = 7,
    ) -> list[Ad]:
        """Возвращает объявления из поиска, которые были активны, но исчезли.

        Это объявления, у которых ``last_seen_at < now - threshold`` и которые
        ранее были активны (имеют ``search_url``, соответствующий поиску).

        Args:
            search_id: ID отслеживаемого поиска (TrackedSearch.id).
            since_days: За сколько последних дней искать исчезнувшие.

        Returns:
            list[Ad]: Исчезнувшие объявления.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            tracked = self.session.get(TrackedSearch, search_id)
            if tracked is None:
                logger.warning(
                    "tracked_search_not_found_for_disappeared",
                    search_id=search_id,
                )
                return []

            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
                days=since_days,
            )
            stmt = (
                select(Ad)
                .where(Ad.search_url == tracked.search_url)
                .where(Ad.last_seen_at.is_not(None))
                .where(Ad.last_seen_at < cutoff)
                .where(Ad.first_seen_at >= cutoff)
            )
            results = self.session.execute(stmt).scalars().all()
            logger.debug(
                "disappeared_ads_fetched",
                search_id=search_id,
                since_days=since_days,
                count=len(results),
            )
            return list(results)
        except SQLAlchemyError as exc:
            logger.error(
                "get_disappeared_ads_failed",
                search_id=search_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get disappeared ads: {exc}"
            ) from exc

    def get_segment_ads(
        self,
        search_id: int,
        segment_key: str | None = None,
        days: int = 30,
        active_only: bool = True,
    ) -> list[Ad]:
        """Возвращает объявления сегмента за указанный период.

        Если ``segment_key`` задан, фильтрует по ``brand`` +
        ``extracted_model`` или ``ad_category``. Если нет — возвращает
        все объявления для ``search_id`` за период.

        Args:
            search_id: ID отслеживаемого поиска (TrackedSearch.id).
            segment_key: Ключ сегмента (опционально).
            days: Количество дней для поиска.
            active_only: Только активные (не исчезнувшие быстро).

        Returns:
            list[Ad]: Объявления сегмента.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            tracked = self.session.get(TrackedSearch, search_id)
            if tracked is None:
                logger.warning(
                    "tracked_search_not_found_for_segment_ads",
                    search_id=search_id,
                )
                return []

            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
            stmt = (
                select(Ad)
                .where(Ad.search_url == tracked.search_url)
                .where(Ad.first_seen_at >= cutoff)
                .where(Ad.price.is_not(None))
                .where(Ad.price > 0)
            )

            if active_only:
                stmt = stmt.where(
                    Ad.is_disappeared_quickly.is_(False)
                    | Ad.is_disappeared_quickly.is_(None),
                )

            if segment_key is not None:
                # segment_key может быть вида «brand:model» или «category:...»
                parts = segment_key.split(":", 1)
                if len(parts) == 2:
                    key_type, key_value = parts
                    if key_type == "category":
                        stmt = stmt.where(Ad.ad_category == key_value)
                    else:
                        # brand:model — фильтруем по brand и extracted_model
                        sub_parts = key_value.split(":", 1)
                        if len(sub_parts) == 2:
                            stmt = stmt.where(
                                Ad.brand == sub_parts[0],
                                Ad.extracted_model == sub_parts[1],
                            )
                        else:
                            stmt = stmt.where(Ad.brand == key_value)

            results = self.session.execute(stmt).scalars().all()
            logger.debug(
                "segment_ads_fetched",
                search_id=search_id,
                segment_key=segment_key,
                days=days,
                active_only=active_only,
                count=len(results),
            )
            return list(results)
        except SQLAlchemyError as exc:
            logger.error(
                "get_segment_ads_failed",
                search_id=search_id,
                segment_key=segment_key,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get segment ads: {exc}"
            ) from exc

    def calculate_fast_sale_stats(
        self,
        search_id: int,
        segment_key: str | None = None,
    ) -> dict:
        """Агрегатный запрос: статистика быстрых продаж в сегменте.

        Для объявлений с ``is_disappeared_quickly = True`` в сегменте
        рассчитывает агрегатные метрики.

        Args:
            search_id: ID отслеживаемого поиска (TrackedSearch.id).
            segment_key: Ключ сегмента (опционально).

        Returns:
            dict: {
                ``fast_sale_count``: int,
                ``fast_sale_price_median``: float | None,
                ``fast_sale_price_mean``: float | None,
                ``median_days_on_market``: float | None,
            }

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            tracked = self.session.get(TrackedSearch, search_id)
            if tracked is None:
                logger.warning(
                    "tracked_search_not_found_for_fast_sale",
                    search_id=search_id,
                )
                return {
                    "fast_sale_count": 0,
                    "fast_sale_price_median": None,
                    "fast_sale_price_mean": None,
                    "median_days_on_market": None,
                }

            stmt = (
                select(Ad)
                .where(Ad.search_url == tracked.search_url)
                .where(Ad.is_disappeared_quickly.is_(True))
                .where(Ad.price.is_not(None))
                .where(Ad.price > 0)
            )

            if segment_key is not None:
                parts = segment_key.split(":", 1)
                if len(parts) == 2:
                    key_type, key_value = parts
                    if key_type == "category":
                        stmt = stmt.where(Ad.ad_category == key_value)
                    else:
                        sub_parts = key_value.split(":", 1)
                        if len(sub_parts) == 2:
                            stmt = stmt.where(
                                Ad.brand == sub_parts[0],
                                Ad.extracted_model == sub_parts[1],
                            )
                        else:
                            stmt = stmt.where(Ad.brand == key_value)

            ads = self.session.execute(stmt).scalars().all()

            if not ads:
                return {
                    "fast_sale_count": 0,
                    "fast_sale_price_median": None,
                    "fast_sale_price_mean": None,
                    "median_days_on_market": None,
                }

            prices = sorted([ad.price for ad in ads if ad.price])
            days_list = sorted(
                [ad.days_on_market for ad in ads if ad.days_on_market is not None],
            )

            fast_sale_price_median = None
            if prices:
                mid = len(prices) // 2
                if len(prices) % 2 == 0:
                    fast_sale_price_median = (prices[mid - 1] + prices[mid]) / 2
                else:
                    fast_sale_price_median = prices[mid]

            fast_sale_price_mean = None
            if prices:
                fast_sale_price_mean = sum(prices) / len(prices)

            median_days = None
            if days_list:
                mid = len(days_list) // 2
                if len(days_list) % 2 == 0:
                    median_days = (days_list[mid - 1] + days_list[mid]) / 2
                else:
                    median_days = days_list[mid]

            result = {
                "fast_sale_count": len(ads),
                "fast_sale_price_median": fast_sale_price_median,
                "fast_sale_price_mean": fast_sale_price_mean,
                "median_days_on_market": median_days,
            }
            logger.debug(
                "fast_sale_stats_calculated",
                search_id=search_id,
                segment_key=segment_key,
                **result,
            )
            return result
        except SQLAlchemyError as exc:
            logger.error(
                "calculate_fast_sale_stats_failed",
                search_id=search_id,
                segment_key=segment_key,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to calculate fast sale stats: {exc}"
            ) from exc

    def get_all_segment_stats_for_search(
        self, search_id: int,
    ) -> list[SegmentStats]:
        """Возвращает все статистики сегментов для конкретного поиска.

        Включает связанные записи истории (eager loading).

        Args:
            search_id: ID отслеживаемого поиска (TrackedSearch.id).

        Returns:
            list[SegmentStats]: Статистики сегментов с историей цен.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = (
                select(SegmentStats)
                .where(SegmentStats.search_id == search_id)
                .options(selectinload(SegmentStats.price_history))
            )
            results = self.session.execute(stmt).scalars().all()
            logger.debug(
                "all_segment_stats_fetched",
                search_id=search_id,
                count=len(results),
            )
            return list(results)
        except SQLAlchemyError as exc:
            logger.error(
                "get_all_segment_stats_for_search_failed",
                search_id=search_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get all segment stats for search: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Seller
    # ------------------------------------------------------------------

    def get_or_create_seller(
        self,
        seller_id: str,
        seller_url: str | None = None,
        seller_name: str | None = None,
    ) -> Seller:
        """Найти или создать продавца по Avito seller_id.

        Использует SAVEPOINT для защиты от race condition при
        параллельной вставке.

        Args:
            seller_id: Строковый ID продавца на Avito.
            seller_url: URL профиля продавца (опционально).
            seller_name: Имя продавца (опционально).

        Returns:
            Seller: Найденный или созданный объект продавца.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = select(Seller).where(Seller.seller_id == seller_id)
            existing = self.session.execute(stmt).scalar_one_or_none()
            if existing is not None:
                logger.debug(
                    "seller_found",
                    seller_id=seller_id,
                    db_id=existing.id,
                )
                return existing

            seller = Seller(
                seller_id=seller_id,
                seller_url=seller_url,
                seller_name=seller_name,
            )
            self.session.add(seller)
            savepoint = self.session.begin_nested()
            try:
                self.session.flush()
                savepoint.commit()
            except IntegrityError:
                # Race condition: другой поток уже вставил запись
                savepoint.rollback()
                logger.warning(
                    "get_or_create_seller_savepoint_rollback",
                    seller_id=seller_id,
                )
                self.session.expire_all()
                stmt = select(Seller).where(Seller.seller_id == seller_id)
                existing = self.session.execute(stmt).scalar_one_or_none()
                if existing is not None:
                    logger.info(
                        "seller_created_by_another_thread",
                        seller_id=seller_id,
                        db_id=existing.id,
                    )
                    return existing
                raise

            logger.info(
                "seller_created",
                seller_id=seller_id,
                db_id=seller.id,
            )
            return seller
        except IntegrityError as exc:
            logger.error(
                "get_or_create_seller_integrity_error",
                seller_id=seller_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get or create seller (integrity): {exc}"
            ) from exc
        except SQLAlchemyError as exc:
            logger.error(
                "get_or_create_seller_failed",
                seller_id=seller_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get or create seller: {exc}"
            ) from exc

    def update_seller(self, seller_id: str, **kwargs) -> bool:
        """Обновить поля продавца по Avito seller_id.

        Args:
            seller_id: Строковый ID продавца на Avito.
            **kwargs: Поля для обновления. Поддерживаются:
                seller_name, rating, reviews_count, total_sold_items,
                last_scraped_at, scrape_status, seller_url.

        Returns:
            bool: ``True``, если продавец найден и обновлён; ``False``, если не найден.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = select(Seller).where(Seller.seller_id == seller_id)
            seller = self.session.execute(stmt).scalar_one_or_none()
            if seller is None:
                logger.warning(
                    "seller_not_found_for_update",
                    seller_id=seller_id,
                )
                return False

            allowed_fields = {
                "seller_name", "rating", "reviews_count",
                "total_sold_items", "last_scraped_at",
                "scrape_status", "seller_url",
            }
            for key, value in kwargs.items():
                if key in allowed_fields and hasattr(seller, key):
                    setattr(seller, key, value)

            self.session.flush()
            logger.info(
                "seller_updated",
                seller_id=seller_id,
                updated_fields=list(kwargs.keys()),
            )
            return True
        except SQLAlchemyError as exc:
            logger.error(
                "update_seller_failed",
                seller_id=seller_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to update seller: {exc}"
            ) from exc

    def get_seller_by_id(self, seller_id: str) -> Seller | None:
        """Найти продавца по Avito seller_id.

        Args:
            seller_id: Строковый ID продавца на Avito.

        Returns:
            Seller | None: Найденный продавец или ``None``.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = select(Seller).where(Seller.seller_id == seller_id)
            result = self.session.execute(stmt).scalar_one_or_none()
            if result is not None:
                logger.debug(
                    "seller_found_by_id",
                    seller_id=seller_id,
                    db_id=result.id,
                )
            else:
                logger.debug("seller_not_found_by_id", seller_id=seller_id)
            return result
        except SQLAlchemyError as exc:
            logger.error(
                "get_seller_by_id_failed",
                seller_id=seller_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get seller by id: {exc}"
            ) from exc

    def get_sellers_due_for_scrape(
        self,
        limit: int = 5,
        interval_hours: float = 24.0,
    ) -> list[Seller]:
        """Получить продавцов, которых нужно парсить.

        Возвращает продавцов, которые:
        - никогда не парсились (``last_scraped_at IS NULL``);
        - или не парсились более ``interval_hours`` часов;
        - при этом ``scrape_status`` не ``'failed'``, либо последняя
          попытка была более ``interval_hours`` назад.

        Результат отсортирован по ``last_scraped_at ASC NULLS FIRST``.

        Args:
            limit: Максимум записей.
            interval_hours: Интервал повторного парсинга в часах.

        Returns:
            list[Seller]: Список продавцов для парсинга.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
                hours=interval_hours,
            )
            stmt = (
                select(Seller)
                .where(
                    # Никогда не парсились ИЛИ парсились давно
                    (Seller.last_scraped_at.is_(None))
                    | (Seller.last_scraped_at < cutoff)
                )
                .where(
                    # Не failed, ИЛИ failed но последняя попытка давно
                    (Seller.scrape_status != "failed")
                    | (Seller.last_scraped_at.is_(None))
                    | (Seller.last_scraped_at < cutoff)
                )
                .order_by(Seller.last_scraped_at.asc().nullsfirst())
                .limit(limit)
            )
            results = self.session.execute(stmt).scalars().all()
            logger.debug(
                "sellers_due_for_scrape_fetched",
                count=len(results),
                limit=limit,
                interval_hours=interval_hours,
            )
            return list(results)
        except SQLAlchemyError as exc:
            logger.error(
                "get_sellers_due_for_scrape_failed",
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get sellers due for scrape: {exc}"
            ) from exc

    def link_ad_to_seller(self, ad_id: int, seller_id_fk: int) -> bool:
        """Привязать объявление к продавцу.

        Устанавливает ``seller_id_fk`` для объявления.

        Args:
            ad_id: Внутренний ID объявления (``Ad.id``).
            seller_id_fk: Внутренний ID продавца (``Seller.id``).

        Returns:
            bool: ``True``, если объявление найдено и обновлено; ``False``, если не найдено.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            ad = self.session.get(Ad, ad_id)
            if ad is None:
                logger.warning("ad_not_found_for_link", ad_id=ad_id)
                return False

            ad.seller_id_fk = seller_id_fk
            self.session.flush()
            logger.info(
                "ad_linked_to_seller",
                ad_id=ad_id,
                seller_id_fk=seller_id_fk,
            )
            return True
        except SQLAlchemyError as exc:
            logger.error(
                "link_ad_to_seller_failed",
                ad_id=ad_id,
                seller_id_fk=seller_id_fk,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to link ad to seller: {exc}"
            ) from exc

    def get_unlinked_seller_ads(self) -> list[Ad]:
        """Получить объявления с заполненным seller_name, но без seller_id_fk.

        Возвращает объявления, у которых указано имя продавца
        (``seller_name IS NOT NULL``), но ещё нет привязки к таблице
        продавцов (``seller_id_fk IS NULL``).

        Returns:
            list[Ad]: Список непривязанных объявлений.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = (
                select(Ad)
                .where(Ad.seller_name.is_not(None))
                .where(Ad.seller_id_fk.is_(None))
            )
            results = self.session.execute(stmt).scalars().all()
            logger.debug(
                "unlinked_seller_ads_fetched",
                count=len(results),
            )
            return list(results)
        except SQLAlchemyError as exc:
            logger.error(
                "get_unlinked_seller_ads_failed",
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get unlinked seller ads: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Product — нормализованный товар
    # ------------------------------------------------------------------

    def get_or_create_product(
        self,
        normalized_key: str,
        brand: str | None = None,
        model: str | None = None,
        category: str | None = None,
    ) -> Product:
        """Найти или создать Product по normalized_key.

        Если товар с таким ключом уже существует — возвращает его.
        Иначе — создаёт новую запись.

        Args:
            normalized_key: Нормализованный ключ товара.
            brand: Бренд (опционально).
            model: Модель (опционально).
            category: Категория (опционально).

        Returns:
            Product: Найденный или созданный товар.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = select(Product).where(
                Product.normalized_key == normalized_key,
            )
            existing = self.session.execute(stmt).scalars().first()
            if existing is not None:
                return existing

            product = Product(
                normalized_key=normalized_key,
                brand=brand,
                model=model,
                category=category,
            )
            self.session.add(product)
            self.session.flush()

            logger.debug(
                "product_created",
                product_id=product.id,
                normalized_key=normalized_key,
            )
            return product
        except IntegrityError:
            # Race condition: другой процесс уже создал
            self.session.rollback()
            stmt = select(Product).where(
                Product.normalized_key == normalized_key,
            )
            existing = self.session.execute(stmt).scalars().first()
            if existing is not None:
                return existing
            raise
        except SQLAlchemyError as exc:
            logger.error(
                "get_or_create_product_failed",
                normalized_key=normalized_key,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get/create product: {exc}"
            ) from exc

    def add_product_price_snapshot(
        self,
        product_id: int,
        price: float,
        ad_id: int | None = None,
    ) -> ProductPriceSnapshot:
        """Добавить снимок цены товара.

        Args:
            product_id: ID товара.
            price: Цена.
            ad_id: ID объявления (опционально).

        Returns:
            ProductPriceSnapshot: Созданный снимок.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            snapshot = ProductPriceSnapshot(
                product_id=product_id,
                price=price,
                ad_id=ad_id,
            )
            self.session.add(snapshot)
            self.session.flush()

            logger.debug(
                "product_price_snapshot_added",
                product_id=product_id,
                price=price,
                ad_id=ad_id,
            )
            return snapshot
        except SQLAlchemyError as exc:
            logger.error(
                "add_product_price_snapshot_failed",
                product_id=product_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to add product price snapshot: {exc}"
            ) from exc

    def update_product_stats(self, product_id: int) -> None:
        """Пересчитать агрегированную статистику товара.

        Пересчитывает median_price, min_price, max_price, listing_count
        на основе всех снимков цен.

        Args:
            product_id: ID товара.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            # Агрегация по снимкам
            stats_stmt = select(
                func.count(ProductPriceSnapshot.id).label("count"),
                func.percentile_cont(0.5).within_group(
                    ProductPriceSnapshot.price
                ).label("median"),
                func.min(ProductPriceSnapshot.price).label("min_price"),
                func.max(ProductPriceSnapshot.price).label("max_price"),
            ).where(ProductPriceSnapshot.product_id == product_id)

            result = self.session.execute(stats_stmt).one()

            product = self.session.get(Product, product_id)
            if product is None:
                return

            product.listing_count = result.count or 0
            product.median_price = result.median
            product.min_price = result.min_price
            product.max_price = result.max_price
            product.last_seen_at = datetime.datetime.now(datetime.timezone.utc)

            self.session.flush()

            logger.debug(
                "product_stats_updated",
                product_id=product_id,
                count=result.count,
                median=result.median,
            )
        except SQLAlchemyError as exc:
            logger.error(
                "update_product_stats_failed",
                product_id=product_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to update product stats: {exc}"
            ) from exc

    def get_product_price_stats(
        self,
        product_id: int,
        days: int = 30,
    ) -> dict[str, float | int | None]:
        """Получить статистику цен товара за указанный период.

        Args:
            product_id: ID товара.
            days: Количество дней для анализа (по умолчанию 30).

        Returns:
            dict: Статистика с ключами median, min, max, count, p25, p75.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)

            stats_stmt = select(
                func.count(ProductPriceSnapshot.id).label("count"),
                func.percentile_cont(0.5).within_group(
                    ProductPriceSnapshot.price
                ).label("median"),
                func.percentile_cont(0.25).within_group(
                    ProductPriceSnapshot.price
                ).label("p25"),
                func.percentile_cont(0.75).within_group(
                    ProductPriceSnapshot.price
                ).label("p75"),
                func.min(ProductPriceSnapshot.price).label("min_price"),
                func.max(ProductPriceSnapshot.price).label("max_price"),
            ).where(
                ProductPriceSnapshot.product_id == product_id,
                ProductPriceSnapshot.snapshot_at >= cutoff,
            )

            result = self.session.execute(stats_stmt).one()

            return {
                "count": result.count or 0,
                "median": result.median,
                "p25": result.p25,
                "p75": result.p75,
                "min": result.min_price,
                "max": result.max_price,
            }
        except SQLAlchemyError as exc:
            logger.error(
                "get_product_price_stats_failed",
                product_id=product_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get product price stats: {exc}"
            ) from exc

    def get_product_by_key(self, normalized_key: str) -> Product | None:
        """Найти товар по normalized_key.

        Args:
            normalized_key: Нормализованный ключ товара.

        Returns:
            Product | None: Найденный товар или None.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = select(Product).where(
                Product.normalized_key == normalized_key,
            )
            return self.session.execute(stmt).scalars().first()
        except SQLAlchemyError as exc:
            logger.error(
                "get_product_by_key_failed",
                normalized_key=normalized_key,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get product by key: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # SoldItem
    # ------------------------------------------------------------------

    def save_sold_items(self, seller_db_id: int, items: list[dict]) -> int:
        """Сохранить список проданных товаров продавца.

        Для каждого товара проверяет, не сохранён ли уже (по уникальной
        паре ``item_id`` + ``seller_id_fk``). Если ``item_id`` равен
        ``None``, пропускает проверку дубликатов.

        Args:
            seller_db_id: Внутренний ID продавца (``Seller.id``).
            items: Список словарей с данными проданных товаров.
                Ожидаемые ключи: ``item_id``, ``title``, ``price``,
                ``price_str``, ``category``, ``sold_date``, ``item_url``.

        Returns:
            int: Количество новых сохранённых товаров.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        if not items:
            return 0

        try:
            saved_count = 0

            # Собрать item_id для массовой проверки дубликатов
            item_ids = [
                item["item_id"] for item in items
                if item.get("item_id") is not None
            ]

            existing_item_ids: set[str | None] = set()
            if item_ids:
                stmt = (
                    select(SoldItem.item_id)
                    .where(
                        SoldItem.seller_id_fk == seller_db_id,
                        SoldItem.item_id.in_(item_ids),
                    )
                )
                rows = self.session.execute(stmt).scalars().all()
                existing_item_ids = set(rows)

            for item_data in items:
                item_id = item_data.get("item_id")

                # Проверка дубликатов по (seller_id_fk, item_id)
                if item_id is not None and item_id in existing_item_ids:
                    logger.debug(
                        "sold_item_already_exists",
                        item_id=item_id,
                        seller_db_id=seller_db_id,
                    )
                    continue

                sold_item = SoldItem(
                    seller_id_fk=seller_db_id,
                    item_id=item_id,
                    title=item_data.get("title", ""),
                    price=item_data.get("price"),
                    price_str=item_data.get("price_str"),
                    category=item_data.get("category"),
                    sold_date=item_data.get("sold_date"),
                    item_url=item_data.get("item_url"),
                )
                self.session.add(sold_item)
                saved_count += 1

            if saved_count > 0:
                self.session.flush()

            logger.info(
                "sold_items_saved",
                seller_db_id=seller_db_id,
                total_items=len(items),
                new_items=saved_count,
            )
            return saved_count
        except SQLAlchemyError as exc:
            logger.error(
                "save_sold_items_failed",
                seller_db_id=seller_db_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to save sold items: {exc}"
            ) from exc

    def get_sold_items_by_seller(
        self,
        seller_id: str,
        limit: int = 100,
    ) -> list[SoldItem]:
        """Получить проданные товары продавца по Avito seller_id.

        Args:
            seller_id: Строковый ID продавца на Avito.
            limit: Максимум записей.

        Returns:
            list[SoldItem]: Список проданных товаров.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            stmt = (
                select(SoldItem)
                .join(Seller, SoldItem.seller_id_fk == Seller.id)
                .where(Seller.seller_id == seller_id)
                .order_by(SoldItem.created_at.desc())
                .limit(limit)
            )
            results = self.session.execute(stmt).scalars().all()
            logger.debug(
                "sold_items_fetched_by_seller",
                seller_id=seller_id,
                count=len(results),
            )
            return list(results)
        except SQLAlchemyError as exc:
            logger.error(
                "get_sold_items_by_seller_failed",
                seller_id=seller_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get sold items by seller: {exc}"
            ) from exc

    def get_seller_sold_stats(self, seller_id: str) -> dict | None:
        """Получить агрегированную статистику продаж продавца.

        Рассчитывает статистику по проданным товарам продавца:
        количество, средняя/мин/макс/медианная цена, список категорий.

        Args:
            seller_id: Строковый ID продавца на Avito.

        Returns:
            dict | None: Словарь со статистикой или ``None``,
                если продавец не найден. Поля:
                ``total_sold``, ``avg_price``, ``min_price``,
                ``max_price``, ``median_price``, ``categories``.

        Raises:
            StorageError: Ошибка при работе с БД.
        """
        try:
            # Проверяем существование продавца
            seller_stmt = select(Seller).where(Seller.seller_id == seller_id)
            seller = self.session.execute(seller_stmt).scalar_one_or_none()
            if seller is None:
                logger.debug(
                    "seller_not_found_for_stats",
                    seller_id=seller_id,
                )
                return None

            # Агрегатный запрос (медиана через percentile_cont на стороне БД)
            stmt = (
                select(
                    func.count(SoldItem.id).label("total_sold"),
                    func.avg(SoldItem.price).label("avg_price"),
                    func.min(SoldItem.price).label("min_price"),
                    func.max(SoldItem.price).label("max_price"),
                    func.percentile_cont(0.5).within_group(
                        SoldItem.price
                    ).label("median_price"),
                )
                .where(SoldItem.seller_id_fk == seller.id)
                .where(SoldItem.price.is_not(None))
            )
            row = self.session.execute(stmt).one()

            # Уникальные категории
            categories_stmt = (
                select(SoldItem.category)
                .where(SoldItem.seller_id_fk == seller.id)
                .where(SoldItem.category.is_not(None))
                .distinct()
            )
            categories = list(
                self.session.execute(categories_stmt).scalars().all()
            )

            stats = {
                "total_sold": row.total_sold or 0,
                "avg_price": float(row.avg_price) if row.avg_price is not None else None,
                "min_price": float(row.min_price) if row.min_price is not None else None,
                "max_price": float(row.max_price) if row.max_price is not None else None,
                "median_price": float(row.median_price) if row.median_price is not None else None,
                "categories": categories,
            }

            logger.debug(
                "seller_sold_stats_fetched",
                seller_id=seller_id,
                total_sold=stats["total_sold"],
            )
            return stats
        except SQLAlchemyError as exc:
            logger.error(
                "get_seller_sold_stats_failed",
                seller_id=seller_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get seller sold stats: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Common
    # ------------------------------------------------------------------

    def commit(self) -> None:
        """Фиксирует текущую транзакцию.

        Raises:
            StorageError: Ошибка при коммите.
        """
        try:
            self.session.commit()
            logger.debug("transaction_committed")
        except SQLAlchemyError as exc:
            logger.error("commit_failed", error=str(exc))
            raise StorageError(f"Failed to commit: {exc}") from exc

    def rollback(self) -> None:
        """Откатывает текущую транзакцию.

        Raises:
            StorageError: Ошибка при откате.
        """
        try:
            self.session.rollback()
            logger.debug("transaction_rolled_back")
        except SQLAlchemyError as exc:
            logger.error("rollback_failed", error=str(exc))
            raise StorageError(f"Failed to rollback: {exc}") from exc

    def close(self) -> None:
        """Закрывает сессию.

        Raises:
            StorageError: Ошибка при закрытии.
        """
        try:
            self.session.close()
            logger.debug("session_closed")
        except SQLAlchemyError as exc:
            logger.error("close_failed", error=str(exc))
            raise StorageError(f"Failed to close session: {exc}") from exc
