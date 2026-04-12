"""Repository pattern — доступ к данным Avito Monitor через SQLAlchemy."""

from __future__ import annotations

import datetime

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

import structlog

from app.utils.exceptions import StorageError
from app.storage.models import (
    Ad,
    AdSnapshot,
    NotificationSent,
    SearchRun,
    TrackedSearch,
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
                started_at=datetime.datetime.utcnow(),
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
            run.completed_at = datetime.datetime.utcnow()
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
            run.completed_at = datetime.datetime.utcnow()
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
            self.session.flush()
            logger.info(
                "ad_created",
                ad_id=ad_id,
                db_id=ad.id,
                search_url=search_url,
            )
            return ad, True
        except SQLAlchemyError as exc:
            logger.error(
                "get_or_create_ad_failed",
                ad_id=ad_id,
                error=str(exc),
            )
            raise StorageError(
                f"Failed to get or create ad: {exc}"
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
            ad.last_scraped_at = datetime.datetime.utcnow()

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
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=hours)
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
                .where(NotificationSent.ad_id == ad_id)
                .where(NotificationSent.notification_type == notification_type)
            )
            result = self.session.execute(stmt).scalar_one_or_none()
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
        try:
            notification = NotificationSent(
                ad_id=ad_id,
                notification_type=notification_type,
                telegram_message_id=telegram_message_id,
            )
            self.session.add(notification)
            self.session.flush()
            logger.info(
                "notification_marked_sent",
                ad_id=ad_id,
                notification_type=notification_type,
                telegram_message_id=telegram_message_id,
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
