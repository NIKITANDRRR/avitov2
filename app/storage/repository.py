"""Repository pattern — доступ к данным Avito Monitor через SQLAlchemy."""

from __future__ import annotations

import datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
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
            try:
                self.session.flush()
            except IntegrityError:
                # Race condition: другой параллельный запрос уже вставил эту запись
                self.session.rollback()
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

        try:
            notification = NotificationSent(
                ad_id=ad_id,
                notification_type=notification_type,
                telegram_message_id=telegram_message_id,
            )
            self.session.add(notification)
            self.session.flush()
            # Немедленный коммит для гарантии, что запись NotificationSent
            # зафиксирована в БД до фактической отправки уведомления.
            self.session.commit()
            logger.info(
                "notification_marked_sent",
                ad_id=ad_id,
                notification_type=notification_type,
                telegram_message_id=telegram_message_id,
            )
        except IntegrityError:
            # Дубль на уровне БД — откатываем и игнорируем
            self.session.rollback()
            logger.debug(
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
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days)
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
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days)
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
            now = datetime.datetime.utcnow()
            stmt = (
                select(TrackedSearch)
                .where(TrackedSearch.is_active.is_(True))
                .order_by(TrackedSearch.priority.desc())
            )
            active = self.session.execute(stmt).scalars().all()

            due = []
            for search in active:
                if search.last_run_at is None:
                    due.append(search)
                else:
                    interval = datetime.timedelta(
                        hours=search.schedule_interval_hours,
                    )
                    if now >= search.last_run_at + interval:
                        due.append(search)

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

            tracked.last_run_at = datetime.datetime.utcnow()
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
