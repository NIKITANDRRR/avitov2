"""SQLAlchemy-модели для хранения данных Avito Monitor."""

from __future__ import annotations

import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.storage.database import Base


def _utcnow() -> datetime.datetime:
    """Возвращает текущее UTC-время."""
    return datetime.datetime.utcnow()


# ---------------------------------------------------------------------------
# TrackedSearch — отслеживаемый поисковый запрос
# ---------------------------------------------------------------------------

class TrackedSearch(Base):
    """Отслеживаемый поисковый запрос Avito.

    Attributes:
        id: Первичный ключ.
        search_url: URL поисковой выдачи Avito.
        search_phrase: Человекочитаемое название/описание поиска.
        is_active: Флаг активности поиска.
        created_at: Дата-время создания записи.
        updated_at: Дата-время последнего обновления записи.
        runs: Связанные запуски поиска (one-to-many).
    """

    __tablename__ = "tracked_searches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    search_url: Mapped[str] = mapped_column(
        String(2048), unique=True, nullable=False, index=True,
    )
    search_phrase: Mapped[str | None] = mapped_column(String(512), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=_utcnow,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=_utcnow, onupdate=_utcnow,
    )

    # Relationships
    runs: Mapped[list[SearchRun]] = relationship(
        "SearchRun",
        back_populates="tracked_search",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    def __repr__(self) -> str:
        return (
            f"<TrackedSearch id={self.id} url={self.search_url!r} "
            f"active={self.is_active}>"
        )


# ---------------------------------------------------------------------------
# SearchRun — один запуск сбора по поиску
# ---------------------------------------------------------------------------

class SearchRun(Base):
    """Один запуск сбора объявлений по отслеживаемому поиску.

    Attributes:
        id: Первичный ключ.
        tracked_search_id: FK на TrackedSearch.
        started_at: Дата-время начала запуска.
        completed_at: Дата-время завершения запуска.
        status: Статус запуска (running/completed/failed).
        ads_found: Количество найденных объявлений.
        ads_new: Количество новых объявлений.
        pages_fetched: Количество загруженных страниц.
        ads_opened: Количество открытых объявлений.
        errors_count: Количество ошибок.
        error_message: Текст ошибки (при наличии).
        tracked_search: Связанный объект TrackedSearch.
    """

    __tablename__ = "search_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tracked_search_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("tracked_searches.id", ondelete="CASCADE"),
        nullable=False,
    )
    started_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False)
    completed_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, nullable=True,
    )
    status: Mapped[str] = mapped_column(String(20), default="running")
    ads_found: Mapped[int] = mapped_column(Integer, default=0)
    ads_new: Mapped[int] = mapped_column(Integer, default=0)
    pages_fetched: Mapped[int] = mapped_column(Integer, default=0)
    ads_opened: Mapped[int] = mapped_column(Integer, default=0)
    errors_count: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Relationships
    tracked_search: Mapped[TrackedSearch] = relationship(
        "TrackedSearch", back_populates="runs",
    )

    def __repr__(self) -> str:
        return (
            f"<SearchRun id={self.id} search_id={self.tracked_search_id} "
            f"status={self.status!r}>"
        )


# ---------------------------------------------------------------------------
# Ad — объявление Avito
# ---------------------------------------------------------------------------

class Ad(Base):
    """Объявление Avito.

    Attributes:
        id: Первичный ключ.
        ad_id: Идентификатор объявления Avito (извлечённый из URL).
        url: Полный URL объявления.
        title: Заголовок объявления.
        price: Цена.
        location: Местоположение.
        seller_name: Имя продавца.
        condition: Состояние товара.
        publication_date: Дата публикации объявления.
        search_url: URL поиска, откуда найдено объявление.
        first_seen_at: Дата-время первого обнаружения.
        last_scraped_at: Дата-время последнего скрейпинга.
        is_undervalued: Флаг недооценённости.
        undervalue_score: Отклонение от медианы (например -0.15 = 15% ниже).
        parse_status: Статус парсинга (pending/parsed/failed).
        last_error: Текст последней ошибки парсинга.
        snapshots: Связанные снимки цен (one-to-many).
        notifications: Связанные отправленные уведомления (one-to-many).
    """

    __tablename__ = "ads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ad_id: Mapped[str] = mapped_column(
        String(64), unique=True, nullable=False, index=True,
    )
    url: Mapped[str] = mapped_column(
        String(2048), unique=True, nullable=False,
    )
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    location: Mapped[str | None] = mapped_column(String(256), nullable=True)
    seller_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    condition: Mapped[str | None] = mapped_column(String(128), nullable=True)
    publication_date: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, nullable=True,
    )
    search_url: Mapped[str | None] = mapped_column(
        String(2048), nullable=True, index=True,
    )
    first_seen_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=_utcnow,
    )
    last_scraped_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=_utcnow,
    )
    is_undervalued: Mapped[bool] = mapped_column(Boolean, default=False)
    undervalue_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    parse_status: Mapped[str] = mapped_column(String(20), default="pending")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Relationships
    snapshots: Mapped[list[AdSnapshot]] = relationship(
        "AdSnapshot",
        back_populates="ad",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    notifications: Mapped[list[NotificationSent]] = relationship(
        "NotificationSent",
        back_populates="ad",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    def __repr__(self) -> str:
        return (
            f"<Ad id={self.id} ad_id={self.ad_id!r} "
            f"title={self.title!r} parse_status={self.parse_status!r}>"
        )


# ---------------------------------------------------------------------------
# AdSnapshot — снимок цены объявления
# ---------------------------------------------------------------------------

class AdSnapshot(Base):
    """Снимок цены объявления на момент скрейпинга.

    Attributes:
        id: Первичный ключ.
        ad_id: FK на Ad.
        price: Зафиксированная цена.
        scraped_at: Дата-время снятия снимка.
        html_path: Путь к файлу с HTML на диске.
        ad: Связанный объект Ad.
    """

    __tablename__ = "ad_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ad_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ads.id", ondelete="CASCADE"),
        nullable=False,
    )
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    scraped_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=_utcnow,
    )
    html_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)

    # Relationships
    ad: Mapped[Ad] = relationship("Ad", back_populates="snapshots")

    def __repr__(self) -> str:
        return (
            f"<AdSnapshot id={self.id} ad_id={self.ad_id} "
            f"price={self.price} scraped_at={self.scraped_at}>"
        )


# ---------------------------------------------------------------------------
# NotificationSent — запись об отправленном уведомлении
# ---------------------------------------------------------------------------

class NotificationSent(Base):
    """Запись об отправленном уведомлении.

    Attributes:
        id: Первичный ключ.
        ad_id: FK на Ad.
        notification_type: Тип уведомления (например telegram_undervalued).
        sent_at: Дата-время отправки.
        telegram_message_id: ID сообщения в Telegram.
        ad: Связанный объект Ad.
    """

    __tablename__ = "notifications_sent"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ad_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ads.id", ondelete="CASCADE"),
        nullable=False,
    )
    notification_type: Mapped[str] = mapped_column(
        String(50), default="telegram_undervalued",
    )
    sent_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=_utcnow,
    )
    telegram_message_id: Mapped[str | None] = mapped_column(
        String(128), nullable=True,
    )

    # Relationships
    ad: Mapped[Ad] = relationship("Ad", back_populates="notifications")

    def __repr__(self) -> str:
        return (
            f"<NotificationSent id={self.id} ad_id={self.ad_id} "
            f"type={self.notification_type!r}>"
        )
