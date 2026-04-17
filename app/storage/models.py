"""SQLAlchemy-модели для хранения данных Avito Monitor."""

from __future__ import annotations

import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DynamicMapped, Mapped, mapped_column, relationship

from app.storage.database import Base


def _utcnow() -> datetime.datetime:
    """Возвращает текущее UTC-время (timezone-aware)."""
    return datetime.datetime.now(datetime.timezone.utc)


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
        schedule_interval_hours: Интервал запуска в часах.
        last_run_at: Время последнего запуска.
        priority: Приоритет поиска (выше = важнее).
        max_ads_to_parse: Сколько карточек парсить за один запуск.
        created_at: Дата-время создания записи.
        updated_at: Дата-время последнего обновления записи.
        category: Категория поиска (для категорийного мониторинга).
        is_category_search: Флаг категорийного поиска.
        runs: Связанные запуски поиска (one-to-many).
    """

    __tablename__ = "tracked_searches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    search_url: Mapped[str] = mapped_column(
        String(2048), unique=True, nullable=False, index=True,
    )
    search_phrase: Mapped[str | None] = mapped_column(String(512), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    schedule_interval_hours: Mapped[float] = mapped_column(
        Float, default=0.5, nullable=False,
    )
    last_run_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    priority: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_ads_to_parse: Mapped[int] = mapped_column(
        Integer, default=3, nullable=False,
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow,
    )
    category: Mapped[str | None] = mapped_column(String(256), nullable=True)
    is_category_search: Mapped[bool] = mapped_column(Boolean, default=False)

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
    started_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
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
        seller_type: Тип продавца («частный», «магазин», «компания» и т.д.).
        condition: Состояние товара.
        publication_date: Дата публикации объявления.
        search_url: URL поиска, откуда найдено объявление.
        first_seen_at: Дата-время первого обнаружения.
        last_scraped_at: Дата-время последнего скрейпинга.
        is_undervalued: Флаг недооценённости.
        undervalue_score: Отклонение от медианы (например -0.15 = 15% ниже).
        z_score: Z-score цена относительно сегмента.
        iqr_outlier: Является ли выбросом по IQR.
        segment_key: Ключ сегмента вида «{condition}_{location}_{seller_type}».
        parse_status: Статус парсинга (pending/parsed/failed).
        last_error: Текст последней ошибки парсинга.
        last_seen_at: Когда объявление последний раз замечено.
        days_on_market: Количество дней на рынке.
        is_disappeared_quickly: Быстро ли исчезло.
        ad_category: Категория объявления.
        brand: Бренд товара.
        extracted_model: Извлечённая модель товара.
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
    seller_id_fk: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("sellers.id"),
        nullable=True,
        index=True,
    )
    seller_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    condition: Mapped[str | None] = mapped_column(String(128), nullable=True)
    publication_date: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    search_url: Mapped[str | None] = mapped_column(
        String(2048), nullable=True, index=True,
    )
    first_seen_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow,
    )
    last_scraped_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow,
    )
    is_undervalued: Mapped[bool] = mapped_column(Boolean, default=False)
    undervalue_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    z_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    iqr_outlier: Mapped[bool] = mapped_column(Boolean, default=False)
    segment_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    parse_status: Mapped[str] = mapped_column(String(20), default="pending")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_seen_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    days_on_market: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_disappeared_quickly: Mapped[bool] = mapped_column(Boolean, default=False)
    ad_category: Mapped[str | None] = mapped_column(String(256), nullable=True)
    brand: Mapped[str | None] = mapped_column(String(256), nullable=True)
    extracted_model: Mapped[str | None] = mapped_column(String(256), nullable=True)

    # Relationships
    seller: Mapped[Seller | None] = relationship("Seller", back_populates="ads")
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
# Seller — продавец Avito
# ---------------------------------------------------------------------------

class Seller(Base):
    """Профиль продавца Avito.

    Attributes:
        id: Первичный ключ.
        seller_id: Строковый ID продавца на Avito (извлечённый из URL).
        seller_url: URL профиля продавца.
        seller_name: Имя продавца.
        rating: Рейтинг продавца.
        reviews_count: Количество отзывов.
        total_sold_items: Общее кол-во проданных товаров (с сайта).
        first_seen_at: Когда впервые обнаружен.
        last_scraped_at: Когда последний раз парсили профиль.
        scrape_status: Статус парсинга (pending/scraped/failed).
        created_at: Дата-время создания записи.
        updated_at: Дата-время последнего обновления записи.
        ads: Связанные объявления (one-to-many).
        sold_items: Связанные проданные товары (one-to-many).
    """

    __tablename__ = "sellers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    seller_id: Mapped[str] = mapped_column(
        String(100), unique=True, nullable=False, index=True,
    )
    seller_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    seller_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    reviews_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_sold_items: Mapped[int | None] = mapped_column(Integer, nullable=True)
    first_seen_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow,
    )
    last_scraped_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    scrape_status: Mapped[str] = mapped_column(String(50), default="pending")
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow,
    )

    # Relationships
    ads: DynamicMapped[Ad] = relationship("Ad", back_populates="seller", lazy="dynamic")
    sold_items: DynamicMapped[SoldItem] = relationship(
        "SoldItem",
        back_populates="seller",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="dynamic",
    )

    __table_args__ = (
        CheckConstraint(
            "scrape_status IN ('pending', 'scraped', 'failed')",
            name="ck_seller_scrape_status",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<Seller id={self.id} seller_id={self.seller_id!r} "
            f"name={self.seller_name!r}>"
        )


# ---------------------------------------------------------------------------
# SoldItem — проданный товар продавца
# ---------------------------------------------------------------------------

class SoldItem(Base):
    """Проданный товар продавца Avito.

    Attributes:
        id: Первичный ключ.
        seller_id_fk: FK на Seller.
        item_id: ID товара на Avito (если доступен).
        title: Название проданного товара.
        price: Цена продажи.
        price_str: Сырая строка цены.
        category: Категория товара.
        sold_date: Дата продажи (если доступна).
        item_url: URL товара.
        scraped_at: Когда спарсено.
        created_at: Дата-время создания записи.
        seller: Связанный объект Seller.
    """

    __tablename__ = "sold_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    seller_id_fk: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("sellers.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    item_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_str: Mapped[str | None] = mapped_column(String(100), nullable=True)
    category: Mapped[str | None] = mapped_column(String(255), nullable=True)
    sold_date: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    item_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    scraped_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow,
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow,
    )

    # Relationships
    seller: Mapped[Seller] = relationship("Seller", back_populates="sold_items")

    def __repr__(self) -> str:
        return (
            f"<SoldItem id={self.id} title={self.title!r} "
            f"price={self.price}>"
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
        DateTime(timezone=True), default=_utcnow,
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
    __table_args__ = (
        UniqueConstraint("ad_id", "notification_type", name="uq_notification_ad_type"),
    )

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
        DateTime(timezone=True), default=_utcnow,
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


# ---------------------------------------------------------------------------
# SegmentStats — статистика сегмента
# ---------------------------------------------------------------------------

class SegmentStats(Base):
    """Статистика ценового сегмента для отслеживаемого поиска.

    Attributes:
        id: Первичный ключ.
        search_id: FK на TrackedSearch.
        segment_key: Ключ сегмента (например «brand:model» или «category:subcategory»).
        segment_name: Человекочитаемое название сегмента.
        median_7d: Медианная цена за 7 дней.
        median_30d: Медианная цена за 30 дней (основная метрика).
        median_90d: Медианная цена за 90 дней.
        mean_price: Средняя цена.
        min_price: Минимальная цена.
        max_price: Максимальная цена.
        price_trend_slope: Наклон тренда цены.
        sample_size: Размер выборки.
        listing_count: Количество активных объявлений.
        appearance_count_90d: Сколько раз товар появлялся за 90 дней.
        median_days_on_market: Медиана дней на рынке.
        listing_price_median: Медиана по активным объявлениям.
        fast_sale_price_median: Медиана цен быстрых продаж.
        liquid_market_estimate: Оценка ликвидной цены.
        is_rare_segment: Признак редкого сегмента.
        calculated_at: Дата-время расчёта.
        updated_at: Дата-время последнего обновления.
        search: Связанный объект TrackedSearch.
    """

    __tablename__ = "segment_stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    search_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("tracked_searches.id", ondelete="CASCADE"),
        nullable=False,
    )
    segment_key: Mapped[str] = mapped_column(String, nullable=False)
    segment_name: Mapped[str | None] = mapped_column(String, nullable=True)
    category: Mapped[str] = mapped_column(String(128), nullable=False, default="unknown")
    brand: Mapped[str] = mapped_column(String(256), nullable=False, default="unknown")
    model: Mapped[str] = mapped_column(String(256), nullable=False, default="unknown")
    condition: Mapped[str] = mapped_column(String(128), nullable=False, default="unknown")
    location: Mapped[str] = mapped_column(String(256), nullable=False, default="unknown")
    seller_type: Mapped[str] = mapped_column(String(128), nullable=False, default="unknown")

    # Ценовые метрики
    median_7d: Mapped[float | None] = mapped_column(Float, nullable=True)
    median_30d: Mapped[float | None] = mapped_column(Float, nullable=True)
    median_90d: Mapped[float | None] = mapped_column(Float, nullable=True)
    mean_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    min_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_price: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Тренд
    price_trend_slope: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Объём
    sample_size: Mapped[int] = mapped_column(Integer, default=0)
    listing_count: Mapped[int] = mapped_column(Integer, default=0)

    # Оборачиваемость
    appearance_count_90d: Mapped[int] = mapped_column(Integer, default=0)
    median_days_on_market: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Двухуровневая цена
    listing_price_median: Mapped[float | None] = mapped_column(Float, nullable=True)
    fast_sale_price_median: Mapped[float | None] = mapped_column(Float, nullable=True)
    liquid_market_estimate: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Редкость
    is_rare_segment: Mapped[bool] = mapped_column(Boolean, default=False)

    # Метаданные
    calculated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow,
    )

    # Связь
    search: Mapped[TrackedSearch] = relationship(
        "TrackedSearch", backref="segment_stats",
    )

    # Уникальный индекс
    __table_args__ = (
        UniqueConstraint("search_id", "segment_key", name="uq_segment_stats_search_key"),
        Index("ix_segment_stats_search_id", "search_id"),
        Index("ix_segment_stats_segment_key", "segment_key"),
    )

    def __repr__(self) -> str:
        return (
            f"<SegmentStats id={self.id} search_id={self.search_id} "
            f"segment_key={self.segment_key!r}>"
        )


# ---------------------------------------------------------------------------
# SegmentPriceHistory — история цен сегмента
# ---------------------------------------------------------------------------

class SegmentPriceHistory(Base):
    """История цен сегмента по дням.

    Attributes:
        id: Первичный ключ.
        segment_stats_id: FK на SegmentStats.
        snapshot_date: Дата снапшота.
        median_price: Медианная цена на дату.
        mean_price: Средняя цена на дату.
        min_price: Минимальная цена на дату.
        max_price: Максимальная цена на дату.
        sample_size: Размер выборки на дату.
        listing_count: Количество объявлений на дату.
        fast_sale_count: Количество быстрых продаж на дату.
        median_days_on_market: Медиана дней на рынке на дату.
        created_at: Дата-время создания записи.
        segment_stats: Связанный объект SegmentStats.
    """

    __tablename__ = "segment_price_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    segment_stats_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("segment_stats.id", ondelete="CASCADE"),
        nullable=False,
    )
    snapshot_date: Mapped[datetime.date] = mapped_column(Date, nullable=False)

    # Цены на дату
    median_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    mean_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    min_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    sample_size: Mapped[int] = mapped_column(Integer, default=0)

    # Оборачиваемость на дату
    listing_count: Mapped[int] = mapped_column(Integer, default=0)
    fast_sale_count: Mapped[int] = mapped_column(Integer, default=0)
    median_days_on_market: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Метаданные
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow,
    )

    # Связь
    segment_stats: Mapped[SegmentStats] = relationship(
        "SegmentStats", backref="price_history",
    )

    # Уникальный индекс — один снапшот в день на сегмент
    __table_args__ = (
        UniqueConstraint(
            "segment_stats_id", "snapshot_date",
            name="uq_segment_price_history_stats_date",
        ),
        Index("ix_segment_price_history_snapshot_date", "snapshot_date"),
    )

    def __repr__(self) -> str:
        return (
            f"<SegmentPriceHistory id={self.id} "
            f"segment_stats_id={self.segment_stats_id} "
            f"date={self.snapshot_date}>"
        )
