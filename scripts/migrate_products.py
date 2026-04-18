"""Миграция: создание таблиц products и product_price_snapshots.

Запуск:
    python -m scripts.migrate_products
"""

from __future__ import annotations

import structlog

from app.storage.database import Base, get_engine
from app.storage.models import Product, ProductPriceSnapshot  # noqa: F401

logger = structlog.get_logger("migrate_products")


def main() -> None:
    """Создать таблицы products и product_price_snapshots."""
    engine = get_engine()

    logger.info("Creating tables: products, product_price_snapshots")

    # Получаем объекты Table из метаданных
    tables = [
        Base.metadata.tables["products"],
        Base.metadata.tables["product_price_snapshots"],
    ]

    Base.metadata.create_all(engine, tables=tables)

    logger.info("Migration complete: products + product_price_snapshots created")


if __name__ == "__main__":
    main()
