"""Миграция БД: добавление поддержки продавцов и проданных товаров.

Создаёт таблицы:
    - sellers — профиль продавца Avito
    - sold_items — проданные товары продавца

Добавляет колонку:
    - ads.seller_id_fk — FK на sellers.id (nullable)

Запуск:
    python -m scripts.migrate_seller_sold_items
"""

from __future__ import annotations

import sys
from pathlib import Path

# Добавляем корень проекта в sys.path, чтобы работали импорты app.*
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import sqlalchemy
from sqlalchemy import text

from app.storage.database import get_engine


MIGRATION_STATEMENTS: list[str] = [
    """CREATE TABLE IF NOT EXISTS sellers (
        id SERIAL PRIMARY KEY,
        seller_id VARCHAR(100) UNIQUE NOT NULL,
        seller_url VARCHAR(500),
        seller_name VARCHAR(255),
        rating FLOAT,
        reviews_count INTEGER,
        total_sold_items INTEGER,
        first_seen_at TIMESTAMP DEFAULT NOW(),
        last_scraped_at TIMESTAMP,
        scrape_status VARCHAR(50) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    )""",
    "CREATE INDEX IF NOT EXISTS ix_sellers_seller_id ON sellers(seller_id)",
    """CREATE TABLE IF NOT EXISTS sold_items (
        id SERIAL PRIMARY KEY,
        seller_id_fk INTEGER NOT NULL REFERENCES sellers(id),
        item_id VARCHAR(100),
        title VARCHAR(500) NOT NULL,
        price FLOAT,
        price_str VARCHAR(100),
        category VARCHAR(255),
        sold_date TIMESTAMP,
        item_url VARCHAR(500),
        scraped_at TIMESTAMP DEFAULT NOW(),
        created_at TIMESTAMP DEFAULT NOW()
    )""",
    "CREATE INDEX IF NOT EXISTS ix_sold_items_seller_id_fk ON sold_items(seller_id_fk)",
    "ALTER TABLE ads ADD COLUMN IF NOT EXISTS seller_id_fk INTEGER REFERENCES sellers(id)",
    "CREATE INDEX IF NOT EXISTS ix_ads_seller_id_fk ON ads(seller_id_fk)",
]


def run_migration() -> None:
    """Применить миграцию продавцов и проданных товаров."""
    print("Начало миграции: продавцы и проданные товары...")

    engine = get_engine()

    with engine.connect() as conn:
        for i, stmt in enumerate(MIGRATION_STATEMENTS, 1):
            try:
                conn.execute(text(stmt))
                conn.commit()
                print(f"  [{i}/{len(MIGRATION_STATEMENTS)}] OK")
            except sqlalchemy.exc.ProgrammingError as exc:
                # Индекс или колонка уже существуют — не ошибка
                if "already exists" in str(exc).lower():
                    print(f"  [{i}/{len(MIGRATION_STATEMENTS)}] SKIP (already exists)")
                else:
                    print(f"  [{i}/{len(MIGRATION_STATEMENTS)}] ERROR: {exc}")
                    raise

    print("Миграция завершена успешно!")


if __name__ == "__main__":
    run_migration()
