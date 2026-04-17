#!/usr/bin/env python3
"""Миграция: Добавление сегментного анализа и трекинга оборачиваемости (PostgreSQL).

Добавляет:
- Новые колонки в tracked_searches: category, is_category_search
- Новые колонки в ads: first_seen_at, last_seen_at, days_on_market,
  is_disappeared_quickly, ad_category, brand, extracted_model
- Новые таблицы: segment_stats, segment_price_history
- Инициализацию first_seen_at для существующих объявлений

Запуск:
    python -m scripts.migrate_segment_analysis --up
    python -m scripts.migrate_segment_analysis --up --dry-run
    python -m scripts.migrate_segment_analysis --down
    python -m scripts.migrate_segment_analysis --up --db-path postgresql://user:pass@host:5432/db
"""

from __future__ import annotations

import argparse
import os
import sys

# Добавляем корень проекта в путь
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def get_db_url() -> str:
    """Получает URL подключения к БД из настроек."""
    from app.config.settings import Settings

    return Settings().DATABASE_URL


def get_connection(db_url: str):
    """Создаёт подключение к PostgreSQL в режиме AUTOCOMMIT."""
    conn = psycopg2.connect(db_url)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


# ---------------------------------------------------------------------------
# SQL-выражения для миграции (PostgreSQL)
# ---------------------------------------------------------------------------

ALTER_TRACKED_SEARCHES = [
    {
        "sql": "ALTER TABLE tracked_searches ADD COLUMN IF NOT EXISTS category VARCHAR(256)",
        "description": "tracked_searches.category",
    },
    {
        "sql": "ALTER TABLE tracked_searches ADD COLUMN IF NOT EXISTS is_category_search BOOLEAN DEFAULT FALSE",
        "description": "tracked_searches.is_category_search",
    },
]

ALTER_ADS = [
    {
        "sql": "ALTER TABLE ads ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP",
        "description": "ads.first_seen_at",
    },
    {
        "sql": "ALTER TABLE ads ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP",
        "description": "ads.last_seen_at",
    },
    {
        "sql": "ALTER TABLE ads ADD COLUMN IF NOT EXISTS days_on_market INTEGER",
        "description": "ads.days_on_market",
    },
    {
        "sql": "ALTER TABLE ads ADD COLUMN IF NOT EXISTS is_disappeared_quickly BOOLEAN DEFAULT FALSE",
        "description": "ads.is_disappeared_quickly",
    },
    {
        "sql": "ALTER TABLE ads ADD COLUMN IF NOT EXISTS ad_category VARCHAR(256)",
        "description": "ads.ad_category",
    },
    {
        "sql": "ALTER TABLE ads ADD COLUMN IF NOT EXISTS brand VARCHAR(256)",
        "description": "ads.brand",
    },
    {
        "sql": "ALTER TABLE ads ADD COLUMN IF NOT EXISTS extracted_model VARCHAR(256)",
        "description": "ads.extracted_model",
    },
]

CREATE_SEGMENT_STATS = """
CREATE TABLE segment_stats (
    id SERIAL PRIMARY KEY,
    search_id INTEGER NOT NULL REFERENCES tracked_searches(id),
    segment_key VARCHAR NOT NULL,
    segment_name VARCHAR,
    category VARCHAR(128) NOT NULL DEFAULT 'unknown',
    brand VARCHAR(256) NOT NULL DEFAULT 'unknown',
    model VARCHAR(256) NOT NULL DEFAULT 'unknown',
    condition VARCHAR(128) NOT NULL DEFAULT 'unknown',
    location VARCHAR(256) NOT NULL DEFAULT 'unknown',
    seller_type VARCHAR(128) NOT NULL DEFAULT 'unknown',
    median_7d FLOAT,
    median_30d FLOAT,
    median_90d FLOAT,
    mean_price FLOAT,
    min_price FLOAT,
    max_price FLOAT,
    price_trend_slope FLOAT,
    sample_size INTEGER DEFAULT 0,
    listing_count INTEGER DEFAULT 0,
    appearance_count_90d INTEGER DEFAULT 0,
    median_days_on_market FLOAT,
    listing_price_median FLOAT,
    fast_sale_price_median FLOAT,
    liquid_market_estimate FLOAT,
    is_rare_segment BOOLEAN DEFAULT FALSE,
    calculated_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_segment_stats_search_key UNIQUE (search_id, segment_key)
)
"""

# Колонки segment_stats для ALTER TABLE (порядок важен: id пропускаем)
SEGMENT_STATS_COLUMNS = {
    "search_id": "INTEGER NOT NULL REFERENCES tracked_searches(id)",
    "segment_key": "VARCHAR NOT NULL",
    "segment_name": "VARCHAR",
    "category": "VARCHAR(128) NOT NULL DEFAULT 'unknown'",
    "brand": "VARCHAR(256) NOT NULL DEFAULT 'unknown'",
    "model": "VARCHAR(256) NOT NULL DEFAULT 'unknown'",
    "condition": "VARCHAR(128) NOT NULL DEFAULT 'unknown'",
    "location": "VARCHAR(256) NOT NULL DEFAULT 'unknown'",
    "seller_type": "VARCHAR(128) NOT NULL DEFAULT 'unknown'",
    "median_7d": "FLOAT",
    "median_30d": "FLOAT",
    "median_90d": "FLOAT",
    "mean_price": "FLOAT",
    "min_price": "FLOAT",
    "max_price": "FLOAT",
    "price_trend_slope": "FLOAT",
    "sample_size": "INTEGER DEFAULT 0",
    "listing_count": "INTEGER DEFAULT 0",
    "appearance_count_90d": "INTEGER DEFAULT 0",
    "median_days_on_market": "FLOAT",
    "listing_price_median": "FLOAT",
    "fast_sale_price_median": "FLOAT",
    "liquid_market_estimate": "FLOAT",
    "is_rare_segment": "BOOLEAN DEFAULT FALSE",
    "calculated_at": "TIMESTAMP DEFAULT NOW()",
    "updated_at": "TIMESTAMP DEFAULT NOW()",
}

CREATE_SEGMENT_STATS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS ix_segment_stats_search_id ON segment_stats(search_id)",
    "CREATE INDEX IF NOT EXISTS ix_segment_stats_segment_key ON segment_stats(segment_key)",
]

CREATE_SEGMENT_PRICE_HISTORY = """
CREATE TABLE segment_price_history (
    id SERIAL PRIMARY KEY,
    segment_stats_id INTEGER NOT NULL REFERENCES segment_stats(id),
    snapshot_date DATE NOT NULL,
    median_price FLOAT,
    mean_price FLOAT,
    min_price FLOAT,
    max_price FLOAT,
    sample_size INTEGER DEFAULT 0,
    listing_count INTEGER DEFAULT 0,
    fast_sale_count INTEGER DEFAULT 0,
    median_days_on_market FLOAT,
    created_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_segment_price_history_stats_date UNIQUE (segment_stats_id, snapshot_date)
)
"""

# Колонки segment_price_history для ALTER TABLE
SEGMENT_PRICE_HISTORY_COLUMNS = {
    "segment_stats_id": "INTEGER NOT NULL REFERENCES segment_stats(id)",
    "snapshot_date": "DATE NOT NULL",
    "median_price": "FLOAT",
    "mean_price": "FLOAT",
    "min_price": "FLOAT",
    "max_price": "FLOAT",
    "sample_size": "INTEGER DEFAULT 0",
    "listing_count": "INTEGER DEFAULT 0",
    "fast_sale_count": "INTEGER DEFAULT 0",
    "median_days_on_market": "FLOAT",
    "created_at": "TIMESTAMP DEFAULT NOW()",
}

CREATE_SEGMENT_PRICE_HISTORY_INDEXES = [
    (
        "CREATE INDEX IF NOT EXISTS ix_segment_price_history_snapshot_date "
        "ON segment_price_history(snapshot_date)"
    ),
]

UPDATE_FIRST_SEEN = (
    "UPDATE ads SET first_seen_at = last_scraped_at "
    "WHERE first_seen_at IS NULL AND last_scraped_at IS NOT NULL"
)

DOWN_DROP_TABLES = [
    "DROP TABLE IF EXISTS segment_price_history",
    "DROP TABLE IF EXISTS segment_stats",
]

DOWN_DROP_COLUMNS = [
    "ALTER TABLE tracked_searches DROP COLUMN IF EXISTS category",
    "ALTER TABLE tracked_searches DROP COLUMN IF EXISTS is_category_search",
    "ALTER TABLE ads DROP COLUMN IF EXISTS first_seen_at",
    "ALTER TABLE ads DROP COLUMN IF EXISTS last_seen_at",
    "ALTER TABLE ads DROP COLUMN IF EXISTS days_on_market",
    "ALTER TABLE ads DROP COLUMN IF EXISTS is_disappeared_quickly",
    "ALTER TABLE ads DROP COLUMN IF EXISTS ad_category",
    "ALTER TABLE ads DROP COLUMN IF EXISTS brand",
    "ALTER TABLE ads DROP COLUMN IF EXISTS extracted_model",
]


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------


def _column_exists(cursor, table: str, column: str) -> bool:
    """Проверяет, существует ли колонка в таблице (PostgreSQL)."""
    cursor.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = %s AND column_name = %s",
        (table, column),
    )
    return cursor.fetchone() is not None


def _table_exists(cursor, table: str) -> bool:
    """Проверяет, существует ли таблица (PostgreSQL)."""
    cursor.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    return cursor.fetchone() is not None


# ---------------------------------------------------------------------------
# Логика миграции
# ---------------------------------------------------------------------------


def _get_existing_columns(cursor, table: str) -> set[str]:
    """Возвращает множество существующих колонок таблицы."""
    cursor.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    return {row[0] for row in cursor.fetchall()}


def _ensure_table_columns(
    cursor,
    table: str,
    create_sql: str,
    required_columns: dict[str, str],
    indexes: list[str],
) -> None:
    """Создаёт таблицу если её нет, иначе добавляет недостающие колонки.

    Args:
        cursor: Курсор БД.
        table: Имя таблицы.
        create_sql: SQL для CREATE TABLE (без IF NOT EXISTS).
        required_columns: Словарь {имя_колонки: определение_типа}.
        indexes: Список SQL для создания индексов.
    """
    if _table_exists(cursor, table):
        print(f"  Таблица {table} существует — проверяем колонки")
        existing = _get_existing_columns(cursor, table)

        added = 0
        for col_name, col_def in required_columns.items():
            if col_name not in existing:
                sql = f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}"
                cursor.execute(sql)
                print(f"  ADD COLUMN: {col_name} {col_def}")
                added += 1
            else:
                print(f"  SKIP (exists): {col_name}")

        if added == 0:
            print(f"  Все колонки {table} в порядке")
    else:
        cursor.execute(create_sql)
        print(f"  OK: таблица {table} создана")

    # Индексы (CREATE INDEX IF NOT EXISTS безопасны)
    for idx_sql in indexes:
        cursor.execute(idx_sql)
    print(f"  OK: {len(indexes)} индексов проверено/создано")


def migrate_up(db_url: str, dry_run: bool = False) -> None:
    """Применяет миграцию."""
    prefix = "[DRY-RUN] " if dry_run else ""
    print(f"{prefix}Миграция UP: сегментный анализ")
    print(f"БД: {db_url}")
    print()

    if dry_run:
        _dry_run_up(db_url)
        return

    conn = get_connection(db_url)
    cursor = conn.cursor()

    try:
        # --- 1. ALTER TABLE tracked_searches ---
        print("=== ALTER TABLE tracked_searches ===")
        for stmt in ALTER_TRACKED_SEARCHES:
            col_name = stmt["description"].split(".")[1]
            if _column_exists(cursor, "tracked_searches", col_name):
                print(f"  SKIP (already exists): {stmt['description']}")
            else:
                cursor.execute(stmt["sql"])
                print(f"  OK: {stmt['description']}")

        # --- 2. ALTER TABLE ads ---
        print("=== ALTER TABLE ads ===")
        for stmt in ALTER_ADS:
            col_name = stmt["description"].split(".")[1]
            if _column_exists(cursor, "ads", col_name):
                print(f"  SKIP (already exists): {stmt['description']}")
            else:
                cursor.execute(stmt["sql"])
                print(f"  OK: {stmt['description']}")

        # --- 3. CREATE/ALTER TABLE segment_stats ---
        print("=== TABLE segment_stats ===")
        _ensure_table_columns(
            cursor,
            "segment_stats",
            CREATE_SEGMENT_STATS,
            SEGMENT_STATS_COLUMNS,
            CREATE_SEGMENT_STATS_INDEXES,
        )

        # --- 4. CREATE/ALTER TABLE segment_price_history ---
        print("=== TABLE segment_price_history ===")
        _ensure_table_columns(
            cursor,
            "segment_price_history",
            CREATE_SEGMENT_PRICE_HISTORY,
            SEGMENT_PRICE_HISTORY_COLUMNS,
            CREATE_SEGMENT_PRICE_HISTORY_INDEXES,
        )

        # --- 5. UPDATE first_seen_at ---
        print("=== UPDATE ads.first_seen_at ===")
        cursor.execute(UPDATE_FIRST_SEEN)
        updated = cursor.rowcount
        print(f"  OK: {updated} rows updated")

        print()
        print("Миграция UP завершена успешно!")

    except Exception as exc:
        print(f"\nОШИБКА: {exc}")
        raise
    finally:
        cursor.close()
        conn.close()


def migrate_down(db_url: str, dry_run: bool = False) -> None:
    """Откатывает миграцию (удаляет таблицы и колонки)."""
    prefix = "[DRY-RUN] " if dry_run else ""
    print(f"{prefix}Миграция DOWN: сегментный анализ")
    print(f"БД: {db_url}")
    print()

    if dry_run:
        print("  Будут удалены таблицы: segment_price_history, segment_stats")
        print("  Будут удалены колонки в tracked_searches: category, is_category_search")
        print("  Будут удалены колонки в ads: first_seen_at, last_seen_at, days_on_market,")
        print("    is_disappeared_quickly, ad_category, brand, extracted_model")
        return

    conn = get_connection(db_url)
    cursor = conn.cursor()

    try:
        # Удаляем в обратном порядке (сначала зависимую таблицу)
        print("=== DROP TABLE segment_price_history ===")
        if _table_exists(cursor, "segment_price_history"):
            cursor.execute("DROP TABLE segment_price_history")
            print("  OK: table dropped")
        else:
            print("  SKIP (table does not exist)")

        print("=== DROP TABLE segment_stats ===")
        if _table_exists(cursor, "segment_stats"):
            cursor.execute("DROP TABLE segment_stats")
            print("  OK: table dropped")
        else:
            print("  SKIP (table does not exist)")

        # Удаляем колонки (PostgreSQL поддерживает DROP COLUMN)
        print("=== DROP COLUMNS tracked_searches ===")
        for sql in DOWN_DROP_COLUMNS:
            if sql.startswith("ALTER TABLE tracked_searches"):
                cursor.execute(sql)
                col_name = sql.split("DROP COLUMN IF EXISTS ")[1]
                print(f"  OK: dropped tracked_searches.{col_name}")

        print("=== DROP COLUMNS ads ===")
        for sql in DOWN_DROP_COLUMNS:
            if sql.startswith("ALTER TABLE ads"):
                cursor.execute(sql)
                col_name = sql.split("DROP COLUMN IF EXISTS ")[1]
                print(f"  OK: dropped ads.{col_name}")

        print()
        print("Миграция DOWN завершена успешно!")

    except Exception as exc:
        print(f"\nОШИБКА: {exc}")
        raise
    finally:
        cursor.close()
        conn.close()


def _dry_run_ensure_columns(
    cursor,
    table: str,
    create_sql: str,
    required_columns: dict[str, str],
    indexes: list[str],
) -> None:
    """Показывает план создания/изменения таблицы без реальных изменений."""
    if cursor and _table_exists(cursor, table):
        print(f"  Таблица {table} существует — проверяем колонки")
        existing = _get_existing_columns(cursor, table)

        for col_name, col_def in required_columns.items():
            if col_name not in existing:
                print(f"  WOULD ADD COLUMN: {col_name} {col_def}")
            else:
                print(f"  SKIP (exists): {col_name}")
    else:
        print(f"  WOULD CREATE TABLE {table}")

    print(f"  WOULD CHECK {len(indexes)} индексов")


def _dry_run_up(db_url: str) -> None:
    """Выводит план миграции без реальных изменений."""
    conn = None
    cursor = None

    try:
        conn = get_connection(db_url)
        cursor = conn.cursor()
    except Exception as exc:
        print(f"  ВНИМАНИЕ: не удалось подключиться к БД: {exc}")
        print("  Показываю план миграции без проверки текущего состояния:")
        print()

    try:
        # ALTER TABLE tracked_searches
        print("=== ALTER TABLE tracked_searches ===")
        for stmt in ALTER_TRACKED_SEARCHES:
            col_name = stmt["description"].split(".")[1]
            if cursor and _column_exists(cursor, "tracked_searches", col_name):
                print(f"  SKIP (already exists): {stmt['description']}")
            else:
                print(f"  WOULD EXECUTE: {stmt['sql']}")

        # ALTER TABLE ads
        print("=== ALTER TABLE ads ===")
        for stmt in ALTER_ADS:
            col_name = stmt["description"].split(".")[1]
            if cursor and _column_exists(cursor, "ads", col_name):
                print(f"  SKIP (already exists): {stmt['description']}")
            else:
                print(f"  WOULD EXECUTE: {stmt['sql']}")

        # TABLE segment_stats
        print("=== TABLE segment_stats ===")
        _dry_run_ensure_columns(
            cursor,
            "segment_stats",
            CREATE_SEGMENT_STATS,
            SEGMENT_STATS_COLUMNS,
            CREATE_SEGMENT_STATS_INDEXES,
        )

        # TABLE segment_price_history
        print("=== TABLE segment_price_history ===")
        _dry_run_ensure_columns(
            cursor,
            "segment_price_history",
            CREATE_SEGMENT_PRICE_HISTORY,
            SEGMENT_PRICE_HISTORY_COLUMNS,
            CREATE_SEGMENT_PRICE_HISTORY_INDEXES,
        )

        # UPDATE first_seen_at
        print("=== UPDATE ads.first_seen_at ===")
        if cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM ads "
                "WHERE first_seen_at IS NULL AND last_scraped_at IS NOT NULL"
            )
            count = cursor.fetchone()[0]
            print(f"  WOULD UPDATE: {count} rows")
        else:
            print("  WOULD UPDATE: unknown (нет подключения к БД)")

        print()
        print("[DRY-RUN] План миграции показан. Изменения НЕ применены.")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def main() -> None:
    """Точка входа."""
    parser = argparse.ArgumentParser(
        description="Миграция БД: сегментный анализ и трекинг оборачиваемости (PostgreSQL)",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--up",
        action="store_true",
        help="Применить миграцию",
    )
    group.add_argument(
        "--down",
        action="store_true",
        help="Откатить миграцию",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="URL подключения к PostgreSQL (по умолчанию — из настроек)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только показать план миграции без реальных изменений",
    )

    args = parser.parse_args()

    db_url: str = args.db_path or get_db_url()

    if args.up:
        migrate_up(db_url, dry_run=args.dry_run)
    elif args.down:
        migrate_down(db_url, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
