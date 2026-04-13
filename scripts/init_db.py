"""Скрипт инициализации базы данных — создание таблиц и добавление новых колонок."""

from __future__ import annotations

from sqlalchemy import inspect
from sqlalchemy.exc import ProgrammingError

from app.storage.database import Base, get_engine
from app.storage.models import TrackedSearch, SearchRun, Ad, AdSnapshot, NotificationSent  # noqa: F401


def _migrate_existing_db(engine) -> None:
    """Добавляет новые колонки в существующие таблицы PostgreSQL.

    PostgreSQL не добавляет колонки через SQLAlchemy
    ``create_all()``, поэтому выполняем миграцию вручную.

    Args:
        engine: SQLAlchemy engine.
    """
    # Маппинг: таблица -> список (колонка, SQL-определение)
    new_columns: dict[str, list[tuple[str, str]]] = {
        "tracked_searches": [
            ("schedule_interval_hours", "INTEGER NOT NULL DEFAULT 2"),
            ("last_run_at", "TIMESTAMP"),
            ("priority", "INTEGER NOT NULL DEFAULT 0"),
            ("max_ads_to_parse", "INTEGER NOT NULL DEFAULT 3"),
        ],
        "ads": [
            ("seller_type", "VARCHAR(128)"),
            ("z_score", "FLOAT"),
            ("iqr_outlier", "BOOLEAN DEFAULT FALSE"),
            ("segment_key", "VARCHAR(512)"),
        ],
    }

    inspector = inspect(engine)

    existing_tables = inspector.get_table_names()

    for table_name, columns in new_columns.items():
        if table_name not in existing_tables:
            continue

        existing_columns = {col["name"] for col in inspector.get_columns(table_name)}

        for col_name, col_def in columns:
            if col_name not in existing_columns:
                raw_conn = engine.raw_connection()
                try:
                    cursor = raw_conn.cursor()
                    cursor.execute(
                        f'ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def}'
                    )
                    raw_conn.commit()
                    print(f"  Added column {table_name}.{col_name}")
                except ProgrammingError as exc:
                    print(f"  Skip {table_name}.{col_name}: {exc}")
                finally:
                    raw_conn.close()

def init_db() -> None:
    """Создаёт все таблицы в базе данных на основе SQLAlchemy-моделей.

    Использует ``Base.metadata.create_all`` для создания таблиц,
    которые ещё не существуют в базе. Затем применяет миграции
    для добавления новых колонок в существующие таблицы.
    """
    engine = get_engine()

    # 1. Создаём таблицы, которых ещё нет
    Base.metadata.create_all(engine)
    print("Database tables created successfully.")

    # 2. Добавляем новые колонки в существующие таблицы (миграция)
    _migrate_existing_db(engine)
    print("Migration check completed.")


if __name__ == "__main__":
    init_db()
