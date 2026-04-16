"""Миграция БД: добавление поддержки категорийного мониторинга.

Запуск:
    python -m scripts.migrate_category_monitoring
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


MIGRATION_SQL = """
-- ============================================================
-- Миграция: Добавление поддержки категорийного мониторинга
-- Версия: 2 (с временными окнами и ликвидностью)
-- ============================================================

-- 1. Новые поля в tracked_searches
ALTER TABLE tracked_searches
    ADD COLUMN IF NOT EXISTS search_type VARCHAR(20) DEFAULT 'model',
    ADD COLUMN IF NOT EXISTS category VARCHAR(128) DEFAULT NULL;

-- 2. Новые поля в ads (атрибуты + оборачиваемость)
ALTER TABLE ads
    ADD COLUMN IF NOT EXISTS ad_category VARCHAR(128) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS brand VARCHAR(128) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS extracted_model VARCHAR(256) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS attributes_raw TEXT DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS days_on_market INTEGER DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS is_disappeared BOOLEAN DEFAULT FALSE;

-- 3. Индексы для новых полей
CREATE INDEX IF NOT EXISTS idx_ads_ad_category ON ads(ad_category);
CREATE INDEX IF NOT EXISTS idx_ads_brand ON ads(brand);
CREATE INDEX IF NOT EXISTS idx_ads_ad_category_brand_model
    ON ads(ad_category, brand, extracted_model);
CREATE INDEX IF NOT EXISTS idx_tracked_searches_search_type
    ON tracked_searches(search_type);
CREATE INDEX IF NOT EXISTS idx_ads_is_disappeared
    ON ads(is_disappeared) WHERE is_disappeared = FALSE;
CREATE INDEX IF NOT EXISTS idx_ads_last_seen_at
    ON ads(last_seen_at);

-- 4. Новая таблица: segment_stats (с временными окнами и ликвидностью)
CREATE TABLE IF NOT EXISTS segment_stats (
    id SERIAL PRIMARY KEY,
    segment_key VARCHAR(512) NOT NULL,
    category VARCHAR(128) NOT NULL,
    brand VARCHAR(128) NOT NULL DEFAULT 'unknown',
    model VARCHAR(256) NOT NULL DEFAULT 'unknown',
    condition VARCHAR(128) NOT NULL DEFAULT 'unknown',
    location VARCHAR(256) NOT NULL DEFAULT 'unknown',

    -- Текущая статистика
    ad_count INTEGER NOT NULL DEFAULT 0,
    median_price FLOAT,
    mean_price FLOAT,
    q1_price FLOAT,
    q3_price FLOAT,
    iqr FLOAT,
    std_dev FLOAT DEFAULT 0.0,
    min_price FLOAT,
    max_price FLOAT,
    lower_fence FLOAT,

    -- Временные окна медианы
    median_7d FLOAT DEFAULT NULL,
    median_30d FLOAT DEFAULT NULL,
    median_90d FLOAT DEFAULT NULL,
    price_trend_slope FLOAT DEFAULT NULL,
    price_trend_r2 FLOAT DEFAULT NULL,

    -- Метрики ликвидности
    appearance_count_90d INTEGER DEFAULT 0,
    median_days_on_market FLOAT DEFAULT NULL,
    fast_sale_price_median FLOAT DEFAULT NULL,
    listing_price_median FLOAT DEFAULT NULL,

    calculated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_segment_stats_key
    ON segment_stats(segment_key);
CREATE INDEX IF NOT EXISTS idx_segment_stats_category
    ON segment_stats(category);
CREATE INDEX IF NOT EXISTS idx_segment_stats_calculated
    ON segment_stats(calculated_at);

-- 5. Новая таблица: segment_price_history (расширенная)
CREATE TABLE IF NOT EXISTS segment_price_history (
    id SERIAL PRIMARY KEY,
    segment_key VARCHAR(512) NOT NULL,
    date DATE NOT NULL,
    median_price FLOAT,
    ad_count INTEGER NOT NULL DEFAULT 0,
    mean_price FLOAT,
    new_listings_count INTEGER DEFAULT 0,
    disappeared_count INTEGER DEFAULT 0,
    median_days_on_market FLOAT DEFAULT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_segment_price_history_key_date
    ON segment_price_history(segment_key, date);

-- 6. Новые поля в segment_stats (трекинг объёма предложения)
ALTER TABLE segment_stats
    ADD COLUMN IF NOT EXISTS ad_count_7d_ago INTEGER;
ALTER TABLE segment_stats
    ADD COLUMN IF NOT EXISTS ad_count_30d_ago INTEGER;
ALTER TABLE segment_stats
    ADD COLUMN IF NOT EXISTS supply_change_percent_30d FLOAT;
"""


def run_migration() -> None:
    """Применить миграцию категорийного мониторинга."""
    print("Начало миграции: категорийный мониторинг v2...")

    engine = get_engine()

    with engine.connect() as conn:
        # Выполняем по отдельным statements для надёжности
        raw_statements = [s.strip() for s in MIGRATION_SQL.split(";") if s.strip()]

        # Убираем строки-комментарии из каждого фрагмента
        statements = []
        for raw in raw_statements:
            lines = [line for line in raw.split("\n")
                     if not line.strip().startswith("--")]
            cleaned = "\n".join(lines).strip()
            if cleaned:
                statements.append(cleaned)

        for i, stmt in enumerate(statements, 1):
            try:
                conn.execute(text(stmt))
                conn.commit()
                print(f"  [{i}/{len(statements)}] OK")
            except sqlalchemy.exc.ProgrammingError as exc:
                # Индекс или колонка уже существуют — не ошибка
                if "already exists" in str(exc).lower():
                    print(f"  [{i}/{len(statements)}] SKIP (already exists)")
                else:
                    print(f"  [{i}/{len(statements)}] ERROR: {exc}")
                    raise

    print("Миграция завершена успешно!")


if __name__ == "__main__":
    run_migration()
