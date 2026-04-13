#!/usr/bin/env python3
"""Скрипт очистки БД от дубликатов объявлений.

Удаляет дубликаты объявлений (записи с одинаковым ad_id, но разными id),
оставляя одну «лучшую» запись для каждого ad_id.
Также сбрасывает last_run_at для всех tracked_searches.

Использование:
    python -m scripts.cleanup_duplicates          # с подтверждением
    python -m scripts.cleanup_duplicates --dry-run # только статистика, без изменений
"""

from __future__ import annotations

import sys
from pathlib import Path

# Добавляем корень проекта в sys.path, чтобы работали импорты app.*
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import func, select, text, update
from sqlalchemy.orm import Session

from app.storage.database import get_engine
from app.storage.models import (
    Ad,
    AdSnapshot,
    NotificationSent,
    TrackedSearch,
)


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------


def _find_duplicate_ad_ids(session: Session) -> list[str]:
    """Возвращает список ad_id, у которых более одной записи в таблице ads."""
    stmt = (
        select(Ad.ad_id)
        .group_by(Ad.ad_id)
        .having(func.count(Ad.id) > 1)
    )
    result = session.execute(stmt).scalars().all()
    return list(result)


def _pick_best_ad_id(session: Session, ad_id: str) -> int:
    """Выбирает id записи, которую нужно оставить для данного ad_id.

    Приоритет:
      1. Запись, у которой есть хотя бы один snapshot.
      2. Если ни у одного нет snapshot — запись с наименьшим id (самая ранняя).
    """
    # Все записи для данного ad_id, отсортированные по id
    ads = (
        session.execute(
            select(Ad).where(Ad.ad_id == ad_id).order_by(Ad.id)
        )
        .scalars()
        .all()
    )

    # Собираем id объявлений, у которых есть snapshots
    ad_db_ids = [ad.id for ad in ads]
    ads_with_snapshots = (
        session.execute(
            select(AdSnapshot.ad_id)
            .where(AdSnapshot.ad_id.in_(ad_db_ids))
            .distinct()
        )
        .scalars()
        .all()
    )

    # Приоритет: запись со snapshot
    for ad in ads:
        if ad.id in ads_with_snapshots:
            return ad.id

    # Fallback: самая ранняя по id
    return ads[0].id


def _count_ads_without_snapshots(session: Session) -> int:
    """Возвращает количество уникальных ad_id без единого snapshot."""
    stmt = (
        select(Ad.id)
        .outerjoin(AdSnapshot, AdSnapshot.ad_id == Ad.id)
        .group_by(Ad.id)
        .having(func.count(AdSnapshot.id) == 0)
    )
    return len(session.execute(stmt).scalars().all())


def _count_unique_ad_ids(session: Session) -> int:
    """Возвращает количество уникальных ad_id."""
    stmt = select(func.count(func.distinct(Ad.ad_id)))
    return session.execute(stmt).scalar() or 0


def _count_total_ads(session: Session) -> int:
    """Возвращает общее количество записей в ads."""
    return session.query(Ad).count()


def _count_snapshots(session: Session) -> int:
    """Возвращает общее количество snapshots."""
    return session.query(AdSnapshot).count()


def _count_notifications(session: Session) -> int:
    """Возвращает общее количество уведомлений."""
    return session.query(NotificationSent).count()


# ---------------------------------------------------------------------------
# Основная логика
# ---------------------------------------------------------------------------


def cleanup_duplicates(session: Session, dry_run: bool = False) -> dict:
    """Удаляет дубликаты объявлений и возвращает статистику.

    Args:
        session: SQLAlchemy-сессия.
        dry_run: Если True — только подсчёт, без удаления.

    Returns:
        Словарь со статистикой.
    """
    stats: dict = {
        "total_ads_before": _count_total_ads(session),
        "unique_ad_ids": _count_unique_ad_ids(session),
        "duplicate_ad_ids": 0,
        "duplicate_records_removed": 0,
        "snapshots_removed": 0,
        "notifications_removed": 0,
        "tracked_searches_reset": 0,
        "ads_without_snapshot": 0,
    }

    # --- Поиск дубликатов ---
    duplicate_ad_ids = _find_duplicate_ad_ids(session)
    stats["duplicate_ad_ids"] = len(duplicate_ad_ids)

    if not duplicate_ad_ids:
        print("\n✅ Дубликаты не найдены.")
    else:
        print(f"\n🔍 Найдено {len(duplicate_ad_ids)} ad_id с дубликатами.")

    # --- Удаление дубликатов ---
    total_snapshots_removed = 0
    total_notifications_removed = 0
    total_ads_removed = 0

    for ad_id in duplicate_ad_ids:
        best_id = _pick_best_ad_id(session, ad_id)

        # Все записи для этого ad_id, КРОМЕ лучшей
        duplicate_ads = (
            session.execute(
                select(Ad).where(Ad.ad_id == ad_id, Ad.id != best_id)
            )
            .scalars()
            .all()
        )

        for dup_ad in duplicate_ads:
            # Удаляем snapshots (если есть)
            snap_count = (
                session.query(AdSnapshot)
                .filter(AdSnapshot.ad_id == dup_ad.id)
                .delete(synchronize_session="fetch")
            )
            total_snapshots_removed += snap_count

            # Удаляем notifications (если есть)
            notif_count = (
                session.query(NotificationSent)
                .filter(NotificationSent.ad_id == dup_ad.id)
                .delete(synchronize_session="fetch")
            )
            total_notifications_removed += notif_count

            # Удаляем саму запись ad
            session.delete(dup_ad)
            total_ads_removed += 1

    stats["duplicate_records_removed"] = total_ads_removed
    stats["snapshots_removed"] = total_snapshots_removed
    stats["notifications_removed"] = total_notifications_removed

    # --- Сброс last_run_at для всех tracked_searches ---
    tracked_searches_count = session.query(TrackedSearch).count()
    if not dry_run:
        session.execute(
            update(TrackedSearch).values(last_run_at=None)
        )
    stats["tracked_searches_reset"] = tracked_searches_count

    # --- Финальная статистика ---
    if not dry_run:
        stats["total_ads_after"] = _count_total_ads(session)
    else:
        stats["total_ads_after"] = stats["total_ads_before"] - total_ads_removed

    stats["ads_without_snapshot"] = _count_ads_without_snapshots(session)

    return stats


def print_stats(stats: dict, dry_run: bool) -> None:
    """Выводит статистику в консоль."""
    mode = "🔍 РЕЖИМ DRY-RUN (изменения НЕ применены)" if dry_run else "✅ ИЗМЕНЕНИЯ ПРИМЕНЕНЫ"
    print(f"\n{'=' * 60}")
    print(f"  СТАТИСТИКА ОЧИСТКИ БД  —  {mode}")
    print(f"{'=' * 60}")
    print(f"  Записей в ads до очистки:       {stats['total_ads_before']}")
    print(f"  Уникальных ad_id:               {stats['unique_ad_ids']}")
    print(f"  ad_id с дубликатами:            {stats['duplicate_ad_ids']}")
    print(f"  Дубликатов удалено (ads):       {stats['duplicate_records_removed']}")
    print(f"  Snapshots удалено:              {stats['snapshots_removed']}")
    print(f"  Уведомлений удалено:            {stats['notifications_removed']}")
    print(f"  Записей в ads после очистки:    {stats['total_ads_after']}")
    print(f"  Объявлений без snapshot:        {stats['ads_without_snapshot']}")
    print(f"  TrackedSearches сброшено:       {stats['tracked_searches_reset']}")
    print(f"{'=' * 60}")

    if stats["duplicate_ad_ids"] == 0:
        print("  Дубликаты не найдены — БД в порядке!")
    else:
        print(
            f"  Удалено {stats['duplicate_records_removed']} дубликатов "
            f"для {stats['duplicate_ad_ids']} ad_id."
        )

    if stats["ads_without_snapshot"] > 0:
        print(
            f"  ⚠ {stats['ads_without_snapshot']} объявлений без snapshot "
            f"— будут обработаны при следующем запуске."
        )

    print()


def main() -> None:
    """Точка входа."""
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        print("🔍 Запуск в режиме DRY-RUN — изменения НЕ будут применены.")
    else:
        print("⚠️  Запуск очистки БД от дубликатов.")
        print("   Используйте --dry-run для предварительного просмотра.")

        # Подтверждение
        try:
            answer = input("\n  Продолжить? [y/N]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nОтменено.")
            sys.exit(0)

        if answer not in ("y", "yes", "д", "да"):
            print("Отменено.")
            sys.exit(0)

    engine = get_engine()

    with Session(engine) as session:
        try:
            stats = cleanup_duplicates(session, dry_run=dry_run)

            if not dry_run:
                session.commit()
                print("\n✅ Транзакция успешно зафиксирована (commit).")
            else:
                # В dry-run откатываем, чтобы ничего не изменилось
                session.rollback()

            print_stats(stats, dry_run)

        except Exception:
            session.rollback()
            print("\n❌ Ошибка при очистке. Транзакция откачена (rollback).")
            raise


if __name__ == "__main__":
    main()
