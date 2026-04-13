"""Скрипт заполнения БД 14 поисковыми запросами по всей России.

Запуск:
    python -m scripts.seed_searches
    # или
    python scripts/seed_searches.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Добавляем корень проекта в sys.path для корректных импортов
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from app.storage.database import Base, get_engine
from app.storage.models import (  # noqa: F401
    Ad,
    AdSnapshot,
    NotificationSent,
    SearchRun,
    TrackedSearch,
)
from app.utils.helpers import build_avito_url

# 14 поисковых запросов по всей России
SEARCHES: list[str] = [
    # iPhone (6)
    "iPhone 15 Pro 128GB",
    "iPhone 15 Pro 256GB",
    "iPhone 15 Pro Max 256GB",
    "iPhone 15 128GB",
    "iPhone 14 Pro 128GB",
    "iPhone 14 Pro Max 256GB",
    # MacBook (4)
    "MacBook Air M2",
    "MacBook Air M3",
    "MacBook Pro M2",
    "MacBook Pro M3",
    # iPad (4)
    "iPad Pro 11 M4",
    "iPad Pro 13 M4",
    "iPad Air M2",
    "iPad mini 6",
]


def seed_searches() -> None:
    """Создать таблицы и добавить 14 поисковых запросов по России.

    - Если таблиц нет — создаёт их.
    - Если поиск с таким URL уже есть — обновляет параметры.
    - Не дублирует поиски при повторном запуске.
    """
    # 1. Инициализация таблиц
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("✅ Таблицы созданы/проверены")

    # 2. Добавление поисковых запросов
    from app.storage import get_session
    from app.storage.repository import Repository

    session = get_session()
    repo = Repository(session)
    try:
        added = 0
        updated = 0
        for query in SEARCHES:
            search_url = build_avito_url(query, "Россия")
            tracked = repo.get_or_create_tracked_search(search_url)

            is_new = tracked.search_phrase is None
            tracked.schedule_interval_hours = 2
            tracked.max_ads_to_parse = 3
            tracked.search_phrase = query
            tracked.is_active = True
            tracked.priority = 1

            if is_new:
                added += 1
            else:
                updated += 1

        repo.commit()
        print(
            f"📊 Поисковые запросы: {added} добавлено, {updated} обновлено "
            f"(всего {len(SEARCHES)})"
        )
    except Exception as exc:
        print(f"❌ Ошибка при заполнении поисков: {exc}")
        raise
    finally:
        repo.close()


if __name__ == "__main__":
    seed_searches()
