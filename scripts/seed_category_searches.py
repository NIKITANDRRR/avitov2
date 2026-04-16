"""Добавление category-поисков в БД для категорийного мониторинга.

Запуск:
    python -m scripts.seed_category_searches
"""

from __future__ import annotations

from app.storage.database import ensure_tables
from app.storage.models import TrackedSearch
from app.storage import get_session, Repository


# Стартовые category-поиски
CATEGORY_SEARCHES = [
    {
        "search_url": "https://www.avito.ru/rossiya/telefony",
        "search_phrase": "Телефоны (широкая лента)",
        "category": "телефоны",
        "schedule_interval_hours": 2,
        "max_ads_to_parse": 20,
        "priority": 10,
    },
    {
        "search_url": "https://www.avito.ru/rossiya/noutbuki",
        "search_phrase": "Ноутбуки (широкая лента)",
        "category": "ноутбуки",
        "schedule_interval_hours": 3,
        "max_ads_to_parse": 15,
        "priority": 8,
    },
    {
        "search_url": "https://www.avito.ru/rossiya/velosipedy",
        "search_phrase": "Велосипеды (широкая лента)",
        "category": "велосипеды",
        "schedule_interval_hours": 4,
        "max_ads_to_parse": 20,
        "priority": 7,
    },
    {
        "search_url": "https://www.avito.ru/rossiya/shiny",
        "search_phrase": "Шины (широкая лента)",
        "category": "шины",
        "schedule_interval_hours": 3,
        "max_ads_to_parse": 20,
        "priority": 7,
    },
]


def seed_category_searches() -> None:
    """Добавить category-поиски в БД."""
    ensure_tables()
    session = get_session()
    repo = Repository(session)

    created_count = 0
    skipped_count = 0

    for data in CATEGORY_SEARCHES:
        existing = repo.get_or_create_tracked_search(data["search_url"])

        # Проверяем, был ли только что создан или уже существует
        if existing.search_phrase is None or existing.search_type == "model":
            # Обновляем поля для category-поиска
            existing.search_type = "category"
            existing.search_phrase = data["search_phrase"]
            existing.category = data["category"]
            existing.schedule_interval_hours = data["schedule_interval_hours"]
            existing.max_ads_to_parse = data["max_ads_to_parse"]
            existing.priority = data["priority"]
            existing.is_active = True
            created_count += 1
            print(f"  [+] Создан category-поиск: {data['search_phrase']}")
        else:
            skipped_count += 1
            print(f"  - Уже существует: {data['search_phrase']}")

    repo.commit()
    repo.close()

    print(f"\nИтого: создано {created_count}, пропущено {skipped_count}")


if __name__ == "__main__":
    seed_category_searches()
