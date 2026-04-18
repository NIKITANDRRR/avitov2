"""Миграция: перенормализация всех товаров новым нормализатором v2.

Алгоритм:
1. Загружает все существующие Product из БД.
2. Собирает уникальные заголовки связанных объявлений (Ad).
3. Перенормализует каждый заголовок новым нормализатором.
4. Группирует продукты по новым ключам.
5. Объединяет продукты с одинаковым новым ключом (переносит price_snapshots).
6. Удаляет пустые продукты (без снапшотов).

Режимы:
    --dry-run    Показать план миграции без изменений (по умолчанию).
    --apply      Применить миграцию к БД.
    --verbose    Показать детали по каждому продукту.

Использование:
    python -m scripts.renormalize_products --dry-run
    python -m scripts.renormalize_products --apply
    python -m scripts.renormalize_products --apply --verbose
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

# Добавляем корень проекта в sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from sqlalchemy import func, select

from app.analysis.product_normalizer import normalize_title
from app.storage.database import get_session_factory
from app.storage.models import Ad, Product, ProductPriceSnapshot


def collect_ad_titles(session) -> dict[int, list[str]]:
    """Собрать заголовки объявлений по product_id.

    Returns:
        Словарь {product_id: [title1, title2, ...]}.
    """
    # Ad имеет поле title, но связь с Product через ProductPriceSnapshot.ad_id
    # Сначала получаем маппинг product_id → ad_id из снапшотов
    rows = session.execute(
        select(
            ProductPriceSnapshot.product_id,
            ProductPriceSnapshot.ad_id,
        ).where(ProductPriceSnapshot.ad_id.isnot(None))
    ).all()

    product_ad_ids: dict[int, set[int]] = defaultdict(set)
    for product_id, ad_id in rows:
        product_ad_ids[product_id].add(ad_id)

    # Получаем заголовки объявлений
    all_ad_ids = set()
    for ad_ids in product_ad_ids.values():
        all_ad_ids.update(ad_ids)

    if not all_ad_ids:
        return {}

    ad_titles: dict[int, str | None] = {}
    # Батчами по 500
    ad_list = sorted(all_ad_ids)
    for i in range(0, len(ad_list), 500):
        batch = ad_list[i : i + 500]
        ads = session.execute(
            select(Ad.id, Ad.title).where(Ad.id.in_(batch))
        ).all()
        for ad_id, title in ads:
            ad_titles[ad_id] = title

    # Собираем по product_id
    result: dict[int, list[str]] = defaultdict(list)
    for product_id, ad_ids in product_ad_ids.items():
        for ad_id in ad_ids:
            title = ad_titles.get(ad_id)
            if title:
                result[product_id].append(title)

    return dict(result)


def compute_new_keys(
    products: list[Product],
    ad_titles_by_product: dict[int, list[str]],
    verbose: bool = False,
) -> dict[int, str]:
    """Вычислить новые ключи для каждого продукта.

    Стратегия: берём самый частый normalized_key среди заголовков объявлений.
    Если объявлений нет — перенормализуем текущий ключ (как заголовок).

    Returns:
        Словарь {product_id: new_normalized_key}.
    """
    product_new_keys: dict[int, str] = {}

    for product in products:
        titles = ad_titles_by_product.get(product.id, [])

        if titles:
            # Нормализуем все заголовки и берём самый частый ключ
            key_counts: dict[str, int] = defaultdict(int)
            for title in titles:
                result = normalize_title(title)
                key_counts[result.normalized_key] += 1

            # Самый частый ключ
            best_key = max(key_counts, key=key_counts.get)  # type: ignore[arg-type]
            product_new_keys[product.id] = best_key

            if verbose:
                print(f"  Product #{product.id} '{product.normalized_key}':")
                print(f"    Titles: {len(titles)}")
                for key, count in sorted(key_counts.items(), key=lambda x: -x[1]):
                    print(f"    → {key} ({count}x)")
                print(f"    Best: {best_key}")
        else:
            # Нет объявлений — пробуем перенормализовать текущий ключ как заголовок
            result = normalize_title(product.normalized_key)
            product_new_keys[product.id] = result.normalized_key

            if verbose:
                print(
                    f"  Product #{product.id} '{product.normalized_key}' "
                    f"(no ads) → '{result.normalized_key}'"
                )

    return product_new_keys


def plan_migration(
    products: list[Product],
    product_new_keys: dict[int, str],
) -> dict[str, list[int]]:
    """Сгруппировать продукты по новым ключам.

    Returns:
        Словарь {new_key: [product_id1, product_id2, ...]}.
    """
    groups: dict[str, list[int]] = defaultdict(list)
    for product in products:
        new_key = product_new_keys[product.id]
        groups[new_key].append(product.id)
    return dict(groups)


def print_migration_plan(
    products: list[Product],
    product_new_keys: dict[int, str],
    groups: dict[str, list[int]],
) -> None:
    """Вывести план миграции."""
    # Маппинг id → Product
    product_map = {p.id: p for p in products}

    changed = 0
    merged = 0
    unchanged = 0

    print("\n" + "=" * 80)
    print("  ПЛАН МИГРАЦИИ: Перенормализация товаров")
    print("=" * 80)

    # Группы слияния (несколько старых продуктов → один новый ключ)
    merge_groups = {k: v for k, v in groups.items() if len(v) > 1}
    # Простые переименования
    renames = {k: v for k, v in groups.items() if len(v) == 1}

    if merge_groups:
        print(f"\n  📦 СЛИЯНИЕ ({len(merge_groups)} групп):")
        print("-" * 60)
        for new_key, product_ids in sorted(merge_groups.items()):
            old_keys = [product_map[pid].normalized_key for pid in product_ids]
            print(f"  → {new_key}")
            for old_key in old_keys:
                print(f"      ← {old_key}")
            merged += len(product_ids) - 1  # Один сохраняется

    # Переименования (ключ изменился)
    print(f"\n  🔄 ПЕРЕИМЕНОВАНИЕ:")
    print("-" * 60)
    for new_key, product_ids in sorted(renames.items()):
        pid = product_ids[0]
        old_key = product_map[pid].normalized_key
        if old_key != new_key:
            print(f"  {old_key} → {new_key}")
            changed += 1
        else:
            unchanged += 1

    print(f"\n  ✅ БЕЗ ИЗМЕНЕНИЙ: {unchanged}")

    print("\n" + "=" * 80)
    print(f"  Всего продуктов:    {len(products)}")
    print(f"  Будут слиты:        {merged}")
    print(f"  Будут переименованы: {changed}")
    print(f"  Без изменений:      {unchanged}")
    print(f"  Итого продуктов:    {len(groups)}")
    print("=" * 80)


def apply_migration(
    session,
    products: list[Product],
    product_new_keys: dict[int, str],
    groups: dict[str, list[int]],
    verbose: bool = False,
) -> None:
    """Применить миграцию к БД."""
    product_map = {p.id: p for p in products}

    processed_keys: set[str] = set()

    # ВАЖНО: сначала обрабатываем слияния (len > 1), потом переименования.
    # Слияние может освобождать ключи, которые нужны для переименования.
    merge_groups = {k: v for k, v in groups.items() if len(v) > 1}
    rename_groups = {k: v for k, v in groups.items() if len(v) == 1}

    # Фаза 1: слияния
    for new_key, product_ids in merge_groups.items():
        if new_key in processed_keys:
            continue
        processed_keys.add(new_key)

        primary = product_map[product_ids[0]]

        if verbose:
            print(
                f"  MERGE: {len(product_ids)} products → {new_key} "
                f"(primary #{primary.id})"
            )

        # Сначала удаляем вторичные продукты (освобождаем ключи)
        for pid in product_ids[1:]:
            secondary = product_map[pid]

            # Переносим снапшоты через raw SQL (без autoflush)
            with session.no_autoflush:
                snapshots = session.execute(
                    select(ProductPriceSnapshot).where(
                        ProductPriceSnapshot.product_id == pid
                    )
                ).scalars().all()

                for snap in snapshots:
                    snap.product_id = primary.id

            if verbose:
                print(
                    f"    DELETE: #{secondary.id} "
                    f"'{secondary.normalized_key}' "
                    f"({len(snapshots)} snapshots moved)"
                )

            # Удаляем старый продукт и сразу флешим
            session.delete(secondary)
            session.flush()

        # Теперь безопасно меняем ключ primary
        primary.normalized_key = new_key
        result = normalize_title(new_key)
        primary.brand = result.brand
        primary.model = result.model

        # Пересчитываем статистику первичного
        _recalc_product_stats(session, primary)
        session.flush()

    # Фаза 2: переименования
    for new_key, product_ids in rename_groups.items():
        if new_key in processed_keys:
            continue
        processed_keys.add(new_key)

        product = product_map[product_ids[0]]
        if product.normalized_key != new_key:
            if verbose:
                print(
                    f"  RENAME: {product.normalized_key} → {new_key}"
                )
            product.normalized_key = new_key
            # Обновляем бренд/модель через нормализатор
            result = normalize_title(new_key)
            product.brand = result.brand
            product.model = result.model

    session.commit()
    print("\n  ✅ Миграция применена успешно.")


def _recalc_product_stats(session, product: Product) -> None:
    """Пересчитать статистику продукта после слияния."""
    stats = session.execute(
        select(
            func.count(ProductPriceSnapshot.id).label("count"),
            func.avg(ProductPriceSnapshot.price).label("avg_price"),
            func.min(ProductPriceSnapshot.price).label("min_price"),
            func.max(ProductPriceSnapshot.price).label("max_price"),
        ).where(ProductPriceSnapshot.product_id == product.id)
    ).one()

    if stats.count and stats.count > 0:
        product.listing_count = stats.count
        product.median_price = float(stats.avg_price) if stats.avg_price else None
        product.min_price = float(stats.min_price) if stats.min_price else None
        product.max_price = float(stats.max_price) if stats.max_price else None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Перенормализация товаров новым нормализатором v2",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Применить миграцию (без флага — dry-run)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Показать детали по каждому продукту",
    )
    args = parser.parse_args()

    dry_run = not args.apply
    verbose = args.verbose

    if dry_run:
        print("  🔍 РЕЖИМ DRY-RUN (изменения не применяются)")
    else:
        print("  ⚠️  РЕЖИМ APPLY (изменения будут записаны в БД)")

    Session = get_session_factory()

    with Session() as session:
        # Загружаем все продукты
        products = list(
            session.execute(select(Product).order_by(Product.id)).scalars().all()
        )
        print(f"\n  Загружено продуктов: {len(products)}")

        if not products:
            print("  Нет продуктов для миграции.")
            return

        # Собираем заголовки объявлений
        print("  Сбор заголовков объявлений...")
        ad_titles = collect_ad_titles(session)
        total_titles = sum(len(v) for v in ad_titles.values())
        print(f"  Собрано заголовков: {total_titles} для {len(ad_titles)} продуктов")

        # Вычисляем новые ключи
        print("  Вычисление новых ключей...")
        product_new_keys = compute_new_keys(products, ad_titles, verbose=verbose)

        # Группируем по новым ключам
        groups = plan_migration(products, product_new_keys)

        # Выводим план
        print_migration_plan(products, product_new_keys, groups)

        if dry_run:
            print("\n  Для применения запустите: python -m scripts.renormalize_products --apply")
        else:
            import sys
            if sys.stdin.isatty():
                confirm = input("\n  Продолжить? (yes/no): ")
                if confirm.lower() != "yes":
                    print("  Отменено.")
                    return
            else:
                # Non-interactive mode: read from stdin pipe
                confirm = sys.stdin.readline().strip()
                if confirm.lower() != "yes":
                    print("  Отменено.")
                    return
            apply_migration(session, products, product_new_keys, groups, verbose)


if __name__ == "__main__":
    main()
