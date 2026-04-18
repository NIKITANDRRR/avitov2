"""Скрипт статистики по спарсенным данным из БД Avito Monitor."""

import sys
import os
from pathlib import Path

# Добавляем корень проекта в sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

# Загружаем .env с явным путём к корню проекта,
# чтобы настройки БД читались независимо от CWD
from dotenv import load_dotenv
load_dotenv(_PROJECT_ROOT / ".env")

from sqlalchemy import create_engine, text


def fmt_num(n) -> str:
    """Форматирование числа с разделителями тысяч."""
    if n is None:
        return "N/A"
    if isinstance(n, float):
        return f"{n:,.2f}".replace(",", " ")
    return f"{n:,}".replace(",", " ")


def fmt_price(n) -> str:
    """Форматирование цены в рублях."""
    if n is None:
        return "N/A"
    return f"{n:,.0f} ₽".replace(",", " ")


def print_separator(char="=", length=80):
    print(char * length)


def print_header(title: str):
    print()
    print_separator()
    print(f"  {title}")
    print_separator()


def main():
    from app.config.settings import get_settings
    settings = get_settings()
    database_url = settings.DATABASE_URL
    engine = create_engine(database_url)

    with engine.connect() as conn:
        # ============================================================
        # 1. ОБЩАЯ СТАТИСТИКА ПО ВСЕМ ТАБЛИЦАМ
        # ============================================================
        print_header("1. ОБЩАЯ СТАТИСТИКА ПО ТАБЛИЦАМ")

        tables = [
            "tracked_searches",
            "search_runs",
            "ads",
            "sellers",
            "sold_items",
            "ad_snapshots",
            "notifications_sent",
            "segment_stats",
            "segment_price_history",
            "products",
            "product_price_snapshots",
        ]

        # Белый список допустимых имён таблиц для защиты от SQL-инъекции
        allowed_tables = set(tables)

        for table in tables:
            if table not in allowed_tables:
                print(f"  {table:30s} : ПРОПУСК (недопустимое имя)")
                continue
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                print(f"  {table:30s} : {fmt_num(count)} записей")
            except Exception as e:
                print(f"  {table:30s} : ОШИБКА - {e}")

        # ============================================================
        # 2. ПОИСКОВЫЕ ЗАПРОСЫ (tracked_searches)
        # ============================================================
        print_header("2. ПОИСКОВЫЕ ЗАПРОСЫ (tracked_searches)")

        result = conn.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE is_active = true) as active,
                COUNT(*) FILTER (WHERE is_active = false) as inactive,
                COUNT(*) FILTER (WHERE is_category_search = true) as category_searches,
                COUNT(*) FILTER (WHERE is_category_search = false) as product_searches
            FROM tracked_searches
        """))
        row = result.mappings().one()
        print(f"  Всего поисковых запросов     : {fmt_num(row['total'])}")
        print(f"  Активных                      : {fmt_num(row['active'])}")
        print(f"  Неактивных                    : {fmt_num(row['inactive'])}")
        print(f"  Категорийные поиски           : {fmt_num(row['category_searches'])}")
        print(f"  Продуктовые поиски            : {fmt_num(row['product_searches'])}")

        # Детализация по поисковым запросам
        result = conn.execute(text("""
            SELECT id, search_phrase, category, is_active, is_category_search,
                   schedule_interval_hours, max_ads_to_parse, last_run_at
            FROM tracked_searches
            ORDER BY id
        """))
        rows = result.mappings().all()
        if rows:
            print("\n  Детализация поисковых запросов:")
            print(f"  {'ID':>4} | {'Фраза/Категория':40} | {'Тип':10} | {'Активен':7} | {'Интервал':8} | {'Макс.объ':8} | {'Последний запуск':20}")
            print(f"  {'-'*4}-+-{'-'*40}-+-{'-'*10}-+-{'-'*7}-+-{'-'*8}-+-{'-'*8}-+-{'-'*20}")
            for r in rows:
                phrase = (r['search_phrase'] or r['category'] or 'N/A')[:40]
                stype = "категория" if r['is_category_search'] else "продукт"
                active = "Да" if r['is_active'] else "Нет"
                interval = f"{r['schedule_interval_hours']}ч"
                max_ads = str(r['max_ads_to_parse'])
                last_run = str(r['last_run_at'])[:19] if r['last_run_at'] else "никогда"
                print(f"  {r['id']:>4} | {phrase:40} | {stype:10} | {active:7} | {interval:8} | {max_ads:8} | {last_run:20}")

        # ============================================================
        # 3. ЗАПУСКИ ПОИСКА (search_runs)
        # ============================================================
        print_header("3. ЗАПУСКИ ПОИСКА (search_runs)")

        result = conn.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE status = 'completed') as completed,
                COUNT(*) FILTER (WHERE status = 'running') as running,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                COALESCE(SUM(ads_found), 0) as total_ads_found,
                COALESCE(SUM(ads_new), 0) as total_ads_new,
                COALESCE(SUM(pages_fetched), 0) as total_pages,
                COALESCE(SUM(ads_opened), 0) as total_ads_opened,
                COALESCE(SUM(errors_count), 0) as total_errors,
                MIN(started_at) as first_run,
                MAX(started_at) as last_run
            FROM search_runs
        """))
        row = result.mappings().one()
        print(f"  Всего запусков                : {fmt_num(row['total'])}")
        print(f"  Успешных (completed)          : {fmt_num(row['completed'])}")
        print(f"  В процессе (running)          : {fmt_num(row['running'])}")
        print(f"  Ошибочных (failed)            : {fmt_num(row['failed'])}")
        print(f"  Всего найдено объявлений      : {fmt_num(row['total_ads_found'])}")
        print(f"  Новых объявлений              : {fmt_num(row['total_ads_new'])}")
        print(f"  Загружено страниц             : {fmt_num(row['total_pages'])}")
        print(f"  Открыто карточек              : {fmt_num(row['total_ads_opened'])}")
        print(f"  Всего ошибок                  : {fmt_num(row['total_errors'])}")
        print(f"  Первый запуск                 : {str(row['first_run'])[:19] if row['first_run'] else 'N/A'}")
        print(f"  Последний запуск              : {str(row['last_run'])[:19] if row['last_run'] else 'N/A'}")

        # ============================================================
        # 4. ОБЪЯВЛЕНИЯ (ads)
        # ============================================================
        print_header("4. ОБЪЯВЛЕНИЯ (ads)")

        result = conn.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE price IS NOT NULL) as with_price,
                COUNT(*) FILTER (WHERE price IS NULL) as without_price,
                COUNT(*) FILTER (WHERE parse_status = 'pending') as pending,
                COUNT(*) FILTER (WHERE parse_status = 'parsed') as parsed,
                COUNT(*) FILTER (WHERE parse_status = 'failed') as parse_failed,
                COUNT(*) FILTER (WHERE is_undervalued = true) as undervalued,
                COUNT(*) FILTER (WHERE iqr_outlier = true) as outliers,
                COUNT(*) FILTER (WHERE seller_id_fk IS NOT NULL) as with_seller,
                MIN(first_seen_at) as first_seen,
                MAX(first_seen_at) as last_seen
            FROM ads
        """))
        row = result.mappings().one()
        print(f"  Всего объявлений              : {fmt_num(row['total'])}")
        print(f"  С ценой                       : {fmt_num(row['with_price'])}")
        print(f"  Без цены                      : {fmt_num(row['without_price'])}")
        print(f"  Статус pending                : {fmt_num(row['pending'])}")
        print(f"  Статус parsed                 : {fmt_num(row['parsed'])}")
        print(f"  Статус failed                 : {fmt_num(row['parse_failed'])}")
        print(f"  Недооценённые                 : {fmt_num(row['undervalued'])}")
        print(f"  Выбросы (IQR)                 : {fmt_num(row['outliers'])}")
        print(f"  С привязанным продавцом       : {fmt_num(row['with_seller'])}")
        print(f"  Первое обнаружение            : {str(row['first_seen'])[:19] if row['first_seen'] else 'N/A'}")
        print(f"  Последнее обнаружение         : {str(row['last_seen'])[:19] if row['last_seen'] else 'N/A'}")

        # ============================================================
        # 5. ЦЕНОВАЯ СТАТИСТИКА
        # ============================================================
        print_header("5. ЦЕНОВАЯ СТАТИСТИКА")

        result = conn.execute(text("""
            SELECT
                COUNT(*) as cnt,
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(price) as avg_price,
                STDDEV(price) as stddev_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) as p25,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) as p75
            FROM ads
            WHERE price IS NOT NULL
        """))
        row = result.mappings().one()
        if row['cnt'] and row['cnt'] > 0:
            print(f"  Объявлений с ценой            : {fmt_num(row['cnt'])}")
            print(f"  Минимальная цена              : {fmt_price(row['min_price'])}")
            print(f"  Максимальная цена             : {fmt_price(row['max_price'])}")
            print(f"  Средняя цена                  : {fmt_price(row['avg_price'])}")
            print(f"  Медианная цена                : {fmt_price(row['median_price'])}")
            print(f"  25-й перцентиль               : {fmt_price(row['p25'])}")
            print(f"  75-й перцентиль               : {fmt_price(row['p75'])}")
            print(f"  Стандартное отклонение        : {fmt_price(row['stddev_price'])}")
            iqr = row['p75'] - row['p25']
            print(f"  IQR (межквартильный размах)   : {fmt_price(iqr)}")
        else:
            print("  Нет данных о ценах")

        # ============================================================
        # 6. СТАТИСТИКА ПО КАТЕГОРИЯМ/ПРОДУКТАМ
        # ============================================================
        print_header("6. ОБЪЯВЛЕНИЯ ПО КАТЕГОРИЯМ И ПОИСКОВЫМ ЗАПРОСАМ")

        # По search_url
        result = conn.execute(text("""
            SELECT
                ts.search_phrase,
                ts.category,
                ts.is_category_search,
                COUNT(a.id) as ad_count,
                COUNT(a.id) FILTER (WHERE a.price IS NOT NULL) as with_price,
                AVG(a.price) FILTER (WHERE a.price IS NOT NULL) as avg_price,
                MIN(a.price) FILTER (WHERE a.price IS NOT NULL) as min_price,
                MAX(a.price) FILTER (WHERE a.price IS NOT NULL) as max_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a.price)
                    FILTER (WHERE a.price IS NOT NULL) as median_price
            FROM tracked_searches ts
            LEFT JOIN ads a ON a.search_url = ts.search_url
            GROUP BY ts.id, ts.search_phrase, ts.category, ts.is_category_search
            ORDER BY ad_count DESC
        """))
        rows = result.mappings().all()
        if rows:
            print(f"\n  {'Поисковый запрос / Категория':45} | {'Тип':10} | {'Всего':>6} | {'С ценой':>6} | {'Мин.цена':>12} | {'Медиана':>12} | {'Средняя':>12} | {'Макс.цена':>12}")
            print(f"  {'-'*45}-+-{'-'*10}-+-{'-'*6}-+-{'-'*6}-+-{'-'*12}-+-{'-'*12}-+-{'-'*12}-+-{'-'*12}")
            for r in rows:
                phrase = (r['search_phrase'] or r['category'] or 'N/A')[:45]
                stype = "категория" if r['is_category_search'] else "продукт"
                ad_count = r['ad_count'] or 0
                with_price = r['with_price'] or 0
                min_p = fmt_price(r['min_price']) if r['min_price'] else "N/A"
                median_p = fmt_price(r['median_price']) if r['median_price'] else "N/A"
                avg_p = fmt_price(r['avg_price']) if r['avg_price'] else "N/A"
                max_p = fmt_price(r['max_price']) if r['max_price'] else "N/A"
                print(f"  {phrase:45} | {stype:10} | {ad_count:>6} | {with_price:>6} | {min_p:>12} | {median_p:>12} | {avg_p:>12} | {max_p:>12}")

        # ============================================================
        # 7. СТАТИСТИКА ПО ad_category (поле категории в ads)
        # ============================================================
        print_header("7. ОБЪЯВЛЕНИЯ ПО ad_category")

        result = conn.execute(text("""
            SELECT
                COALESCE(ad_category, '(без категории)') as ad_category,
                COUNT(*) as cnt,
                COUNT(*) FILTER (WHERE price IS NOT NULL) as with_price,
                AVG(price) FILTER (WHERE price IS NOT NULL) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                    FILTER (WHERE price IS NOT NULL) as median_price,
                MIN(price) FILTER (WHERE price IS NOT NULL) as min_price,
                MAX(price) FILTER (WHERE price IS NOT NULL) as max_price
            FROM ads
            GROUP BY ad_category
            ORDER BY cnt DESC
        """))
        rows = result.mappings().all()
        if rows:
            print(f"  {'Категория':40} | {'Всего':>6} | {'С ценой':>6} | {'Мин':>12} | {'Медиана':>12} | {'Средняя':>12} | {'Макс':>12}")
            print(f"  {'-'*40}-+-{'-'*6}-+-{'-'*6}-+-{'-'*12}-+-{'-'*12}-+-{'-'*12}-+-{'-'*12}")
            for r in rows:
                cat = r['ad_category'][:40]
                cnt = r['cnt'] or 0
                wp = r['with_price'] or 0
                mn = fmt_price(r['min_price']) if r['min_price'] else "N/A"
                med = fmt_price(r['median_price']) if r['median_price'] else "N/A"
                avg = fmt_price(r['avg_price']) if r['avg_price'] else "N/A"
                mx = fmt_price(r['max_price']) if r['max_price'] else "N/A"
                print(f"  {cat:40} | {cnt:>6} | {wp:>6} | {mn:>12} | {med:>12} | {avg:>12} | {mx:>12}")

        # ============================================================
        # 8. ПРОДАВЦЫ (sellers)
        # ============================================================
        print_header("8. ПРОДАВЦЫ (sellers)")

        result = conn.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE scrape_status = 'pending') as pending,
                COUNT(*) FILTER (WHERE scrape_status = 'scraped') as scraped,
                COUNT(*) FILTER (WHERE scrape_status = 'failed') as failed,
                COUNT(*) FILTER (WHERE rating IS NOT NULL) as with_rating,
                AVG(rating) FILTER (WHERE rating IS NOT NULL) as avg_rating,
                AVG(reviews_count) FILTER (WHERE reviews_count IS NOT NULL) as avg_reviews,
                AVG(total_sold_items) FILTER (WHERE total_sold_items IS NOT NULL) as avg_sold_items,
                MIN(first_seen_at) as first_seen,
                MAX(last_scraped_at) as last_scraped
            FROM sellers
        """))
        row = result.mappings().one()
        print(f"  Всего продавцов               : {fmt_num(row['total'])}")
        print(f"  Статус pending                : {fmt_num(row['pending'])}")
        print(f"  Статус scraped                : {fmt_num(row['scraped'])}")
        print(f"  Статус failed                 : {fmt_num(row['failed'])}")
        print(f"  С рейтингом                   : {fmt_num(row['with_rating'])}")
        print(f"  Средний рейтинг               : {fmt_num(row['avg_rating'])}")
        print(f"  Среднее кол-во отзывов        : {fmt_num(row['avg_reviews'])}")
        print(f"  Среднее кол-во продаж         : {fmt_num(row['avg_sold_items'])}")
        print(f"  Первый обнаружен              : {str(row['first_seen'])[:19] if row['first_seen'] else 'N/A'}")
        print(f"  Последний скрейпинг           : {str(row['last_scraped'])[:19] if row['last_scraped'] else 'N/A'}")

        # Топ продавцов по количеству объявлений
        result = conn.execute(text("""
            SELECT
                s.seller_name,
                s.seller_id,
                s.rating,
                s.reviews_count,
                s.total_sold_items,
                s.scrape_status,
                COUNT(a.id) as ad_count
            FROM sellers s
            LEFT JOIN ads a ON a.seller_id_fk = s.id
            GROUP BY s.id, s.seller_name, s.seller_id, s.rating,
                     s.reviews_count, s.total_sold_items, s.scrape_status
            ORDER BY ad_count DESC
            LIMIT 15
        """))
        rows = result.mappings().all()
        if rows:
            print(f"\n  Топ продавцов по объявлениям:")
            print(f"  {'Имя':30} | {'Рейтинг':>7} | {'Отзывы':>7} | {'Продажи':>8} | {'Объявл.':>8} | {'Статус':10}")
            print(f"  {'-'*30}-+-{'-'*7}-+-{'-'*7}-+-{'-'*8}-+-{'-'*8}-+-{'-'*10}")
            for r in rows:
                name = (r['seller_name'] or r['seller_id'] or 'N/A')[:30]
                rating = f"{r['rating']:.1f}" if r['rating'] else "N/A"
                reviews = fmt_num(r['reviews_count'])
                sold = fmt_num(r['total_sold_items'])
                ad_cnt = r['ad_count'] or 0
                status = r['scrape_status']
                print(f"  {name:30} | {rating:>7} | {reviews:>7} | {sold:>8} | {ad_cnt:>8} | {status:10}")

        # ============================================================
        # 9. ПРОДАННЫЕ ТОВАРЫ (sold_items)
        # ============================================================
        print_header("9. ПРОДАННЫЕ ТОВАРЫ (sold_items)")

        result = conn.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE price IS NOT NULL) as with_price,
                COUNT(DISTINCT seller_id_fk) as unique_sellers,
                MIN(scraped_at) as first_scraped,
                MAX(scraped_at) as last_scraped
            FROM sold_items
        """))
        row = result.mappings().one()
        print(f"  Всего проданных товаров       : {fmt_num(row['total'])}")
        print(f"  С ценой                       : {fmt_num(row['with_price'])}")
        print(f"  Уникальных продавцов          : {fmt_num(row['unique_sellers'])}")
        print(f"  Первый спарсенный             : {str(row['first_scraped'])[:19] if row['first_scraped'] else 'N/A'}")
        print(f"  Последний спарсенный          : {str(row['last_scraped'])[:19] if row['last_scraped'] else 'N/A'}")

        # Ценовая статистика по проданным товарам
        result = conn.execute(text("""
            SELECT
                COUNT(*) as cnt,
                AVG(price) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM sold_items
            WHERE price IS NOT NULL
        """))
        row = result.mappings().one()
        if row['cnt'] and row['cnt'] > 0:
            print(f"\n  Ценовая статистика проданных товаров:")
            print(f"    Средняя цена     : {fmt_price(row['avg_price'])}")
            print(f"    Медианная цена   : {fmt_price(row['median_price'])}")
            print(f"    Минимальная цена : {fmt_price(row['min_price'])}")
            print(f"    Максимальная цена: {fmt_price(row['max_price'])}")

        # По категориям проданных товаров
        result = conn.execute(text("""
            SELECT
                COALESCE(category, '(без категории)') as cat,
                COUNT(*) as cnt,
                COUNT(*) FILTER (WHERE price IS NOT NULL) as with_price,
                AVG(price) FILTER (WHERE price IS NOT NULL) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                    FILTER (WHERE price IS NOT NULL) as median_price
            FROM sold_items
            GROUP BY category
            ORDER BY cnt DESC
            LIMIT 15
        """))
        rows = result.mappings().all()
        if rows:
            print(f"\n  Проданные товары по категориям (топ-15):")
            print(f"  {'Категория':40} | {'Всего':>6} | {'С ценой':>6} | {'Медиана':>12} | {'Средняя':>12}")
            print(f"  {'-'*40}-+-{'-'*6}-+-{'-'*6}-+-{'-'*12}-+-{'-'*12}")
            for r in rows:
                cat = r['cat'][:40]
                cnt = r['cnt'] or 0
                wp = r['with_price'] or 0
                med = fmt_price(r['median_price']) if r['median_price'] else "N/A"
                avg = fmt_price(r['avg_price']) if r['avg_price'] else "N/A"
                print(f"  {cat:40} | {cnt:>6} | {wp:>6} | {med:>12} | {avg:>12}")

        # ============================================================
        # 10. СЕГМЕНТЫ (segment_stats)
        # ============================================================
        print_header("10. СЕГМЕНТЫ (segment_stats)")

        result = conn.execute(text("""
            SELECT
                COUNT(*) as total_segments,
                COUNT(*) FILTER (WHERE is_rare_segment = true) as rare_segments,
                AVG(sample_size) as avg_sample_size,
                AVG(median_30d) FILTER (WHERE median_30d IS NOT NULL) as avg_median_30d,
                MIN(calculated_at) as first_calc,
                MAX(calculated_at) as last_calc
            FROM segment_stats
        """))
        row = result.mappings().one()
        print(f"  Всего сегментов               : {fmt_num(row['total_segments'])}")
        print(f"  Редких сегментов              : {fmt_num(row['rare_segments'])}")
        print(f"  Средний размер выборки        : {fmt_num(row['avg_sample_size'])}")
        print(f"  Средняя медиана 30d           : {fmt_price(row['avg_median_30d'])}")
        print(f"  Первый расчёт                 : {str(row['first_calc'])[:19] if row['first_calc'] else 'N/A'}")
        print(f"  Последний расчёт              : {str(row['last_calc'])[:19] if row['last_calc'] else 'N/A'}")

        # Детализация по сегментам
        result = conn.execute(text("""
            SELECT
                ss.segment_key,
                ss.segment_name,
                ts.search_phrase,
                ss.sample_size,
                ss.listing_count,
                ss.median_7d,
                ss.median_30d,
                ss.median_90d,
                ss.mean_price,
                ss.min_price,
                ss.max_price,
                ss.is_rare_segment,
                ss.calculated_at
            FROM segment_stats ss
            JOIN tracked_searches ts ON ts.id = ss.search_id
            ORDER BY ss.sample_size DESC
        """))
        rows = result.mappings().all()
        if rows:
            print(f"\n  Детализация сегментов:")
            for r in rows:
                seg_name = (r['segment_name'] or r['segment_key'] or 'N/A')[:50]
                search = (r['search_phrase'] or 'N/A')[:30]
                rare = "РЕДКИЙ" if r['is_rare_segment'] else ""
                print(f"\n  Сегмент: {seg_name} {rare}")
                print(f"    Поиск: {search}")
                print(f"    Размер выборки: {r['sample_size']}, Объявлений: {r['listing_count']}")
                print(f"    Медиана 7d: {fmt_price(r['median_7d'])} | 30d: {fmt_price(r['median_30d'])} | 90d: {fmt_price(r['median_90d'])}")
                print(f"    Средняя: {fmt_price(r['mean_price'])} | Мин: {fmt_price(r['min_price'])} | Макс: {fmt_price(r['max_price'])}")
                print(f"    Рассчитано: {str(r['calculated_at'])[:19]}")

        # ============================================================
        # 11. СНИМКИ ЦЕН (ad_snapshots)
        # ============================================================
        print_header("11. СНИМКИ ЦЕН (ad_snapshots)")

        result = conn.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE price IS NOT NULL) as with_price,
                MIN(scraped_at) as first_snapshot,
                MAX(scraped_at) as last_snapshot,
                COUNT(DISTINCT ad_id) as unique_ads
            FROM ad_snapshots
        """))
        row = result.mappings().one()
        print(f"  Всего снимков                 : {fmt_num(row['total'])}")
        print(f"  С ценой                       : {fmt_num(row['with_price'])}")
        print(f"  Уникальных объявлений         : {fmt_num(row['unique_ads'])}")
        print(f"  Первый снимок                 : {str(row['first_snapshot'])[:19] if row['first_snapshot'] else 'N/A'}")
        print(f"  Последний снимок              : {str(row['last_snapshot'])[:19] if row['last_snapshot'] else 'N/A'}")

        # ============================================================
        # 12. ИСТОРИЯ ЦЕН СЕГМЕНТОВ
        # ============================================================
        print_header("12. ИСТОРИЯ ЦЕН СЕГМЕНТОВ (segment_price_history)")

        result = conn.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT segment_stats_id) as unique_segments,
                MIN(snapshot_date) as first_date,
                MAX(snapshot_date) as last_date
            FROM segment_price_history
        """))
        row = result.mappings().one()
        print(f"  Всего записей                 : {fmt_num(row['total'])}")
        print(f"  Уникальных сегментов          : {fmt_num(row['unique_segments'])}")
        print(f"  Первая дата                   : {str(row['first_date']) if row['first_date'] else 'N/A'}")
        print(f"  Последняя дата                : {str(row['last_date']) if row['last_date'] else 'N/A'}")

        # ============================================================
        # 13. УВЕДОМЛЕНИЯ
        # ============================================================
        print_header("13. УВЕДОМЛЕНИЯ (notifications_sent)")

        result = conn.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT ad_id) as unique_ads,
                notification_type,
                MIN(sent_at) as first_sent,
                MAX(sent_at) as last_sent
            FROM notifications_sent
            GROUP BY notification_type
        """))
        rows = result.mappings().all()
        if rows:
            for r in rows:
                print(f"  Тип: {r['notification_type']}")
                print(f"    Всего: {fmt_num(r['total'])}, Уникальных объявлений: {fmt_num(r['unique_ads'])}")
                print(f"    Первое: {str(r['first_sent'])[:19]}, Последнее: {str(r['last_sent'])[:19]}")
        else:
            print("  Уведомлений нет")

        # ============================================================
        # 14. ДОСТАТОЧНОСТЬ ДАННЫХ ДЛЯ МЕДИАНЫ
        # ============================================================
        print_header("14. ОЦЕНКА ДОСТАТОЧНОСТИ ДАННЫХ ДЛЯ МЕДИАНЫ")

        # Проверяем по каждому поисковому запросу
        result = conn.execute(text("""
            SELECT
                ts.search_phrase,
                ts.category,
                ts.is_category_search,
                COUNT(a.id) FILTER (WHERE a.price IS NOT NULL) as priced_ads,
                CASE WHEN COUNT(a.id) FILTER (WHERE a.price IS NOT NULL) >= 3
                     THEN true ELSE false END as enough_for_median
            FROM tracked_searches ts
            LEFT JOIN ads a ON a.search_url = ts.search_url
            GROUP BY ts.id, ts.search_phrase, ts.category, ts.is_category_search
            ORDER BY priced_ads DESC
        """))
        rows = result.mappings().all()

        enough_count = 0
        not_enough_count = 0
        print(f"\n  Порог для расчёта медианы: минимум 3 объявления с ценой")
        print(f"\n  {'Поисковый запрос / Категория':45} | {'С ценой':>7} | {'Достаточно':>10}")
        print(f"  {'-'*45}-+-{'-'*7}-+-{'-'*10}")

        for r in rows:
            phrase = (r['search_phrase'] or r['category'] or 'N/A')[:45]
            priced = r['priced_ads'] or 0
            enough = "✓ Да" if r['enough_for_median'] else "✗ Нет"
            if r['enough_for_median']:
                enough_count += 1
            else:
                not_enough_count += 1
            print(f"  {phrase:45} | {priced:>7} | {enough:>10}")

        print(f"\n  ИТОГО:")
        print(f"    Поисков с достаточными данными: {enough_count}")
        print(f"    Поисков с недостаточными данными: {not_enough_count}")

        # Проверяем по сегментам
        result = conn.execute(text("""
            SELECT
                segment_key,
                sample_size,
                CASE WHEN sample_size >= 3 THEN true ELSE false END as enough
            FROM segment_stats
            ORDER BY sample_size DESC
        """))
        seg_rows = result.mappings().all()
        seg_enough = sum(1 for r in seg_rows if r['enough'])
        seg_not_enough = sum(1 for r in seg_rows if not r['enough'])
        print(f"\n  По сегментам:")
        print(f"    Сегментов с sample_size >= 3: {seg_enough}")
        print(f"    Сегментов с sample_size < 3:  {seg_not_enough}")

        # ============================================================
        # 15. АНАЛИЗ ПО segment_key В ОБЪЯВЛЕНИЯХ
        # ============================================================
        print_header("15. ОБЪЯВЛЕНИЯ ПО segment_key")

        result = conn.execute(text("""
            SELECT
                COALESCE(segment_key, '(без сегмента)') as seg_key,
                COUNT(*) as cnt,
                COUNT(*) FILTER (WHERE price IS NOT NULL) as with_price,
                AVG(price) FILTER (WHERE price IS NOT NULL) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)
                    FILTER (WHERE price IS NOT NULL) as median_price
            FROM ads
            GROUP BY segment_key
            ORDER BY cnt DESC
            LIMIT 20
        """))
        rows = result.mappings().all()
        if rows:
            print(f"  {'Сегмент':50} | {'Всего':>6} | {'С ценой':>6} | {'Медиана':>12} | {'Средняя':>12}")
            print(f"  {'-'*50}-+-{'-'*6}-+-{'-'*6}-+-{'-'*12}-+-{'-'*12}")
            for r in rows:
                seg = r['seg_key'][:50]
                cnt = r['cnt'] or 0
                wp = r['with_price'] or 0
                med = fmt_price(r['median_price']) if r['median_price'] else "N/A"
                avg = fmt_price(r['avg_price']) if r['avg_price'] else "N/A"
                print(f"  {seg:50} | {cnt:>6} | {wp:>6} | {med:>12} | {avg:>12}")

        # ============================================================
        # 16. РАСПРЕДЕЛЕНИЕ ПО СОСТОЯНИЮ И ПРОДАВЦАМ
        # ============================================================
        print_header("16. РАСПРЕДЕЛЕНИЕ ПО ПАРАМЕТРАМ")

        # По состоянию
        result = conn.execute(text("""
            SELECT COALESCE(condition, '(не указано)') as cond, COUNT(*) as cnt
            FROM ads GROUP BY condition ORDER BY cnt DESC
        """))
        rows = result.mappings().all()
        print("\n  По состоянию товара:")
        for r in rows:
            print(f"    {r['cond']:30} : {fmt_num(r['cnt'])}")

        # По типу продавца
        result = conn.execute(text("""
            SELECT COALESCE(seller_type, '(не указано)') as stype, COUNT(*) as cnt
            FROM ads GROUP BY seller_type ORDER BY cnt DESC
        """))
        rows = result.mappings().all()
        print("\n  По типу продавца:")
        for r in rows:
            print(f"    {r['stype']:30} : {fmt_num(r['cnt'])}")

        # По location
        result = conn.execute(text("""
            SELECT COALESCE(location, '(не указано)') as loc, COUNT(*) as cnt
            FROM ads GROUP BY location ORDER BY cnt DESC LIMIT 15
        """))
        rows = result.mappings().all()
        print("\n  По местоположению (топ-15):")
        for r in rows:
            print(f"    {r['loc']:30} : {fmt_num(r['cnt'])}")

        # По brand
        result = conn.execute(text("""
            SELECT COALESCE(brand, '(не указан)') as br, COUNT(*) as cnt
            FROM ads GROUP BY brand ORDER BY cnt DESC LIMIT 15
        """))
        rows = result.mappings().all()
        print("\n  По бренду (топ-15):")
        for r in rows:
            print(f"    {r['br']:30} : {fmt_num(r['cnt'])}")

        # ============================================================
        # 17. PRODUCT СТАТИСТИКА
        # ============================================================
        print_header("17. PRODUCT СТАТИСТИКА (products)")

        try:
            result = conn.execute(text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE median_price IS NOT NULL) as with_median,
                    COUNT(*) FILTER (WHERE median_price IS NULL) as without_median,
                    AVG(listing_count) as avg_listings,
                    AVG(median_price) FILTER (WHERE median_price IS NOT NULL) as avg_median,
                    MIN(first_seen_at) as first_seen,
                    MAX(last_seen_at) as last_seen
                FROM products
            """))
            row = result.mappings().one()
            print(f"  Всего продуктов               : {fmt_num(row['total'])}")
            print(f"  С медианой (готовые)         : {fmt_num(row['with_median'])}")
            print(f"  Без медианы (сырьё)          : {fmt_num(row['without_median'])}")
            print(f"  Среднее кол-во объявлений    : {fmt_num(row['avg_listings'])}")
            print(f"  Средняя медианная цена       : {fmt_price(row['avg_median'])}")
            print(f"  Первый обнаружен             : {str(row['first_seen'])[:19] if row['first_seen'] else 'N/A'}")
            print(f"  Последний обнаружен          : {str(row['last_seen'])[:19] if row['last_seen'] else 'N/A'}")
        except Exception as e:
            print(f"  ОШИБКА - {e}")

        # ============================================================
        # 18. SNAPSHOT СТАТИСТИКА (product_price_snapshots)
        # ============================================================
        print_header("18. SNAPSHOT СТАТИСТИКА (product_price_snapshots)")

        try:
            result = conn.execute(text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(DISTINCT product_id) as products_covered,
                    MIN(snapshot_at) as first_snapshot,
                    MAX(snapshot_at) as last_snapshot
                FROM product_price_snapshots
            """))
            row = result.mappings().one()
            print(f"  Всего снапшотов              : {fmt_num(row['total'])}")
            print(f"  Покрыто продуктов            : {fmt_num(row['products_covered'])}")
            print(f"  Первый снапшот               : {str(row['first_snapshot'])[:19] if row['first_snapshot'] else 'N/A'}")
            print(f"  Последний снапшот            : {str(row['last_snapshot'])[:19] if row['last_snapshot'] else 'N/A'}")

            # Снапшоты за последние 24 часа
            result = conn.execute(text("""
                SELECT COUNT(*) as cnt
                FROM product_price_snapshots
                WHERE snapshot_at >= NOW() - INTERVAL '24 hours'
            """))
            row24 = result.mappings().one()
            print(f"  Снапшотов за 24ч             : {fmt_num(row24['cnt'])}")

            # Снапшоты за последние 7 дней
            result = conn.execute(text("""
                SELECT COUNT(*) as cnt
                FROM product_price_snapshots
                WHERE snapshot_at >= NOW() - INTERVAL '7 days'
            """))
            row7d = result.mappings().one()
            print(f"  Снапшотов за 7 дней          : {fmt_num(row7d['cnt'])}")
        except Exception as e:
            print(f"  ОШИБКА - {e}")

        # ============================================================
        # 19. ТОП ПРОДУКТОВ
        # ============================================================
        print_header("19. ТОП ПРОДУКТОВ (по кол-ву объявлений)")

        try:
            result = conn.execute(text("""
                SELECT
                    p.normalized_key,
                    p.brand,
                    p.model,
                    p.category,
                    p.listing_count,
                    p.median_price,
                    p.min_price,
                    p.max_price,
                    (SELECT COUNT(*) FROM product_price_snapshots ps
                     WHERE ps.product_id = p.id) as snapshot_count
                FROM products p
                ORDER BY p.listing_count DESC
                LIMIT 25
            """))
            rows = result.mappings().all()
            if rows:
                print(f"  {'Ключ':45} | {'Бренд':12} | {'Объявл.':>7} | {'Снапш.':>7} | {'Мин':>10} | {'Медиана':>10} | {'Макс':>10}")
                print(f"  {'-'*45}-+-{'-'*12}-+-{'-'*7}-+-{'-'*7}-+-{'-'*10}-+-{'-'*10}-+-{'-'*10}")
                for r in rows:
                    key = (r['normalized_key'] or '')[:45]
                    brand = (r['brand'] or '')[:12]
                    lc = r['listing_count'] or 0
                    sc = r['snapshot_count'] or 0
                    mn = fmt_price(r['min_price']) if r['min_price'] else "N/A"
                    med = fmt_price(r['median_price']) if r['median_price'] else "N/A"
                    mx = fmt_price(r['max_price']) if r['max_price'] else "N/A"
                    print(f"  {key:45} | {brand:12} | {lc:>7} | {sc:>7} | {mn:>10} | {med:>10} | {mx:>10}")
            else:
                print("  Нет данных о продуктах")
        except Exception as e:
            print(f"  ОШИБКА - {e}")

        # ============================================================
        # 20. ГОТОВНОСТЬ К ДЕТЕКЦИИ «БРИЛЛИАНТОВ»
        # ============================================================
        print_header("20. ГОТОВНОСТЬ К ДЕТЕКЦИИ «БРИЛЛИАНТОВ»")

        try:
            result = conn.execute(text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE listing_count >= 3) as with_3plus_listings,
                    COUNT(*) FILTER (WHERE listing_count >= 5) as with_5plus_listings,
                    COUNT(*) FILTER (WHERE median_price IS NOT NULL) as with_median,
                    COUNT(*) FILTER (WHERE listing_count >= 3 AND median_price IS NOT NULL) as ready_for_diamonds
                FROM products
            """))
            row = result.mappings().one()
            print(f"  Всего продуктов               : {fmt_num(row['total'])}")
            print(f"  С >=3 объявлений             : {fmt_num(row['with_3plus_listings'])}")
            print(f"  С >=5 объявлений             : {fmt_num(row['with_5plus_listings'])}")
            print(f"  С медианой                   : {fmt_num(row['with_median'])}")
            print(f"  ГОТОВЫ к детекции            : {fmt_num(row['ready_for_diamonds'])}")

            if row['ready_for_diamonds'] == 0:
                print("\n  ⚠️  НЕТ продуктов, готовых к детекции «бриллиантов»!")
                print("     Возможные причины:")
                print("     1. Мало данных — нужно больше циклов сбора")
                print("     2. Нормализация слишком дробит товары")
                print("     3. Pipeline не пишет product_price_snapshots")
            else:
                print(f"\n  ✅ {fmt_num(row['ready_for_diamonds'])} продуктов готовы к детекции")

            # Дополнительно: продукты с наибольшим числом снапшотов
            result = conn.execute(text("""
                SELECT
                    p.normalized_key,
                    COUNT(ps.id) as snap_count,
                    p.median_price
                FROM products p
                JOIN product_price_snapshots ps ON ps.product_id = p.id
                GROUP BY p.id, p.normalized_key, p.median_price
                ORDER BY snap_count DESC
                LIMIT 10
            """))
            rows = result.mappings().all()
            if rows:
                print(f"\n  Продукты с наибольшим числом снапшотов:")
                print(f"  {'Ключ':50} | {'Снапшоты':>10} | {'Медиана':>12}")
                print(f"  {'-'*50}-+-{'-'*10}-+-{'-'*12}")
                for r in rows:
                    key = (r['normalized_key'] or '')[:50]
                    med = fmt_price(r['median_price']) if r['median_price'] else "N/A"
                    print(f"  {key:50} | {fmt_num(r['snap_count']):>10} | {med:>12}")
        except Exception as e:
            print(f"  ОШИБКА - {e}")

        # ============================================================
        # 21. КАЧЕСТВО НОРМАЛИЗАЦИИ
        # ============================================================
        print_header("21. КАЧЕСТВО НОРМАЛИЗАЦИИ")

        try:
            # Распределение по длине ключа
            result = conn.execute(text("""
                SELECT
                    COUNT(*) as total,
                    AVG(LENGTH(normalized_key)) as avg_key_len,
                    MAX(LENGTH(normalized_key)) as max_key_len,
                    MIN(LENGTH(normalized_key)) as min_key_len,
                    COUNT(*) FILTER (WHERE LENGTH(normalized_key) > 80) as long_keys,
                    COUNT(*) FILTER (WHERE LENGTH(normalized_key) <= 20) as short_keys,
                    COUNT(DISTINCT brand) as unique_brands,
                    COUNT(DISTINCT category) as unique_categories
                FROM products
            """))
            row = result.mappings().one()
            print(f"  Всего уникальных ключей      : {fmt_num(row['total'])}")
            print(f"  Средняя длина ключа          : {fmt_num(row['avg_key_len'])} символов")
            print(f"  Мин. длина ключа             : {fmt_num(row['min_key_len'])}")
            print(f"  Макс. длина ключа            : {fmt_num(row['max_key_len'])}")
            print(f"  Длинных ключей (>80 симв.)   : {fmt_num(row['long_keys'])}")
            print(f"  Коротких ключей (<=20 симв.) : {fmt_num(row['short_keys'])}")
            print(f"  Уникальных брендов           : {fmt_num(row['unique_brands'])}")
            print(f"  Уникальных категорий         : {fmt_num(row['unique_categories'])}")

            # Топ брендов
            result = conn.execute(text("""
                SELECT COALESCE(brand, '(нет бренда)') as br, COUNT(*) as cnt
                FROM products
                GROUP BY brand
                ORDER BY cnt DESC
                LIMIT 15
            """))
            rows = result.mappings().all()
            if rows:
                print(f"\n  Топ брендов по продуктам:")
                for r in rows:
                    print(f"    {r['br']:30} : {fmt_num(r['cnt'])}")

            # Подозрительные ключи (слишком длинные или мусорные)
            result = conn.execute(text("""
                SELECT normalized_key, listing_count
                FROM products
                WHERE LENGTH(normalized_key) > 80
                   OR normalized_key LIKE '%разное%'
                   OR normalized_key LIKE '%прочее%'
                   OR listing_count = 1
                ORDER BY listing_count ASC
                LIMIT 20
            """))
            rows = result.mappings().all()
            if rows:
                print(f"\n  Подозрительные ключи (длинные/мусор/одиночки):")
                print(f"  {'Ключ':65} | {'Объявл.':>7}")
                print(f"  {'-'*65}-+-{'-'*7}")
                for r in rows:
                    key = (r['normalized_key'] or '')[:65]
                    print(f"  {key:65} | {r['listing_count']:>7}")
            else:
                print("\n  ✅ Подозрительных ключей не обнаружено")
        except Exception as e:
            print(f"  ОШИБКА - {e}")

        # ============================================================
        # 22. СВЯЗЬ ADS ↔ PRODUCTS (coverage)
        # ============================================================
        print_header("22. СВЯЗЬ ADS ↔ PRODUCTS (coverage)")

        try:
            # Сколько объявлений привязаны к продуктам через snapshots
            result = conn.execute(text("""
                SELECT
                    (SELECT COUNT(*) FROM ads) as total_ads,
                    (SELECT COUNT(*) FROM product_price_snapshots) as total_snapshots,
                    (SELECT COUNT(DISTINCT ad_id) FROM product_price_snapshots
                     WHERE ad_id IS NOT NULL) as ads_with_snapshots,
                    (SELECT COUNT(*) FROM products) as total_products
            """))
            row = result.mappings().one()
            total_ads = row['total_ads'] or 0
            ads_with_snaps = row['ads_with_snapshots'] or 0
            coverage_pct = (ads_with_snaps / total_ads * 100) if total_ads > 0 else 0
            print(f"  Всего объявлений             : {fmt_num(total_ads)}")
            print(f"  Всего снапшотов              : {fmt_num(row['total_snapshots'])}")
            print(f"  Объявлений со снапшотами     : {fmt_num(ads_with_snaps)} ({coverage_pct:.1f}%)")
            print(f"  Всего продуктов              : {fmt_num(row['total_products'])}")

            if coverage_pct < 10 and total_ads > 50:
                print("\n  ⚠️  НИЗКИЙ COVERAGE! Менее 10% объявлений имеют product-снапшоты.")
                print("     Pipeline может не создавать snapshots корректно.")
            elif coverage_pct > 50:
                print(f"\n  ✅ Coverage хороший: {coverage_pct:.1f}%")
        except Exception as e:
            print(f"  ОШИБКА - {e}")

        print()
        print_separator()
        print("  КОНЕЦ ОТЧЁТА")
        print_separator()


if __name__ == "__main__":
    main()
