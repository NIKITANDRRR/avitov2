"""Данные для нормализатора товаров v2.

Содержит словари брендов, шумовых слов, product-specific правила
и бренд-специфичные паттерны моделей.

См. plans/normalizer_v2_plan.md — полное описание архитектуры.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Маппинги брендов (рус → англ, алиасы)
# ---------------------------------------------------------------------------

BRAND_ALIASES: dict[str, str] = {
    # === Apple ===
    "iphone": "apple",
    "айфон": "apple",
    "ipad": "apple",
    "айпад": "apple",
    "macbook": "apple",
    "макбук": "apple",
    "airpods": "apple",
    "эйрподс": "apple",
    "apple": "apple",
    "appletv": "apple",
    "imac": "apple",
    "mac": "apple",

    # === Samsung ===
    "samsung": "samsung",
    "самсунг": "samsung",
    "galaxy": "samsung",

    # === LG ===
    "lg": "lg",
    "элджи": "lg",

    # === Sony ===
    "sony": "sony",
    "сони": "sony",
    "playstation": "sony",
    "ps5": "sony",
    "ps4": "sony",
    "bravia": "sony",
    "xperia": "sony",

    # === Xiaomi ===
    "xiaomi": "xiaomi",
    "сяоми": "xiaomi",
    "redmi": "xiaomi",
    "poco": "xiaomi",
    "mi": "xiaomi",

    # === Huawei / Honor ===
    "huawei": "huawei",
    "хуавей": "huawei",
    "honor": "honor",
    "хонор": "honor",

    # === NVIDIA ===
    "nvidia": "nvidia",
    "geforce": "nvidia",
    "shield": "nvidia",

    # === Google ===
    "google": "google",
    "pixel": "google",
    "chromecast": "google",

    # === Аудио ===
    "jbl": "jbl",
    "marshall": "marshall",
    "маршалл": "marshall",
    "yamaha": "yamaha",
    "ямаха": "yamaha",
    "bose": "bose",
    "sennheiser": "sennheiser",
    "bang": "bangolufsen",
    "olufsen": "bangolufsen",
    "harman": "harman",
    "kardon": "harman",
    "jvc": "jvc",
    "pioneer": "pioneer",
    "kenwood": "kenwood",
    "denon": "denon",
    "onkyo": "onkyo",
    "creative": "creative",
    "logitech": "logitech",
    "razer": "razer",
    "fender": "fender",
    "behringer": "behringer",

    # === ТВ ===
    "hisense": "hisense",
    "хайсенс": "hisense",
    "tcl": "tcl",
    "philips": "philips",
    "филипс": "philips",
    "toshiba": "toshiba",
    "sharp": "sharp",
    "supra": "supra",
    "dexp": "dexp",
    "haier": "haier",
    "skyworth": "skyworth",

    # === Велосипеды ===
    "stels": "stels",
    "стелс": "stels",
    "trek": "trek",
    "merida": "merida",
    "forward": "forward",
    "форвард": "forward",
    "altair": "altair",
    "альтаир": "altair",
    "author": "author",
    "giant": "giant",
    "specialized": "specialized",
    "scott": "scott",
    "cube": "cube",
    "cannondale": "cannondale",
    "norco": "norco",
    "bianchi": "bianchi",
    "orbea": "orbea",
    "focus": "focus",
    "lapierre": "lapierre",
    "kross": "kross",
    "outleap": "outleap",
    "format": "format",
    "triad": "triad",
    "shulz": "shulz",
    "шульц": "shulz",
    "stern": "stern",

    # === ТВ-приставки ===
    "beelink": "beelink",
    "minix": "minix",
    "tanix": "tanix",
    "mecool": "mecool",

    # === Ноутбуки / ПК ===
    "asus": "asus",
    "acer": "acer",
    "lenovo": "lenovo",
    "hp": "hp",
    "dell": "dell",
    "msi": "msi",
    "gigabyte": "gigabyte",
    "thinkpad": "lenovo",
    "ideapad": "lenovo",
    "legion": "lenovo",
    "predator": "acer",
    "nitro": "acer",
    "rog": "asus",
    "zenbook": "asus",
    "vivobook": "asus",
    "omen": "hp",
    "pavilion": "hp",
    "elitebook": "hp",
    "inspiron": "dell",
    "xps": "dell",
    "alienware": "dell",

    # === Фото/Видео ===
    "canon": "canon",
    "nikon": "nikon",
    "gopro": "gopro",
    "dji": "dji",
    "fuji": "fujifilm",
    "fujifilm": "fujifilm",
    "olympus": "olympus",
    "panasonic": "panasonic",
    "lumix": "panasonic",
    "leica": "leica",

    # === Инструменты ===
    "bosch": "bosch",
    "бош": "bosch",
    "makita": "makita",
    "макита": "makita",
    "dewalt": "dewalt",
    "metabo": "metabo",
    "ryobi": "ryobi",
    "milwaukee": "milwaukee",
    "зубр": "zubr",
    "zubr": "zubr",
    "интерскол": "interskol",
    "interskol": "interskol",

    # === Пылесосы / Бытовая техника ===
    "dyson": "dyson",
    "дайсон": "dyson",
    "tefal": "tefal",
    "bork": "bork",
    "kitfort": "kitfort",
    "braun": "braun",

    # === Игры / Консоли ===
    "nintendo": "nintendo",
    "xbox": "microsoft",
    "microsoft": "microsoft",

    # === Телефоны ===
    "oneplus": "oneplus",
    "oppo": "oppo",
    "vivo": "vivo",
    "realme": "realme",
    "nothing": "nothing",

    # === Аксессуары (бренды) ===
    "anker": "anker",
    "baseus": "baseus",
    "ugreen": "ugreen",
}

# ---------------------------------------------------------------------------
# Шумовые слова — удаляются из заголовка перед нормализацией
# ---------------------------------------------------------------------------

NOISE_WORDS_SET: frozenset[str] = frozenset({
    # --- Состояние ---
    "новый", "новая", "новое", "новые", "нов",
    "б/у", "бу", "б.у",
    "идеал", "идеальное", "идеальный", "идеально",
    "отличн", "отличное", "отличный", "отлично",
    "хорош", "хорошее", "хороший", "хорошо",
    "удовлетворительное", "рабочий", "рабочая", "рабочее",
    "состояние", "сост",
    "практически",
    "работает", "проверен", "проверенный",
    "тест", "тестиров",
    "целый", "целая",
    "sealed", "new", "used", "refurbished", "like",
    "mint", "opened", "unopened", "locked", "unlocked",
    "box",

    # --- Коммерческие ---
    "в", "наличии", "вналичии", "наличие",
    "под", "заказ", "подзаказ",
    "скидк", "скидка", "скидки", "акция",
    "торг", "торгуюсь", "обмен", "меняю",
    "доставка", "самовывоз", "отправк", "отправляю",
    "оптом", "розниц", "розница",
    "купить", "продать", "продаю", "куплю", "продажа",
    "срочно", "быстро", "дешево", "недорого", "выгодно",
    "цена", "цен", "руб", "рублей", "₽",
    "оригинал", "копия", "реплика", "подделк", "лицензия",
    "гарантия", "гаранти", "гарант",
    "чек", "документ", "докум",
    "комплект", "коробк", "упаковк", "заводск",

    # --- Цвета ---
    "черный", "чёрный", "черная", "чёрная", "черное", "чёрное",
    "белый", "белая", "белое", "белые",
    "серый", "серая", "серое", "серые",
    "золотой", "золотая", "золотое",
    "красный", "красная", "красное",
    "синий", "синяя", "синее",
    "зеленый", "зелёный", "зеленая", "зелёная",
    "голубой", "голубая",
    "розовый", "розовая",
    "фиолетовый", "фиолетовая",
    "бирюзовый", "бирюзовая",
    "графитовый", "графит",
    "серебристый", "серебро", "серебристая",
    "темно", "тёмно",
    "бордовый", "коричневый", "оранжевый", "желтый", "жёлтый",
    "персиковый", "бежевый", "хаки", "камуфляж",
    "color", "цвет", "цвета", "цветов", "расцветк",

    # --- Города и регионы ---
    "россия", "москва", "спб", "казань", "новосибирск",
    "екб", "тюмень", "челябинск", "екатеринбург", "самара",
    "ростов", "краснодар", "нижний", "новгород", "воронеж",
    "мск", "spb", "санкт-петербург",

    # --- Прочий шум ---
    "рст", "ростест", "евротест", "сша", "глобал", "global",
    "eac", "certified", "реестр",
    "шт", "штука", "комплектация",
    "класс", "выбор", "размер", "размеры",
    "модель", "арт", "артикул",
    "подходит", "совместим", "универсальный",
    "пульт", "кабель", "зарядка",  # аксессуары
    "чехол", "крышка", "подставка", "кронштейн", "крепление",
    "ремешок", "браслет", "стекло", "пленка", "плёнка",
    "переходник", "удлинитель",
    "лучшая", "лучший", "лучшее", "популярный", "топ",
    "оригинальный", "оригинальная",

    # --- Generic слова категорий ---
    "телевизор", "телевизоры",
    "колонка", "колонки", "саундбар",
    "велосипед", "велосипеды",
    "приставка", "приставки",
    "телефон", "смартфон",
    "наушники",
    "ноутбук", "планшет",
    "часы",
    "камера", "видеокамера",
    "монитор",
    "принтер",
    "роутер",

    # --- Предлоги и местоимения ---
    "и", "с", "за", "из", "по", "от", "до", "для", "к", "у", "о",
    "the", "a", "an", "in", "on", "at", "for",
    "with", "from", "to", "of",
    "is", "are", "was", "were", "be", "been", "being",
    "or", "not", "no", "yes",
    "without",
})

# ---------------------------------------------------------------------------
# Модификаторы моделей — НЕ удаляются как шум
# ---------------------------------------------------------------------------

MODEL_MODIFIERS: frozenset[str] = frozenset({
    "pro", "max", "mini", "ultra", "lite", "plus", "air",
    "se", "fe", "neo", "edge", "super", "xl", "xxl",
    "classic", "elite", "prime", "sport", "active",
})

# ---------------------------------------------------------------------------
# Product-specific правила
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProductRule:
    """Правило для известного продукта."""

    pattern: re.Pattern[str]
    canonical_key: str
    brand: str
    model: str


PRODUCT_RULES: list[ProductRule] = [
    # === NVIDIA Shield (порядок: от специфичного к общему) ===
    ProductRule(
        pattern=re.compile(r"nvidia\s+shield\s+(?:tv\s+)?pro\s+2019", re.I),
        canonical_key="nvidia_shield_tv_pro_2019",
        brand="nvidia",
        model="shield_tv_pro_2019",
    ),
    ProductRule(
        pattern=re.compile(r"nvidia\s+shield\s+(?:tv\s+)?(?:2019|tube)", re.I),
        canonical_key="nvidia_shield_tv_2019",
        brand="nvidia",
        model="shield_tv_2019",
    ),
    ProductRule(
        pattern=re.compile(r"nvidia\s+shield\s+(?:tv\s+)?pro\b", re.I),
        canonical_key="nvidia_shield_tv_pro_2019",
        brand="nvidia",
        model="shield_tv_pro_2019",
    ),
    ProductRule(
        pattern=re.compile(r"nvidia\s+shield\s+tv\b", re.I),
        canonical_key="nvidia_shield_tv_2019",
        brand="nvidia",
        model="shield_tv_2019",
    ),
    ProductRule(
        pattern=re.compile(r"nvidia\s+shield\b", re.I),
        canonical_key="nvidia_shield_tv",
        brand="nvidia",
        model="shield_tv",
    ),

    # === Apple TV ===
    ProductRule(
        pattern=re.compile(r"apple\s*tv\s*4k", re.I),
        canonical_key="apple_tv_4k",
        brand="apple",
        model="apple_tv_4k",
    ),
    ProductRule(
        pattern=re.compile(r"apple\s*tv\b", re.I),
        canonical_key="apple_tv",
        brand="apple",
        model="apple_tv",
    ),

    # === Google Chromecast ===
    ProductRule(
        pattern=re.compile(r"chromecast\s*(?:with\s*)?google\s*tv\s*4k", re.I),
        canonical_key="google_chromecast_4k",
        brand="google",
        model="chromecast_4k",
    ),
    ProductRule(
        pattern=re.compile(r"chromecast\b", re.I),
        canonical_key="google_chromecast",
        brand="google",
        model="chromecast",
    ),

    # === Samsung The Frame ===
    ProductRule(
        pattern=re.compile(r"(?:samsung\s+)?(?:the\s+)?frame\b", re.I),
        canonical_key="samsung_the_frame",
        brand="samsung",
        model="the_frame",
    ),

    # === Samsung SWA (тыловые колонки) ===
    ProductRule(
        pattern=re.compile(r"swa\s*[-]?\s*8500s?", re.I),
        canonical_key="samsung_swa_8500s",
        brand="samsung",
        model="swa_8500s",
    ),
    ProductRule(
        pattern=re.compile(r"swa\s*[-]?\s*9100s?", re.I),
        canonical_key="samsung_swa_9100s",
        brand="samsung",
        model="swa_9100s",
    ),
]

# ---------------------------------------------------------------------------
# Бренд-специфичные паттерны моделей
# ---------------------------------------------------------------------------

# Каждый паттерн — tuple(regex, callable для нормализации match → str)
BRAND_MODEL_PATTERNS: dict[str, list[tuple[re.Pattern[str], object]]] = {
    "apple": [
        # iPhone 13 Pro Max → iphone_13_pro_max
        (
            re.compile(r"iphone\s*\d+\s*(?:pro(?:\s*max)?|max|mini|plus|se)?", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        # iPad Air 5 → ipad_air_5
        (
            re.compile(r"ipad\s*(?:air|mini|pro)?\s*\d*", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        # MacBook Air M2 → macbook_air_m2
        (
            re.compile(r"macbook\s*(?:air|pro)?\s*(?:m\d+)?", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        # AirPods Pro 2 → airpods_pro_2
        (
            re.compile(r"airpods\s*(?:pro|max)?\s*\d*", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
    ],
    "samsung": [
        # Galaxy S21 Ultra → galaxy_s21_ultra
        (
            re.compile(r"galaxy\s*[szanm]\d+\s*(?:ultra|plus|fe|neo|edge|lite)?", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        # Galaxy Note 20 → galaxy_note_20
        (
            re.compile(r"galaxy\s*note\s*\d+\s*(?:ultra|plus|fe)?", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        # UE55TU8000 / QE55Q80C — модельный номер ТВ
        (
            re.compile(r"[a-z]{1,3}\d{2}[a-z]{1,4}\d{3,4}[a-z]*", re.I),
            lambda m: m.group(0).lower(),
        ),
    ],
    "jbl": [
        (
            re.compile(r"partybox\s*\d+", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        (
            re.compile(r"charge\s*\d+", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        (
            re.compile(r"flip\s*\d+", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        (
            re.compile(r"boombox\s*\d+", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        (
            re.compile(r"tune\s*\d+\w*", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        (
            re.compile(r"live\s*\d+\w*", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        (
            re.compile(r"stage\s*\w*\d+", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        (
            re.compile(r"bar\s*[\d.]+", re.I),
            lambda m: m.group(0).lower().replace(" ", "_").replace(".", "_"),
        ),
        (
            re.compile(r"endurance\s*\w+", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        (
            re.compile(r"reflect\s*\w+", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
    ],
    "stels": [
        (
            re.compile(r"navigator\s*\d+\s*\w*", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
        (
            re.compile(r"pilot\s*\d+", re.I),
            lambda m: m.group(0).lower().replace(" ", "_"),
        ),
    ],
    "lg": [
        # OLED55C2 / 55UN81006LA — модельный номер ТВ
        (
            re.compile(r"(?:oled)?\d{2}[a-z]{1,3}\d{1,4}[a-z]*", re.I),
            lambda m: m.group(0).lower(),
        ),
    ],
}

# ---------------------------------------------------------------------------
# Бренды по категориям (для контекстного извлечения)
# ---------------------------------------------------------------------------

CATEGORY_BRANDS: dict[str, list[str]] = {
    "телевизоры": [
        "samsung", "lg", "hisense", "tcl", "sony", "philips",
        "xiaomi", "acer", "toshiba", "sharp", "supra", "dexp",
        "haier", "skyworth",
    ],
    "саундбары": [
        "jbl", "sony", "samsung", "yamaha", "marshall", "bose",
        "lg", "philips", "pioneer", "denon", "onkyo", "creative",
    ],
    "велосипеды": [
        "stels", "trek", "merida", "forward", "altair", "author",
        "giant", "specialized", "scott", "cube", "cannondale",
        "norco", "bianchi", "orbea", "focus", "lapierre",
        "kross", "outleap", "format", "triad", "shulz", "stern",
    ],
    "тв приставки": [
        "nvidia", "apple", "google", "xiaomi", "beelink",
        "minix", "tanix", "mecool",
    ],
}
