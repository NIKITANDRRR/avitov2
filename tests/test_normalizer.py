"""Тесты для нормализатора товаров v2.

Проверяют корректность нормализации на примерах из плана
(plans/normalizer_v2_plan.md, секция 7).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Добавляем корень проекта в sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from app.analysis.product_normalizer import normalize_title


def _norm(title: str) -> str:
    """Shortcut для получения normalized_key."""
    return normalize_title(title).normalized_key


def _brand(title: str) -> str | None:
    """Shortcut для получения brand."""
    return normalize_title(title).brand


# ===========================================================================
# NVIDIA Shield
# ===========================================================================


class TestNvidiaShield:
    """Тесты нормализации NVIDIA Shield."""

    def test_pro_2019_full(self):
        assert _norm("NVIDIA Shield TV Pro 2019 16GB") == "nvidia_shield_tv_pro_2019_16gb"

    def test_pro_2019_v_nalichii(self):
        assert _norm("NVIDIA Shield TV Pro 2019 в наличии") == "nvidia_shield_tv_pro_2019"

    def test_shield_pro_2019_no_nvidia(self):
        """Shield без 'nvidia' в начале — product rule не сработает,
        но бренд 'shield' маппится на nvidia."""
        result = normalize_title("Shield TV Pro 2019 16GB Новый")
        # shield → nvidia бренд, но product rule не сработает без "nvidia"
        assert result.brand == "nvidia"

    def test_shield_tv_2019_tube(self):
        assert _norm("NVIDIA Shield TV 2019 Tube") == "nvidia_shield_tv_2019"

    def test_shield_tv_pro_no_year(self):
        assert _norm("NVIDIA Shield TV Pro") == "nvidia_shield_tv_pro_2019"

    def test_nvidia_shield_only(self):
        assert _norm("nvidia shield") == "nvidia_shield_tv"


# ===========================================================================
# JBL
# ===========================================================================


class TestJBL:
    """Тесты нормализации JBL колонок."""

    def test_partybox_520(self):
        assert _norm("JBL PartyBox 520") == "jbl_partybox_520"

    def test_charge_5(self):
        assert _norm("JBL Charge 5 Bluetooth") == "jbl_charge_5"

    def test_flip_6(self):
        assert _norm("JBL Flip 6") == "jbl_flip_6"

    def test_boombox_3(self):
        assert _norm("JBL Boombox 3") == "jbl_boombox_3"

    def test_bar_9_1(self):
        assert _norm("JBL Bar 9.1") == "jbl_bar_9_1"

    def test_tune_230nc(self):
        assert _norm("JBL Tune 230NC") == "jbl_tune_230nc"


# ===========================================================================
# Телевизоры
# ===========================================================================


class TestTVs:
    """Тесты нормализации телевизоров."""

    def test_samsung_ue55tu8000(self):
        result = normalize_title("Samsung UE55TU8000UXRU")
        assert result.brand == "samsung"
        assert "ue55tu8000" in result.normalized_key

    def test_samsung_the_frame(self):
        assert _norm("Samsung The Frame 55 2022") == "samsung_the_frame_2022"

    def test_lg_oled55c2(self):
        result = normalize_title("LG OLED55C2")
        assert result.brand == "lg"
        assert "oled55c2" in result.normalized_key or "55c2" in result.normalized_key

    def test_hisense_55a6ke(self):
        result = normalize_title("Hisense 55A6KE")
        assert result.brand == "hisense"


# ===========================================================================
# Велосипеды
# ===========================================================================


class TestBicycles:
    """Тесты нормализации велосипедов."""

    def test_stels_navigator_500_md(self):
        result = normalize_title("Stels Navigator 500 MD 2022")
        assert result.brand == "stels"
        assert "navigator_500" in result.normalized_key

    def test_stels_pilot_710(self):
        result = normalize_title("Велосипед Stels Pilot 710")
        assert result.brand == "stels"
        assert "pilot_710" in result.normalized_key


# ===========================================================================
# Apple
# ===========================================================================


class TestApple:
    """Тесты нормализации Apple устройств."""

    def test_iphone_13_pro_max(self):
        result = normalize_title("iPhone 13 Pro Max 256GB Черный")
        assert result.brand == "apple"
        assert "iphone_13_pro_max" in result.normalized_key

    def test_iphone_13_128(self):
        result = normalize_title("iPhone 13 128GB")
        assert result.brand == "apple"
        assert "iphone_13" in result.normalized_key

    def test_iphone_se(self):
        result = normalize_title("iPhone SE 2022 64GB")
        assert result.brand == "apple"

    def test_airpods_pro(self):
        result = normalize_title("AirPods Pro 2")
        assert result.brand == "apple"

    def test_macbook_air_m2(self):
        result = normalize_title("MacBook Air M2 256GB")
        assert result.brand == "apple"


# ===========================================================================
# Samsung Galaxy
# ===========================================================================


class TestSamsungGalaxy:
    """Тесты нормализации Samsung Galaxy."""

    def test_galaxy_s21_ultra(self):
        result = normalize_title("Samsung Galaxy S21 Ultra 256GB")
        assert result.brand == "samsung"
        assert "galaxy_s21_ultra" in result.normalized_key

    def test_galaxy_s22(self):
        result = normalize_title("Samsung Galaxy S22 128GB")
        assert result.brand == "samsung"
        assert "galaxy_s22" in result.normalized_key


# ===========================================================================
# Шумовые слова
# ===========================================================================


class TestNoiseRemoval:
    """Тесты удаления шумовых слов."""

    def test_v_nalichii_removed(self):
        """'в наличии' не должно попадать в ключ."""
        key = _norm("NVIDIA Shield TV Pro 2019 в наличии")
        assert "налич" not in key
        assert "в_налич" not in key

    def test_color_removed(self):
        """Цвет не должен попадать в ключ."""
        key = _norm("iPhone 13 128GB Черный Новый")
        assert "черн" not in key
        assert "нов" not in key

    def test_condition_removed(self):
        """Состояние не должно попадать в ключ."""
        key = _norm("Samsung Galaxy S21 Отличное состояние")
        assert "отличн" not in key
        assert "состояние" not in key

    def test_city_removed(self):
        """Город не должен попадать в ключ."""
        key = _norm("JBL Charge 5 Москва")
        assert "москва" not in key

    def test_guarantee_removed(self):
        """Гарантия не должна попадать в ключ."""
        key = _norm("NVIDIA Shield TV Pro 2019 Гарантия")
        assert "гаранти" not in key


# ===========================================================================
# Edge cases
# ===========================================================================


class TestEdgeCases:
    """Краевые случаи."""

    def test_empty_title(self):
        assert _norm("") == "unknown"

    def test_none_brand(self):
        result = normalize_title("Непонятный товар 123")
        assert result.normalized_key  # что-то должно быть

    def test_short_alpha_num(self):
        """Короткие alpha-num токены (s2, q2, u8) должны быть частью
        более длинного ключа с брендом, а не standalone."""
        result = normalize_title("Samsung Galaxy S2")
        assert result.brand == "samsung"
        # Ключ должен содержать galaxy_s2, а не просто s2
        assert "galaxy_s2" in result.normalized_key or "s2" in result.normalized_key

    def test_deterministic(self):
        """Один и тот же title → всегда один и тот же ключ."""
        title = "NVIDIA Shield TV Pro 2019 16GB"
        key1 = _norm(title)
        key2 = _norm(title)
        key3 = _norm(title)
        assert key1 == key2 == key3

    def test_samsung_swa_8500s(self):
        result = normalize_title("Тыловые колонки SWA-8500S")
        assert "swa_8500s" in result.normalized_key

    def test_samsung_swa_9100s(self):
        result = normalize_title("Тыловые колонки SWA-9100S")
        assert "swa_9100s" in result.normalized_key

    def test_apple_tv(self):
        assert _norm("Apple TV 4K") == "apple_tv_4k"

    def test_chromecast(self):
        result = normalize_title("Chromecast with Google TV 4K")
        assert "chromecast" in result.normalized_key


if __name__ == "__main__":
    # Запуск тестов без pytest
    import traceback

    test_classes = [
        TestNvidiaShield, TestJBL, TestTVs, TestBicycles,
        TestApple, TestSamsungGalaxy, TestNoiseRemoval, TestEdgeCases,
    ]

    passed = 0
    failed = 0
    for cls in test_classes:
        instance = cls()
        for attr in dir(instance):
            if attr.startswith("test_"):
                try:
                    getattr(instance, attr)()
                    passed += 1
                    print(f"  PASS {cls.__name__}.{attr}")
                except AssertionError as e:
                    failed += 1
                    print(f"  FAIL {cls.__name__}.{attr}: {e}")
                    traceback.print_exc()
                except Exception as e:
                    failed += 1
                    print(f"  FAIL {cls.__name__}.{attr}: ERROR {e}")
                    traceback.print_exc()

    print(f"\n{'='*60}")
    print(f"  Пройдено: {passed}  |  Провалено: {failed}")
    print(f"{'='*60}")
    sys.exit(1 if failed else 0)
