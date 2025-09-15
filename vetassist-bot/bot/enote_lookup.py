
# enote_lookup.py
# Завдання: за списком номерів пацієнтів повернути клички (поки заглушка).
# Далі підключимо OData і будемо повертати реальні клички та GUID.

from typing import Dict, List

def get_patient_names_by_numbers(numbers: List[str]) -> Dict[str, str]:
    """
    Вхід: ["1472","1396","1475"]
    Вихід: {"1472": "Барні", "1396": "Капучино", ...} (поки порожньо)
    """
    return {n: "" for n in numbers}

