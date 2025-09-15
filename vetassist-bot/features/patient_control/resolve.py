# resolve.py
# Завдання: дістати ВСІ номери пацієнтів із шапок повідомлень
# (рядки формату "<Ім'я (123) Прізвище>, [dd.mm.yyyy hh:mm]"), ігноруючи "Pet Wealth".

import re
from typing import List

# Рядок-шапка: "<display_name>, [dd.mm.yyyy hh:mm]" (час може бути з секундами)
_HEADER_RE = re.compile(r"^(?P<name>.+?),\s*\[\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}(?::\d{2})?\]\s*$")

# У дужках можуть бути кілька чисел і розділювачі
def _extract_numbers_from_name(name: str) -> List[str]:
    # знайти усі групи в дужках і з них дістати всі числові токени
    nums: List[str] = []
    for paren in re.findall(r"\(([^)]*\d[^)]*)\)", name):
        tokens = re.split(r"\D+", paren)
        nums.extend([t for t in tokens if t.isdigit()])
    # унікальні з збереженням порядку
    seen = set()
    out: List[str] = []
    for n in nums:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out

def extract_patient_numbers(raw_text: str) -> List[str]:
    """
    Пробігаємося рядками, беремо тільки шапки з датою/часом.
    Ігноруємо шапки, що починаються з 'Pet Wealth'.
    Повертаємо унікальні номери в порядку появи.
    """
    if not raw_text:
        return []

    nums: List[str] = []
    seen = set()

    for line in (ln.strip() for ln in raw_text.splitlines() if ln.strip()):
        m = _HEADER_RE.match(line)
        if not m:
            continue
        name = m.group("name")
        if name.lower().startswith("pet wealth"):
            continue  # це наш співробітник/бот — пропускаємо
        # дістаємо всі номери з частини імені
        found = _extract_numbers_from_name(name)
        for n in found:
            if n not in seen:
                seen.add(n)
                nums.append(n)

    return nums
