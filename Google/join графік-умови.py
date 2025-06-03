"""
Скрипт для розрахунку збігів між таблицею `zp_worktime` та таблицею правил `zp_фктУмовиОплати`.
✅ Підтягує рівень співробітника
✅ Підтягує Rule_ID у worktime (якщо немає помилок і колізій)
✅ Розраховує Matches та Score для кожного запису
✅ Записує колізії у поле `Colision`, інші помилки — у поле `ErrorLog`
"""

import pymysql
from dotenv import load_dotenv
import os

# Завантаження змінних оточення
load_dotenv("C:/Users/la/OneDrive/Pet Wealth/Analytics/Python_script/.env")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

# Ваги для полів
weights = {
    'position': 50,
    'last_name': 40,
    'department': 30,
    'level': 20,
    'shift_type': 10
}

field_mapping = {
    'position': 'Посада',
    'last_name': 'Прізвище',
    'department': 'Відділення',
    'level': 'Рівень',
    'shift_type': 'ТипЗміни'
}

try:
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE,
        cursorclass=pymysql.cursors.DictCursor
    )

    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM zp_worktime")
        worktime_rows = cursor.fetchall()

        for work_row in worktime_rows:
            work_date = work_row['date_shift']
            last_name = str(work_row['last_name']).strip()
            position = str(work_row['position']).strip()
            department = str(work_row['department']).strip()

            error_messages = []
            colision_messages = []

            print(f"\n🔎 Перевірка рядка ID={work_row['idx']} — {last_name}, {position}, {department}, Дата={work_date}")

            # 1️⃣ Підтягуємо рівень
            cursor.execute("""
                SELECT Рівень
                FROM zp_фктРівніСпівробітників
                WHERE TRIM(LOWER(Прізвище)) = %s
                  AND TRIM(LOWER(Посада)) = %s
                  AND TRIM(LOWER(Відділення)) = %s
                  AND ДатаПочатку <= %s
                  AND (ДатаЗакінчення >= %s OR ДатаЗакінчення IS NULL)
            """, (
                last_name.lower(),
                position.lower(),
                department.lower(),
                work_date,
                work_date
            ))
            levels = cursor.fetchall()

            if not levels:
                cursor.execute("""
                    SELECT Рівень
                    FROM zp_фктРівніСпівробітників
                    WHERE TRIM(LOWER(Прізвище)) = %s
                      AND TRIM(LOWER(Посада)) = %s
                      AND (Відділення IS NULL OR TRIM(Відділення) = '')
                      AND ДатаПочатку <= %s
                      AND (ДатаЗакінчення >= %s OR ДатаЗакінчення IS NULL)
                """, (
                    last_name.lower(),
                    position.lower(),
                    work_date,
                    work_date
                ))
                levels = cursor.fetchall()

            if len(levels) > 1:
                error_messages.append(f"Колізія рівнів: знайдено {len(levels)} записів ({last_name}, {position}, {department}) на дату {work_date}.")
                level_value = None
            elif len(levels) == 1:
                level_value = levels[0]['Рівень']
            else:
                error_messages.append(f"Не знайдено рівень для {last_name}, {position}, {department} на дату {work_date}.")
                level_value = None

            cursor.execute("""
                UPDATE zp_worktime
                SET level = %s
                WHERE date_shift = %s AND idx = %s
            """, (level_value, work_row['date_shift'], work_row['idx']))

            # 2️⃣ Підтягуємо правила
            cursor.execute("""
                SELECT *
                FROM zp_фктУмовиОплати
                WHERE ДатаПочатку <= %s AND (ДатаЗакінчення >= %s OR ДатаЗакінчення IS NULL)
            """, (work_date, work_date))
            rules = cursor.fetchall()

            best_matches = []
            for rule in rules:
                matches = 0
                score = 0
                skip_rule = False

                for field, weight in weights.items():
                    work_value = work_row.get(field)
                    rule_value = rule.get(field_mapping[field])

                    work_val_norm = str(work_value).strip().lower() if work_value else ''
                    rule_val_norm = str(rule_value).strip().lower() if rule_value else ''

                    if rule_val_norm:
                        if work_val_norm == rule_val_norm:
                            matches += 1
                            score += weight
                        else:
                            skip_rule = True
                            break
                    # Пусте поле у правила — не додає Matches/Score

                if not skip_rule:
                    best_matches.append({
                        'rule': rule,
                        'matches': matches,
                        'score': score
                    })

            # 3️⃣ Обробка результатів
            top_matches = 0
            top_score = 0
            top_rule = None

            if best_matches:
                best_matches.sort(key=lambda x: (-x['matches'], -x['score']))
                top = best_matches[0]
                top_matches = top['matches']
                top_score = top['score']
                top_rule = top['rule']

                # Колізія, якщо більше одного з однаковими Matches і Score і РІЗНИМИ Rule_ID
                same_top = [bm for bm in best_matches if bm['matches'] == top_matches and bm['score'] == top_score]
                unique_rule_ids = set(bm['rule']['Rule_ID'] for bm in same_top)
                if len(unique_rule_ids) > 1:
                    colision_messages.append(f"Колізія Rule_ID: {', '.join(str(rid) for rid in unique_rule_ids)}")

            # 4️⃣ Запис у worktime
            update_sql = """
                UPDATE zp_worktime
                SET
                    Matches = %s,
                    Score = %s,
                    Colision = %s,
                    СтавкаЗміна = %s,
                    СтавкаГодина = %s,
                    АнЗП = %s,
                    Ан_Призначив = %s,
                    Ан_Виконав = %s,
                    АнЗП_Колективний = %s,
                    Ан_Колективний = %s,
                    Rule_ID = %s,
                    ErrorLog = %s
                WHERE date_shift = %s AND idx = %s
            """

            if top_rule and not colision_messages and not error_messages:
                cursor.execute(update_sql, (
                    top_matches,
                    top_score,
                    '',
                    top_rule['СтавкаЗміна'],
                    top_rule.get('СтавкаГодина', 0),
                    top_rule.get('АнЗП', ''),
                    top_rule.get('Ан_Призначив', 0),
                    top_rule.get('Ан_Виконав', 0),
                    top_rule.get('АнЗП_Колективний', 0),
                    top_rule.get('Ан_Колективний', 0),
                    top_rule.get('Rule_ID'),
                    '',
                    work_row['date_shift'],
                    work_row['idx']
                ))
            else:
                cursor.execute(update_sql, (
                    top_matches,
                    top_score,
                    "\n".join(colision_messages),
                    0.000,
                    0.000,
                    0.000,
                    0.000,
                    0.000,
                    0.000,
                    0.000,
                    None,
                    "\n".join(error_messages),
                    work_row['date_shift'],
                    work_row['idx']
                ))

        connection.commit()
        print("\n✅ Обробка завершена.")
finally:
    connection.close()
