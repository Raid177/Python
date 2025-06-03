"""
Скрипт для розрахунку збігів між таблицею `zp_worktime` та таблицею правил `zp_фктУмовиОплати`.
✅ Підтягує рівень співробітника
✅ Підтягує Rule_ID у worktime (якщо немає помилок і колізій)
✅ Розраховує Matches та Score для кожного запису
✅ Записує колізії у поле `Colision`, інші помилки — у поле `ErrorLog`
"""

import os
import pymysql
from dotenv import load_dotenv

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
        # Завантажуємо всі рядки worktime
        cursor.execute("SELECT * FROM zp_worktime")
        worktime_rows = cursor.fetchall()

        # Завантажуємо всі рівні співробітників
        cursor.execute("SELECT * FROM zp_фктРівніСпівробітників")
        levels_rows = cursor.fetchall()

        # Завантажуємо всі активні правила
        cursor.execute("""
            SELECT *
            FROM zp_фктУмовиОплати
            WHERE ДатаЗакінчення IS NULL OR ДатаЗакінчення >= CURDATE()
        """)
        rules_rows = cursor.fetchall()

        # Створюємо мапу рівнів
        levels_map = {}
        for level in levels_rows:
            key = (
                level['Прізвище'].strip().lower(),
                level['Посада'].strip().lower(),
                level['Відділення'].strip().lower()
            )
            levels_map.setdefault(key, []).append(level)

        for work_row in worktime_rows:
            work_date = work_row['date_shift']
            last_name = str(work_row['last_name']).strip()
            position = str(work_row['position']).strip()
            department = str(work_row['department']).strip()

            error_messages = []
            colision_messages = []

            key_specific = (last_name.lower(), position.lower(), department.lower())
            key_generic = (last_name.lower(), position.lower(), '')

            matched_levels = levels_map.get(key_specific, []) + levels_map.get(key_generic, [])
            matched_levels = [
                lvl for lvl in matched_levels
                if lvl['ДатаПочатку'] <= work_date and 
                   (lvl['ДатаЗакінчення'] is None or lvl['ДатаЗакінчення'] >= work_date)
            ]

            if len(matched_levels) > 1:
                error_messages.append(f"Колізія рівнів: знайдено {len(matched_levels)} записів ({last_name}, {position}, {department}) на дату {work_date}.")
                level_value = None
            elif len(matched_levels) == 1:
                level_value = matched_levels[0]['Рівень']
            else:
                error_messages.append(f"Не знайдено рівень для {last_name}, {position}, {department} на дату {work_date}.")
                level_value = None

            cursor.execute("""
                UPDATE zp_worktime
                SET level = %s
                WHERE date_shift = %s AND idx = %s
            """, (level_value, work_row['date_shift'], work_row['idx']))

            best_matches = []
            for rule in rules_rows:
                if not (rule['ДатаПочатку'] <= work_date and 
                        (rule['ДатаЗакінчення'] is None or rule['ДатаЗакінчення'] >= work_date)):
                    continue

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

                if not skip_rule:
                    best_matches.append({
                        'rule': rule,
                        'matches': matches,
                        'score': score
                    })

            top_matches = 0
            top_score = 0
            top_rule = None

            if best_matches:
                best_matches.sort(key=lambda x: (-x['matches'], -x['score']))
                top = best_matches[0]
                top_matches = top['matches']
                top_score = top['score']
                top_rule = top['rule']

                same_top = [bm for bm in best_matches if bm['matches'] == top_matches and bm['score'] == top_score]
                unique_rule_ids = set(bm['rule']['Rule_ID'] for bm in same_top)
                if len(unique_rule_ids) > 1:
                    colision_messages.append(f"Колізія Rule_ID: {', '.join(str(rid) for rid in unique_rule_ids)}")

            # Оновлюємо worktime
            update_sql = """
                UPDATE zp_worktime
                SET
                    Matches = %s,
                    Score = %s,
                    Colision = %s,
                    СтавкаЗміна = %s,
                    СтавкаГодина = %s,
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
                    float(top_rule['СтавкаЗміна']) / 12 if top_rule['СтавкаЗміна'] else 0,
                    top_rule['Rule_ID'],
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
                    None,
                    "\n".join(error_messages),
                    work_row['date_shift'],
                    work_row['idx']
                ))

        connection.commit()
        print("\n✅ Обробка завершена.")
finally:
    connection.close()
