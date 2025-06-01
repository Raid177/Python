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
            last_name = work_row['last_name']
            position = work_row['position']
            department = work_row['department']

            error_messages = []

            # 🔎 1️⃣ Підтягуємо рівень із zp_фктРівніСпівробітників
            level_value = ''
            cursor.execute("""
                SELECT Рівень
                FROM zp_фктРівніСпівробітників
                WHERE Прізвище = %s
                  AND Посада = %s
                  AND Відділення = %s
                  AND ДатаПочатку <= %s
                  AND (ДатаЗакінчення >= %s OR ДатаЗакінчення IS NULL)
            """, (last_name, position, department, work_date, work_date))
            levels = cursor.fetchall()

            if not levels:
                # Шукаємо універсальне відділення
                cursor.execute("""
                    SELECT Рівень
                    FROM zp_фктРівніСпівробітників
                    WHERE Прізвище = %s
                      AND Посада = %s
                      AND (Відділення IS NULL OR Відділення = '')
                      AND ДатаПочатку <= %s
                      AND (ДатаЗакінчення >= %s OR ДатаЗакінчення IS NULL)
                """, (last_name, position, work_date, work_date))
                levels = cursor.fetchall()

            if len(levels) > 1:
                error_message = (f"Колізія рівнів: знайдено {len(levels)} записів "
                                 f"({last_name}, {position}, {department}) на дату {work_date}.")
                print(f"[⚠️] {error_message}")
                error_messages.append(error_message)
            elif len(levels) == 1:
                level_value = levels[0]['Рівень']
            else:
                error_message = (f"Не вдалося визначити рівень для {last_name}, {position}, {department} "
                                 f"на дату {work_date} — відсутній рівень для конкретного відділення і універсального.")
                print(f"[⚠️] {error_message}")
                error_messages.append(error_message)

            # Оновлюємо рівень у worktime
            cursor.execute("""
                UPDATE zp_worktime
                SET level = %s
                WHERE date_shift = %s AND idx = %s
            """, (level_value, work_row['date_shift'], work_row['idx']))

            # 🔎 2️⃣ Підтягуємо умови оплати
            cursor.execute("""
                SELECT * FROM zp_фктУмовиОплати
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
                    rule_field = field_mapping[field]
                    rule_value = rule.get(rule_field)

                    if field == 'level':
                        if rule_value and work_value:
                            if str(work_value).strip().lower() == str(rule_value).strip().lower():
                                matches += 1
                                score += weight
                            else:
                                skip_rule = True
                                break
                        elif not rule_value:
                            # універсальне правило по level
                            matches += 1
                            score += weight
                        else:
                            error_message = (f"Помилка таблиць (Level mismatch)! ID={work_row['idx']} дата={work_date}: "
                                             f"rule_level='{rule_value}' vs worktime_level='{work_value}'.")
                            print(f"[⚠️] {error_message}")
                            error_messages.append(error_message)
                            skip_rule = True
                            break
                    else:
                        if rule_value:
                            if work_value and str(work_value).strip().lower() == str(rule_value).strip().lower():
                                matches += 1
                                score += weight
                            else:
                                skip_rule = True
                                break
                        else:
                            # універсальне правило по цьому полю
                            matches += 1
                            score += weight

                if not skip_rule:
                    best_matches.append({
                        'rule': rule,
                        'matches': matches,
                        'score': score
                    })

            print(f"\n[🔎] Перевірка правил для ID {work_row['idx']} на дату {work_date}:")
            for bm in best_matches:
                rule_id = bm['rule']['id']
                m = bm['matches']
                s = bm['score']
                print(f"  🔸 Правило ID={rule_id} → Matches={m}, Score={s}")

            colision = ""
            top_matches = 0
            top_score = 0

            if best_matches:
                best_matches.sort(key=lambda x: (-x['matches'], -x['score']))

                top = best_matches[0]
                top_matches = top['matches']
                top_score = top['score']
                top_rule = top['rule']

                same_top = [bm for bm in best_matches if bm['matches'] == top_matches and bm['score'] == top_score]
                if len(same_top) > 1:
                    colision = "Колізія: " + ", ".join([str(bm['rule']['id']) for bm in same_top])
                    print(f"[⚠️] Колізія для ID {work_row['idx']} на дату {work_date}: {colision}")
                    error_messages.append(f"Колізія: {colision}")

            # 🔥 3️⃣ Запис у worktime
            if error_messages or not best_matches:
                # помилка або немає правил → обнуляємо зарплату
                cursor.execute("""
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
                        ErrorLog = %s
                    WHERE
                        date_shift = %s AND idx = %s
                """, (
                    top_matches,
                    top_score,
                    colision,
                    0.000,
                    0.000,
                    0.000,
                    0.000,
                    0.000,
                    0.000,
                    0.000,
                    "\n".join(error_messages),
                    work_row['date_shift'],
                    work_row['idx']
                ))
                print(f"[⚠️] Помилка: зарплата не розрахована для ID {work_row['idx']}.")
            else:
                cursor.execute("""
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
                        ErrorLog = %s
                    WHERE
                        date_shift = %s AND idx = %s
                """, (
                    top_matches,
                    top_score,
                    colision,
                    top_rule['СтавкаЗміна'],
                    top_rule['СтавкаГодина'],
                    top_rule['АнЗП'],
                    top_rule['Ан_Призначив'],
                    top_rule['Ан_Виконав'],
                    top_rule['АнЗП_Колективний'],
                    top_rule['Ан_Колективний'],
                    "\n".join(error_messages),
                    work_row['date_shift'],
                    work_row['idx']
                ))
                print(f"[✅] Оновлено рядок ID {work_row['idx']} Matches={top_matches}, Score={top_score}")

        connection.commit()
        print("\n[🎉] Обробка завершена.")
finally:
    connection.close()
