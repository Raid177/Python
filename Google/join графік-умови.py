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

            print(f"\n🔎 Перевірка рівня для ID={work_row['idx']} Прізвище='{last_name}', Посада='{position}', Відділення='{department}', Дата={work_date}")

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
                # Шукаємо універсальний рядок
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
                error_message = (f"Колізія рівнів: знайдено {len(levels)} записів "
                                 f"({last_name}, {position}, {department}) на дату {work_date}.")
                print(f"[⚠️] {error_message}")
                error_messages.append(error_message)
                level_value = None
            elif len(levels) == 1:
                level_value = levels[0]['Рівень']
                print(f"✅ Знайдено рівень: {level_value}")
            else:
                error_message = (f"Не вдалося визначити рівень для {last_name}, {position}, {department} "
                                 f"на дату {work_date} — відсутній рівень для конкретного відділення і універсального.")
                print(f"[⚠️] {error_message}")
                error_messages.append(error_message)
                level_value = None

            # Оновлюємо рівень у worktime
            cursor.execute("""
                UPDATE zp_worktime
                SET level = %s
                WHERE date_shift = %s AND idx = %s
            """, (level_value, work_row['date_shift'], work_row['idx']))

            # 2️⃣ Перевірка правил у zp_фктУмовиОплати
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

                    work_val_norm = str(work_value).strip().lower() if work_value else ''
                    rule_val_norm = str(rule_value).strip().lower() if rule_value else ''

                    print(f"🔍 Порівнюємо поле '{field}': worktime='{work_val_norm}' vs rule='{rule_val_norm}'")

                    if field == 'level':
                        if rule_val_norm and work_val_norm:
                            if work_val_norm == rule_val_norm:
                                matches += 1
                                score += weight
                            else:
                                skip_rule = True
                                break
                        elif not rule_val_norm:
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
                        if rule_val_norm:
                            if work_val_norm == rule_val_norm:
                                matches += 1
                                score += weight
                            else:
                                skip_rule = True
                                break
                        else:
                            # універсальне правило
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
                    reference_rule = same_top[0]['rule']
                    collision_detected = False
                    for bm in same_top[1:]:
                        rule = bm['rule']
                        rule_an_zp = str(rule.get('АнЗП') or '').strip().lower()
                        ref_an_zp = str(reference_rule.get('АнЗП') or '').strip().lower()

                        rule_an_collective = str(rule.get('АнЗП_Колективний') or '').strip().lower()
                        ref_an_collective = str(reference_rule.get('АнЗП_Колективний') or '').strip().lower()

                        if rule_an_zp == ref_an_zp and rule_an_collective == ref_an_collective:
                            collision_detected = True
                            break

                    if collision_detected:
                        colision = "Колізія: " + ", ".join([str(bm['rule']['id']) for bm in same_top])
                        print(f"[⚠️] Колізія для ID {work_row['idx']} на дату {work_date}: {colision}")
                        error_messages.append(f"Колізія: {colision}")
                    else:
                        print(f"[ℹ️] Збіг Matches/Score, але АнЗП або АнЗП_Колективний різні — колізія ігнорується.")

            # 3️⃣ Запис у worktime
            if error_messages or not best_matches:
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
