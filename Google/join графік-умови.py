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

            # 🔥 Підтягуємо level з zp_фктРівніСпівробітників
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

            if len(levels) > 1:
                print(f"[⚠️] Колізія рівнів! ID={work_row['idx']} дата={work_row['date_shift']}: "
                      f"Прізвище='{last_name}', Посада='{position}', Відділення='{department}' — знайдено {len(levels)} рівнів.")
                level_value = ''
            elif len(levels) == 1:
                level_value = levels[0]['Рівень']
            else:
                level_value = ''

            cursor.execute("""
                UPDATE zp_worktime
                SET level = %s
                WHERE date_shift = %s AND idx = %s
            """, (level_value, work_row['date_shift'], work_row['idx']))

            # 🔎 Тепер розрахунок Matches/Score
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
                        elif not rule_value and not work_value:
                            matches += 1
                            score += weight
                        else:
                            print(f"[⚠️] Помилка таблиць! ID={work_row['idx']} дата={work_row['date_shift']}: "
                                  f"rule_level='{rule_value}' vs worktime_level='{work_value}'.")
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

                if not skip_rule:
                    best_matches.append({
                        'rule': rule,
                        'matches': matches,
                        'score': score
                    })

            print(f"\n[🔎] Перевірка правил для ID {work_row['idx']} на дату {work_row['date_shift']}:")
            for bm in best_matches:
                rule_id = bm['rule']['id']
                m = bm['matches']
                s = bm['score']
                print(f"  🔸 Правило ID={rule_id} → Matches={m}, Score={s}")

            if best_matches:
                best_matches.sort(key=lambda x: (-x['matches'], -x['score']))

                top = best_matches[0]
                top_matches = top['matches']
                top_score = top['score']
                top_rule = top['rule']

                colision = ""
                same_top = [bm for bm in best_matches if bm['matches'] == top_matches and bm['score'] == top_score]
                if len(same_top) > 1:
                    colision = "Колізія: " + ", ".join([str(bm['rule']['id']) for bm in same_top])
                    print(f"[⚠️] Колізія для ID {work_row['idx']} на дату {work_row['date_shift']}: {colision}")

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
                        Ан_Колективний = %s
                    WHERE
                        date_shift = %s AND idx = %s
                """
                cursor.execute(update_sql, (
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
                    work_row['date_shift'],
                    work_row['idx']
                ))

                print(f"[✅] Оновлено рядок ID {work_row['idx']} Matches={top_matches}, Score={top_score}")
            else:
                print(f"[ℹ️] Для ID {work_row['idx']} на дату {work_row['date_shift']} не знайдено підходящих правил.")

        connection.commit()
        print("\n[🎉] Обробка завершена.")
finally:
    connection.close()
