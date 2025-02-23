import os
import time
import requests
import mysql.connector
from dotenv import load_dotenv

# Завантаження змінних середовища
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")

ODATA_USER = os.getenv("ODATA_USER")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")
ODATA_URL = os.getenv("ODATA_URL")

if not ODATA_URL:
    raise ValueError("ODATA_URL не знайдено в змінних середовища")

# Підключення до MySQL
def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )

# Функція отримання та запису даних
def fetch_and_store_data():
    session = requests.Session()
    session.auth = (ODATA_USER, ODATA_PASSWORD)
    
    skip = 0  # Лічильник пропущених записів
    
    while True:
        next_url = f"{ODATA_URL}Catalog_Номенклатура?$format=json&$top=1000&$skip={skip}&$orderby=Ref_Key"
        response = session.get(next_url)

        if response.status_code != 200:
            print(f"Помилка запиту: {response.status_code} {response.text}")
            break

        data = response.json().get("value", [])
        if not data:
            print("Дані відсутні або отримано всі записи.")
            break

        print(f"Отримано {len(data)} записів ({skip + 1} - {skip + len(data)})")

        conn = get_db_connection()
        cursor = conn.cursor()

        for record in data:
            # Перевіряємо наявність запису в БД та порівнюємо DataVersion
            cursor.execute(
                "SELECT DataVersion FROM et_Catalog_Номенклатура WHERE Ref_Key = %s",
                (record["Ref_Key"],)
            )
            existing_data_version = cursor.fetchone()

            # Підготовка всіх полів для вставки/оновлення
            fields = (
                record.get("Ref_Key", None), record.get("DataVersion", None),
                record.get("DeletionMark", None), record.get("Parent_Key", None),
                record.get("IsFolder", None), record.get("Code", None),
                record.get("Description", None), record.get("SOVA_UDSМаксимальныйПроцентОплатыБаллами", None),
                record.get("SOVA_UDSНеПрименятьСкидку", None), record.get("SOVA_UDSПроцентДополнительногоНачисления", None),
                record.get("АналитикаПоЗарплате_Key", None), record.get("Артикул", None),
                record.get("Весовой", None), record.get("ВестиУчетПоСериям", None), record.get("Вид_Key", None),
                record.get("ВидНоменклатуры", None), record.get("ЕдиницаБазовая_Key", None),
                record.get("ЕдиницаДляОтчетов_Key", None), record.get("ЕдиницаИнвентаризации_Key", None),
                record.get("ЕдиницаПоставок_Key", None), record.get("ЕдиницаРозницы_Key", None),
                record.get("ЕдиницаФиксированная_Key", None), record.get("ЕдиницаХраненияОстатков_Key", None),
                record.get("ЕдиницаЦены_Key", None), record.get("ЗапрещенаРозничнаяТорговля", None),
                record.get("КодВнешнейБазы", None), record.get("КоличествоПериодов", None),
                record.get("Комментарий", None), record.get("Консультация", None),
                record.get("КонсультацияКлассическая", None), record.get("МаркируемаяПродукция", None),
                record.get("МинимальнаяЕдиница_Key", None), record.get("МинимальноеКоличество", None),
                record.get("Наценка", None), record.get("НеАктуальна", None),
                record.get("НеПечататьВНазначении", None), record.get("ОсновнойПоставщик_Key", None),
                record.get("ОсобенностиНазначения", None), record.get("Период", None),
                record.get("СистемаНалогообложения", None), record.get("СтавкаНалога_Key", None),
                record.get("СтатьяЗатратСписания_Key", None), record.get("СтранаПроизводитель", None),
                record.get("ТипМП", None), record.get("Фото_Key", None),
                record.get("УчетНаркотическихСредств", None), record.get("Изделие_Key", None),
                record.get("АвтоЗаполнениеРасходники", None), record.get("АвтоЗаполнениеДопПозиций", None),
                record.get("SOVA_UDSExternalId", None), record.get("SOVA_UDSNodeId", None),
                record.get("SOVA_UDSIdТовара", None), record.get("SOVA_UDSКатегория_Key", None),
                record.get("SOVA_UDSНаименование", None), record.get("SOVA_UDSНеВыгружатьОстаток", None),
                record.get("ОбменССайтом", None), record.get("ЗапретСписанияМенееМинимальнойЕдиницы", None),
                record.get("МПК_ЗаписьРазрешена", None), record.get("МПК_Продолжительность", None),
                record.get("МПК_ТипУслуги", None), record.get("ID", None),
                record.get("ЗапрещенаПродажаВрачебнымЧеком", None), record.get("ВидПродукцииИС", None),
                record.get("ОтпускатьПоРецепту", None), record.get("УДАЛИТЬАнкета", None),
                record.get("Манипуляции", None), record.get("Резервы", None), record.get("Запасы", None),
                record.get("Состав", None), record.get("ЕдиницыРозницы", None),
                record.get("ДополнительныеПозицииЧека", None), record.get("МПК_МедицинскиеСпециализации", None),
                record.get("ИсторияВидаСтавкиНалога", None), record.get("Predefined", None),
                record.get("PredefinedDataName", None)
            )

            if existing_data_version:
                # Якщо DataVersion не співпадає, оновлюємо запис
                if existing_data_version[0] != record["DataVersion"]:
                    cursor.execute(
                """
                INSERT INTO et_Catalog_Номенклатура 
                (Ref_Key, DataVersion, DeletionMark, Parent_Key, IsFolder, Code, Description, 
                SOVA_UDSМаксимальныйПроцентОплатыБаллами, SOVA_UDSНеПрименятьСкидку, 
                SOVA_UDSПроцентДополнительногоНачисления, АналитикаПоЗарплате_Key, Артикул, 
                Весовой, ВестиУчетПоСериям, Вид_Key, ВидНоменклатуры, ЕдиницаБазовая_Key, 
                ЕдиницаДляОтчетов_Key, ЕдиницаИнвентаризации_Key, ЕдиницаПоставок_Key, 
                ЕдиницаРозницы_Key, ЕдиницаФиксированная_Key, ЕдиницаХраненияОстатков_Key, 
                ЕдиницаЦены_Key, ЗапрещенаРозничнаяТорговля, КодВнешнейБазы, КоличествоПериодов, 
                Комментарий, Консультация, КонсультацияКлассическая, МаркируемаяПродукция, 
                МинимальнаяЕдиница_Key, МинимальноеКоличество, Наценка, НеАктуальна, 
                НеПечататьВНазначении, ОсновнойПоставщик_Key, ОсобенностиНазначения, Период, 
                СистемаНалогообложения, СтавкаНалога_Key, СтатьяЗатратСписания_Key, 
                СтранаПроизводитель, ТипМП, Фото_Key, УчетНаркотическихСредств, Изделие_Key, 
                АвтоЗаполнениеРасходники, АвтоЗаполнениеДопПозиций, SOVA_UDSExternalId, 
                SOVA_UDSNodeId, SOVA_UDSIdТовара, SOVA_UDSКатегория_Key, SOVA_UDSНаименование, 
                SOVA_UDSНеВыгружатьОстаток, ОбменССайтом, ЗапретСписанияМенееМинимальнойЕдиницы, 
                МПК_ЗаписьРазрешена, МПК_Продолжительность, МПК_ТипУслуги, ID, 
                ЗапрещенаПродажаВрачебнымЧеком, ВидПродукцииИС, ОтпускатьПоРецепту, УДАЛИТЬАнкета, 
                Манипуляции, Резервы, Запасы, Состав, ЕдиницыРозницы, ДополнительныеПозицииЧека, 
                МПК_МедицинскиеСпециализации, ИсторияВидаСтавкиНалога, Predefined, PredefinedDataName
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                *fields  # Розпаковка списку fields у аргументи запиту
            )

                    
            else:
                # Якщо запису немає в БД, додаємо новий
                cursor.execute(
    """
    INSERT INTO et_Catalog_Номенклатура 
    (Ref_Key, DataVersion, DeletionMark, Parent_Key, IsFolder, Code, Description, 
    SOVA_UDSМаксимальныйПроцентОплатыБаллами, SOVA_UDSНеПрименятьСкидку, 
    SOVA_UDSПроцентДополнительногоНачисления, АналитикаПоЗарплате_Key, Артикул, 
    Весовой, ВестиУчетПоСериям, Вид_Key, ВидНоменклатуры, ЕдиницаБазовая_Key, 
    ЕдиницаДляОтчетов_Key, ЕдиницаИнвентаризации_Key, ЕдиницаПоставок_Key, 
    ЕдиницаРозницы_Key, ЕдиницаФиксированная_Key, ЕдиницаХраненияОстатков_Key, 
    ЕдиницаЦены_Key, ЗапрещенаРозничнаяТорговля, КодВнешнейБазы, КоличествоПериодов, 
    Комментарий, Консультация, КонсультацияКлассическая, МаркируемаяПродукция, 
    МинимальнаяЕдиница_Key, МинимальноеКоличество, Наценка, НеАктуальна, 
    НеПечататьВНазначении, ОсновнойПоставщик_Key, ОсобенностиНазначения, Период, 
    СистемаНалогообложения, СтавкаНалога_Key, СтатьяЗатратСписания_Key, 
    СтранаПроизводитель, ТипМП, Фото_Key, УчетНаркотическихСредств, Изделие_Key, 
    АвтоЗаполнениеРасходники, АвтоЗаполнениеДопПозиций, SOVA_UDSExternalId, 
    SOVA_UDSNodeId, SOVA_UDSIdТовара, SOVA_UDSКатегория_Key, SOVA_UDSНаименование, 
    SOVA_UDSНеВыгружатьОстаток, ОбменССайтом, ЗапретСписанияМенееМинимальнойЕдиницы, 
    МПК_ЗаписьРазрешена, МПК_Продолжительность, МПК_ТипУслуги, ID, 
    ЗапрещенаПродажаВрачебнымЧеком, ВидПродукцииИС, ОтпускатьПоРецепту, УДАЛИТЬАнкета, 
    Манипуляции, Резервы, Запасы, Состав, ЕдиницыРозницы, ДополнительныеПозицииЧека, 
    МПК_МедицинскиеСпециализации, ИсторияВидаСтавкиНалога, Predefined, PredefinedDataName
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
    *fields  # Розпаковка списку fields у аргументи запиту
)



        # Затримка між пачками
        conn.commit()
        cursor.close()
        conn.close()
        skip += len(data)
        time.sleep(3)  # 3 секунди між пачками

# Запуск процесу
fetch_and_store_data()
