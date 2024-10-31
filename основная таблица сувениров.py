import pandas as pd
import pyodbc
import numpy as np
file_path = r'C:\Data.xlsx'
connection = pyodbc.connect('DRIVER={SQL Server};'
                            'SERVER=LAPTOP-N7166GCI\MSSQLSERVER5;'
                            'DATABASE=SouvenirShop;'
                            'Trusted_Connection=yes;')

def import_data(file_path, connection):
    try:
        cursor = connection.cursor()

        df = pd.read_excel(file_path, engine="openpyxl")
        df = df.replace({np.nan: None})

        def get_or_create_ids(cursor, table, column, values):
            """
            Получает или создает идентификаторы для уникальных значений из справочника.
            Если значение уже существует, возвращается его идентификатор.
            Если нет, оно добавляется в таблицу, и возвращается новый идентификатор.
            """
            id_map = {}
            for value in values:
                cursor.execute(f"SELECT ID FROM {table} WHERE {column} = ?", value)
                result = cursor.fetchone()
                if result:
                    id_map[value] = result[0]  # Используем существующий ID
                else:
                    # Добавляем новое значение и получаем его ID
                    cursor.execute(f"INSERT INTO {table} ({column}) VALUES (?)", value)
                    id_map[value] = cursor.execute("SELECT @@IDENTITY").fetchone()[0]
            return id_map

        # Инициализируем справочники для Colors, SouvenirMaterials и ApplicationMethods
        unique_colors = df['color'].dropna().unique()
        color_id_map = get_or_create_ids(cursor, "Colors", "Name", unique_colors)

        unique_materials = df['material'].dropna().unique()
        material_id_map = get_or_create_ids(cursor, "SouvenirMaterials", "Name", unique_materials)

        unique_methods = df['applicMetod'].dropna().unique()
        method_id_map = get_or_create_ids(cursor, "ApplicationMethods", "Name", unique_methods)

        # Фиксируем изменения в справочниках
        connection.commit()

        # Задаем список обязательных полей
        required_fields = ['id', 'url', 'shortname', 'name', 'description', 'rating', 'categoryid',
                           'color', 'prodsize', 'material', 'applicMetod', 'fullCategories', 'dealerPrice', 'price']

        # Счетчики пропущенных записей по разным причинам
        skipped_records = {'missing_required_fields': 0, 'missing_references': 0}
        missing_field_counts = {field: 0 for field in required_fields}

        # Обрабатываем данные для вставки в таблицу Souvenirs
        for index, row in df.iterrows():
            # Проверяем наличие обязательных полей
            missing_fields = [field for field in required_fields if row[field] is None]
            if missing_fields:
                for field in missing_fields:
                    missing_field_counts[field] += 1
                print(
                    f"Запись на строке {index + 2} пропущена из-за отсутствия значений в полях: {', '.join(missing_fields)}.")
                skipped_records['missing_required_fields'] += 1
                continue

            # Получаем данные для внешних ключей
            color = row['color']
            material = row['material']
            applic_method = row['applicMetod']
            category_id = row['categoryid']

            # Проверяем наличие категории, добавляем новую, если не существует
            cursor.execute("SELECT ID FROM SouvenirsCategories WHERE ID = ?", category_id)
            if not cursor.fetchone():
                cursor.execute(
                    "INSERT INTO SouvenirsCategories (ID, IdParent, Name) VALUES (?, NULL, 'Неизвестная категория')",
                    category_id)

            # Получаем идентификаторы из справочников
            id_color = color_id_map.get(color)
            id_material = material_id_map.get(material)
            id_applic_method = method_id_map.get(applic_method)

            # Пропускаем запись, если не удалось найти данные в справочниках
            if None in [id_color, id_material, id_applic_method]:
                print(f"Запись на строке {index + 2} пропущена из-за отсутствия данных в справочниках.")
                skipped_records['missing_references'] += 1
                continue

            # Подготовка данных для вставки
            insert_data = (
                int(row['id']),
                row['url'],
                row['shortname'],
                row['name'],
                row['description'],
                int(row['rating']) if row['rating'] else 0,
                category_id,
                id_color,
                row['prodsize'],
                id_material,
                row['weight'],
                row['qtypics'],
                row['picssize'],
                id_applic_method,
                row['fullCategories'],
                row['dealerPrice'],
                row['price'],
                None  # Comments
            )

            # Вставка данных в таблицу Souvenirs
            insert_query = """
                INSERT INTO Souvenirs (
                    ID, URL, ShortName, Name, Description, Rating, IdCategory, IdColor, Size, 
                    IdMaterial, Weight, QTypics, PicsSize, IdApplicMetod, AllCategories, 
                    DealerPrice, Price, Comments
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(insert_query, insert_data)
            print(f"Сувенир '{row['name']}' добавлен.")

        # Фиксируем вставку данных в основную таблицу
        connection.commit()
        print(f"Импорт данных завершен. Пропущено записей: {sum(skipped_records.values())}")
        print(f"Причины пропусков: {skipped_records['missing_required_fields']} из-за отсутствия обязательных полей, "
              f"{skipped_records['missing_references']} из-за отсутствия данных в справочниках.")
        print("Статистика по отсутствующим обязательным полям:")
        for field, count in missing_field_counts.items():
            print(f"Поле '{field}' отсутствовало {count} раз.")

    except Exception as e:
        # Обработка исключений и откат изменений в случае ошибки
        print(f"Ошибка при импорте данных: {e}")
        connection.rollback()


# Основная функция для запуска процесса импорта
def main():
    import_data(file_path, connection)
    connection.close()
    print("Соединение с базой данных закрыто.")


if __name__ == '__main__':
    main()
