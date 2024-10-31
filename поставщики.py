import pyodbc

# Подключение к SQL Server
connection = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=LAPTOP-N7166GCI\MSSQLSERVER5;'
    'DATABASE=SouvenirShop;'
    'Trusted_Connection=yes;'
)

# Функция для заполнения таблицы providers
def populate_providers(conn):
    try:
        cursor = conn.cursor()

        # Данные для заполнения таблицы
        providers = []
        for i in range(1, 101):
            name = f'Поставщик {i}'
            email = f'prov{i}@gmail.com'
            contact_person = f'Контактное лицо {i}'
            comments = f'Комментарий {i}'
            providers.append((name, email, contact_person, comments))

        # Запрос на вставку данных
        insert_query = """
        INSERT INTO Providers (Name, Email, ContactPerson, Comments)
        VALUES (?, ?, ?, ?);
        """

        # Вставка данных и вывод ID
        provider_ids = []
        for provider in providers:
            cursor.execute(insert_query, provider)
            conn.commit()  # фиксируем каждую вставку

            # Получение автоматически присвоенного ID
            provider_id = cursor.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
            provider_ids.append(provider_id)
            print(f"Поставщик '{provider[0]}' добавлен с ID {provider_id}.")

        # Закрытие курсора
        cursor.close()
        return provider_ids

    except Exception as e:
        print(f"Ошибка при заполнении таблицы providers: {e}")
        conn.rollback()


# Вызов функции для заполнения таблицы providers
populate_providers(connection)

# Закрытие соединения
connection.close()
