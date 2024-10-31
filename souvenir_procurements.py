import pyodbc
import random
import datetime

def create_connection():
    try:
        conn = pyodbc.connect('DRIVER={SQL Server};''SERVER=LAPTOP-N7166GCI\MSSQLSERVER5;''DATABASE=SouvenirShop;''Trusted_Connection=yes;')
        print("Подключение к базе данных успешно выполнено.")
        return conn
    except Exception as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        return None

# Получение списка ID поставщиков
def get_provider_ids(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT ID FROM Providers")
    provider_ids = [row.ID for row in cursor.fetchall()]
    return provider_ids

# Получение списка ID статусов
def get_status_ids(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT ID FROM ProcurementStatuses")
    status_ids = [row.ID for row in cursor.fetchall()]
    return status_ids


# Заполнение таблицы souvenirprocurements
def populate_souvenirprocurements(conn, provider_ids, status_ids):
    try:
        cursor = conn.cursor()

        # Генерация данных для вставки
        for i in range(1, 11):
            id_provider = random.choice(provider_ids)
            date = datetime.date(2024, random.randint(1, 12), random.randint(1, 2))
            id_status = random.choice(status_ids)

            # Формирование запроса через f-строку
            insert_query = f"""
            INSERT INTO SouvenirProcurements (IdProvider, Date, IdStatus)
            VALUES ({id_provider}, '{date}', {id_status});
            """
            cursor.execute(insert_query)
            print(f"Запись добавлена: (IdProvider={id_provider}, Date={date}, IdStatus={id_status})")

        conn.commit()  # Сохранение изменений
        cursor.close()
        print("Все данные успешно добавлены в SouvenirProcurements.")

    except Exception as e:
        print(f"Ошибка при заполнении таблицы SouvenirProcurements: {e}")
        conn.rollback()


# Пример вызова
def main():
    conn = pyodbc.connect('DRIVER={SQL Server};''SERVER=LAPTOP-N7166GCI\MSSQLSERVER5;''DATABASE=SouvenirShop;''Trusted_Connection=yes;')
    print("Подключение к базе данных успешно выполнено.")

    # Получение ID из других таблиц
    cursor = conn.cursor()
    cursor.execute("SELECT ID FROM Providers")
    provider_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT ID FROM ProcurementStatuses")
    status_ids = [row[0] for row in cursor.fetchall()]

    populate_souvenirprocurements(conn, provider_ids, status_ids)
    conn.close()


if __name__ == "__main__":
    main()