import pyodbc
import random

# Подключение к SQL Server
def get_connection():
    try:
        connection = pyodbc.connect(
            'DRIVER={SQL Server};'
            'SERVER=LAPTOP-N7166GCI\\MSSQLSERVER5;'
            'DATABASE=SouvenirShop;'
            'Trusted_Connection=yes;'
        )
        print("Соединение с базой данных установлено.")
        return connection
    except pyodbc.Error as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        return None

# Функция для заполнения таблицы SouvenirStores
def populate_souvenirstores(conn, souvenir_ids, procurement_ids):
    try:
        cursor = conn.cursor()
        store_entries = []

        # Формируем данные для вставки
        for id_souvenir in souvenir_ids:
            id_procurement = random.choice(procurement_ids)
            amount = random.randint(10, 100)
            comments = f"Партия товара {id_souvenir}"
            store_entries.append((id_souvenir, id_procurement, amount, comments))

        # Запрос на вставку данных
        insert_query = """
        INSERT INTO SouvenirStores (IdSouvenir, IdProcurement, Amount, Comments)
        VALUES (?, ?, ?, ?);
        """

        # Вставка каждой записи
        for entry in store_entries:
            cursor.execute(insert_query, entry)
            store_id = cursor.execute("SELECT @@IDENTITY").fetchone()[0]  # Получаем ID вставленной строки
            print(f"Запись на складе добавлена с ID {store_id}.")

        conn.commit()  # Завершаем транзакцию
        cursor.close()
        print("Таблица SouvenirStores успешно заполнена.")

    except Exception as e:
        print(f"Ошибка при заполнении таблицы SouvenirStores: {e}")
        conn.rollback()

# Основной блок выполнения
if __name__ == '__main__':
    conn = get_connection()
    if conn is not None:
        cursor = conn.cursor()

        # Получаем ID сувениров из таблицы Souvenirs
        cursor.execute("SELECT ID FROM Souvenirs")
        souvenir_ids = [row[0] for row in cursor.fetchall()]

        # Получаем ID закупок из таблицы SouvenirProcurements
        cursor.execute("SELECT ID FROM SouvenirProcurements")
        procurement_ids = [row[0] for row in cursor.fetchall()]

        # Заполняем таблицу SouvenirStores
        populate_souvenirstores(conn, souvenir_ids, procurement_ids)

        # Закрываем соединение
        conn.close()
        print("Соединение с базой данных закрыто.")
