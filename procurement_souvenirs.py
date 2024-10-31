import pyodbc
import random

# Подключение к SQL Server
connection_string = (
    'DRIVER={SQL Server};'
    'SERVER=LAPTOP-N7166GCI\\MSSQLSERVER5;'
    'DATABASE=SouvenirShop;'
    'Trusted_Connection=yes;'
)

# Функция для заполнения таблицы ProcurementSouvenirs
def populate_procurementsouvenirs(conn, procurement_ids):
    try:
        cursor = conn.cursor()

        # Получение всех доступных ID из таблицы Souvenirs
        cursor.execute("SELECT ID FROM Souvenirs")
        souvenir_ids = [row[0] for row in cursor.fetchall()]

        if not souvenir_ids:
            print("Нет доступных ID сувениров для вставки.")
            return

        procurement_souvenirs = []

        # Формируем данные для вставки
        for procurement_id in procurement_ids:
            num_items = random.randint(1, 20)  # случайное количество сувениров для каждой закупки
            for _ in range(num_items):
                id_souvenir = random.choice(souvenir_ids)
                amount = random.randint(10, 200)
                price = round(random.uniform(50, 2000), 2)
                procurement_souvenirs.append((id_souvenir, procurement_id, amount, price))

        # Запрос на вставку данных
        insert_query = """
        INSERT INTO ProcurementSouvenirs (IdSouvenir, IdProcurement, Amount, Price)
        VALUES (?, ?, ?, ?);
        """

        # Вставка каждой записи
        for id_souvenir, procurement_id, amount, price in procurement_souvenirs:
            cursor.execute(insert_query, (id_souvenir, procurement_id, amount, price))
            ps_id = cursor.execute("SELECT @@IDENTITY").fetchone()[0]
            print(f"Закупленный сувенир добавлен с ID {ps_id}.")

        conn.commit()  # Завершаем транзакцию
        cursor.close()

    except Exception as e:
        print(f"Ошибка при заполнении таблицы ProcurementSouvenirs: {e}")
        conn.rollback()

# Основной блок выполнения
if __name__ == '__main__':
    conn = pyodbc.connect(connection_string)
    if conn:
        procurement_ids = [1, 2, 3]  # Замените на реальные данные
        populate_procurementsouvenirs(conn, procurement_ids)
        conn.close()
        print("Соединение с базой данных закрыто.")
