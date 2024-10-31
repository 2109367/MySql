import pandas as pd
import pyodbc

# Подключение к SQL Server
connection = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=LAPTOP-N7166GCI\MSSQLSERVER5;'
    'DATABASE=SouvenirShop;'
    'Trusted_Connection=yes;'
)

# Функция для заполнения таблицы procurementstatuses
def populate_procurementstatuses(conn):
    try:
        cursor = conn.cursor()
        statuses = [
            'Новый',
            'В обработке',
            'Отгружен',
            'Доставлен',
            'Отменен',
            'Ожидает оплаты',
            'Оплачен',
            'Завершен',
            'Возврат',
            'Закрыт'
        ]
        insert_query = """
        INSERT INTO ProcurementStatuses (Name)
        VALUES (%s) RETURNING ID;
        """
        status_ids = []
        for status in statuses:
            cursor.execute(insert_query, (status,))
            status_id = cursor.fetchone()[0]
            status_ids.append(status_id)
            print(f"Статус закупки '{status}' добавлен с ID {status_id}.")
        conn.commit()
        cursor.close()
        return status_ids
    except Exception as e:
        print(f"Ошибка при заполнении таблицы procurementstatuses: {e}")
        conn.rollback()