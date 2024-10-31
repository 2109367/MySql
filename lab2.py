import pandas as pd
import pyodbc
import openpyxl

# Подключение к SQL Server
conn = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=LAPTOP-N7166GCI\MSSQLSERVER5;'
    'DATABASE=SouvenirShop;'
    'Trusted_Connection=yes;'
)

# Чтение Excel файла
file_path = r'C:\data.xlsx'

df = pd.read_excel(file_path, sheet_name='Sheet1')

# Вставка данных в таблицу Souvenirs
cursor = conn.cursor()
for index, row in df.iterrows():
    cursor.execute('''
        INSERT INTO Colors (Name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', row['color'])

conn.commit()
cursor.close()
conn.close()
