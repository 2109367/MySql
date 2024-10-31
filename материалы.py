import pandas as pd
import pyodbc

connection = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=LAPTOP-N7166GCI\MSSQLSERVER5;'
    'DATABASE=SouvenirShop;'
    'Trusted_Connection=yes;'
)
file_path = r'C:\data.xlsx'
df = pd.read_excel(file_path, sheet_name='Sheet1', usecols=['material'])
df['material'] = df['material'].astype(str)
cursor = connection.cursor()

# Цикл по каждой строке DataFrame для вставки в базу данных
for index, row in df.iterrows():
    cursor.execute('''
        INSERT INTO SouvenirMaterials (Name)
        VALUES (?)
    ''', row['material'])

# Фиксация изменений и закрытие соединения
connection.commit()
cursor.close()
connection.close()
