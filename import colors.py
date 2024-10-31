import pandas as pd
import pyodbc

connection = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=LAPTOP-N7166GCI\MSSQLSERVER5;'
    'DATABASE=SouvenirShop;'
    'Trusted_Connection=yes;'
)
file_path = r'C:\data.xlsx'
df = pd.read_excel(file_path, sheet_name='Sheet1', usecols=['color'])
df['color'] = df['color'].astype(str)
cursor = connection.cursor()

# Цикл по каждой строке DataFrame для вставки в базу данных
for index, row in df.iterrows():
    cursor.execute('''
        INSERT INTO Colors (Name)
        VALUES (?)
    ''', row['color'])

# Фиксация изменений и закрытие соединения
connection.commit()
cursor.close()
connection.close()
