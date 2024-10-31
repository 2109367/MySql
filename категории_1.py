import pandas as pd
import pyodbc
import numpy as np

# Путь к текстовому файлу
file_path = r'C:\categories.txt'

# Загрузка данных из текстового файла с разделением запятыми
df = pd.read_csv(file_path, delimiter=',', na_values=['""'])

# Преобразование колонок 'parent_id' в числовые значения
df['id'] = pd.to_numeric(df['id'], errors='coerce').replace(np.nan, None)
df['parent_id'] = pd.to_numeric(df['parent_id'], errors='coerce').replace(np.nan, None)

# Убедитесь, что 'name' - строка
df['name'] = df['name'].astype(str)

connection = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=LAPTOP-N7166GCI\MSSQLSERVER5;'
    'DATABASE=SouvenirShop;'
    'Trusted_Connection=yes;'
)

# Создание курсора
cursor = connection.cursor()

# Вставка данных
for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO SouvenirsCategories (ID, IdParent, Name) 
            VALUES (?, ?, ?)
        ''', row['id'], row['parent_id'], row['name'])


# Сохранение изменений и закрытие соединения
connection.commit()
cursor.close()
connection.close()
