# %%

# Выгрузка необходимых библиотек
import pandas as pd
import numpy as np
import os
# %%

# Выгрузка самописных функций
from functions.format_fu import rename, format_dates_column, clean_commas
from functions.main_fu import removing_columns
# %%

# Снятие ограничения на отображение колонок
# pd.set_option('display.max_columns', None)
# %%

# Снятие ограничения на отображение строк
# pd.set_option('display.max_rows', None)
# %%

df1 = pd.read_csv('data\\CHMF Акции.csv')
df2 = pd.read_csv('data\\MAGN Акции.csv')
df3 = pd.read_csv('data\\NLMK Акции.csv')
# df4 = pd.read_csv('data\\sample_submission.csv')
# df5 = pd.read_csv('data\\Грузоперевозки.csv')
# df6 = pd.read_csv('data\\Данные-рынка-стройматериалов.csv')
df7 = pd.read_csv('data\\Индекс-LME.csv')
df8 = pd.read_csv('data\\Макропоказатели.csv')
# df9 = pd.read_csv('data\\Показатели-рынка-металла.csv')
df10 = pd.read_csv('data\\Топливо.csv', sep=';')
df11 = pd.read_csv('data\\Цены-на-сырье.csv')
# %%

for i in ["data\\Грузоперевозки.csv",
          "data\\Данные-рынка-стройматериалов.csv",
          "data\\Показатели-рынка-металла.csv",
          "data\\sample_submission.csv"
          ]:
    try:
        os.remove(i)
    except:
        print(f'Файла {i} не существует.')
# %%

file_paths = [
    'data\\Макропоказатели.csv',
    'data\\Цены-на-сырье.csv'
    ]

for path in file_paths:
    removed = removing_columns(path)
    print(f"Файл {path}: удалено {removed} столбцов")
# %% 

try:
    # Преобразование столбцов в тип float
    df10['Цена на бензин'] = df10['Цена на бензин'].str.replace(',', '.').astype(float)
    df10['Цена на дт'] = df10['Цена на дт'].str.replace(',', '.').astype(float)

    # Изменили разделитель на запятую и сохраним файл
    df10.to_csv('data\\топливо.csv', sep=',', index=False)
except:
    print("Данные уже изменены!")

# %%

rename()
# %%

file_paths = [
    'rename_data\\CHMF.csv',
    'rename_data\\MAGN.csv',
    'rename_data\\NLMK.csv',
    'rename_data\\индекс-LME.csv',
    'rename_data\\макропоказатели.csv',
    'rename_data\\цены-на-сырье.csv',
    'rename_data\\топливо.csv'
]

for i in file_paths:
    format_dates_column(i, 'date')

# %%

# dd = pd.read_csv('rename_data\\CHMF.csv')

# dd['date'].info()

# %%

# Приводим числа в единый формат, т.е. удаляем ненужные запятые в колонках
try:

    file_paths = [
        'rename_data\\CHMF.csv',
        'rename_data\\MAGN.csv',
        'rename_data\\NLMK.csv',
        ]

    # Удаление лишних запятых в остальных файлах, с помощью написанной функции
    for i in file_paths:
        clean_commas(i)

    print("Запятые успешно удалены, тип данных изменён.")
except Exception as e:
    print(f"Произошла ошибка при загрузке данных: {e}")

# %%