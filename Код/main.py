# %%

# Выгрузка необходимых библиотек
import pandas as pd
import numpy as np
# %%

# Выгрузка самописных функций
from functions.format_fu import rename, format_dates_column, clean_commas, m_and_k
from functions.main_fu import removing_columns, remove_unnecessary_columns
# %%

# Снятие ограничения на отображение колонок
# pd.set_option('display.max_columns', None)

# Снятие ограничения на отображение строк
# pd.set_option('display.max_rows', None)
# %%

df1 = pd.read_csv('data\\CHMF Акции.csv')
df2 = pd.read_csv('data\\MAGN Акции.csv')
df3 = pd.read_csv('data\\NLMK Акции.csv')
df4 = pd.read_csv('data\\sample_submission.csv')
df5 = pd.read_csv('data\\Грузоперевозки.csv')
df6 = pd.read_csv('data\\Данные-рынка-стройматериалов.csv')
df7 = pd.read_csv('data\\Индекс-LME.csv')
df8 = pd.read_csv('data\\Макропоказатели.csv')
df9 = pd.read_csv('data\\Показатели-рынка-металла.csv')
df10 = pd.read_csv('data\\Топливо.csv', sep=';')
df11 = pd.read_csv('data\\Цены-на-сырье.csv')
# Не будут использоваться из-за даты: "data\\Грузоперевозки.csv", "data\\Данные-рынка-стройматериалов.csv", "data\\Показатели-рынка-металла.csv", "data\\sample_submission.csv"
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

# Список файлов для обработки
files = [
    'rename_data/CHMF.csv',
    'rename_data/MAGN.csv',
    'rename_data/NLMK.csv'
    ]
    
# Колонки, которые нужно удалить
columns_to_drop = ['open', 'high_price', 'low_price']

remove_unnecessary_columns(files, columns_to_drop)
# %%
for path in files:
    df = pd.read_csv(path)  # Загрузка данных
    df['volume'] = df['volume'].apply(m_and_k)  # Применение функции к столбцу 'volume'
    df.to_csv(path, sep=',', index=False)   # Сохранение данных

# %%

columns_to_keep = [
    'ЖРС_Китай Iron ore fines Fe 62%, CFR',
    'ЖРС_Российские окатыши Fe 62-65,5%,SiO2 5,8-8,65, DAP Забайкальск-Манжули, $/т',
    'ЖРС_Россия концентрат Fe 64-68%, FCA руб./т, без НДС',
    'ЖРС_Средневзвешенная цена концентрат Fe 64-68%, Россия FCA руб./т, без НДС',
    'Концентрат коксующегося угля_Россия марка КО FCA руб./т, без НДС',
    'Концентрат коксующегося угля_Россия марка КС FCA руб./т, без НДС',
    'Концентрат коксующегося угля_HCC Австралия, $/t FOB',
    'Лом_HMS 1/2 80:20, FOB EC Роттердам, $/т',
    'Лом_3А, РФ CPT ж/д Центральный ФО, руб./т, без НДС',
    'Лом_3А, FOB РФ Черное море, $/т',
    'Чугун_CFR Италия, $/т',
    'Чугун_Россия, FCA руб./т, без НДС',
    'Чугун_FOB Россия Черное море, $/т',
    'ГБЖ_CFR Италия, $/т',
    'ЖРС_Средневзвешенная цена окатыши Fe 62-65,5%, Россия FCA руб./т, без НДС',
    'ЖРС_Средневзвешенная цена аглоруда Fe 52-60%, Россия FCA руб./т, без НДС',
    'date'
]

# Загрузка данных (проверьте кодировку и разделитель при необходимости)
df = pd.read_csv('rename_data\\цены-на-сырье.csv', encoding='utf-8')

# Проверка наличия колонок в DataFrame
existing_columns = df.columns.tolist()
missing_columns = [col for col in columns_to_keep if col not in existing_columns]

if missing_columns:
    print(f'Предупреждение: следующие колонки отсутствуют в файле:\n{missing_columns}')

# Фильтрация колонок
filtered_columns = [col for col in columns_to_keep if col in existing_columns]
df_filtered = df[filtered_columns]

# Сохранение результата
df_filtered.to_csv('rename_data\\processed_prices.csv', index=False, encoding='utf-8')
print('Файл успешно обработан и сохранен как: rename_data\\processed_prices.csv')
# %%

# # Загрузка и переименование колонок для каждого файла
# chmf = pd.read_csv('rename_data/CHMF.csv')
# chmf.columns = ['date'] + [f'CHMF_{col}' for col in chmf.columns if col != 'date']

# magn = pd.read_csv('rename_data/MAGN.csv', decimal=',')
# magn.columns = ['date'] + [f'MAGN_{col}' for col in magn.columns if col != 'date']

# nlmk = pd.read_csv('rename_data/NLMK.csv')
# nlmk.columns = ['date'] + [f'NLMK_{col}' for col in nlmk.columns if col != 'date']

# lme = pd.read_csv('rename_data\\индекс-LME.csv')
# # Объединение данных по колонке date
# merged_df = chmf.merge(magn, on='date').merge(nlmk, on='date').merge(lme, on='date').merge(df_filtered, on='date')

# merged_df.to_csv('merged_df.csv', index=False)



import pandas as pd

# Функция для загрузки и подготовки данных
def load_and_prepare(file_path, prefix, decimal='.'):
    df = pd.read_csv(file_path, decimal=decimal)
    df['date'] = pd.to_datetime(df['date'])  # Конвертация в datetime
    df.columns = ['date'] + [f'{prefix}_{col}' for col in df.columns if col != 'date']
    return df

# Загрузка данных с переименованием колонок
chmf = load_and_prepare('rename_data/CHMF.csv', 'CHMF')
magn = load_and_prepare('rename_data/MAGN.csv', 'MAGN')
nlmk = load_and_prepare('rename_data/NLMK.csv', 'NLMK')

# Объединение с outer join (сохраняем все даты)
merged_df = chmf.merge(magn, on='date', how='outer').merge(nlmk, on='date', how='outer')

# Сортировка по дате (опционально)
merged_df = merged_df.sort_values(by='date').reset_index(drop=True)

# Вывод результата
merged_df.to_csv('merged_df.csv', index=False)
# %%



# print(merged_df.shape)
# %%
