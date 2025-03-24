# %%

# Выгрузка необходимых библиотек
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
# Выгрузка метрики
from metric import decision_prices
# %%

# Выгрузка самописных функций
from functions.format_fu import rename, format_dates_column, clean_commas, m_and_k
from functions.main_fu import removing_columns, remove_unnecessary_columns, remove_outliers_iqr, plot_outliers
# %%

# Снятие ограничения на отображение колонок
pd.set_option('display.max_columns', None)

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
    format_dates_column(i, 'dt')
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
    'dt',
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
    'ЖРС_Средневзвешенная цена аглоруда Fe 52-60%, Россия FCA руб./т, без НДС'
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

# Функция для загрузки и подготовки данных
def load_and_prepare_prefix(file_path, prefix, decimal='.'):
    df = pd.read_csv(file_path, decimal=decimal)
    df['dt'] = pd.to_datetime(df['dt'])  # Конвертация в datetime
    df.columns = ['dt'] + [f'{prefix}_{col}' for col in df.columns if col != 'dt']
    return df

# Загрузка данных с переименованием колонок
chmf = load_and_prepare_prefix('rename_data\\CHMF.csv', 'CHMF')
magn = load_and_prepare_prefix('rename_data\\MAGN.csv', 'MAGN')
nlmk = load_and_prepare_prefix('rename_data\\NLMK.csv', 'NLMK')


def load_and_prepare(file_path, decimal='.'):
    df = pd.read_csv(file_path, decimal=decimal)
    df['dt'] = pd.to_datetime(df['dt'])  # Конвертация в datetime
    df.columns = ['dt'] + [col for col in df.columns if col != 'dt']
    return df

lme = load_and_prepare('rename_data\\индекс-LME.csv')
makro = load_and_prepare('rename_data\\макропоказатели.csv')
raw_materials = load_and_prepare('rename_data\\цены-на-сырье.csv')
fuel = load_and_prepare('rename_data\\топливо.csv')

# Объединение с outer join (сохраняем все даты)
merged_df = chmf.merge(magn, on='dt', how='outer').merge(nlmk, on='dt', how='outer').merge(lme, on='dt', how='outer').merge(makro, on='dt', how='outer').merge(raw_materials, on='dt', how='outer').merge(fuel, on='dt', how='outer')

# Сортировка по дате (опционально)
merged_df = merged_df.sort_values(by='dt').reset_index(drop=True)

merged_df.dropna(thresh=6, inplace=True)


# Получаем список всех колонок, кроме dt
columns_to_fill = [col for col in merged_df.columns if col != "dt"]
    
# Заполняем пропуски во всех колонках независимо от их названия
merged_df[columns_to_fill] = merged_df[columns_to_fill].ffill()

# Вывод результата
merged_df.to_csv('merged_df.csv', index=False)
# %%


merged_df.shape
# %%

na_counts = merged_df.isna().sum()
na_counts[na_counts > 0]
# %%

merged_df = merged_df.drop(['lme_price', 'Чугун_CFR Китай, $/т'], axis=1).dropna()
merged_df.to_csv('merged_df.csv', index=False)
# %%

print(merged_df.shape)
# %%

# Создаем копию исходного датафрейма для обработки
df_encoded = merged_df.copy()

# Преобразование столбца с датой
df_encoded['dt'] = pd.to_datetime(df_encoded['dt'])

# Обработка столбцов с процентами (включая замену запятых на точки)
percent_columns = [col for col in df_encoded.columns if '_change_price' in col]
for col in percent_columns:
    df_encoded[col] = (
        df_encoded[col]
        .astype(str)
        .str.replace('%', '', regex=False)  # Удаляем знак процента
        .str.replace(',', '.', regex=False)  # Заменяем запятые на точки
        .str.strip()  # Удаляем пробелы по краям
        .astype(float)
        / 100  # Конвертация в десятичные дроби
    )

# Преобразование остальных числовых столбцов (также обрабатываем запятые)
numeric_columns = df_encoded.columns.difference(['dt'] + percent_columns)
for col in numeric_columns:
    df_encoded[col] = (
        df_encoded[col]
        .astype(str)
        .str.replace(',', '.', regex=False)  # Заменяем запятые на точки
        .str.replace('[^\d.-]', '', regex=True)  # Удаляем все нецифровые символы кроме . и -
        .replace('', pd.NA)  # Заменяем пустые строки на NA
        .astype(float)
    )

# Вывод результатов
print("Первые строки закодированного датафрейма:")
print(df_encoded.head(2).to_string())
# %%

# Расчет корреляционной матрицы
correlation_matrix = df_encoded.drop(columns=['dt']).corr(numeric_only=True)

# Настройка стиля графиков
plt.style.use('ggplot')
plt.rcParams['figure.figsize'] = [100, 100]  # Размер холста

# Создаем маску для верхнего треугольника
mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))

# Создаем тепловую карту
sns.heatmap(
    correlation_matrix,
    mask=mask,
    annot=True,
    fmt=".2f",  # Формат отображения чисел
    cmap='coolwarm',  # Цветовая схема
    vmin=-1,
    vmax=1,
    linewidths=0.5,
    cbar_kws={'label': 'Коэффициент корреляции'}
)

# Настройки отображения
plt.title('Матрица корреляций', fontsize=18, pad=20)
plt.xticks(rotation=45, ha='right', fontsize=10)
plt.yticks(rotation=0, fontsize=10)
plt.tight_layout()

# Сохранение и отображение графика
plt.savefig('correlation_matrix.png', dpi=300, bbox_inches='tight')
plt.show()
# %%

try:
    merged_df = merged_df.drop(columns=['CHMF_change_price', 'MAGN_change_price', 'NLMK_change_price'], axis=1)
except:
    print('Колонки уже были удалены')

merged_df.to_csv('merged_df.csv', index=False)
# %%

merged_df.head()
# %%

# Использование
numeric_cols = merged_df.columns[merged_df.columns != 'dt'].tolist()
plot_outliers(merged_df, numeric_cols)

# %%

# Использование (предполагая, что merged_df и numeric_cols уже определены)
cleaned_iqr = remove_outliers_iqr(merged_df, numeric_cols)
# %%

print(merged_df.shape)
print(cleaned_iqr.shape)
# %%

dtype_dict = merged_df.dtypes.astype(str).to_list()
print(set(dtype_dict))
# %%
