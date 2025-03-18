# %%

# Выгрузка необходимых библиотек
import pandas as pd
import numpy as np
# %%

# Выгрузка самописных функций
from functions.format_fu import rename
# %%

# Снятие ограничения на отображение колонок
pd.set_option('display.max_columns', None)
# %%

# Снятие ограничения на отображение строк
pd.set_option('display.max_rows', None)
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
df10 = pd.read_csv('data\\Топливо.csv')
df11 = pd.read_csv('data\\Цены-на-сырье.csv')
# %%

# for i in [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11]:
#     print(i[''].isna().sum())
# %%

rename()
# %%
