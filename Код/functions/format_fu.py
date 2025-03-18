
import pandas as pd

def format_dates_column(df, column_name):
    # Пытаемся преобразовать в datetime с разными параметрами
    df[column_name] = pd.to_datetime(
        df[column_name],
        errors='coerce',
        infer_datetime_format=True,
        dayfirst=False  # Сначала пробуем форматы типа MM/DD/YYYY
    )
    
    # Заменяем оставшиеся NaT значениями с dayfirst=True
    mask = df[column_name].isna()
    df.loc[mask, column_name] = pd.to_datetime(
        df.loc[mask, column_name],
        errors='coerce',
        dayfirst=True  # Затем пробуем форматы типа DD.MM.YYYY
    )
    
    # Форматируем результат в нужный строковый формат
    df[column_name] = df[column_name].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def rename():

    f = ['data\\CHMF Акции.csv', 'data\\MAGN Акции.csv', 'data\\NLMK Акции.csv',
         'data\\sample_submission.csv', 'data\\Грузоперевозки.csv', 'data\\Данные-рынка-стройматериалов.csv',
         'data\\Индекс-LME.csv', 'data\\Макропоказатели.csv', 'data\\Показатели-рынка-металла.csv',
         'data\\Топливо.csv', 'data\\Цены-на-сырье.csv']
    
    rf = ['rename_data\\CHMF.csv', 'rename_data\\MAGN.csv', 'rename_data\\NLMK.csv',
          'rename_data\\образец-подачи.csv', 'rename_data\\грузоперевозки.csv', 'rename_data\\данные-рынка-стройматериалов.csv',
          'rename_data\\индекс-LME.csv', 'rename_data\\макропоказатели.csv', 'rename_data\\показатели-рынка-металла.csv',
          'rename_data\\топливо.csv', 'rename_data\\цены-на-сырье.csv']

    # Обработка MAGN
    column_mapping = {
        "Дата": "date",
        "Цена": "price",
        "Откр.": "open",
        "Макс.": "high_price",
        "Мин.": "low_price",
        "Объём": "volume",
        "Изм. %": "change_price"
    }

    df = pd.read_csv(f[1], encoding="utf-8")
    df.rename(columns=column_mapping, inplace=True)
    df.to_csv(rf[1], index=False, encoding="utf-8")



    # Обработка CHMF и NLMK
    column_mapping = {
        "Date": "date",
        "Price": "price",
        "Open": "open",
        "High": "high_price",
        "Low": "low_price",
        "Vol.": "volume",
        "Change %": "change_price"
    }

    for file in [f[0], f[2]]:
        df = pd.read_csv(file)
        df.rename(columns=column_mapping, inplace=True)
        df.to_csv(rf[0] if file == f[0] else rf[2], index=False, encoding="utf-8")

    
    # Обработка образец-подачи
    column_mapping = {
        "dt": "date",
        "Цена на арматуру": "price",
        "Объем": "volume"
    }

    df = pd.read_csv(f[3], encoding="utf-8")
    df.rename(columns=column_mapping, inplace=True)
    df.to_csv(rf[3], index=False, encoding="utf-8")


    # Обработка грузоперевозки
    column_mapping = {
        "dt": "date"
    }

    df = pd.read_csv(f[4], encoding="utf-8")
    df.rename(columns=column_mapping, inplace=True)
    df.to_csv(rf[4], index=False, encoding="utf-8")

    # Обработка данные-рынка-стройматериалов
    column_mapping = {
        "dt": "date"
    }

    df = pd.read_csv(f[5], encoding="utf-8")
    df.rename(columns=column_mapping, inplace=True)
    df.to_csv(rf[5], index=False, encoding="utf-8")


    # Обработка индекс-LME
    column_mapping = {
        "дата": "date",
        "цена": "lme_price"
    }

    df = pd.read_csv(f[5], encoding="utf-8")
    df.rename(columns=column_mapping, inplace=True)
    df.to_csv(rf[5], index=False, encoding="utf-8")


    # Обработка макропоказатели
    column_mapping = {
        "dt": "date"
    }

    df = pd.read_csv(f[5], encoding="utf-8")
    df.rename(columns=column_mapping, inplace=True)
    df.to_csv(rf[5], index=False, encoding="utf-8")