
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


# Переименовывает столбцы в файлах и сохраняет в новые пути
def rename():

    # Конфигурация обработки файлов
    file_config = [
        {
            'input': 'data\\CHMF Акции.csv',
            'output': 'rename_data\\CHMF.csv',
            'columns': {
                "Date": "date",
                "Price": "price",
                "Open": "open",
                "High": "high_price",
                "Low": "low_price",
                "Vol.": "volume",
                "Change %": "change_price"
            }
        },
        {
            'input': 'data\\MAGN Акции.csv',
            'output': 'rename_data\\MAGN.csv',
            'columns': {
                "Дата": "date",
                "Цена": "price",
                "Откр.": "open",
                "Макс.": "high_price",
                "Мин.": "low_price",
                "Объём": "volume",
                "Изм. %": "change_price"
            }
        },
        {
            'input': 'data\\NLMK Акции.csv',
            'output': 'rename_data\\NLMK.csv',
            'columns': {
                "Date": "date",
                "Price": "price",
                "Open": "open",
                "High": "high_price",
                "Low": "low_price",
                "Vol.": "volume",
                "Change %": "change_price"
            }
        },
        {
            'input': 'data\\sample_submission.csv',
            'output': 'rename_data\\образец-подачи.csv',
            'columns': {
                "dt": "date",
                "Цена на арматуру": "price",
                "Объем": "volume"
            }
        },
        {
            'input': 'data\\Грузоперевозки.csv',
            'output': 'rename_data\\грузоперевозки.csv',
            'columns': {"dt": "date"}
        },
        {
            'input': 'data\\Данные-рынка-стройматериалов.csv',
            'output': 'rename_data\\данные-рынка-стройматериалов.csv',
            'columns': {"dt": "date"}
        },
        {
            'input': 'data\\Индекс-LME.csv',
            'output': 'rename_data\\индекс-LME.csv',
            'columns': {
                "дата": "date",
                "цена": "lme_price"
            }
        },
        {
            'input': 'data\\Макропоказатели.csv',
            'output': 'rename_data\\макропоказатели.csv',
            'columns': {
                "dt": "date",
                "Курс доллара": "Курс $"
            }
        },
        {
            'input': 'data\\Показатели-рынка-металла.csv',
            'output': 'rename_data\\показатели-рынка-металла.csv',
            'columns': {"dt": "date"}
        },
        {
            'input': 'data\\Топливо.csv',
            'output': 'rename_data\\топливо.csv',
            'columns': {"dt": "date"}
        },
        {
            'input': 'data\\Цены-на-сырье.csv',
            'output': 'rename_data\\цены-на-сырье.csv',
            'columns': {"dt": "date"}
        }
    ]

    for config in file_config:
        try:
            # Чтение файла с указанием кодировки
            df = pd.read_csv(config['input'], encoding='utf-8-sig')
            
            # Переименование столбцов
            df.rename(columns=config['columns'], inplace=True)
            
            # Сохранение результата
            df.to_csv(config['output'], index=False, encoding='utf-8-sig')
            
            print(f"Успешно обработан: {config['input']} -> {config['output']}")
            
        except FileNotFoundError:
            print(f"Ошибка: файл {config['input']} не найден")
        except KeyError as e:
            print(f"Ошибка в файле {config['input']}: отсутствует столбец {e}")
        except Exception as e:
            print(f"Неизвестная ошибка при обработке {config['input']}: {str(e)}")
