
import pandas as pd
from decimal import Decimal, InvalidOperation


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


# Форматирует даты
def format_dates_column(file_name, column_name):

    df = pd.read_csv(file_name)

    # Преобразование дат
    df[column_name] = pd.to_datetime(
        df[column_name],
        errors='coerce',
        infer_datetime_format=True,
        dayfirst=False
    )
    
    mask = df[column_name].isna()
    df.loc[mask, column_name] = pd.to_datetime(
        df.loc[mask, column_name],
        errors='coerce',
        dayfirst=True
    )
    
    df[column_name] = df[column_name].dt.strftime("%Y-%m-%d %H:%M:%S")
    
    # Сохранение в файл
    df.to_csv(file_name, index=False)
    
    print(f"Файл сохранен как: {file_name}")
    return df


def clean_commas(path):
         # Загрузка данных
        df = pd.read_csv(path)

        # Применение ко всем числовым столбцам
        for col in df.select_dtypes(include=['object']).columns:
            if col not in ['date', 'volume', 'change_price']:
                df[col] =  df[col].replace(',', '', regex=True).astype(float)

        df.to_csv(path, sep=',', index=False)


def m_and_k(entrie):
    # Если entrie уже является числом, возвращаем его
    if isinstance(entrie, (int, float)):
        return Decimal(str(entrie))  # Преобразуем в Decimal для единообразия

    # Если entrie — строка, обрабатываем её
    try:
        if 'M' in entrie:
            return Decimal(entrie.replace(',', '.').replace('M', '')) * Decimal('1000000')
        elif 'K' in entrie:
            return Decimal(entrie.replace(',', '.').replace('K', '')) * Decimal('1000')
        else:
            return Decimal(entrie.replace(',', '.'))
    except InvalidOperation:
        # Если преобразование в Decimal не удалось, возвращаем исходное значение
        return entrie