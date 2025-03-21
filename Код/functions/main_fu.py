
import pandas as pd


# Удаляет столбцы с >60% пропусков, сохраняет результат в исходный файл и возвращает кол-во удаленных
def removing_columns(file_path):

    df = pd.read_csv(file_path, na_values=[''])
    threshold = 0.6 * len(df)
    
    # Находим столбцы для удаления
    cols_to_drop = df.columns[df.isna().sum() >= threshold].tolist()
    num_removed = len(cols_to_drop)
    
    if num_removed > 0:
        df = df.drop(columns=cols_to_drop)
        
    # Сохраняем обратно в тот же файл
    df.to_csv(file_path, index=False, encoding='utf-8-sig')  # utf-8-sig для корректного отображения кириллицы
    return num_removed


def remove_unnecessary_columns(files, columns_to_drop):
    
    for file_path in files:
        # Читаем CSV-файл
        df = pd.read_csv(file_path)
        
        # Удаляем ненужные колонки (игнорируем ошибки если колонки нет)
        df.drop(columns=columns_to_drop, axis=1, inplace=True, errors='ignore')
        
        # Перезаписываем исходный файл
        df.to_csv(file_path, index=False)



import pandas as pd

# Список колонок для сохранения
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
    'ЖРС_Средневзвешенная цена аглоруда Fe 52-60%, Россия FCA руб./т, без НДС'
]



