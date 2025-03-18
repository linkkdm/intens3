
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
