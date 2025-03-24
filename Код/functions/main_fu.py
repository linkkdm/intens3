
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Удаляет столбцы с >70% пропусков, сохраняет результат в исходный файл и возвращает кол-во удаленных
def removing_columns(file_path):

    df = pd.read_csv(file_path, na_values=[''])
    threshold = 0.7 * len(df)
    
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


def remove_outliers_iqr(df, columns):
    df_clean = df.copy()
    
    # Создаем маску для всех строк без выбросов
    mask = pd.Series(True, index=df.index)  # Изначально все строки включены
    
    for col in columns:
        Q1 = df_clean[col].quantile(0.25)
        Q3 = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        
        # Если все значения одинаковы (IQR=0), пропускаем столбец
        if IQR == 0:
            continue  
            
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        
        # Обновляем маску: оставляем строки, удовлетворяющие условию для текущего столбца
        mask &= (df_clean[col] >= lower) & (df_clean[col] <= upper)
    
    return df_clean[mask]


def plot_outliers(df, numeric_cols, max_cols_per_plot=15):
    num_plots = (len(numeric_cols) // max_cols_per_plot + 1)
    
    for plot_idx in range(num_plots):
        start_idx = plot_idx * max_cols_per_plot
        end_idx = start_idx + max_cols_per_plot
        cols_batch = numeric_cols[start_idx:end_idx]
        
        plt.figure(figsize=(20, 10))
        plt.suptitle(f"Выбросы (группа {plot_idx + 1}/{num_plots})", fontsize=14, y=1.02)
        
        for i, col in enumerate(cols_batch, 1):
            plt.subplot(3, 5, i)  # 3 строки, 5 столбцов = 15 графиков на фигуру
            sns.boxplot(y=df[col], color='skyblue', width=0.4)
            plt.title(col[:20] + "..." if len(col) > 20 else col, fontsize=9)
            plt.ylabel('')  # Убираем подписи осей для экономии места
            plt.xlabel('')
            
        plt.tight_layout()
        plt.show()