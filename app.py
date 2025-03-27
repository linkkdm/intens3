import tkinter as tk
from tkinter import ttk
import pandas as pd
import xgboost as xgb
import joblib 
from datetime import datetime, timedelta

class ProcurementRecommender:
    def __init__(self, master):
        self.master = master
        self.master.title("Рекомендационная система закупок")
        
        # Загрузка модели и данных
        self.load_model_and_data()
        
        # Создание интерфейса
        self.create_widgets()
        self.show_current_price()

    def load_model_and_data(self):
        """Загрузка модели и данных с обработкой исключений"""
        try:
            # Загрузка модели
            with open('xgboost_model.pkl', 'rb') as f:
                self.model = joblib.load(f)  # Или pickle.load(f)
            
            # Загрузка данных
            self.data = pd.read_csv('Код\\traintest\\train.csv')
            
            # Проверка необходимых колонок
            if 'dt' not in self.data or 'Цена на арматуру' not in self.data:
                raise ValueError("CSV-файл должен содержать колонки 'date' и 'price'")
                
        except Exception as e:
            tk.messagebox.showerror("Ошибка загрузки", f"Ошибка: {str(e)}")
            self.master.destroy()

    def create_widgets(self):
        self.main_frame = ttk.Frame(self.master, padding="15")
        self.main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Информация о текущей цене
        self.current_price_header = ttk.Label(self.main_frame, 
                                            text="Текущая ситуация на рынке:",
                                            font=('Helvetica', 12, 'bold'))
        self.current_price_header.grid(row=0, column=0, columnspan=2, pady=5, sticky=tk.W)
        
        self.current_price_label = ttk.Label(self.main_frame, text="Цена: ")
        self.current_price_label.grid(row=1, column=0, sticky=tk.W)
        
        self.price_date_label = ttk.Label(self.main_frame, text="Дата: ")
        self.price_date_label.grid(row=1, column=1, sticky=tk.W)
        
        # Кнопка прогноза
        self.predict_button = ttk.Button(self.main_frame, 
                                       text="Получить рекомендацию", 
                                       command=self.predict_recommendation)
        self.predict_button.grid(row=2, column=0, columnspan=2, pady=10)
        
        # Результаты прогноза
        self.recommendation_header = ttk.Label(self.main_frame, 
                                             text="Рекомендация по закупке:",
                                             font=('Helvetica', 12, 'bold'))
        self.recommendation_header.grid(row=3, column=0, columnspan=2, pady=5, sticky=tk.W)
        
        self.forecast_label = ttk.Label(self.main_frame, text="Прогноз цены: ")
        self.forecast_label.grid(row=4, column=0, columnspan=2, sticky=tk.W)
        
        self.recommendation_label = ttk.Label(self.main_frame, 
                                            text="Рекомендуемый объем закупки: ",
                                            font=('Helvetica', 10, 'bold'))
        self.recommendation_label.grid(row=5, column=0, columnspan=2, pady=5, sticky=tk.W)

    def show_current_price(self):
        """Отображает текущую цену и дату"""
        last_row = self.data.iloc[-1]
        current_price = last_row['Цена на арматуру']
        price_date = last_row['dt']
        
        self.current_price_label.config(text=f"Цена: {current_price:.2f} руб/т")
        self.price_date_label.config(text=f"Дата: {price_date}")

    def prepare_features(self):
        """Подготовка данных для прогноза"""
        # Убедитесь, что названия колонок совпадают с тренировочными данными
        features = self.data.tail(7).drop(['dt', 'Цена на арматуру'], axis=1)
        return features  # Возвращаем DataFrame вместо DMatrix

    def predict_recommendation(self):
        """Выполнение прогноза и вывод результатов"""
        try:
            # Получение прогноза
            features = self.prepare_features()
            prediction = self.model.predict(features)
            
            # ... (остальная логика остается без изменений)

        except Exception as e:
            self.recommendation_label.config(
                text=f"Ошибка: {str(e)}",
                foreground="red"
            )

if __name__ == "__main__":
    root = tk.Tk()
    app = ProcurementRecommender(root)
    root.mainloop()