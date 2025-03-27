import pandas as pd
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import pickle
from datetime import datetime, timedelta
import asyncio
import logging
import numpy as np

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Загрузка данных и модели
try:
    # Загрузка и подготовка данных
    train = pd.read_csv('traintest/train.csv', 
                       parse_dates=['dt'], 
                       index_col="dt")
    
    test = pd.read_csv('traintest/test.csv',
                      parse_dates=['dt'],
                      index_col='dt')
    
    # Объединение train и test для максимальной истории
    full_data = pd.concat([train, test])
    full_data = full_data.sort_index()
    
    # Подготовка данных с лагами
    def prepare_data(df, target_col='Цена на арматуру', window=3):
        df = df.copy()
        for i in range(1, window+1):
            df[f'lag_{i}'] = df[target_col].shift(i)
        df['rolling_mean'] = df[target_col].rolling(window).mean()
        return df.dropna()
    
    processed_data = prepare_data(full_data)
    
    # Загрузка модели
    with open('rf_model.pkl', 'rb') as f:
        model = pickle.load(f)
    logger.info("Model loaded successfully")

except Exception as e:
    logger.error(f"Initialization error: {e}")
    raise

# Инициализация бота
bot = Bot(token='7611697591:AAEY-sZNKJrdqgmdu5bHGDdO-mKxKlF-i4k')
dp = Dispatcher()

def get_last_available_date():
    return processed_data.index.max().date()

def prepare_features(target_date: datetime):
    """Подготовка признаков для указанной даты"""
    try:
        target_date = target_date.date()
        last_available = get_last_available_date()
        
        # Если дата в будущем - используем последние доступные данные
        if target_date > last_available:
            logger.info("Using last available data for future prediction")
            latest = processed_data.iloc[-1]
            return pd.DataFrame([{
                'lag_1': latest['Цена на арматуру'],
                'lag_2': processed_data.iloc[-2]['Цена на арматуру'],
                'lag_3': processed_data.iloc[-3]['Цена на арматуру'],
                'rolling_mean': latest['rolling_mean']
            }])
            
        # Если дата в прошлом - используем исторические данные
        if target_date in processed_data.index:
            return processed_data.loc[[target_date], ['lag_1', 'lag_2', 'lag_3', 'rolling_mean']]
            
        return None
        
    except Exception as e:
        logger.error(f"Feature preparation error: {e}")
        return None

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "🏗 Бот для прогнозирования закупок арматуры\n\n"
        "📅 Введите дату в формате ГГГГ-ММ-ДД\n"
        "Пример: 2024-03-25\n"
        "Я предскажу оптимальное количество недель для закупки!"
    )

@dp.message()
async def handle_message(message: types.Message):
    try:
        date_str = message.text.strip()
        target_date = datetime.strptime(date_str, '%Y-%m-%d')
        
        # Проверка что это понедельник
        if target_date.weekday() != 0:
            await message.answer("📅 Дата должна быть понедельником!")
            return
            
        # Подготовка признаков
        features = prepare_features(target_date)
        
        if features is None or features.isna().any().any():
            await message.answer("⚠️ Недостаточно данных для анализа")
            return

        # Прогнозирование
        prediction = model.predict(features)[0]
        weeks = max(1, min(6, int(round(prediction))))
        
        response = (
            f"📅 На дату {target_date.strftime('%d.%m.%Y')}\n"
            f"📈 Рекомендуемое количество недель для закупки: {weeks}\n\n"
            f"Совет: {'Увеличьте объем закупки' if weeks > 3 else 'Закупайте небольшими партиями'}"
        )
        
        await message.answer(response)

    except ValueError:
        await message.answer("❌ Неверный формат даты! Используйте ГГГГ-ММ-ДД")
    except Exception as e:
        logger.error(f"Error: {e}")
        await message.answer("⚠️ Ошибка обработки запроса")

async def main():
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())