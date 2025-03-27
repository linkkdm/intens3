import pandas as pd
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import pickle
from datetime import datetime, timedelta
import asyncio
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Загрузка модели и данных
try:
    with open('catboost_model.pkl', 'rb') as f:
        model = pickle.load(f)
    logger.info("Model loaded successfully")

    transformed_weekly = pd.read_csv('optimized_transformed_data.csv', parse_dates=['dt'])
    train = pd.read_csv('traintest/train.csv', parse_dates=['dt'])
    
    # Объединение данных
    merged_data = pd.merge(train, transformed_weekly, on='dt', how='left')
    merged_data.set_index('dt', inplace=True)
    last_historical_date = merged_data.index.max()
    logger.info(f"Last available historical date: {last_historical_date}")

except Exception as e:
    logger.error(f"Initialization error: {e}")
    raise

# Инициализация бота
bot = Bot(token='7611697591:AAEY-sZNKJrdqgmdu5bHGDdO-mKxKlF-i4k')
dp = Dispatcher()

def generate_future_features(target_date: datetime, last_known_data: pd.Series) -> pd.DataFrame:
    """Генерация признаков для будущих дат на основе последних доступных данных"""
    features = {
        'price_lag_1': last_known_data['Цена на арматуру'].iloc[-1],
        'price_lag_2': last_known_data['Цена на арматуру'].iloc[-2],
        'price_lag_3': last_known_data['Цена на арматуру'].iloc[-3],
        'rolling_mean_3': last_known_data['Цена на арматуру'].rolling(3).mean().iloc[-1]
    }
    
    # Добавление внешних признаков (пример для одного показателя)
    for col in transformed_weekly.columns:
        if col != 'dt' and col in last_known_data.columns:
            features[col] = last_known_data[col].iloc[-1]
    
    return pd.DataFrame([features])

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "🏗 Привет! Я помощник для закупки арматуры\n\n"
        "📅 Введите дату в формате ГГГГ-ММ-ДД (только будущие даты)\n"
        "Пример: 2024-03-25\n"
        "Я подскажу оптимальное количество недель для закупки!"
    )

@dp.message()
async def handle_message(message: types.Message):
    try:
        date_str = message.text.strip()
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        today = datetime.now().date()
        
        # Проверка что дата не в прошлом
        if target_date <= today:
            await message.answer("🚫 Введите будущую дату (позже завтрашнего дня)")
            return
            
        # Проверка что это понедельник
        if target_date.weekday() != 0:
            await message.answer("📅 Дата должна быть понедельником!")
            return

        # Используем последние доступные данные
        last_4_weeks = merged_data.last('4W')
        if len(last_4_weeks) < 3:
            await message.answer("⚠️ Недостаточно данных для прогноза")
            return

        # Генерация признаков
        features = generate_future_features(target_date, last_4_weeks)
        
        # Прогноз
        prediction = model.predict(features)[0]
        prediction = max(1, min(6, prediction))  # Ограничение от 1 до 6 недель
        
        response = (
            f"📅 Прогноз на {target_date.strftime('%d.%m.%Y')}\n"
            f"🔮 Рекомендуемая закупка на: {prediction} недель\n\n"
            f"Совет: {'Увеличьте закупку' if prediction > 3 else 'Закупайте небольшими партиями'}"
        )
        
        await message.answer(response)

    except ValueError:
        await message.answer("❌ Неверный формат! Используйте ГГГГ-ММ-ДД\nПример: 2024-03-25")
    except Exception as e:
        logger.error(f"Error: {e}")
        await message.answer("⚠️ Ошибка обработки запроса")

async def main():
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())