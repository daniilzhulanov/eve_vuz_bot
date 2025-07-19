from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
import os
import pandas as pd
import requests
from io import BytesIO

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Токен не найден. Установите переменную окружения TOKEN.")

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

PROGRAMS = {
    "hse": {
        "name": "📊 Экономика",
        "url": "https://enrol.hse.ru/storage/public_report_2025/moscow/Bachelors/BD_moscow_Economy_O.xlsx",
        "priority": 1,
        "places": 10
    },
    "resh": {
        "name": "📘 Совбак НИУ ВШЭ и РЭШ",
        "url": "https://enrol.hse.ru/storage/public_report_2025/moscow/Bachelors/BD_moscow_RESH_O.xlsx",
        "priority": 2,
        "places": 6
    }
}

def get_program_info(program_key, user_number=4272684):
    program = PROGRAMS[program_key]
    url = program["url"]
    K = program["priority"]
    places = program["places"]
    
    # Загрузка Excel файла
    response = requests.get(url)
    if response.status_code != 200:
        return "Ошибка: не удалось загрузить файл."
    
    # Чтение Excel файла
    df = pd.read_excel(BytesIO(response.content), header=None)
    
    # Фильтрация строк, где столбец 10 (индекс 9) равен "Да"
    df_filtered = df[df.iloc[:, 9] == "Да"]
    
    # Поиск строки пользователя
    user_row = df_filtered[df_filtered.iloc[:, 1] == user_number]
    if user_row.empty:
        return "Ошибка: пользователь не найден."
    
    user_score = user_row.iloc[0, 18]
    
    # Поиск всех с приоритетом K
    priority_K = df_filtered[df_filtered.iloc[:, 11] == K]
    scores_K = priority_K.iloc[:, 18]
    N_higher = (scores_K > user_score).sum()
    rank = N_higher + 1
    
    # Подсчет людей с другим приоритетом и более высоким баллом
    other_priority = 3 - K
    priority_other = df_filtered[df_filtered.iloc[:, 11] == other_priority]
    num_higher_other = (priority_other.iloc[:, 18] > user_score).sum()
    
    # Формирование сообщения
    message = f"📅 Дата обновления данных: 19.07.2025 19:06:10\n\n"
    message += f"🎯 Мест на программе: {places}\n\n"
    message += f"✅ Твой рейтинг среди {K} приоритета: {rank}\n\n"
    message += f"🔺 Людей с {other_priority} приоритетом и баллом выше: {num_higher_other}"
    
    return message

@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("Экономика", callback_data="hse"))
    keyboard.add(types.InlineKeyboardButton("Совбак НИУ ВШЭ и РЭШ", callback_data="resh"))
    await message.reply("Выберите программу:", reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data in ["hse", "resh"])
async def process_callback(callback_query: types.CallbackQuery):
    program_key = callback_query.data
    info = get_program_info(program_key)
    await bot.send_message(callback_query.from_user.id, info)
    await callback_query.answer()

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
