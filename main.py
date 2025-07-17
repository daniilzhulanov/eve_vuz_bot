import pandas as pd
from io import BytesIO
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.enums import ParseMode
import asyncio
import os
import logging
import aiohttp
import nest_asyncio

# Настройка окружения
nest_asyncio.apply()

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Токен не найден. Установите переменную окружения TOKEN.")

# Настройки ВШЭ
HSE_PROGRAMS = {
    "hse": {
        "name": "Экономика",
        "url": "https://enrol.hse.ru/storage/public_report_2025/moscow/Bachelors/BD_moscow_Economy_O.xlsx",
        "priority": 1,
        "places": 10
    },
    "resh": {
        "name": "Совбак НИУ ВШЭ и РЭШ",
        "url": "https://enrol.hse.ru/storage/public_report_2025/moscow/Bachelors/BD_moscow_RESH_O.xlsx",
        "priority": 2,
        "places": 6
    }
}

# Закрепленные клавиатуры
def get_main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Экономика"), KeyboardButton(text="📘 Совбак")]
        ],
        resize_keyboard=True,
        persistent=True
    )

def get_back_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True,
        persistent=True
    )

# Обработчики команд
async def start(message: types.Message):
    await message.answer(
        "Выберите программу ВШЭ:",
        reply_markup=get_main_keyboard()
    )

async def handle_back(message: types.Message):
    await message.answer(
        "Выберите программу ВШЭ:",
        reply_markup=get_main_keyboard()
    )

# Обработчик программ ВШЭ
async def process_hse_program(message: types.Message):
    user_id = message.from_user.id
    key = None
    
    if message.text == "📊 Экономика":
        key = "hse"
    elif message.text == "📘 Совбак":
        key = "resh"
    
    if not key or key not in HSE_PROGRAMS:
        await message.answer("Неизвестная команда")
        return
        
    program = HSE_PROGRAMS[key]
    
    try:
        logger.info(f"User {user_id} selected: {program['name']}")
        msg = await message.answer(f"🔄 Загружаю данные: {program['name']}", reply_markup=get_back_keyboard())

        async with aiohttp.ClientSession() as session:
            async with session.get(program['url'], timeout=10) as response:
                content = await response.read()
                df = pd.read_excel(BytesIO(content), engine='openpyxl', header=None)

        if df.shape[1] < 19:
            await message.answer("❌ Файл содержит недостаточно столбцов.")
            return

        report_datetime = df.iloc[4, 5] if pd.notna(df.iloc[4, 5]) else "не указана"
        if pd.api.types.is_datetime64_any_dtype(df.iloc[4, 5]):
            report_datetime = report_datetime.strftime("%d.%m.%Y %H:%M")
        
        target_priority = program["priority"]
        places = program["places"]
        result_msg = f"📅 Дата обновления: {report_datetime}\n🎯 Мест: {places}\n\n"
        
        # Фильтрация по приоритету
        filtered = df[
            (df[9].astype(str).str.strip().str.upper() == "ДА") & 
            (df[11].astype(str).str.strip() == str(target_priority))
        ].copy()

        if filtered.empty:
            await message.answer(f"⚠️ Нет абитуриентов с {target_priority} приоритетом")
            return

        filtered = filtered.sort_values(by=18, ascending=False)
        filtered['rank'] = range(1, len(filtered) + 1)

        # Поиск абитуриента
        applicant = filtered[filtered[1].astype(str).str.strip() == "4272684"]  
        if applicant.empty:
            await message.answer(f"🚫 Номер 4272684 не найден среди {target_priority} приоритета")
            return

        rank = applicant['rank'].values[0]
        score = applicant[18].values[0]
        result_msg += f"✅ Ваш рейтинг: {rank}\n🔢 Ваш балл: {score}"

        # Сравнение с другим приоритетом
        other_priority = 2 if target_priority == 1 else 1
        other_df = df[
            (df[9].astype(str).str.strip().str.upper() == "ДА") & 
            (df[11].astype(str).str.strip() == str(other_priority))
        ]

        if not other_df.empty:
            higher_others = len(other_df[other_df[18] > score])
            result_msg += f"\n\n🔺 Абитуриентов с {other_priority} приоритетом и баллом выше: {higher_others}"

        await message.answer(result_msg)

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        await message.answer(f"❌ Ошибка: {str(e)[:200]}")

# Основная функция
async def main():
    try:
        bot = Bot(token=TOKEN)
        dp = Dispatcher()
        
        # Регистрация обработчиков
        dp.message.register(start, F.text == "/start")
        dp.message.register(handle_back, F.text == "🔙 Назад")
        dp.message.register(process_hse_program, F.text.in_(["📊 Экономика", "📘 Совбак"]))
        
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
