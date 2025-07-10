import pandas as pd
import requests
from io import BytesIO
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
import asyncio
import os
import logging
from datetime import datetime
import aiohttp
import nest_asyncio

# Применяем исправление для работы с event loop
nest_asyncio.apply()

# Настройка логирования
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

# Словарь программ
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

# Вспомогательные функции должны быть определены перед их использованием
def log_user_action(user_id: int, action: str):
    """Логирование действий пользователя"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"User ID: {user_id} - Action: {action} - Time: {timestamp}")

def get_program_keyboard(include_refresh=False, current_program=None):
    buttons = [
        [InlineKeyboardButton(text=PROGRAMS["hse"]["name"], callback_data="hse")],
        [InlineKeyboardButton(text=PROGRAMS["resh"]["name"], callback_data="resh")]
    ]
    
    if include_refresh and current_program in PROGRAMS:
        buttons.append([InlineKeyboardButton(text="🔄 Обновить данные", callback_data=f"refresh_{current_program}")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# Обработчики команд
async def start(message: types.Message):
    log_user_action(message.from_user.id, "Started bot")
    await message.answer("Выбери программу для анализа рейтинга:", reply_markup=get_program_keyboard())

async def process_program(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    # Обработка кнопки обновления
    if callback.data.startswith("refresh_"):
        key = callback.data.split("_")[1]
        await callback.answer("Обновляю данные...")
    else:
        key = callback.data
    
    if key not in PROGRAMS:
        await callback.answer("Неизвестная программа")
        return
        
    program = PROGRAMS[key]
    
    try:
        log_user_action(user_id, f"Selected program: {program['name']}")
        await callback.answer()
        msg = await callback.message.answer(f"🔄 Загружаю данные: *{program['name']}*", parse_mode=ParseMode.MARKDOWN)

        try:
            log_user_action(user_id, f"Downloading data from {program['url']}")
            async with aiohttp.ClientSession() as session:
                async with session.get(program['url'], timeout=10) as response:
                    response.raise_for_status()
                    content = await response.read()
                    df = pd.read_excel(BytesIO(content), engine='openpyxl', header=None)
        except Exception as e:
            error_msg = f"Ошибка загрузки: {str(e)[:200]}"
            log_user_action(user_id, error_msg)
            await callback.message.answer(f"❌ {error_msg}", reply_markup=get_program_keyboard(include_refresh=True, current_program=key))
            return

        if df.shape[1] < 19:
            error_msg = "Файл содержит недостаточно столбцов."
            log_user_action(user_id, error_msg)
            await callback.message.answer(f"❌ {error_msg}", reply_markup=get_program_keyboard(include_refresh=True, current_program=key))
            return

        try:
            report_datetime = df.iloc[4, 5] if pd.notna(df.iloc[4, 5]) else "не указана"
            
            if pd.api.types.is_datetime64_any_dtype(df.iloc[4, 5]):
                report_datetime = report_datetime.strftime("%d.%m.%Y %H:%M")
            
            target_priority = program["priority"]
            places = program["places"]
            
            if target_priority == 1:
                filtered_1 = df[
                    (df[7].astype(str).str.strip().str.upper() == "ДА") &
                    (df[11].astype(str).str.strip() == "1")
                ].copy()

                if filtered_1.empty:
                    log_user_action(user_id, "No applicants with priority 1 found")
                    await callback.message.answer("⚠️ Нет абитуриентов с 1 приоритетом.", 
                                               reply_markup=get_program_keyboard(include_refresh=True, current_program=key))
                    return

                filtered_1 = filtered_1.sort_values(by=18, ascending=False)
                filtered_1['rank'] = range(1, len(filtered_1) + 1)

                applicant = filtered_1[filtered_1[1].astype(str).str.strip() == "4272684"]
                if applicant.empty:
                    log_user_action(user_id, "Applicant 4272684 not found in priority 1")
                    await callback.message.answer("🚫 Номер 4272684 не найден среди 1 приоритета.", 
                                               reply_markup=get_program_keyboard(include_refresh=True, current_program=key))
                    return

                rank = applicant['rank'].values[0]
                score = applicant[18].values[0]

                result_msg = (
                    f"📅 *Дата обновления данных:* {report_datetime}\n\n"
                    f"🎯 Мест на программе: *{places}*\n\n"
                    f"✅ Твой рейтинг среди 1 приоритета: *{rank}*"
                )

                filtered_2 = df[
                    (df[7].astype(str).str.strip().str.upper() == "ДА") &
                    (df[11].astype(str).str.strip() == "2")
                ].copy()

                if not filtered_2.empty:
                    higher_2_than_her = filtered_2[filtered_2[18] > score]
                    count_higher_2 = len(higher_2_than_her) + 1
                    result_msg += f"\n\n🔺 Людей со 2 приоритетом и баллом выше: *{count_higher_2}*"
                else:
                    result_msg += "\n\n🔺 Людей со 2 приоритетом и баллом выше: *0*"

            else:
                filtered_2 = df[
                    (df[7].astype(str).str.strip().str.upper() == "ДА") &
                    (df[11].astype(str).str.strip() == "2")
                ].copy()

                if filtered_2.empty:
                    log_user_action(user_id, "No applicants with priority 2 found")
                    await callback.message.answer("⚠️ Нет абитуриентов со 2 приоритетом.", 
                                               reply_markup=get_program_keyboard(include_refresh=True, current_program=key))
                    return

                filtered_2 = filtered_2.sort_values(by=18, ascending=False)
                filtered_2['rank_2'] = range(1, len(filtered_2) + 1)

                applicant = filtered_2[filtered_2[1].astype(str).str.strip() == "4272684"]
                if applicant.empty:
                    log_user_action(user_id, "Applicant 4272684 not found in priority 2")
                    await callback.message.answer("🚫 Номер 4272684 не найден среди 2 приоритета.", 
                                               reply_markup=get_program_keyboard(include_refresh=True, current_program=key))
                    return

                rank_2 = applicant['rank_2'].values[0]
                score = applicant[18].values[0]

                result_msg = (
                    f"📅 *Дата обновления данных:* {report_datetime}\n\n"
                    f"🎯 Мест на программе: *{places}*\n\n"
                    f"✅ Твой рейтинг среди 2 приоритета: *{rank_2}*"
                )

                filtered_1 = df[
                    (df[7].astype(str).str.strip().str.upper() == "ДА") &
                    (df[11].astype(str).str.strip() == "1")
                ].copy()

                if not filtered_1.empty:
                    higher_1_than_her = filtered_1[filtered_1[18] > score]
                    count_higher_1 = len(higher_1_than_her) + 1
                    result_msg += f"\n\n🔺 Людей с 1 приоритетом и баллом выше: *{count_higher_1}*"
                else:
                    result_msg += "\n\n🔺 Людей с 1 приоритетом и баллом выше: *0*"

            log_user_action(user_id, f"Successfully processed request")
            await callback.message.answer(result_msg, 
                                         parse_mode=ParseMode.MARKDOWN, 
                                         reply_markup=get_program_keyboard(include_refresh=True, current_program=key))

        except Exception as e:
            error_msg = f"Ошибка обработки данных: {str(e)[:200]}"
            log_user_action(user_id, error_msg)
            await callback.message.answer(f"❌ {error_msg}", 
                                        reply_markup=get_program_keyboard(include_refresh=True, current_program=key))

    except Exception as e:
        logger.exception("Unexpected error in process_program")
        await callback.message.answer("⚠️ Произошла непредвиденная ошибка", 
                                    reply_markup=get_program_keyboard(include_refresh=True, current_program=key))

async def main():
    try:
        logger.info("Starting bot...")
        bot = Bot(token=TOKEN)
        dp = Dispatcher()
        
        dp.message.register(start, F.text == "/start")
        dp.callback_query.register(process_program, F.data.startswith("hse") | F.data.startswith("resh") | F.data.startswith("refresh_"))
        
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        logger.info("Bot stopped by cancellation")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        raise
    finally:
        if 'bot' in locals():
            await bot.session.close()
        logger.info("Bot fully stopped")

if __name__ == "__main__":
    asyncio.run(main())
