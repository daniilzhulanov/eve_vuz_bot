import pandas as pd
from io import BytesIO
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.filters import Command
import asyncio
import os
import logging
from datetime import datetime
import aiohttp
import nest_asyncio
from collections import defaultdict
import hashlib

# Настройка event loop
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

# Конфигурация
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Токен не найден. Установите переменную окружения TOKEN.")

# Словарь программ
PROGRAMS = {
    "hse": {
        "name": "📊 Экономика",
        "url": "https://enrol.hse.ru/storage/public_report_2025/moscow/Bachelors/BD_moscow_Economy_O.xlsx",
        "priority": 1,
        "places": 10,
        "last_hash": None,
        "last_rank": None,
        "last_other_higher": None,
        "last_consent_higher": None  # Новое поле для отслеживания согласий
    },
    "resh": {
        "name": "📘 Совбак НИУ ВШЭ и РЭШ",
        "url": "https://enrol.hse.ru/storage/public_report_2025/moscow/Bachelors/BD_moscow_RESH_O.xlsx",
        "priority": 2,
        "places": 6,
        "last_hash": None,
        "last_rank": None,
        "last_other_higher": None,
        "last_consent_higher": None  # Новое поле для отслеживания согласий
    }
}

# Хранилище активных пользователей
active_users = set()
check_task = None

def format_change(current, previous):
    if previous is None:
        return ""
    change = current - previous
    if change == 0:
        return " (не изменилось)"
    elif change > 0:
        return f" (+{change})"
    else:
        return f" ({change})"

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

async def download_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.read()

async def process_data(program_key, user_id=None, is_update=False):
    program = PROGRAMS[program_key]
    try:
        content = await download_data(program["url"])
        current_hash = hashlib.md5(content).hexdigest()
        
        if is_update and program["last_hash"] == current_hash:
            return None
            
        df = pd.read_excel(BytesIO(content), engine='openpyxl', header=None)
        
        if df.shape[1] < 32:
            raise ValueError(f"Файл содержит {df.shape[1]} столбцов (ожидалось 32)")
        
        report_datetime = df.iloc[4, 5] if pd.notna(df.iloc[4, 5]) else "не указана"
        if pd.api.types.is_datetime64_any_dtype(df.iloc[4, 5]):
            report_datetime = report_datetime.strftime("%d.%m.%Y %H:%M")
        
        target_priority = program["priority"]
        places = program["places"]
        
        # Преобразуем баллы в числа и фильтруем некорректные значения
        df[18] = pd.to_numeric(df[18], errors='coerce')
        df = df[pd.notna(df[18])]  # Удаляем строки с некорректными баллами
        
        # Фильтрация по квоте (столбец 9) и приоритету (столбец 11)
        filtered = df[
            (df[9].astype(str).str.strip().str.upper() == "ДА") & 
            (df[11].astype(str).str.strip() == str(target_priority))
        ].copy()
        
        if filtered.empty:
            return None
        
        filtered = filtered.sort_values(by=18, ascending=False)
        filtered['rank'] = range(1, len(filtered) + 1)

        applicant = filtered[filtered[1].astype(str).str.strip() == "4272684"]  
        if applicant.empty:
            return None
        
        rank = applicant['rank'].values[0]
        score = float(applicant[18].values[0])  # Явное преобразование в float
        
        # Расчет людей с другим приоритетом и баллом выше (без согласия)
        other_priority = 1 if target_priority == 2 else 2
        filtered_other = df[
            (df[9].astype(str).str.strip().str.upper() == "ДА") & 
            (df[11].astype(str).str.strip() == str(other_priority))
        ].copy()
        
        count_higher = 0
        if not filtered_other.empty:
            higher_other = filtered_other[filtered_other[18] > score]
            count_higher = len(higher_other)

        # Фильтр для согласий (используем столбец 25 как указано в комментарии)
        consent_priority = 1
        consent_filtered = df[
            (df[9].astype(str).str.strip().str.upper() == "ДА") &
            (df[11].astype(str).str.strip() == str(consent_priority)) &
            (df[24].astype(str).str.strip().str.upper() == "ДА") &  # Столбец 25 для согласия
            (df[18] > score)
        ]
        
        count_consent_higher = len(consent_filtered)
        
        # Формируем сообщение с изменениями
        rank_change = format_change(rank, program["last_rank"])
        higher_change = format_change(count_higher, program["last_other_higher"])
        consent_change = format_change(count_consent_higher, program["last_consent_higher"])
        
        # Обновляем сообщение с новым пунктом
        if is_update:
            result_msg = (
                f"🔔 *Обновление данных*\n"
                f"📌 *Направление:* {program['name']}\n\n"
                f"📅 *Дата обновления:* {report_datetime}\n\n"
                f"🎯 Мест на программе: *{places}*\n\n"
                f"✅ Твой рейтинг среди {target_priority} приоритета: *{rank}{rank_change}*\n\n"
                f"📥 Подано согласий с баллом выше (для 1 приоритета): *{count_consent_higher}{consent_change}*\n\n"
                f"🔺 Людей с {other_priority} приоритетом и баллом выше: *{count_higher}{higher_change}*"
            )
        else:
            result_msg = (
                f"📌 *Направление:* {program['name']}\n\n"
                f"📅 *Дата обновления:* {report_datetime}\n\n"
                f"🎯 Мест на программе: *{places}*\n\n"
                f"✅ Твой рейтинг среди {target_priority} приоритета: *{rank}*\n\n"
                f"📥 Подано согласий с баллом выше (для 1 приоритета): *{count_consent_higher}*\n\n"
                f"🔺 Людей с {other_priority} приоритетом и баллом выше: *{count_higher}*"
            )
        
        # Сохраняем текущие значения
        program["last_hash"] = current_hash
        program["last_rank"] = rank
        program["last_other_higher"] = count_higher
        program["last_consent_higher"] = count_consent_higher  
        
        return result_msg
    except Exception as e:
        logger.error(f"Ошибка обработки данных: {e}")
        return None

async def check_updates(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60)  # Проверка каждые минуту
            
            for program_key in PROGRAMS:
                update_msg = await process_data(program_key, is_update=True)
                if update_msg:
                    for user_id in list(active_users):  # Используем копию списка на случай изменений
                        try:
                            await bot.send_message(
                                user_id,
                                update_msg,
                                parse_mode=ParseMode.MARKDOWN,
                                reply_markup=get_program_keyboard(
                                    include_refresh=True,
                                    current_program=program_key
                                )
                            )
                        except Exception as e:
                            logger.error(f"Ошибка отправки пользователю {user_id}: {e}")
                            # Удаляем неактивного пользователя
                            active_users.discard(user_id)
        except Exception as e:
            logger.error(f"Ошибка в check_updates: {e}")

async def start(message: types.Message):
    user_id = message.from_user.id
    active_users.add(user_id)  # Добавляем пользователя в список активных
    log_user_action(user_id, "Started bot")
    await message.answer(
        "Выберите программу для анализа рейтинга:",
        reply_markup=get_program_keyboard()
    )

async def process_program(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    active_users.add(user_id)  # Добавляем пользователя в список активных
    
    if callback.data.startswith("refresh_"):
        key = callback.data.split("_")[1]
        await callback.answer("Обновляем данные...")
    else:
        key = callback.data
    
    if key not in PROGRAMS:
        await callback.answer("Неизвестная программа")
        return
        
    program = PROGRAMS[key]
    log_user_action(user_id, f"Selected program: {program['name']}")
    
    try:
        await callback.answer()
        status_msg = await process_data(key, user_id)
        if status_msg:
            await callback.message.answer(
                status_msg,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_program_keyboard(
                    include_refresh=True,
                    current_program=key
                )
            )
        else:
            await callback.message.answer(
                "❌ Не удалось получить данные",
                reply_markup=get_program_keyboard(
                    include_refresh=True,
                    current_program=key
                )
            )
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await callback.message.answer(
            "⚠️ Произошла ошибка",
            reply_markup=get_program_keyboard(
                include_refresh=True,
                current_program=key
            )
        )

async def on_startup(bot: Bot):
    global check_task
    check_task = asyncio.create_task(check_updates(bot))

async def on_shutdown(bot: Bot):
    if check_task:
        check_task.cancel()
    await bot.session.close()

async def main():
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    dp.message.register(start, Command("start"))
    dp.callback_query.register(process_program)
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
