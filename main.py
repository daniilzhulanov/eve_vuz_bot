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
        "last_hash": None
    },
    "resh": {
        "name": "📘 Совбак НИУ ВШЭ и РЭШ",
        "url": "https://enrol.hse.ru/storage/public_report_2025/moscow/Bachelors/BD_moscow_RESH_O.xlsx",
        "priority": 2,
        "places": 6,
        "last_hash": None
    }
}

# Хранилище подписок
subscriptions = defaultdict(dict)
check_task = None

def log_user_action(user_id: int, action: str):
    """Логирование действий пользователя"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"User ID: {user_id} - Action: {action} - Time: {timestamp}")

def get_program_keyboard(include_refresh=False, include_subscribe=False, current_program=None):
    buttons = [
        [InlineKeyboardButton(text=PROGRAMS["hse"]["name"], callback_data="hse")],
        [InlineKeyboardButton(text=PROGRAMS["resh"]["name"], callback_data="resh")]
    ]
    
    if include_refresh and current_program in PROGRAMS:
        buttons.append([InlineKeyboardButton(text="🔄 Обновить данные", callback_data=f"refresh_{current_program}")])
    
    if include_subscribe and current_program in PROGRAMS:
        is_subscribed = subscriptions.get(current_program, {}).get("subscribed", False)
        text = "🔴 Отписаться" if is_subscribed else "🟢 Подписаться"
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"subscribe_{current_program}")])
    
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
            
        program["last_hash"] = current_hash
        df = pd.read_excel(BytesIO(content), engine='openpyxl', header=None)
        
        if df.shape[1] < 32:
            raise ValueError(f"Файл содержит {df.shape[1]} столбцов (ожидалось 32)")
        
        report_datetime = df.iloc[4, 5] if pd.notna(df.iloc[4, 5]) else "не указана"
        if pd.api.types.is_datetime64_any_dtype(df.iloc[4, 5]):
            report_datetime = report_datetime.strftime("%d.%m.%Y %H:%M")
        
        target_priority = program["priority"]
        places = program["places"]
        
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
        score = applicant[18].values[0]
        
        # Оригинальное оформление сообщения
        if is_update:
            result_msg = (
                f"🔔 *Обновление данных*\n\n"
                f"📅 *Дата обновления:* {report_datetime}\n\n"
                f"🎯 Мест на программе: *{places}*\n\n"
                f"✅ Твой рейтинг среди {target_priority} приоритета: *{rank}*"
            )
        else:
            result_msg = (
                f"📅 *Дата обновления:* {report_datetime}\n\n"
                f"🎯 Мест на программе: *{places}*\n\n"
                f"✅ Твой рейтинг среди {target_priority} приоритета: *{rank}*"
            )
        
        other_priority = 1 if target_priority == 2 else 2
        filtered_other = df[
            (df[9].astype(str).str.strip().str.upper() == "ДА") & 
            (df[11].astype(str).str.strip() == str(other_priority))
        ].copy()
        
        if not filtered_other.empty:
            higher_other = filtered_other[filtered_other[18] > score]
            count_higher = len(higher_other)
            result_msg += f"\n\n🔺 Людей с {other_priority} приоритетом и баллом выше: *{count_higher}*"
        else:
            result_msg += f"\n\n🔺 Людей с {other_priority} приоритетом и баллом выше: *0*"
        
        return result_msg
    except Exception as e:
        logger.error(f"Ошибка обработки данных: {e}")
        return None

async def check_updates(bot: Bot):
    while True:
        try:
            await asyncio.sleep(1800)  # Проверка каждые 30 минут
            
            for program_key in PROGRAMS:
                update_msg = await process_data(program_key, is_update=True)
                if update_msg:
                    for user_id in subscriptions.get(program_key, {}).get("users", []):
                        try:
                            await bot.send_message(
                                user_id,
                                update_msg,
                                parse_mode=ParseMode.MARKDOWN,
                                reply_markup=get_program_keyboard(
                                    include_refresh=True,
                                    include_subscribe=True,
                                    current_program=program_key
                                )
                            )
                        except Exception as e:
                            logger.error(f"Ошибка отправки: {e}")
        except Exception as e:
            logger.error(f"Ошибка в check_updates: {e}")

async def start(message: types.Message):
    log_user_action(message.from_user.id, "Started bot")
    await message.answer(
        "Выберите программу для анализа рейтинга:",
        reply_markup=get_program_keyboard()
    )

async def process_program(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if callback.data.startswith("refresh_"):
        key = callback.data.split("_")[1]
        await callback.answer("Обновляем данные...")
    elif callback.data.startswith("subscribe_"):
        key = callback.data.split("_")[1]
        if user_id not in subscriptions.setdefault(key, {}).setdefault("users", []):
            subscriptions[key]["users"].append(user_id)
            subscriptions[key]["subscribed"] = True
            await callback.answer("✅ Вы подписались на обновления")
        else:
            subscriptions[key]["users"].remove(user_id)
            subscriptions[key]["subscribed"] = False
            await callback.answer("❌ Вы отписались от обновлений")
        return
    else:
        key = callback.data
    
    if key not in PROGRAMS:
        await callback.answer("Неизвестная программа")
        return
        
    program = PROGRAMS[key]
    log_user_action(user_id, f"Selected program: {program['name']}")
    
    try:
        await callback.answer()
        msg = await callback.message.answer("⏳ Загружаю данные...")
        
        status_msg = await process_data(key, user_id)
        if status_msg:
            await callback.message.edit_text(
                status_msg,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_program_keyboard(
                    include_refresh=True,
                    include_subscribe=True,
                    current_program=key
                )
            )
        else:
            await callback.message.edit_text(
                "❌ Не удалось получить данные",
                reply_markup=get_program_keyboard(
                    include_refresh=True,
                    current_program=key
                )
            )
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await callback.message.edit_text(
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