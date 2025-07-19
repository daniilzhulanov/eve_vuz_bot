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

def get_token():
    """Получение и валидация токена"""
    token = os.environ.get("TOKEN")
    if not token or not token.startswith('') or ':' not in token:  # Базовая проверка формата
        raise ValueError("Неверный формат токена. Установите переменную окружения TOKEN.")
    return token

TOKEN = get_token()

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

def log_user_action(user_id: int, action: str):
    """Логирование действий пользователя"""
    logger.info(f"User {user_id}: {action}")

def get_program_keyboard(include_refresh=False, current_program=None):
    buttons = [
        [InlineKeyboardButton(text=PROGRAMS["hse"]["name"], callback_data="hse")],
        [InlineKeyboardButton(text=PROGRAMS["resh"]["name"], callback_data="resh")]
    ]
    
    if include_refresh and current_program in PROGRAMS:
        buttons.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=f"refresh_{current_program}")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def start(message: types.Message):
    log_user_action(message.from_user.id, "Start command")
    await message.answer("Выберите программу:", reply_markup=get_program_keyboard())

async def process_program(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    try:
        if callback.data.startswith("refresh_"):
            program_key = callback.data.split("_")[1]
            await callback.answer("Обновление...")
        else:
            program_key = callback.data
        
        if program_key not in PROGRAMS:
            await callback.answer("Неизвестная программа")
            return
            
        program = PROGRAMS[program_key]
        log_user_action(user_id, f"Selected {program['name']}")
        
        await callback.message.edit_text(f"⏳ Загрузка данных {program['name']}...")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(program['url'], timeout=30) as response:
                    response.raise_for_status()
                    content = await response.read()
                    
            with BytesIO(content) as excel_file:
                df = pd.read_excel(excel_file, engine='openpyxl', header=None)
                
            # Проверка структуры файла
            if df.shape[1] < 20:
                raise ValueError("Неверный формат файла")
                
            # Получение даты отчета
            report_date = df.iloc[4, 5]
            if pd.isna(report_date):
                report_date = "неизвестно"
            elif hasattr(report_date, 'strftime'):
                report_date = report_date.strftime("%d.%m.%Y %H:%M")
                
            # Фильтрация данных
            filtered = df[
                (df[9].astype(str).str.strip().str.upper() == "ДА") & 
                (df[11].astype(str).str.strip() == str(program['priority']))
            ].copy()
            
            if filtered.empty:
                raise ValueError("Нет данных по выбранному приоритету")
                
            # Сортировка и ранжирование
            filtered = filtered.sort_values(by=18, ascending=False)
            filtered['rank'] = range(1, len(filtered)+1)
            
            # Поиск абитуриента
            applicant = filtered[filtered[1].astype(str).str.strip() == "4272684"]
            if applicant.empty:
                raise ValueError("Абитуриент не найден")
                
            rank = applicant['rank'].values[0]
            score = applicant[18].values[0]
            
            # Анализ других приоритетов
            other_priority = 1 if program['priority'] == 2 else 2
            higher_other = len(df[
                (df[9].astype(str).str.strip().str.upper() == "ДА") & 
                (df[11].astype(str).str.strip() == str(other_priority)) &
                (df[18] > score)
            ])
            
            # Формирование ответа
            message = (
                f"📅 Дата обновления: {report_date}\n\n"
                f"🎯 Мест: {program['places']}\n\n"
                f"✅ Рейтинг ({program['priority']} приоритет): {rank}\n\n"
                f"🔺 Выше с {other_priority} приоритетом: {higher_other}"
            )
            
            await callback.message.edit_text(
                message,
                reply_markup=get_program_keyboard(include_refresh=True, current_program=program_key)
            )
            
        except Exception as e:
            logger.error(f"Error processing {program['name']}: {str(e)}")
            await callback.message.edit_text(
                f"❌ Ошибка: {str(e)[:100]}",
                reply_markup=get_program_keyboard(include_refresh=True, current_program=program_key)
            )
            
    except Exception as e:
        logger.exception("Unexpected error in callback")
        await callback.answer("Произошла ошибка")

async def main():
    bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
    dp = Dispatcher()
    
    dp.message.register(start, F.text == "/start")
    dp.callback_query.register(process_program, F.data.startswith(("hse", "resh", "refresh_")))
    
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
