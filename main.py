import pandas as pd
from io import BytesIO
from aiogram import Bot, Dispatcher, types, F
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
from bs4 import BeautifulSoup
import re

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

# Словарь программ (только МГУ)
PROGRAMS = {
    "mgu": {
        "name": "🏛️ Экономика (МГУ)",
        "type": "mgu",
        "url": "https://cpk.msu.ru/rating/dep_14#14_02_1_04_1",  # URL для квоты 1
        "url_quota2": "https://cpk.msu.ru/rating/dep_14#14_02_1_04_2",  # URL для квоты 2
        "places": 17,
        "user_id": "129025",
        "last_hash": None,
        "last_rank": None,
        "last_bvi_consents": None,
        "last_higher_consents": None,
        "last_update": None
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

def get_reply_keyboard():
    """Создает Reply-клавиатуру с кнопками выбора программ"""
    keyboard = [
        [types.KeyboardButton(text="🏛️ МГУ Экономика")]
    ]
    return types.ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        is_persistent=True
    )

async def download_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.read()

async def parse_mgu_page(url):
    """Парсинг HTML страницы МГУ"""
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.text()

def extract_date_from_mgu_html(html_content):
    """Извлечение даты обновления из HTML МГУ"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Ищем параграф с датой
    date_paragraph = soup.find('p', string=lambda text: text and 'Состояние на:' in text)
    if date_paragraph:
        date_text = date_paragraph.get_text()
        # Извлекаем дату из текста
        match = re.search(r'Состояние на: (.+)', date_text)
        if match:
            return match.group(1).strip()
    
    return "не указана"

def parse_mgu_table(html_content, table_id):
    """Парсинг таблицы МГУ по ID"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Находим заголовок с нужным ID
    header = soup.find(id=table_id)
    if not header:
        return None, None
    
    # Находим следующую таблицу после заголовка
    table = header.find_next('table')
    if not table:
        return None, None
    
    # Извлекаем дату из предыдущего параграфа
    date_p = header.find_next('p')
    date = extract_date_from_mgu_html(str(date_p)) if date_p else "не указана"
    
    # Парсим таблицу
    rows = table.find_all('tr')[1:]  # Пропускаем заголовок
    data = []
    
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 8:  # Минимум 8 столбцов для квоты 1
            row_data = []
            for cell in cells:
                row_data.append(cell.get_text().strip())
            data.append(row_data)
    
    return data, date

async def process_mgu_data(program_key, user_id=None, is_update=False):
    """Обработка данных МГУ"""
    program = PROGRAMS[program_key]
    
    try:
        # Получаем данные с обеих страниц
        html1 = await parse_mgu_page(program["url"])
        html2 = await parse_mgu_page(program["url_quota2"])
        
        # Создаем хеш для проверки изменений
        current_hash = hashlib.md5((html1 + html2).encode()).hexdigest()
        
        if is_update and program["last_hash"] == current_hash:
            return None
        
        # Парсим обе таблицы
        quota1_data, date1 = parse_mgu_table(html1, "14_02_1_04_1")
        quota2_data, date2 = parse_mgu_table(html2, "14_02_1_04_2")
        
        if not quota1_data or not quota2_data:
            return None
        
        # Используем дату из первой таблицы
        report_datetime = date1
        
        # Подсчет БВИ согласий (квота 1)
        bvi_consents = 0
        for row in quota1_data:
            if len(row) >= 4:
                consent = row[2].strip()  # 3-й столбец (согласие)
                priority = row[3].strip()  # 4-й столбец (приоритет)
                
                if consent.upper() == "ДА" and priority == "1":
                    bvi_consents += 1
        
        # Находим пользователя в квоте 2 и его балл
        user_score = None
        for row in quota2_data:
            if len(row) >= 8:
                user_id_col = row[1].strip()  # 2-й столбец (ID)
                if user_id_col == program["user_id"]:
                    user_score = int(row[7].strip())  # 8-й столбец (сумма баллов)
                    break
        
        if user_score is None:
            return None
        
        # Подсчет людей с баллом выше и согласием (квота 2)
        higher_consents = 0
        for row in quota2_data:
            if len(row) >= 8:
                consent = row[2].strip()  # 3-й столбец (согласие)
                priority = row[3].strip()  # 4-й столбец (приоритет)
                score = int(row[7].strip()) if row[7].strip().isdigit() else 0  # 8-й столбец
                
                if (consent.upper() == "ДА" and 
                    priority == "1" and 
                    score > user_score):
                    higher_consents += 1
        
        # Расчет текущего места
        current_position = bvi_consents + higher_consents + 1
        
        # Формируем сообщение с изменениями
        if is_update:
            bvi_change = format_change(bvi_consents, program["last_bvi_consents"])
            higher_change = format_change(higher_consents, program["last_higher_consents"])
            position_change = format_change(current_position, program["last_rank"])
            
            result_msg = (
                f"🔔 *Обновление данных*\n"
                f"📌 *Направление:* {program['name']}\n\n"
                f"📅 *Дата обновления:* {report_datetime}\n\n"
                f"🎯 Мест на программе: *{program['places']}*\n\n"
                f"👥 Количество согласий у БВИ: *{bvi_consents}{bvi_change}*\n\n"
                f"📊 Количество согласий у людей с баллом выше: *{higher_consents}{higher_change}*\n\n"
                f"🏆 Твое текущее место: *{current_position}{position_change}*"
            )
        else:
            result_msg = (
                f"📌 *Направление:* {program['name']}\n\n"
                f"📅 *Дата обновления:* {report_datetime}\n\n"
                f"🎯 Мест на программе: *{program['places']}*\n\n"
                f"👥 Количество согласий у БВИ: *{bvi_consents}*\n\n"
                f"📊 Количество согласий у людей с баллом выше: *{higher_consents}*\n\n"
                f"🏆 Твое текущее место: *{current_position}*"
            )
        
        # Сохраняем текущие значения
        program["last_hash"] = current_hash
        program["last_rank"] = current_position
        program["last_bvi_consents"] = bvi_consents
        program["last_higher_consents"] = higher_consents
        program["last_update"] = datetime.now()
        
        return result_msg
        
    except Exception as e:
        logger.error(f"Ошибка обработки данных МГУ: {e}")
        return None

async def process_data(program_key, user_id=None, is_update=False):
    """Универсальная функция обработки данных"""
    program = PROGRAMS.get(program_key)
    if not program:
        return None
    
    if program["type"] == "mgu":
        return await process_mgu_data(program_key, user_id, is_update)
    
    return None

async def check_updates(bot: Bot):
    while True:
        try:
            await asyncio.sleep(300)  # Проверка каждую минуту
            
            for program_key in PROGRAMS:
                update_msg = await process_data(program_key, is_update=True)
                if update_msg:
                    for user_id in list(active_users):  # Используем копию списка на случай изменений
                        try:
                            await bot.send_message(
                                user_id,
                                update_msg,
                                parse_mode=ParseMode.MARKDOWN
                            )
                        except Exception as e:
                            logger.error(f"Ошибка отправки пользователю {user_id}: {e}")
                            # Удаляем неактивного пользователя
                            active_users.discard(user_id)
        except Exception as e:
            logger.error(f"Ошибка в check_updates: {e}")

async def start(message: types.Message):
    user_id = message.from_user.id
    active_users.add(user_id)
    log_user_action(user_id, "Started bot")
    await message.answer(
        "Выберите программу для анализа рейтинга:",
        reply_markup=get_reply_keyboard()
    )

async def handle_program_selection(message: types.Message):
    user_id = message.from_user.id
    active_users.add(user_id)
    
    program_mapping = {
        "🏛️ МГУ Экономика": "mgu"
    }
    
    key = program_mapping.get(message.text)
    if key is None:
        if message.text == "🔄 Обновить данные":
            await message.answer("Пожалуйста, выберите программу для обновления")
            return
        await message.answer("Неизвестная команда")
        return
    
    program = PROGRAMS.get(key)
    if not program:
        await message.answer("Программа не найдена")
        return
    
    log_user_action(user_id, f"Selected program: {program['name']}")
    
    try:
        # Отправляем сообщение о загрузке
        loading_msg = await message.answer("⏳ Загружаю данные...")
        
        status_msg = await process_data(key, user_id)
        
        # Удаляем сообщение о загрузке
        await loading_msg.delete()
        
        if status_msg:
            await message.answer(
                status_msg,
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.answer("❌ Не удалось получить данные")
            
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await message.answer("⚠️ Произошла ошибка")

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
    dp.message.register(handle_program_selection, F.text.in_([
        "🏛️ МГУ Экономика",
        "🔄 Обновить данные"
    ]))
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
