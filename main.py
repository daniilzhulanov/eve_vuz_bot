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

# Словарь программ
PROGRAMS = {
    # ВШЭ программы
    "hse": {
        "name": "📊 Экономика (ВШЭ)",
        "type": "hse",
        "url": "https://enrol.hse.ru/storage/public_report_2025/moscow/Bachelors/BD_moscow_Economy_O.xlsx",
        "priority": 1,
        "places": 10,
        "last_hash": None,
        "last_rank": None,
        "last_other_higher": None,
        "last_consent_higher": None
    },
    "resh": {
        "name": "📘 Совбак НИУ ВШЭ и РЭШ",
        "type": "hse",
        "url": "https://enrol.hse.ru/storage/public_report_2025/moscow/Bachelors/BD_moscow_RESH_O.xlsx",
        "priority": 2,
        "places": 6,
        "last_hash": None,
        "last_rank": None,
        "last_other_higher": None,
        "last_consent_higher": None
    },
    # МГУ программа
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
        [types.KeyboardButton(text="📊 ВШЭ Экономика")],
        [types.KeyboardButton(text="📘 ВШЭ Совбак НИУ ВШЭ и РЭШ")],
        [types.KeyboardButton(text="🏛️ МГУ Экономика")],
        [types.KeyboardButton(text="🔄 Обновить данные")]
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

async def process_hse_data(program_key, user_id=None, is_update=False):
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
        
        # Преобразуем баллы в числа
        df[18] = pd.to_numeric(df[18], errors='coerce')
        df = df[pd.notna(df[18])]

        # Находим все записи абитуриента (для диагностики)
        all_user_records = df[df[1].astype(str).str.strip() == "4272684"]
        logger.debug(f"Все записи пользователя:\n{all_user_records.to_string()}")
        
        # Основная запись (с квотой и нужным приоритетом)
        applicant_data = df[
            (df[1].astype(str).str.strip() == "4272684") & 
            (df[9].astype(str).str.strip().str.upper() == "ДА") &
            (df[11].astype(str).str.strip() == str(target_priority))
        ]
        
        if applicant_data.empty:
            logger.warning(f"Не найдена запись абитуриента (ID: 4272684, приоритет: {target_priority}, квота: ДА)")
            return "❌ Ваши данные не найдены. Проверьте, правильно ли указаны приоритет и квота."
        
        applicant_score = float(applicant_data[18].iloc[0])

        # 1. БВИ с согласием (4="Да", 10="Да", 12="1", 25="Да")
        bvi_consents = len(df[
            (df[3].astype(str).str.strip().str.upper() == "ДА") &    # БВИ
            (df[9].astype(str).str.strip().str.upper() == "ДА") &     # Квота
            (df[11].astype(str).str.strip() == "1") &                 # Приоритет 1
            (df[24].astype(str).str.strip().str.upper() == "ДА")      # Согласие
        ])

        # 2. Не БВИ с согласием и баллом выше (10="Да", 12="1", 25="Да", балл > user_score, 4≠"Да")
        non_bvi_higher = len(df[
            (df[9].astype(str).str.strip().str.upper() == "ДА") &    # Квота
            (df[11].astype(str).str.strip() == "1") &                # Приоритет 1
            (df[24].astype(str).str.strip().str.upper() == "ДА") &   # Согласие
            (df[18] > applicant_score) &                             # Балл выше
            (df[3].astype(str).str.strip().str.upper() != "ДА")      # Не БВИ
        ])

        # 3. Текущее место при подаче согласия
        current_position = bvi_consents + non_bvi_higher + 1

        # 4. Рейтинг в своем приоритете
        priority_applicants = df[
            (df[9].astype(str).str.strip().str.upper() == "ДА") & 
            (df[11].astype(str).str.strip() == str(target_priority))
        ].copy()
        
        priority_applicants = priority_applicants.sort_values(by=18, ascending=False)
        priority_applicants['rank'] = range(1, len(priority_applicants) + 1)
        
        try:
            applicant_rank = priority_applicants[
                priority_applicants[1].astype(str).str.strip() == "4272684"
            ]['rank'].iloc[0]
        except IndexError:
            applicant_rank = "не определен"

        # Формирование сообщения
        if is_update:
            rank_change = format_change(applicant_rank, program.get("last_rank", None))
            bvi_change = format_change(bvi_consents, program.get("last_bvi_consents", None))
            higher_change = format_change(non_bvi_higher, program.get("last_non_bvi_higher", None))
            pos_change = format_change(current_position, program.get("last_position", None))
            
            result_msg = (
                f"🔔 *Обновление данных*\n"
                f"📌 *Направление:* {program['name']}\n\n"
                f"📅 *Дата обновления:* {report_datetime}\n\n"
                f"🎯 Всего мест на программе: *{places}*\n\n"
                f"✅ Твой рейтинг среди {target_priority} приоритета: *{applicant_rank}{rank_change}*\n\n"
                f"👥 Количество согласий у БВИ: *{bvi_consents}{bvi_change}*\n\n"
                f"📊 Количество согласий у людей с баллом выше (не БВИ): *{non_bvi_higher}{higher_change}*\n\n"
                f"🏆 Твое место, если подашь согласие: *{current_position}{pos_change}*"
            )
        else:
            result_msg = (
                f"📌 *Направление:* {program['name']}\n\n"
                f"📅 *Дата обновления:* {report_datetime}\n\n"
                f"🎯 Всего мест на программе: *{places}*\n\n"
                f"✅ Твой рейтинг среди {target_priority} приоритета: *{applicant_rank}*\n\n"
                f"👥 Количество согласий у БВИ: *{bvi_consents}*\n\n"
                f"📊 Количество согласий у людей с баллом выше (не БВИ): *{non_bvi_higher}*\n\n"
                f"🏆 Твое место, если подашь согласие: *{current_position}*"
            )
        
        # Сохраняем текущие значения
        program["last_hash"] = current_hash
        program["last_rank"] = applicant_rank
        program["last_bvi_consents"] = bvi_consents
        program["last_non_bvi_higher"] = non_bvi_higher
        program["last_position"] = current_position
        
        return result_msg
        
    except Exception as e:
        logger.error(f"Ошибка обработки данных: {e}", exc_info=True)
        return f"⚠️ Ошибка обработки данных: {str(e)}"

async def process_data(program_key, user_id=None, is_update=False):
    """Универсальная функция обработки данных"""
    program = PROGRAMS.get(program_key)
    if not program:
        return None
    
    if program["type"] == "hse":
        return await process_hse_data(program_key, user_id, is_update)
    elif program["type"] == "mgu":
        return await process_mgu_data(program_key, user_id, is_update)
    
    return None

async def check_updates(bot: Bot):
    while True:
        try:
            await asyncio.sleep(60)  # Проверка каждую минуту
            
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
        "📊 ВШЭ Экономика": "hse",
        "📘 ВШЭ Совбак НИУ ВШЭ и РЭШ": "resh",
        "🏛️ МГУ Экономика": "mgu"
    }
    
    key = program_mapping.get(message.text)
    if key is None:
        if message.text == "🔄 Обновить данные":
            # Логика для обновления последней просмотренной программы
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
        "📊 ВШЭ Экономика",
        "📘 ВШЭ Совбак НИУ ВШЭ и РЭШ",
        "🏛️ МГУ Экономика",
        "🔄 Обновить данные"
    ]))
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
