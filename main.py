import pandas as pd
import requests
from io import BytesIO
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.enums import ParseMode
import asyncio
import os
import logging
from datetime import datetime
import aiohttp
import nest_asyncio
from bs4 import BeautifulSoup
from aiogram.exceptions import TelegramRetryAfter

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

def log_user_action(user_id: int, action: str):
    logger.info(f"User {user_id}: {action}")

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Токен не найден. Установите переменную окружения TOKEN.")

# Словарь программ ВШЭ
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

# Настройки для МГУ
MSU_SETTINGS = {
    "url": "https://cpk.msu.ru/exams/",
    "target_title_part": "Математика ДВИ (четвертый поток) 18 Июля 2025 г.",
    "target_surname": "МИЛАЕВА",
    "check_interval": 3000,
    "notification_users": set()
}

# Клавиатуры
def get_main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏛 ВШЭ"), KeyboardButton(text="🏫 МГУ")]
        ],
        resize_keyboard=True,
        persistent=True
    )

def get_hse_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Экономика"), KeyboardButton(text="📘 Совбак")],
            [KeyboardButton(text="🔄 Обновить"), KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True,
        persistent=True
    )

def get_msu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔍 Проверить сейчас")],
            [KeyboardButton(text="🔔 Подписаться"), KeyboardButton(text="🔕 Отписаться")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True,
        persistent=True
    )

# Обработчики команд
async def start(message: types.Message):
    log_user_action(message.from_user.id, "Started bot")
    await message.answer(
        "Выберите университет:",
        reply_markup=get_main_keyboard()
    )

async def handle_hse(message: types.Message):
    await message.answer(
        "Выберите программу ВШЭ:",
        reply_markup=get_hse_keyboard()
    )

async def handle_msu(message: types.Message):
    await message.answer(
        "Действия со списками МГУ:",
        reply_markup=get_msu_keyboard()
    )

async def handle_back(message: types.Message):
    await message.answer(
        "Возвращаемся в главное меню:",
        reply_markup=get_main_keyboard()
    )

async def process_hse_program(message: types.Message):
    user_id = message.from_user.id
    key = None
    
    if message.text == "📊 Экономика":
        key = "hse"
    elif message.text == "📘 Совбак":
        key = "resh"
    elif message.text == "🔄 Обновить":
        await message.answer("Данные обновлены!")
        return
    
    if not key or key not in HSE_PROGRAMS:
        await message.answer("Неизвестная команда")
        return
        
    program = HSE_PROGRAMS[key]
    
    try:
        log_user_action(user_id, f"Selected program: {program['name']}")
        msg = await message.answer(f"🔄 Загружаю данные: *{program['name']}*", parse_mode=ParseMode.MARKDOWN)

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
        
        # Инициализируем result_msg в начале
        result_msg = f"📅 Дата обновления данных: {report_datetime}\n\n🎯 Мест на программе: {places}\n\n"
        
        if target_priority == 1:
            filtered_1 = df[
                (df[9].astype(str).str.strip().str.upper() == "ДА") & 
                (df[11].astype(str).str.strip() == "1")
            ].copy()

            if filtered_1.empty:
                await message.answer("⚠️ Нет абитуриентов с 1 приоритетом.")
                return

            filtered_1 = filtered_1.sort_values(by=18, ascending=False)
            filtered_1['rank'] = range(1, len(filtered_1) + 1)

            applicant = filtered_1[filtered_1[1].astype(str).str.strip() == "4272684"]  
            if applicant.empty:
                await message.answer("🚫 Номер 4272684 не найден среди 1 приоритета.")
                return

            rank = applicant['rank'].values[0]
            score = applicant[18].values[0]

            result_msg += f"✅ Твой рейтинг среди 1 приоритета: {rank}"

            filtered_2 = df[
                (df[9].astype(str).str.strip().str.upper() == "ДА") & 
                (df[11].astype(str).str.strip() == "2")
            ].copy()

            if not filtered_2.empty:
                higher_2_than_her = filtered_2[filtered_2[18] > score]
                count_higher_2 = len(higher_2_than_her)
                result_msg += f"\n\n🔺 Людей со 2 приоритетом и баллом выше: {count_higher_2}"
            else:
                result_msg += "\n\n🔺 Людей со 2 приоритетом и баллом выше: 0"

        else:  # Обработка для приоритета 2 (Совбак)
            filtered_2 = df[
                (df[9].astype(str).str.strip().str.upper() == "ДА") &  
                (df[11].astype(str).str.strip() == "2")
            ].copy()

            if filtered_2.empty:
                await message.answer("⚠️ Нет абитуриентов со 2 приоритетом.")
                return

            filtered_2 = filtered_2.sort_values(by=18, ascending=False)
            filtered_2['rank_2'] = range(1, len(filtered_2) + 1)

            applicant = filtered_2[filtered_2[1].astype(str).str.strip() == "4272684"]  
            if applicant.empty:
                await message.answer("🚫 Номер 4272684 не найден среди 2 приоритета.")
                return

            rank_2 = applicant['rank_2'].values[0]
            score = applicant[18].values[0]

            result_msg += f"✅ Твой рейтинг среди 2 приоритета: {rank_2}"

            filtered_1 = df[
                (df[9].astype(str).str.strip().str.upper() == "ДА") & 
                (df[11].astype(str).str.strip() == "1")
            ].copy()

            if not filtered_1.empty:
                higher_1_than_her = filtered_1[filtered_1[18] > score]
                count_higher_1 = len(higher_1_than_her)
                result_msg += f"\n\n🔺 Людей с 1 приоритетом и баллом выше: {count_higher_1}"
            else:
                result_msg += "\n\n🔺 Людей с 1 приоритетом и баллом выше: 0"

        await message.answer(result_msg)

    except Exception as e:
        logger.error(f"Error in process_hse_program: {e}")
        await message.answer(f"❌ Ошибка при обработке данных: {str(e)[:200]}")
async def check_msu_lists(message: types.Message):
    user_id = message.from_user.id
    await message.answer("Проверяю списки МГУ...")
    
    try:
        found = await check_msu_page()
        if found:
            await message.answer("🎉 Страница с результатами появилась! Фамилия МИЛАЕВА найдена.")
            MSU_SETTINGS["notification_users"].discard(user_id)
        else:
            await message.answer("ℹ️ Страница с результатами еще не появилась или фамилия не найдена.")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")

async def subscribe_msu_notifications(message: types.Message):
    user_id = message.from_user.id
    MSU_SETTINGS["notification_users"].add(user_id)
    await message.answer("✅ Вы подписались на уведомления о списках МГУ")

async def unsubscribe_msu_notifications(message: types.Message):
    user_id = message.from_user.id
    MSU_SETTINGS["notification_users"].discard(user_id)
    await message.answer("🔕 Вы отписались от уведомлений о списках МГУ")

async def check_msu_page():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(MSU_SETTINGS["url"]) as response:
                content = await response.text()
        
        soup = BeautifulSoup(content, 'html.parser')
        exam_links = soup.find_all('a', href=True)
        
        for link in exam_links:
            if MSU_SETTINGS["target_title_part"] in link.text:
                found_page = link['href']
                if not found_page.startswith('http'):
                    found_page = MSU_SETTINGS["url"] + found_page.lstrip('/')
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(found_page) as response:
                        exam_content = await response.text()
                        if MSU_SETTINGS["target_surname"] in exam_content:
                            return True
        return False
    except Exception as e:
        logger.error(f"Error checking MSU page: {e}")
        return False

async def start_msu_monitoring(bot: Bot):
    while True:
        try:
            found = await check_msu_page()
            if found and MSU_SETTINGS["notification_users"]:
                for user_id in list(MSU_SETTINGS["notification_users"]):
                    try:
                        await bot.send_message(user_id, "🚨 Появились списки МГУ! Фамилия МИЛАЕВА найдена.")
                        MSU_SETTINGS["notification_users"].remove(user_id)
                    except Exception as e:
                        logger.error(f"Error sending notification: {e}")
            await asyncio.sleep(MSU_SETTINGS["check_interval"])
        except Exception as e:
            logger.error(f"Monitoring error: {e}")
            await asyncio.sleep(60)

async def main():
    try:
        bot = Bot(token=TOKEN)
        dp = Dispatcher()
        
        dp.message.register(start, F.text == "/start")
        dp.message.register(handle_hse, F.text == "🏛 ВШЭ")
        dp.message.register(handle_msu, F.text == "🏫 МГУ")
        dp.message.register(handle_back, F.text == "🔙 Назад")
        dp.message.register(process_hse_program, F.text.in_(["📊 Экономика", "📘 Совбак", "🔄 Обновить"]))
        dp.message.register(check_msu_lists, F.text == "🔍 Проверить сейчас")
        dp.message.register(subscribe_msu_notifications, F.text == "🔔 Подписаться")
        dp.message.register(unsubscribe_msu_notifications, F.text == "🔕 Отписаться")
        
        asyncio.create_task(start_msu_monitoring(bot))
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
