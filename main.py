import pandas as pd
import requests
from io import BytesIO
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
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

# Функция для логирования действий пользователей
def log_user_action(user_id: int, action: str):
    """Логирует действия пользователя"""
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

# Закрепленные клавиатуры
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

async def process_hse_program(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    # Обработка кнопки обновления
    if callback.data.startswith("refresh_"):
        key = callback.data.split("_")[1]
        await callback.answer("Обновляю данные...")
    else:
        key = callback.data
    
    if key not in HSE_PROGRAMS:
        await callback.answer("Неизвестная программа")
        return
        
    program = HSE_PROGRAMS[key]
    
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
            await callback.message.answer(f"❌ {error_msg}", reply_markup=get_hse_program_keyboard(include_refresh=True, current_program=key))
            return

        if df.shape[1] < 19:
            error_msg = "Файл содержит недостаточно столбцов."
            log_user_action(user_id, error_msg)
            await callback.message.answer(f"❌ {error_msg}", reply_markup=get_hse_program_keyboard(include_refresh=True, current_program=key))
            return

        try:
            report_datetime = df.iloc[4, 5] if pd.notna(df.iloc[4, 5]) else "не указана"
            
            if pd.api.types.is_datetime64_any_dtype(df.iloc[4, 5]):
                report_datetime = report_datetime.strftime("%d.%m.%Y %H:%M")
            
            target_priority = program["priority"]
            places = program["places"]
            
            if target_priority == 1:
                filtered_1 = df[
                    (df[9].astype(str).str.strip().str.upper() == "ДА") & 
                    (df[11].astype(str).str.strip() == "1")
                ].copy()

                if filtered_1.empty:
                    log_user_action(user_id, "No applicants with priority 1 found")
                    await callback.message.answer("⚠️ Нет абитуриентов с 1 приоритетом.", 
                                               reply_markup=get_hse_program_keyboard(include_refresh=True, current_program=key))
                    return

                filtered_1 = filtered_1.sort_values(by=18, ascending=False)
                filtered_1['rank'] = range(1, len(filtered_1) + 1)

                applicant = filtered_1[filtered_1[1].astype(str).str.strip() == "4272684"]  
                if applicant.empty:
                    log_user_action(user_id, "Applicant 4272684 not found in priority 1")
                    await callback.message.answer("🚫 Номер 4272684 не найден среди 1 приоритета.", 
                                               reply_markup=get_hse_program_keyboard(include_refresh=True, current_program=key))
                    return

                rank = applicant['rank'].values[0]
                score = applicant[18].values[0]

                result_msg = (
                    f"📅 *Дата обновления данных:* {report_datetime}\n\n"
                    f"🎯 Мест на программе: *{places}*\n\n"
                    f"✅ Твой рейтинг среди 1 приоритета: *{rank}*"
                )

                filtered_2 = df[
                    (df[9].astype(str).str.strip().str.upper() == "ДА") & 
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
                    (df[9].astype(str).str.strip().str.upper() == "ДА") &  
                    (df[11].astype(str).str.strip() == "2")
                ].copy()

                if filtered_2.empty:
                    log_user_action(user_id, "No applicants with priority 2 found")
                    await callback.message.answer("⚠️ Нет абитуриентов со 2 приоритетом.", 
                                               reply_markup=get_hse_program_keyboard(include_refresh=True, current_program=key))
                    return

                filtered_2 = filtered_2.sort_values(by=18, ascending=False)
                filtered_2['rank_2'] = range(1, len(filtered_2) + 1)

                applicant = filtered_2[filtered_2[1].astype(str).str.strip() == "4272684"]  
                if applicant.empty:
                    log_user_action(user_id, "Applicant 4272684 not found in priority 2")
                    await callback.message.answer("🚫 Номер 4272684 не найден среди 2 приоритета.", 
                                               reply_markup=get_hse_program_keyboard(include_refresh=True, current_program=key))
                    return

                rank_2 = applicant['rank_2'].values[0]
                score = applicant[18].values[0]

                result_msg = (
                    f"📅 *Дата обновления данных:* {report_datetime}\n\n"
                    f"🎯 Мест на программе: *{places}*\n\n"
                    f"✅ Твой рейтинг среди 2 приоритета: *{rank_2}*"
                )

                filtered_1 = df[
                    (df[9].astype(str).str.strip().str.upper() == "ДА") & 
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
                                         reply_markup=get_hse_program_keyboard(include_refresh=True, current_program=key))

        except Exception as e:
            error_msg = f"Ошибка обработки данных: {str(e)[:200]}"
            log_user_action(user_id, error_msg)
            await callback.message.answer(f"❌ {error_msg}", 
                                        reply_markup=get_hse_program_keyboard(include_refresh=True, current_program=key))

    except Exception as e:
        logger.exception("Unexpected error in process_program")
        await callback.message.answer("⚠️ Произошла непредвиденная ошибка", 
                                    reply_markup=get_hse_program_keyboard(include_refresh=True, current_program=key))

async def check_msu_lists(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await callback.answer("Проверяю списки МГУ...")
    
    try:
        log_user_action(user_id, "Checking MSU lists")
        
        # Проверяем текущий статус
        found = await check_msu_page()
        
        if found:
            result_msg = "🎉 Страница с результатами появилась! Фамилия МИЛАЕВА найдена."
            # Удаляем пользователя из списка, так как уже нашли
            MSU_SETTINGS["notification_users"].discard(user_id)
        else:
            result_msg = (
                "ℹ️ Страница с результатами еще не появилась или фамилия не найдена.\n\n"
                "Вы можете подписаться на уведомления ниже ⬇️"
            )
        
        await callback.message.answer(result_msg, reply_markup=get_msu_keyboard())
        
    except Exception as e:
        error_msg = f"Ошибка при проверке списков МГУ: {str(e)}"
        log_user_action(user_id, error_msg)
        await callback.message.answer(error_msg, reply_markup=get_msu_keyboard())

async def subscribe_msu_notifications(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    MSU_SETTINGS["notification_users"].add(user_id)
    await callback.answer("✅ Вы подписались на уведомления о списках МГУ")
    await callback.message.answer(
        "Вы будете получать уведомления, как только списки появятся.",
        reply_markup=get_msu_keyboard()
    )

async def unsubscribe_msu_notifications(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    MSU_SETTINGS["notification_users"].discard(user_id)
    await callback.answer("🔕 Вы отписались от уведомлений о списках МГУ")
    await callback.message.answer(
        "Вы больше не будете получать уведомления о списках МГУ.",
        reply_markup=get_msu_keyboard()
    )

async def check_msu_page():
    url = MSU_SETTINGS["url"]
    target_title_part = MSU_SETTINGS["target_title_part"]
    target_surname = MSU_SETTINGS["target_surname"]
    
    try:
        # Получаем главную страницу экзаменов
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                content = await response.text()
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Ищем все ссылки на страницы экзаменов
        exam_links = soup.find_all('a', href=True)
        
        found_page = None
        
        # Проверяем каждую ссылку на соответствие названию
        for link in exam_links:
            if target_title_part in link.text:
                found_page = link['href']
                break
                
        if found_page:
            logger.info("Найдена страница экзамена МГУ!")
            
            # Если ссылка относительная, делаем ее абсолютной
            if not found_page.startswith('http'):
                found_page = url + found_page.lstrip('/')
                
            # Переходим на страницу экзамена
            async with aiohttp.ClientSession() as session:
                async with session.get(found_page) as response:
                    response.raise_for_status()
                    exam_content = await response.text()
            
            # Проверяем наличие фамилии
            if target_surname in exam_content:
                logger.info(f"Фамилия {target_surname} найдена на странице!")
                return True
            else:
                logger.info(f"Фамилия {target_surname} не найдена.")
                return False
        else:
            logger.info("Страница экзамена МГУ еще не появилась.")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при проверке МГУ: {e}")
        return False

async def start_msu_monitoring(bot: Bot):
    while True:
        try:
            # Проверяем страницу МГУ
            found = await check_msu_page()
            
            if found and MSU_SETTINGS["notification_users"]:
                # Если страница найдена и есть подписчики
                notification_msg = "🚨 Появились списки МГУ! Фамилия МИЛАЕВА найдена."
                
                # Отправляем уведомления всем подписчикам
                for user_id in list(MSU_SETTINGS["notification_users"]):
                    try:
                        await bot.send_message(user_id, notification_msg)
                        # Удаляем пользователя из списка после уведомления
                        MSU_SETTINGS["notification_users"].remove(user_id)
                    except TelegramRetryAfter as e:
                        # Если превышен лимит сообщений, ждем
                        await asyncio.sleep(e.retry_after)
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления пользователю {user_id}: {e}")
            
            # Ждем перед следующей проверкой
            await asyncio.sleep(MSU_SETTINGS["check_interval"])
            
        except Exception as e:
            logger.error(f"Ошибка в мониторинге МГУ: {e}")
            await asyncio.sleep(60)  # ждем минуту при ошибке

# ... (предыдущий код остается без изменений до функции main)

async def back_to_main_menu(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "Возвращаемся в главное меню:",
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

async def show_hse_menu(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "Выберите программу ВШЭ:",
        reply_markup=get_hse_keyboard()
    )
    await callback.answer()

async def show_msu_menu(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "Действия со списками МГУ:",
        reply_markup=get_msu_keyboard()
    )
    await callback.answer()

async def main():
    try:
        logger.info("Starting bot...")
        bot = Bot(token=TOKEN)
        dp = Dispatcher()
        
        # Регистрация обработчиков сообщений
        dp.message.register(start, F.text == "/start")
        dp.message.register(handle_hse, F.text == "🏛 ВШЭ")
        dp.message.register(handle_msu, F.text == "🏫 МГУ")
        dp.message.register(handle_back, F.text == "🔙 Назад")

        # Обработчики для кнопок ВШЭ
        dp.message.register(process_hse_program, F.text.in_(["📊 Экономика", "📘 Совбак", "🔄 Обновить"]))
        
        # Обработчики для кнопок МГУ
        dp.message.register(check_msu_lists, F.text == "🔍 Проверить сейчас")
        dp.message.register(subscribe_msu_notifications, F.text == "🔔 Подписаться")
        dp.message.register(unsubscribe_msu_notifications, F.text == "🔕 Отписаться")

    
        asyncio.create_task(start_msu_monitoring(bot))
        
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
