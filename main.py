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
from bs4 import BeautifulSoup
from aiogram.exceptions import TelegramRetryAfter

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

def log_user_action(user_id: int, action: str):
    logger.info(f"User {user_id}: {action}")

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

# Настройки СПбГУ
SPBU_SETTINGS = {
    "base_url": "https://enrollelists.spbu.ru",
    "search_url": "https://enrollelists.spbu.ru/view-filters",
    "params": {
        "trajectory": "Поступаю как гражданин РФ",
        "scenario": "Приём поступающих на программы бакалавриата и программы специалитета",
        "group": "38.03.01 Экономика; Экономический факультет; Академический бакалавриат; Бюджетная основа; Отдельная квота; Экономика"
    },
    "target_id": "4272684"
}

# Настройки МГУ
MSU_SETTINGS = {
    "url": "https://cpk.msu.ru/exams/",
    "target_title_part": "Математика ДВИ (четвертый поток) 18 Июля 2025 г.",
    "target_surname": "МИЛАЕВА",
    "check_interval": 300,
    "notification_users": set()
}

# Клавиатуры
def get_main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏛 ВШЭ"), KeyboardButton(text="🏫 МГУ"), KeyboardButton(text="🏰 СПбГУ")]
        ],
        resize_keyboard=True,
        persistent=True
    )

def get_hse_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Экономика"), KeyboardButton(text="📘 Совбак")],
            [KeyboardButton(text="🔙 Назад")]
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

def get_spbu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📈 Экономика СПбГУ")],
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

async def handle_spbu(message: types.Message):
    await message.answer(
        "Выберите программу СПбГУ:",
        reply_markup=get_spbu_keyboard()
    )

async def handle_back(message: types.Message):
    await message.answer(
        "Возвращаемся в главное меню:",
        reply_markup=get_main_keyboard()
    )

# Обработчики ВШЭ
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

        else:
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

# Обработчики СПбГУ
async def parse_spbu_economics(message: types.Message):
    try:
        await message.answer("🔄 Загружаю данные по СПбГУ (Экономика)...")
        
        async with aiohttp.ClientSession() as session:
            # Получаем начальную страницу для токена
            async with session.get(SPBU_SETTINGS['search_url']) as resp:
                if resp.status != 200:
                    await message.answer("❌ Не удалось загрузить страницу СПбГУ")
                    return
                
                html = await resp.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Более надежное извлечение CSRF-токена
                csrf_input = soup.find('input', {'name': '_csrf'})
                if not csrf_input:
                    await message.answer("❌ Не найден CSRF-токен на странице")
                    return
                
                csrf_token = csrf_input.get('value')
                if not csrf_token:
                    await message.answer("❌ Пустой CSRF-токен")
                    return

            # Подготавливаем данные для запроса
            form_data = aiohttp.FormData()
            form_data.add_field('_csrf', csrf_token)
            form_data.add_field('TrajectoryFilter[trajectory]', SPBU_SETTINGS['params']['trajectory'])
            form_data.add_field('ScenarioFilter[scenario]', SPBU_SETTINGS['params']['scenario'])
            form_data.add_field('CompetitiveGroupFilter[group]', SPBU_SETTINGS['params']['group'])
            form_data.add_field('ajax', 'view-filters-form')

            # Отправляем POST-запрос
            async with session.post(
                SPBU_SETTINGS['search_url'],
                data=form_data,
                headers={
                    'X-Requested-With': 'XMLHttpRequest',
                    'Referer': SPBU_SETTINGS['search_url']
                }
            ) as resp:
                if resp.status != 200:
                    await message.answer("❌ Ошибка при запросе данных")
                    return
                
                try:
                    data = await resp.json()
                except:
                    await message.answer("❌ Неверный формат ответа от сервера")
                    return

                if not data.get('success'):
                    await message.answer("❌ Ошибка при формировании списка")
                    return
                
                # Парсим HTML с результатами
                html = data.get('content', '')
                if not html:
                    await message.answer("❌ Нет данных в ответе")
                    return
                
                soup = BeautifulSoup(html, 'html.parser')
                table = soup.find('table', {'class': 'table'})
                
                if not table:
                    await message.answer("❌ Не найдена таблица с результатами")
                    return
                
                # Обрабатываем таблицу
                rows = table.find_all('tr')[1:]  # Пропускаем заголовок
                if not rows:
                    await message.answer("❌ Нет данных в таблице")
                    return
                
                applicants = []
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 6:
                        try:
                            applicant = {
                                'id': cols[0].text.strip(),
                                'priority': int(cols[3].text.strip()),
                                'score': float(cols[4].text.strip()),
                                'original': cols[5].text.strip().lower() == 'да'
                            }
                            applicants.append(applicant)
                        except (ValueError, AttributeError):
                            continue
                
                if not applicants:
                    await message.answer("❌ Не удалось извлечь данные абитуриентов")
                    return
                
                # Фильтруем по 2 приоритету и оригиналам
                priority_2 = [a for a in applicants if a['priority'] == 2 and a['original']]
                if not priority_2:
                    await message.answer("ℹ️ Нет абитуриентов с 2 приоритетом и оригиналами")
                    return
                
                priority_2_sorted = sorted(priority_2, key=lambda x: x['score'], reverse=True)
                
                # Ищем нашего абитуриента
                target_pos = None
                for i, applicant in enumerate(priority_2_sorted, 1):
                    if applicant['id'] == SPBU_SETTINGS['target_id']:
                        target_pos = i
                        target_score = applicant['score']
                        break
                
                if not target_pos:
                    await message.answer("🚫 Ваш номер не найден в списке 2 приоритета")
                    return
                
                # Формируем отчет
                higher = sum(1 for a in priority_2_sorted if a['score'] > target_score)
                total = len(priority_2_sorted)
                
                report = (
                    f"📊 СПбГУ Экономика (2 приоритет)\n\n"
                    f"👤 Ваша позиция: {target_pos} из {total}\n"
                    f"🎯 Ваш балл: {target_score}\n"
                    f"🔝 Абитуриентов с более высокими баллами: {higher}\n"
                    f"📌 Всего оригиналов: {total}"
                )
                
                await message.answer(report)
                
    except Exception as e:
        logger.error(f"SPBU parse error: {str(e)}", exc_info=True)
        await message.answer("❌ Произошла ошибка при обработке данных СПбГУ")

# Обработчики МГУ
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

# Основная функция
async def main():
    try:
        bot = Bot(token=TOKEN)
        dp = Dispatcher()
        
        # Регистрация обработчиков
        dp.message.register(start, F.text == "/start")
        dp.message.register(handle_hse, F.text == "🏛 ВШЭ")
        dp.message.register(handle_msu, F.text == "🏫 МГУ")
        dp.message.register(handle_spbu, F.text == "🏰 СПбГУ")
        dp.message.register(handle_back, F.text == "🔙 Назад")
        dp.message.register(process_hse_program, F.text.in_(["📊 Экономика", "📘 Совбак"]))
        dp.message.register(parse_spbu_economics, F.text == "📈 Экономика СПбГУ")
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
