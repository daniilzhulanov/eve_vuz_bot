import os
import logging
from datetime import datetime
import requests
import pandas as pd
from io import BytesIO
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Получение токена из переменной окружения
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Токен не найден. Установите переменную окружения TOKEN.")

# ID пользователя для поиска
USER_ID = "4272684"

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

def download_excel_file(url):
    """Загружает Excel файл по URL"""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return BytesIO(response.content)
    except requests.RequestException as e:
        logger.error(f"Ошибка при загрузке файла {url}: {e}")
        return None

def analyze_program_data(program_key):
    """Анализирует данные программы и возвращает статистику"""
    program = PROGRAMS[program_key]
    
    # Загружаем файл
    excel_file = download_excel_file(program["url"])
    if not excel_file:
        return None
    
    try:
        # Читаем Excel файл
        df = pd.read_excel(excel_file, engine='openpyxl')
        
        # Находим пользователя
        # Столбец B (индекс 1) - номер человека
        # Столбец J (индекс 9) - статус "Да"
        # Столбец L (индекс 11) - приоритет
        # Столбец S (индекс 18) - балл
        
        user_row = df[df.iloc[:, 1].astype(str) == USER_ID]
        
        if user_row.empty:
            return {
                "error": f"Пользователь {USER_ID} не найден в списках программы"
            }
        
        user_data = user_row.iloc[0]
        
        # Проверяем, что в 10 столбце "Да"
        if str(user_data.iloc[9]).strip().lower() != "да":
            return {
                "error": "Пользователь не соответствует критериям (10 столбец не содержит 'Да')"
            }
        
        user_priority = int(user_data.iloc[11])  # приоритет пользователя
        user_score = float(user_data.iloc[18])   # балл пользователя
        
        # Фильтруем данные: только те, у кого в 10 столбце "Да"
        valid_candidates = df[df.iloc[:, 9].astype(str).str.strip().str.lower() == "да"].copy()
        
        # Считаем рейтинг среди своего приоритета
        same_priority = valid_candidates[valid_candidates.iloc[:, 11] == user_priority]
        same_priority_better = same_priority[same_priority.iloc[:, 18] > user_score]
        rank_in_priority = len(same_priority_better) + 1
        
        # Считаем людей с другим приоритетом и баллом выше
        if user_priority == 1:
            other_priority = 2
        else:
            other_priority = 1
            
        other_priority_candidates = valid_candidates[valid_candidates.iloc[:, 11] == other_priority]
        other_priority_better = other_priority_candidates[other_priority_candidates.iloc[:, 18] > user_score]
        other_priority_count = len(other_priority_better)
        
        # Получаем текущую дату и время
        update_time = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        
        return {
            "update_time": update_time,
            "places": program["places"],
            "user_priority": user_priority,
            "rank_in_priority": rank_in_priority,
            "other_priority_count": other_priority_count,
            "other_priority": other_priority
        }
        
    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {e}")
        return {"error": f"Ошибка при обработке данных: {str(e)}"}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    keyboard = [
        [InlineKeyboardButton("📊 Экономика", callback_data='hse')],
        [InlineKeyboardButton("📘 Совбак НИУ ВШЭ и РЭШ", callback_data='resh')]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        'Выберите программу для получения информации о поступлении:',
        reply_markup=reply_markup
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на кнопки"""
    query = update.callback_query
    await query.answer()
    
    program_key = query.data
    
    if program_key not in PROGRAMS:
        await query.edit_message_text("Неизвестная программа")
        return
    
    program = PROGRAMS[program_key]
    
    # Показываем индикатор загрузки
    await query.edit_message_text("🔄 Загружаю данные...")
    
    # Анализируем данные
    result = analyze_program_data(program_key)
    
    if not result:
        await query.edit_message_text("❌ Ошибка при загрузке данных. Попробуйте позже.")
        return
    
    if "error" in result:
        await query.edit_message_text(f"❌ {result['error']}")
        return
    
    # Формируем сообщение с результатами
    message = f"""📊 {program['name']}

📅 Дата обновления данных: {result['update_time']}
🎯 Мест на программе: {result['places']}
✅ Твой рейтинг среди {result['user_priority']} приоритета: {result['rank_in_priority']}
🔺 Людей с {result['other_priority']} приоритетом и баллом выше: {result['other_priority_count']}"""
    
    # Добавляем кнопки для повторного выбора
    keyboard = [
        [InlineKeyboardButton("📊 Экономика", callback_data='hse')],
        [InlineKeyboardButton("📘 Совбак НИУ ВШЭ и РЭШ", callback_data='resh')]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message, reply_markup=reply_markup)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    logger.error(f"Ошибка: {context.error}")

def main():
    """Главная функция запуска бота"""
    # Создаем приложение
    application = Application.builder().token(TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_error_handler(error_handler)
    
    # Запускаем бота
    logger.info("Бот запущен")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
