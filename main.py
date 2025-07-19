import logging
from telegram import ReplyKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import pandas as pd
import io
import requests
from datetime import datetime

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация программ
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

USER_ID = 4272684  # Замените на ваш ID

def get_excel_data(url):
    """Загрузка и парсинг Excel файла"""
    response = requests.get(url)
    return pd.read_excel(io.BytesIO(response.content)), datetime.now()

def analyze_program_data(df, program_key):
    """Анализ данных для конкретной программы"""
    program = PROGRAMS[program_key]
    
    # Фильтрация данных
    df = df[df.iloc[:, 10] == "Да"]  # Столбец 10
    df = df[df.iloc[:, 12] == program["priority"]]  # Столбец 12
    
    # Находим пользователя
    user_row = df[df.iloc[:, 2] == USER_ID]  # Столбец 2
    
    if user_row.empty:
        return None
    
    user_score = user_row.iloc[0, 19]  # Столбец 19
    
    # Рейтинг среди приоритета
    priority_df = df[df.iloc[:, 12] == program["priority"]].copy()
    priority_df['rank'] = priority_df.iloc[:, 19].rank(ascending=False, method='min')
    user_priority_rank = int(priority_df[priority_df.iloc[:, 2] == USER_ID]['rank'].iloc[0])
    
    # Людей с другим приоритетом и баллом выше
    other_priority = 2 if program["priority"] == 1 else 1
    higher_priority_above = len(df[(df.iloc[:, 12] == other_priority) & (df.iloc[:, 19] > user_score)])
    
    return {
        "user_priority_rank": user_priority_rank,
        "higher_priority_above": higher_priority_above,
        "is_accepted": True
    }

def start(update, context):
    """Обработка команды /start"""
    update.message.reply_text(
        "Выберите программу:",
        reply_markup=ReplyKeyboardMarkup([
            [PROGRAMS["hse"]["name"], PROGRAMS["resh"]["name"]]
        ], resize_keyboard=True, one_time_keyboard=True)
    )

def handle_program_selection(update, context):
    """Обработка выбора программы"""
    program_name = update.message.text
    program_key = None
    
    for key, data in PROGRAMS.items():
        if data["name"] == program_name:
            program_key = key
            break
    
    if not program_key:
        update.message.reply_text("Программа не найдена")
        return
    
    try:
        df, update_time = get_excel_data(PROGRAMS[program_key]["url"])
        analysis = analyze_program_data(df, program_key)
        
        if not analysis:
            update.message.reply_text("Ваши данные не найдены в списках")
            return
        
        message = (
            f"📅 Дата обновления данных: {update_time.strftime('%d.%m.%Y %H:%M:%S')}\n\n"
            f"🎯 Мест на программе: {PROGRAMS[program_key]['places']}\n\n"
            f"✅ Твой рейтинг среди {PROGRAMS[program_key]['priority']} приоритета: {analysis['user_priority_rank']}\n\n"
            f"🔺 Людей с {2 if PROGRAMS[program_key]['priority'] == 1 else 1} приоритетом и баллом выше: {analysis['higher_priority_above']}"
        )
        
        update.message.reply_text(message)
        
    except Exception as e:
        logger.error(f"Error processing program data: {e}")
        update.message.reply_text("Произошла ошибка при обработке данных. Попробуйте позже.")

def error(update, context):
    logger.warning(f'Update "{update}" caused error "{context.error}"')

def main():

    TOKEN = os.environ.get("TOKEN")
    if not TOKEN:
        raise ValueError("Токен не найден. Установите переменную окружения TOKEN.")
        
    updater = Updater("TOKEN", use_context=True)
    dp = updater.dispatcher
    
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(MessageHandler(Filters.text & (~Filters.command), handle_program_selection))
    dp.add_error_handler(error)
    
    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()
