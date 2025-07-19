import os
import pandas as pd
import requests
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext

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

# ID пользователя для поиска
USER_ID = 4272684

def download_excel(url: str):
    """Скачивает и читает Excel-файл."""
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_excel(response.content, header=None, engine='openpyxl')

def process_data(df: pd.DataFrame, program_code: str):
    """Обрабатывает данные и возвращает статистику."""
    try:
        # Извлечение даты обновления
        report_datetime = df.iloc[4, 5]
        if pd.isna(report_datetime):
            report_datetime_str = "не указана"
        elif pd.api.types.is_datetime64_any_dtype(report_datetime):
            report_datetime_str = report_datetime.strftime("%d.%m.%Y %H:%M:%S")
        else:
            report_datetime_str = str(report_datetime)
    except:
        report_datetime_str = "ошибка получения"

    # Фильтрация данных
    consent_col = 9  # Столбец с согласием (индекс 9)
    priority_col = 11  # Столбец с приоритетом (индекс 11)
    score_col = 18  # Столбец с баллами (индекс 18)
    id_col = 1  # Столбец с ID (индекс 1)

    # Основные фильтры
    has_consent = (df[consent_col] == "Да")
    current_priority = PROGRAMS[program_code]["priority"]
    
    # Данные для текущего приоритета
    priority_df = df[has_consent & (df[priority_col] == current_priority)].copy()
    priority_df.sort_values(by=score_col, ascending=False, inplace=True)
    priority_df['rank'] = range(1, len(priority_df) + 1
    
    # Поиск пользователя
    user_row = priority_df[priority_df[id_col] == USER_ID]
    user_rank = user_row['rank'].values[0] if not user_row.empty else None
    user_score = user_row[score_col].values[0] if not user_row.empty else None

    # Конкуренты с другим приоритетом
    other_priority = 2 if current_priority == 1 else 1
    competitors = df[
        has_consent & 
        (df[priority_col] == other_priority) & 
        (df[score_col] > user_score)
    ]
    
    return {
        "date": report_datetime_str,
        "places": PROGRAMS[program_code]["places"],
        "user_rank": user_rank,
        "competitors_count": len(competitors)
    }

def get_program_info(program_code: str):
    """Получает информацию о программе."""
    try:
        df = download_excel(PROGRAMS[program_code]["url"])
        data = process_data(df, program_code)
        
        if data["user_rank"] is None:
            return "❌ Ваши данные не найдены в списке"
            
        return (
            f"📅 Дата обновления данных: {data['date']}\n\n"
            f"🎯 Мест на программе: {data['places']}\n\n"
            f"✅ Твой рейтинг среди {PROGRAMS[program_code]['priority']} приоритета: {data['user_rank']}\n\n"
            f"🔺 Людей с {3 - PROGRAMS[program_code]['priority']} приоритетом и баллом выше: {data['competitors_count']}"
        )
    except Exception as e:
        return f"⚠️ Ошибка при обработке данных: {str(e)}"

# Обработчики Telegram
def start(update: Update, context: CallbackContext) -> None:
    buttons = [
        [KeyboardButton(PROGRAMS["hse"]["name"])],
        [KeyboardButton(PROGRAMS["resh"]["name"])]
    ]
    reply_markup = ReplyKeyboardMarkup(buttons, resize_keyboard=True)
    update.message.reply_text("Выберите программу:", reply_markup=reply_markup)

def handle_message(update: Update, context: CallbackContext) -> None:
    text = update.message.text
    if text == PROGRAMS["hse"]["name"]:
        message = get_program_info("hse")
    elif text == PROGRAMS["resh"]["name"]:
        message = get_program_info("resh")
    else:
        message = "Используйте кнопки для выбора программы"
    update.message.reply_text(message)

def main() -> None:
    TOKEN = os.environ.get("TOKEN")
    if not TOKEN:
        raise ValueError("Токен не найден. Установите переменную окружения TOKEN.")
    
    updater = Updater(TOKEN)
    dispatcher = updater.dispatcher

    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
