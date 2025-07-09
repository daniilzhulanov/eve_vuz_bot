import pandas as pd
import requests
from io import BytesIO
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
import asyncio
import os

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise ValueError("Токен не найден. Установите переменную окружения TOKEN.")

bot = Bot(token=TOKEN)
dp = Dispatcher()

# Словарь программ
PROGRAMS = {
    "hse": {
        "name": "📊 Экономика",
        "url": "https://enrol.hse.ru/storage/public_report_2025/moscow/Bachelors/BD_moscow_Economy_O.xlsx",
        "priority": 1,  # У неё 1 приоритет на экономике
        "places": 10    # Количество мест
    },
    "resh": {
        "name": "📘 Совбак НИУ ВШЭ и РЭШ",
        "url": "https://enrol.hse.ru/storage/public_report_2025/moscow/Bachelors/BD_moscow_RESH_O.xlsx",
        "priority": 2,  # У неё 2 приоритет на совбаке
        "places": 6     # Количество мест
    }
}

# Клавиатура выбора программы
keyboard = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text=PROGRAMS["hse"]["name"], callback_data="hse")],
    [InlineKeyboardButton(text=PROGRAMS["resh"]["name"], callback_data="resh")]
])

@dp.message(F.text == "/start")
async def start(message: types.Message):
    await message.answer("Выбери программу для анализа рейтинга:", reply_markup=keyboard)

@dp.callback_query(F.data.in_(PROGRAMS.keys()))
async def process_program(callback: types.CallbackQuery):
    key = callback.data
    program = PROGRAMS[key]
    await callback.answer()
    await callback.message.answer(f"🔄 Загружаю данные: *{program['name']}*", parse_mode=ParseMode.MARKDOWN)

    try:
        response = requests.get(program['url'], timeout=10)
        response.raise_for_status()
        df = pd.read_excel(BytesIO(response.content), header=None)
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка загрузки: {e}")
        return

    if df.shape[1] < 19:
        await callback.message.answer("❌ Файл содержит недостаточно столбцов.")
        return

    try:
        # Определяем приоритет и количество мест для этой программы
        target_priority = program["priority"]
        places = program["places"]
        
        if target_priority == 1:
            # ===== ЭКОНОМИКА (1 приоритет) =====
            # Фильтруем людей с 1 приоритетом
            filtered_1 = df[
                (df[7].astype(str).str.strip().str.upper() == "ДА") &
                (df[11].astype(str).str.strip() == "1")
            ].copy()

            if filtered_1.empty:
                await callback.message.answer("⚠️ Нет абитуриентов с 1 приоритетом.")
                return

            # Сортируем по баллам
            filtered_1 = filtered_1.sort_values(by=18, ascending=False)
            filtered_1['rank'] = range(1, len(filtered_1) + 1)

            # Ищем абитуриента
            applicant = filtered_1[filtered_1[1].astype(str).str.strip() == "4272684"]
            if applicant.empty:
                await callback.message.answer("🚫 Номер 4272684 не найден среди 1 приоритета.")
                return

            rank = applicant['rank'].values[0]
            score = applicant[18].values[0]

            result_msg = f"🎯 Мест на программе: *{places}*\n\n✅ Твой рейтинг среди 1 приоритета: *{rank}*"

            # Дополнительно: количество людей со 2 приоритетом с баллом выше
            filtered_2 = df[
                (df[7].astype(str).str.strip().str.upper() == "ДА") &
                (df[11].astype(str).str.strip() == "2")
            ].copy()

            if not filtered_2.empty:
                higher_2_than_her = filtered_2[filtered_2[18] > score]
                count_higher_2 = len(higher_2_than_her) + 1
                result_msg += f"\n\n🔺 Людей со 2 приоритетом и баллом выше: *{count_higher_2}*"
            else:
                result_msg += "\n\n🔺 Людей со 2 приоритетом и баллом выше: *0*"

        else:
            # ===== СОВБАК (2 приоритет) =====
            # Фильтруем людей со 2 приоритетом
            filtered_2 = df[
                (df[7].astype(str).str.strip().str.upper() == "ДА") &
                (df[11].astype(str).str.strip() == "2")
            ].copy()

            if filtered_2.empty:
                await callback.message.answer("⚠️ Нет абитуриентов со 2 приоритетом.")
                return

            # Сортируем по баллам
            filtered_2 = filtered_2.sort_values(by=18, ascending=False)
            filtered_2['rank_2'] = range(1, len(filtered_2) + 1)

            # Ищем абитуриента
            applicant = filtered_2[filtered_2[1].astype(str).str.strip() == "4272684"]
            if applicant.empty:
                await callback.message.answer("🚫 Номер 4272684 не найден среди 2 приоритета.")
                return

            rank_2 = applicant['rank_2'].values[0]
            score = applicant[18].values[0]

            result_msg = f"🎯 Мест на программе: *{places}*\n\n✅ Твой рейтинг среди 2 приоритета: *{rank_2}*"

            # Дополнительно: количество людей с 1 приоритетом с баллом выше
            filtered_1 = df[
                (df[7].astype(str).str.strip().str.upper() == "ДА") &
                (df[11].astype(str).str.strip() == "1")
            ].copy()

            if not filtered_1.empty:
                higher_1_than_her = filtered_1[filtered_1[18] > score]
                count_higher_1 = len(higher_1_than_her) + 1
                result_msg += f"\n\n🔺 Людей с 1 приоритетом и баллом выше: *{count_higher_1}*"
            else:
                result_msg += "\n\n🔺 Людей с 1 приоритетом и баллом выше: *0*"

        await callback.message.answer(result_msg, parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        await callback.message.answer(f"❌ Ошибка обработки: {e}")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
