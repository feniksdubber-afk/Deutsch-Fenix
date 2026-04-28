import os
import sqlite3
import logging
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")

bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

class QuizState(StatesGroup):
    waiting_for_answer = State()

# ======================
# YORDAMCHI FUNKSIYALAR (Smart Logic)
# ======================

def simplify_uz(text):
    """ O'zbekcha matnni solishtirish uchun normallashtirish """
    if not text: return ""
    # Hammasini kichik harfga o'tkazish, bo'shliqlarni olib tashlash
    text = text.strip().lower()
    # Barcha turdagi apostrof va tutuq belgilarini bitta standartga keltirish
    replacements = ["‘", "’", "`", "´", "ʻ", "ʼ"]
    for char in replacements:
        text = text.replace(char, "'")
    return text

def format_german(german, article):
    """ Nemischa otlarni har doim katta harf bilan formatlash """
    german = german.strip()
    # Agar so'z ot bo'lsa (artikli bo'lsa), uni katta harf bilan boshlaymiz
    if article and article.lower() in ['der', 'die', 'das']:
        german = german.capitalize()
    return f"{article.lower()} {german}" if article else german

# ======================
# DATABASE
# ======================
conn = sqlite3.connect("words.db", check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS words (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        german TEXT, uzbek TEXT, article TEXT, archived INTEGER DEFAULT 0
    )
""")
conn.commit()

# ======================
# HANDLERS
# ======================

@dp.message_handler(commands=["start"], state="*")
async def start(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer("🇩🇪 **Deutsch Fenix v2.0**\n\n- So'z qo'shish: `der Tisch - stol` \n- Test: /test\n- Statistika: /stats", parse_mode="Markdown")

@dp.message_handler(commands=["test"], state="*")
async def test_word(message: types.Message, state: FSMContext):
    cursor.execute("SELECT id, german, uzbek, article FROM words WHERE archived = 0 ORDER BY RANDOM() LIMIT 1")
    word = cursor.fetchone()
    
    if not word:
        await message.answer("⚠️ Aktiv so'zlar yo'q.")
        return
        
    word_id, german, uzbek, article = word
    # Nemischa so'zni formatlash (Ot bo'lsa katta harf bilan)
    display_word = format_german(german, article)
    
    await state.update_data(correct_answer=uzbek, german_word=display_word)
    
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("👁 Tarjima", callback_data=f"show_{word_id}"),
        InlineKeyboardButton("✅ Yod oldim", callback_data=f"archive_{word_id}"),
        InlineKeyboardButton("🔁 Keyingi", callback_data="next")
    )
    
    await message.answer(f"Tarjimasi nima?\n\n👉 **{display_word}**", reply_markup=kb, parse_mode="Markdown")
    await QuizState.waiting_for_answer.set()

@dp.message_handler(state=QuizState.waiting_for_answer)
async def handle_quiz(message: types.Message, state: FSMContext):
    if message.text.startswith('/'):
        await state.finish()
        # Komandani bajarish...
        return

    user_ans = simplify_uz(message.text)
    data = await state.get_data()
    correct_ans = simplify_uz(data.get("correct_answer"))
    german_full = data.get("german_word")

    if user_ans == correct_ans:
        await message.reply(f"🌟 **BARAKALLA!**\n{german_full} = {message.text}")
        await test_word(message, state)
    else:
        # Agar noto'g'ri bo'lsa, bazadagi original holatini ko'rsatadi
        await message.answer(f"❌ **Xato!**\n\nTo'g'ri javob: `{data.get('correct_answer').upper()}`", parse_mode="Markdown")

@dp.message_handler(state=None)
async def bulk_add(message: types.Message):
    lines = message.text.strip().split('\n')
    added = 0
    for line in lines:
        if "-" in line:
            left, right = line.split("-", 1)
            left, right = left.strip(), right.strip()
            
            article = ""
            german = left
            for art in ["der ", "die ", "das "]:
                if left.lower().startswith(art):
                    article = art.strip()
                    german = left[len(art):].strip().capitalize() # Bazaga saqlashda ham o'zi katta qilib qo'yadi
                    break
            
            cursor.execute("INSERT INTO words (german, uzbek, article) VALUES (?, ?, ?)", (german, right, article))
            added += 1
    
    if added > 0:
        conn.commit()
        await message.reply(f"✅ {added} ta so'z muvaffaqiyatli qo'shildi!")

@dp.callback_query_handler(state="*")
async def process_callback(call: types.CallbackQuery, state: FSMContext):
    if call.data == "next":
        await call.message.delete()
        await test_word(call.message, state)
    elif call.data.startswith("archive_"):
        word_id = call.data.split("_")[1]
        cursor.execute("UPDATE words SET archived = 1 WHERE id = ?", (word_id,))
        conn.commit()
        await call.message.answer("✅ Arxivlandi!")
        await test_word(call.message, state)
    elif call.data.startswith("show_"):
        data = await state.get_data()
        await call.answer(f"Javob: {data.get('correct_answer')}", show_alert=True)

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
