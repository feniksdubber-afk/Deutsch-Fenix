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
# FSM (Holatlar) uchun MemoryStorage kerak
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ======================
# STATES (Holatlar)
# ======================
class QuizState(StatesGroup):
    waiting_for_answer = State()

# ======================
# DATABASE
# ======================
conn = sqlite3.connect("words.db", check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS words (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        german TEXT,
        uzbek TEXT,
        article TEXT,
        archived INTEGER DEFAULT 0
    )
""")
conn.commit()

# --- DB Funksiyalari (avvalgilari qoladi) ---
def get_random_word():
    cursor.execute("SELECT id, german, uzbek, article FROM words WHERE archived = 0 ORDER BY RANDOM() LIMIT 1")
    return cursor.fetchone()

def archive_word(word_id):
    cursor.execute("UPDATE words SET archived = 1 WHERE id = ?", (word_id,))
    conn.commit()

# ======================
# KEYBOARDS
# ======================
def test_keyboard(word_id):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("👁 Tarjimani ko'rish", callback_data=f"show_{word_id}"),
        InlineKeyboardButton("✅ Yod oldim (Arxivlash)", callback_data=f"archive_{word_id}"),
        InlineKeyboardButton("🔁 Keyingisi", callback_data="next")
    )
    return kb

# ======================
# HANDLERS
# ======================
@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    await message.answer("🇩🇪 Nemis tili o'qituvchi botiga xush kelibsiz!\n\nMen so'zni yuboraman, siz esa tarjimasi yozasiz.")

@dp.message_handler(commands=["test"], state="*")
async def test_word(message: types.Message, state: FSMContext):
    word = get_random_word()
    if not word:
        await message.answer("⚠️ Aktiv so'zlar tugadi.")
        return
        
    word_id, german, uzbek, article = word
    await state.update_data(correct_answer=uzbek, current_word_id=word_id, german_word=f"{article} {german}")
    
    question = f"Tarjimasi nima?\n\n👉  **{article} {german}**"
    await message.answer(question, reply_markup=test_keyboard(word_id), parse_mode="Markdown")
    await QuizState.waiting_for_answer.set()

@dp.callback_query_handler(lambda c: c.data == "next", state="*")
async def next_callback(call: types.CallbackQuery, state: FSMContext):
    await call.message.delete()
    await test_word(call.message, state)

@dp.callback_query_handler(lambda c: c.data.startswith("show_"), state="*")
async def show_answer(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    answer = data.get("correct_answer")
    await call.answer(f"To'g'ri javob: {answer}", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith("archive_"), state="*")
async def archive_callback(call: types.CallbackQuery, state: FSMContext):
    word_id = int(call.data.split("_")[1])
    archive_word(word_id)
    await call.message.answer("✅ So'z yodlanganlar ro'yxatiga qo'shildi!")
    await test_word(call.message, state)

@dp.message_handler(state=QuizState.waiting_for_answer)
async def check_answer(message: types.Message, state: FSMContext):
    user_answer = message.text.strip().lower()
    data = await state.get_data()
    correct_answer = data.get("correct_answer").lower()
    german_word = data.get("german_word")

    if user_answer == correct_answer:
        await message.reply(f"🌟 **To'g'ri!** \n\n_{german_word}_ — _{user_answer}_", parse_mode="Markdown")
        await test_word(message, state) # Keyingi so'zga o'tish
    else:
        await message.answer(
            f"❌ Noto'g'ri.\n\n"
            f"Aslida: **{german_word}** — **{correct_answer.upper()}** bo'ladi.\n\n"
            f"Qaytadan urinib ko'ring yoki keyingisiga o'ting.",
            parse_mode="Markdown"
        )

# (Qolgan /stats va so'z qo'shish funksiyalari o'zgarishsiz qoladi...)

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
