import os
import sqlite3
import logging
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from dotenv import load_dotenv

# Loglarni yoqish
logging.basicConfig(level=logging.INFO)
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")

bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ======================
# STATES (Holatlar)
# ======================
class QuizState(StatesGroup):
    waiting_for_answer = State()

# ======================
# DATABASE (SQLite3)
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

# --- DB Funksiyalari ---
def add_word(german, uzbek, article=""):
    cursor.execute("INSERT INTO words (german, uzbek, article) VALUES (?, ?, ?)", (german, uzbek, article))
    conn.commit()

def get_random_word():
    cursor.execute("SELECT id, german, uzbek, article FROM words WHERE archived = 0 ORDER BY RANDOM() LIMIT 1")
    return cursor.fetchone()

def archive_word(word_id):
    cursor.execute("UPDATE words SET archived = 1 WHERE id = ?", (word_id,))
    conn.commit()

def stats_data():
    cursor.execute("SELECT COUNT(*) FROM words")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM words WHERE archived = 1")
    archived = cursor.fetchone()[0]
    return total, archived, total - archived

def parse_word(text):
    if "-" not in text: return None
    left, right = text.split("-", 1)
    left, right = left.strip(), right.strip()
    article = ""
    german = left
    for art in ["der ", "die ", "das "]:
        if left.lower().startswith(art):
            article = art.strip()
            german = left[len(art):].strip()
            break
    return german, right, article

# ======================
# KEYBOARDS
# ======================
def test_keyboard(word_id):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("👁 Tarjima", callback_data=f"show_{word_id}"),
        InlineKeyboardButton("✅ Yod oldim", callback_data=f"archive_{word_id}")
    )
    kb.add(InlineKeyboardButton("🔁 Keyingi so'z", callback_data="next"))
    return kb

# ======================
# COMMANDS
# ======================
@dp.message_handler(commands=["start"], state="*")
async def start(message: types.Message, state: FSMContext):
    await state.finish() # Har ehtimolga qarshi holatni tozalash
    await message.answer(
        "🇩🇪 **Nemis tili o'qituvchi botiga xush kelibsiz!**\n\n"
        "🔹 **So'z qo'shish:** `der Tisch - stol` shaklida yuboring.\n"
        "🔹 **O'rganish:** /test buyrug'ini bosing.\n"
        "🔹 **Statistika:** /stats buyrug'ini bosing.",
        parse_mode="Markdown"
    )

@dp.message_handler(commands=["stats"], state="*")
async def show_stats(message: types.Message):
    total, archived, active = stats_data()
    await message.answer(f"📊 **Statistika:**\n\nJami: {total}\nYodlangan: {archived}\nAktiv: {active}", parse_mode="Markdown")

@dp.message_handler(commands=["test"], state="*")
async def test_word(message: types.Message, state: FSMContext):
    word = get_random_word()
    if not word:
        await message.answer("⚠️ Hozircha aktiv so'zlar yo'q. Avval so'z qo'shing!")
        return
        
    word_id, german, uzbek, article = word
    # Ma'lumotlarni holatda saqlab turamiz
    await state.update_data(correct_answer=uzbek, current_word_id=word_id, german_word=f"{article} {german}")
    
    await message.answer(f"Bu so'zning tarjimasi nima?\n\n👉 **{article} {german}**", 
                         reply_markup=test_keyboard(word_id), parse_mode="Markdown")
    await QuizState.waiting_for_answer.set()

# ======================
# CALLBACKS
# ======================
@dp.callback_query_handler(state="*")
async def callbacks(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    if call.data == "next":
        await call.message.delete()
        await test_word(call.message, state)
        
    elif call.data.startswith("show_"):
        data = await state.get_data()
        ans = data.get("correct_answer", "Xatolik!")
        await call.message.answer(f"💡 Javob: **{ans.upper()}**", parse_mode="Markdown")
        
    elif call.data.startswith("archive_"):
        word_id = int(call.data.split("_")[1])
        archive_word(word_id)
        await call.message.answer("✅ Zo'r! Bu so'zni arxivga oldik.")
        await test_word(call.message, state)

# ======================
# TEXT HANDLERS
# ======================
@dp.message_handler(state=QuizState.waiting_for_answer)
async def handle_quiz(message: types.Message, state: FSMContext):
    # Agar foydalanuvchi test vaqtida / komandasini yozsa
    if message.text.startswith('/'):
        await state.finish()
        # Komandani qayta ishlash uchun xabarni "qaytarib" yuboramiz
        if message.text == "/test": await test_word(message, state)
        elif message.text == "/stats": await show_stats(message)
        return

    user_answer = message.text.strip().lower()
    data = await state.get_data()
    correct_answer = data.get("correct_answer", "").lower()
    german_full = data.get("german_word")

    if user_answer == correct_answer:
        await message.reply(f"🌟 **TO'G'RI!**\n_{german_full}_ — _{user_answer}_", parse_mode="Markdown")
        await test_word(message, state)
    else:
        # So'z qo'shish formatini tekshirish (test ichida ham so'z qo'shish imkoni)
        parsed = parse_word(message.text)
        if parsed:
            add_word(*parsed)
            await message.reply("✅ Test to'xtatildi va yangi so'z saqlandi!")
            await state.finish()
        else:
            await message.answer(
                f"❌ Noto'g'ri!\n\nJavob: **{correct_answer.upper()}**\n\n"
                "Qaytadan yozing yoki 'Keyingi so'z'ni bosing.", 
                parse_mode="Markdown"
            )

@dp.message_handler(state=None) # Test rejimi bo'lmagan holatda
async def save_new_word(message: types.Message):
    parsed = parse_word(message.text)
    if parsed:
        add_word(*parsed)
        await message.reply("✅ Yangi so'z bazaga qo'shildi!")
    else:
        await message.answer("Tushunmadim 🥸\n\nSo'z qo'shish uchun: `der Tisch - stol` formatida yozing.")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
