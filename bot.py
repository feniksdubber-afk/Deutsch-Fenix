import os
import sqlite3
import logging
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

# Loglarni sozlash (Xatoliklarni ko'rish uchun)
logging.basicConfig(level=logging.INFO)

# Sozlamalarni yuklash
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")

if not TOKEN:
    raise ValueError("BOT_TOKEN topilmadi! Railway Variables qismini tekshiring.")

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

# ======================
# DATABASE (SQLite3)
# ======================
def get_db_connection():
    # Railway'da fayllar bilan ishlashda check_same_thread=False zarur
    conn = sqlite3.connect("words.db", check_same_thread=False)
    return conn

conn = get_db_connection()
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

# ======================
# FUNCTIONS
# ======================
def add_word(german, uzbek, article=""):
    cursor.execute(
        "INSERT INTO words (german, uzbek, article) VALUES (?, ?, ?)",
        (german, uzbek, article)
    )
    conn.commit()

def get_random_word():
    cursor.execute(
        "SELECT id, german, uzbek, article FROM words WHERE archived = 0 ORDER BY RANDOM() LIMIT 1"
    )
    return cursor.fetchone()

def archive_word(word_id):
    cursor.execute("UPDATE words SET archived = 1 WHERE id = ?", (word_id,))
    conn.commit()

def delete_word(word_id):
    cursor.execute("DELETE FROM words WHERE id = ?", (word_id,))
    conn.commit()

def export_words():
    cursor.execute("SELECT german, uzbek FROM words")
    words = cursor.fetchall()
    file_name = "all_words.txt"
    with open(file_name, "w", encoding="utf-8") as f:
        for german, uzbek in words:
            f.write(f"{german} - {uzbek}\n")
    return file_name

def stats_data():
    cursor.execute("SELECT COUNT(*) FROM words")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM words WHERE archived = 1")
    archived = cursor.fetchone()[0]
    cursor.execute("SELECT german FROM words ORDER BY id DESC LIMIT 1")
    last = cursor.fetchone()
    last_word = last[0] if last else "Yo'q"
    return total, archived, total - archived, last_word

def parse_word(text):
    if "-" not in text:
        return None
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

def word_buttons(word_id):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("✅ Yod oldim", callback_data=f"archive_{word_id}"),
        InlineKeyboardButton("❌ O'chirish", callback_data=f"delete_{word_id}"),
        InlineKeyboardButton("🔁 Keyingi savol", callback_data="next")
    )
    return kb

# ======================
# COMMANDS
# ======================
@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    await message.answer(
        "🇩🇪 Nemis tili lug'at botiga xush kelibsiz!\n\n"
        "So'z qo'shish formati: `der Tisch - stol` yoki shunchaki `Tisch - stol` \n\n"
        "Buyruqlar:\n"
        "/test - Testni boshlash\n"
        "/stats - Statistika\n"
        "/file - Barcha so'zlarni yuklash",
        parse_mode="Markdown"
    )

@dp.message_handler(commands=["test"])
async def test_word(message: types.Message):
    word = get_random_word()
    if not word:
        await message.answer("⚠️ Hozircha aktiv so'zlar yo'q. Avval so'z qo'shing!")
        return
        
    word_id, german, uzbek, article = word
    question = f"**{article} {german}** ?" if article else f"**{german}** ?"
    
    await message.answer(f"Bu so'zning tarjimasi nima?\n\n{question}", 
                         reply_markup=word_buttons(word_id), 
                         parse_mode="Markdown")

@dp.message_handler(commands=["stats"])
async def show_stats(message: types.Message):
    total, archived, active, last = stats_data()
    await message.answer(
        f"📊 **Statistika:**\n\n"
        f"Jami so'zlar: {total}\n"
        f"Yod olingan: {archived}\n"
        f"Aktiv so'zlar: {active}\n"
        f"Oxirgi qo'shilgan: {last}",
        parse_mode="Markdown"
    )

@dp.message_handler(commands=["file"])
async def send_file(message: types.Message):
    file_path = export_words()
    if os.path.exists(file_path):
        with open(file_path, "rb") as f:
            await message.answer_document(f, caption="Sizning barcha so'zlaringiz.")
    else:
        await message.answer("Fayl topilmadi.")

@dp.callback_query_handler()
async def callbacks(call: types.CallbackQuery):
    await call.answer()
    data = call.data
    
    if data.startswith("archive_"):
        word_id = int(data.split("_")[1])
        archive_word(word_id)
        await call.message.edit_text("✅ Barakalla! So'z arxivga o'tkazildi.")
        await test_word(call.message)
        
    elif data.startswith("delete_"):
        word_id = int(data.split("_")[1])
        delete_word(word_id)
        await call.message.edit_text("❌ So'z butunlay o'chirildi.")
        await test_word(call.message)
        
    elif data == "next":
        await call.message.delete()
        await test_word(call.message)

@dp.message_handler()
async def save_words(message: types.Message):
    lines = message.text.strip().split('\n')
    added = 0
    for line in lines:
        parsed = parse_word(line)
        if parsed:
            add_word(*parsed)
            added += 1
            
    if added > 0:
        await message.reply(f"✅ {added} ta so'z bazaga qo'shildi!")
    else:
        await message.reply("Tushunmadim. So'zni 'Nemischa - Uzbekcha' formatida yuboring.")

# ======================
# RUN BOT
# ======================
if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
