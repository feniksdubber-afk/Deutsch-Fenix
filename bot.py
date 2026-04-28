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
# YORDAMCHI FUNKSIYALAR
# ======================

def simplify_uz(text):
    """O'zbekcha matnni solishtirish uchun normallashtirish"""
    if not text:
        return ""
    text = text.strip().lower()
    for char in ["'", "'", "`", "´", "ʻ", "ʼ"]:
        text = text.replace(char, "'")
    return text

def format_german(german, article):
    """Nemischa otlarni har doim katta harf bilan formatlash"""
    german = german.strip()
    if article and article.lower() in ['der', 'die', 'das']:
        german = german.capitalize()
    return f"{article.lower()} {german}" if article else german

def parse_word_line(line):
    """'der Tisch - stol' kabi qatorni ajratib olish"""
    if "-" not in line:
        return None
    left, right = line.split("-", 1)
    left, right = left.strip(), right.strip()
    if not left or not right:
        return None

    article = ""
    german = left
    for art in ["der ", "die ", "das "]:
        if left.lower().startswith(art):
            article = art.strip()
            german = left[len(art):].strip().capitalize()
            break

    return article, german, right

# ======================
# DATABASE
# ======================

# Railway uchun: /data papkasida saqlash (persistent volume bo'lsa)
# Agar yo'q bo'lsa, joriy papkada
DB_PATH = os.getenv("DB_PATH", "words.db")

# Papka mavjud bo'lmasa, avtomatik yaratish
db_dir = os.path.dirname(DB_PATH)
if db_dir:
    os.makedirs(db_dir, exist_ok=True)

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS words (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        german TEXT NOT NULL,
        uzbek TEXT NOT NULL,
        article TEXT DEFAULT '',
        archived INTEGER DEFAULT 0
    )
""")
conn.commit()

# ======================
# HANDLERS
# ======================

@dp.message_handler(commands=["start"], state="*")
async def start(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer(
        "🇩🇪 *Deutsch Fenix v2.1*\n\n"
        "📥 *So'z qo'shish:*\n"
        "`der Tisch - stol`\n"
        "`laufen - yugurmoq`\n"
        "_(bir nechta qator ham bo'ladi)_\n\n"
        "📚 *Komandalar:*\n"
        "/test — So'z testi\n"
        "/list — Barcha so'zlar\n"
        "/stats — Statistika\n"
        "/archived — Arxivlangan so'zlar",
        parse_mode="Markdown"
    )

@dp.message_handler(commands=["stats"], state="*")
async def stats(message: types.Message, state: FSMContext):
    await state.finish()
    cursor.execute("SELECT COUNT(*) FROM words WHERE archived = 0")
    active = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM words WHERE archived = 1")
    archived = cursor.fetchone()[0]
    await message.answer(
        f"📊 *Statistika:*\n\n"
        f"✅ Aktiv so'zlar: *{active}* ta\n"
        f"📦 Arxivlangan: *{archived}* ta\n"
        f"📚 Jami: *{active + archived}* ta",
        parse_mode="Markdown"
    )

@dp.message_handler(commands=["list"], state="*")
async def list_words(message: types.Message, state: FSMContext):
    await state.finish()
    cursor.execute("SELECT german, article, uzbek FROM words WHERE archived = 0 ORDER BY german")
    words = cursor.fetchall()
    if not words:
        await message.answer("📭 Hozircha so'z yo'q. Qo'shing!")
        return
    text = "📋 *Aktiv so'zlar:*\n\n"
    for german, article, uzbek in words:
        display = format_german(german, article)
        text += f"• *{display}* — {uzbek}\n"
    # Telegram 4096 belgi limitiga ko'ra bo'lib yuborish
    for i in range(0, len(text), 4000):
        await message.answer(text[i:i+4000], parse_mode="Markdown")

@dp.message_handler(commands=["archived"], state="*")
async def list_archived(message: types.Message, state: FSMContext):
    await state.finish()
    cursor.execute("SELECT id, german, article, uzbek FROM words WHERE archived = 1 ORDER BY german")
    words = cursor.fetchall()
    if not words:
        await message.answer("📭 Arxivlangan so'z yo'q.")
        return
    text = "📦 *Arxivlangan so'zlar:*\n\n"
    for word_id, german, article, uzbek in words:
        display = format_german(german, article)
        text += f"• *{display}* — {uzbek}\n"
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("♻️ Hammasini qaytarish", callback_data="unarchive_all")
    )
    await message.answer(text, parse_mode="Markdown", reply_markup=kb)

@dp.message_handler(commands=["test"], state="*")
async def test_word(message: types.Message, state: FSMContext):
    await state.finish()
    cursor.execute("SELECT id, german, uzbek, article FROM words WHERE archived = 0 ORDER BY RANDOM() LIMIT 1")
    word = cursor.fetchone()

    if not word:
        await message.answer("⚠️ Aktiv so'zlar yo'q. Avval so'z qo'shing!")
        return

    word_id, german, uzbek, article = word
    display_word = format_german(german, article)

    await state.update_data(correct_answer=uzbek, german_word=display_word, word_id=word_id)

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("👁 Ko'rsatish", callback_data=f"show_{word_id}"),
        InlineKeyboardButton("✅ Yod oldim", callback_data=f"archive_{word_id}"),
    )
    kb.add(InlineKeyboardButton("🔁 Keyingisi", callback_data="next"))

    await message.answer(
        f"🇩🇪 Tarjimasi nima?\n\n👉 *{display_word}*",
        reply_markup=kb,
        parse_mode="Markdown"
    )
    await QuizState.waiting_for_answer.set()

@dp.message_handler(state=QuizState.waiting_for_answer)
async def handle_quiz(message: types.Message, state: FSMContext):
    # Komanda kelsa — testni to'xtatib, o'sha komandani ishlatamiz
    if message.text.startswith('/'):
        await state.finish()
        # Komandani dispatcher orqali qayta yuborish
        new_message = message
        await dp.process_update(
            types.Update(update_id=0, message=new_message)
        )
        return

    user_ans = simplify_uz(message.text)
    data = await state.get_data()
    correct_ans = simplify_uz(data.get("correct_answer", ""))
    german_full = data.get("german_word", "")
    correct_original = data.get("correct_answer", "")

    if not correct_ans:
        await state.finish()
        await message.answer("⚠️ Xatolik yuz berdi. /test bilan qayta boshlang.")
        return

    if user_ans == correct_ans:
        await message.reply(
            f"🌟 *BARAKALLA!*\n_{german_full}_ = {correct_original}",
            parse_mode="Markdown"
        )
        # Keyingi so'zga o'tish
        await test_word(message, state)
    else:
        kb = InlineKeyboardMarkup(row_width=2)
        word_id = data.get("word_id")
        kb.add(
            InlineKeyboardButton("🔁 Keyingisi", callback_data="next"),
            InlineKeyboardButton("✅ Yod oldim", callback_data=f"archive_{word_id}"),
        )
        await message.answer(
            f"❌ *Xato!*\n\nTo'g'ri javob: `{correct_original}`",
            parse_mode="Markdown",
            reply_markup=kb
        )

@dp.message_handler(state=None)
async def bulk_add(message: types.Message):
    """So'z qo'shish: 'der Tisch - stol' yoki bir nechta qator"""
    lines = message.text.strip().split('\n')
    added = 0
    duplicates = []
    errors = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        parsed = parse_word_line(line)
        if not parsed:
            errors.append(line)
            continue

        article, german, uzbek = parsed

        # Dublikat tekshiruvi
        cursor.execute(
            "SELECT id FROM words WHERE LOWER(german) = LOWER(?) AND LOWER(article) = LOWER(?)",
            (german, article)
        )
        existing = cursor.fetchone()
        if existing:
            duplicates.append(format_german(german, article))
            continue

        cursor.execute(
            "INSERT INTO words (german, uzbek, article) VALUES (?, ?, ?)",
            (german, uzbek, article)
        )
        added += 1

    if added > 0:
        conn.commit()

    # Natija xabari
    parts = []
    if added > 0:
        parts.append(f"✅ *{added} ta so'z qo'shildi!*")
    if duplicates:
        dup_list = ", ".join(f"`{w}`" for w in duplicates)
        parts.append(f"⚠️ Allaqachon bor: {dup_list}")
    if errors:
        parts.append(f"❓ Format noto'g'ri: {len(errors)} ta qator\n_Format: `der Tisch - stol`_")

    if parts:
        await message.reply("\n".join(parts), parse_mode="Markdown")
    else:
        await message.reply("❓ Hech narsa qo'shilmadi.\n_Format: `der Tisch - stol`_", parse_mode="Markdown")

@dp.callback_query_handler(state="*")
async def process_callback(call: types.CallbackQuery, state: FSMContext):
    if call.data == "next":
        await call.message.delete()
        await test_word(call.message, state)

    elif call.data.startswith("archive_"):
        word_id = call.data.split("_")[1]
        cursor.execute("UPDATE words SET archived = 1 WHERE id = ?", (word_id,))
        conn.commit()
        await call.answer("📦 Arxivlandi!", show_alert=False)
        await call.message.delete()
        await test_word(call.message, state)

    elif call.data.startswith("show_"):
        data = await state.get_data()
        answer = data.get("correct_answer")
        if answer:
            await call.answer(f"💡 Javob: {answer}", show_alert=True)
        else:
            await call.answer("⚠️ Ma'lumot topilmadi. /test bilan qayta boshlang.", show_alert=True)

    elif call.data == "unarchive_all":
        cursor.execute("UPDATE words SET archived = 0 WHERE archived = 1")
        count = cursor.rowcount
        conn.commit()
        await call.answer(f"♻️ {count} ta so'z qaytarildi!", show_alert=True)
        await call.message.delete()

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
