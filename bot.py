import sqlite3
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import TOKEN

# ======================
# BOT SETTINGS
# ======================

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

# ======================
# DATABASE
# ======================

conn = sqlite3.connect("words.db")
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
    cursor.execute(
        "UPDATE words SET archived = 1 WHERE id = ?",
        (word_id,)
    )
    conn.commit()



def delete_word(word_id):
    cursor.execute(
        "DELETE FROM words WHERE id = ?",
        (word_id,)
    )
    conn.commit()



def export_words():
    cursor.execute("SELECT german, uzbek FROM words")
    words = cursor.fetchall()

    with open("all_words.txt", "w", encoding="utf-8") as f:
        for german, uzbek in words:
            f.write(f"{german} - {uzbek}
")

    return "all_words.txt"



def stats():
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
    left = left.strip()
    right = right.strip()

    article = ""
    german = left

    if left.lower().startswith(("der ", "die ", "das ")):
        article, german = left.split(" ", 1)

    return german, right, article



def word_buttons(word_id):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ Yod oldim", callback_data=f"archive_{word_id}"))
    kb.add(InlineKeyboardButton("📦 Arxiv", callback_data=f"archive_{word_id}"))
    kb.add(InlineKeyboardButton("❌ O'chirish", callback_data=f"delete_{word_id}"))
    kb.add(InlineKeyboardButton("🔁 Keyingi savol", callback_data="next"))
    return kb


# ======================
# COMMANDS
# ======================


@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    await message.answer(
        "So'z yuboring:

"
        "das Handy - qo'l telefoni
"
        "Haus - uy

"
        "/test - test rejimi
"
        "/stats - statistika
"
        "/file - fayl yuklab olish"
    )


@dp.message_handler(commands=["test"])
async def test_word(message: types.Message):
    word = get_random_word()

    if not word:
        await message.answer("So'z topilmadi")
        return

    word_id, german, uzbek, article = word

    question = f"{article} {german} ?" if article else f"{german} ?"

    await message.answer(
        question,
        reply_markup=word_buttons(word_id)
    )


@dp.message_handler(commands=["stats"])
async def show_stats(message: types.Message):
    total, archived, active, last = stats()

    await message.answer(
        f"Jami so'zlar: {total}
"
        f"Arxivdagilar: {archived}
"
        f"Aktiv so'zlar: {active}
"
        f"Oxirgi qo'shilgan: {last}"
    )


@dp.message_handler(commands=["file"])
async def send_file(message: types.Message):
    file = export_words()
    await message.answer_document(open(file, "rb"))


@dp.callback_query_handler()
async def callbacks(call: types.CallbackQuery):
    data = call.data

    if data.startswith("archive_"):
        word_id = int(data.split("_")[1])
        archive_word(word_id)
        await call.message.answer("So'z arxivga olindi 📦")

    elif data.startswith("delete_"):
        word_id = int(data.split("_")[1])
        delete_word(word_id)
        await call.message.answer("So'z o'chirildi ❌")

    elif data == "next":
        await test_word(call.message)


@dp.message_handler()
async def save_words(message: types.Message):
    lines = message.text.split("
")
    added = 0

    for line in lines:
        parsed = parse_word(line)
        if parsed:
            german, uzbek, article = parsed
            add_word(german, uzbek, article)
            added += 1

    if added:
        await message.answer(f"{added} ta so'z saqlandi ✅")
    else:
        await message.answer("Format noto'g'ri")


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
