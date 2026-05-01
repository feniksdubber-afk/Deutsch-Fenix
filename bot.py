import os
import io
import csv
import sqlite3
import logging
import random
import asyncio
from datetime import date, datetime, timedelta
import pytz
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from gtts import gTTS
from dotenv import load_dotenv

TZ = pytz.timezone("Asia/Tashkent")  # UTC+5 — O'zbekiston vaqti

logging.basicConfig(level=logging.INFO)
load_dotenv()
TOKEN    = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")  # Kunlik eslatma uchun (ixtiyoriy)

bot     = Bot(token=TOKEN)
storage = MemoryStorage()
dp      = Dispatcher(bot, storage=storage)

# ======================
# STATES
# ======================
class QuizState(StatesGroup):
    waiting_for_answer  = State()
    waiting_for_article = State()

class CategoryState(StatesGroup):
    waiting_name        = State()
    waiting_assign_word = State()
    waiting_new_word    = State()   # Kategoriyaga yangi so'z qo'shish

class ReminderState(StatesGroup):
    waiting_time = State()

class DeleteState(StatesGroup):
    waiting_word = State()

# ======================
# DATABASE
# ======================
DB_PATH = os.getenv("DB_PATH", "words.db")
db_dir  = os.path.dirname(DB_PATH)
if db_dir:
    os.makedirs(db_dir, exist_ok=True)

conn   = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()

cursor.executescript("""
    CREATE TABLE IF NOT EXISTS words (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        german        TEXT NOT NULL,
        uzbek         TEXT NOT NULL,
        article       TEXT DEFAULT '',
        archived      INTEGER DEFAULT 0,
        category_id   INTEGER DEFAULT NULL,
        wrong_count   INTEGER DEFAULT 0,
        correct_count INTEGER DEFAULT 0,
        level         TEXT DEFAULT 'custom',
        -- SM-2 Spaced Repetition fields
        ease_factor   REAL DEFAULT 2.5,
        interval      INTEGER DEFAULT 1,
        repetitions   INTEGER DEFAULT 0,
        next_review   TEXT DEFAULT NULL
    );
    CREATE TABLE IF NOT EXISTS categories (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        name      TEXT NOT NULL,
        parent_id INTEGER DEFAULT NULL,
        UNIQUE(name, parent_id)
    );
    CREATE TABLE IF NOT EXISTS stats (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        test_date TEXT NOT NULL,
        correct   INTEGER DEFAULT 0,
        wrong     INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS user_settings (
        user_id         INTEGER PRIMARY KEY,
        reminder_hour   INTEGER DEFAULT 20,
        reminder_minute INTEGER DEFAULT 0,
        reminder_active INTEGER DEFAULT 1
    );
""")

# Mavjud DB uchun yangi ustunlar
for col_sql in [
    "ALTER TABLE words ADD COLUMN category_id   INTEGER DEFAULT NULL",
    "ALTER TABLE words ADD COLUMN wrong_count   INTEGER DEFAULT 0",
    "ALTER TABLE words ADD COLUMN correct_count INTEGER DEFAULT 0",
    "ALTER TABLE words ADD COLUMN level         TEXT DEFAULT 'custom'",
    "ALTER TABLE words ADD COLUMN ease_factor   REAL DEFAULT 2.5",
    "ALTER TABLE words ADD COLUMN interval      INTEGER DEFAULT 1",
    "ALTER TABLE words ADD COLUMN repetitions   INTEGER DEFAULT 0",
    "ALTER TABLE words ADD COLUMN next_review   TEXT DEFAULT NULL",
    "ALTER TABLE categories ADD COLUMN parent_id INTEGER DEFAULT NULL",
]:
    try:
        cursor.execute(col_sql)
    except Exception:
        pass

conn.commit()

# ======================
# B1 SO'ZLARI (namuna — to'liq ro'yxat alohida fayl yoki DB dan)
# Bu yerda 30 ta namuna, asl bot.py ga 800 ta qo'shish kerak
# ======================
B1_WORDS = [
    ("", "erklären",   "tushuntirmoq"),
    ("", "beschreiben","tasvirlamoq"),
    ("", "vergleichen","solishtirishmoq"),
    ("", "verbessern", "yaxshilamoq"),
    ("", "entwickeln", "rivojlantirmoq"),
    ("", "entscheiden","qaror qilmoq"),
    ("", "bemerken",   "payqamoq"),
    ("", "erinnern",   "eslatmoq"),
    ("", "untersuchen","tekshirmoq"),
    ("", "vermeiden",  "qochmoq"),
    ("die", "Erfahrung",  "tajriba"),
    ("die", "Meinung",    "fikr"),
    ("die", "Möglichkeit","imkoniyat"),
    ("die", "Lösung",     "yechim"),
    ("die", "Veränderung","o'zgarish"),
    ("die", "Gesellschaft","jamiyat"),
    ("die", "Umgebung",   "muhit"),
    ("die", "Beziehung",  "munosabat"),
    ("die", "Entscheidung","qaror"),
    ("die", "Bedeutung",  "ma'no, ahamiyat"),
    ("der", "Unterschied","farq"),
    ("der", "Einfluss",   "ta'sir"),
    ("der", "Vorteil",    "afzallik"),
    ("der", "Nachteil",   "kamchilik"),
    ("der", "Vorschlag",  "taklif"),
    ("das", "Ergebnis",   "natija"),
    ("das", "Verhalten",  "xulq-atvor"),
    ("das", "Beispiel",   "misol"),
    ("das", "Ziel",       "maqsad"),
    ("das", "Thema",      "mavzu"),
]

def seed_b1_words():
    """B1 so'zlarini DB ga bir marta qo'shish"""
    # B1 kategoriyasini yaratish
    try:
        cursor.execute("INSERT INTO categories (name) VALUES (?)", ("🎯 B1 darajasi",))
        conn.commit()
    except sqlite3.IntegrityError:
        pass

    cursor.execute("SELECT id FROM categories WHERE name=?", ("🎯 B1 darajasi",))
    row = cursor.fetchone()
    if not row:
        return
    cat_id = row[0]

    added = 0
    for article, german, uzbek in B1_WORDS:
        cursor.execute(
            "SELECT id FROM words WHERE LOWER(german)=LOWER(?) AND LOWER(article)=LOWER(?)",
            (german, article)
        )
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO words (german, uzbek, article, level, category_id) VALUES (?, ?, ?, 'B1', ?)",
                (german, uzbek, article, cat_id)
            )
            added += 1
    if added:
        conn.commit()
        logging.info(f"B1: {added} ta so'z qo'shildi.")

seed_b1_words()

# ======================
# YORDAMCHI FUNKSIYALAR
# ======================

def simplify_uz(text):
    if not text:
        return ""
    text = text.strip().lower()
    for char in ["\u2018","\u2019","\u02bc","\u02bb","\u0060","\u00b4","\u02c8","ʻ","ʼ","'","'","`","´"]:
        text = text.replace(char, "'")
    return text

def format_german(german, article):
    german = german.strip()
    if article and article.lower() in ["der","die","das"]:
        german = german.capitalize()
    return f"{article.lower()} {german}" if article else german

def parse_word_line(line):
    """
    Qatorni parse qiladi. Ajratuvchi sifatida ' – ', ' - ', '–', '-' ni qabul qiladi.
    Ko'p so'zli iboralar ham ishlaydi (masalan: auf dem Land leben – ...).
    """
    # Barcha tire turlarini normallashtirish
    normalized = line.replace(" – ", " - ").replace("–", " - ").replace("—", " - ")
    if " - " not in normalized:
        return None
    left, right = normalized.split(" - ", 1)
    left, right = left.strip(), right.strip()
    if not left or not right:
        return None
    article, german = "", left
    for art in ["der ", "die ", "das "]:
        if left.lower().startswith(art):
            article = art.strip()
            german  = left[len(art):].strip()
            # Artikl bor bo'lsa birinchi harf katta
            if german and not any(c.islower() for c in german[:2]):
                pass  # Allaqachon katta
            else:
                german = german[0].upper() + german[1:] if german else german
            break
    return article, german, right

def log_stat(correct: bool):
    today = str(date.today())
    cursor.execute("SELECT id FROM stats WHERE test_date = ?", (today,))
    row = cursor.fetchone()
    if row:
        col = "correct" if correct else "wrong"
        cursor.execute(f"UPDATE stats SET {col} = {col} + 1 WHERE id = ?", (row[0],))
    else:
        c, w = (1, 0) if correct else (0, 1)
        cursor.execute("INSERT INTO stats (test_date, correct, wrong) VALUES (?, ?, ?)", (today, c, w))
    conn.commit()

def get_streak():
    cursor.execute("SELECT test_date FROM stats ORDER BY test_date DESC")
    rows = cursor.fetchall()
    if not rows:
        return 0
    streak     = 0
    check_date = date.today()
    for (d_str,) in rows:
        d = date.fromisoformat(d_str)
        if d == check_date or d == check_date - timedelta(days=1):
            streak    += 1
            check_date = d - timedelta(days=1)
        else:
            break
    return streak

# ======================
# SM-2 SPACED REPETITION
# ======================

def sm2_update(word_id: int, quality: int):
    """
    SM-2 algoritmi: quality 0-5 (0-2 = xato, 3-5 = to'g'ri)
    """
    cursor.execute(
        "SELECT ease_factor, interval, repetitions FROM words WHERE id=?", (word_id,)
    )
    row = cursor.fetchone()
    if not row:
        return
    ef, iv, rep = row

    if quality >= 3:
        if rep == 0:
            iv = 1
        elif rep == 1:
            iv = 6
        else:
            iv = round(iv * ef)
        rep += 1
    else:
        rep = 0
        iv  = 1

    ef = max(1.3, ef + 0.1 - (5 - quality) * (0.08 + (5 - quality) * 0.02))
    next_review = str(date.today() + timedelta(days=iv))

    cursor.execute(
        "UPDATE words SET ease_factor=?, interval=?, repetitions=?, next_review=? WHERE id=?",
        (ef, iv, rep, next_review, word_id)
    )
    conn.commit()

def pick_sr_word(category_id=None):
    """
    Spaced Repetition: bugun takrorlanishi kerak so'zlarni birinchi tanlash,
    aks holda eng kam bilgan so'zni olish.
    """
    today = str(date.today())
    base  = "WHERE archived=0"
    if category_id:
        base += f" AND category_id={category_id}"

    # Bugun yoki o'tgan kun scheduled so'zlar
    cursor.execute(
        f"SELECT id, german, uzbek, article FROM words {base} "
        f"AND (next_review IS NULL OR next_review <= ?) ORDER BY RANDOM() LIMIT 1",
        (today,)
    )
    word = cursor.fetchone()
    if not word:
        # Hamma so'z scheduled — eng yaqin keluvchini ol
        cursor.execute(
            f"SELECT id, german, uzbek, article FROM words {base} ORDER BY RANDOM() LIMIT 1"
        )
        word = cursor.fetchone()
    return word

# ======================
# FOYDALANUVCHI SOZLAMALARI
# ======================

def get_user_settings(user_id: int):
    cursor.execute("SELECT reminder_hour, reminder_minute, reminder_active FROM user_settings WHERE user_id=?", (user_id,))
    row = cursor.fetchone()
    if row:
        return {"hour": row[0], "minute": row[1], "active": bool(row[2])}
    return {"hour": 20, "minute": 0, "active": True}

def save_user_settings(user_id: int, hour: int, minute: int, active: int = 1):
    cursor.execute(
        "INSERT INTO user_settings (user_id, reminder_hour, reminder_minute, reminder_active) "
        "VALUES (?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET "
        "reminder_hour=excluded.reminder_hour, reminder_minute=excluded.reminder_minute, "
        "reminder_active=excluded.reminder_active",
        (user_id, hour, minute, active)
    )
    conn.commit()

# ======================
# KUNLIK ESLATMA (background task)
# ======================

async def daily_reminder_loop():
    """Har daqiqa tekshirib, kerakli vaqtda eslatma yuboradi"""
    sent_today: set = set()
    while True:
        await asyncio.sleep(60)
        now = datetime.now(TZ)
        today_key = now.strftime("%Y-%m-%d")

        cursor.execute(
            "SELECT user_id, reminder_hour, reminder_minute FROM user_settings WHERE reminder_active=1"
        )
        users = cursor.fetchall()
        for user_id, r_hour, r_minute in users:
            key = f"{user_id}_{today_key}"
            if key in sent_today:
                continue
            if now.hour == r_hour and now.minute == r_minute:
                try:
                    kb = InlineKeyboardMarkup(row_width=2)
                    kb.add(
                        InlineKeyboardButton("🎯 Test boshlash", callback_data="reminder_test"),
                        InlineKeyboardButton("⏰ O'zgartirish",   callback_data="reminder_change"),
                    )
                    await bot.send_message(
                        user_id,
                        "🔔 *Kunlik eslatma!*\n\n"
                        "Bugun test qildingizmi? 🇩🇪\n"
                        "Har kun biroz mashq qilish — eng yaxshi yo'l! 💪",
                        reply_markup=kb,
                        parse_mode="Markdown"
                    )
                    sent_today.add(key)
                except Exception as e:
                    logging.warning(f"Reminder failed for {user_id}: {e}")

# ======================
# TEST YUBORISH (Spaced Repetition bilan)
# ======================

async def send_test_question(message: types.Message, state: FSMContext, category_id=None):
    await state.finish()

    word = pick_sr_word(category_id)

    if not word:
        label = "Bu kategoriyada" if category_id else "Aktiv"
        await message.answer(f"⚠️ {label} so'zlar yo'q.")
        return

    word_id, german, uzbek, article = word
    display_german = format_german(german, article)

    modes = ["de_uz", "uz_de"]
    if article:
        modes.append("article")
    mode = random.choice(modes)

    await state.update_data(
        word_id=word_id, german=german, uzbek=uzbek,
        article=article, display_german=display_german,
        mode=mode, category_id=category_id
    )

    if mode == "de_uz":
        nav_kb = _quiz_kb(word_id, "next")
        await message.answer(
            f"🇩🇪 ➜ 🇺🇿  *Tarjimasi nima?*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👉  *{display_german}*\n"
            f"━━━━━━━━━━━━━━━",
            reply_markup=nav_kb, parse_mode="Markdown"
        )
        await QuizState.waiting_for_answer.set()

    elif mode == "uz_de":
        nav_kb = _quiz_kb(word_id, "next")
        await message.answer(
            f"🇺🇿 ➜ 🇩🇪  *Nemischasi nima?*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👉  *{uzbek}*\n"
            f"━━━━━━━━━━━━━━━",
            reply_markup=nav_kb, parse_mode="Markdown"
        )
        await QuizState.waiting_for_answer.set()

    else:  # article
        art_kb = InlineKeyboardMarkup(row_width=3)
        art_kb.add(
            InlineKeyboardButton("🔵 der", callback_data="art_der"),
            InlineKeyboardButton("🔴 die", callback_data="art_die"),
            InlineKeyboardButton("🟢 das", callback_data="art_das"),
        )
        art_kb.add(InlineKeyboardButton("⏭ Keyingisi", callback_data="next"))
        await message.answer(
            f"🎯  *Artiklni toping:*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"❓  *___ {german}*\n"
            f"━━━━━━━━━━━━━━━",
            reply_markup=art_kb, parse_mode="Markdown"
        )
        await QuizState.waiting_for_article.set()

async def send_test_question_from_ids(message: types.Message, state: FSMContext, word_ids: list):
    """ID ro'yxatidan (sub+parent) bitta so'z tanlash va savol berish"""
    await state.finish()
    if not word_ids:
        await message.answer("⚠️ Bu bo'limlarda so'z yo'q.")
        return

    today = str(date.today())
    # Bugun takrorlanishi kerak bo'lganlarni birinchi olish
    placeholders = ",".join("?" * len(word_ids))
    cursor.execute(
        f"SELECT id, german, uzbek, article FROM words "
        f"WHERE id IN ({placeholders}) AND archived=0 "
        f"AND (next_review IS NULL OR next_review <= ?) ORDER BY RANDOM() LIMIT 1",
        (*word_ids, today)
    )
    word = cursor.fetchone()
    if not word:
        cursor.execute(
            f"SELECT id, german, uzbek, article FROM words "
            f"WHERE id IN ({placeholders}) AND archived=0 ORDER BY RANDOM() LIMIT 1",
            word_ids
        )
        word = cursor.fetchone()

    if not word:
        await message.answer("⚠️ So'z topilmadi.")
        return

    word_id, german, uzbek, article = word
    display_german = format_german(german, article)
    modes = ["de_uz", "uz_de"]
    if article:
        modes.append("article")
    mode = random.choice(modes)

    await state.update_data(
        word_id=word_id, german=german, uzbek=uzbek,
        article=article, display_german=display_german,
        mode=mode, category_id=None, test_word_ids=word_ids
    )

    if mode == "de_uz":
        await message.answer(
            f"🇩🇪 ➜ 🇺🇿  *Tarjimasi nima?*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👉  *{display_german}*\n"
            f"━━━━━━━━━━━━━━━",
            reply_markup=_quiz_kb(word_id, "next_ids"), parse_mode="Markdown"
        )
    elif mode == "uz_de":
        await message.answer(
            f"🇺🇿 ➜ 🇩🇪  *Nemischasi nima?*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👉  *{uzbek}*\n"
            f"━━━━━━━━━━━━━━━",
            reply_markup=_quiz_kb(word_id, "next_ids"), parse_mode="Markdown"
        )
    else:
        art_kb = InlineKeyboardMarkup(row_width=3)
        art_kb.add(
            InlineKeyboardButton("🔵 der", callback_data="art_der"),
            InlineKeyboardButton("🔴 die", callback_data="art_die"),
            InlineKeyboardButton("🟢 das", callback_data="art_das"),
        )
        art_kb.add(InlineKeyboardButton("⏭ Keyingisi", callback_data="next_ids"))
        await message.answer(
            f"🎯  *Artiklni toping:*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"❓  *___ {german}*\n"
            f"━━━━━━━━━━━━━━━",
            reply_markup=art_kb, parse_mode="Markdown"
        )
    await QuizState.waiting_for_answer.set()


def _quiz_kb(word_id, next_cb="next"):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("💡 Ko'rsatish",  callback_data=f"show_{word_id}"),
        InlineKeyboardButton("🔊 Talaffuz",    callback_data=f"audio_{word_id}"),
    )
    kb.add(
        InlineKeyboardButton("✅ Yod oldim",   callback_data=f"archive_{word_id}"),
        InlineKeyboardButton("🗑 O'chirish",    callback_data=f"delete_{word_id}"),
    )
    kb.add(InlineKeyboardButton("⏭ Keyingisi", callback_data=next_cb))
    return kb

# ======================
# /start
# ======================
@dp.message_handler(commands=["start"], state="*")
async def start(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id
    # Default settings saqlash
    cursor.execute(
        "INSERT OR IGNORE INTO user_settings (user_id, reminder_hour, reminder_minute, reminder_active) VALUES (?,20,0,1)",
        (user_id,)
    )
    conn.commit()

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🎯 Test boshlash",   callback_data="menu_test"),
        InlineKeyboardButton("💀 Qiyin so'zlar",   callback_data="menu_hard"),
    )
    kb.add(
        InlineKeyboardButton("📊 Statistika",       callback_data="menu_stats"),
        InlineKeyboardButton("📋 So'zlar ro'yxati", callback_data="menu_list"),
    )
    kb.add(
        InlineKeyboardButton("📁 Kategoriyalar",    callback_data="menu_cats"),
        InlineKeyboardButton("🌐 B1 darajasi",      callback_data="menu_b1"),
    )
    kb.add(
        InlineKeyboardButton("📤 Eksport",          callback_data="menu_export"),
        InlineKeyboardButton("⏰ Eslatma vaqti",    callback_data="reminder_change"),
    )
    kb.add(
        InlineKeyboardButton("🗑 So'z o'chirish",   callback_data="menu_delete"),
        InlineKeyboardButton("📦 Arxiv",            callback_data="menu_archived"),
    )

    await message.answer(
        "🇩🇪 *Deutsch Fenix v4.0*\n\n"
        "📥 *So'z qo'shish:*\n"
        "`der Tisch - stol`\n"
        "`laufen - yugurmoq`\n"
        "_(bir nechta qator ham bo'ladi)_\n\n"
        "⬇️ Yoki quyidagi tugmalardan foydalaning:",
        reply_markup=kb, parse_mode="Markdown"
    )

# ======================
# MENYU CALLBACK LARI
# ======================
@dp.callback_query_handler(lambda c: c.data in [
    "menu_test","menu_hard","menu_stats","menu_list",
    "menu_cats","menu_b1","menu_export","menu_delete","menu_archived",
    "reminder_test"
], state="*")
async def menu_callbacks(call: types.CallbackQuery, state: FSMContext):
    d = call.data
    await call.answer()

    if d in ("menu_test", "reminder_test"):
        data   = await state.get_data()
        cat_id = data.get("active_category")
        await call.message.delete()
        await send_test_question(call.message, state, category_id=cat_id)

    elif d == "menu_hard":
        await call.message.delete()
        await hard_test(call.message, state)

    elif d == "menu_stats":
        await call.message.delete()
        await stats_send(call.message, state)

    elif d == "menu_list":
        await call.message.delete()
        await list_words_send(call.message, state)

    elif d == "menu_cats":
        await call.message.delete()
        await kategoriya_send(call.message, state)

    elif d == "menu_b1":
        # B1 kategoriyasini ochish
        cursor.execute("SELECT id FROM categories WHERE name=?", ("🎯 B1 darajasi",))
        row = cursor.fetchone()
        if row:
            cid = row[0]
            await state.update_data(active_category=cid)
            await call.message.delete()
            await send_test_question(call.message, state, category_id=cid)
        else:
            await call.answer("B1 kategoriyasi topilmadi.", show_alert=True)

    elif d == "menu_export":
        await call.message.delete()
        await export_send(call.message)

    elif d == "menu_delete":
        await call.message.answer(
            "🗑 *So'z o'chirish*\n\n"
            "O'chirmoqchi bo'lgan so'zni yozing (german):\n"
            "Masalan: `Tisch` yoki `der Tisch`\n\n"
            "Bekor qilish: /start",
            parse_mode="Markdown"
        )
        await DeleteState.waiting_word.set()

    elif d == "menu_archived":
        await call.message.delete()
        await list_archived_send(call.message)

# ======================
# /test, /hard, /stats, /list, /archived, /kategoriya
# ======================
@dp.message_handler(commands=["test"], state="*")
async def test_cmd(message: types.Message, state: FSMContext):
    data   = await state.get_data()
    cat_id = data.get("active_category")
    await send_test_question(message, state, category_id=cat_id)

@dp.message_handler(commands=["hard"], state="*")
async def hard_test(message: types.Message, state: FSMContext):
    await state.finish()
    cursor.execute(
        "SELECT id, german, uzbek, article, wrong_count FROM words "
        "WHERE archived=0 AND wrong_count>0 ORDER BY wrong_count DESC LIMIT 1"
    )
    word = cursor.fetchone()
    if not word:
        await message.answer("🎉 Hozircha xato so'zlar yo'q! /test dan foydalaning.")
        return

    word_id, german, uzbek, article, wrong_count = word
    display_german = format_german(german, article)

    await state.update_data(
        word_id=word_id, german=german, uzbek=uzbek,
        article=article, display_german=display_german,
        mode="de_uz", category_id=None, hard_mode=True
    )
    kb = _quiz_kb(word_id, "hard_next")
    await message.answer(
        f"💀 *Qiyin so'z* ({wrong_count}× xato)\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"👉  *{display_german}*\n"
        f"━━━━━━━━━━━━━━━\n"
        f"_Tarjimasi nima?_",
        reply_markup=kb, parse_mode="Markdown"
    )
    await QuizState.waiting_for_answer.set()

@dp.message_handler(commands=["stats"], state="*")
async def stats(message: types.Message, state: FSMContext):
    await state.finish()
    await stats_send(message, state)

async def stats_send(message: types.Message, state: FSMContext):
    cursor.execute("SELECT COUNT(*) FROM words WHERE archived=0")
    active = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM words WHERE archived=1")
    archived_cnt = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM words WHERE wrong_count>2 AND archived=0")
    hard_cnt = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM words WHERE level='B1'")
    b1_cnt = cursor.fetchone()[0]

    today = str(date.today())
    cursor.execute("SELECT correct, wrong FROM stats WHERE test_date=?", (today,))
    row  = cursor.fetchone()
    t_c, t_w = (row[0], row[1]) if row else (0, 0)

    cursor.execute("SELECT SUM(correct), SUM(wrong) FROM stats")
    total = cursor.fetchone()
    tot_c = total[0] or 0
    tot_w = total[1] or 0
    tot   = tot_c + tot_w
    acc   = round(tot_c / tot * 100) if tot else 0

    streak  = get_streak()
    s_emoji = "🔥" if streak >= 3 else "📅"

    # SR: bugun takrorlanishi kerak so'zlar soni
    cursor.execute(
        "SELECT COUNT(*) FROM words WHERE archived=0 AND (next_review IS NULL OR next_review<=?)",
        (today,)
    )
    due_cnt = cursor.fetchone()[0]

    await message.answer(
        f"📊 *Statistika*\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📚 So'zlar: *{active}* aktiv · *{archived_cnt}* arxiv\n"
        f"🌐 B1 darajasi: *{b1_cnt}* ta\n"
        f"💀 Qiyin (3+ xato): *{hard_cnt}* ta\n"
        f"⏳ Bugun takrorlanishi kerak: *{due_cnt}* ta\n\n"
        f"📅 Bugun:  ✅ {t_c}  ❌ {t_w}\n"
        f"🏆 Jami:   ✅ {tot_c}  ❌ {tot_w}\n"
        f"🎯 Aniqlik: *{acc}%*\n\n"
        f"{s_emoji} Streak: *{streak}* kun",
        parse_mode="Markdown"
    )

@dp.message_handler(commands=["list"], state="*")
async def list_words(message: types.Message, state: FSMContext):
    await state.finish()
    await list_words_send(message, state)

async def list_words_send(message: types.Message, state: FSMContext):
    cursor.execute("""
        SELECT w.german, w.article, w.uzbek, c.name, w.level
        FROM words w
        LEFT JOIN categories c ON w.category_id = c.id
        WHERE w.archived = 0
        ORDER BY w.german
    """)
    words = cursor.fetchall()
    if not words:
        await message.answer("📭 Hozircha so'z yo'q. Qo'shing!")
        return
    text = "📋 *Barcha so'zlar:*\n\n"
    for german, article, uzbek, cat_name, level in words:
        display = format_german(german, article)
        tag     = f" `[{level}]`" if level and level != "custom" else ""
        cat_str = f" _{cat_name}_" if cat_name else ""
        text   += f"• *{display}* — {uzbek}{tag}{cat_str}\n"
    for i in range(0, len(text), 4000):
        await message.answer(text[i:i+4000], parse_mode="Markdown")

@dp.message_handler(commands=["archived"], state="*")
async def list_archived(message: types.Message, state: FSMContext):
    await state.finish()
    await list_archived_send(message)

async def list_archived_send(message: types.Message):
    cursor.execute("SELECT german, article, uzbek FROM words WHERE archived=1 ORDER BY german")
    words = cursor.fetchall()
    if not words:
        await message.answer("📭 Arxivlangan so'z yo'q.")
        return
    text = "📦 *Arxivlangan so'zlar:*\n\n"
    for german, article, uzbek in words:
        text += f"• *{format_german(german, article)}* — {uzbek}\n"
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("♻️ Hammasini qaytarish", callback_data="unarchive_all")
    )
    for i in range(0, len(text), 4000):
        await message.answer(text[i:i+4000], parse_mode="Markdown",
                             reply_markup=kb if i == 0 else None)

@dp.message_handler(commands=["kategoriya"], state="*")
async def kategoriya_menu(message: types.Message, state: FSMContext):
    await state.finish()
    await kategoriya_send(message, state)

async def kategoriya_send(message: types.Message, state: FSMContext, parent_id=None):
    """
    parent_id=None  → root kategoriyalar
    parent_id=X     → X ning sub-kategoriyalari
    """
    if parent_id is None:
        cursor.execute(
            "SELECT id, name FROM categories WHERE parent_id IS NULL ORDER BY name"
        )
    else:
        cursor.execute(
            "SELECT id, name FROM categories WHERE parent_id=? ORDER BY name", (parent_id,)
        )
    cats = cursor.fetchall()

    kb = InlineKeyboardMarkup(row_width=1)

    # Yuqoriga qaytish (agar sub-daraja bo'lsa)
    if parent_id is not None:
        cursor.execute("SELECT id, name, parent_id FROM categories WHERE id=?", (parent_id,))
        prow = cursor.fetchone()
        back_cb = f"cat_open_{prow[0]}"
        kb.add(InlineKeyboardButton("◀️ Orqaga", callback_data=back_cb))

    kb.add(InlineKeyboardButton(
        "➕ Yangi kategoriya",
        callback_data=f"cat_new_{parent_id}" if parent_id else "cat_new_root"
    ))

    for cat_id, cat_name in cats:
        # So'zlar soni (bu kategoriya + barcha sub-kategoriyalar)
        direct_cnt = _count_words_recursive(cat_id)
        sub_cnt = _count_subcats(cat_id)
        label = f"📁 {cat_name} ({direct_cnt} so'z"
        if sub_cnt:
            label += f", {sub_cnt} bo'lim"
        label += ")"
        kb.add(InlineKeyboardButton(label, callback_data=f"cat_open_{cat_id}"))

    title = "📁 *Kategoriyalar:*"
    if parent_id is not None:
        cursor.execute("SELECT name FROM categories WHERE id=?", (parent_id,))
        pname = cursor.fetchone()
        if pname:
            title = f"📁 *{pname[0]}* — bo'limlar:"

    await message.answer(title, reply_markup=kb, parse_mode="Markdown")


def _count_words_recursive(cat_id):
    """Kategoriya va barcha sub-kategoriyalaridagi so'zlar soni"""
    cursor.execute("SELECT COUNT(*) FROM words WHERE category_id=? AND archived=0", (cat_id,))
    count = cursor.fetchone()[0]
    cursor.execute("SELECT id FROM categories WHERE parent_id=?", (cat_id,))
    for (sub_id,) in cursor.fetchall():
        count += _count_words_recursive(sub_id)
    return count


def _count_subcats(cat_id):
    cursor.execute("SELECT COUNT(*) FROM categories WHERE parent_id=?", (cat_id,))
    return cursor.fetchone()[0]


def _get_all_word_ids_recursive(cat_id):
    """Kategoriya va barcha sub-kategoriyalardan word id larni olish"""
    cursor.execute("SELECT id FROM words WHERE category_id=? AND archived=0", (cat_id,))
    ids = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT id FROM categories WHERE parent_id=?", (cat_id,))
    for (sub_id,) in cursor.fetchall():
        ids.extend(_get_all_word_ids_recursive(sub_id))
    return ids

# ======================
# EKSPORT
# ======================
@dp.message_handler(commands=["export"], state="*")
async def export_cmd(message: types.Message, state: FSMContext):
    await state.finish()
    await export_send(message)

async def export_send(message: types.Message):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📄 TXT formatda", callback_data="export_txt"),
        InlineKeyboardButton("📊 CSV formatda", callback_data="export_csv"),
    )
    await message.answer("📤 *Eksport formati:*", reply_markup=kb, parse_mode="Markdown")

@dp.callback_query_handler(lambda c: c.data in ["export_txt","export_csv"], state="*")
async def do_export(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    cursor.execute("""
        SELECT w.german, w.article, w.uzbek, c.name, w.level,
               w.correct_count, w.wrong_count
        FROM words w
        LEFT JOIN categories c ON w.category_id = c.id
        ORDER BY w.german
    """)
    words = cursor.fetchall()
    if not words:
        await call.answer("📭 So'z yo'q!", show_alert=True)
        return

    if call.data == "export_txt":
        lines = ["Deutsch Fenix — So'zlar ro'yxati", "=" * 40, ""]
        for german, article, uzbek, cat_name, level, correct, wrong in words:
            display = format_german(german, article)
            cat_str = f" [{cat_name}]" if cat_name else ""
            lvl_str = f" ({level})" if level and level != "custom" else ""
            lines.append(f"{display} - {uzbek}{cat_str}{lvl_str}  ✅{correct} ❌{wrong}")
        content = "\n".join(lines).encode("utf-8")
        bio     = io.BytesIO(content)
        bio.name = "sozlar.txt"
        await call.message.answer_document(bio, caption="📄 Barcha so'zlar (TXT)")

    else:  # CSV
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["German", "Article", "Uzbek", "Category", "Level", "Correct", "Wrong"])
        for german, article, uzbek, cat_name, level, correct, wrong in words:
            writer.writerow([german, article, uzbek, cat_name or "", level or "", correct, wrong])
        content = output.getvalue().encode("utf-8-sig")
        bio     = io.BytesIO(content)
        bio.name = "sozlar.csv"
        await call.message.answer_document(bio, caption="📊 Barcha so'zlar (CSV)")

# ======================
# SO'Z O'CHIRISH
# ======================
@dp.message_handler(commands=["ochir"], state="*")
async def delete_cmd(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer(
        "🗑 O'chirmoqchi bo'lgan so'zni yozing (german):\n"
        "Masalan: `Tisch` yoki `der Tisch`\n\nBekor qilish: /start",
        parse_mode="Markdown"
    )
    await DeleteState.waiting_word.set()

@dp.message_handler(state=DeleteState.waiting_word)
async def delete_word_handler(message: types.Message, state: FSMContext):
    if message.text.startswith("/"):
        await state.finish()
        return

    line    = message.text.strip()
    article, word = "", line
    for art in ["der ","die ","das "]:
        if line.lower().startswith(art):
            article = art.strip()
            word    = line[len(art):].strip().capitalize()
            break

    cursor.execute(
        "SELECT id, german, article, uzbek FROM words WHERE LOWER(german)=LOWER(?) AND LOWER(article)=LOWER(?)",
        (word, article)
    )
    row = cursor.fetchone()
    if not row:
        # Artikl bo'lmasa ham qidirish
        cursor.execute(
            "SELECT id, german, article, uzbek FROM words WHERE LOWER(german)=LOWER(?)",
            (word,)
        )
        row = cursor.fetchone()

    if not row:
        await message.answer(f"❌ *{line}* topilmadi.", parse_mode="Markdown")
        await state.finish()
        return

    wid, g, a, u = row
    display = format_german(g, a)
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Ha, o'chir",   callback_data=f"confirm_del_{wid}"),
        InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_del"),
    )
    await message.answer(
        f"🗑 *{display}* — {u}\n\nShu so'zni o'chirmoqchimisiz?",
        reply_markup=kb, parse_mode="Markdown"
    )
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("confirm_del_") or c.data == "cancel_del", state="*")
async def confirm_delete(call: types.CallbackQuery, state: FSMContext):
    if call.data == "cancel_del":
        await call.answer("Bekor qilindi.")
        await call.message.delete()
        return
    wid = int(call.data.split("_")[2])
    cursor.execute("DELETE FROM words WHERE id=?", (wid,))
    conn.commit()
    await call.answer("🗑 O'chirildi!", show_alert=True)
    await call.message.delete()

# Test paytida ham o'chirish tugmasi
@dp.callback_query_handler(lambda c: c.data.startswith("delete_"), state="*")
async def delete_from_test(call: types.CallbackQuery, state: FSMContext):
    wid = int(call.data.split("_")[1])
    cursor.execute("SELECT german, article, uzbek FROM words WHERE id=?", (wid,))
    row = cursor.fetchone()
    if not row:
        await call.answer("Topilmadi.", show_alert=True)
        return
    g, a, u = row
    display = format_german(g, a)
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Ha, o'chir",   callback_data=f"confirm_del_{wid}"),
        InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_del"),
    )
    await call.message.answer(
        f"🗑 *{display}* — {u}\n\nShu so'zni o'chirmoqchimisiz?",
        reply_markup=kb, parse_mode="Markdown"
    )
    await call.answer()

# ======================
# KUNLIK ESLATMA SOZLASH
# ======================
@dp.callback_query_handler(lambda c: c.data == "reminder_change", state="*")
async def reminder_change(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await call.message.answer(
        "⏰ *Eslatma vaqtini o'zgartirish*\n\n"
        "Soatni kiriting (masalan: `20:00` yoki `09:30`):\n\n"
        "O'chirish uchun: `off`\n"
        "Bekor qilish: /start",
        parse_mode="Markdown"
    )
    await ReminderState.waiting_time.set()

@dp.message_handler(commands=["eslatma"], state="*")
async def reminder_cmd(message: types.Message, state: FSMContext):
    await state.finish()
    settings = get_user_settings(message.from_user.id)
    status   = "✅ Yoqilgan" if settings["active"] else "❌ O'chirilgan"
    await message.answer(
        f"⏰ *Eslatma sozlamalari*\n\n"
        f"Vaqt: *{settings['hour']:02d}:{settings['minute']:02d}*\n"
        f"Holat: *{status}*\n\n"
        f"O'zgartirish uchun: vaqtni yuboring (masalan `20:00`)\n"
        f"O'chirish uchun: `off`",
        parse_mode="Markdown"
    )
    await ReminderState.waiting_time.set()

@dp.message_handler(state=ReminderState.waiting_time)
async def save_reminder_time(message: types.Message, state: FSMContext):
    text = message.text.strip().lower()
    if text.startswith("/"):
        await state.finish()
        return

    if text == "off":
        save_user_settings(message.from_user.id, 20, 0, active=0)
        await message.answer("🔕 Eslatma o'chirildi.")
        await state.finish()
        return

    try:
        parts  = text.split(":")
        hour   = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
    except (ValueError, IndexError):
        await message.answer("❌ Format noto'g'ri. Masalan: `20:00` yoki `9:30`", parse_mode="Markdown")
        return

    save_user_settings(message.from_user.id, hour, minute, active=1)
    await message.answer(
        f"✅ Eslatma *{hour:02d}:{minute:02d}* ga sozlandi!\n"
        f"Har kuni o'sha vaqtda xabar olasiz 🔔",
        parse_mode="Markdown"
    )
    await state.finish()

# ======================
# QUIZ: yozma javob
# ======================
@dp.message_handler(state=QuizState.waiting_for_answer)
async def handle_quiz_answer(message: types.Message, state: FSMContext):
    if message.text.startswith('/'):
        await state.finish()
        await dp.process_update(types.Update(update_id=0, message=message))
        return

    data         = await state.get_data()
    mode         = data.get("mode", "de_uz")
    uzbek        = data.get("uzbek", "")
    german       = data.get("german", "")
    article      = data.get("article", "")
    display_g    = data.get("display_german", "")
    word_id      = data.get("word_id")
    category_id  = data.get("category_id")
    hard_mode    = data.get("hard_mode", False)
    user_input   = message.text.strip()

    if mode == "de_uz":
        is_correct       = simplify_uz(user_input) == simplify_uz(uzbek)
        correct_display  = uzbek
        question_display = display_g
    else:
        u_norm          = user_input.lower()
        full_correct    = format_german(german, article).lower()
        is_correct      = u_norm == full_correct or u_norm == german.lower()
        correct_display  = format_german(german, article)
        question_display = uzbek

    if is_correct:
        cursor.execute("UPDATE words SET correct_count = correct_count + 1 WHERE id=?", (word_id,))
        conn.commit()
        log_stat(True)
        sm2_update(word_id, quality=4)
        await message.reply(
            f"✨ *BARAKALLA!*\n\n_{question_display}_ = *{correct_display}*",
            parse_mode="Markdown"
        )
        if hard_mode:
            await hard_test(message, state)
        else:
            await send_test_question(message, state, category_id=category_id)
    else:
        cursor.execute("UPDATE words SET wrong_count = wrong_count + 1 WHERE id=?", (word_id,))
        conn.commit()
        log_stat(False)
        sm2_update(word_id, quality=1)
        next_cb = "hard_next" if hard_mode else "next"
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("⏭ Keyingisi",  callback_data=next_cb),
            InlineKeyboardButton("✅ Yod oldim",  callback_data=f"archive_{word_id}"),
        )
        await message.answer(
            f"❌ *Xato!*\n\n"
            f"To'g'ri javob: `{correct_display}`",
            reply_markup=kb, parse_mode="Markdown"
        )

# ======================
# QUIZ: artikl testi
# ======================
@dp.callback_query_handler(lambda c: c.data.startswith("art_"), state=QuizState.waiting_for_article)
async def handle_article_answer(call: types.CallbackQuery, state: FSMContext):
    chosen  = call.data.split("_")[1]
    data    = await state.get_data()
    correct = data.get("article", "").lower()
    german  = data.get("german", "")
    uzbek   = data.get("uzbek", "")
    word_id = data.get("word_id")
    cat_id  = data.get("category_id")

    await call.message.delete()

    if chosen == correct:
        cursor.execute("UPDATE words SET correct_count = correct_count + 1 WHERE id=?", (word_id,))
        conn.commit()
        log_stat(True)
        sm2_update(word_id, quality=4)
        await call.message.answer(
            f"✨ *BARAKALLA!*\n_{chosen} {german}_ = {uzbek}",
            parse_mode="Markdown"
        )
        await send_test_question(call.message, state, category_id=cat_id)
    else:
        cursor.execute("UPDATE words SET wrong_count = wrong_count + 1 WHERE id=?", (word_id,))
        conn.commit()
        log_stat(False)
        sm2_update(word_id, quality=1)
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("⏭ Keyingisi", callback_data="next"),
            InlineKeyboardButton("✅ Yod oldim", callback_data=f"archive_{word_id}"),
        )
        await call.message.answer(
            f"❌ *Xato!* Sen: `{chosen}`\n\nTo'g'ri: `{correct} {german}` = {uzbek}",
            reply_markup=kb, parse_mode="Markdown"
        )

# ======================
# /vaqt — vaqtni tekshirish
# ======================
@dp.message_handler(commands=["vaqt"], state="*")
async def show_time(message: types.Message, state: FSMContext):
    await state.finish()
    now = datetime.now(TZ)
    await message.answer(
        f"🕐 *Bot vaqti (Toshkent):* `{now.strftime('%H:%M')}`\n"
        f"📅 Sana: `{now.strftime('%Y-%m-%d')}`",
        parse_mode="Markdown"
    )

# ======================
# AUDIO TALAFFUZ
# ======================
@dp.callback_query_handler(lambda c: c.data.startswith("audio_"), state="*")
async def send_audio(call: types.CallbackQuery, state: FSMContext):
    await call.answer("🔊 Tayyorlanmoqda...")
    word_id = int(call.data.split("_")[1])
    cursor.execute("SELECT german, article FROM words WHERE id=?", (word_id,))
    row = cursor.fetchone()
    if not row:
        await call.answer("❌ Topilmadi.", show_alert=True)
        return
    german, article = row
    text = f"{article} {german}".strip() if article else german
    try:
        tts = gTTS(text=text, lang="de", slow=False)
        buf = io.BytesIO()
        tts.write_to_fp(buf)
        buf.seek(0)
        buf.name = "audio.mp3"
        await call.message.answer_voice(buf, caption=f"🔊 *{text}*", parse_mode="Markdown")
    except Exception as e:
        logging.error(f"gTTS xato: {e}")
        await call.message.answer("❌ Audio yuborishda xatolik. Internet bor-yo'qligini tekshiring.")

# ======================
# SO'Z QO'SHISH (umumiy — state=None)
# ======================
@dp.message_handler(state=None)
async def bulk_add(message: types.Message):
    """
    Oddiy so'z qo'shish va "Kategoriya:" formatini qo'llab-quvvatlaydi.
    Agar matn ichida "NomKategoriya:" satri bo'lsa — keyingi so'zlar
    o'sha kategoriyaga ham qo'shiladi.
    """
    lines = message.text.strip().split('\n')

    # Avval "Kategoriya:" formati borligini aniqlash
    has_cat_format = any(
        l.strip().endswith(":") and not _looks_like_word_line(l.strip())
        for l in lines if l.strip()
    )

    if has_cat_format:
        await _bulk_add_with_categories(message, lines, parent_id=None)
    else:
        await _bulk_add_simple(message, lines, category_id=None)


def _looks_like_word_line(line):
    """'–' yoki ' - ' bo'lsa — so'z satri"""
    return " – " in line or " - " in line or "–" in line


async def _bulk_add_simple(message, lines, category_id):
    """Kategoriyasiz yoki bitta kategoriyaga oddiy so'z qo'shish"""
    added, duplicates, errors = 0, [], []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        parsed = parse_word_line(line)
        if not parsed:
            errors.append(line)
            continue
        article, german, uzbek = parsed
        cursor.execute(
            "SELECT id FROM words WHERE LOWER(german)=LOWER(?) AND LOWER(article)=LOWER(?)",
            (german, article)
        )
        existing = cursor.fetchone()
        if existing:
            if category_id:
                cursor.execute("UPDATE words SET category_id=? WHERE id=?", (category_id, existing[0]))
            duplicates.append(format_german(german, article))
        else:
            cursor.execute(
                "INSERT INTO words (german, uzbek, article, category_id) VALUES (?, ?, ?, ?)",
                (german, uzbek, article, category_id)
            )
            added += 1
    if added > 0 or duplicates:
        conn.commit()
    parts = []
    if added:
        parts.append(f"✅ *{added} ta so'z qo'shildi!*")
    if duplicates:
        parts.append(f"⚠️ Allaqachon bor: {', '.join(f'`{w}`' for w in duplicates[:5])}")
    if errors:
        parts.append(f"❓ Format noto'g'ri: {len(errors)} ta\n_Format: `der Tisch - stol`_")
    if not parts:
        parts.append("❓ Hech narsa qo'shilmadi.\n_Format: `der Tisch - stol`_")
    await message.reply("\n".join(parts), parse_mode="Markdown")


async def _bulk_add_with_categories(message, lines, parent_id):
    """
    'Kategoriya:' + so'zlar formatini parse qilish.
    parent_id — ota-kategoriya (None = root, int = sub-kategoriya ichida).
    """
    current_cat_id = None
    current_cat_name = None
    total_added = 0
    total_dup   = 0
    cats_created = []
    errors = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Kategoriya sarlavhasi: oxiri ':' bilan tugaydi va so'z satri emas
        if line.endswith(":") and not _looks_like_word_line(line):
            cat_name = line[:-1].strip()
            if not cat_name:
                continue
            # Kategoriyani topish yoki yaratish
            if parent_id is not None:
                cursor.execute(
                    "SELECT id FROM categories WHERE LOWER(name)=LOWER(?) AND parent_id=?",
                    (cat_name, parent_id)
                )
            else:
                cursor.execute(
                    "SELECT id FROM categories WHERE LOWER(name)=LOWER(?) AND parent_id IS NULL",
                    (cat_name,)
                )
            row = cursor.fetchone()
            if row:
                current_cat_id = row[0]
            else:
                cursor.execute(
                    "INSERT INTO categories (name, parent_id) VALUES (?, ?)",
                    (cat_name, parent_id)
                )
                conn.commit()
                current_cat_id = cursor.lastrowid
                cats_created.append(cat_name)
            current_cat_name = cat_name
            continue

        # So'z satri
        parsed = parse_word_line(line)
        if not parsed:
            errors.append(line)
            continue
        article, german, uzbek = parsed
        cursor.execute(
            "SELECT id FROM words WHERE LOWER(german)=LOWER(?) AND LOWER(article)=LOWER(?)",
            (german, article)
        )
        existing = cursor.fetchone()
        if existing:
            if current_cat_id:
                cursor.execute("UPDATE words SET category_id=? WHERE id=?", (current_cat_id, existing[0]))
            total_dup += 1
        else:
            cursor.execute(
                "INSERT INTO words (german, uzbek, article, category_id) VALUES (?, ?, ?, ?)",
                (german, uzbek, article, current_cat_id)
            )
            total_added += 1

    conn.commit()

    parts = []
    if cats_created:
        parts.append(f"📁 Yaratildi: *{', '.join(cats_created)}*")
    if total_added:
        parts.append(f"✅ *{total_added} ta so'z* qo'shildi!")
    if total_dup:
        parts.append(f"🔗 Mavjud so'zlar kategoriyaga biriktirildi: *{total_dup} ta*")
    if errors:
        parts.append(f"❓ Format noto'g'ri: {len(errors)} ta")
    if not parts:
        parts.append("❓ Hech narsa qo'shilmadi.")
    await message.reply("\n".join(parts), parse_mode="Markdown")


# ======================
# CALLBACK HANDLER (umumiy)
# ======================
@dp.callback_query_handler(state="*")
async def process_callback(call: types.CallbackQuery, state: FSMContext):
    d       = call.data
    st_data = await state.get_data()
    cat_id  = st_data.get("category_id")

    if d == "noop":
        await call.answer()

    elif d == "next":
        await call.message.delete()
        await send_test_question(call.message, state, category_id=cat_id)

    elif d == "next_ids":
        word_ids = st_data.get("test_word_ids")
        await call.message.delete()
        if word_ids:
            await send_test_question_from_ids(call.message, state, word_ids)
        else:
            await send_test_question(call.message, state, category_id=cat_id)

    elif d == "hard_next":
        await call.message.delete()
        await hard_test(call.message, state)

    elif d.startswith("archive_"):
        word_id = d.split("_")[1]
        cursor.execute("UPDATE words SET archived=1 WHERE id=?", (word_id,))
        conn.commit()
        await call.answer("📦 Arxivlandi!")
        await call.message.delete()
        await send_test_question(call.message, state, category_id=cat_id)

    elif d == "unarchive_all":
        cursor.execute("UPDATE words SET archived=0 WHERE archived=1")
        count = cursor.rowcount
        conn.commit()
        await call.answer(f"♻️ {count} ta so'z qaytarildi!", show_alert=True)
        await call.message.delete()

    elif d.startswith("show_"):
        mode   = st_data.get("mode", "de_uz")
        answer = st_data.get("display_german") if mode == "uz_de" else st_data.get("uzbek")
        await call.answer(f"💡 Javob: {answer}" if answer else "⚠️ Ma'lumot topilmadi.", show_alert=True)

    # --- Kategoriya ---
    elif d in ("cat_new_root",) or d.startswith("cat_new_"):
        # parent_id aniqlash
        if d == "cat_new_root":
            p_id = None
            p_label = "root"
        else:
            p_id = int(d.split("_")[2])
            p_label = str(p_id)
        await state.update_data(new_cat_parent_id=p_id)
        await call.message.answer(
            "📁 Yangi kategoriya nomini yozing:\n\n"
            "Bekor qilish: /start"
        )
        await CategoryState.waiting_name.set()

    elif d == "cat_new":
        # Eski mos kelish (orqaga muvofiqlik)
        await state.update_data(new_cat_parent_id=None)
        await call.message.answer("📁 Yangi kategoriya nomini yozing:\n\nBekor qilish: /start")
        await CategoryState.waiting_name.set()

    elif d.startswith("cat_open_"):
        cid = int(d.split("_")[2])
        cursor.execute("SELECT name, parent_id FROM categories WHERE id=?", (cid,))
        row = cursor.fetchone()
        if not row:
            await call.answer("Topilmadi.", show_alert=True)
            return
        cat_name, cat_parent = row[0], row[1]

        # Sub-kategoriyalar bor?
        cursor.execute("SELECT id, name FROM categories WHERE parent_id=? ORDER BY name", (cid,))
        subcats = cursor.fetchall()

        direct_cnt  = _count_words_recursive(cid)
        sub_cnt     = len(subcats)

        kb = InlineKeyboardMarkup(row_width=1)

        # Orqaga tugmasi
        if cat_parent is not None:
            kb.add(InlineKeyboardButton("◀️ Orqaga", callback_data=f"cat_open_{cat_parent}"))
        else:
            kb.add(InlineKeyboardButton("◀️ Kategoriyalar", callback_data="menu_cats"))

        # Test — sub-kategoriya bor bo'lsa tanlov so'raydi
        kb.add(InlineKeyboardButton("🎯 Test boshlash", callback_data=f"cat_test_choose_{cid}"))
        kb.add(InlineKeyboardButton("📋 So'zlarini ko'rish", callback_data=f"cat_list_{cid}"))

        # Sub-kategoriyalarni ko'rsatish
        if subcats:
            kb.add(InlineKeyboardButton("─── Bo'limlar ───", callback_data="noop"))
            for sc_id, sc_name in subcats:
                sc_cnt = _count_words_recursive(sc_id)
                kb.add(InlineKeyboardButton(
                    f"  📂 {sc_name} ({sc_cnt} so'z)",
                    callback_data=f"cat_open_{sc_id}"
                ))

        kb.add(InlineKeyboardButton("─── Boshqarish ───", callback_data="noop"))
        kb.add(InlineKeyboardButton("➕ So'z qo'shish",           callback_data=f"cat_addword_{cid}"))
        kb.add(InlineKeyboardButton("📁 Yangi bo'lim qo'shish",   callback_data=f"cat_new_{cid}"))
        kb.add(InlineKeyboardButton("🔗 Mavjud so'z biriktirish", callback_data=f"cat_assign_{cid}"))
        kb.add(InlineKeyboardButton("🗑 Kategoriyani o'chirish",  callback_data=f"cat_del_{cid}"))

        title = (
            f"📁 *{cat_name}*\n"
            f"_{direct_cnt} so'z"
            + (f", {sub_cnt} bo'lim" if sub_cnt else "")
            + "_"
        )
        try:
            await call.message.edit_text(title, reply_markup=kb, parse_mode="Markdown")
        except Exception:
            await call.message.answer(title, reply_markup=kb, parse_mode="Markdown")

    elif d.startswith("cat_test_choose_"):
        cid = int(d.split("_")[3])
        cursor.execute("SELECT name FROM categories WHERE id=?", (cid,))
        row = cursor.fetchone()
        if not row:
            await call.answer("Topilmadi.", show_alert=True)
            return
        cat_name = row[0]

        cursor.execute("SELECT id, name FROM categories WHERE parent_id=? ORDER BY name", (cid,))
        subcats = cursor.fetchall()

        if not subcats:
            # Sub-kategoriya yo'q — to'g'ridan test
            await state.update_data(active_category=cid, test_word_ids=None)
            await call.message.delete()
            await send_test_question(call.message, state, category_id=cid)
            return

        # Tanlov klaviaturasi
        kb = InlineKeyboardMarkup(row_width=1)
        all_ids = _get_all_word_ids_recursive(cid)
        kb.add(InlineKeyboardButton(
            f"📚 Umumiy ({len(all_ids)} so'z)",
            callback_data=f"cat_test_all_{cid}"
        ))
        for sc_id, sc_name in subcats:
            sc_cnt = _count_words_recursive(sc_id)
            kb.add(InlineKeyboardButton(
                f"📂 {sc_name} ({sc_cnt} so'z)",
                callback_data=f"cat_test_{sc_id}"
            ))
        kb.add(InlineKeyboardButton("◀️ Orqaga", callback_data=f"cat_open_{cid}"))

        try:
            await call.message.edit_text(
                f"🎯 *{cat_name}* — Qaysi mavzudan test?",
                reply_markup=kb, parse_mode="Markdown"
            )
        except Exception:
            await call.message.answer(
                f"🎯 *{cat_name}* — Qaysi mavzudan test?",
                reply_markup=kb, parse_mode="Markdown"
            )

    elif d.startswith("cat_test_all_"):
        # Ota-kategoriya + barcha sub-kategoriyalar so'zlari
        cid = int(d.split("_")[3])
        word_ids = _get_all_word_ids_recursive(cid)
        if not word_ids:
            await call.answer("Bu kategoriyada so'z yo'q!", show_alert=True)
            return
        await state.update_data(active_category=cid, test_word_ids=word_ids)
        await call.message.delete()
        await send_test_question_from_ids(call.message, state, word_ids)



    elif d.startswith("cat_addword_"):
        cid = int(d.split("_")[2])
        cursor.execute("SELECT name FROM categories WHERE id=?", (cid,))
        row = cursor.fetchone()
        if not row:
            await call.answer("Topilmadi.", show_alert=True)
            return
        cat_name = row[0]

        # Kategoriyaga hali biriktirilmagan mavjud so'zlar sonini ko'rsatamiz
        cursor.execute(
            "SELECT COUNT(*) FROM words WHERE (category_id IS NULL OR category_id != ?) AND archived=0",
            (cid,)
        )
        free_cnt = cursor.fetchone()[0]

        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton(
                f"📚 Mavjud so'zlardan qo'shish ({free_cnt} ta)",
                callback_data=f"cat_fromexisting_{cid}"
            ),
            InlineKeyboardButton(
                "✏️ Yangi so'z qo'shish",
                callback_data=f"cat_newword_{cid}"
            ),
            InlineKeyboardButton("◀️ Orqaga", callback_data=f"cat_open_{cid}"),
        )
        await call.message.edit_text(
            f"📁 *{cat_name}* — So'z qo'shish\n\n"
            "Qanday qo'shmoqchisiz?",
            reply_markup=kb, parse_mode="Markdown"
        )

    elif d.startswith("cat_fromexisting_"):
        cid = int(d.split("_")[2])
        cursor.execute("SELECT name FROM categories WHERE id=?", (cid,))
        row = cursor.fetchone()
        if not row:
            await call.answer("Topilmadi.", show_alert=True)
            return
        cat_name = row[0]

        # Bu kategoriyada bo'lmagan so'zlarni ko'rsatish (max 30 ta)
        cursor.execute(
            """SELECT id, german, article, uzbek FROM words
               WHERE (category_id IS NULL OR category_id != ?) AND archived=0
               ORDER BY german LIMIT 30""",
            (cid,)
        )
        words = cursor.fetchall()
        if not words:
            await call.answer("Birikmagan so'z topilmadi!", show_alert=True)
            return

        kb = InlineKeyboardMarkup(row_width=1)
        for wid, german, article, uzbek in words:
            display = format_german(german, article)
            kb.add(InlineKeyboardButton(
                f"➕ {display} — {uzbek}",
                callback_data=f"cat_pick_{cid}_{wid}"
            ))
        kb.add(InlineKeyboardButton("◀️ Orqaga", callback_data=f"cat_addword_{cid}"))

        await call.message.edit_text(
            f"📚 *{cat_name}* ga qo'shish\n\n"
            "Qaysi so'zni qo'shmoqchisiz?",
            reply_markup=kb, parse_mode="Markdown"
        )

    elif d.startswith("cat_pick_"):
        parts = d.split("_")
        cid  = int(parts[2])
        wid  = int(parts[3])
        cursor.execute("UPDATE words SET category_id=? WHERE id=?", (cid, wid))
        conn.commit()
        cursor.execute("SELECT german, article FROM words WHERE id=?", (wid,))
        w = cursor.fetchone()
        cursor.execute("SELECT name FROM categories WHERE id=?", (cid,))
        cat_row = cursor.fetchone()
        if w and cat_row:
            display = format_german(w[0], w[1])
            await call.answer(f"✅ '{display}' qo'shildi!", show_alert=False)
        # Ro'yxatni yangilaymiz
        cursor.execute(
            """SELECT id, german, article, uzbek FROM words
               WHERE (category_id IS NULL OR category_id != ?) AND archived=0
               ORDER BY german LIMIT 30""",
            (cid,)
        )
        words = cursor.fetchall()
        cursor.execute("SELECT name FROM categories WHERE id=?", (cid,))
        cat_name = cursor.fetchone()[0]

        if not words:
            kb2 = InlineKeyboardMarkup()
            kb2.add(InlineKeyboardButton("◀️ Kategoriyaga qaytish", callback_data=f"cat_open_{cid}"))
            await call.message.edit_text(
                f"✅ Barcha mavjud so'zlar *{cat_name}* ga qo'shildi!",
                reply_markup=kb2, parse_mode="Markdown"
            )
            return

        kb2 = InlineKeyboardMarkup(row_width=1)
        for wid2, german, article, uzbek in words:
            display = format_german(german, article)
            kb2.add(InlineKeyboardButton(
                f"➕ {display} — {uzbek}",
                callback_data=f"cat_pick_{cid}_{wid2}"
            ))
        kb2.add(InlineKeyboardButton("◀️ Orqaga", callback_data=f"cat_addword_{cid}"))
        await call.message.edit_text(
            f"📚 *{cat_name}* ga qo'shish\n\nQaysi so'zni qo'shmoqchisiz?",
            reply_markup=kb2, parse_mode="Markdown"
        )

    elif d.startswith("cat_newword_"):
        cid = int(d.split("_")[2])
        cursor.execute("SELECT name FROM categories WHERE id=?", (cid,))
        row = cursor.fetchone()
        if not row:
            await call.answer("Topilmadi.", show_alert=True)
            return
        await state.update_data(target_cat_id=cid)
        await call.message.answer(
            f"✏️ *{row[0]}* kategoriyasiga yangi so'z qo'shish\n\n"
            "So'zlarni quyidagi formatda yozing:\n"
            "`der Tisch - stol`\n"
            "`laufen - yugurmoq`\n"
            "_(Bir nechta qator ham bo'ladi)_\n\n"
            "Bekor qilish: /start",
            parse_mode="Markdown"
        )
        await CategoryState.waiting_new_word.set()


    elif d.startswith("cat_test_"):
        cid = int(d.split("_")[2])
        await state.update_data(active_category=cid)
        await call.message.delete()
        await send_test_question(call.message, state, category_id=cid)

    elif d.startswith("cat_list_"):
        cid = int(d.split("_")[2])
        cursor.execute("SELECT name FROM categories WHERE id=?", (cid,))
        cat_name = cursor.fetchone()[0]
        cursor.execute("SELECT german, article, uzbek FROM words WHERE category_id=? AND archived=0", (cid,))
        words = cursor.fetchall()
        if not words:
            await call.answer(f"{cat_name} da so'z yo'q.", show_alert=True)
            return
        text = f"📁 *{cat_name}:*\n\n"
        for g, a, u in words:
            text += f"• *{format_german(g, a)}* — {u}\n"
        await call.message.answer(text, parse_mode="Markdown")

    elif d.startswith("cat_assign_"):
        cid = int(d.split("_")[2])
        cursor.execute("SELECT name FROM categories WHERE id=?", (cid,))
        cat_name = cursor.fetchone()[0]
        await state.update_data(assign_cat_id=cid)
        await call.message.answer(
            f"📌 *{cat_name}* ga so'zlarni biriktirish\n\n"
            "So'zlarni qatorma-qator yozing:\n"
            "`der Tisch`\n`laufen`\n\nBekor qilish: /start",
            parse_mode="Markdown"
        )
        await CategoryState.waiting_assign_word.set()

    elif d.startswith("cat_del_"):
        cid = int(d.split("_")[2])
        cursor.execute("SELECT name FROM categories WHERE id=?", (cid,))
        row = cursor.fetchone()
        if row:
            cursor.execute("UPDATE words SET category_id=NULL WHERE category_id=?", (cid,))
            cursor.execute("DELETE FROM categories WHERE id=?", (cid,))
            conn.commit()
            await call.answer(f"🗑 '{row[0]}' o'chirildi.", show_alert=True)
        await call.message.delete()

    else:
        await call.answer()

# ======================
# KATEGORIYA STATELARI
# ======================
@dp.message_handler(state=CategoryState.waiting_name)
async def cat_save_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    if not name or message.text.startswith("/"):
        await state.finish()
        return
    data      = await state.get_data()
    parent_id = data.get("new_cat_parent_id")  # None = root

    try:
        cursor.execute(
            "INSERT INTO categories (name, parent_id) VALUES (?, ?)",
            (name, parent_id)
        )
        conn.commit()
        new_id = cursor.lastrowid
        if parent_id is not None:
            cursor.execute("SELECT name FROM categories WHERE id=?", (parent_id,))
            pname = cursor.fetchone()
            pname = pname[0] if pname else "kategoriya"
            await message.answer(
                f"✅ *{name}* bo'limi *{pname}* ichiga yaratildi!",
                parse_mode="Markdown"
            )
        else:
            await message.answer(f"✅ *{name}* kategoriyasi yaratildi!", parse_mode="Markdown")
    except sqlite3.IntegrityError:
        await message.answer(f"⚠️ *{name}* allaqachon mavjud.", parse_mode="Markdown")
    await state.finish()

@dp.message_handler(state=CategoryState.waiting_assign_word)
async def cat_assign_words(message: types.Message, state: FSMContext):
    data  = await state.get_data()
    cid   = data.get("assign_cat_id")
    lines = message.text.strip().split('\n')
    found, not_found = 0, []

    for line in lines:
        line = line.strip()
        if not line:
            continue
        article, word = "", line
        for art in ["der ","die ","das "]:
            if line.lower().startswith(art):
                article = art.strip()
                word    = line[len(art):].strip().capitalize()
                break
        cursor.execute(
            "SELECT id FROM words WHERE LOWER(german)=LOWER(?) AND LOWER(article)=LOWER(?)",
            (word, article)
        )
        row = cursor.fetchone()
        if row:
            cursor.execute("UPDATE words SET category_id=? WHERE id=?", (cid, row[0]))
            found += 1
        else:
            not_found.append(line)

    conn.commit()
    await state.finish()

    parts = []
    if found:
        parts.append(f"✅ *{found} ta so'z* biriktrildi!")
    if not_found:
        parts.append(f"⚠️ Topilmadi: {', '.join(f'`{w}`' for w in not_found[:5])}")
    await message.answer("\n".join(parts) or "Hech narsa topilmadi.", parse_mode="Markdown")


@dp.message_handler(state=CategoryState.waiting_new_word)
async def cat_new_word_handler(message: types.Message, state: FSMContext):
    """Kategoriyaga to'g'ridan-to'g'ri yangi so'z qo'shish.
    'Bo'lim:' formatini ham qo'llab-quvvatlaydi — sub-kategoriyalar yaratadi."""
    if message.text.startswith("/"):
        await state.finish()
        return

    data    = await state.get_data()
    cid     = data.get("target_cat_id")
    lines   = message.text.strip().split('\n')

    # "Bo'lim:" formati bormi?
    has_subcat_format = any(
        l.strip().endswith(":") and not _looks_like_word_line(l.strip())
        for l in lines if l.strip()
    )

    if has_subcat_format:
        # Sub-kategoriya sifatida qo'shish (parent_id = cid)
        await _bulk_add_with_categories(message, lines, parent_id=cid)
    else:
        # Oddiy so'zlar — to'g'ridan cid ga
        added, duplicates, errors = 0, [], []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            parsed = parse_word_line(line)
            if not parsed:
                errors.append(line)
                continue
            article, german, uzbek = parsed
            cursor.execute(
                "SELECT id FROM words WHERE LOWER(german)=LOWER(?) AND LOWER(article)=LOWER(?)",
                (german, article)
            )
            existing = cursor.fetchone()
            if existing:
                cursor.execute("UPDATE words SET category_id=? WHERE id=?", (cid, existing[0]))
                duplicates.append(format_german(german, article))
            else:
                cursor.execute(
                    "INSERT INTO words (german, uzbek, article, category_id) VALUES (?, ?, ?, ?)",
                    (german, uzbek, article, cid)
                )
                added += 1
        if added > 0 or duplicates:
            conn.commit()

        cursor.execute("SELECT name FROM categories WHERE id=?", (cid,))
        cat_row = cursor.fetchone()
        cat_name = cat_row[0] if cat_row else "Kategoriya"

        parts = []
        if added:
            parts.append(f"✅ *{added} ta yangi so'z* qo'shildi → *{cat_name}*")
        if duplicates:
            parts.append(f"🔗 Mavjud, biriktirildi: {', '.join(f'`{w}`' for w in duplicates[:5])}")
        if errors:
            parts.append(f"❓ Format noto'g'ri ({len(errors)} ta):\n_Format: `der Tisch - stol`_")
        if not parts:
            parts.append("❓ Hech narsa qo'shilmadi.\n_Format: `der Tisch - stol`_")

        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("➕ Yana so'z qo'shish",      callback_data=f"cat_newword_{cid}"),
            InlineKeyboardButton("◀️ Kategoriyaga qaytish",  callback_data=f"cat_open_{cid}"),
        )
        await message.reply("\n".join(parts), reply_markup=kb, parse_mode="Markdown")

    await state.finish()

# ======================
# MAIN
# ======================
async def on_startup(dp):
    asyncio.create_task(daily_reminder_loop())
    logging.info("Bot ishga tushdi ✅")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
