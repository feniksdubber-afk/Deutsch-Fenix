import os
import io
import csv
import json
import sqlite3
import logging
import random
import asyncio
from datetime import date, datetime, timedelta
import pytz
import httpx
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from gtts import gTTS
from dotenv import load_dotenv

TZ = pytz.timezone("Asia/Tashkent")

logging.basicConfig(level=logging.INFO)
load_dotenv()
TOKEN     = os.getenv("BOT_TOKEN")
ADMIN_ID  = os.getenv("ADMIN_ID")
DEEPL_KEY = os.getenv("DEEPL_KEY", "622ac6d3-2e74-42fe-ba8c-a3bee328673e:fx")

bot     = Bot(token=TOKEN)
storage = MemoryStorage()
dp      = Dispatcher(bot, storage=storage)

# ======================
# TILLAR / I18N
# ======================
TEXTS = {
    "uz": {
        "start_greeting": "🇩🇪 *Deutsch Fenix v5.0*\n\n📥 *So'z qo'shish:*\n`der Tisch - stol`\n`laufen - yugurmoq`\n_(bir nechta qator ham bo'ladi)_\n\n⬇️ Yoki quyidagi tugmalardan foydalaning:",
        "test_start": "🎯 Test boshlash",
        "hard_words": "💀 Qiyin so'zlar",
        "stats": "📊 Statistika",
        "word_list": "📋 So'zlar ro'yxati",
        "categories": "📁 Kategoriyalar",
        "b1_level": "🌐 B1 darajasi",
        "export": "📤 Eksport",
        "reminder": "⏰ Eslatma vaqti",
        "delete_word": "🗑 So'z o'chirish",
        "archive": "📦 Arxiv",
        "translate": "🔍 Tarjimasini bil",
        "make_sentence": "📝 Gap tuzish",
        "flashcard": "🃏 Flashcard",
        "session": "🔁 Sessiya",
        "profile": "👤 Profil",
        "grammar_check": "✏️ Grammatika tekshirish",
        "chat_mode": "💬 Suhbat rejimi",
        "proverbs": "📜 Maqollar",
        "error_notebook": "📒 Xato daftari",
        "correct": "✨ BARAKALLA!",
        "wrong": "❌ Xato!",
        "next": "⏭ Keyingisi",
        "show_answer": "💡 Ko'rsatish",
        "pronounce": "🔊 Talaffuz",
        "memorized": "✅ Yod oldim",
        "home": "🏠 Menyu",
        "freeze": "❄️ Muzlatish",
        "de_uz_q": "🇩🇪 ➜ 🇺🇿  *Tarjimasi nima?*",
        "uz_de_q": "🇺🇿 ➜ 🇩🇪  *Nemischasi nima?*",
        "article_q": "🎯  *Artiklni toping:*",
        "no_words": "⚠️ So'zlar yo'q.",
        "archived_msg": "📦 Arxivlandi!",
        "session_complete": "🎉 Sessiya tugadi!\n\n✅ To'g'ri: *{correct}*\n❌ Xato: *{wrong}*\n🎯 Aniqlik: *{acc}%*",
        "goal_progress": "🎯 Kunlik maqsad: *{done}/{goal}* so'z",
        "streak": "🔥 Streak",
        "language_select": "🌐 Interfeys tilini tanlang:",
    },
    "ru": {
        "start_greeting": "🇩🇪 *Deutsch Fenix v5.0*\n\n📥 *Добавить слово:*\n`der Tisch - стол`\n`laufen - бежать`\n_(можно несколько строк)_\n\n⬇️ Или используйте кнопки ниже:",
        "test_start": "🎯 Начать тест",
        "hard_words": "💀 Сложные слова",
        "stats": "📊 Статистика",
        "word_list": "📋 Список слов",
        "categories": "📁 Категории",
        "b1_level": "🌐 Уровень B1",
        "export": "📤 Экспорт",
        "reminder": "⏰ Время напоминания",
        "delete_word": "🗑 Удалить слово",
        "archive": "📦 Архив",
        "translate": "🔍 Узнать перевод",
        "make_sentence": "📝 Составить предложение",
        "flashcard": "🃏 Флэшкарты",
        "session": "🔁 Сессия",
        "profile": "👤 Профиль",
        "grammar_check": "✏️ Проверка грамматики",
        "chat_mode": "💬 Режим разговора",
        "proverbs": "📜 Поговорки",
        "error_notebook": "📒 Тетрадь ошибок",
        "correct": "✨ ОТЛИЧНО!",
        "wrong": "❌ Ошибка!",
        "next": "⏭ Следующий",
        "show_answer": "💡 Показать",
        "pronounce": "🔊 Произношение",
        "memorized": "✅ Запомнил",
        "home": "🏠 Меню",
        "freeze": "❄️ Заморозить",
        "de_uz_q": "🇩🇪 ➜ 🇷🇺  *Перевод?*",
        "uz_de_q": "🇷🇺 ➜ 🇩🇪  *По-немецки?*",
        "article_q": "🎯  *Найдите артикль:*",
        "no_words": "⚠️ Нет слов.",
        "archived_msg": "📦 Архивировано!",
        "session_complete": "🎉 Сессия завершена!\n\n✅ Правильно: *{correct}*\n❌ Ошибок: *{wrong}*\n🎯 Точность: *{acc}%*",
        "goal_progress": "🎯 Дневная цель: *{done}/{goal}* слов",
        "streak": "🔥 Серия",
        "language_select": "🌐 Выберите язык интерфейса:",
    },
    "en": {
        "start_greeting": "🇩🇪 *Deutsch Fenix v5.0*\n\n📥 *Add a word:*\n`der Tisch - table`\n`laufen - to run`\n_(multiple lines supported)_\n\n⬇️ Or use the buttons below:",
        "test_start": "🎯 Start Test",
        "hard_words": "💀 Hard Words",
        "stats": "📊 Statistics",
        "word_list": "📋 Word List",
        "categories": "📁 Categories",
        "b1_level": "🌐 B1 Level",
        "export": "📤 Export",
        "reminder": "⏰ Reminder Time",
        "delete_word": "🗑 Delete Word",
        "archive": "📦 Archive",
        "translate": "🔍 Get Translation",
        "make_sentence": "📝 Make Sentence",
        "flashcard": "🃏 Flashcards",
        "session": "🔁 Session",
        "profile": "👤 Profile",
        "grammar_check": "✏️ Grammar Check",
        "chat_mode": "💬 Chat Mode",
        "proverbs": "📜 Proverbs",
        "error_notebook": "📒 Error Notebook",
        "correct": "✨ GREAT JOB!",
        "wrong": "❌ Wrong!",
        "next": "⏭ Next",
        "show_answer": "💡 Show",
        "pronounce": "🔊 Pronounce",
        "memorized": "✅ Memorized",
        "home": "🏠 Menu",
        "freeze": "❄️ Freeze",
        "de_uz_q": "🇩🇪 ➜ 🇬🇧  *Translation?*",
        "uz_de_q": "🇬🇧 ➜ 🇩🇪  *In German?*",
        "article_q": "🎯  *Find the article:*",
        "no_words": "⚠️ No words found.",
        "archived_msg": "📦 Archived!",
        "session_complete": "🎉 Session complete!\n\n✅ Correct: *{correct}*\n❌ Wrong: *{wrong}*\n🎯 Accuracy: *{acc}%*",
        "goal_progress": "🎯 Daily goal: *{done}/{goal}* words",
        "streak": "🔥 Streak",
        "language_select": "🌐 Select interface language:",
    }
}

GERMAN_PROVERBS = [
    ("Übung macht den Meister.", "Mashq ustani tarbiyalaydi. / Practice makes perfect."),
    ("Aller Anfang ist schwer.", "Har qanday boshlanish qiyin. / Every beginning is hard."),
    ("Ende gut, alles gut.", "Oxiri yaxshi — hammasi yaxshi. / All's well that ends well."),
    ("Morgenstund hat Gold im Mund.", "Ertalab turish oltin qadar. / The early bird catches the worm."),
    ("Wer rastet, der rostet.", "Kim dam olsa, u zanglaydi. / He who rests grows rusty."),
    ("Einmal ist keinmal.", "Bir marta — hech marta emas. / Once doesn't count."),
    ("Lügen haben kurze Beine.", "Yolg'onning oyog'i qisqa. / Lies have short legs."),
    ("Viele Wege führen nach Rom.", "Ko'p yo'l Rimga olib boradi. / Many roads lead to Rome."),
    ("Ohne Fleiß kein Preis.", "Mehnat qilmasang — mukofot olmaysan. / No pain, no gain."),
    ("Hunger ist der beste Koch.", "Ochlik — eng yaxshi oshpaz. / Hunger is the best cook."),
    ("Gut Ding will Weile haben.", "Yaxshi narsa vaqt talab qiladi. / Good things take time."),
    ("Hunde, die bellen, beißen nicht.", "Huradigan it tishlamaydi. / Barking dogs seldom bite."),
    ("Man soll den Tag nicht vor dem Abend loben.", "Kunni kechqurun maqta. / Don't praise the day before evening."),
    ("Wer wagt, gewinnt.", "Kim xavf qilsa, yutadi. / Fortune favors the bold."),
    ("Kleider machen Leute.", "Kiyim odam qiladi. / Clothes make the man."),
]

# ======================
# STATES
# ======================
class QuizState(StatesGroup):
    waiting_for_answer  = State()
    waiting_for_article = State()

class CategoryState(StatesGroup):
    waiting_name        = State()
    waiting_assign_word = State()
    waiting_new_word    = State()

class ReminderState(StatesGroup):
    waiting_time = State()

class DeleteState(StatesGroup):
    waiting_word = State()

class TranslateState(StatesGroup):
    waiting_word    = State()

class GapState(StatesGroup):
    waiting_word = State()

class ProfileState(StatesGroup):
    waiting_name  = State()
    waiting_goal  = State()
    waiting_level = State()

class GrammarState(StatesGroup):
    waiting_sentence = State()

class ChatModeState(StatesGroup):
    chatting = State()

class FlashcardState(StatesGroup):
    viewing = State()

class SessionState(StatesGroup):
    in_session = State()

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
        frozen        INTEGER DEFAULT 0,
        category_id   INTEGER DEFAULT NULL,
        wrong_count   INTEGER DEFAULT 0,
        correct_count INTEGER DEFAULT 0,
        level         TEXT DEFAULT 'custom',
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
        reminder_active INTEGER DEFAULT 1,
        lang            TEXT DEFAULT 'uz',
        daily_goal      INTEGER DEFAULT 10,
        user_name       TEXT DEFAULT '',
        target_level    TEXT DEFAULT 'B1',
        goal_text       TEXT DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS daily_progress (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id   INTEGER NOT NULL,
        prog_date TEXT NOT NULL,
        words_done INTEGER DEFAULT 0,
        UNIQUE(user_id, prog_date)
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
    "ALTER TABLE words ADD COLUMN frozen        INTEGER DEFAULT 0",
    "ALTER TABLE categories ADD COLUMN parent_id INTEGER DEFAULT NULL",
    "ALTER TABLE user_settings ADD COLUMN lang          TEXT DEFAULT 'uz'",
    "ALTER TABLE user_settings ADD COLUMN daily_goal    INTEGER DEFAULT 10",
    "ALTER TABLE user_settings ADD COLUMN user_name     TEXT DEFAULT ''",
    "ALTER TABLE user_settings ADD COLUMN target_level  TEXT DEFAULT 'B1'",
    "ALTER TABLE user_settings ADD COLUMN goal_text     TEXT DEFAULT ''",
]:
    try:
        cursor.execute(col_sql)
    except Exception:
        pass

conn.commit()

# ======================
# B1 SO'ZLARI
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

def get_lang(user_id: int) -> str:
    cursor.execute("SELECT lang FROM user_settings WHERE user_id=?", (user_id,))
    row = cursor.fetchone()
    return row[0] if row and row[0] in TEXTS else "uz"

def t(user_id: int, key: str) -> str:
    lang = get_lang(user_id)
    return TEXTS[lang].get(key, TEXTS["uz"].get(key, key))

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
            if german and not any(c.islower() for c in german[:2]):
                pass
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

def increment_daily_progress(user_id: int):
    today = str(date.today())
    cursor.execute(
        "INSERT INTO daily_progress (user_id, prog_date, words_done) VALUES (?,?,1) "
        "ON CONFLICT(user_id, prog_date) DO UPDATE SET words_done=words_done+1",
        (user_id, today)
    )
    conn.commit()

def get_daily_progress(user_id: int):
    today = str(date.today())
    cursor.execute("SELECT words_done FROM daily_progress WHERE user_id=? AND prog_date=?", (user_id, today))
    row = cursor.fetchone()
    return row[0] if row else 0

# ======================
# SM-2 SPACED REPETITION
# ======================

def sm2_update(word_id: int, quality: int):
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
    today = str(date.today())
    base  = "WHERE archived=0 AND frozen=0"
    if category_id:
        base += f" AND category_id={category_id}"
    cursor.execute(
        f"SELECT id, german, uzbek, article FROM words {base} "
        f"AND (next_review IS NULL OR next_review <= ?) ORDER BY RANDOM() LIMIT 1",
        (today,)
    )
    word = cursor.fetchone()
    if not word:
        cursor.execute(
            f"SELECT id, german, uzbek, article FROM words {base} ORDER BY RANDOM() LIMIT 1"
        )
        word = cursor.fetchone()
    return word

# ======================
# DEEPL + AI API
# ======================

async def deepl_translate(text: str, target: str = "EN") -> str:
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.post(
                "https://api-free.deepl.com/v2/translate",
                data={
                    "auth_key": DEEPL_KEY,
                    "text": text,
                    "source_lang": "DE",
                    "target_lang": target,
                },
            )
            data = r.json()
            return data["translations"][0]["text"]
    except Exception as e:
        logging.warning(f"DeepL xato: {e}")
        return None

async def ai_request(prompt: str, system: str = None, max_tokens: int = 500) -> str:
    """Anthropic Claude API (yoki boshqa provider) orqali AI javob olish"""
    # Bu yerda siz o'zingizning AI API kalitingizni .env ga qo'shing:
    # ANTHROPIC_KEY=sk-ant-...
    anthropic_key = os.getenv("ANTHROPIC_KEY", "")
    if not anthropic_key:
        return None
    try:
        messages = [{"role": "user", "content": prompt}]
        payload = {
            "model": "claude-haiku-4-5",
            "max_tokens": max_tokens,
            "messages": messages,
        }
        if system:
            payload["system"] = system
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": anthropic_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json=payload,
            )
            data = r.json()
            return data["content"][0]["text"]
    except Exception as e:
        logging.warning(f"AI API xato: {e}")
        return None

# ======================
# FOYDALANUVCHI SOZLAMALARI
# ======================

def get_user_settings(user_id: int):
    cursor.execute(
        "SELECT reminder_hour, reminder_minute, reminder_active, lang, daily_goal, user_name, target_level, goal_text "
        "FROM user_settings WHERE user_id=?", (user_id,)
    )
    row = cursor.fetchone()
    if row:
        return {
            "hour": row[0], "minute": row[1], "active": bool(row[2]),
            "lang": row[3] or "uz", "daily_goal": row[4] or 10,
            "user_name": row[5] or "", "target_level": row[6] or "B1",
            "goal_text": row[7] or ""
        }
    return {"hour": 20, "minute": 0, "active": True, "lang": "uz", "daily_goal": 10,
            "user_name": "", "target_level": "B1", "goal_text": ""}

def save_user_settings(user_id: int, **kwargs):
    existing = get_user_settings(user_id)
    existing.update(kwargs)
    cursor.execute(
        "INSERT INTO user_settings (user_id, reminder_hour, reminder_minute, reminder_active, "
        "lang, daily_goal, user_name, target_level, goal_text) "
        "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET "
        "reminder_hour=excluded.reminder_hour, reminder_minute=excluded.reminder_minute, "
        "reminder_active=excluded.reminder_active, lang=excluded.lang, "
        "daily_goal=excluded.daily_goal, user_name=excluded.user_name, "
        "target_level=excluded.target_level, goal_text=excluded.goal_text",
        (user_id, existing["hour"], existing["minute"], 1 if existing["active"] else 0,
         existing["lang"], existing["daily_goal"], existing["user_name"],
         existing["target_level"], existing["goal_text"])
    )
    conn.commit()

# ======================
# KUNLIK ESLATMA
# ======================

async def daily_reminder_loop():
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
                        InlineKeyboardButton("⏰ O'zgartirish",  callback_data="reminder_change"),
                    )
                    progress = get_daily_progress(user_id)
                    settings = get_user_settings(user_id)
                    goal     = settings["daily_goal"]
                    name     = settings["user_name"] or "Siz"
                    await bot.send_message(
                        user_id,
                        f"🔔 *Kunlik eslatma!*\n\n"
                        f"Salom, {name}! 👋\n"
                        f"Bugun: *{progress}/{goal}* so'z\n"
                        f"Har kun biroz mashq qilish — eng yaxshi yo'l! 💪",
                        reply_markup=kb,
                        parse_mode="Markdown"
                    )
                    sent_today.add(key)
                except Exception as e:
                    logging.warning(f"Reminder failed for {user_id}: {e}")

# ======================
# QUIZ KEYBOARD
# ======================

def _quiz_kb(word_id, next_cb="next", user_id=0):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton(t(user_id, "show_answer"), callback_data=f"show_{word_id}"),
        InlineKeyboardButton(t(user_id, "pronounce"),   callback_data=f"audio_{word_id}"),
    )
    kb.add(
        InlineKeyboardButton(t(user_id, "memorized"),   callback_data=f"archive_{word_id}"),
        InlineKeyboardButton(t(user_id, "freeze"),      callback_data=f"freeze_{word_id}"),
    )
    kb.add(
        InlineKeyboardButton(t(user_id, "next"),        callback_data=next_cb),
        InlineKeyboardButton(t(user_id, "home"),        callback_data="go_home"),
    )
    return kb

# ======================
# TEST YUBORISH
# ======================

async def send_test_question(message: types.Message, state: FSMContext, category_id=None):
    await state.finish()
    word = pick_sr_word(category_id)
    uid  = message.chat.id

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
        mode=mode, category_id=category_id, uid=uid
    )

    progress = get_daily_progress(uid)
    settings = get_user_settings(uid)
    goal     = settings["daily_goal"]
    prog_bar = f"\n{t(uid,'goal_progress').format(done=progress, goal=goal)}" if goal else ""

    if mode == "de_uz":
        nav_kb = _quiz_kb(word_id, "next", uid)
        await message.answer(
            f"{t(uid,'de_uz_q')}\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👉  *{display_german}*\n"
            f"━━━━━━━━━━━━━━━{prog_bar}",
            reply_markup=nav_kb, parse_mode="Markdown"
        )
        await QuizState.waiting_for_answer.set()

    elif mode == "uz_de":
        nav_kb = _quiz_kb(word_id, "next", uid)
        await message.answer(
            f"{t(uid,'uz_de_q')}\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👉  *{uzbek}*\n"
            f"━━━━━━━━━━━━━━━{prog_bar}",
            reply_markup=nav_kb, parse_mode="Markdown"
        )
        await QuizState.waiting_for_answer.set()

    else:
        art_kb = InlineKeyboardMarkup(row_width=3)
        art_kb.add(
            InlineKeyboardButton("🔵 der", callback_data="art_der"),
            InlineKeyboardButton("🔴 die", callback_data="art_die"),
            InlineKeyboardButton("🟢 das", callback_data="art_das"),
        )
        art_kb.add(InlineKeyboardButton(t(uid, "next"), callback_data="next"))
        await message.answer(
            f"{t(uid,'article_q')}\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"❓  *___ {german}*\n"
            f"━━━━━━━━━━━━━━━{prog_bar}",
            reply_markup=art_kb, parse_mode="Markdown"
        )
        await QuizState.waiting_for_article.set()

async def send_test_question_from_ids(message: types.Message, state: FSMContext, word_ids: list):
    await state.finish()
    if not word_ids:
        await message.answer("⚠️ Bu bo'limlarda so'z yo'q.")
        return
    uid   = message.chat.id
    today = str(date.today())
    placeholders = ",".join("?" * len(word_ids))
    cursor.execute(
        f"SELECT id, german, uzbek, article FROM words "
        f"WHERE id IN ({placeholders}) AND archived=0 AND frozen=0 "
        f"AND (next_review IS NULL OR next_review <= ?) ORDER BY RANDOM() LIMIT 1",
        (*word_ids, today)
    )
    word = cursor.fetchone()
    if not word:
        cursor.execute(
            f"SELECT id, german, uzbek, article FROM words "
            f"WHERE id IN ({placeholders}) AND archived=0 AND frozen=0 ORDER BY RANDOM() LIMIT 1",
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
        mode=mode, category_id=None, test_word_ids=word_ids, uid=uid
    )

    if mode == "de_uz":
        await message.answer(
            f"{t(uid,'de_uz_q')}\n\n━━━━━━━━━━━━━━━\n👉  *{display_german}*\n━━━━━━━━━━━━━━━",
            reply_markup=_quiz_kb(word_id, "next_ids", uid), parse_mode="Markdown"
        )
    elif mode == "uz_de":
        await message.answer(
            f"{t(uid,'uz_de_q')}\n\n━━━━━━━━━━━━━━━\n👉  *{uzbek}*\n━━━━━━━━━━━━━━━",
            reply_markup=_quiz_kb(word_id, "next_ids", uid), parse_mode="Markdown"
        )
    else:
        art_kb = InlineKeyboardMarkup(row_width=3)
        art_kb.add(
            InlineKeyboardButton("🔵 der", callback_data="art_der"),
            InlineKeyboardButton("🔴 die", callback_data="art_die"),
            InlineKeyboardButton("🟢 das", callback_data="art_das"),
        )
        art_kb.add(InlineKeyboardButton(t(uid,"next"), callback_data="next_ids"))
        await message.answer(
            f"{t(uid,'article_q')}\n\n━━━━━━━━━━━━━━━\n❓  *___ {german}*\n━━━━━━━━━━━━━━━",
            reply_markup=art_kb, parse_mode="Markdown"
        )
    await QuizState.waiting_for_answer.set()

# ======================
# /start
# ======================
@dp.message_handler(commands=["start"], state="*")
async def start(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id
    cursor.execute(
        "INSERT OR IGNORE INTO user_settings (user_id, reminder_hour, reminder_minute, reminder_active) VALUES (?,20,0,1)",
        (user_id,)
    )
    conn.commit()

    uid = user_id
    kb  = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton(t(uid,"test_start"),    callback_data="menu_test"),
        InlineKeyboardButton(t(uid,"hard_words"),    callback_data="menu_hard"),
    )
    kb.add(
        InlineKeyboardButton(t(uid,"stats"),         callback_data="menu_stats"),
        InlineKeyboardButton(t(uid,"word_list"),     callback_data="menu_list"),
    )
    kb.add(
        InlineKeyboardButton(t(uid,"categories"),    callback_data="menu_cats"),
        InlineKeyboardButton(t(uid,"b1_level"),      callback_data="menu_b1"),
    )
    kb.add(
        InlineKeyboardButton(t(uid,"flashcard"),     callback_data="menu_flashcard"),
        InlineKeyboardButton(t(uid,"session"),       callback_data="menu_session"),
    )
    kb.add(
        InlineKeyboardButton(t(uid,"profile"),       callback_data="menu_profile"),
        InlineKeyboardButton(t(uid,"proverbs"),      callback_data="menu_proverbs"),
    )
    kb.add(
        InlineKeyboardButton(t(uid,"grammar_check"), callback_data="menu_grammar"),
        InlineKeyboardButton(t(uid,"chat_mode"),     callback_data="menu_chat"),
    )
    kb.add(
        InlineKeyboardButton(t(uid,"error_notebook"),callback_data="menu_errors"),
        InlineKeyboardButton(t(uid,"translate"),     callback_data="menu_translate"),
    )
    kb.add(
        InlineKeyboardButton(t(uid,"export"),        callback_data="menu_export"),
        InlineKeyboardButton(t(uid,"reminder"),      callback_data="reminder_change"),
    )
    kb.add(
        InlineKeyboardButton(t(uid,"delete_word"),   callback_data="menu_delete"),
        InlineKeyboardButton(t(uid,"archive"),       callback_data="menu_archived"),
    )
    kb.add(
        InlineKeyboardButton("🌐 Til / Язык / Language", callback_data="menu_lang"),
    )

    settings = get_user_settings(uid)
    name     = settings["user_name"] or message.from_user.first_name or ""
    greeting = f"Salom, *{name}*! 👋\n\n" if name else ""

    await message.answer(
        greeting + t(uid, "start_greeting"),
        reply_markup=kb, parse_mode="Markdown"
    )

# ======================
# MENYU CALLBACKLAR
# ======================
@dp.callback_query_handler(lambda c: c.data in [
    "menu_test","menu_hard","menu_stats","menu_list",
    "menu_cats","menu_b1","menu_export","menu_delete","menu_archived",
    "reminder_test","menu_translate","menu_gap","menu_flashcard",
    "menu_session","menu_profile","menu_grammar","menu_chat",
    "menu_proverbs","menu_errors","menu_lang"
], state="*")
async def menu_callbacks(call: types.CallbackQuery, state: FSMContext):
    d   = call.data
    uid = call.from_user.id
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
            "🗑 *So'z o'chirish*\n\nO'chirmoqchi bo'lgan so'zni yozing (german):\n"
            "Masalan: `Tisch` yoki `der Tisch`\n\nBekor qilish: /start",
            parse_mode="Markdown"
        )
        await DeleteState.waiting_word.set()

    elif d == "menu_archived":
        await call.message.delete()
        await list_archived_send(call.message)

    elif d == "menu_translate":
        await call.message.answer(
            "🔍 *Tarjimasini bilish*\n\nNemischa so'z yozing — inglizcha tarjimasini aytaman.\n"
            "_(Hech narsa saqlanmaydi!)_\n\nBekor qilish: /start",
            parse_mode="Markdown"
        )
        await TranslateState.waiting_word.set()

    elif d == "menu_gap":
        await call.message.answer(
            "📝 *Gap tuzish*\n\nNemischa so'z yozing — o'sha so'z bilan gap tuzib beraman.\n"
            "_(Hech narsa saqlanmaydi!)_\n\nBekor qilish: /start",
            parse_mode="Markdown"
        )
        await GapState.waiting_word.set()

    elif d == "menu_flashcard":
        await call.message.delete()
        await flashcard_start(call.message, state)

    elif d == "menu_session":
        await call.message.delete()
        await session_choose(call.message, state)

    elif d == "menu_profile":
        await call.message.delete()
        await profile_show(call.message, uid)

    elif d == "menu_grammar":
        await call.message.answer(
            "✏️ *Grammatika tekshirish*\n\n"
            "Nemischa gap yozing — grammatik xatolaringizni topib beraman.\n\n"
            "Bekor qilish: /start",
            parse_mode="Markdown"
        )
        await GrammarState.waiting_sentence.set()

    elif d == "menu_chat":
        await call.message.answer(
            "💬 *Suhbat rejimi*\n\n"
            "Men siz bilan nemischa gaplashaman! 🇩🇪\n"
            "Biror narsa yozing — men javob beraman va xatolaringizni to'g'rilayman.\n\n"
            "Chiqish: /start",
            parse_mode="Markdown"
        )
        await state.update_data(chat_history=[])
        await ChatModeState.chatting.set()

    elif d == "menu_proverbs":
        await call.message.delete()
        proverb = random.choice(GERMAN_PROVERBS)
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🎲 Boshqa", callback_data="menu_proverbs"),
            InlineKeyboardButton(t(uid,"home"), callback_data="go_home"),
        )
        await call.message.answer(
            f"📜 *Nemis maqoli:*\n\n"
            f"🇩🇪 *{proverb[0]}*\n\n"
            f"💡 _{proverb[1]}_",
            reply_markup=kb, parse_mode="Markdown"
        )

    elif d == "menu_errors":
        await call.message.delete()
        await error_notebook_send(call.message)

    elif d == "menu_lang":
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("🇺🇿 O'zbekcha", callback_data="lang_uz"),
            InlineKeyboardButton("🇷🇺 Русский",   callback_data="lang_ru"),
            InlineKeyboardButton("🇬🇧 English",   callback_data="lang_en"),
        )
        await call.message.answer(t(uid,"language_select"), reply_markup=kb)

# ======================
# TIL TANLASH
# ======================
@dp.callback_query_handler(lambda c: c.data in ["lang_uz","lang_ru","lang_en"], state="*")
async def set_language(call: types.CallbackQuery, state: FSMContext):
    lang = call.data.split("_")[1]
    save_user_settings(call.from_user.id, lang=lang)
    await call.answer(f"✅ Til o'rnatildi!")
    await call.message.delete()
    await start(call.message, state)

# ======================
# PROFIL
# ======================
async def profile_show(message: types.Message, user_id: int):
    s = get_user_settings(user_id)
    cursor.execute("SELECT COUNT(*) FROM words WHERE archived=0")
    total_words = cursor.fetchone()[0]
    cursor.execute("SELECT SUM(correct), SUM(wrong) FROM stats")
    tot = cursor.fetchone()
    tot_c = tot[0] or 0
    tot_w = tot[1] or 0
    streak = get_streak()
    progress = get_daily_progress(user_id)

    name        = s["user_name"] or "—"
    level       = s["target_level"] or "B1"
    goal        = s["goal_text"] or "—"
    daily_goal  = s["daily_goal"]
    acc = round(tot_c / (tot_c+tot_w) * 100) if (tot_c+tot_w) else 0

    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("✏️ Ismni o'zgartirish",       callback_data="profile_name"),
        InlineKeyboardButton("🎯 Maqsad o'rnatish",         callback_data="profile_goal"),
        InlineKeyboardButton("📊 Daraja o'rnatish",         callback_data="profile_level"),
        InlineKeyboardButton("🏠 Menyu",                    callback_data="go_home"),
    )
    await message.answer(
        f"👤 *Profil*\n\n"
        f"📛 Ism: *{name}*\n"
        f"🎓 Daraja: *{level}*\n"
        f"🎯 Maqsad: _{goal}_\n"
        f"📅 Kunlik maqsad: *{progress}/{daily_goal}* so'z\n\n"
        f"📚 Jami so'zlar: *{total_words}*\n"
        f"✅ To'g'ri: *{tot_c}*  ❌ Xato: *{tot_w}*\n"
        f"🎯 Aniqlik: *{acc}%*\n"
        f"🔥 Streak: *{streak}* kun",
        reply_markup=kb, parse_mode="Markdown"
    )

@dp.callback_query_handler(lambda c: c.data in ["profile_name","profile_goal","profile_level"], state="*")
async def profile_edit(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    if call.data == "profile_name":
        await call.message.answer("📛 Ismingizni yozing:\n\nBekor qilish: /start")
        await ProfileState.waiting_name.set()
    elif call.data == "profile_goal":
        await call.message.answer(
            "🎯 *Maqsad o'rnatish*\n\n"
            "Kunlik nechta so'z o'rganmoqchisiz? (masalan: `10`)\n\n"
            "Yoki maqsadingizni yozing: `B2 ga 6 oyda yetaman`\n\nBekor qilish: /start",
            parse_mode="Markdown"
        )
        await ProfileState.waiting_goal.set()
    elif call.data == "profile_level":
        kb = InlineKeyboardMarkup(row_width=3)
        for lv in ["A1","A2","B1","B2","C1","C2"]:
            kb.add(InlineKeyboardButton(lv, callback_data=f"setlevel_{lv}"))
        await call.message.answer("📊 Darajangizni tanlang:", reply_markup=kb)

@dp.message_handler(state=ProfileState.waiting_name)
async def profile_set_name(message: types.Message, state: FSMContext):
    if message.text.startswith("/"): await state.finish(); return
    save_user_settings(message.from_user.id, user_name=message.text.strip())
    await state.finish()
    await message.answer(f"✅ Ism saqlandi: *{message.text.strip()}*", parse_mode="Markdown")

@dp.message_handler(state=ProfileState.waiting_goal)
async def profile_set_goal(message: types.Message, state: FSMContext):
    if message.text.startswith("/"): await state.finish(); return
    text = message.text.strip()
    try:
        daily = int(text)
        save_user_settings(message.from_user.id, daily_goal=daily)
        await message.answer(f"✅ Kunlik maqsad: *{daily}* so'z", parse_mode="Markdown")
    except ValueError:
        save_user_settings(message.from_user.id, goal_text=text)
        await message.answer(f"✅ Maqsad saqlandi: _{text}_", parse_mode="Markdown")
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("setlevel_"), state="*")
async def profile_set_level(call: types.CallbackQuery, state: FSMContext):
    level = call.data.split("_")[1]
    save_user_settings(call.from_user.id, target_level=level)
    await call.answer(f"✅ Daraja: {level}")
    await call.message.delete()

# ======================
# FLASHCARD REJIMI
# ======================
async def flashcard_start(message: types.Message, state: FSMContext):
    await state.finish()
    uid = message.chat.id
    cursor.execute("SELECT id, german, uzbek, article FROM words WHERE archived=0 AND frozen=0 ORDER BY RANDOM() LIMIT 1")
    word = cursor.fetchone()
    if not word:
        await message.answer("⚠️ So'zlar yo'q.")
        return
    word_id, german, uzbek, article = word
    display = format_german(german, article)
    await state.update_data(fc_id=word_id, fc_german=display, fc_uzbek=uzbek, fc_shown=False)

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔄 Ag'darish (tarjima)", callback_data="fc_flip"),
        InlineKeyboardButton("⏭ Keyingisi",           callback_data="fc_next"),
    )
    kb.add(
        InlineKeyboardButton("🔊 Talaffuz",            callback_data=f"audio_{word_id}"),
        InlineKeyboardButton("🏠 Menyu",               callback_data="go_home"),
    )
    await message.answer(
        f"🃏 *Flashcard*\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"🇩🇪  *{display}*\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"_Ag'daring yoki keyingisiga o'ting_",
        reply_markup=kb, parse_mode="Markdown"
    )
    await FlashcardState.viewing.set()

@dp.callback_query_handler(lambda c: c.data in ["fc_flip","fc_next"], state="*")
async def flashcard_action(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    data = await state.get_data()

    if call.data == "fc_flip":
        german = data.get("fc_german","")
        uzbek  = data.get("fc_uzbek","")
        word_id = data.get("fc_id")
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("⏭ Keyingisi",  callback_data="fc_next"),
            InlineKeyboardButton("🏠 Menyu",      callback_data="go_home"),
        )
        try:
            await call.message.edit_text(
                f"🃏 *Flashcard*\n\n"
                f"━━━━━━━━━━━━━━━\n"
                f"🇩🇪  *{german}*\n"
                f"🇺🇿  _{uzbek}_\n"
                f"━━━━━━━━━━━━━━━",
                reply_markup=kb, parse_mode="Markdown"
            )
        except:
            pass
    else:
        await call.message.delete()
        await flashcard_start(call.message, state)

# ======================
# SESSIYA REJIMI
# ======================
async def session_choose(message: types.Message, state: FSMContext):
    await state.finish()
    kb = InlineKeyboardMarkup(row_width=3)
    kb.add(
        InlineKeyboardButton("10 ta",  callback_data="sess_10"),
        InlineKeyboardButton("20 ta",  callback_data="sess_20"),
        InlineKeyboardButton("30 ta",  callback_data="sess_30"),
    )
    kb.add(InlineKeyboardButton("🏠 Menyu", callback_data="go_home"))
    await message.answer(
        "🔁 *Sessiya rejimi*\n\nNechta savoldan iborat sessiya boshlaylik?",
        reply_markup=kb, parse_mode="Markdown"
    )

@dp.callback_query_handler(lambda c: c.data in ["sess_10","sess_20","sess_30"], state="*")
async def session_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    count = int(call.data.split("_")[1])
    await state.update_data(sess_total=count, sess_done=0, sess_correct=0, sess_wrong=0)
    await call.message.delete()
    await send_test_question(call.message, state)
    await SessionState.in_session.set()

# ======================
# XATO DAFTARI
# ======================
async def error_notebook_send(message: types.Message):
    cursor.execute(
        "SELECT german, article, uzbek, wrong_count FROM words "
        "WHERE wrong_count > 0 AND archived=0 ORDER BY wrong_count DESC LIMIT 30"
    )
    words = cursor.fetchall()
    if not words:
        await message.answer("📒 Hozircha xato daftari bo'sh! 🎉")
        return
    text = "📒 *Xato daftari (eng ko'p xato):*\n\n"
    for german, article, uzbek, wc in words:
        display = format_german(german, article)
        bar = "🔴" * min(wc, 5)
        text += f"{bar} *{display}* — {uzbek} _{wc}× xato_\n"
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🎯 Xato so'zlardan test", callback_data="menu_hard"))
    kb.add(InlineKeyboardButton("🏠 Menyu", callback_data="go_home"))
    for i in range(0, len(text), 4000):
        await message.answer(text[i:i+4000], parse_mode="Markdown",
                             reply_markup=kb if i == 0 else None)

# ======================
# GRAMMATIKA TEKSHIRISH
# ======================
@dp.message_handler(state=GrammarState.waiting_sentence)
async def grammar_check_handler(message: types.Message, state: FSMContext):
    if message.text.startswith("/"): await state.finish(); return
    sentence = message.text.strip()
    await message.answer("⏳ Tekshirilmoqda...")
    result = await ai_request(
        prompt=f"Please check this German sentence for grammar errors and explain corrections: \"{sentence}\"\n\nAnswer in this format:\n1. If correct: '✅ Correct! [brief explanation]'\n2. If wrong: '❌ Error found: [what is wrong]\n✍️ Corrected: [corrected sentence]\n💡 Explanation: [why]'",
        system="You are a German language teacher. Be concise and helpful.",
        max_tokens=300
    )
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✏️ Yana tekshirish", callback_data="menu_grammar"),
        InlineKeyboardButton("🏠 Menyu",           callback_data="go_home"),
    )
    if result:
        await message.answer(
            f"✏️ *Grammatika tekshiruvi:*\n\n`{sentence}`\n\n{result}",
            reply_markup=kb, parse_mode="Markdown"
        )
    else:
        await message.answer(
            "⚠️ AI xizmat hozir mavjud emas.\n_(ANTHROPIC_KEY ni .env ga qo'shing)_",
            reply_markup=kb, parse_mode="Markdown"
        )
    await state.finish()

# ======================
# SUHBAT REJIMI
# ======================
@dp.message_handler(state=ChatModeState.chatting)
async def chat_mode_handler(message: types.Message, state: FSMContext):
    if message.text.startswith("/"):
        await state.finish()
        return
    data    = await state.get_data()
    history = data.get("chat_history", [])
    history.append({"role": "user", "content": message.text})

    await message.answer("⏳ Javob tayyorlanmoqda...")

    # History ni cheklash
    if len(history) > 10:
        history = history[-10:]

    result = await ai_request(
        prompt="\n".join([f"{m['role'].upper()}: {m['content']}" for m in history]),
        system=(
            "You are a friendly German language tutor having a conversation in German. "
            "The user may write in German, English, Russian or Uzbek. "
            "Always respond primarily in German but gently correct any German mistakes. "
            "Keep responses short (2-4 sentences). "
            "If the user makes a German grammar/vocabulary mistake, first correct it briefly then continue. "
            "Format: [Correction if needed]\n[Your German response]\n[Brief Uzbek/English translation]"
        ),
        max_tokens=400
    )
    if result:
        history.append({"role": "assistant", "content": result})
        await state.update_data(chat_history=history)
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("🚪 Suhbatdan chiqish", callback_data="go_home"))
        await message.answer(
            f"💬 *Nemischa suhbat:*\n\n{result}",
            reply_markup=kb, parse_mode="Markdown"
        )
    else:
        await message.answer(
            "⚠️ AI xizmat hozir mavjud emas.\n_(ANTHROPIC_KEY ni .env ga qo'shing)_"
        )
        await state.finish()

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
        "WHERE archived=0 AND frozen=0 AND wrong_count>0 ORDER BY wrong_count DESC LIMIT 1"
    )
    word = cursor.fetchone()
    uid  = message.chat.id
    if not word:
        await message.answer("🎉 Hozircha xato so'zlar yo'q! /test dan foydalaning.")
        return
    word_id, german, uzbek, article, wrong_count = word
    display_german = format_german(german, article)
    await state.update_data(
        word_id=word_id, german=german, uzbek=uzbek,
        article=article, display_german=display_german,
        mode="de_uz", category_id=None, hard_mode=True, uid=uid
    )
    kb = _quiz_kb(word_id, "hard_next", uid)
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
    uid = message.chat.id
    cursor.execute("SELECT COUNT(*) FROM words WHERE archived=0")
    active = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM words WHERE archived=1")
    archived_cnt = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM words WHERE frozen=1 AND archived=0")
    frozen_cnt = cursor.fetchone()[0]
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

    cursor.execute(
        "SELECT COUNT(*) FROM words WHERE archived=0 AND frozen=0 AND (next_review IS NULL OR next_review<=?)",
        (today,)
    )
    due_cnt = cursor.fetchone()[0]
    progress = get_daily_progress(uid)
    settings = get_user_settings(uid)
    goal     = settings["daily_goal"]

    await message.answer(
        f"📊 *Statistika*\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📚 So'zlar: *{active}* aktiv · *{archived_cnt}* arxiv · *{frozen_cnt}* muzlatilgan\n"
        f"🌐 B1 darajasi: *{b1_cnt}* ta\n"
        f"💀 Qiyin (3+ xato): *{hard_cnt}* ta\n"
        f"⏳ Bugun takrorlanishi kerak: *{due_cnt}* ta\n\n"
        f"🎯 Bugungi maqsad: *{progress}/{goal}* so'z\n\n"
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
        SELECT w.german, w.article, w.uzbek, c.name, w.level, w.frozen
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
    for german, article, uzbek, cat_name, level, frozen in words:
        display = format_german(german, article)
        tag     = f" `[{level}]`" if level and level != "custom" else ""
        cat_str = f" _{cat_name}_" if cat_name else ""
        frz     = " ❄️" if frozen else ""
        text   += f"• *{display}* — {uzbek}{tag}{cat_str}{frz}\n"
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
    if parent_id is None:
        cursor.execute("SELECT id, name FROM categories WHERE parent_id IS NULL ORDER BY name")
    else:
        cursor.execute("SELECT id, name FROM categories WHERE parent_id=? ORDER BY name", (parent_id,))
    cats = cursor.fetchall()

    kb = InlineKeyboardMarkup(row_width=1)
    if parent_id is not None:
        cursor.execute("SELECT id FROM categories WHERE id=?", (parent_id,))
        prow = cursor.fetchone()
        kb.add(InlineKeyboardButton("◀️ Orqaga", callback_data=f"cat_open_{prow[0]}"))

    kb.add(InlineKeyboardButton(
        "➕ Yangi kategoriya",
        callback_data=f"cat_new_{parent_id}" if parent_id else "cat_new_root"
    ))
    for cat_id, cat_name in cats:
        direct_cnt = _count_words_recursive(cat_id)
        sub_cnt    = _count_subcats(cat_id)
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
    cursor.execute("SELECT id FROM words WHERE category_id=? AND archived=0 AND frozen=0", (cat_id,))
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
    else:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["German","Article","Uzbek","Category","Level","Correct","Wrong"])
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
        "🗑 O'chirmoqchi bo'lgan so'zni yozing (german):\nMasalan: `Tisch` yoki `der Tisch`\n\nBekor qilish: /start",
        parse_mode="Markdown"
    )
    await DeleteState.waiting_word.set()

@dp.message_handler(state=DeleteState.waiting_word)
async def delete_word_handler(message: types.Message, state: FSMContext):
    if message.text.startswith("/"): await state.finish(); return
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
        cursor.execute(
            "SELECT id, german, article, uzbek FROM words WHERE LOWER(german)=LOWER(?)", (word,)
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
# MUZLATISH (FREEZE)
# ======================
@dp.callback_query_handler(lambda c: c.data.startswith("freeze_"), state="*")
async def freeze_word(call: types.CallbackQuery, state: FSMContext):
    wid = int(call.data.split("_")[1])
    cursor.execute("SELECT frozen FROM words WHERE id=?", (wid,))
    row = cursor.fetchone()
    if not row:
        await call.answer("Topilmadi.")
        return
    new_frozen = 0 if row[0] else 1
    cursor.execute("UPDATE words SET frozen=? WHERE id=?", (new_frozen, wid))
    conn.commit()
    msg = "❄️ So'z muzlatildi! Keyinroq o'rganasiz." if new_frozen else "✅ So'z faollashtirildi!"
    await call.answer(msg, show_alert=True)

# ======================
# KUNLIK ESLATMA SOZLASH
# ======================
@dp.callback_query_handler(lambda c: c.data == "reminder_change", state="*")
async def reminder_change(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await call.message.answer(
        "⏰ *Eslatma vaqtini o'zgartirish*\n\n"
        "Soatni kiriting (masalan: `20:00` yoki `09:30`):\n\n"
        "O'chirish uchun: `off`\nBekor qilish: /start",
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
    if text.startswith("/"): await state.finish(); return
    if text == "off":
        save_user_settings(message.from_user.id, active=False)
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
    save_user_settings(message.from_user.id, hour=hour, minute=minute, active=True)
    await message.answer(
        f"✅ Eslatma *{hour:02d}:{minute:02d}* ga sozlandi! 🔔",
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
    uid          = data.get("uid", message.chat.id)
    user_input   = message.text.strip()

    # Sessiya davomi
    sess_total   = data.get("sess_total")
    sess_done    = data.get("sess_done", 0)
    sess_correct = data.get("sess_correct", 0)
    sess_wrong   = data.get("sess_wrong", 0)

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
        increment_daily_progress(uid)
        await message.reply(
            f"{t(uid,'correct')}\n\n_{question_display}_ = *{correct_display}*",
            parse_mode="Markdown"
        )
        if sess_total:
            sess_done += 1
            sess_correct += 1
            if sess_done >= sess_total:
                acc = round(sess_correct / sess_total * 100) if sess_total else 0
                kb = InlineKeyboardMarkup(row_width=2)
                kb.add(
                    InlineKeyboardButton("🔁 Yana sessiya", callback_data="menu_session"),
                    InlineKeyboardButton("🏠 Menyu",        callback_data="go_home"),
                )
                await state.finish()
                await message.answer(
                    t(uid,"session_complete").format(
                        correct=sess_correct, wrong=sess_wrong, acc=acc
                    ),
                    reply_markup=kb, parse_mode="Markdown"
                )
                return
            await state.update_data(sess_done=sess_done, sess_correct=sess_correct, sess_wrong=sess_wrong)
        if hard_mode:
            await hard_test(message, state)
        else:
            await send_test_question(message, state, category_id=category_id)
    else:
        cursor.execute("UPDATE words SET wrong_count = wrong_count + 1 WHERE id=?", (word_id,))
        conn.commit()
        log_stat(False)
        sm2_update(word_id, quality=1)
        if sess_total:
            sess_wrong += 1
            await state.update_data(sess_wrong=sess_wrong)
        next_cb = "hard_next" if hard_mode else "next"
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton(t(uid,"next"),       callback_data=next_cb),
            InlineKeyboardButton(t(uid,"memorized"),  callback_data=f"archive_{word_id}"),
        )
        await message.answer(
            f"{t(uid,'wrong')}\n\nTo'g'ri javob: `{correct_display}`",
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
    uid     = data.get("uid", call.from_user.id)

    await call.message.delete()

    if chosen == correct:
        cursor.execute("UPDATE words SET correct_count = correct_count + 1 WHERE id=?", (word_id,))
        conn.commit()
        log_stat(True)
        sm2_update(word_id, quality=4)
        increment_daily_progress(uid)
        await call.message.answer(
            f"{t(uid,'correct')}\n_{chosen} {german}_ = {uzbek}",
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
            InlineKeyboardButton(t(uid,"next"),      callback_data="next"),
            InlineKeyboardButton(t(uid,"memorized"), callback_data=f"archive_{word_id}"),
        )
        await call.message.answer(
            f"{t(uid,'wrong')} Sen: `{chosen}`\n\nTo'g'ri: `{correct} {german}` = {uzbek}",
            reply_markup=kb, parse_mode="Markdown"
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
        await call.message.answer("❌ Audio yuborishda xatolik.")

# ======================
# SO'Z QO'SHISH
# ======================
@dp.message_handler(state=None)
async def bulk_add(message: types.Message):
    lines = message.text.strip().split('\n')
    has_cat_format = any(
        l.strip().endswith(":") and not _looks_like_word_line(l.strip())
        for l in lines if l.strip()
    )
    if has_cat_format:
        await _bulk_add_with_categories(message, lines, parent_id=None)
    else:
        await _bulk_add_simple(message, lines, category_id=None)

def _looks_like_word_line(line):
    return " – " in line or " - " in line or "–" in line

async def _bulk_add_simple(message, lines, category_id):
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
    current_cat_id = None
    total_added = 0
    total_dup   = 0
    cats_created = []
    errors = []

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.endswith(":") and not _looks_like_word_line(line):
            cat_name = line[:-1].strip()
            if not cat_name:
                continue
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
# TARJIMA
# ======================
@dp.message_handler(commands=["tarjima"], state="*")
async def tarjima_cmd(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer(
        "🔍 *Tarjimasini bilish*\n\nNemischa so'z yozing.\n_(Hech narsa saqlanmaydi!)_\n\nBekor qilish: /start",
        parse_mode="Markdown"
    )
    await TranslateState.waiting_word.set()

@dp.message_handler(state=TranslateState.waiting_word)
async def translate_word_handler(message: types.Message, state: FSMContext):
    if message.text.startswith("/"): await state.finish(); return
    word = message.text.strip()
    await message.answer("⏳ Tarjima qilinmoqda...")
    translation = await deepl_translate(word)
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔍 Yana so'rov", callback_data="menu_translate"),
        InlineKeyboardButton("🏠 Menyu",       callback_data="go_home"),
    )
    if translation:
        await message.answer(
            f"🇩🇪 *{word}*\n🇬🇧 `{translation}`\n\n_O'zbekchasi — o'zing top! 😄_",
            reply_markup=kb, parse_mode="Markdown"
        )
    else:
        await message.answer("❌ Tarjima qilishda xatolik.", reply_markup=kb, parse_mode="Markdown")
    await state.finish()

# ======================
# GAP TUZISH
# ======================
@dp.message_handler(commands=["gap"], state="*")
async def gap_cmd(message: types.Message, state: FSMContext):
    await state.finish()
    word = message.get_args()
    if word:
        await _send_gap_examples(message, state, word)
    else:
        await message.answer(
            "📝 *Gap tuzish*\n\nNemischa so'z yozing.\n\nBekor qilish: /start",
            parse_mode="Markdown"
        )
        await GapState.waiting_word.set()

@dp.message_handler(state=GapState.waiting_word)
async def gap_word_handler(message: types.Message, state: FSMContext):
    if message.text.startswith("/"): await state.finish(); return
    await _send_gap_examples(message, state, message.text.strip())

async def _send_gap_examples(message: types.Message, state: FSMContext, word: str):
    await state.finish()
    await message.answer("⏳ Gaplar tayyorlanmoqda...")
    sentences = [
        f"Ich lerne das Wort \"{word}\" heute.",
        f"Das Wort \"{word}\" ist sehr nützlich für mich.",
        f"Können Sie \"{word}\" in einem Satz benutzen?",
    ]
    results = []
    for sent in sentences:
        en = await deepl_translate(sent)
        if en:
            results.append(f"🇩🇪 _{sent}_\n🇬🇧 {en}")
        else:
            results.append(f"🇩🇪 _{sent}_")
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📝 Yana so'rov", callback_data="menu_gap"),
        InlineKeyboardButton("🏠 Menyu",       callback_data="go_home"),
    )
    text = f"📝 *\"{word}\"* so'zi bilan gaplar:\n\n" + "\n\n".join(results)
    await message.answer(text, reply_markup=kb, parse_mode="Markdown")

# ======================
# UMUMIY CALLBACK HANDLER
# ======================
@dp.callback_query_handler(state="*")
async def process_callback(call: types.CallbackQuery, state: FSMContext):
    d       = call.data
    st_data = await state.get_data()
    cat_id  = st_data.get("category_id")
    uid     = call.from_user.id

    if d == "noop":
        await call.answer()

    elif d == "go_home":
        await state.finish()
        await call.message.delete()
        await start(call.message, state)

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
        await call.answer(t(uid, "archived_msg"))
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
        if d == "cat_new_root":
            p_id = None
        else:
            p_id = int(d.split("_")[2]) if len(d.split("_")) > 2 else None
        await state.update_data(new_cat_parent_id=p_id)
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
        cursor.execute("SELECT id, name FROM categories WHERE parent_id=? ORDER BY name", (cid,))
        subcats   = cursor.fetchall()
        direct_cnt = _count_words_recursive(cid)
        sub_cnt    = len(subcats)

        kb = InlineKeyboardMarkup(row_width=1)
        if cat_parent is not None:
            kb.add(InlineKeyboardButton("◀️ Orqaga", callback_data=f"cat_open_{cat_parent}"))
        else:
            kb.add(InlineKeyboardButton("◀️ Kategoriyalar", callback_data="menu_cats"))

        kb.add(InlineKeyboardButton("🎯 Test boshlash",     callback_data=f"cat_test_choose_{cid}"))
        kb.add(InlineKeyboardButton("📋 So'zlarini ko'rish", callback_data=f"cat_list_{cid}"))

        if subcats:
            kb.add(InlineKeyboardButton("─── Bo'limlar ───", callback_data="noop"))
            for sc_id, sc_name in subcats:
                sc_cnt = _count_words_recursive(sc_id)
                kb.add(InlineKeyboardButton(
                    f"  📂 {sc_name} ({sc_cnt} so'z)",
                    callback_data=f"cat_open_{sc_id}"
                ))

        kb.add(InlineKeyboardButton("─── Boshqarish ───",         callback_data="noop"))
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
            await state.update_data(active_category=cid, test_word_ids=None)
            await call.message.delete()
            await send_test_question(call.message, state, category_id=cid)
            return
        kb = InlineKeyboardMarkup(row_width=1)
        all_ids = _get_all_word_ids_recursive(cid)
        kb.add(InlineKeyboardButton(f"📚 Umumiy ({len(all_ids)} so'z)", callback_data=f"cat_test_all_{cid}"))
        for sc_id, sc_name in subcats:
            sc_cnt = _count_words_recursive(sc_id)
            kb.add(InlineKeyboardButton(f"📂 {sc_name} ({sc_cnt} so'z)", callback_data=f"cat_test_{sc_id}"))
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
        cid      = int(d.split("_")[3])
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
        cursor.execute(
            "SELECT COUNT(*) FROM words WHERE (category_id IS NULL OR category_id != ?) AND archived=0",
            (cid,)
        )
        free_cnt = cursor.fetchone()[0]
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton(f"📚 Mavjud so'zlardan qo'shish ({free_cnt} ta)", callback_data=f"cat_fromexisting_{cid}"),
            InlineKeyboardButton("✏️ Yangi so'z qo'shish",                         callback_data=f"cat_newword_{cid}"),
            InlineKeyboardButton("◀️ Orqaga",                                      callback_data=f"cat_open_{cid}"),
        )
        await call.message.edit_text(
            f"📁 *{cat_name}* — So'z qo'shish\n\nQanday qo'shmoqchisiz?",
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
            kb.add(InlineKeyboardButton(f"➕ {display} — {uzbek}", callback_data=f"cat_pick_{cid}_{wid}"))
        kb.add(InlineKeyboardButton("◀️ Orqaga", callback_data=f"cat_addword_{cid}"))
        await call.message.edit_text(
            f"📚 *{cat_name}* ga qo'shish\n\nQaysi so'zni qo'shmoqchisiz?",
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
        if w:
            display = format_german(w[0], w[1])
            await call.answer(f"✅ '{display}' qo'shildi!")
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
            kb2.add(InlineKeyboardButton(f"➕ {display} — {uzbek}", callback_data=f"cat_pick_{cid}_{wid2}"))
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
            "Format:\n`der Tisch - stol`\n`laufen - yugurmoq`\n\nBekor qilish: /start",
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
            "So'zlarni qatorma-qator yozing:\n`der Tisch`\n`laufen`\n\nBekor qilish: /start",
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

    elif d.startswith("setlevel_"):
        level = d.split("_")[1]
        save_user_settings(uid, target_level=level)
        await call.answer(f"✅ Daraja: {level}")
        try:
            await call.message.delete()
        except:
            pass

    else:
        await call.answer()

# ======================
# KATEGORIYA STATELARI
# ======================
@dp.message_handler(state=CategoryState.waiting_name)
async def cat_save_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    if not name or message.text.startswith("/"): await state.finish(); return
    data      = await state.get_data()
    parent_id = data.get("new_cat_parent_id")
    try:
        cursor.execute("INSERT INTO categories (name, parent_id) VALUES (?, ?)", (name, parent_id))
        conn.commit()
        if parent_id is not None:
            cursor.execute("SELECT name FROM categories WHERE id=?", (parent_id,))
            pname = cursor.fetchone()
            pname = pname[0] if pname else "kategoriya"
            await message.answer(f"✅ *{name}* bo'limi *{pname}* ichiga yaratildi!", parse_mode="Markdown")
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
        if not line: continue
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
    if message.text.startswith("/"): await state.finish(); return
    data  = await state.get_data()
    cid   = data.get("target_cat_id")
    lines = message.text.strip().split('\n')
    has_subcat_format = any(
        l.strip().endswith(":") and not _looks_like_word_line(l.strip())
        for l in lines if l.strip()
    )
    if has_subcat_format:
        await _bulk_add_with_categories(message, lines, parent_id=cid)
    else:
        added, duplicates, errors = 0, [], []
        for line in lines:
            line = line.strip()
            if not line: continue
            parsed = parse_word_line(line)
            if not parsed: errors.append(line); continue
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
        if added > 0 or duplicates: conn.commit()
        cursor.execute("SELECT name FROM categories WHERE id=?", (cid,))
        cat_row = cursor.fetchone()
        cat_name = cat_row[0] if cat_row else "Kategoriya"
        parts = []
        if added: parts.append(f"✅ *{added} ta yangi so'z* qo'shildi → *{cat_name}*")
        if duplicates: parts.append(f"🔗 Mavjud, biriktirildi: {', '.join(f'`{w}`' for w in duplicates[:5])}")
        if errors: parts.append(f"❓ Format noto'g'ri ({len(errors)} ta)")
        if not parts: parts.append("❓ Hech narsa qo'shilmadi.")
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("➕ Yana so'z qo'shish",     callback_data=f"cat_newword_{cid}"),
            InlineKeyboardButton("◀️ Kategoriyaga qaytish",   callback_data=f"cat_open_{cid}"),
        )
        await message.reply("\n".join(parts), reply_markup=kb, parse_mode="Markdown")
    await state.finish()

# ======================
# VAQT KOMANDA
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
# MAIN
# ======================
async def on_startup(dp):
    asyncio.create_task(daily_reminder_loop())
    logging.info("Deutsch Fenix v5.0 ishga tushdi ✅")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
