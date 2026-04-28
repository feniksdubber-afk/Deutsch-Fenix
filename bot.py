import os
import sqlite3
import logging
import random
from datetime import date, timedelta
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

# ======================
# STATES
# ======================
class QuizState(StatesGroup):
    waiting_for_answer  = State()   # DE->UZ yoki UZ->DE (yozma)
    waiting_for_article = State()   # Artikl testi (tugma)

class CategoryState(StatesGroup):
    waiting_name        = State()
    waiting_assign_word = State()

# ======================
# DATABASE
# ======================
DB_PATH = os.getenv("DB_PATH", "words.db")
db_dir = os.path.dirname(DB_PATH)
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
        correct_count INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS categories (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE
    );
    CREATE TABLE IF NOT EXISTS stats (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        test_date TEXT NOT NULL,
        correct   INTEGER DEFAULT 0,
        wrong     INTEGER DEFAULT 0
    );
""")

# Mavjud DB uchun yangi ustunlar (xato chiqmasin)
for col_sql in [
    "ALTER TABLE words ADD COLUMN category_id   INTEGER DEFAULT NULL",
    "ALTER TABLE words ADD COLUMN wrong_count   INTEGER DEFAULT 0",
    "ALTER TABLE words ADD COLUMN correct_count INTEGER DEFAULT 0",
]:
    try:
        cursor.execute(col_sql)
    except Exception:
        pass

conn.commit()

# ======================
# YORDAMCHI FUNKSIYALAR
# ======================

def simplify_uz(text):
    """O'zbekcha matnni solishtirish uchun normallashtirish"""
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
    if "-" not in line:
        return None
    left, right = line.split("-", 1)
    left, right = left.strip(), right.strip()
    if not left or not right:
        return None
    article, german = "", left
    for art in ["der ","die ","das "]:
        if left.lower().startswith(art):
            article = art.strip()
            german  = left[len(art):].strip().capitalize()
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
    streak      = 0
    check_date  = date.today()
    for (d_str,) in rows:
        d = date.fromisoformat(d_str)
        if d == check_date or d == check_date - timedelta(days=1):
            streak    += 1
            check_date = d - timedelta(days=1)
        else:
            break
    return streak

# ======================
# TEST YUBORISH (markaziy funksiya)
# ======================
async def send_test_question(message: types.Message, state: FSMContext, category_id=None):
    await state.finish()

    if category_id:
        cursor.execute(
            "SELECT id, german, uzbek, article FROM words WHERE archived=0 AND category_id=? ORDER BY RANDOM() LIMIT 1",
            (category_id,)
        )
    else:
        cursor.execute(
            "SELECT id, german, uzbek, article FROM words WHERE archived=0 ORDER BY RANDOM() LIMIT 1"
        )
    word = cursor.fetchone()

    if not word:
        label = "Bu kategoriyada" if category_id else "Aktiv"
        await message.answer(f"⚠️ {label} so'zlar yo'q.")
        return

    word_id, german, uzbek, article = word
    display_german = format_german(german, article)

    # Test turini tasodifiy tanlash
    modes = ["de_uz", "uz_de"]
    if article:
        modes.append("article")
    mode = random.choice(modes)

    await state.update_data(
        word_id=word_id, german=german, uzbek=uzbek,
        article=article, display_german=display_german,
        mode=mode, category_id=category_id
    )

    nav_kb = InlineKeyboardMarkup(row_width=2)

    if mode == "de_uz":
        nav_kb.add(
            InlineKeyboardButton("👁 Ko'rsatish", callback_data=f"show_{word_id}"),
            InlineKeyboardButton("✅ Yod oldim",  callback_data=f"archive_{word_id}"),
        )
        nav_kb.add(InlineKeyboardButton("🔁 Keyingisi", callback_data="next"))
        await message.answer(
            f"🇩🇪 → 🇺🇿  Tarjimasi nima?\n\n👉 *{display_german}*",
            reply_markup=nav_kb, parse_mode="Markdown"
        )
        await QuizState.waiting_for_answer.set()

    elif mode == "uz_de":
        nav_kb.add(
            InlineKeyboardButton("👁 Ko'rsatish", callback_data=f"show_{word_id}"),
            InlineKeyboardButton("✅ Yod oldim",  callback_data=f"archive_{word_id}"),
        )
        nav_kb.add(InlineKeyboardButton("🔁 Keyingisi", callback_data="next"))
        await message.answer(
            f"🇺🇿 → 🇩🇪  Nemischasi nima?\n\n👉 *{uzbek}*",
            reply_markup=nav_kb, parse_mode="Markdown"
        )
        await QuizState.waiting_for_answer.set()

    else:  # article
        art_kb = InlineKeyboardMarkup(row_width=3)
        art_kb.add(
            InlineKeyboardButton("der", callback_data="art_der"),
            InlineKeyboardButton("die", callback_data="art_die"),
            InlineKeyboardButton("das", callback_data="art_das"),
        )
        art_kb.add(InlineKeyboardButton("🔁 Keyingisi", callback_data="next"))
        await message.answer(
            f"🎯 Artiklni toping:\n\n👉 *___ {german}*",
            reply_markup=art_kb, parse_mode="Markdown"
        )
        await QuizState.waiting_for_article.set()

# ======================
# /start
# ======================
@dp.message_handler(commands=["start"], state="*")
async def start(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer(
        "🇩🇪 *Deutsch Fenix v3.0*\n\n"
        "📥 *So'z qo'shish:*\n"
        "`der Tisch - stol`\n"
        "`laufen - yugurmoq`\n"
        "_(bir nechta qator ham bo'ladi)_\n\n"
        "📚 *Komandalar:*\n"
        "/test — Aralash test (DE↔UZ + artikl)\n"
        "/hard — Eng qiyin so'zlar\n"
        "/stats — Statistika & streak 🔥\n"
        "/list — Barcha so'zlar\n"
        "/archived — Arxivlangan so'zlar\n"
        "/kategoriya — Kategoriyalar",
        parse_mode="Markdown"
    )

# ======================
# /test
# ======================
@dp.message_handler(commands=["test"], state="*")
async def test_cmd(message: types.Message, state: FSMContext):
    data = await state.get_data()
    cat_id = data.get("active_category")
    await send_test_question(message, state, category_id=cat_id)

# ======================
# /hard
# ======================
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
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("👁 Ko'rsatish", callback_data=f"show_{word_id}"),
        InlineKeyboardButton("✅ Yod oldim",  callback_data=f"archive_{word_id}"),
    )
    kb.add(InlineKeyboardButton("🔁 Keyingisi", callback_data="hard_next"))

    await message.answer(
        f"💀 *Qiyin so'z* ({wrong_count}x xato)\n\nTarjimasi nima?\n\n👉 *{display_german}*",
        reply_markup=kb, parse_mode="Markdown"
    )
    await QuizState.waiting_for_answer.set()

# ======================
# /stats
# ======================
@dp.message_handler(commands=["stats"], state="*")
async def stats(message: types.Message, state: FSMContext):
    await state.finish()

    cursor.execute("SELECT COUNT(*) FROM words WHERE archived=0")
    active = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM words WHERE archived=1")
    archived_cnt = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM words WHERE wrong_count>2 AND archived=0")
    hard_cnt = cursor.fetchone()[0]

    today = str(date.today())
    cursor.execute("SELECT correct, wrong FROM stats WHERE test_date=?", (today,))
    row = cursor.fetchone()
    t_c, t_w = (row[0], row[1]) if row else (0, 0)

    cursor.execute("SELECT SUM(correct), SUM(wrong) FROM stats")
    total = cursor.fetchone()
    tot_c = total[0] or 0
    tot_w = total[1] or 0
    tot   = tot_c + tot_w
    acc   = round(tot_c / tot * 100) if tot else 0

    streak = get_streak()
    s_emoji = "🔥" if streak >= 3 else "📅"

    await message.answer(
        f"📊 *Statistika*\n\n"
        f"📚 So'zlar: *{active}* aktiv · *{archived_cnt}* arxiv\n"
        f"💀 Qiyin (3+ xato): *{hard_cnt}* ta\n\n"
        f"📅 Bugun:  ✅ {t_c}  ❌ {t_w}\n"
        f"🏆 Jami:   ✅ {tot_c}  ❌ {tot_w}\n"
        f"🎯 Aniqlik: *{acc}%*\n\n"
        f"{s_emoji} Streak: *{streak}* kun",
        parse_mode="Markdown"
    )

# ======================
# /list
# ======================
@dp.message_handler(commands=["list"], state="*")
async def list_words(message: types.Message, state: FSMContext):
    await state.finish()
    cursor.execute("""
        SELECT w.german, w.article, w.uzbek, c.name
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
    for german, article, uzbek, cat_name in words:
        display  = format_german(german, article)
        cat_str  = f" _{cat_name}_" if cat_name else ""
        text    += f"• *{display}* — {uzbek}{cat_str}\n"
    for i in range(0, len(text), 4000):
        await message.answer(text[i:i+4000], parse_mode="Markdown")

# ======================
# /archived
# ======================
@dp.message_handler(commands=["archived"], state="*")
async def list_archived(message: types.Message, state: FSMContext):
    await state.finish()
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

# ======================
# /kategoriya
# ======================
@dp.message_handler(commands=["kategoriya"], state="*")
async def kategoriya_menu(message: types.Message, state: FSMContext):
    await state.finish()
    cursor.execute("SELECT id, name FROM categories ORDER BY name")
    cats = cursor.fetchall()

    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("➕ Yangi kategoriya", callback_data="cat_new"))
    for cat_id, cat_name in cats:
        cursor.execute("SELECT COUNT(*) FROM words WHERE category_id=? AND archived=0", (cat_id,))
        cnt = cursor.fetchone()[0]
        kb.add(InlineKeyboardButton(f"📁 {cat_name} ({cnt} ta)", callback_data=f"cat_open_{cat_id}"))

    await message.answer("📁 *Kategoriyalar:*", reply_markup=kb, parse_mode="Markdown")

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
        is_correct      = simplify_uz(user_input) == simplify_uz(uzbek)
        correct_display = uzbek
        question_display = display_g
    else:  # uz_de
        u_norm          = user_input.lower()
        full_correct    = format_german(german, article).lower()
        is_correct      = u_norm == full_correct or u_norm == german.lower()
        correct_display  = format_german(german, article)
        question_display = uzbek

    if is_correct:
        cursor.execute("UPDATE words SET correct_count = correct_count + 1 WHERE id=?", (word_id,))
        conn.commit()
        log_stat(True)
        await message.reply(
            f"🌟 *BARAKALLA!*\n_{question_display}_ = {correct_display}",
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
        next_cb = "hard_next" if hard_mode else "next"
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🔁 Keyingisi",   callback_data=next_cb),
            InlineKeyboardButton("✅ Yod oldim",   callback_data=f"archive_{word_id}"),
        )
        await message.answer(
            f"❌ *Xato!*\n\nTo'g'ri javob: `{correct_display}`",
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
        await call.message.answer(
            f"🌟 *BARAKALLA!*\n_{chosen} {german}_ = {uzbek}",
            parse_mode="Markdown"
        )
        await send_test_question(call.message, state, category_id=cat_id)
    else:
        cursor.execute("UPDATE words SET wrong_count = wrong_count + 1 WHERE id=?", (word_id,))
        conn.commit()
        log_stat(False)
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🔁 Keyingisi", callback_data="next"),
            InlineKeyboardButton("✅ Yod oldim",  callback_data=f"archive_{word_id}"),
        )
        await call.message.answer(
            f"❌ *Xato!* Sen: `{chosen}`\n\nTo'g'ri: `{correct} {german}` = {uzbek}",
            reply_markup=kb, parse_mode="Markdown"
        )

# ======================
# SO'Z QO'SHISH
# ======================
@dp.message_handler(state=None)
async def bulk_add(message: types.Message):
    lines = message.text.strip().split('\n')
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
        if cursor.fetchone():
            duplicates.append(format_german(german, article))
            continue
        cursor.execute(
            "INSERT INTO words (german, uzbek, article) VALUES (?, ?, ?)",
            (german, uzbek, article)
        )
        added += 1

    if added > 0:
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

# ======================
# CALLBACK HANDLER (umumiy)
# ======================
@dp.callback_query_handler(state="*")
async def process_callback(call: types.CallbackQuery, state: FSMContext):
    d       = call.data
    st_data = await state.get_data()
    cat_id  = st_data.get("category_id")

    if d == "next":
        await call.message.delete()
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
    elif d == "cat_new":
        await call.message.answer("📁 Yangi kategoriya nomini yozing:")
        await CategoryState.waiting_name.set()

    elif d.startswith("cat_open_"):
        cid = int(d.split("_")[2])
        cursor.execute("SELECT name FROM categories WHERE id=?", (cid,))
        row = cursor.fetchone()
        if not row:
            await call.answer("Topilmadi.", show_alert=True)
            return
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("🎯 Test boshlash",    callback_data=f"cat_test_{cid}"),
            InlineKeyboardButton("📋 So'zlarini ko'rish", callback_data=f"cat_list_{cid}"),
            InlineKeyboardButton("🔗 So'z biriktirish",  callback_data=f"cat_assign_{cid}"),
            InlineKeyboardButton("🗑 O'chirish",          callback_data=f"cat_del_{cid}"),
        )
        await call.message.edit_text(f"📁 *{row[0]}*", reply_markup=kb, parse_mode="Markdown")

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
            "So'zlarni qatorma-qator yozing (artikl bilan yoki artiklsiz):\n"
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

# ======================
# KATEGORIYA STATELARI
# ======================
@dp.message_handler(state=CategoryState.waiting_name)
async def cat_save_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    if not name:
        await message.answer("❌ Nom bo'sh bo'lishi mumkin emas.")
        return
    try:
        cursor.execute("INSERT INTO categories (name) VALUES (?)", (name,))
        conn.commit()
        await message.answer(f"✅ *{name}* kategoriyasi yaratildi!", parse_mode="Markdown")
    except sqlite3.IntegrityError:
        await message.answer(f"⚠️ *{name}* allaqachon mavjud.", parse_mode="Markdown")
    await state.finish()

@dp.message_handler(state=CategoryState.waiting_assign_word)
async def cat_assign_words(message: types.Message, state: FSMContext):
    data    = await state.get_data()
    cid     = data.get("assign_cat_id")
    lines   = message.text.strip().split('\n')
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

# ======================
# MAIN
# ======================
if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
