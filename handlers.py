import asyncio
import os
import re
from datetime import datetime, timezone, timedelta
from aiogram import Router, F, Bot
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import CommandStart, Command

from config import ADMIN_IDS, SUPERADMIN_IDS, PVZ_ADDRESS, GROUP_CHAT_ID
import database as db

router = Router()
ALL_ADMINS = list(set(ADMIN_IDS + SUPERADMIN_IDS))
MSK = timezone(timedelta(hours=3))

# Состояния пользователей
user_state = {}
shift_cache = {}
break_cache = {}


async def update_user_cache(uid: int):
    shift = await db.get_active_shift(uid)
    break_active = await db.get_active_break(uid)
    shift_cache[uid] = {
        "shift_active": shift is not None,
        "break_active": break_active is not None,
        "shift_opened_at": shift["opened_at"] if shift else None
    }


def get_keyboard(uid: int, shift_active: bool = None, break_active: bool = None):
    is_sa = uid in SUPERADMIN_IDS
    is_adm = uid in ALL_ADMINS
    
    if shift_active is None:
        cached = shift_cache.get(uid, {})
        shift_active = cached.get("shift_active", False)
        break_active = cached.get("break_active", False)
    
    keyboard = []
    
    if not shift_active:
        keyboard.append([KeyboardButton(text="🟢 Открыть смену")])
    else:
        if not break_active:
            keyboard.append([KeyboardButton(text="☕ Перерыв")])
        else:
            keyboard.append([KeyboardButton(text="✅ Закончить перерыв")])
        keyboard.append([KeyboardButton(text="🔴 Закрыть смену")])
    
    if is_sa or is_adm:
        keyboard.append([KeyboardButton(text="👥 Сотрудники"), KeyboardButton(text="📊 Статистика")])
        keyboard.append([KeyboardButton(text="🟢 Активные")])
    
    if is_sa:
        keyboard.append([KeyboardButton(text="⚙️ Админ-панель")])
        keyboard.append([KeyboardButton(text="📜 История правок")])
    
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def cancel_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True)


def admin_panel_kb():
    keyboard = [
        [KeyboardButton(text="✏️ Редактировать сотрудника")],
        [KeyboardButton(text="⏰ Редактировать смену")],
        [KeyboardButton(text="📋 Все сотрудники")],
        [KeyboardButton(text="💾 Создать бэкап")],
        [KeyboardButton(text="📜 История правок")],
        [KeyboardButton(text="🔙 Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def edit_employee_kb():
    keyboard = [
        [KeyboardButton(text="📝 Изменить ФИО")],
        [KeyboardButton(text="🆔 Изменить WB ID")],
        [KeyboardButton(text="❌ Удалить сотрудника")],
        [KeyboardButton(text="🔙 Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


# ---------- Утилиты ----------

def fmt_time(s: str):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").strftime("%H:%M")
    except:
        return s[:5] if len(s) > 5 else s


def fmt_datetime(s: str):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y %H:%M")
    except:
        return s


def now_str():
    return datetime.now(MSK).strftime("%d.%m.%Y %H:%M")


def now_db():
    return datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S")


def fmt_duration(minutes: int):
    if minutes < 0:
        return "0 мин"
    if minutes < 60:
        return f"{minutes} мин"
    h = minutes // 60
    m = minutes % 60
    return f"{h}ч {m}м" if m else f"{h}ч"


async def notify_admins(bot: Bot, text: str, photo_id: str = None):
    for aid in ALL_ADMINS:
        try:
            if photo_id:
                await bot.send_photo(aid, photo=photo_id, caption=text, parse_mode="Markdown")
            else:
                await bot.send_message(aid, text, parse_mode="Markdown")
        except:
            pass


async def send_to_chat(bot: Bot, text: str, photo_id: str = None):
    if GROUP_CHAT_ID:
        try:
            if photo_id:
                await bot.send_photo(GROUP_CHAT_ID, photo=photo_id, caption=text, parse_mode="Markdown")
            else:
                await bot.send_message(GROUP_CHAT_ID, text, parse_mode="Markdown")
        except:
            pass


async def log_edit(admin_id: int, admin_name: str, edit_type: str, target_id: int, target_name: str, old_value: str, new_value: str, reason: str):
    """Сохранить запись о редактировании в БД"""
    await db.add_edit_log(admin_id, admin_name, edit_type, target_id, target_name, old_value, new_value, reason)


async def send_weekly_report_to_chat(bot: Bot):
    employees = await db.get_approved_employees()
    if not employees:
        return
    
    now = datetime.now(MSK)
    monday = now - timedelta(days=now.weekday())
    lines = [f"📊 *Итоги недели {monday.strftime('%d.%m')}–{now.strftime('%d.%m.%Y')}*\n"]
    
    for emp in employees:
        shifts = await db.get_shifts_this_week(emp["telegram_id"])
        breaks = await db.get_breaks_for_week(emp["telegram_id"])
        
        shift_mins = sum(s.get("duration_minutes", 0) for s in shifts)
        break_mins = sum(b.get("duration_minutes", 0) for b in breaks)
        
        lines.append(f"👤 *{emp['full_name']}*\n   📅 Смен: {len(shifts)} ({fmt_duration(shift_mins)})\n   ☕ Перерывов: {len(breaks)} ({fmt_duration(break_mins)})\n")
    
    await send_to_chat(bot, "\n".join(lines))


# ---------- Команды ----------

@router.message(CommandStart())
async def cmd_start(message: Message):
    uid = message.from_user.id
    emp = await db.get_employee(uid)
    await update_user_cache(uid)
    
    if uid in SUPERADMIN_IDS and not emp:
        user_state[uid] = "waiting_name"
        await message.answer("👋 Суперадмин! Введите ваше ФИО:", reply_markup=cancel_kb())
        return
    
    if uid in ALL_ADMINS and not emp:
        user_state[uid] = "waiting_name"
        await message.answer("👋 Админ! Введите ваше ФИО:", reply_markup=cancel_kb())
        return
    
    if not emp:
        user_state[uid] = "waiting_name"
        await message.answer("👋 Добро пожаловать! Введите ваше ФИО для регистрации:", reply_markup=cancel_kb())
        return
    
    if not emp["approved"]:
        await message.answer("⏳ Ваша заявка на одобрении.")
        return
    
    menu = get_keyboard(uid)
    await message.answer(f"👋 С возвращением, {emp['full_name']}!", reply_markup=menu)


@router.message(Command("help"))
async def cmd_help(message: Message):
    uid = message.from_user.id
    text = """
📋 *Помощь*

👤 *Сотрудник:*
🟢 Открыть смену — начало работы (нужно фото)
☕ Перерыв — начало перерыва (нужно фото)
✅ Закончить перерыв — завершение
🔴 Закрыть смену — конец работы (нужно фото)

👑 *Админ:*
👥 Сотрудники — список всех
📊 Статистика — отчёт по сотруднику
🟢 Активные — кто сейчас на смене

⚙️ *Суперадмин:*
✏️ Редактировать сотрудника — ФИО, WB ID, удаление
⏰ Редактировать смену — изменить время начала/конца
📋 Все сотрудники — полный список с ID
💾 Создать бэкап — резервная копия БД
📜 История правок — все изменения

⌨️ *Команды:*
/start — главное меню
/help — эта справка
/active — кто на смене
/stats — статистика за сегодня
/rating — рейтинг сотрудников
/mystats — моя статистика
/top — топ сегодняшних
/backup — создать бэкап БД
/history — история правок
"""
    await message.answer(text, parse_mode="Markdown")


@router.message(Command("history"))
async def cmd_history(message: Message):
    uid = message.from_user.id
    if uid not in SUPERADMIN_IDS:
        await message.answer("❗ Только для суперадминов")
        return
    
    logs = await db.get_edit_logs(limit=20)
    if not logs:
        await message.answer("📜 История правок пуста")
        return
    
    lines = ["📜 *История правок (последние 20)*\n"]
    for log in logs:
        lines.append(
            f"🕐 {log['created_at'][:16]}\n"
            f"👤 {log['admin_name']}\n"
            f"📌 {log['edit_type']}\n"
            f"🎯 {log['target_name']}\n"
            f"📝 {log['reason'][:50]}{'...' if len(log['reason']) > 50 else ''}\n"
        )
    
    await message.answer("\n".join(lines), parse_mode="Markdown")


@router.message(Command("active"))
async def cmd_active(message: Message):
    shifts = await db.get_all_active_shifts()
    if not shifts:
        await message.answer("😴 Нет открытых смен")
        return
    
    now = datetime.now(MSK)
    lines = [f"👁 *Активные смены ({len(shifts)})*:\n"]
    
    for s in shifts:
        opened = datetime.strptime(s["opened_at"], "%Y-%m-%d %H:%M:%S")
        dur = int((now - opened).total_seconds() // 60)
        break_active = await db.get_active_break(s["telegram_id"])
        status = "☕ перерыв" if break_active else "🟢 работает"
        lines.append(f"• *{s['full_name']}* — {status}\n  с {fmt_time(s['opened_at'])} ({fmt_duration(dur)})")
    
    await message.answer("\n".join(lines), parse_mode="Markdown")


@router.message(Command("chatid"))
async def cmd_chatid(message: Message):
    await message.answer(f"Chat ID: `{message.chat.id}`", parse_mode="Markdown")


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    uid = message.from_user.id
    if uid not in ALL_ADMINS:
        await message.answer("❗ Только для админов")
        return
    
    today = await db.get_today_stats()
    rating = await db.get_employee_rating()
    
    text = f"📊 *Статистика за сегодня*\n\n"
    text += f"👥 Активных сотрудников: {today['active_employees']}\n"
    text += f"🟢 Открыто смен: {today['total_shifts']}\n"
    text += f"⏱ Отработано: {fmt_duration(today['total_minutes'])}\n\n"
    
    text += "🏆 *Топ сотрудников по часам:*\n"
    for i, emp in enumerate(rating[:5], 1):
        text += f"{i}. {emp['full_name']} — {fmt_duration(emp['total_work_minutes'])} ({emp['total_shifts']} смен)\n"
    
    await message.answer(text, parse_mode="Markdown")


@router.message(Command("rating"))
async def cmd_rating(message: Message):
    rating = await db.get_employee_rating()
    if not rating:
        await message.answer("Нет данных")
        return
    
    text = "🏆 *Рейтинг сотрудников*\n\n"
    medals = ["🥇", "🥈", "🥉"]
    for i, emp in enumerate(rating[:10]):
        medal = medals[i] if i < 3 else f"{i+1}."
        text += f"{medal} *{emp['full_name']}*\n"
        text += f"   📅 {emp['total_shifts']} смен | ⏱ {fmt_duration(emp['total_work_minutes'])}\n\n"
    
    await message.answer(text, parse_mode="Markdown")


@router.message(Command("mystats"))
async def cmd_mystats(message: Message):
    uid = message.from_user.id
    emp = await db.get_employee(uid)
    
    if not emp or not emp["approved"]:
        await message.answer("❗ Вы не зарегистрированы")
        return
    
    shifts = await db.get_shifts_this_week(uid)
    breaks = await db.get_breaks_for_week(uid)
    
    shift_mins = sum(s.get("duration_minutes", 0) for s in shifts)
    break_mins = sum(b.get("duration_minutes", 0) for b in breaks)
    
    text = f"📊 *Твоя статистика*\n\n"
    text += f"👤 {emp['full_name']}\n"
    text += f"🏷 WB ID: `{emp['wb_employee_id']}`\n\n"
    text += f"📅 Смен за неделю: {len(shifts)}\n"
    text += f"⏱ Отработано: {fmt_duration(shift_mins)}\n"
    text += f"☕ Перерывов: {len(breaks)}\n"
    text += f"⏸ Время перерывов: {fmt_duration(break_mins)}\n"
    
    await message.answer(text, parse_mode="Markdown")


@router.message(Command("top"))
async def cmd_top(message: Message):
    import aiosqlite
    async with aiosqlite.connect(db.DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("""
            SELECT e.full_name, COUNT(s.id) as shift_count
            FROM shifts s
            JOIN employees e ON s.telegram_id = e.telegram_id
            WHERE date(s.opened_at) = date(datetime('now', '+3 hours'))
            GROUP BY s.telegram_id
            ORDER BY shift_count DESC
            LIMIT 5
        """) as cur:
            today_top = await cur.fetchall()
    
    if not today_top:
        await message.answer("Сегодня никто не работал 😴")
        return
    
    text = "🔥 *Топ сегодняшних трудяг*\n\n"
    for i, emp in enumerate(today_top, 1):
        text += f"{i}. {emp['full_name']} — {emp['shift_count']} смен\n"
    
    await message.answer(text, parse_mode="Markdown")


@router.message(Command("backup"))
async def cmd_backup(message: Message, bot: Bot):
    uid = message.from_user.id
    if uid not in SUPERADMIN_IDS:
        await message.answer("❗ Только для суперадминов")
        return
    
    await message.answer("⏳ Создаю резервную копию...")
    backup_path = await db.create_backup()
    
    if backup_path and os.path.exists(backup_path):
        await message.answer(f"✅ Бэкап создан: `{backup_path}`", parse_mode="Markdown")
        await bot.send_document(uid, open(backup_path, 'rb'))
    else:
        await message.answer("❌ Не удалось создать бэкап")


# ---------- Отмена и назад ----------

@router.message(F.text == "❌ Отмена")
async def cancel(message: Message):
    uid = message.from_user.id
    if uid in user_state:
        del user_state[uid]
    await update_user_cache(uid)
    await message.answer("❌ Отменено", reply_markup=get_keyboard(uid))


@router.message(F.text == "🔙 Назад")
async def back_to_main(message: Message):
    uid = message.from_user.id
    if uid in user_state:
        del user_state[uid]
    await update_user_cache(uid)
    await message.answer("🔙 Вернулся в главное меню", reply_markup=get_keyboard(uid))


# ---------- Регистрация ----------

@router.message(lambda m: user_state.get(m.from_user.id) == "waiting_name")
async def reg_name(message: Message):
    name = message.text.strip()
    if len(name) < 3:
        await message.answer("❗ Минимум 3 символа. Введите ФИО:")
        return
    user_state[message.from_user.id] = f"waiting_wb:{name}"
    await message.answer(f"✅ Имя: {name}\n\n🔢 Введите ID сотрудника WB:", reply_markup=cancel_kb())


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("waiting_wb:"))
async def reg_wb(message: Message):
    uid = message.from_user.id
    wb_id = message.text.strip()
    name = user_state[uid].split(":", 1)[1]
    
    await db.register_employee(uid, name, wb_id)
    del user_state[uid]
    
    if uid in SUPERADMIN_IDS or uid in ALL_ADMINS:
        await db.approve_employee(uid)
        await update_user_cache(uid)
        await message.answer(f"✅ Добро пожаловать, {name}!", reply_markup=get_keyboard(uid))
    else:
        await message.answer("✅ Заявка отправлена! Ожидайте одобрения.")
        
        username = f"@{message.from_user.username}" if message.from_user.username else "нет"
        for aid in SUPERADMIN_IDS:
            try:
                await message.bot.send_message(aid, f"🆕 Новая заявка!\n👤 {name}\n🆔 {uid}\n📱 {username}\n🏷 {wb_id}")
            except:
                pass


# ========== АДМИН-ПАНЕЛЬ ==========

@router.message(F.text == "⚙️ Админ-панель")
async def admin_panel(message: Message):
    uid = message.from_user.id
    if uid not in SUPERADMIN_IDS:
        await message.answer("❗ Нет доступа")
        return
    
    await message.answer("⚙️ *Админ-панель*\n\nВыберите действие:", parse_mode="Markdown", reply_markup=admin_panel_kb())


@router.message(F.text == "📋 Все сотрудники")
async def all_employees_list(message: Message):
    uid = message.from_user.id
    if uid not in SUPERADMIN_IDS:
        await message.answer("❗ Нет доступа")
        return
    
    employees = await db.get_all_employees()
    if not employees:
        await message.answer("Сотрудников пока нет")
        return
    
    lines = ["📋 *Полный список сотрудников:*\n"]
    for emp in employees:
        status = "✅" if emp["approved"] else "⏳"
        lines.append(f"{status} *{emp['full_name']}*\n   🆔 Telegram: `{emp['telegram_id']}`\n   🏷 WB ID: `{emp['wb_employee_id']}`\n   📅 Регистрация: {emp['registered_at']}\n   📊 Смен: {emp.get('total_shifts', 0)}\n")
    
    full_text = "\n".join(lines)
    if len(full_text) > 4000:
        for i in range(0, len(full_text), 4000):
            await message.answer(full_text[i:i+4000], parse_mode="Markdown")
    else:
        await message.answer(full_text, parse_mode="Markdown")


@router.message(F.text == "💾 Создать бэкап")
async def admin_backup(message: Message, bot: Bot):
    uid = message.from_user.id
    if uid not in SUPERADMIN_IDS:
        await message.answer("❗ Нет доступа")
        return
    
    await message.answer("⏳ Создаю резервную копию...")
    backup_path = await db.create_backup()
    
    if backup_path and os.path.exists(backup_path):
        await message.answer(f"✅ Бэкап создан: `{backup_path}`", parse_mode="Markdown")
        await bot.send_document(uid, open(backup_path, 'rb'))
    else:
        await message.answer("❌ Не удалось создать бэкап")


@router.message(F.text == "📜 История правок")
async def show_edit_history(message: Message):
    uid = message.from_user.id
    if uid not in SUPERADMIN_IDS:
        await message.answer("❗ Только для суперадминов")
        return
    
    logs = await db.get_edit_logs(limit=30)
    if not logs:
        await message.answer("📜 История правок пуста")
        return
    
    lines = ["📜 *История правок (последние 30)*\n"]
    for log in logs:
        lines.append(
            f"🕐 {log['created_at'][:16]}\n"
            f"👤 {log['admin_name']}\n"
            f"📌 {log['edit_type']}\n"
            f"🎯 {log['target_name']}\n"
            f"📝 Причина: {log['reason'][:60]}{'...' if len(log['reason']) > 60 else ''}\n"
            f"---\n"
        )
    
    full_text = "\n".join(lines)
    if len(full_text) > 4000:
        for i in range(0, len(full_text), 4000):
            await message.answer(full_text[i:i+4000], parse_mode="Markdown")
    else:
        await message.answer(full_text, parse_mode="Markdown")


# ---------- Редактирование сотрудника ----------

@router.message(F.text == "✏️ Редактировать сотрудника")
async def edit_employee_start(message: Message):
    uid = message.from_user.id
    if uid not in SUPERADMIN_IDS:
        await message.answer("❗ Нет доступа")
        return
    
    employees = await db.get_all_employees()
    if not employees:
        await message.answer("Нет сотрудников для редактирования")
        return
    
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=emp["full_name"])] for emp in employees] + [[KeyboardButton(text="🔙 Назад")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    user_state[uid] = "edit_select_employee"
    await message.answer("👥 Выберите сотрудника для редактирования:", reply_markup=kb)


@router.message(lambda m: user_state.get(m.from_user.id) == "edit_select_employee")
async def edit_employee_selected(message: Message):
    uid = message.from_user.id
    name = message.text.strip()
    
    if name == "🔙 Назад":
        del user_state[uid]
        await message.answer("⚙️ Админ-панель", reply_markup=admin_panel_kb())
        return
    
    emp = await db.get_employee_by_name(name)
    if not emp:
        await message.answer("Сотрудник не найден, попробуйте ещё раз")
        return
    
    user_state[uid] = f"edit_employee:{emp['telegram_id']}:{emp['full_name']}"
    await message.answer(
        f"✏️ *Редактирование сотрудника*\n\n"
        f"📝 ФИО: {emp['full_name']}\n"
        f"🆔 WB ID: {emp['wb_employee_id']}\n"
        f"📊 Статус: {'✅ Одобрен' if emp['approved'] else '⏳ Ожидает'}\n"
        f"📅 Смен всего: {emp.get('total_shifts', 0)}\n"
        f"⏱ Отработано: {fmt_duration(emp.get('total_work_minutes', 0))}\n\n"
        f"Что хотите изменить?",
        parse_mode="Markdown",
        reply_markup=edit_employee_kb()
    )


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_employee:") and m.text == "📝 Изменить ФИО")
async def edit_employee_name_start(message: Message):
    uid = message.from_user.id
    user_state[uid] = user_state[uid] + ":edit_name"
    await message.answer("📝 Введите новое ФИО сотрудника:", reply_markup=cancel_kb())


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_employee:") and ":edit_name" in user_state.get(m.from_user.id, ""))
async def edit_employee_name_save(message: Message, bot: Bot):
    uid = message.from_user.id
    new_name = message.text.strip()
    
    if len(new_name) < 3:
        await message.answer("❗ Минимум 3 символа. Введите ФИО:")
        return
    
    parts = user_state[uid].split(":")
    emp_id = int(parts[1])
    old_name = parts[2]
    
    # Запрашиваем причину
    user_state[uid] = f"edit_employee_reason:{emp_id}:{old_name}:name:{new_name}"
    await message.answer("📝 Укажите причину изменения ФИО:", reply_markup=cancel_kb())


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_employee_reason:") and "name" in user_state.get(m.from_user.id, ""))
async def edit_employee_name_reason(message: Message, bot: Bot):
    uid = message.from_user.id
    reason = message.text.strip()
    
    parts = user_state[uid].split(":")
    emp_id = int(parts[1])
    old_name = parts[2]
    new_name = parts[4]
    
    admin = await db.get_employee(uid)
    admin_name = admin["full_name"] if admin else str(uid)
    
    await db.update_employee_name(emp_id, new_name)
    await log_edit(uid, admin_name, "Изменение ФИО", emp_id, old_name, old_name, new_name, reason)
    
    del user_state[uid]
    await message.answer(f"✅ ФИО сотрудника изменено с *{old_name}* на *{new_name}*", parse_mode="Markdown", reply_markup=admin_panel_kb())
    await notify_admins(bot, f"✏️ *Изменение ФИО*\n👤 {admin_name}\n🎯 {old_name} → {new_name}\n📝 Причина: {reason}")


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_employee:") and m.text == "🆔 Изменить WB ID")
async def edit_employee_wb_start(message: Message):
    uid = message.from_user.id
    user_state[uid] = user_state[uid] + ":edit_wb"
    await message.answer("🆔 Введите новый WB ID сотрудника:", reply_markup=cancel_kb())


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_employee:") and ":edit_wb" in user_state.get(m.from_user.id, ""))
async def edit_employee_wb_save(message: Message, bot: Bot):
    uid = message.from_user.id
    new_wb = message.text.strip()
    
    if not new_wb:
        await message.answer("❗ Введите WB ID:")
        return
    
    parts = user_state[uid].split(":")
    emp_id = int(parts[1])
    emp_name = parts[2]
    old_wb = (await db.get_employee(emp_id))["wb_employee_id"]
    
    user_state[uid] = f"edit_employee_reason:{emp_id}:{emp_name}:wb:{old_wb}:{new_wb}"
    await message.answer("📝 Укажите причину изменения WB ID:", reply_markup=cancel_kb())


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_employee_reason:") and "wb" in user_state.get(m.from_user.id, ""))
async def edit_employee_wb_reason(message: Message, bot: Bot):
    uid = message.from_user.id
    reason = message.text.strip()
    
    parts = user_state[uid].split(":")
    emp_id = int(parts[1])
    emp_name = parts[2]
    old_wb = parts[4]
    new_wb = parts[5]
    
    admin = await db.get_employee(uid)
    admin_name = admin["full_name"] if admin else str(uid)
    
    await db.update_employee_wb(emp_id, new_wb)
    await log_edit(uid, admin_name, "Изменение WB ID", emp_id, emp_name, old_wb, new_wb, reason)
    
    del user_state[uid]
    await message.answer(f"✅ WB ID сотрудника *{emp_name}* изменён с `{old_wb}` на `{new_wb}`", parse_mode="Markdown", reply_markup=admin_panel_kb())
    await notify_admins(bot, f"✏️ *Изменение WB ID*\n👤 {admin_name}\n🎯 {emp_name}\n🆔 {old_wb} → {new_wb}\n📝 Причина: {reason}")


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_employee:") and m.text == "❌ Удалить сотрудника")
async def edit_employee_delete_confirm(message: Message):
    uid = message.from_user.id
    parts = user_state[uid].split(":")
    name = parts[2]
    
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=f"✅ ДА, удалить {name}")], [KeyboardButton(text="❌ Нет, отмена")]],
        resize_keyboard=True
    )
    user_state[uid] = user_state[uid] + ":confirm_delete"
    await message.answer(f"⚠️ *ВНИМАНИЕ!*\n\nВы действительно хотите удалить сотрудника *{name}*?\n\nВсе его смены и перерывы будут удалены!", parse_mode="Markdown", reply_markup=kb)


@router.message(lambda m: user_state.get(m.from_user.id, "").endswith("confirm_delete") and m.text.startswith("✅ ДА, удалить"))
async def edit_employee_delete_execute(message: Message, bot: Bot):
    uid = message.from_user.id
    parts = user_state[uid].split(":")
    emp_id = int(parts[1])
    emp_name = parts[2]
    
    user_state[uid] = f"edit_employee_reason:{emp_id}:{emp_name}:delete"
    await message.answer("📝 Укажите причину удаления сотрудника:", reply_markup=cancel_kb())


@router.message(lambda m: user_state.get(m.from_user.id, "").endswith("delete"))
async def edit_employee_delete_reason(message: Message, bot: Bot):
    uid = message.from_user.id
    reason = message.text.strip()
    
    parts = user_state[uid].split(":")
    emp_id = int(parts[1])
    emp_name = parts[2]
    
    admin = await db.get_employee(uid)
    admin_name = admin["full_name"] if admin else str(uid)
    
    await db.delete_employee_full(emp_id)
    await log_edit(uid, admin_name, "Удаление сотрудника", emp_id, emp_name, "-", "-", reason)
    
    del user_state[uid]
    await message.answer(f"✅ Сотрудник *{emp_name}* удалён", parse_mode="Markdown", reply_markup=admin_panel_kb())
    await notify_admins(bot, f"🗑 *Удаление сотрудника*\n👤 {admin_name}\n🎯 {emp_name}\n📝 Причина: {reason}")


@router.message(lambda m: user_state.get(m.from_user.id, "").endswith("confirm_delete") and m.text == "❌ Нет, отмена")
async def edit_employee_delete_cancel(message: Message):
    uid = message.from_user.id
    del user_state[uid]
    await message.answer("❌ Удаление отменено", reply_markup=admin_panel_kb())


# ========== РЕДАКТИРОВАНИЕ СМЕНЫ (ПОЛНОСТЬЮ ПЕРЕПИСАНО) ==========

@router.message(F.text == "⏰ Редактировать смену")
async def edit_shift_start(message: Message):
    uid = message.from_user.id
    if uid not in SUPERADMIN_IDS:
        await message.answer("❗ Нет доступа")
        return
    
    employees = await db.get_approved_employees()
    if not employees:
        await message.answer("Нет одобренных сотрудников")
        return
    
    # Простая клавиатура со списком сотрудников
    kb_buttons = []
    for emp in employees:
        kb_buttons.append([KeyboardButton(text=emp["full_name"])])
    kb_buttons.append([KeyboardButton(text="🔙 Назад"])
    
    kb = ReplyKeyboardMarkup(keyboard=kb_buttons, resize_keyboard=True)
    user_state[uid] = "edit_shift_select_employee"
    await message.answer("👥 Выберите сотрудника:", reply_markup=kb)


@router.message(lambda m: user_state.get(m.from_user.id) == "edit_shift_select_employee")
async def edit_shift_list_shifts(message: Message):
    uid = message.from_user.id
    name = message.text.strip()
    
    if name == "🔙 Назад":
        del user_state[uid]
        await message.answer("⚙️ Админ-панель", reply_markup=admin_panel_kb())
        return
    
    emp = await db.get_employee_by_name(name)
    if not emp:
        await message.answer("❌ Сотрудник не найден")
        return
    
    # Сохраняем ID сотрудника
    user_state[uid] = f"edit_shift_list:{emp['telegram_id']}:{emp['full_name']}"
    
    # Получаем смены за последние 14 дней
    shifts = await db.get_employee_shifts_recent(emp["telegram_id"], 14)
    
    if not shifts:
        await message.answer(f"📭 У {emp['full_name']} нет смен за последние 14 дней.\n\nНажмите «🔙 Назад» чтобы выбрать другого сотрудника")
        return
    
    # Показываем список смен в виде текста + просим ввести ID
    shifts_text = f"📋 *Смены сотрудника {emp['full_name']}:*\n\n"
    for s in shifts:
        status = "🟢 АКТИВНА" if not s["closed_at"] else "🔴 ЗАКРЫТА"
        time_str = fmt_datetime(s["opened_at"])
        shifts_text += f"`#{s['id']}` — {status}\n   🕐 Открыта: {time_str}\n"
        if s["closed_at"]:
            shifts_text += f"   🕐 Закрыта: {fmt_datetime(s['closed_at'])}\n"
        shifts_text += f"   ⏱ Длительность: {fmt_duration(s.get('duration_minutes', 0))}\n\n"
    
    await message.answer(
        f"{shifts_text}\n"
        f"✏️ Введите ID смены (число после #), которую хотите отредактировать:\n\n"
        f"Например: `123`",
        parse_mode="Markdown",
        reply_markup=cancel_kb()
    )
    
    # Переходим в состояние ожидания ID смены
    user_state[uid] = f"edit_shift_wait_id:{emp['telegram_id']}:{emp['full_name']}"


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_wait_id:"))
async def edit_shift_get_id(message: Message):
    uid = message.from_user.id
    parts = user_state[uid].split(":")
    emp_tg_id = int(parts[1])
    emp_name = parts[2]
    
    shift_id_text = message.text.strip()
    
    if shift_id_text == "❌ Отмена":
        del user_state[uid]
        await message.answer("❌ Отменено", reply_markup=admin_panel_kb())
        return
    
    try:
        shift_id = int(shift_id_text)
    except ValueError:
        await message.answer("❌ Введите число — ID смены (например: 123)\n\nНажмите «❌ Отмена» для выхода")
        return
    
    shift = await db.get_shift_by_id(shift_id)
    if not shift or shift["telegram_id"] != emp_tg_id:
        await message.answer(f"❌ Смена #{shift_id} не найдена у сотрудника {emp_name}\n\nПроверьте ID и попробуйте снова")
        return
    
    # Сохраняем ID смены и показываем меню действий
    user_state[uid] = f"edit_shift_action:{shift_id}:{emp_tg_id}:{emp_name}"
    
    action_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⏰ Изменить время начала")],
            [KeyboardButton(text="🔴 Изменить время окончания")],
            [KeyboardButton(text="🗑 Удалить смену")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True
    )
    
    status = "🔴 Закрыта" if shift["closed_at"] else "🟢 Активна"
    duration = fmt_duration(shift.get("duration_minutes", 0))
    
    await message.answer(
        f"✏️ *Редактирование смены #{shift_id}*\n\n"
        f"👤 Сотрудник: {emp_name}\n"
        f"🟢 Открыта: {fmt_datetime(shift['opened_at'])}\n"
        f"🔴 Закрыта: {fmt_datetime(shift['closed_at']) if shift['closed_at'] else '—'}\n"
        f"📊 Статус: {status}\n"
        f"⏱ Длительность: {duration}\n\n"
        f"Что хотите сделать?",
        parse_mode="Markdown",
        reply_markup=action_kb
    )


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_action:") and m.text == "⏰ Изменить время начала")
async def edit_shift_change_start_time(message: Message):
    uid = message.from_user.id
    parts = user_state[uid].split(":")
    shift_id = int(parts[1])
    emp_tg_id = int(parts[2])
    emp_name = parts[3]
    
    user_state[uid] = f"edit_shift_start_new:{shift_id}:{emp_tg_id}:{emp_name}"
    await message.answer(
        "⏰ Введите *НОВОЕ ВРЕМЯ НАЧАЛА* смены:\n\n"
        "Варианты:\n"
        "• `2025-01-15 09:00:00` — абсолютное время\n"
        "• `+2 часа` — сдвинуть на 2 часа вперёд\n"
        "• `-30 минут` — сдвинуть назад\n\n"
        "Затем я спрошу причину изменения.",
        parse_mode="Markdown",
        reply_markup=cancel_kb()
    )


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_start_new:"))
async def edit_shift_save_start_time(message: Message):
    uid = message.from_user.id
    parts = user_state[uid].split(":")
    shift_id = int(parts[1])
    emp_tg_id = int(parts[2])
    emp_name = parts[3]
    
    time_value = message.text.strip()
    
    if time_value == "❌ Отмена":
        del user_state[uid]
        await message.answer("❌ Отменено", reply_markup=admin_panel_kb())
        return
    
    # Сохраняем новое время и ждём причину
    user_state[uid] = f"edit_shift_start_reason:{shift_id}:{emp_tg_id}:{emp_name}:{time_value}"
    await message.answer(
        "📝 Укажите *ПРИЧИНУ* изменения времени начала смены:\n\n"
        "Например: «Ошибка при открытии», «Сотрудник задержался» и т.д.",
        parse_mode="Markdown",
        reply_markup=cancel_kb()
    )


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_start_reason:"))
async def edit_shift_apply_start_time(message: Message, bot: Bot):
    uid = message.from_user.id
    reason = message.text.strip()
    
    if reason == "❌ Отмена":
        del user_state[uid]
        await message.answer("❌ Отменено", reply_markup=admin_panel_kb())
        return
    
    parts = user_state[uid].split(":")
    shift_id = int(parts[1])
    emp_tg_id = int(parts[2])
    emp_name = parts[3]
    time_value = parts[4]
    
    shift = await db.get_shift_by_id(shift_id)
    if not shift:
        await message.answer("❌ Смена не найдена")
        del user_state[uid]
        return
    
    old_time = shift["opened_at"]
    
    try:
        # Парсим время
        if time_value.startswith("+") or time_value.startswith("-") or "час" in time_value or "минут" in time_value:
            # Относительное время
            import re
            match = re.match(r"([+-]?\d+)\s*(час|часа|часов|ч|минут|минуты|минуту|мин|м)", time_value.lower())
            if match:
                num = int(match.group(1))
                unit = match.group(2)
                current = datetime.strptime(old_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
                if unit in ["час", "часа", "часов", "ч"]:
                    new_time = current + timedelta(hours=num)
                else:
                    new_time = current + timedelta(minutes=num)
                new_time_str = new_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                raise ValueError("Неверный формат")
        else:
            # Абсолютное время
            new_time = datetime.strptime(time_value, "%Y-%m-%d %H:%M:%S")
            new_time_str = new_time.strftime("%Y-%m-%d %H:%M:%S")
        
        await db.update_shift_start(shift_id, new_time_str)
        
        # Логируем
        admin = await db.get_employee(uid)
        admin_name = admin["full_name"] if admin else str(uid)
        await db.add_edit_log(uid, admin_name, "Изменение времени начала смены", emp_tg_id, emp_name, old_time, new_time_str, reason)
        
        del user_state[uid]
        await message.answer(
            f"✅ Время начала смены *#{shift_id}* изменено!\n\n"
            f"🕐 Было: {fmt_datetime(old_time)}\n"
            f"🕐 Стало: {fmt_datetime(new_time_str)}\n"
            f"📝 Причина: {reason}",
            parse_mode="Markdown",
            reply_markup=admin_panel_kb()
        )
        await notify_admins(bot, f"✏️ *Изменение времени начала смены*\n👤 {admin_name}\n🎯 {emp_name}\n🆔 Смена #{shift_id}\n⏰ {fmt_datetime(old_time)} → {fmt_datetime(new_time_str)}\n📝 Причина: {reason}")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}\n\nПример: `2025-01-15 09:00:00` или `+2 часа`", parse_mode="Markdown")


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_action:") and m.text == "🔴 Изменить время окончания")
async def edit_shift_change_end_time(message: Message):
    uid = message.from_user.id
    parts = user_state[uid].split(":")
    shift_id = int(parts[1])
    emp_tg_id = int(parts[2])
    emp_name = parts[3]
    
    user_state[uid] = f"edit_shift_end_new:{shift_id}:{emp_tg_id}:{emp_name}"
    await message.answer(
        "🔴 Введите *НОВОЕ ВРЕМЯ ОКОНЧАНИЯ* смены:\n\n"
        "Варианты:\n"
        "• `2025-01-15 18:00:00` — абсолютное время\n"
        "• `+2 часа` — сдвинуть на 2 часа вперёд\n"
        "• `-30 минут` — сдвинуть назад\n"
        "• `оставить пустым` — сделать смену активной (открытой)\n\n"
        "Затем я спрошу причину изменения.",
        parse_mode="Markdown",
        reply_markup=cancel_kb()
    )


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_end_new:"))
async def edit_shift_save_end_time(message: Message):
    uid = message.from_user.id
    parts = user_state[uid].split(":")
    shift_id = int(parts[1])
    emp_tg_id = int(parts[2])
    emp_name = parts[3]
    
    time_value = message.text.strip()
    
    if time_value == "❌ Отмена":
        del user_state[uid]
        await message.answer("❌ Отменено", reply_markup=admin_panel_kb())
        return
    
    # Сохраняем новое время и ждём причину
    user_state[uid] = f"edit_shift_end_reason:{shift_id}:{emp_tg_id}:{emp_name}:{time_value}"
    await message.answer(
        "📝 Укажите *ПРИЧИНУ* изменения времени окончания смены:",
        parse_mode="Markdown",
        reply_markup=cancel_kb()
    )


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_end_reason:"))
async def edit_shift_apply_end_time(message: Message, bot: Bot):
    uid = message.from_user.id
    reason = message.text.strip()
    
    if reason == "❌ Отмена":
        del user_state[uid]
        await message.answer("❌ Отменено", reply_markup=admin_panel_kb())
        return
    
    parts = user_state[uid].split(":")
    shift_id = int(parts[1])
    emp_tg_id = int(parts[2])
    emp_name = parts[3]
    time_value = parts[4]
    
    shift = await db.get_shift_by_id(shift_id)
    if not shift:
        await message.answer("❌ Смена не найдена")
        del user_state[uid]
        return
    
    old_time = shift["closed_at"] if shift["closed_at"] else "активна"
    
    try:
        if time_value == "":
            # Делаем смену активной
            await db.update_shift_make_active(shift_id)
            new_time_str = "активна (открыта)"
            
            admin = await db.get_employee(uid)
            admin_name = admin["full_name"] if admin else str(uid)
            await db.add_edit_log(uid, admin_name, "Смена сделана активной", emp_tg_id, emp_name, old_time, new_time_str, reason)
            
            del user_state[uid]
            await message.answer(
                f"✅ Смена *#{shift_id}* теперь активна (открыта)!\n\n"
                f"📝 Причина: {reason}",
                parse_mode="Markdown",
                reply_markup=admin_panel_kb()
            )
            await notify_admins(bot, f"✏️ *Смена сделана активной*\n👤 {admin_name}\n🎯 {emp_name}\n🆔 Смена #{shift_id}\n📝 Причина: {reason}")
            return
        
        # Парсим время
        if time_value.startswith("+") or time_value.startswith("-") or "час" in time_value or "минут" in time_value:
            import re
            match = re.match(r"([+-]?\d+)\s*(час|часа|часов|ч|минут|минуты|минуту|мин|м)", time_value.lower())
            if match:
                num = int(match.group(1))
                unit = match.group(2)
                base_time = shift["closed_at"] if shift["closed_at"] else shift["opened_at"]
                current = datetime.strptime(base_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
                if unit in ["час", "часа", "часов", "ч"]:
                    new_time = current + timedelta(hours=num)
                else:
                    new_time = current + timedelta(minutes=num)
                new_time_str = new_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                raise ValueError("Неверный формат")
        else:
            new_time = datetime.strptime(time_value, "%Y-%m-%d %H:%M:%S")
            new_time_str = new_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Проверяем, чтобы время окончания было позже начала
        opened = datetime.strptime(shift["opened_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
        if new_time <= opened:
            await message.answer("❌ Время окончания должно быть ПОЗЖЕ времени начала смены!")
            return
        
        await db.update_shift_end(shift_id, new_time_str)
        
        admin = await db.get_employee(uid)
        admin_name = admin["full_name"] if admin else str(uid)
        await db.add_edit_log(uid, admin_name, "Изменение времени окончания смены", emp_tg_id, emp_name, old_time, new_time_str, reason)
        
        del user_state[uid]
        await message.answer(
            f"✅ Время окончания смены *#{shift_id}* изменено!\n\n"
            f"🕐 Было: {fmt_datetime(old_time) if old_time != 'активна' else 'активна'}\n"
            f"🕐 Стало: {fmt_datetime(new_time_str)}\n"
            f"📝 Причина: {reason}",
            parse_mode="Markdown",
            reply_markup=admin_panel_kb()
        )
        await notify_admins(bot, f"✏️ *Изменение времени окончания смены*\n👤 {admin_name}\n🎯 {emp_name}\n🆔 Смена #{shift_id}\n⏰ {fmt_datetime(old_time) if old_time != 'активна' else 'активна'} → {fmt_datetime(new_time_str)}\n📝 Причина: {reason}")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}\n\nПример: `2025-01-15 18:00:00` или `+2 часа` или отправьте пустое сообщение", parse_mode="Markdown")


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_action:") and m.text == "🗑 Удалить смену")
async def edit_shift_delete_confirm(message: Message):
    uid = message.from_user.id
    parts = user_state[uid].split(":")
    shift_id = int(parts[1])
    emp_tg_id = int(parts[2])
    emp_name = parts[3]
    
    shift = await db.get_shift_by_id(shift_id)
    if not shift:
        await message.answer("❌ Смена не найдена")
        del user_state[uid]
        return
    
    user_state[uid] = f"edit_shift_delete_reason:{shift_id}:{emp_tg_id}:{emp_name}"
    await message.answer(
        f"⚠️ *ВНИМАНИЕ!*\n\nВы действительно хотите удалить смену *#{shift_id}*?\n\n"
        f"👤 Сотрудник: {emp_name}\n"
        f"🟢 Открыта: {fmt_datetime(shift['opened_at'])}\n"
        f"{'🔴 Закрыта: ' + fmt_datetime(shift['closed_at']) if shift['closed_at'] else '🟢 Активна'}\n\n"
        f"📝 Укажите *ПРИЧИНУ* удаления:",
        parse_mode="Markdown",
        reply_markup=cancel_kb()
    )


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_delete_reason:"))
async def edit_shift_delete_execute(message: Message, bot: Bot):
    uid = message.from_user.id
    reason = message.text.strip()
    
    if reason == "❌ Отмена":
        del user_state[uid]
        await message.answer("❌ Удаление отменено", reply_markup=admin_panel_kb())
        return
    
    parts = user_state[uid].split(":")
    shift_id = int(parts[1])
    emp_tg_id = int(parts[2])
    emp_name = parts[3]
    
    shift = await db.get_shift_by_id(shift_id)
    if shift:
        admin = await db.get_employee(uid)
        admin_name = admin["full_name"] if admin else str(uid)
        
        await db.delete_shift(shift_id)
        await db.add_edit_log(uid, admin_name, "Удаление смены", emp_tg_id, emp_name, f"смена #{shift_id}", "удалено", reason)
        
        del user_state[uid]
        await message.answer(
            f"✅ Смена *#{shift_id}* удалена!\n\n"
            f"👤 Сотрудник: {emp_name}\n"
            f"📝 Причина: {reason}",
            parse_mode="Markdown",
            reply_markup=admin_panel_kb()
        )
        await notify_admins(bot, f"🗑 *Удаление смены*\n👤 {admin_name}\n🎯 {emp_name}\n🆔 Смена #{shift_id}\n📝 Причина: {reason}")
    else:
        del user_state[uid]
        await message.answer("❌ Смена не найдена", reply_markup=admin_panel_kb())


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_action:") and m.text == "🔙 Назад")
async def edit_shift_back_to_admin(message: Message):
    uid = message.from_user.id
    del user_state[uid]
    await message.answer("⚙️ Админ-панель", reply_markup=admin_panel_kb())


# ---------- Открыть смену ----------

@router.message(F.text == "🟢 Открыть смену")
async def open_shift(message: Message):
    uid = message.from_user.id
    emp = await db.get_employee(uid)
    
    if not emp or not emp["approved"]:
        await message.answer("❗ Вы не зарегистрированы. Напишите /start")
        return
    
    if await db.get_active_shift(uid):
        await message.answer("⚠️ У вас уже открыта смена!")
        return
    
    user_state[uid] = "waiting_open"
    await message.answer("📸 Пришлите фото ПВЗ для открытия смены:", reply_markup=cancel_kb())


@router.message(lambda m: user_state.get(m.from_user.id) == "waiting_open" and m.photo)
async def open_shift_photo(message: Message, bot: Bot):
    uid = message.from_user.id
    emp = await db.get_employee(uid)
    photo_id = message.photo[-1].file_id
    
    await db.open_shift(uid, photo_id)
    del user_state[uid]
    await update_user_cache(uid)
    
    await message.answer(f"✅ Смена открыта в {now_str()}!\nХорошей работы, {emp['full_name']}!", reply_markup=get_keyboard(uid))
    await notify_admins(bot, f"🟢 Смена открыта\n👤 {emp['full_name']}\n📍 {PVZ_ADDRESS}\n🕐 {now_str()}", photo_id)


@router.message(lambda m: user_state.get(m.from_user.id) == "waiting_open" and not m.photo)
async def open_shift_wrong(message: Message):
    await message.answer("❗ Отправьте ФОТО или нажмите ❌ Отмена")


# ---------- Перерыв ----------

@router.message(F.text == "☕ Перерыв")
async def start_break(message: Message):
    uid = message.from_user.id
    
    if not await db.get_active_shift(uid):
        await message.answer("⚠️ Сначала откройте смену!")
        return
    
    if await db.get_active_break(uid):
        await message.answer("⚠️ Перерыв уже активен!")
        return
    
    user_state[uid] = "waiting_break"
    await message.answer("📸 Пришлите фото для начала перерыва:", reply_markup=cancel_kb())


@router.message(lambda m: user_state.get(m.from_user.id) == "waiting_break" and m.photo)
async def start_break_photo(message: Message, bot: Bot):
    uid = message.from_user.id
    emp = await db.get_employee(uid)
    photo_id = message.photo[-1].file_id
    
    await db.start_break(uid, photo_id)
    del user_state[uid]
    await update_user_cache(uid)
    
    await message.answer(f"☕ Перерыв начат в {now_str()}.\nНажмите «✅ Закончить перерыв» когда вернётесь.", reply_markup=get_keyboard(uid))
    await notify_admins(bot, f"☕ Перерыв начат\n👤 {emp['full_name']}\n📍 {PVZ_ADDRESS}\n🕐 {now_str()}", photo_id)


@router.message(lambda m: user_state.get(m.from_user.id) == "waiting_break" and not m.photo)
async def start_break_wrong(message: Message):
    await message.answer("❗ Отправьте ФОТО или нажмите ❌ Отмена")


# ---------- Закончить перерыв ----------

@router.message(F.text == "✅ Закончить перерыв")
async def end_break(message: Message, bot: Bot):
    uid = message.from_user.id
    active = await db.get_active_break(uid)
    
    if not active:
        await message.answer("⚠️ Нет активного перерыва!")
        return
    
    emp = await db.get_employee(uid)
    await db.end_break(active["id"])
    await update_user_cache(uid)
    
    start_str = active["started_at"]
    end_str = now_db()
    
    try:
        s = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
        e = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
        dur = int((e - s).total_seconds() // 60)
    except:
        dur = 0
    
    await message.answer(f"✅ Перерыв завершён!\n⏱ {fmt_time(start_str)}–{fmt_time(end_str)} ({fmt_duration(dur)})", reply_markup=get_keyboard(uid))
    
    text = f"✅ Перерыв завершён\n👤 {emp['full_name']}\n🕐 {fmt_time(start_str)}–{fmt_time(end_str)}\n⏱ {fmt_duration(dur)}"
    await notify_admins(bot, text, active["photo_id"])
    
    if dur > 15:
        await send_to_chat(bot, f"⚠️ {text}", active["photo_id"])


# ---------- Закрыть смену ----------

@router.message(F.text == "🔴 Закрыть смену")
async def close_shift(message: Message):
    uid = message.from_user.id
    
    if not await db.get_active_shift(uid):
        await message.answer("⚠️ Нет открытой смены!")
        return
    
    if await db.get_active_break(uid):
        await message.answer("⚠️ Сначала завершите перерыв!")
        return
    
    user_state[uid] = "waiting_close"
    await message.answer("📸 Пришлите фото ПВЗ для закрытия смены:", reply_markup=cancel_kb())


@router.message(lambda m: user_state.get(m.from_user.id) == "waiting_close" and m.photo)
async def close_shift_photo(message: Message, bot: Bot):
    uid = message.from_user.id
    active = await db.get_active_shift(uid)
    emp = await db.get_employee(uid)
    photo_id = message.photo[-1].file_id
    
    if not active:
        await message.answer("⚠️ Нет открытой смены!")
        if uid in user_state:
            del user_state[uid]
        return
    
    await db.close_shift(active["id"], photo_id)
    del user_state[uid]
    await update_user_cache(uid)
    
    open_str = active["opened_at"]
    close_str = now_db()
    
    try:
        o = datetime.strptime(open_str, "%Y-%m-%d %H:%M:%S")
        c = datetime.strptime(close_str, "%Y-%m-%d %H:%M:%S")
        dur = int((c - o).total_seconds() // 60)
    except:
        dur = 0
    
    await message.answer(f"🔴 Смена закрыта в {now_str()}!\n⏱ {fmt_time(open_str)}–{fmt_time(close_str)} ({fmt_duration(dur)})", reply_markup=get_keyboard(uid))
    await notify_admins(bot, f"🔴 Смена закрыта\n👤 {emp['full_name']}\n📍 {PVZ_ADDRESS}\n🕐 {fmt_time(open_str)}–{fmt_time(close_str)}\n⏱ {fmt_duration(dur)}", photo_id)
    
    if datetime.now(MSK).weekday() == 6:
        if await db.count_active_shifts() == 0:
            await send_weekly_report_to_chat(bot)


# ---------- Админ кнопки ----------

@router.message(F.text == "👥 Сотрудники")
async def list_employees(message: Message):
    uid = message.from_user.id
    if uid not in ALL_ADMINS:
        await message.answer("❗ Нет доступа")
        return
    
    employees = await db.get_all_employees()
    if not employees:
        await message.answer("Сотрудников пока нет")
        return
    
    lines = ["👥 *Список сотрудников:*\n"]
    for emp in employees:
        status = "✅" if emp["approved"] else "⏳"
        lines.append(f"{status} *{emp['full_name']}*\n   🆔 Telegram: `{emp['telegram_id']}`\n   🏷 WB: `{emp['wb_employee_id']}`\n   📅 Смен: {emp.get('total_shifts', 0)}\n")
    
    await message.answer("\n".join(lines), parse_mode="Markdown")


@router.message(F.text == "🟢 Активные")
async def active_shifts(message: Message):
    await cmd_active(message)


@router.message(F.text == "📊 Статистика")
async def stats_menu(message: Message):
    uid = message.from_user.id
    if uid not in ALL_ADMINS:
        await message.answer("❗ Нет доступа")
        return
    
    employees = await db.get_approved_employees()
    if not employees:
        await message.answer("Нет одобренных сотрудников")
        return
    
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=emp["full_name"])] for emp in employees] + [[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    user_state[uid] = "stats_select"
    await message.answer("Выберите сотрудника:", reply_markup=kb)


@router.message(lambda m: user_state.get(m.from_user.id) == "stats_select")
async def stats_show(message: Message):
    uid = message.from_user.id
    name = message.text.strip()
    
    if name == "❌ Отмена":
        del user_state[uid]
        await update_user_cache(uid)
        await message.answer("Отменено", reply_markup=get_keyboard(uid))
        return
    
    emp = await db.get_employee_by_name(name)
    if not emp:
        await message.answer("Сотрудник не найден, попробуйте ещё раз")
        return
    
    shifts = await db.get_shifts_this_week(emp["telegram_id"])
    breaks = await db.get_breaks_for_week(emp["telegram_id"])
    
    shift_mins = sum(s.get("duration_minutes", 0) for s in shifts)
    break_mins = sum(b.get("duration_minutes", 0) for b in breaks)
    
    text = f"📊 *{emp['full_name']}*\n\n📅 Смен за неделю: {len(shifts)} ({fmt_duration(shift_mins)})\n☕ Перерывов: {len(breaks)} ({fmt_duration(break_mins)})"
    await message.answer(text, parse_mode="Markdown")
    
    del user_state[uid]
    await update_user_cache(uid)
    await message.answer("Меню:", reply_markup=get_keyboard(uid))


# ---------- Fallback ----------

@router.message()
async def fallback(message: Message):
    pass
