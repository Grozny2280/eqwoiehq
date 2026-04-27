import asyncio
import os
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
user_state = {}  # {user_id: "waiting_name" / "waiting_wb:name" / "waiting_open" / ...}
shift_cache = {}
break_cache = {}


async def update_user_cache(uid: int):
    """Обновить кеш состояния пользователя"""
    shift = await db.get_active_shift(uid)
    break_active = await db.get_active_break(uid)
    shift_cache[uid] = {
        "shift_active": shift is not None,
        "break_active": break_active is not None,
        "shift_opened_at": shift["opened_at"] if shift else None
    }


def get_keyboard(uid: int, shift_active: bool = None, break_active: bool = None):
    """Динамическая клавиатура в зависимости от состояния"""
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
    
    # Кнопки админа
    if is_sa or is_adm:
        keyboard.append([KeyboardButton(text="👥 Сотрудники"), KeyboardButton(text="📊 Статистика")])
        keyboard.append([KeyboardButton(text="🟢 Активные")])
    
    # Дополнительные кнопки для суперадмина
    if is_sa:
        keyboard.append([KeyboardButton(text="⚙️ Админ-панель")])
    
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def cancel_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True)


def admin_panel_kb():
    keyboard = [
        [KeyboardButton(text="✏️ Редактировать сотрудника")],
        [KeyboardButton(text="⏰ Редактировать смену")],
        [KeyboardButton(text="📋 Все сотрудники")],
        [KeyboardButton(text="💾 Создать бэкап")],
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


async def send_weekly_report_to_chat(bot: Bot):
    """Отправить недельный отчёт в чат"""
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

⚙️ *Суперадмин (дополнительно):*
✏️ Редактировать сотрудника — ФИО, WB ID, удаление
⏰ Редактировать смену — изменить время начала/конца
📋 Все сотрудники — полный список с ID
💾 Создать бэкап — резервная копия БД

⌨️ *Команды:*
/start — главное меню
/help — эта справка
/active — кто на смене
/stats — статистика за сегодня
/rating — рейтинг сотрудников
/mystats — моя статистика
/top — топ сегодняшних
/backup — создать бэкап БД
"""
    await message.answer(text, parse_mode="Markdown")


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


# ---------- Отмена ----------

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


# ========== АДМИН-ПАНЕЛЬ (ТОЛЬКО ДЛЯ СУПЕРАДМИНОВ) ==========

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
    
    # Разбиваем на части если слишком длинное
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
    
    # Создаём клавиатуру со списком сотрудников
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
async def edit_employee_name_save(message: Message):
    uid = message.from_user.id
    new_name = message.text.strip()
    
    if len(new_name) < 3:
        await message.answer("❗ Минимум 3 символа. Введите ФИО:")
        return
    
    parts = user_state[uid].split(":")
    emp_id = int(parts[1])
    
    await db.update_employee_name(emp_id, new_name)
    
    del user_state[uid]
    await message.answer(f"✅ ФИО сотрудника изменено на *{new_name}*", parse_mode="Markdown", reply_markup=admin_panel_kb())
    await update_user_cache(emp_id)


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_employee:") and m.text == "🆔 Изменить WB ID")
async def edit_employee_wb_start(message: Message):
    uid = message.from_user.id
    user_state[uid] = user_state[uid] + ":edit_wb"
    await message.answer("🆔 Введите новый WB ID сотрудника:", reply_markup=cancel_kb())


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_employee:") and ":edit_wb" in user_state.get(m.from_user.id, ""))
async def edit_employee_wb_save(message: Message):
    uid = message.from_user.id
    new_wb = message.text.strip()
    
    if not new_wb:
        await message.answer("❗ Введите WB ID:")
        return
    
    parts = user_state[uid].split(":")
    emp_id = int(parts[1])
    
    await db.update_employee_wb(emp_id, new_wb)
    
    del user_state[uid]
    await message.answer(f"✅ WB ID изменён на `{new_wb}`", parse_mode="Markdown", reply_markup=admin_panel_kb())


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
    emp_name = parts[2] if len(parts) > 2 else "сотрудник"
    
    await db.delete_employee_full(emp_id)
    
    del user_state[uid]
    await message.answer(f"✅ Сотрудник *{emp_name}* удалён", parse_mode="Markdown", reply_markup=admin_panel_kb())
    await notify_admins(bot, f"🗑 *Удаление сотрудника*\n👤 {emp_name}\n🆔 {emp_id}")


@router.message(lambda m: user_state.get(m.from_user.id, "").endswith("confirm_delete") and m.text == "❌ Нет, отмена")
async def edit_employee_delete_cancel(message: Message):
    uid = message.from_user.id
    del user_state[uid]
    await message.answer("❌ Удаление отменено", reply_markup=admin_panel_kb())


# ---------- Редактирование смены ----------

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
    
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=emp["full_name"])] for emp in employees] + [[KeyboardButton(text="🔙 Назад")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    user_state[uid] = "edit_shift_select_employee"
    await message.answer("👥 Выберите сотрудника, чью смену хотите отредактировать:", reply_markup=kb)


@router.message(lambda m: user_state.get(m.from_user.id) == "edit_shift_select_employee")
async def edit_shift_select_shift(message: Message):
    uid = message.from_user.id
    name = message.text.strip()
    
    if name == "🔙 Назад":
        del user_state[uid]
        await message.answer("⚙️ Админ-панель", reply_markup=admin_panel_kb())
        return
    
    emp = await db.get_employee_by_name(name)
    if not emp:
        await message.answer("Сотрудник не найден")
        return
    
    # Получаем все смены сотрудника за последние 14 дней
    shifts = await db.get_employee_shifts_recent(emp["telegram_id"], 14)
    
    if not shifts:
        await message.answer(f"У {emp['full_name']} нет смен за последние 14 дней")
        return
    
    # Создаём кнопки со сменами
    shift_buttons = []
    for s in shifts:
        status = "🔴" if s["closed_at"] else "🟢"
        date_str = fmt_datetime(s["opened_at"])[:16]
        shift_buttons.append([KeyboardButton(text=f"{status} #{s['id']} | {date_str}")])
    
    kb = ReplyKeyboardMarkup(
        keyboard=shift_buttons + [[KeyboardButton(text="🔙 Назад")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    user_state[uid] = f"edit_shift_select:{emp['telegram_id']}:{emp['full_name']}"
    await message.answer(f"👤 *{emp['full_name']}*\n\nВыберите смену для редактирования:", parse_mode="Markdown", reply_markup=kb)


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_select:"))
async def edit_shift_choose_action(message: Message):
    uid = message.from_user.id
    text = message.text.strip()
    
    if text == "🔙 Назад":
        del user_state[uid]
        await message.answer("⚙️ Админ-панель", reply_markup=admin_panel_kb())
        return
    
    # Парсим ID смены из кнопки
    try:
        # Формат: "🟢 #123 | 15.01.2025 09:00" или "🔴 #123 | 15.01.2025 09:00"
        shift_id = int(text.split("#")[1].split("|")[0].strip())
    except:
        await message.answer("❗ Не удалось определить смену")
        return
    
    shift = await db.get_shift_by_id(shift_id)
    if not shift:
        await message.answer("Смена не найдена")
        return
    
    user_state[uid] = f"edit_shift_action:{shift_id}"
    
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
        f"👤 Сотрудник: {user_state[uid].split(':')[2] if len(user_state[uid].split(':')) > 2 else shift['telegram_id']}\n"
        f"🟢 Открыта: {fmt_datetime(shift['opened_at'])}\n"
        f"🔴 Закрыта: {fmt_datetime(shift['closed_at']) if shift['closed_at'] else '—'}\n"
        f"📊 Статус: {status}\n"
        f"⏱ Длительность: {duration}\n\n"
        f"Что хотите изменить?",
        parse_mode="Markdown",
        reply_markup=action_kb
    )


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_action:") and m.text == "⏰ Изменить время начала")
async def edit_shift_change_start(message: Message):
    uid = message.from_user.id
    user_state[uid] = user_state[uid] + ":change_start"
    await message.answer(
        "⏰ Введите новое время начала смены в формате:\n\n"
        "`2025-01-15 09:00:00`\n\n"
        "Или *относительно*: `+2 часа`, `-30 минут`",
        parse_mode="Markdown",
        reply_markup=cancel_kb()
    )


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_action:") and m.text == "🔴 Изменить время окончания")
async def edit_shift_change_end(message: Message):
    uid = message.from_user.id
    user_state[uid] = user_state[uid] + ":change_end"
    await message.answer(
        "🔴 Введите новое время окончания смены в формате:\n\n"
        "`2025-01-15 18:00:00`\n\n"
        "Или *относительно*: `+2 часа`, `-30 минут`\n\n"
        "*Пустое значение* — сделать смену активной (открытой)",
        parse_mode="Markdown",
        reply_markup=cancel_kb()
    )


async def parse_time_change(value: str, current_time: str):
    """Парсит ввод времени: абсолютное значение или относительное"""
    import re
    
    # Пустое значение
    if not value or value.strip() == "":
        return None
    
    # Абсолютное время
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
    except:
        pass
    
    # Относительное время
    match = re.match(r"([+-]?\d+)\s*(час|часа|часов|ч|минут|минуты|минуту|мин|м|hour|h|minute|min)", value.lower())
    if match:
        num = int(match.group(1))
        unit = match.group(2)
        current = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
        
        if unit in ["час", "часа", "часов", "ч", "hour", "h"]:
            return current + timedelta(hours=num)
        else:
            return current + timedelta(minutes=num)
    
    raise ValueError("Неверный формат времени")


@router.message(lambda m: user_state.get(m.from_user.id, "").endswith("change_start"))
async def edit_shift_save_start(message: Message, bot: Bot):
    uid = message.from_user.id
    parts = user_state[uid].split(":")
    shift_id = int(parts[1])
    
    shift = await db.get_shift_by_id(shift_id)
    if not shift:
        await message.answer("Смена не найдена")
        del user_state[uid]
        return
    
    try:
        new_time = await parse_time_change(message.text.strip(), shift["opened_at"])
        if new_time is None:
            await message.answer("❗ Время не может быть пустым")
            return
        
        await db.update_shift_start(shift_id, new_time.strftime("%Y-%m-%d %H:%M:%S"))
        
        del user_state[uid]
        await message.answer(f"✅ Время начала смены изменено на {fmt_datetime(new_time.strftime('%Y-%m-%d %H:%M:%S'))}", reply_markup=admin_panel_kb())
        
        # Уведомление админам
        emp = await db.get_employee(shift["telegram_id"])
        await notify_admins(bot, f"✏️ *Изменение смены*\n👤 {emp['full_name'] if emp else shift['telegram_id']}\n🆔 Смена #{shift_id}\n⏰ Новое начало: {fmt_datetime(new_time.strftime('%Y-%m-%d %H:%M:%S'))}")
        
    except ValueError as e:
        await message.answer(f"❌ {str(e)}\n\nПример: `2025-01-15 09:00:00` или `+2 часа`", parse_mode="Markdown")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@router.message(lambda m: user_state.get(m.from_user.id, "").endswith("change_end"))
async def edit_shift_save_end(message: Message, bot: Bot):
    uid = message.from_user.id
    parts = user_state[uid].split(":")
    shift_id = int(parts[1])
    
    shift = await db.get_shift_by_id(shift_id)
    if not shift:
        await message.answer("Смена не найдена")
        del user_state[uid]
        return
    
    new_time_raw = message.text.strip()
    
    try:
        if new_time_raw == "":
            # Делаем смену активной (открытой)
            await db.update_shift_make_active(shift_id)
            del user_state[uid]
            await message.answer(f"✅ Смена #{shift_id} теперь активна (не закрыта)", reply_markup=admin_panel_kb())
            
            emp = await db.get_employee(shift["telegram_id"])
            await notify_admins(bot, f"✏️ *Изменение смены*\n👤 {emp['full_name'] if emp else shift['telegram_id']}\n🆔 Смена #{shift_id}\n🔴 Смена сделана активной")
            return
        
        # Используем текущее время как базовое для парсинга
        base_time = shift["closed_at"] if shift["closed_at"] else shift["opened_at"]
        new_time = await parse_time_change(new_time_raw, base_time)
        if new_time is None:
            await message.answer("❗ Время не может быть пустым")
            return
        
        # Проверяем, чтобы время окончания было позже времени начала
        opened = datetime.strptime(shift["opened_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
        if new_time <= opened:
            await message.answer("❌ Время окончания должно быть позже времени начала смены!")
            return
        
        await db.update_shift_end(shift_id, new_time.strftime("%Y-%m-%d %H:%M:%S"))
        
        del user_state[uid]
        await message.answer(f"✅ Время окончания смены изменено на {fmt_datetime(new_time.strftime('%Y-%m-%d %H:%M:%S'))}", reply_markup=admin_panel_kb())
        
        emp = await db.get_employee(shift["telegram_id"])
        await notify_admins(bot, f"✏️ *Изменение смены*\n👤 {emp['full_name'] if emp else shift['telegram_id']}\n🆔 Смена #{shift_id}\n🔴 Новое окончание: {fmt_datetime(new_time.strftime('%Y-%m-%d %H:%M:%S'))}")
        
    except ValueError as e:
        await message.answer(f"❌ {str(e)}\n\nПример: `2025-01-15 18:00:00` или `+2 часа` или ` ` (пустое поле для открытия)", parse_mode="Markdown")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_action:") and m.text == "🗑 Удалить смену")
async def edit_shift_delete_confirm(message: Message):
    uid = message.from_user.id
    parts = user_state[uid].split(":")
    shift_id = int(parts[1])
    
    shift = await db.get_shift_by_id(shift_id)
    if not shift:
        await message.answer("Смена не найдена")
        del user_state[uid]
        return
    
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="✅ ДА, удалить смену")], [KeyboardButton(text="❌ Нет, отмена")]],
        resize_keyboard=True
    )
    user_state[uid] = f"edit_shift_delete_confirm:{shift_id}"
    await message.answer(
        f"⚠️ *ВНИМАНИЕ!*\n\nВы действительно хотите удалить смену *#{shift_id}*\n\n"
        f"🟢 Открыта: {fmt_datetime(shift['opened_at'])}\n"
        f"{'🔴 Закрыта: ' + fmt_datetime(shift['closed_at']) if shift['closed_at'] else '🟢 Активна'}\n\n"
        f"Это действие необратимо!",
        parse_mode="Markdown",
        reply_markup=kb
    )


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_delete_confirm:") and m.text == "✅ ДА, удалить смену")
async def edit_shift_delete_execute(message: Message, bot: Bot):
    uid = message.from_user.id
    parts = user_state[uid].split(":")
    shift_id = int(parts[1])
    
    shift = await db.get_shift_by_id(shift_id)
    if shift:
        emp = await db.get_employee(shift["telegram_id"])
        await db.delete_shift(shift_id)
        await notify_admins(bot, f"🗑 *Удаление смены*\n👤 {emp['full_name'] if emp else shift['telegram_id']}\n🆔 Смена #{shift_id}\n🟢 Открыта: {fmt_datetime(shift['opened_at'])}")
    
    del user_state[uid]
    await message.answer(f"✅ Смена #{shift_id} удалена", reply_markup=admin_panel_kb())


@router.message(lambda m: user_state.get(m.from_user.id, "").startswith("edit_shift_delete_confirm:") and m.text == "❌ Нет, отмена")
async def edit_shift_delete_cancel(message: Message):
    uid = message.from_user.id
    del user_state[uid]
    await message.answer("❌ Удаление отменено", reply_markup=admin_panel_kb())


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
    
    # Отчёт в воскресенье после последней смены
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
    # Игнорируем все остальные сообщения
    pass
