import asyncio
from datetime import datetime, timezone, timedelta
from aiogram import Router, F, Bot
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import CommandStart, Command

from config import ADMIN_IDS, SUPERADMIN_IDS, PVZ_ADDRESS, GROUP_CHAT_ID
import database as db

router = Router()
ALL_ADMINS = list(set(ADMIN_IDS + SUPERADMIN_IDS))
MSK = timezone(timedelta(hours=3))

# Состояния пользователей (временные)
user_state = {}  # {user_id: "waiting_name" / "waiting_wb" / "waiting_open" / "waiting_close" / "waiting_break"}

# ---------- Клавиатуры ----------

def get_keyboard(uid: int):
    """Динамическая клавиатура в зависимости от состояния"""
    shift = asyncio.run_coroutine_threadsafe(db.get_active_shift(uid), asyncio.get_event_loop()).result()
    break_active = asyncio.run_coroutine_threadsafe(db.get_active_break(uid), asyncio.get_event_loop()).result()
    
    is_sa = uid in SUPERADMIN_IDS
    is_adm = uid in ALL_ADMINS
    
    keyboard = []
    
    if not shift:
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
    
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def cancel_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True)


# ---------- Утилиты ----------

def fmt_time(s: str):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").strftime("%H:%M")
    except:
        return s[:5] if len(s) > 5 else s


def now_str():
    return datetime.now(MSK).strftime("%d.%m.%Y %H:%M")


def now_db():
    return datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S")


def fmt_duration(minutes: int):
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


# ---------- Команды ----------

@router.message(CommandStart())
async def cmd_start(message: Message):
    uid = message.from_user.id
    emp = await db.get_employee(uid)
    
    # Суперадмин без регистрации
    if uid in SUPERADMIN_IDS and not emp:
        user_state[uid] = "waiting_name"
        await message.answer("👋 Суперадмин! Введите ваше ФИО:", reply_markup=cancel_kb())
        return
    
    # Обычный админ без регистрации
    if uid in ALL_ADMINS and not emp:
        user_state[uid] = "waiting_name"
        await message.answer("👋 Админ! Введите ваше ФИО:", reply_markup=cancel_kb())
        return
    
    # Нет регистрации
    if not emp:
        user_state[uid] = "waiting_name"
        await message.answer("👋 Добро пожаловать! Введите ваше ФИО для регистрации:", reply_markup=cancel_kb())
        return
    
    # Ждёт одобрения
    if not emp["approved"]:
        await message.answer("⏳ Ваша заявка на одобрении.")
        return
    
    # Всё ок
    await message.answer(f"👋 С возвращением, {emp['full_name']}!", reply_markup=get_keyboard(uid))


@router.message(Command("help"))
async def cmd_help(message: Message):
    text = """
📋 *Помощь*

👤 *Сотрудник:*
🟢 Открыть смену
☕ Перерыв
✅ Закончить перерыв
🔴 Закрыть смену

👑 *Админ:*
👥 Сотрудники — список
📊 Статистика — отчёт
🟢 Активные — кто на смене

⌨️ *Команды:*
/start — главное меню
/help — эта справка
/active — кто на смене
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


# ---------- Отмена ----------

@router.message(F.text == "❌ Отмена")
async def cancel(message: Message):
    uid = message.from_user.id
    if uid in user_state:
        del user_state[uid]
    await message.answer("❌ Отменено", reply_markup=get_keyboard(uid))


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
    
    # Автоодобрение для админов
    if uid in SUPERADMIN_IDS or uid in ALL_ADMINS:
        await db.approve_employee(uid)
        await message.answer(f"✅ Добро пожаловать, {name}!", reply_markup=get_keyboard(uid))
    else:
        await message.answer("✅ Заявка отправлена! Ожидайте одобрения.")
        
        # Уведомить суперадминов
        username = f"@{message.from_user.username}" if message.from_user.username else "нет"
        for aid in SUPERADMIN_IDS:
            try:
                await message.bot.send_message(aid, f"🆕 Новая заявка!\n👤 {name}\n🆔 {uid}\n📱 {username}\n🏷 {wb_id}")
            except:
                pass


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
            await send_weekly_report(bot)


async def send_weekly_report(bot: Bot):
    employees = await db.get_approved_employees()
    if not employees:
        return
    
    now = datetime.now(MSK)
    monday = now - timedelta(days=now.weekday())
    lines = [f"📊 *Итоги недели {monday.strftime('%d.%m')}–{now.strftime('%d.%m.%Y')}*\n"]
    
    for emp in employees:
        shifts = await db.get_shifts_this_week(emp["telegram_id"])
        breaks = await db.get_breaks_for_week(emp["telegram_id"])
        
        shift_mins = 0
        for s in shifts:
            if s["closed_at"]:
                try:
                    o = datetime.strptime(s["opened_at"], "%Y-%m-%d %H:%M:%S")
                    c = datetime.strptime(s["closed_at"], "%Y-%m-%d %H:%M:%S")
                    shift_mins += int((c - o).total_seconds() // 60)
                except:
                    pass
        
        break_mins = 0
        for b in breaks:
            if b["ended_at"]:
                try:
                    s = datetime.strptime(b["started_at"], "%Y-%m-%d %H:%M:%S")
                    e = datetime.strptime(b["ended_at"], "%Y-%m-%d %H:%M:%S")
                    break_mins += int((e - s).total_seconds() // 60)
                except:
                    pass
        
        lines.append(f"👤 *{emp['full_name']}*\n   📅 Смен: {len(shifts)} ({fmt_duration(shift_mins)})\n   ☕ Перерывов: {len(breaks)} ({fmt_duration(break_mins)})\n")
    
    await send_to_chat(bot, "\n".join(lines))


# ---------- Админ кнопки ----------

@router.message(F.text == "👥 Сотрудники")
async def list_employees(message: Message):
    if message.from_user.id not in ALL_ADMINS:
        await message.answer("❗ Нет доступа")
        return
    
    employees = await db.get_all_employees()
    if not employees:
        await message.answer("Сотрудников пока нет")
        return
    
    lines = ["👥 *Список сотрудников:*\n"]
    for emp in employees:
        status = "✅" if emp["approved"] else "⏳"
        lines.append(f"{status} *{emp['full_name']}*\n   ID: `{emp['telegram_id']}`\n   WB: `{emp['wb_employee_id']}`\n")
    
    await message.answer("\n".join(lines), parse_mode="Markdown")


@router.message(F.text == "🟢 Активные")
async def active_shifts(message: Message):
    await cmd_active(message)


@router.message(F.text == "📊 Статистика")
async def stats_menu(message: Message):
    if message.from_user.id not in ALL_ADMINS:
        await message.answer("❗ Нет доступа")
        return
    
    employees = await db.get_approved_employees()
    if not employees:
        await message.answer("Нет одобренных сотрудников")
        return
    
    # Сохраняем режим выбора сотрудника
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=emp["full_name"])] for emp in employees] + [[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    user_state[message.from_user.id] = "stats_select"
    await message.answer("Выберите сотрудника:", reply_markup=kb)


@router.message(lambda m: user_state.get(m.from_user.id) == "stats_select")
async def stats_show(message: Message):
    uid = message.from_user.id
    name = message.text.strip()
    
    if name == "❌ Отмена":
        del user_state[uid]
        await message.answer("Отменено", reply_markup=get_keyboard(uid))
        return
    
    emp = await db.get_employee_by_name(name)
    if not emp:
        await message.answer("Сотрудник не найден, попробуйте ещё раз")
        return
    
    shifts = await db.get_shifts_this_week(emp["telegram_id"])
    breaks = await db.get_breaks_for_week(emp["telegram_id"])
    
    shift_mins = 0
    for s in shifts:
        if s["closed_at"]:
            try:
                o = datetime.strptime(s["opened_at"], "%Y-%m-%d %H:%M:%S")
                c = datetime.strptime(s["closed_at"], "%Y-%m-%d %H:%M:%S")
                shift_mins += int((c - o).total_seconds() // 60)
            except:
                pass
    
    break_mins = 0
    for b in breaks:
        if b["ended_at"]:
            try:
                s = datetime.strptime(b["started_at"], "%Y-%m-%d %H:%M:%S")
                e = datetime.strptime(b["ended_at"], "%Y-%m-%d %H:%M:%S")
                break_mins += int((e - s).total_seconds() // 60)
            except:
                pass
    
    text = f"📊 *{emp['full_name']}*\n\n📅 Смен за неделю: {len(shifts)} ({fmt_duration(shift_mins)})\n☕ Перерывов: {len(breaks)} ({fmt_duration(break_mins)})"
    await message.answer(text, parse_mode="Markdown")
    
    del user_state[uid]
    await message.answer("Меню:", reply_markup=get_keyboard(uid))


# ---------- /chatid ----------

@router.message(Command("chatid"))
async def chatid(message: Message):
    await message.answer(f"Chat ID: `{message.chat.id}`", parse_mode="Markdown")
