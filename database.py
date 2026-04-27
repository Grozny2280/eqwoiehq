import aiosqlite
import os
import shutil
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "pvz.db")
BACKUP_DIR = os.path.join(os.path.dirname(__file__), "backups")


def _now():
    return "datetime('now', '+3 hours')"


async def create_backup():
    """Создаёт резервную копию БД"""
    if not os.path.exists(DB_PATH):
        return None
    
    os.makedirs(BACKUP_DIR, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"pvz_backup_{timestamp}.db")
    
    shutil.copy2(DB_PATH, backup_path)
    
    # Удаляем старые бэкапы (оставляем последние 10)
    backups = sorted([f for f in os.listdir(BACKUP_DIR) if f.startswith("pvz_backup_")])
    for old_backup in backups[:-10]:
        os.remove(os.path.join(BACKUP_DIR, old_backup))
    
    return backup_path


async def init_db():
    """Создаёт все таблицы при первом запуске"""
    async with aiosqlite.connect(DB_PATH) as db:
        # Таблица сотрудников
        await db.execute(f"""
            CREATE TABLE IF NOT EXISTS employees (
                telegram_id INTEGER PRIMARY KEY,
                full_name TEXT NOT NULL,
                wb_employee_id TEXT NOT NULL,
                registered_at TEXT DEFAULT ({_now()}),
                approved INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                total_shifts INTEGER DEFAULT 0,
                total_work_minutes INTEGER DEFAULT 0
            )
        """)
        
        # Таблица смен
        await db.execute(f"""
            CREATE TABLE IF NOT EXISTS shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                opened_at TEXT DEFAULT ({_now()}),
                closed_at TEXT,
                photo_open_id TEXT,
                photo_close_id TEXT,
                duration_minutes INTEGER DEFAULT 0,
                FOREIGN KEY (telegram_id) REFERENCES employees(telegram_id)
            )
        """)
        
        # Таблица перерывов
        await db.execute(f"""
            CREATE TABLE IF NOT EXISTS breaks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                started_at TEXT DEFAULT ({_now()}),
                ended_at TEXT,
                photo_id TEXT,
                duration_minutes INTEGER DEFAULT 0,
                FOREIGN KEY (telegram_id) REFERENCES employees(telegram_id)
            )
        """)
        
        # Таблица уведомлений (лог)
        await db.execute(f"""
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER,
                type TEXT,
                message TEXT,
                created_at TEXT DEFAULT ({_now()})
            )
        """)
        
        # Таблица настроек
        await db.execute(f"""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT DEFAULT ({_now()})
            )
        """)
        
        await db.commit()
        
        # Дефолтные настройки
        await db.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('break_alert_minutes', '15')"
        )
        await db.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('auto_backup', '1')"
        )
        await db.commit()

        # Миграции для старых БД
        for col in ["closed_at", "photo_open_id", "photo_close_id"]:
            try:
                await db.execute(f"ALTER TABLE shifts ADD COLUMN {col} TEXT")
                await db.commit()
            except:
                pass
        try:
            await db.execute("ALTER TABLE shifts ADD COLUMN duration_minutes INTEGER DEFAULT 0")
            await db.commit()
        except:
            pass
        try:
            await db.execute("ALTER TABLE breaks ADD COLUMN duration_minutes INTEGER DEFAULT 0")
            await db.commit()
        except:
            pass
        try:
            await db.execute("ALTER TABLE employees ADD COLUMN total_shifts INTEGER DEFAULT 0")
            await db.commit()
        except:
            pass
        try:
            await db.execute("ALTER TABLE employees ADD COLUMN total_work_minutes INTEGER DEFAULT 0")
            await db.commit()
        except:
            pass
        try:
            await db.execute("ALTER TABLE employees ADD COLUMN is_active INTEGER DEFAULT 1")
            await db.commit()
        except:
            pass


# ========== РАБОТА С СОТРУДНИКАМИ ==========

async def get_employee(telegram_id: int):
    """Получить сотрудника по Telegram ID"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM employees WHERE telegram_id = ?", (telegram_id,)) as cur:
            return await cur.fetchone()


async def get_employee_by_name(full_name: str):
    """Получить сотрудника по полному имени"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM employees WHERE full_name = ?", (full_name,)) as cur:
            return await cur.fetchone()


async def register_employee(telegram_id: int, full_name: str, wb_id: str):
    """Зарегистрировать нового сотрудника (статус approved = 0)"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO employees (telegram_id, full_name, wb_employee_id, approved) VALUES (?, ?, ?, 0)",
            (telegram_id, full_name, wb_id)
        )
        await db.commit()


async def approve_employee(telegram_id: int):
    """Одобрить сотрудника"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE employees SET approved = 1 WHERE telegram_id = ?", (telegram_id,))
        await db.commit()


async def delete_employee(telegram_id: int):
    """Удалить сотрудника (мягкое удаление)"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE employees SET is_active = 0 WHERE telegram_id = ?", (telegram_id,))
        await db.commit()


async def delete_employee_full(telegram_id: int):
    """Полностью удалить сотрудника и все его данные"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM breaks WHERE telegram_id = ?", (telegram_id,))
        await db.execute("DELETE FROM shifts WHERE telegram_id = ?", (telegram_id,))
        await db.execute("DELETE FROM employees WHERE telegram_id = ?", (telegram_id,))
        await db.commit()


async def get_all_employees():
    """Получить всех активных сотрудников"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM employees WHERE is_active = 1 ORDER BY registered_at DESC") as cur:
            return await cur.fetchall()


async def get_approved_employees():
    """Получить только одобренных сотрудников"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM employees WHERE approved = 1 AND is_active = 1 ORDER BY full_name") as cur:
            return await cur.fetchall()


async def update_employee_name(telegram_id: int, new_name: str):
    """Изменить ФИО сотрудника"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE employees SET full_name = ? WHERE telegram_id = ?", (new_name, telegram_id))
        await db.commit()


async def update_employee_wb(telegram_id: int, new_wb_id: str):
    """Изменить WB ID сотрудника"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE employees SET wb_employee_id = ? WHERE telegram_id = ?", (new_wb_id, telegram_id))
        await db.commit()


async def update_employee_stats(telegram_id: int):
    """Обновить статистику сотрудника (общее количество смен и время)"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE employees SET
                total_shifts = (SELECT COUNT(*) FROM shifts WHERE telegram_id = ? AND closed_at IS NOT NULL),
                total_work_minutes = COALESCE((SELECT SUM(duration_minutes) FROM shifts WHERE telegram_id = ?), 0)
            WHERE telegram_id = ?
        """, (telegram_id, telegram_id, telegram_id))
        await db.commit()


# ========== РАБОТА СО СМЕНАМИ ==========

async def open_shift(telegram_id: int, photo_id: str):
    """Открыть новую смену"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO shifts (telegram_id, photo_open_id) VALUES (?, ?)",
            (telegram_id, photo_id)
        )
        await db.commit()


async def get_active_shift(telegram_id: int):
    """Получить активную (незакрытую) смену сотрудника"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM shifts WHERE telegram_id = ? AND closed_at IS NULL ORDER BY id DESC LIMIT 1",
            (telegram_id,)
        ) as cur:
            return await cur.fetchone()


async def close_shift(shift_id: int, photo_id: str):
    """Закрыть смену"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE shifts SET 
                closed_at = datetime('now', '+3 hours'), 
                photo_close_id = ?,
                duration_minutes = CAST(
                    (julianday(datetime('now', '+3 hours')) - julianday(opened_at)) * 24 * 60 AS INTEGER
                )
            WHERE id = ?
        """, (photo_id, shift_id))
        await db.commit()
    
    # Обновляем статистику сотрудника
    shift = await get_shift_by_id(shift_id)
    if shift:
        await update_employee_stats(shift["telegram_id"])


async def get_shift_by_id(shift_id: int):
    """Получить смену по ID"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM shifts WHERE id = ?", (shift_id,)) as cur:
            return await cur.fetchone()


async def get_shifts_this_week(telegram_id: int):
    """Получить все смены сотрудника за текущую неделю (с понедельника)"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM shifts
            WHERE telegram_id = ?
              AND date(opened_at) >= date(datetime('now', '+3 hours'), 'weekday 1', '-7 days')
            ORDER BY opened_at DESC
        """, (telegram_id,)) as cur:
            return await cur.fetchall()


async def get_employee_shifts_recent(telegram_id: int, days: int = 7):
    """Получить смены сотрудника за последние N дней"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM shifts
            WHERE telegram_id = ?
              AND date(opened_at) >= date(datetime('now', '+3 hours'), ? || ' days')
            ORDER BY opened_at DESC
        """, (telegram_id, f"-{days}")) as cur:
            return await cur.fetchall()


async def count_active_shifts():
    """Посчитать количество активных смен у всех сотрудников"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM shifts WHERE closed_at IS NULL") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


async def get_all_active_shifts():
    """Получить все активные смены с именами сотрудников"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT s.*, e.full_name, e.wb_employee_id
            FROM shifts s
            JOIN employees e ON s.telegram_id = e.telegram_id
            WHERE s.closed_at IS NULL AND e.is_active = 1
            ORDER BY s.opened_at ASC
        """) as cur:
            return await cur.fetchall()


async def get_today_stats():
    """Статистика за сегодня"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT 
                COUNT(DISTINCT telegram_id) as active_employees,
                COUNT(*) as total_shifts,
                COALESCE(SUM(duration_minutes), 0) as total_minutes
            FROM shifts
            WHERE date(opened_at) = date(datetime('now', '+3 hours'))
        """) as cur:
            return await cur.fetchone()


async def get_employee_rating():
    """Рейтинг сотрудников по отработанным часам"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT 
                e.full_name,
                e.total_shifts,
                e.total_work_minutes,
                ROUND(e.total_work_minutes / 60.0, 1) as total_hours
            FROM employees e
            WHERE e.approved = 1 AND e.is_active = 1
            ORDER BY e.total_work_minutes DESC
            LIMIT 10
        """) as cur:
            return await cur.fetchall()


# ========== РЕДАКТИРОВАНИЕ СМЕН ==========

async def update_shift_start(shift_id: int, new_start: str):
    """Изменить время начала смены"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE shifts SET 
                opened_at = ?,
                duration_minutes = CASE 
                    WHEN closed_at IS NOT NULL THEN 
                        CAST((julianday(closed_at) - julianday(?)) * 24 * 60 AS INTEGER)
                    ELSE duration_minutes
                END
            WHERE id = ?
        """, (new_start, new_start, shift_id))
        await db.commit()
    
    # Обновляем статистику
    shift = await get_shift_by_id(shift_id)
    if shift:
        await update_employee_stats(shift["telegram_id"])


async def update_shift_end(shift_id: int, new_end: str):
    """Изменить время окончания смены"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE shifts SET 
                closed_at = ?,
                duration_minutes = CAST((julianday(?) - julianday(opened_at)) * 24 * 60 AS INTEGER)
            WHERE id = ?
        """, (new_end, new_end, shift_id))
        await db.commit()
    
    # Обновляем статистику
    shift = await get_shift_by_id(shift_id)
    if shift:
        await update_employee_stats(shift["telegram_id"])


async def update_shift_make_active(shift_id: int):
    """Сделать смену активной (открытой) — очистить closed_at"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE shifts SET 
                closed_at = NULL,
                duration_minutes = 0
            WHERE id = ?
        """, (shift_id,))
        await db.commit()


async def delete_shift(shift_id: int):
    """Удалить смену"""
    async with aiosqlite.connect(DB_PATH) as db:
        # Получаем telegram_id перед удалением
        async with db.execute("SELECT telegram_id FROM shifts WHERE id = ?", (shift_id,)) as cur:
            row = await cur.fetchone()
            tg_id = row[0] if row else None
        
        await db.execute("DELETE FROM shifts WHERE id = ?", (shift_id,))
        await db.commit()
        
        # Обновляем статистику сотрудника
        if tg_id:
            await update_employee_stats(tg_id)


# ========== РАБОТА С ПЕРЕРЫВАМИ ==========

async def start_break(telegram_id: int, photo_id: str):
    """Начать перерыв"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO breaks (telegram_id, photo_id) VALUES (?, ?)",
            (telegram_id, photo_id)
        )
        await db.commit()


async def get_active_break(telegram_id: int):
    """Получить активный (незавершённый) перерыв сотрудника"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM breaks WHERE telegram_id = ? AND ended_at IS NULL ORDER BY id DESC LIMIT 1",
            (telegram_id,)
        ) as cur:
            return await cur.fetchone()


async def end_break(break_id: int):
    """Завершить перерыв"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE breaks SET 
                ended_at = datetime('now', '+3 hours'),
                duration_minutes = CAST(
                    (julianday(datetime('now', '+3 hours')) - julianday(started_at)) * 24 * 60 AS INTEGER
                )
            WHERE id = ?
        """, (break_id,))
        await db.commit()


async def get_breaks_for_week(telegram_id: int):
    """Получить все перерывы сотрудника за текущую неделю"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM breaks
            WHERE telegram_id = ?
              AND date(started_at) >= date(datetime('now', '+3 hours'), 'weekday 1', '-7 days')
            ORDER BY started_at ASC
        """, (telegram_id,)) as cur:
            return await cur.fetchall()


# ========== УВЕДОМЛЕНИЯ ==========

async def log_notification(telegram_id: int, notif_type: str, message: str):
    """Логировать уведомление"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO notifications (telegram_id, type, message) VALUES (?, ?, ?)",
            (telegram_id, notif_type, message)
        )
        await db.commit()


# ========== НАСТРОЙКИ ==========

async def get_setting(key: str, default: str = None):
    """Получить настройку"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else default


async def set_setting(key: str, value: str):
    """Установить настройку"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, datetime('now', '+3 hours'))",
            (key, value)
        )
        await db.commit()
