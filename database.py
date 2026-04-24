import aiosqlite
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "pvz.db")


def _now():
    return "datetime('now', '+3 hours')"


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
                approved INTEGER DEFAULT 0
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
                photo_close_id TEXT
            )
        """)
        
        # Таблица перерывов
        await db.execute(f"""
            CREATE TABLE IF NOT EXISTS breaks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                started_at TEXT DEFAULT ({_now()}),
                ended_at TEXT,
                photo_id TEXT
            )
        """)
        
        await db.commit()

        # Миграции для старых БД (если таблицы уже были)
        for col in ["closed_at", "photo_open_id", "photo_close_id"]:
            try:
                await db.execute(f"ALTER TABLE shifts ADD COLUMN {col} TEXT")
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
    """Удалить сотрудника"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM employees WHERE telegram_id = ?", (telegram_id,))
        await db.commit()


async def get_all_employees():
    """Получить всех сотрудников (сортировка по дате регистрации)"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM employees ORDER BY registered_at DESC") as cur:
            return await cur.fetchall()


async def get_approved_employees():
    """Получить только одобренных сотрудников (сортировка по имени)"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM employees WHERE approved = 1 ORDER BY full_name") as cur:
            return await cur.fetchall()


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
        await db.execute(
            "UPDATE shifts SET closed_at = datetime('now', '+3 hours'), photo_close_id = ? WHERE id = ?",
            (photo_id, shift_id)
        )
        await db.commit()


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
            WHERE s.closed_at IS NULL
            ORDER BY s.opened_at ASC
        """) as cur:
            return await cur.fetchall()


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
        await db.execute(
            "UPDATE breaks SET ended_at = datetime('now', '+3 hours') WHERE id = ?",
            (break_id,)
        )
        await db.commit()


async def get_breaks_for_week(telegram_id: int):
    """Получить все перерывы сотрудника за текущую неделю (с понедельника)"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM breaks
            WHERE telegram_id = ?
              AND date(started_at) >= date(datetime('now', '+3 hours'), 'weekday 1', '-7 days')
            ORDER BY started_at ASC
        """, (telegram_id,)) as cur:
            return await cur.fetchall()
