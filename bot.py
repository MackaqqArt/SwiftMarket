from __future__ import annotations
# ═══════════════════════════════════════════════════════════════
#  🛒 TELEGRAM SHOP BOT  —  aiogram 3.13 + SQLite
#  pip install aiogram==3.13.0
# ═══════════════════════════════════════════════════════════════
import asyncio, os, re, sqlite3, uuid
from datetime import datetime
from html import escape
from pathlib import Path

try:
    from aiogram import Bot, Dispatcher, F, Router
    from aiogram.enums import ParseMode
    from aiogram.filters import CommandStart
    from aiogram.fsm.context import FSMContext
    from aiogram.fsm.state import State, StatesGroup
    from aiogram.fsm.storage.memory import MemoryStorage
    from aiogram.types import (
        CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
        KeyboardButton, Message, ReplyKeyboardMarkup, ReplyKeyboardRemove,
    )
except ModuleNotFoundError as exc:
    raise SystemExit("Не найден aiogram. Установи зависимость: pip install aiogram==3.13.0") from exc

# ═══════════════════════════════════════════════
#  CONFIG  — вставь свои данные
# ═══════════════════════════════════════════════
BASE_DIR = Path(__file__).resolve().parent
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "8552652045:AAGnmBsrWqQqXw4RE_ARcCE4mhqzSGmoBm8").strip()
ADMIN_ID  = int(os.getenv("ADMIN_ID", "944986617"))
DB_FILE   = str((BASE_DIR / os.getenv("SHOP_DB_FILE", "shop.db")).resolve())
CATEGORY_RE = re.compile(r"^[A-Za-z0-9_ ]{2,48}$")

ORDER_BANK_NAMES = {
    "privat": "💳 PrivatBank",
    "mono": "🖤 Monobank",
}
TOPUP_BANK_NAMES = ORDER_BANK_NAMES.copy()
WITHDRAW_BANK_NAMES = {
    "privat": "💳 PrivatBank",
    "mono": "🖤 Monobank",
    "other": "🏦 Другой банк",
}

# ═══════════════════════════════════════════════
#  FORMATTING HELPERS
# ═══════════════════════════════════════════════
def fmt(amount: float, currency: str = "₴") -> str:
    """1234567.5  →  '1,234,567.50 ₴'"""
    return f"{amount:,.2f} {currency}"

def h(value: object) -> str:
    return escape(str(value), quote=False)

def short_text(value: object, limit: int = 24) -> str:
    text = str(value).replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[:max(0, limit - 3)].rstrip() + "..."

def new_id() -> str:
    return str(uuid.uuid4())[:8].upper()

def now_str() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M")

def sql_datetime_order_expr(column: str = "created_at") -> str:
    return (
        f"substr({column},7,4) || substr({column},4,2) || substr({column},1,2) || "
        f"replace(substr({column},12,5), ':', '')"
    )

def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def username_value(user: dict | None) -> str:
    return user.get("username", "") if user else ""

def display_username(user: dict | None, fallback_id: int | None = None) -> str:
    username = username_value(user)
    if username:
        return f"@{username}"
    if fallback_id is not None:
        return f"id{fallback_id}"
    if user and user.get("user_id") is not None:
        return f"id{user['user_id']}"
    return "—"

def format_card_number(card_number: str) -> str:
    digits = re.sub(r"\D", "", card_number)
    return " ".join(digits[i:i + 4] for i in range(0, len(digits), 4))

def normalize_card_number(card_number: str) -> str | None:
    digits = re.sub(r"\D", "", card_number)
    if not 12 <= len(digits) <= 19:
        return None
    return format_card_number(digits)

def settings_bank_key_name(setting_key: str) -> str | None:
    return {
        "privat_card": "💳 PrivatBank",
        "mono_card": "🖤 Monobank",
        "topup_privat_card": "💳 PrivatBank",
        "topup_mono_card": "🖤 Monobank",
    }.get(setting_key)

def validate_runtime_config():
    errors = []
    if not BOT_TOKEN or ":" not in BOT_TOKEN:
        errors.append("BOT_TOKEN не задан или имеет неверный формат.")
    if ADMIN_ID <= 0:
        errors.append("ADMIN_ID должен быть положительным числом.")
    if errors:
        raise RuntimeError("Ошибка конфигурации:\n- " + "\n- ".join(errors))

STATUS = {
    "pending_payment":   "⏳ Ожидает оплаты",
    "receipt_sent":      "📸 Чек отправлен",
    "payment_confirmed": "✅ Оплата подтверждена",
    "invite_sent":       "📨 Выполнен",
    "cancelled":         "❌ Отменён",
    "rejected":          "❌ Отклонён",
    "pending":           "⏳ Ожидает",
    "confirmed":         "✅ Подтверждено",
    "transfer_sent":     "💸 Перевод отправлен",
    "completed":         "✅ Завершён",
    "balance_paid":      "💰 Оплачен с баланса",
}
def slabel(s: str) -> str:
    return STATUS.get(s, s)

PRODUCT_STATUS_LABELS = {
    "active": "✅ Показан",
    "soon": "🕒 Скоро",
    "hidden": "🔒 Скрыт",
}

UNLIMITED_STOCK = 999999

FIELD_LABELS = {
    "name": "название", "description": "описание", "price": "цену",
    "emoji": "эмодзи", "badge": "бейдж", "category": "категорию", "stock": "остаток",
}

# ═══════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════
def dbc() -> sqlite3.Connection:
    c = sqlite3.connect(DB_FILE, timeout=30)
    c.row_factory = sqlite3.Row
    return c

def ensure_column(c: sqlite3.Connection, table: str, column: str, definition: str):
    cols = {row["name"] for row in c.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

def db_init():
    with dbc() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS users (
                user_id    INTEGER PRIMARY KEY,
                username   TEXT    DEFAULT '',
                balance    REAL    DEFAULT 0.0,
                purchases  INTEGER DEFAULT 0,
                created_at TEXT    DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS categories (
                name       TEXT PRIMARY KEY,
                created_at TEXT DEFAULT '',
                sort_order INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS products (
                id          TEXT PRIMARY KEY,
                category    TEXT    DEFAULT '',
                emoji       TEXT    DEFAULT '📦',
                name        TEXT    NOT NULL,
                badge       TEXT    DEFAULT '',
                description TEXT    DEFAULT '',
                price       REAL    DEFAULT 0.0,
                currency    TEXT    DEFAULT '₴',
                product_status TEXT DEFAULT 'active',
                available   INTEGER DEFAULT 1,
                stock       INTEGER DEFAULT 999999,
                photo_id    TEXT    DEFAULT NULL,
                sort_order  INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS orders (
                id              TEXT PRIMARY KEY,
                user_id         INTEGER,
                username        TEXT    DEFAULT '',
                product_id      TEXT,
                product_name    TEXT    DEFAULT '',
                product_emoji   TEXT    DEFAULT '',
                product_badge   TEXT    DEFAULT '',
                product_quantity INTEGER DEFAULT 1,
                product_unit_price REAL DEFAULT 0.0,
                product_price   REAL    DEFAULT 0.0,
                product_currency TEXT   DEFAULT '₴',
                email           TEXT    DEFAULT '',
                bank            TEXT    DEFAULT '',
                status          TEXT    DEFAULT 'pending_payment',
                paid_by_balance INTEGER DEFAULT 0,
                created_at      TEXT    DEFAULT '',
                receipt_file_id TEXT    DEFAULT NULL
            );
            CREATE TABLE IF NOT EXISTS topups (
                id              TEXT PRIMARY KEY,
                user_id         INTEGER,
                username        TEXT    DEFAULT '',
                amount          REAL    DEFAULT 0,
                bank            TEXT    DEFAULT '',
                status          TEXT    DEFAULT 'pending',
                created_at      TEXT    DEFAULT '',
                receipt_file_id TEXT    DEFAULT NULL
            );
            CREATE TABLE IF NOT EXISTS withdrawals (
                id          TEXT PRIMARY KEY,
                user_id     INTEGER,
                username    TEXT    DEFAULT '',
                amount      REAL    DEFAULT 0,
                bank_name   TEXT    DEFAULT '',
                card_number TEXT    DEFAULT '',
                status      TEXT    DEFAULT 'pending',
                created_at  TEXT    DEFAULT ''
            );
        """)
        ensure_column(c, "products", "product_status", "TEXT DEFAULT 'active'")
        ensure_column(c, "products", "stock", f"INTEGER DEFAULT {UNLIMITED_STOCK}")
        ensure_column(c, "orders", "product_name", "TEXT DEFAULT ''")
        ensure_column(c, "orders", "product_emoji", "TEXT DEFAULT ''")
        ensure_column(c, "orders", "product_badge", "TEXT DEFAULT ''")
        ensure_column(c, "orders", "product_quantity", "INTEGER DEFAULT 1")
        ensure_column(c, "orders", "product_unit_price", "REAL DEFAULT 0.0")
        ensure_column(c, "orders", "product_price", "REAL DEFAULT 0.0")
        ensure_column(c, "orders", "product_currency", "TEXT DEFAULT '₴'")
        c.execute("""
            UPDATE products
               SET product_status = CASE
                   WHEN product_status NOT IN ('active', 'soon', 'hidden') OR product_status = '' THEN
                       CASE WHEN COALESCE(available, 1) = 1 THEN 'active' ELSE 'soon' END
                   ELSE product_status
               END
        """)
        c.execute("""
            UPDATE products
               SET product_status = 'soon'
             WHERE COALESCE(available, 1) = 0
               AND product_status = 'active'
        """)
        c.execute("""
            UPDATE products
               SET available = CASE WHEN product_status = 'active' THEN 1 ELSE 0 END
        """)
        c.execute(f"UPDATE products SET stock = {UNLIMITED_STOCK} WHERE stock IS NULL OR stock < 0")
        existing_categories = {
            str(row["name"] or "").strip()
            for row in c.execute("SELECT name FROM categories").fetchall()
        }
        category_names = [
            str(row["category"] or "").strip()
            for row in c.execute("SELECT DISTINCT category FROM products WHERE TRIM(COALESCE(category,'')) <> ''").fetchall()
        ]
        category_sort = c.execute("SELECT COALESCE(MAX(sort_order),0) FROM categories").fetchone()[0]
        for category_name in category_names:
            if category_name and category_name not in existing_categories:
                category_sort += 1
                c.execute(
                    "INSERT INTO categories(name, created_at, sort_order) VALUES(?,?,?)",
                    (category_name, now_str(), category_sort),
                )
        c.execute("""
            UPDATE orders
               SET product_name = COALESCE(NULLIF(product_name, ''), (SELECT name FROM products WHERE id = product_id), ''),
                   product_emoji = COALESCE(NULLIF(product_emoji, ''), (SELECT emoji FROM products WHERE id = product_id), ''),
                   product_badge = COALESCE(NULLIF(product_badge, ''), (SELECT badge FROM products WHERE id = product_id), ''),
                   product_quantity = CASE WHEN product_quantity <= 0 THEN 1 ELSE product_quantity END,
                   product_unit_price = CASE
                       WHEN product_unit_price = 0 THEN COALESCE((SELECT price FROM products WHERE id = product_id), product_price, 0)
                       ELSE product_unit_price
                   END,
                   product_price = CASE
                       WHEN product_price = 0 THEN COALESCE((SELECT price FROM products WHERE id = product_id), 0)
                       ELSE product_price
                   END,
                   product_currency = CASE
                       WHEN product_currency = '' THEN COALESCE((SELECT currency FROM products WHERE id = product_id), '₴')
                       ELSE product_currency
                   END
        """)
        _seed_settings(c)
        _seed_products(c)

def _seed_settings(c: sqlite3.Connection):
    defaults = {
        "support_user":        "@support_manager",
        "privat_card":         "4149 6090 1234 5678",
        "mono_card":           "5375 4141 8765 4321",
        "topup_privat_card":   "4149 6090 1234 5678",
        "topup_mono_card":     "5375 4141 8765 4321",
    }
    for k, v in defaults.items():
        c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k, v))

def _seed_products(c: sqlite3.Connection):
    if c.execute("SELECT COUNT(*) FROM products").fetchone()[0] > 0:
        return
    rows = [
        ("yt_default", "youtube",  "❤️",  "YouTube Premium", "DEFAULT",
         "• Без рекламы на всех устройствах\n• Фоновое воспроизведение\n• Скачивание видео оффлайн\n• YouTube Music Premium\n• Активация через Family-аккаунт\n\n⏱ Срок: 1 месяц · Активация до 24 ч",
         149.0, "₴", 1, None, 1),
        ("yt_renewal", "youtube",  "🔄",  "YouTube Premium", "RENEWAL",
         "• Всё то же, что DEFAULT\n• Для действующей подписки\n• Продление без потери истории\n\n⏱ Срок: 1 месяц · Активация до 24 ч",
         129.0, "₴", 1, None, 2),
        ("spotify",    "spotify",  "💚",  "Spotify Premium",  "",
         "• Музыка без рекламы\n• Оффлайн прослушивание\n• Неограниченные пропуски\n\n⏱ Срок: 1 месяц · Активация до 24 ч",
         99.0,  "₴", 0, None, 3),
        ("chatgpt",    "chatgpt",  "🤖",  "ChatGPT Plus",     "",
         "• GPT-4o без ограничений\n• Генерация изображений DALL·E 3\n\n⏱ Срок: 1 месяц · Активация до 24 ч",
         199.0, "₴", 0, None, 4),
        ("netflix",    "netflix",  "🎬",  "Netflix Premium",  "",
         "• 4K Ultra HD · 4 экрана\n• Скачивание для оффлайн\n\n⏱ Срок: 1 месяц · Активация до 24 ч",
         249.0, "₴", 0, None, 5),
    ]
    c.executemany(
        "INSERT OR IGNORE INTO products("
        "id,category,emoji,name,badge,description,price,currency,product_status,available,stock,photo_id,sort_order"
        ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                pid, category, emoji, name, badge, description, price, currency,
                "active" if available else "soon",
                available, UNLIMITED_STOCK, photo_id, sort_order,
            )
            for pid, category, emoji, name, badge, description, price, currency, available, photo_id, sort_order in rows
        ]
    )

# ── Settings ───────────────────────────────────
def setting_get(key: str, fallback: str = "") -> str:
    with dbc() as c:
        r = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return r[0] if r else fallback

def setting_set(key: str, value: str):
    with dbc() as c:
        c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, value))

# ── Users ──────────────────────────────────────
def user_get(uid: int) -> dict | None:
    with dbc() as c:
        r = c.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
        return dict(r) if r else None

def user_upsert(uid: int, username: str):
    with dbc() as c:
        c.execute("""
            INSERT INTO users(user_id,username,balance,purchases,created_at)
            VALUES(?,?,0.0,0,?)
            ON CONFLICT(user_id) DO UPDATE SET username=excluded.username
        """, (uid, username or "", now_str()))

def user_add_balance(uid: int, amount: float):
    with dbc() as c:
        c.execute("UPDATE users SET balance=ROUND(balance+?,2) WHERE user_id=?", (amount, uid))

def user_sub_balance(uid: int, amount: float):
    with dbc() as c:
        c.execute("UPDATE users SET balance=ROUND(balance-?,2) WHERE user_id=?", (amount, uid))

def user_inc_purchases(uid: int, qty: int = 1):
    qty = max(1, int(qty))
    with dbc() as c:
        c.execute("UPDATE users SET purchases=purchases+? WHERE user_id=?", (qty, uid))

def users_list(search: str = "", offset: int = 0, limit: int = 8) -> list[dict]:
    with dbc() as c:
        order_expr = sql_datetime_order_expr("created_at")
        if search:
            rows = c.execute(
                "SELECT * FROM users WHERE username LIKE ? OR CAST(user_id AS TEXT) LIKE ? "
                f"ORDER BY {order_expr} DESC LIMIT ? OFFSET ?",
                (f"%{search}%", f"%{search}%", limit, offset)
            ).fetchall()
        else:
            rows = c.execute(
                f"SELECT * FROM users ORDER BY {order_expr} DESC LIMIT ? OFFSET ?",
                (limit, offset)
            ).fetchall()
        return [dict(r) for r in rows]

def users_count(search: str = "") -> int:
    with dbc() as c:
        if search:
            return c.execute(
                "SELECT COUNT(*) FROM users WHERE username LIKE ? OR CAST(user_id AS TEXT) LIKE ?",
                (f"%{search}%", f"%{search}%")
            ).fetchone()[0]
        return c.execute("SELECT COUNT(*) FROM users").fetchone()[0]

def all_user_ids(exclude_admin: bool = False) -> list[int]:
    with dbc() as c:
        if exclude_admin:
            rows = c.execute(
                "SELECT user_id FROM users WHERE user_id != ? ORDER BY user_id",
                (ADMIN_ID,)
            ).fetchall()
        else:
            rows = c.execute("SELECT user_id FROM users ORDER BY user_id").fetchall()
        return [int(r[0]) for r in rows]

def product_status_value(product: dict | None) -> str:
    if not product:
        return "hidden"
    status = str(product.get("product_status") or "").strip().lower()
    if status in PRODUCT_STATUS_LABELS:
        return status
    return "active" if product.get("available") else "hidden"

def is_unlimited_stock(stock: int | float | None) -> bool:
    try:
        return int(stock or 0) >= UNLIMITED_STOCK
    except (TypeError, ValueError):
        return False

def product_stock_value(product: dict | None) -> int:
    if not product:
        return 0
    try:
        stock = int(product.get("stock", UNLIMITED_STOCK))
    except (TypeError, ValueError):
        stock = UNLIMITED_STOCK
    return max(0, stock)

def product_stock_text(product: dict | None) -> str:
    stock = product_stock_value(product)
    return "∞" if is_unlimited_stock(stock) else str(stock)

def product_group_key(product: dict | None) -> str:
    if not product:
        return ""
    category = str(product.get("category") or "").strip()
    return category if category else str(product.get("id") or "")

def product_is_group(product: dict | None) -> bool:
    return bool(product and str(product.get("category") or "").strip())

def product_is_visible(product: dict | None) -> bool:
    return product_status_value(product) != "hidden"

def product_is_active(product: dict | None) -> bool:
    return product_status_value(product) == "active"

def product_is_buyable(product: dict | None, qty: int = 1) -> bool:
    if not product or not product_is_active(product):
        return False
    stock = product_stock_value(product)
    if is_unlimited_stock(stock):
        return True
    return stock >= max(1, int(qty))

def product_selectable_quantities(product: dict | None) -> list[int]:
    if not product_is_active(product):
        return []
    stock = product_stock_value(product)
    limit = 10 if is_unlimited_stock(stock) else min(stock, 10)
    choices = [1, 2, 3, 5, 10]
    return [q for q in choices if q <= limit]

def normalize_product(product: dict | None) -> dict | None:
    if not product:
        return None
    status = product_status_value(product)
    stock = product_stock_value(product)
    normalized = dict(product)
    normalized["product_status"] = status
    normalized["available"] = 1 if status == "active" else 0
    normalized["stock"] = stock
    return normalized

def product_take_stock(pid: str, qty: int) -> bool:
    qty = max(1, int(qty))
    with dbc() as c:
        product = c.execute("SELECT stock, product_status FROM products WHERE id=?", (pid,)).fetchone()
        if not product:
            return False
        stock = max(0, int(product["stock"] or 0))
        status = str(product["product_status"] or "").strip().lower()
        if status != "active":
            return False
        if is_unlimited_stock(stock):
            return True
        if stock < qty:
            return False
        c.execute("UPDATE products SET stock = stock - ? WHERE id=?", (qty, pid))
        return True

def product_restore_stock(pid: str, qty: int):
    qty = max(1, int(qty))
    with dbc() as c:
        product = c.execute("SELECT stock FROM products WHERE id=?", (pid,)).fetchone()
        if not product:
            return
        stock = max(0, int(product["stock"] or 0))
        if is_unlimited_stock(stock):
            return
        c.execute("UPDATE products SET stock = stock + ? WHERE id=?", (qty, pid))

def normalize_category_name(value: str) -> str:
    return " ".join(str(value or "").strip().split())

def categories_all() -> list[dict]:
    with dbc() as c:
        return [dict(row) for row in c.execute(
            "SELECT * FROM categories ORDER BY sort_order, name COLLATE NOCASE"
        ).fetchall()]

def category_create(name: str) -> dict | None:
    name = normalize_category_name(name)
    if not name:
        return None
    with dbc() as c:
        existing = c.execute("SELECT * FROM categories WHERE name=?", (name,)).fetchone()
        if existing:
            return dict(existing)
        sort_order = c.execute("SELECT COALESCE(MAX(sort_order),0) FROM categories").fetchone()[0] + 1
        c.execute(
            "INSERT INTO categories(name, created_at, sort_order) VALUES(?,?,?)",
            (name, now_str(), sort_order),
        )
        created = c.execute("SELECT * FROM categories WHERE name=?", (name,)).fetchone()
        return dict(created) if created else None

# ── Products ───────────────────────────────────
def products_all(only_available: bool = False) -> list[dict]:
    with dbc() as c:
        q = "SELECT * FROM products ORDER BY sort_order"
        rows = [normalize_product(dict(r)) for r in c.execute(q).fetchall()]
        if only_available:
            rows = [r for r in rows if product_is_active(r)]
        return rows

def product_get(pid: str) -> dict | None:
    with dbc() as c:
        r = c.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
        return normalize_product(dict(r)) if r else None

def product_update(pid: str, **kw):
    if not kw:
        return
    if "product_status" in kw:
        status = str(kw["product_status"]).strip().lower()
        if status not in PRODUCT_STATUS_LABELS:
            raise ValueError("Некорректный статус товара.")
        kw["product_status"] = status
        kw["available"] = 1 if status == "active" else 0
    if "stock" in kw:
        stock = int(kw["stock"])
        if stock < 0:
            raise ValueError("Остаток товара не может быть отрицательным.")
        kw["stock"] = stock
    if "category" in kw:
        kw["category"] = normalize_category_name(kw["category"])
        if kw["category"]:
            category_create(kw["category"])
    cols = ", ".join(f"{k}=?" for k in kw)
    with dbc() as c:
        c.execute(f"UPDATE products SET {cols} WHERE id=?", (*kw.values(), pid))

def product_create(pid, name, emoji, category, badge, description, price, currency="₴", product_status="active", stock=UNLIMITED_STOCK) -> dict:
    product_status = str(product_status).strip().lower()
    if product_status not in PRODUCT_STATUS_LABELS:
        product_status = "active"
    stock = max(0, int(stock))
    category = normalize_category_name(category)
    if category:
        category_create(category)
    with dbc() as c:
        so = c.execute("SELECT COALESCE(MAX(sort_order),0) FROM products").fetchone()[0]
        c.execute(
            "INSERT INTO products(id,category,emoji,name,badge,description,price,currency,product_status,available,stock,photo_id,sort_order)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (pid, category, emoji, name, badge, description, price, currency, product_status, 1 if product_status == "active" else 0, stock, None, so + 1)
        )
    return product_get(pid)

def product_delete(pid: str):
    with dbc() as c:
        c.execute("DELETE FROM products WHERE id=?", (pid,))

# ── Orders ─────────────────────────────────────
def order_get(oid: str) -> dict | None:
    with dbc() as c:
        r = c.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
        return dict(r) if r else None

def order_create(oid, uid, username, pid, email, bank, paid_by_balance=0, quantity=1) -> dict:
    p = product_get(pid) or {}
    quantity = max(1, int(quantity))
    unit_price = float(p.get("price", 0.0) or 0.0)
    total_price = round(unit_price * quantity, 2)
    with dbc() as c:
        c.execute("""
            INSERT INTO orders(
                id,user_id,username,product_id,product_name,product_emoji,product_badge,
                product_quantity,product_unit_price,product_price,product_currency,email,bank,status,paid_by_balance,created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'pending_payment',?,?)
        """, (
            oid, uid, username, pid,
            p.get("name", ""), p.get("emoji", ""), p.get("badge", ""),
            quantity, unit_price, total_price, p.get("currency", "₴"),
            email, bank, paid_by_balance, now_str()
        ))
    return order_get(oid)

def order_set(oid: str, **kw):
    cols = ", ".join(f"{k}=?" for k in kw)
    with dbc() as c:
        c.execute(f"UPDATE orders SET {cols} WHERE id=?", (*kw.values(), oid))

def orders_by_user(uid: int) -> list[dict]:
    with dbc() as c:
        order_expr = sql_datetime_order_expr("created_at")
        return [dict(r) for r in c.execute(
            f"SELECT * FROM orders WHERE user_id=? ORDER BY {order_expr} DESC", (uid,)
        ).fetchall()]

# ── Topups ─────────────────────────────────────
def topup_get(tid: str) -> dict | None:
    with dbc() as c:
        r = c.execute("SELECT * FROM topups WHERE id=?", (tid,)).fetchone()
        return dict(r) if r else None

def topup_create(tid, uid, username, amount, bank) -> dict:
    with dbc() as c:
        c.execute(
            "INSERT INTO topups(id,user_id,username,amount,bank,status,created_at) VALUES(?,?,?,?,?,'pending',?)",
            (tid, uid, username, amount, bank, now_str())
        )
    return topup_get(tid)

def topup_set(tid: str, **kw):
    cols = ", ".join(f"{k}=?" for k in kw)
    with dbc() as c:
        c.execute(f"UPDATE topups SET {cols} WHERE id=?", (*kw.values(), tid))

def topups_by_user(uid: int) -> list[dict]:
    with dbc() as c:
        order_expr = sql_datetime_order_expr("created_at")
        return [dict(r) for r in c.execute(
            f"SELECT * FROM topups WHERE user_id=? ORDER BY {order_expr} DESC", (uid,)
        ).fetchall()]

# ── Withdrawals ────────────────────────────────
def withdrawal_get(wid: str) -> dict | None:
    with dbc() as c:
        r = c.execute("SELECT * FROM withdrawals WHERE id=?", (wid,)).fetchone()
        return dict(r) if r else None

def withdrawal_create(wid, uid, username, amount, bank_name, card_number) -> dict:
    with dbc() as c:
        c.execute(
            "INSERT INTO withdrawals(id,user_id,username,amount,bank_name,card_number,status,created_at)"
            " VALUES(?,?,?,?,?,?,'pending',?)",
            (wid, uid, username, amount, bank_name, card_number, now_str())
        )
    return withdrawal_get(wid)

def withdrawal_set(wid: str, **kw):
    cols = ", ".join(f"{k}=?" for k in kw)
    with dbc() as c:
        c.execute(f"UPDATE withdrawals SET {cols} WHERE id=?", (*kw.values(), wid))

def withdrawals_by_user(uid: int) -> list[dict]:
    with dbc() as c:
        order_expr = sql_datetime_order_expr("created_at")
        return [dict(r) for r in c.execute(
            f"SELECT * FROM withdrawals WHERE user_id=? ORDER BY {order_expr} DESC", (uid,)
        ).fetchall()]

# ═══════════════════════════════════════════════
#  FSM STATES
# ═══════════════════════════════════════════════
class OrderFSM(StatesGroup):
    waiting_email   = State()
    choosing_bank   = State()
    waiting_receipt = State()

class TopUpFSM(StatesGroup):
    choosing_amount = State()
    choosing_bank   = State()
    waiting_receipt = State()

class WithdrawFSM(StatesGroup):
    choosing_bank   = State()
    entering_amount = State()
    entering_card   = State()
    confirming_card = State()

class AdminFSM(StatesGroup):
    # product edit
    edit_field       = State()
    new_catalog_category = State()
    new_name         = State()
    new_emoji        = State()
    new_category     = State()
    new_badge        = State()
    new_description  = State()
    new_price        = State()
    new_stock        = State()
    new_photo        = State()
    # user management
    user_search      = State()
    manual_balance   = State()
    # settings
    editing_setting  = State()
    # messaging
    sending_message  = State()   # admin sends message to user

# ═══════════════════════════════════════════════
#  KEYBOARD BUILDERS
# ═══════════════════════════════════════════════
def ikb(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t, callback_data=d) for t, d in row]
        for row in rows
    ])

def reply_main(uid: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="🛒 Купить"),           KeyboardButton(text="👤 Профиль")],
        [KeyboardButton(text="💰 Пополнить баланс"), KeyboardButton(text="❤️ Помощь")],
    ]
    if is_admin(uid):
        rows.append([KeyboardButton(text="⚙️ Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True,
                               input_field_placeholder="Выбери раздел...")

def reply_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
        input_field_placeholder="Можно отменить текущий процесс",
    )

# ── Catalogue keyboards ────────────────────────
def ikb_catalogue() -> InlineKeyboardMarkup:
    all_p = [p for p in products_all() if product_is_visible(p)]
    seen: dict[str, dict] = {}
    grouped: dict[str, list[dict]] = {}
    for p in all_p:
        key = product_group_key(p)
        grouped.setdefault(key, []).append(p)
        current = seen.get(key)
        if current is None or (not product_is_active(current) and product_is_active(p)):
            seen[key] = p
    rows = []
    for key, p in seen.items():
        group = grouped.get(key, [])
        has_available = any(product_is_active(item) for item in group)
        has_stock = any(product_is_buyable(item) for item in group)
        suffix = "  ·  скоро" if not has_available else ("  ·  нет в наличии" if not has_stock else "")
        label = f"{p['emoji']} {p['name']}{suffix}"
        callback = f"cat_{key}" if product_is_group(p) else f"product_{p['id']}"
        rows.append([(label, callback)])
    return ikb(rows)

def ikb_cat_products(category: str) -> InlineKeyboardMarkup:
    ps   = [p for p in products_all() if p["category"] == category and product_is_visible(p)]
    rows = []
    for p in ps:
        badge   = f"  [{p['badge']}]" if p.get("badge") else ""
        if product_status_value(p) == "soon":
            suffix = "  ·  скоро"
        elif not product_is_buyable(p):
            suffix = "  ·  нет в наличии"
        else:
            suffix = ""
        rows.append([(f"{p['emoji']} {p['name']}{badge}{suffix}", f"product_{p['id']}")])
    rows.append([("🔙 Назад", "buy")])
    return ikb(rows)

def ikb_product_card(pid: str, user_balance: float, selected_qty: int = 1) -> InlineKeyboardMarkup:
    p    = product_get(pid)
    if not p:
        return ikb([[("🔙 Назад", "buy")]])
    cat  = p["category"] or pid
    rows = []
    qty_choices = product_selectable_quantities(p)
    if qty_choices:
        qty_buttons = []
        for qty in qty_choices:
            label = f"• {qty} шт" if qty == selected_qty else f"{qty} шт"
            qty_buttons.append((label, f"pqty_{qty}_{pid}"))
        rows.append(qty_buttons)
    total = round(float(p["price"]) * max(1, selected_qty), 2)
    if product_is_buyable(p, selected_qty):
        if user_balance >= total:
            rows.append([("💰 Оплатить с баланса", f"buy_balance_{selected_qty}_{pid}")])
        rows.append([("🛒 Купить картой", f"buy_card_{selected_qty}_{pid}")])
    rows.append([("🔙 Назад", f"cat_{cat}")])
    return ikb(rows)

# ── Order payment keyboards ────────────────────
def ikb_choose_bank_order(oid: str) -> InlineKeyboardMarkup:
    return ikb([
        [("💳 PrivatBank", f"obank_privat_{oid}"), ("🖤 Monobank", f"obank_mono_{oid}")],
        [("❌ Отменить", f"ocancel_{oid}")],
    ])

def ikb_payment(oid: str) -> InlineKeyboardMarkup:
    return ikb([
        [("✅ Я оплатил — отправить чек", f"opaid_{oid}")],
        [("❌ Отменить оплату", f"ocancel_{oid}")],
    ])

# ── Topup keyboards ────────────────────────────
def ikb_topup_amounts() -> InlineKeyboardMarkup:
    amounts = [50, 100, 150, 200, 300, 500]
    rows    = []
    for i in range(0, len(amounts), 3):
        rows.append([(f"{fmt(a)}", f"tamount_{a}") for a in amounts[i:i+3]])
    rows.append([("✏️ Своя сумма", "tcustom")])
    return ikb(rows)

def ikb_choose_bank_topup(tid: str) -> InlineKeyboardMarkup:
    return ikb([
        [("💳 PrivatBank", f"tbank_privat_{tid}"), ("🖤 Monobank", f"tbank_mono_{tid}")],
        [("❌ Отменить", f"tcancel_{tid}")],
    ])

def ikb_topup_payment(tid: str) -> InlineKeyboardMarkup:
    return ikb([
        [("✅ Я оплатил — отправить чек", f"tpaid_{tid}")],
        [("❌ Отменить", f"tcancel_{tid}")],
    ])

# ── Withdrawal keyboards ───────────────────────
def ikb_withdraw_bank() -> InlineKeyboardMarkup:
    return ikb([
        [("💳 PrivatBank", "wbank_privat"), ("🖤 Monobank", "wbank_mono")],
        [("🏦 Другой банк",  "wbank_other")],
        [("❌ Отмена",       "wcancel")],
    ])

def ikb_withdraw_card_confirm() -> InlineKeyboardMarkup:
    return ikb([
        [("✅ Да, всё верно", "wcard_confirm")],
        [("✏️ Ввести заново", "wcard_reenter")],
        [("❌ Отмена", "wcancel")],
    ])

# ── Profile keyboard ───────────────────────────
def ikb_profile() -> InlineKeyboardMarkup:
    return ikb([
        [("💰 Пополнить баланс", "topup_start"), ("💸 Вывести средства", "withdraw_start")],
        [("📋 История", "my_history")],
    ])

# ── Admin keyboards ────────────────────────────
def ikb_admin_main() -> InlineKeyboardMarkup:
    return ikb([
        [("📦 Товары",      "adm_products"), ("👥 Пользователи", "adm_users")],
        [("📊 Статистика",  "adm_stats"),    ("📢 Рассылка",    "adm_broadcast")],
        [("⚙️ Настройки",   "adm_settings")],
    ])

def admin_product_back_cb(product: dict | None) -> str:
    if not product:
        return "adm_products"
    category = normalize_category_name(product.get("category") or "")
    return f"admcat_{category}" if category else "adm_products"

def admin_product_text(product: dict) -> str:
    st    = PRODUCT_STATUS_LABELS[product_status_value(product)]
    ph    = f"<code>{h(str(product['photo_id'])[:30])}…</code>" if product.get("photo_id") else "нет"
    badge = f" <code>[{h(product['badge'])}]</code>" if product.get("badge") else ""
    category = str(product.get("category") or "").strip() or "без категории"
    return (
        "📦 <b>Редактирование товара</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 <code>{product['id']}</code> · {st}\n\n"
        f"{h(product['emoji'])} <b>{h(product['name'])}{badge}</b>\n"
        f"📂 Категория: <code>{h(category)}</code>\n"
        f"💲 Цена: <code>{fmt(product['price'], product['currency'])}</code>\n"
        f"📦 Остаток: <code>{product_stock_text(product)}</code>\n"
        f"🖼 Фото: {ph}\n\n"
        f"📋 <b>Описание:</b>\n{h(product['description'])}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

def admin_category_overview() -> tuple[list[tuple[str, str]], list[dict]]:
    groups: dict[str, list[dict]] = {normalize_category_name(cat["name"]): [] for cat in categories_all()}
    standalone: list[dict] = []
    for product in products_all():
        category = normalize_category_name(product.get("category") or "")
        if category:
            groups.setdefault(category, []).append(product)
        else:
            standalone.append(product)
    category_rows: list[tuple[str, str]] = []
    for category, items in groups.items():
        visible = sum(1 for item in items if product_status_value(item) == "active")
        soon = sum(1 for item in items if product_status_value(item) == "soon")
        hidden = sum(1 for item in items if product_status_value(item) == "hidden")
        suffix = f"{len(items)} шт"
        if soon:
            suffix += f" · скоро {soon}"
        if hidden:
            suffix += f" · скрыто {hidden}"
        icon = "✅" if visible else ("🕒" if soon else "🔒")
        category_rows.append((f"{icon} 📂 {category} · {suffix}", f"admcat_{category}"))
    category_rows.sort(key=lambda item: item[0].lower())
    return category_rows, standalone

def ikb_admin_products() -> InlineKeyboardMarkup:
    rows = []
    categories, standalone = admin_category_overview()
    for label, callback in categories:
        rows.append([(label, callback)])
    for product in standalone:
        st    = {"active": "✅", "soon": "🕒", "hidden": "🔒"}[product_status_value(product)]
        badge = f" [{product['badge']}]" if product.get("badge") else ""
        stock = f" · {product_stock_text(product)} шт"
        rows.append([(f"{st} {product['emoji']} {product['name']}{badge}{stock}", f"admprod_{product['id']}")])
    rows.append([("📂 Создать категорию", "adm_newcat")])
    rows.append([("➕ Создать товар", "adm_newprod"), ("🔙 Назад", "adm_main")])
    return ikb(rows)

def ikb_admin_category(category: str) -> InlineKeyboardMarkup:
    rows = []
    category = normalize_category_name(category)
    for product in [p for p in products_all() if normalize_category_name(p.get("category") or "") == category]:
        st    = {"active": "✅", "soon": "🕒", "hidden": "🔒"}[product_status_value(product)]
        badge = f" [{product['badge']}]" if product.get("badge") else ""
        stock = f" · {product_stock_text(product)} шт"
        rows.append([(f"{st} {product['emoji']} {product['name']}{badge}{stock}", f"admprod_{product['id']}")])
    rows.append([("🔙 К товарам", "adm_products")])
    return ikb(rows)

def ikb_admin_product(pid: str, back_cb: str | None = None) -> InlineKeyboardMarkup:
    p      = product_get(pid)
    if not p:
        return ikb([[("🔙 К списку", "adm_products")]])
    back_cb = back_cb or admin_product_back_cb(p)
    del_photo = [("🗑 Удалить фото", f"admrphoto_{pid}")] if p.get("photo_id") else []
    rows = [
        [("✏️ Название",    f"admedit_{pid}_name"),
         ("✏️ Описание",    f"admedit_{pid}_description")],
        [("✏️ Цена",        f"admedit_{pid}_price"),
         ("✏️ Эмодзи",     f"admedit_{pid}_emoji")],
        [("✏️ Бейдж",       f"admedit_{pid}_badge"),
         ("✏️ Категория",  f"admedit_{pid}_category")],
        [("📦 Наличие",     f"admedit_{pid}_stock"),
         ("🖼 Фото",        f"admedit_{pid}_photo")],
        [("✅ Показать",    f"admstatus|{pid}|active"),
         ("🕒 Скоро",      f"admstatus|{pid}|soon"),
         ("🔒 Скрыть",     f"admstatus|{pid}|hidden")],
    ]
    if del_photo:
        rows.append(del_photo)
    rows.append([("🗑 Удалить товар", f"admdelete_{pid}")])
    rows.append([("🔙 К списку",      back_cb)])
    return ikb(rows)

def ikb_admin_users(offset: int = 0, search: str = "") -> InlineKeyboardMarkup:
    users = users_list(search=search, offset=offset, limit=8)
    total = users_count(search=search)
    rows  = []
    for u in users:
        uname = display_username(u)
        bal   = fmt(u["balance"])
        rows.append([(f"{uname}  ·  {bal}  ·  🛍{u['purchases']}", f"admusr_{u['user_id']}")])
    nav = []
    if offset > 0:
        nav.append(("◀️", f"admup_{max(0,offset-8)}_{search}"))
    if offset + 8 < total:
        nav.append(("▶️", f"admup_{offset+8}_{search}"))
    if nav:
        rows.append(nav)
    rows.append([("🔍 Поиск", "adm_usersearch"), ("🔙 Назад", "adm_main")])
    return ikb(rows)

def ikb_admin_user(uid: int) -> InlineKeyboardMarkup:
    return ikb([
        [("➕ Пополнить",    f"admbal_add_{uid}"), ("➖ Списать",   f"admbal_sub_{uid}")],
        [("📩 Написать",     f"admmsg_{uid}"),     ("📋 История",  f"admhist_{uid}")],
        [("🔙 К списку",     "adm_users")],
    ])

def ikb_admin_order(oid: str) -> InlineKeyboardMarkup:
    return ikb([
        [("✅ Подтвердить оплату",      f"aconfirm_{oid}")],
        [("📨 Приглашение отправлено",  f"ainvite_{oid}")],
        [("📩 Написать клиенту",        f"amsg_order_{oid}")],
        [("❌ Отклонить",               f"areject_{oid}")],
    ])

def ikb_admin_order_confirmed(oid: str) -> InlineKeyboardMarkup:
    return ikb([
        [("✅ Оплата подтверждена ✔️",  f"aconfirm_{oid}")],
        [("📨 Приглашение отправлено",  f"ainvite_{oid}")],
        [("📩 Написать клиенту",        f"amsg_order_{oid}")],
        [("❌ Отклонить",               f"areject_{oid}")],
    ])

def ikb_admin_order_done(oid: str) -> InlineKeyboardMarkup:
    return ikb([
        [("✅ Оплата подтверждена ✔️",    f"aconfirm_{oid}")],
        [("📨 Приглашение отправлено ✔️", f"ainvite_{oid}")],
        [("📩 Написать клиенту",          f"amsg_order_{oid}")],
    ])

def ikb_admin_topup(tid: str) -> InlineKeyboardMarkup:
    return ikb([
        [("✅ Подтвердить пополнение", f"atconfirm_{tid}")],
        [("📩 Написать клиенту",       f"amsg_topup_{tid}")],
        [("❌ Отклонить",              f"atreject_{tid}")],
    ])

def ikb_admin_topup_done(tid: str) -> InlineKeyboardMarkup:
    return ikb([
        [("✅ Пополнение подтверждено ✔️", f"atconfirm_{tid}")],
        [("📩 Написать клиенту",           f"amsg_topup_{tid}")],
    ])

def ikb_admin_withdrawal(wid: str) -> InlineKeyboardMarkup:
    return ikb([
        [("✅ Подтвердить вывод",      f"awconfirm_{wid}")],
        [("📩 Написать клиенту", f"amsg_wd_{wid}")],
        [("❌ Отклонить",        f"awreject_{wid}")],
    ])

def ikb_admin_withdrawal_transfer(wid: str) -> InlineKeyboardMarkup:
    return ikb([
        [("💸 Деньги отправлены", f"awsent_{wid}")],
        [("📩 Написать клиенту",  f"amsg_wd_{wid}")],
        [("❌ Отклонить",         f"awreject_{wid}")],
    ])

def ikb_admin_withdrawal_done(wid: str) -> InlineKeyboardMarkup:
    return ikb([
        [("✅ Перевод отправлен ✔️", f"awsent_{wid}")],
        [("📩 Написать клиенту",     f"amsg_wd_{wid}")],
    ])

def ikb_settings() -> InlineKeyboardMarkup:
    return ikb([
        [("💬 Контакт поддержки",  "adms_support")],
        [("💳 Карта PrivatBank",   "adms_privat"), ("🖤 Карта Mono", "adms_mono")],
        [("💳 Пополнение PrivatBank",   "adms_topup_privat"), ("🖤 Пополнение Mono", "adms_topup_mono")],
        [("🔙 Назад",              "adm_main")],
    ])

def ikb_tx_detail(back_cb: str, receipt_cb: str | None = None) -> InlineKeyboardMarkup:
    rows = []
    if receipt_cb:
        rows.append([("📸 Посмотреть чек", receipt_cb)])
    rows.append([("🔙 К истории", back_cb)])
    return ikb(rows)

def ikb_user_history_list(orders: list[dict], topups: list[dict], wds: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for o in orders[:7]:
        rows.append([(f"📦 #{o['id']} · {short_text(o.get('product_name', '?'), 24)}", f"histo_{o['id']}")])
    for t in topups[:5]:
        rows.append([(f"💰 #{t['id']} · +{fmt(t['amount'])}", f"histt_{t['id']}")])
    for w in wds[:5]:
        rows.append([(f"💸 #{w['id']} · -{fmt(w['amount'])}", f"histw_{w['id']}")])
    rows.append([("🔙 Назад", "profile_back")])
    return ikb(rows)

def ikb_admin_history_list(uid: int, orders: list[dict], topups: list[dict], wds: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for o in orders[:8]:
        rows.append([(f"📦 #{o['id']} · {short_text(o.get('product_name', '?'), 24)}", f"ahisto_{o['id']}")])
    for t in topups[:5]:
        rows.append([(f"💰 #{t['id']} · +{fmt(t['amount'])}", f"ahistt_{t['id']}")])
    for w in wds[:5]:
        rows.append([(f"💸 #{w['id']} · -{fmt(w['amount'])}", f"ahistw_{w['id']}")])
    rows.append([("🔙 Назад", f"admusr_{uid}")])
    return ikb(rows)

# ── Accepted / done buttons ────────────────────
IKB_ACCEPTED = ikb([[("✅ Я принял приглашение", "accepted")]])

# ═══════════════════════════════════════════════
#  TEXT BUILDERS
# ═══════════════════════════════════════════════
def product_card_text(p: dict, user_balance: float = 0.0) -> str:
    badge   = f"\n🏷 <b>Вариант</b>: <code>{h(p['badge'])}</code>" if p.get("badge") else ""
    status  = product_status_value(p)
    qtys    = product_selectable_quantities(p)
    stock_line = f"📦 <b>В наличии</b>: <code>{product_stock_text(p)}</code>"
    if status == "soon":
        availability = "🕒 <b>Статус</b>: скоро в продаже"
    elif not product_is_buyable(p):
        availability = "⛔ <b>Статус</b>: нет в наличии"
    else:
        availability = "✅ <b>Статус</b>: доступен"
    bal_str = ""
    if user_balance > 0:
        mark    = " ✅" if user_balance >= p["price"] else ""
        bal_str = f"💰 <b>Ваш баланс</b>: <code>{fmt(user_balance)}</code>{mark}"
    qty_hint = ""
    if qtys:
        qty_hint = f"🧮 <b>Можно купить за раз</b>: до <code>{max(qtys)}</code> шт."
    return (
        f"{h(p['emoji'])} <b>{h(p['name'])}</b>{badge}\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💲 <b>Цена за 1 шт.</b>: <code>{fmt(p['price'], h(p['currency']))}</code>\n"
        f"{stock_line}\n"
        f"{availability}\n"
        + (f"{qty_hint}\n" if qty_hint else "")
        + (f"{bal_str}\n" if bal_str else "")
        + "\n📋 <b>Описание</b>\n"
        + f"{h(p['description'])}\n"
        + "\n━━━━━━━━━━━━━━━━━━━━"
    )

def order_admin_text(order: dict) -> str:
    product_name = order.get("product_name") or "?"
    product_emoji = order.get("product_emoji", "")
    product_badge = order.get("product_badge", "")
    badge   = f" [{h(product_badge)}]" if product_badge else ""
    paid_b  = order.get("paid_by_balance", 0)
    if paid_b:
        pay = "💰 С баланса"
    else:
        bank = order.get("bank","")
        bname = ORDER_BANK_NAMES.get(bank, h(bank))
        pay  = bname
    receipt = "прикреплён ниже" if order.get("receipt_file_id") else "не прикреплён"
    username = f"@{h(order['username'])}" if order.get("username") else "—"
    return (
        "🔔 <b>Новый заказ!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 <b>Товар</b>: {h(product_emoji)} {h(product_name)}{badge}\n"
        f"🧮 <b>Количество</b>: <code>{order.get('product_quantity', 1)}</code>\n"
        f"💸 <b>Цена за 1 шт.</b>: <code>{fmt(order.get('product_unit_price', order.get('product_price', 0)), h(order.get('product_currency','₴')))}</code>\n"
        f"💲 <b>Сумма</b>: <code>{fmt(order.get('product_price',0), h(order.get('product_currency','₴')))}</code>\n"
        f"💳 <b>Оплата</b>: {pay}\n"
        f"📸 <b>Чек</b>: {receipt}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🔖 <b>Заказ</b>: <code>#{order['id']}</code>\n"
        f"🕐 <b>Время</b>: {order['created_at']}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>User ID</b>: <code>{order['user_id']}</code>\n"
        f"📧 <b>Gmail</b>: <code>{h(order['email'])}</code>\n"
        f"🔗 <b>Username</b>: {username}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

def topup_admin_text(t: dict) -> str:
    bank    = TOPUP_BANK_NAMES.get(t.get("bank",""), h(t.get("bank","")))
    receipt = "прикреплён ниже" if t.get("receipt_file_id") else "не прикреплён"
    username = f"@{h(t['username'])}" if t.get("username") else "—"
    return (
        "💰 <b>Запрос на пополнение</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💵 <b>Сумма</b>: <code>{fmt(t['amount'])}</code>\n"
        f"💳 <b>Банк</b>: {bank}\n"
        f"📸 <b>Чек</b>: {receipt}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🔖 <b>ID</b>: <code>#{t['id']}</code>\n"
        f"🕐 <b>Время</b>: {t['created_at']}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>User ID</b>: <code>{t['user_id']}</code>\n"
        f"🔗 <b>Username</b>: {username}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

def withdrawal_admin_text(w: dict) -> str:
    username = f"@{h(w['username'])}" if w.get("username") else "—"
    return (
        "💸 <b>Запрос на вывод средств</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💵 <b>Сумма</b>: <code>{fmt(w['amount'])}</code>\n"
        f"🏦 <b>Банк</b>: {h(w['bank_name'])}\n"
        f"💳 <b>Карта</b>: <code>{h(w['card_number'])}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🔖 <b>ID</b>: <code>#{w['id']}</code>\n"
        f"🕐 <b>Время</b>: {w['created_at']}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>User ID</b>: <code>{w['user_id']}</code>\n"
        f"🔗 <b>Username</b>: {username}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

def history_overview_text(title: str, orders: list[dict], topups: list[dict], wds: list[dict]) -> str:
    lines = [f"{title}\n━━━━━━━━━━━━━━━━━━━━\n"]
    if orders:
        lines.append("📦 <b>Заказы:</b>")
        for o in orders[:7]:
            lines.append(
                f"• <code>#{o['id']}</code> {h(o.get('product_emoji', ''))} {h(o.get('product_name', '?'))}\n"
                f"  {slabel(o['status'])} · {o['created_at']}"
            )
    else:
        lines.append("📦 Заказов нет")
    lines.append("")
    if topups:
        lines.append("💰 <b>Пополнения:</b>")
        for t in topups[:5]:
            lines.append(f"• <code>#{t['id']}</code> +{fmt(t['amount'])}\n  {slabel(t['status'])} · {t['created_at']}")
    else:
        lines.append("💰 Пополнений нет")
    lines.append("")
    if wds:
        lines.append("💸 <b>Выводы:</b>")
        for w in wds[:5]:
            lines.append(f"• <code>#{w['id']}</code> -{fmt(w['amount'])}\n  {slabel(w['status'])} · {w['created_at']}")
    else:
        lines.append("💸 Выводов нет")
    if orders or topups or wds:
        lines.append("")
        lines.append("<i>Нажми на нужную операцию кнопками ниже, чтобы открыть детали.</i>")
    return "\n".join(lines)

def order_history_detail_text(order: dict) -> str:
    product_name = order.get("product_name") or "?"
    product_emoji = order.get("product_emoji", "")
    product_badge = order.get("product_badge", "")
    badge = f" <code>[{h(product_badge)}]</code>" if product_badge else ""
    pay_method = "💰 С баланса" if order.get("paid_by_balance") else ORDER_BANK_NAMES.get(order.get("bank", ""), h(order.get("bank", "—")))
    receipt = "📸 Прикреплён" if order.get("receipt_file_id") else "—"
    status_line = f"📌 <b>Статус</b>: {slabel(order['status'])}"
    if order["status"] == "cancelled":
        status_line += "\n❌ <b>Заказ был отменён</b>"
    return (
        "📦 <b>Детали заказа</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔖 <b>ID</b>: <code>#{order['id']}</code>\n"
        f"{status_line}\n"
        f"🕐 <b>Дата</b>: {order['created_at']}\n"
        f"📦 <b>Товар</b>: {h(product_emoji)} {h(product_name)}{badge}\n"
        f"🧮 <b>Количество</b>: <code>{order.get('product_quantity', 1)}</code>\n"
        f"💸 <b>Цена за 1 шт.</b>: <code>{fmt(order.get('product_unit_price', order.get('product_price', 0)), h(order.get('product_currency', '₴')))}</code>\n"
        f"💲 <b>Сумма</b>: <code>{fmt(order.get('product_price', 0), h(order.get('product_currency', '₴')))}</code>\n"
        f"📧 <b>Gmail</b>: <code>{h(order.get('email', '—'))}</code>\n"
        f"💳 <b>Оплата</b>: {pay_method}\n"
        f"📸 <b>Чек</b>: {receipt}"
    )

def topup_history_detail_text(topup: dict) -> str:
    bank = TOPUP_BANK_NAMES.get(topup.get("bank", ""), h(topup.get("bank", "—")))
    receipt = "📸 Прикреплён" if topup.get("receipt_file_id") else "—"
    return (
        "💰 <b>Детали пополнения</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔖 <b>ID</b>: <code>#{topup['id']}</code>\n"
        f"🕐 <b>Дата</b>: {topup['created_at']}\n"
        f"💵 <b>Сумма</b>: <code>{fmt(topup['amount'])}</code>\n"
        f"🏦 <b>Банк</b>: {bank}\n"
        f"📸 <b>Чек</b>: {receipt}\n"
        f"📌 <b>Статус</b>: {slabel(topup['status'])}"
    )

def withdrawal_history_detail_text(withdrawal: dict) -> str:
    return (
        "💸 <b>Детали вывода</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔖 <b>ID</b>: <code>#{withdrawal['id']}</code>\n"
        f"🕐 <b>Дата</b>: {withdrawal['created_at']}\n"
        f"💵 <b>Сумма</b>: <code>{fmt(withdrawal['amount'])}</code>\n"
        f"🏦 <b>Банк</b>: {h(withdrawal.get('bank_name', '—'))}\n"
        f"💳 <b>Карта</b>: <code>{h(withdrawal.get('card_number', '—'))}</code>\n"
        f"📌 <b>Статус</b>: {slabel(withdrawal['status'])}"
    )

async def send_receipt_preview(call: CallbackQuery, file_id: str, title: str, entity_id: str):
    try:
        await call.message.answer_photo(
            photo=file_id,
            caption=(
                f"📸 <b>{h(title)}</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🔖 <code>#{h(entity_id)}</code>"
            ),
            parse_mode=ParseMode.HTML,
        )
        await call.answer("📸 Чек отправлен ниже.")
    except Exception as e:
        print(f"[RECEIPT] {e}")
        await call.answer("Не удалось открыть чек.", show_alert=True)

async def replace_callback_text(call: CallbackQuery, text: str, reply_markup: InlineKeyboardMarkup | None = None):
    try:
        await call.message.edit_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception:
        try:
            await call.message.delete()
        except Exception:
            pass
        await call.message.answer(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def finalize_withdrawal_request(
    message: Message,
    state: FSMContext,
    bot: Bot,
    card_number: str,
    actor_id: int,
    actor_username: str | None,
):
    data = await state.get_data()
    amount = data.get("withdraw_amount")
    bname = data.get("withdraw_bank")
    if amount is None or not bname:
        await state.clear()
        await message.answer(
            "❌ Сессия вывода устарела. Начни заново.",
            reply_markup=reply_main(actor_id),
        )
        return

    user_upsert(actor_id, actor_username or "")
    u = user_get(actor_id)
    if not u:
        await state.clear()
        await message.answer(
            "❌ Не удалось загрузить профиль. Попробуй ещё раз.",
            reply_markup=reply_main(actor_id),
        )
        return
    if amount > u["balance"]:
        await state.clear()
        await message.answer(
            f"❌ На балансе недостаточно средств.\nСейчас доступно: <code>{fmt(u['balance'])}</code>",
            reply_markup=reply_main(actor_id),
            parse_mode=ParseMode.HTML,
        )
        return

    wid = new_id()
    user_sub_balance(actor_id, amount)
    withdrawal_create(wid, actor_id, username_value(u), amount, bname, card_number)
    await state.clear()

    u_after = user_get(actor_id)
    await message.answer(
        "✅ <b>Заявка на вывод создана!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔖 ID: <code>#{wid}</code>\n"
        f"💵 Сумма: <code>{fmt(amount)}</code>\n"
        f"💳 Карта: <code>{h(card_number)}</code>\n"
        f"🏦 Банк: {h(bname)}\n\n"
        f"💰 Остаток баланса: <code>{fmt(u_after['balance'])}</code>\n\n"
        "⏳ Заявка передана в обработку. Сначала мы подтвердим вывод, потом отдельно сообщим, когда перевод уже отправлен на карту.",
        reply_markup=reply_main(actor_id),
        parse_mode=ParseMode.HTML,
    )
    w = withdrawal_get(wid)
    try:
        await bot.send_message(
            ADMIN_ID,
            withdrawal_admin_text(w),
            reply_markup=ikb_admin_withdrawal(wid),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        print(f"[ADMIN] {e}")

def profile_text(uid: int, username: str) -> str:
    user_upsert(uid, username or "")
    u     = user_get(uid)
    if not u:
        return (
            "👤 <b>Личный кабинет</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆔 <b>ID</b>: <code>{uid}</code>\n"
            "❌ Не удалось загрузить профиль. Попробуй ещё раз."
        )
    uname = display_username(u)
    return (
        "👤 <b>Личный кабинет</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 <b>ID</b>: <code>{uid}</code>\n"
        f"👤 <b>Юзернейм</b>: {h(uname)}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Баланс</b>: <code>{fmt(u['balance'])}</code>\n"
        f"🛍 <b>Покупок</b>: <code>{u['purchases']}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

# ═══════════════════════════════════════════════
#  ROUTER
# ═══════════════════════════════════════════════
router = Router()

# ───────────────────────────────────────────────
#  /start
# ───────────────────────────────────────────────
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_upsert(message.from_user.id, message.from_user.username or "")
    await message.answer(
        f"👋 Привет, <b>{h(message.from_user.first_name or 'друг')}</b>!\n\n"
        "🏪 <b>Добро пожаловать в наш магазин</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Здесь ты можешь приобрести цифровые подписки\n"
        "быстро, безопасно и по выгодным ценам.\n\n"
        "⬇️ <i>Используй кнопки ниже</i>",
        reply_markup=reply_main(message.from_user.id),
        parse_mode=ParseMode.HTML,
    )

# ───────────────────────────────────────────────
#  REPLY KEYBOARD HANDLERS
# ───────────────────────────────────────────────
@router.message(F.text == "🛒 Купить")
async def rpl_buy(message: Message, state: FSMContext):
    if await state.get_state():
        await state.clear()
    await message.answer(
        "🛒 <b>Каталог товаров</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Выбери категорию 👇",
        reply_markup=ikb_catalogue(),
        parse_mode=ParseMode.HTML,
    )

@router.message(F.text == "👤 Профиль")
async def rpl_profile(message: Message, state: FSMContext):
    if await state.get_state():
        await state.clear()
    await message.answer(
        profile_text(message.from_user.id, message.from_user.username or ""),
        reply_markup=ikb_profile(),
        parse_mode=ParseMode.HTML,
    )

@router.message(F.text == "💰 Пополнить баланс")
async def rpl_topup(message: Message, state: FSMContext):
    if await state.get_state():
        await state.clear()
    await _show_topup_menu(message)

@router.message(F.text == "❤️ Помощь")
async def rpl_help(message: Message, state: FSMContext):
    if await state.get_state():
        await state.clear()
    support = setting_get("support_user", "@support_manager")
    await message.answer(
        "❤️ <b>Помощь & Поддержка</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "По всем вопросам обращайся к нашему\n"
        "менеджеру — он на связи 24/7\n\n"
        f"👨‍💼 <b>Поддержка</b>: {h(support)}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⏱ <i>Среднее время ответа: до 15 минут</i>",
        parse_mode=ParseMode.HTML,
    )

@router.message(F.text == "⚙️ Админ-панель")
async def rpl_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    if await state.get_state():
        await state.clear()
    await message.answer(
        "⚙️ <b>Админ-панель</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Выбери раздел 👇",
        reply_markup=ikb_admin_main(),
        parse_mode=ParseMode.HTML,
    )

# ───────────────────────────────────────────────
#  CATALOGUE
# ───────────────────────────────────────────────
@router.callback_query(F.data == "buy")
async def cb_buy(call: CallbackQuery):
    await replace_callback_text(
        call,
        "🛒 <b>Каталог товаров</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Выбери категорию 👇",
        reply_markup=ikb_catalogue(),
    )
    await call.answer()

@router.callback_query(F.data.startswith("cat_"))
async def cb_category(call: CallbackQuery):
    category = call.data[4:]
    ps       = [p for p in products_all() if p["category"] == category and product_is_visible(p)]
    if not ps:
        await call.answer("Товары не найдены.", show_alert=True)
        return
    lead = next((item for item in ps if product_is_active(item)), ps[0])
    await replace_callback_text(
        call,
        f"{h(lead['emoji'])} <b>{h(lead['name'])}</b>\n\nВыбери вариант 👇",
        reply_markup=ikb_cat_products(category),
    )
    await call.answer()

@router.callback_query(F.data.startswith("product_"))
async def cb_product(call: CallbackQuery):
    pid = call.data[8:]
    p   = product_get(pid)
    if not p:
        await call.answer("Товар не найден.", show_alert=True)
        return
    if not product_is_visible(p):
        await call.answer("Этот товар скрыт.", show_alert=True)
        return
    user_upsert(call.from_user.id, call.from_user.username or "")
    u = user_get(call.from_user.id)
    await _show_product_card(call, p, u["balance"] if u else 0)

@router.callback_query(F.data.startswith("pqty_"))
async def cb_product_quantity(call: CallbackQuery):
    payload = call.data[5:]
    try:
        qty_raw, pid = payload.split("_", 1)
        qty = max(1, int(qty_raw))
    except (ValueError, TypeError):
        await call.answer("Ошибка количества.", show_alert=True)
        return
    p = product_get(pid)
    if not p or not product_is_visible(p):
        await call.answer("Товар не найден.", show_alert=True)
        return
    allowed = product_selectable_quantities(p)
    if qty not in allowed:
        await call.answer("Такое количество сейчас недоступно.", show_alert=True)
        return
    user_upsert(call.from_user.id, call.from_user.username or "")
    u = user_get(call.from_user.id)
    await _show_product_card(call, p, u["balance"] if u else 0, selected_qty=qty)

async def _show_product_card(call: CallbackQuery, p: dict, user_balance: float, selected_qty: int = 1):
    if selected_qty not in product_selectable_quantities(p):
        selected_qty = 1
    text = product_card_text(p, user_balance)
    total = round(float(p["price"]) * selected_qty, 2)
    text += (
        "\n\n🛒 <b>Текущий выбор</b>\n"
        f"🧮 <b>Количество</b>: <code>{selected_qty}</code> шт.\n"
        f"💲 <b>Итого</b>: <code>{fmt(total, h(p['currency']))}</code>"
    )
    kb   = ikb_product_card(p["id"], user_balance, selected_qty=selected_qty)
    if p.get("photo_id") and getattr(call.message, "photo", None):
        try:
            await call.message.edit_caption(caption=text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except Exception:
            await call.message.answer_photo(photo=p["photo_id"], caption=text,
                                            reply_markup=kb, parse_mode=ParseMode.HTML)
    elif p.get("photo_id"):
        try:
            await call.message.delete()
        except Exception:
            pass
        await call.message.answer_photo(photo=p["photo_id"], caption=text,
                                        reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except Exception:
            await call.message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await call.answer()

# ───────────────────────────────────────────────
#  PROFILE inline
# ───────────────────────────────────────────────
@router.callback_query(F.data == "my_history")
async def cb_my_history(call: CallbackQuery):
    uid    = call.from_user.id
    orders = orders_by_user(uid)
    topups = topups_by_user(uid)
    wds    = withdrawals_by_user(uid)
    await replace_callback_text(
        call,
        history_overview_text("📋 <b>Твоя история</b>", orders, topups, wds),
        reply_markup=ikb_user_history_list(orders, topups, wds),
    )
    await call.answer()

@router.callback_query(F.data.startswith("histo_"))
async def cb_history_order_detail(call: CallbackQuery):
    oid = call.data[6:]
    order = order_get(oid)
    if not order or order["user_id"] != call.from_user.id:
        await call.answer("Заказ не найден.", show_alert=True)
        return
    await call.message.edit_text(
        order_history_detail_text(order),
        reply_markup=ikb_tx_detail("my_history", f"histor_{oid}" if order.get("receipt_file_id") else None),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("histt_"))
async def cb_history_topup_detail(call: CallbackQuery):
    tid = call.data[6:]
    topup = topup_get(tid)
    if not topup or topup["user_id"] != call.from_user.id:
        await call.answer("Пополнение не найдено.", show_alert=True)
        return
    await call.message.edit_text(
        topup_history_detail_text(topup),
        reply_markup=ikb_tx_detail("my_history", f"histtr_{tid}" if topup.get("receipt_file_id") else None),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("histw_"))
async def cb_history_withdrawal_detail(call: CallbackQuery):
    wid = call.data[6:]
    withdrawal = withdrawal_get(wid)
    if not withdrawal or withdrawal["user_id"] != call.from_user.id:
        await call.answer("Вывод не найден.", show_alert=True)
        return
    await call.message.edit_text(
        withdrawal_history_detail_text(withdrawal),
        reply_markup=ikb_tx_detail("my_history"),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("histor_"))
async def cb_history_order_receipt(call: CallbackQuery):
    oid = call.data[7:]
    order = order_get(oid)
    if not order or order["user_id"] != call.from_user.id:
        await call.answer("Заказ не найден.", show_alert=True)
        return
    receipt = order.get("receipt_file_id")
    if not receipt:
        await call.answer("Чек не прикреплён.", show_alert=True)
        return
    await send_receipt_preview(call, receipt, "Чек по заказу", oid)

@router.callback_query(F.data.startswith("histtr_"))
async def cb_history_topup_receipt(call: CallbackQuery):
    tid = call.data[7:]
    topup = topup_get(tid)
    if not topup or topup["user_id"] != call.from_user.id:
        await call.answer("Пополнение не найдено.", show_alert=True)
        return
    receipt = topup.get("receipt_file_id")
    if not receipt:
        await call.answer("Чек не прикреплён.", show_alert=True)
        return
    await send_receipt_preview(call, receipt, "Чек по пополнению", tid)

@router.callback_query(F.data == "profile_back")
async def cb_profile_back(call: CallbackQuery):
    await replace_callback_text(
        call,
        profile_text(call.from_user.id, call.from_user.username or ""),
        reply_markup=ikb_profile(),
    )
    await call.answer()

# ───────────────────────────────────────────────
#  BUY — card flow
# ───────────────────────────────────────────────
@router.callback_query(F.data.startswith("buy_card_"))
async def cb_buy_card(call: CallbackQuery, state: FSMContext):
    if await state.get_state():
        await call.answer("У тебя есть незавершённый процесс.", show_alert=True)
        return
    try:
        qty_raw, pid = call.data[9:].split("_", 1)
        qty = max(1, int(qty_raw))
    except (TypeError, ValueError):
        pid = call.data[9:]
        qty = 1
    p = product_get(pid)
    if not p:
        await call.answer("Товар не найден.", show_alert=True)
        return
    if not product_is_active(p):
        await call.answer("Этот товар пока недоступен.", show_alert=True)
        return
    if not product_is_buyable(p, qty):
        await call.answer("Такого количества нет в наличии.", show_alert=True)
        return
    total = round(float(p["price"]) * qty, 2)
    await state.update_data(product_id=pid, product_qty=qty, pay_method="card")
    await state.set_state(OrderFSM.waiting_email)
    await call.message.answer(
        "📧 <b>Введи свой Gmail</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 {h(p['emoji'])} <b>{h(p['name'])}</b>\n"
        f"🧮 Количество: <code>{qty}</code> шт.\n"
        f"💲 Итого: <code>{fmt(total, h(p['currency']))}</code>\n\n"
        "Укажи Gmail для активации подписки ✨\n\n"
        "<i>Пример: yourname@gmail.com</i>",
        reply_markup=reply_cancel(),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("buy_balance_"))
async def cb_buy_balance(call: CallbackQuery, state: FSMContext):
    if await state.get_state():
        await call.answer("У тебя есть незавершённый процесс.", show_alert=True)
        return
    try:
        qty_raw, pid = call.data[12:].split("_", 1)
        qty = max(1, int(qty_raw))
    except (TypeError, ValueError):
        pid = call.data[12:]
        qty = 1
    p   = product_get(pid)
    if not p:
        await call.answer("Товар не найден.", show_alert=True)
        return
    if not product_is_active(p):
        await call.answer("Этот товар пока недоступен.", show_alert=True)
        return
    if not product_is_buyable(p, qty):
        await call.answer("Такого количества нет в наличии.", show_alert=True)
        return
    user_upsert(call.from_user.id, call.from_user.username or "")
    u = user_get(call.from_user.id)
    if not u:
        await call.answer("Профиль не найден. Нажми /start.", show_alert=True)
        return
    total = round(float(p["price"]) * qty, 2)
    if u["balance"] < total:
        await call.answer("❌ Недостаточно средств!", show_alert=True)
        return
    await state.update_data(product_id=pid, product_qty=qty, pay_method="balance")
    await state.set_state(OrderFSM.waiting_email)
    await call.message.answer(
        "📧 <b>Введи свой Gmail</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 {h(p['emoji'])} <b>{h(p['name'])}</b>\n"
        f"🧮 Количество: <code>{qty}</code> шт.\n"
        f"💲 Итого: <code>{fmt(total, h(p['currency']))}</code>\n\n"
        "Укажи Gmail для активации подписки ✨\n\n"
        "<i>Пример: yourname@gmail.com</i>",
        reply_markup=reply_cancel(),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.message(OrderFSM.waiting_email)
async def fsm_order_email(message: Message, state: FSMContext):
    email = (message.text or "").strip()
    if not re.match(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", email):
        await message.answer(
            "❌ <b>Некорректный email</b>\n\n"
            "Введи действующий Gmail.\n<i>Пример: yourname@gmail.com</i>",
            parse_mode=ParseMode.HTML,
        )
        return
    data       = await state.get_data()
    pid        = data.get("product_id")
    qty        = max(1, int(data.get("product_qty", 1) or 1))
    pay_method = data.get("pay_method", "card")
    if not pid or pay_method not in {"card", "balance"}:
        await state.clear()
        await message.answer("❌ Сессия устарела. Начни заново.", reply_markup=reply_main(message.from_user.id))
        return
    p          = product_get(pid)
    if not p or not product_is_active(p):
        await state.clear()
        await message.answer("❌ Товар больше недоступен. Выбери другой.", reply_markup=reply_main(message.from_user.id))
        return
    if not product_is_buyable(p, qty):
        await state.clear()
        await message.answer("❌ Такого количества сейчас нет в наличии. Оформи заказ заново.", reply_markup=reply_main(message.from_user.id))
        return
    oid        = new_id()
    await state.update_data(email=email, order_id=oid)

    if pay_method == "balance":
        user_upsert(message.from_user.id, message.from_user.username or "")
        u = user_get(message.from_user.id)
        if not u:
            await state.clear()
            await message.answer("❌ Профиль не найден. Нажми /start.", reply_markup=reply_main(message.from_user.id))
            return
        total_price = round(float(p["price"]) * qty, 2)
        if u["balance"] < total_price:
            await message.answer("❌ <b>Недостаточно средств!</b>",
                                 reply_markup=reply_main(message.from_user.id),
                                 parse_mode=ParseMode.HTML)
            await state.clear()
            return
        if not product_take_stock(pid, qty):
            await state.clear()
            await message.answer("❌ Такого количества уже нет в наличии.", reply_markup=reply_main(message.from_user.id))
            return
        user_sub_balance(message.from_user.id, total_price)
        order_create(oid, message.from_user.id, username_value(u), pid, email, "balance",
                     paid_by_balance=1, quantity=qty)
        order_set(oid, status="receipt_sent")
        await state.clear()
        await message.answer(
            "✅ <b>Заказ оплачен с баланса!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🔖 Заказ: <code>#{oid}</code>\n"
            f"📦 {h(p['emoji'])} <b>{h(p['name'])}</b>\n"
            f"🧮 <code>{qty}</code> шт.\n"
            f"📧 <code>{h(email)}</code>\n\n"
            "⏳ Менеджер обработает в течение <b>15 минут</b>.",
            reply_markup=reply_main(message.from_user.id),
            parse_mode=ParseMode.HTML,
        )
        order = order_get(oid)
        try:
            await message.bot.send_message(
                ADMIN_ID, order_admin_text(order),
                reply_markup=ikb_admin_order(oid),
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            print(f"[ADMIN] {e}")
        return

    await state.set_state(OrderFSM.choosing_bank)
    await message.answer(
        f"✅ Gmail: <code>{email}</code>\n"
        f"🧮 Количество: <code>{qty}</code> шт.\n\n"
        "💳 <b>Выбери банк для оплаты</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 <b>Сумма</b>: <code>{fmt(round(float(p['price']) * qty, 2), p['currency'])}</code>",
        reply_markup=ikb_choose_bank_order(oid),
        parse_mode=ParseMode.HTML,
    )

@router.callback_query(F.data.startswith("obank_"))
async def cb_order_bank(call: CallbackQuery, state: FSMContext):
    # obank_{privat|mono}_{oid}
    parts    = call.data.split("_", 2)
    if len(parts) != 3:
        await call.answer("Ошибка данных.", show_alert=True)
        return
    bank_key = parts[1]
    oid      = parts[2]
    if await state.get_state() != OrderFSM.choosing_bank:
        await call.answer("Сессия устарела. Начни заново.", show_alert=True)
        return
    if bank_key not in ORDER_BANK_NAMES:
        await call.answer("Неизвестный банк.", show_alert=True)
        return
    data  = await state.get_data()
    pid   = data.get("product_id")
    email = data.get("email")
    qty   = max(1, int(data.get("product_qty", 1) or 1))
    order_id = data.get("order_id")
    if not pid or not email or order_id != oid:
        await state.clear()
        await call.answer("Сессия устарела. Начни заново.", show_alert=True)
        return
    p     = product_get(pid)
    if not p or not product_is_active(p):
        await state.clear()
        await call.answer("Товар больше недоступен.", show_alert=True)
        return
    if not product_take_stock(pid, qty):
        await state.clear()
        await call.answer("Такого количества больше нет в наличии.", show_alert=True)
        return
    user_upsert(call.from_user.id, call.from_user.username or "")
    u    = user_get(call.from_user.id)
    card = setting_get("privat_card" if bank_key == "privat" else "mono_card")
    bname = ORDER_BANK_NAMES[bank_key]
    total = round(float(p["price"]) * qty, 2)
    order_create(oid, call.from_user.id, username_value(u), pid, email, bank_key, quantity=qty)
    await state.update_data(bank=bank_key)
    await state.set_state(OrderFSM.waiting_receipt)
    await call.message.edit_text(
        "💳 <b>Оплата заказа</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔖 <b>Заказ</b>: <code>#{oid}</code>\n"
        f"📦 <b>Товар</b>: {h(p['emoji'])} {h(p['name'])}\n\n"
        f"🧮 <b>Количество</b>: <code>{qty}</code> шт.\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"{bname}\n"
        f"💳 <b>Карта</b>:\n<code>{h(card)}</code>\n\n"
        f"💰 <b>Сумма</b>: <code>{fmt(total, p['currency'])}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "📌 <i>Переведи сумму и нажми кнопку ниже.</i>",
        reply_markup=ikb_payment(oid),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("opaid_"))
async def cb_order_paid(call: CallbackQuery, state: FSMContext):
    oid   = call.data[6:]
    order = order_get(oid)
    if not order:
        await call.answer("Заказ не найден.", show_alert=True)
        return
    if order["user_id"] != call.from_user.id:
        await call.answer("Это не твой заказ.", show_alert=True)
        return
    if order["status"] != "pending_payment":
        await call.answer("Этот заказ уже обработан.", show_alert=True)
        return
    await state.set_state(OrderFSM.waiting_receipt)
    await state.update_data(order_id=oid)
    await call.message.edit_text(
        "📸 <b>Отправь скриншот оплаты</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔖 Заказ: <code>#{oid}</code>\n\n"
        "Сделай скриншот и отправь <b>следующим сообщением</b>.\n\n"
        "⬇️ <i>Ожидаю фото...</i>",
        parse_mode=ParseMode.HTML,
    )
    await call.message.answer("❌ Нажми кнопку ниже, если хочешь отменить заказ.", reply_markup=reply_cancel())
    await call.answer()

@router.message(OrderFSM.waiting_receipt, F.photo)
async def fsm_order_receipt(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    oid  = data.get("order_id")
    order = order_get(oid) if oid else None
    if not oid or not order:
        await message.answer("❌ Сессия устарела. /start")
        await state.clear()
        return
    if order["user_id"] != message.from_user.id or order["status"] != "pending_payment":
        await state.clear()
        await message.answer("❌ Этот заказ уже нельзя оплатить повторно.", reply_markup=reply_main(message.from_user.id))
        return
    file_id = message.photo[-1].file_id
    order_set(oid, receipt_file_id=file_id, status="receipt_sent")
    order = order_get(oid)
    await state.clear()
    await message.answer(
        "⏳ <b>Чек получен!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔖 Заказ <code>#{oid}</code> передан менеджеру.\n\n"
        "✅ Проверка занимает до <b>15 минут</b>.",
        reply_markup=reply_main(message.from_user.id),
        parse_mode=ParseMode.HTML,
    )
    try:
        await bot.send_photo(ADMIN_ID, photo=file_id, caption=order_admin_text(order),
                             reply_markup=ikb_admin_order(oid), parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[ADMIN] {e}")

@router.message(OrderFSM.waiting_receipt)
async def fsm_order_receipt_wrong(message: Message):
    await message.answer("📸 Нужно отправить <b>фото</b> (скриншот оплаты).",
                         reply_markup=reply_cancel(),
                         parse_mode=ParseMode.HTML)

@router.callback_query(F.data.startswith("ocancel_"))
async def cb_order_cancel(call: CallbackQuery, state: FSMContext):
    oid   = call.data[8:]
    order = order_get(oid)
    if order and order["user_id"] != call.from_user.id:
        await call.answer("Это не твой заказ.", show_alert=True)
        return
    if order and order["status"] in ("pending_payment", "choosing_bank"):
        product_restore_stock(order["product_id"], order.get("product_quantity", 1))
        order_set(oid, status="cancelled")
    await state.clear()
    await call.message.edit_text("❌ <b>Заказ отменён</b>\n\nМожешь оформить заново.",
                                 parse_mode=ParseMode.HTML)
    await call.message.answer("👇", reply_markup=reply_main(call.from_user.id))
    await call.answer("Отменён.")

# ───────────────────────────────────────────────
#  TOPUP FLOW
# ───────────────────────────────────────────────
async def _show_topup_menu(target):
    text = ("💰 <b>Пополнение баланса</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Выбери сумму или введи свою 👇\n\n"
            "<i>Минимум: 50 ₴</i>")
    kb = ikb_topup_amounts()
    if isinstance(target, Message):
        await target.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        try:
            await target.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except Exception:
            await target.message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "topup_start")
async def cb_topup_start(call: CallbackQuery, state: FSMContext):
    if await state.get_state():
        await call.answer("Есть незавершённый процесс.", show_alert=True)
        return
    await _show_topup_menu(call)
    await call.answer()

@router.callback_query(F.data.startswith("tamount_"))
async def cb_topup_amount(call: CallbackQuery, state: FSMContext):
    if await state.get_state():
        await call.answer("Есть незавершённый процесс.", show_alert=True)
        return
    try:
        amount = float(call.data[8:])
    except ValueError:
        await call.answer("Некорректная сумма.", show_alert=True)
        return
    if amount <= 0:
        await call.answer("Некорректная сумма.", show_alert=True)
        return
    tid    = new_id()
    await state.update_data(topup_amount=amount, topup_id=tid)
    await state.set_state(TopUpFSM.choosing_bank)
    await call.message.edit_text(
        "💳 <b>Выбери банк</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💵 <b>Сумма</b>: <code>{fmt(amount)}</code>\n\n"
        "С какого банка переводишь? 👇",
        reply_markup=ikb_choose_bank_topup(tid),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data == "tcustom")
async def cb_topup_custom(call: CallbackQuery, state: FSMContext):
    if await state.get_state():
        await call.answer("Есть незавершённый процесс.", show_alert=True)
        return
    await state.set_state(TopUpFSM.choosing_amount)
    await call.message.edit_text(
        "✏️ <b>Введи сумму пополнения</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "<i>Минимум 50 ₴ · Максимум 10 000 ₴</i>",
        parse_mode=ParseMode.HTML,
    )
    await call.message.answer("❌ Нажми кнопку ниже, если хочешь отменить пополнение.", reply_markup=reply_cancel())
    await call.answer()

@router.message(TopUpFSM.choosing_amount)
async def fsm_topup_custom_amount(message: Message, state: FSMContext):
    try:
        amount = float((message.text or "").replace(",", ".").strip())
    except ValueError:
        await message.answer("❌ Введи число. Пример: <code>200</code>", parse_mode=ParseMode.HTML)
        return
    if amount < 50:
        await message.answer("❌ Минимум <b>50 ₴</b>.", parse_mode=ParseMode.HTML)
        return
    if amount > 10000:
        await message.answer("❌ Максимум <b>10 000 ₴</b>.", parse_mode=ParseMode.HTML)
        return
    tid = new_id()
    await state.update_data(topup_amount=amount, topup_id=tid)
    await state.set_state(TopUpFSM.choosing_bank)
    await message.answer(
        "💳 <b>Выбери банк</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💵 <b>Сумма</b>: <code>{fmt(amount)}</code>\n\n"
        "С какого банка переводишь? 👇",
        reply_markup=ikb_choose_bank_topup(tid),
        parse_mode=ParseMode.HTML,
    )

@router.callback_query(F.data.startswith("tbank_"))
async def cb_topup_bank(call: CallbackQuery, state: FSMContext):
    parts    = call.data.split("_", 2)
    if len(parts) != 3:
        await call.answer("Ошибка данных.", show_alert=True)
        return
    bank_key = parts[1]
    tid      = parts[2]
    if await state.get_state() != TopUpFSM.choosing_bank:
        await call.answer("Сессия устарела.", show_alert=True)
        return
    if bank_key not in TOPUP_BANK_NAMES:
        await call.answer("Неизвестный банк.", show_alert=True)
        return
    data   = await state.get_data()
    amount = data.get("topup_amount")
    topup_id = data.get("topup_id")
    if amount is None or topup_id != tid:
        await state.clear()
        await call.answer("Сессия устарела.", show_alert=True)
        return
    card   = setting_get("topup_privat_card" if bank_key == "privat" else "topup_mono_card")
    bname  = TOPUP_BANK_NAMES[bank_key]
    user_upsert(call.from_user.id, call.from_user.username or "")
    u = user_get(call.from_user.id)
    topup_create(tid, call.from_user.id, username_value(u), amount, bank_key)
    await state.update_data(topup_bank=bank_key)
    await state.set_state(TopUpFSM.waiting_receipt)
    await call.message.edit_text(
        "💳 <b>Оплата пополнения</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔖 <b>ID</b>: <code>#{tid}</code>\n\n"
        f"{bname}\n"
        f"💳 <b>Карта</b>:\n<code>{h(card)}</code>\n\n"
        f"💵 <b>Сумма</b>: <code>{fmt(amount)}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "📌 <i>Переведи и нажми кнопку ниже.</i>",
        reply_markup=ikb_topup_payment(tid),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("tpaid_"))
async def cb_topup_paid(call: CallbackQuery, state: FSMContext):
    tid   = call.data[6:]
    topup = topup_get(tid)
    if not topup:
        await call.answer("Запрос не найден.", show_alert=True)
        return
    if topup["user_id"] != call.from_user.id:
        await call.answer("Это не твой запрос.", show_alert=True)
        return
    if topup["status"] != "pending":
        await call.answer("Уже обработан.", show_alert=True)
        return
    await state.set_state(TopUpFSM.waiting_receipt)
    await state.update_data(topup_id=tid)
    await call.message.edit_text(
        "📸 <b>Отправь скриншот оплаты</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔖 <code>#{tid}</code>\n\n"
        "Отправь скриншот <b>следующим сообщением</b>.",
        parse_mode=ParseMode.HTML,
    )
    await call.message.answer("❌ Нажми кнопку ниже, если хочешь отменить пополнение.", reply_markup=reply_cancel())
    await call.answer()

@router.message(TopUpFSM.waiting_receipt, F.photo)
async def fsm_topup_receipt(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    tid  = data.get("topup_id")
    topup = topup_get(tid) if tid else None
    if not tid or not topup:
        await message.answer("❌ Сессия устарела. /start")
        await state.clear()
        return
    if topup["user_id"] != message.from_user.id or topup["status"] != "pending":
        await state.clear()
        await message.answer("❌ Этот запрос уже нельзя изменить.", reply_markup=reply_main(message.from_user.id))
        return
    file_id = message.photo[-1].file_id
    topup_set(tid, receipt_file_id=file_id, status="receipt_sent")
    topup = topup_get(tid)
    await state.clear()
    await message.answer(
        "⏳ <b>Чек получен!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔖 <code>#{tid}</code>\n\n"
        "💰 Баланс пополним в течение <b>15 минут</b>.",
        reply_markup=reply_main(message.from_user.id),
        parse_mode=ParseMode.HTML,
    )
    try:
        await bot.send_photo(ADMIN_ID, photo=file_id, caption=topup_admin_text(topup),
                             reply_markup=ikb_admin_topup(tid), parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[ADMIN] {e}")

@router.message(TopUpFSM.waiting_receipt)
async def fsm_topup_receipt_wrong(message: Message):
    await message.answer("📸 Нужно отправить <b>фото</b> (скриншот).",
                         reply_markup=reply_cancel(),
                         parse_mode=ParseMode.HTML)

@router.callback_query(F.data.startswith("tcancel_"))
async def cb_topup_cancel(call: CallbackQuery, state: FSMContext):
    tid   = call.data[8:]
    topup = topup_get(tid)
    if topup and topup["user_id"] != call.from_user.id:
        await call.answer("Это не твой запрос.", show_alert=True)
        return
    if topup and topup["status"] == "pending":
        topup_set(tid, status="cancelled")
    await state.clear()
    await call.message.edit_text("❌ <b>Пополнение отменено</b>", parse_mode=ParseMode.HTML)
    await call.message.answer("👇", reply_markup=reply_main(call.from_user.id))
    await call.answer()

# ───────────────────────────────────────────────
#  WITHDRAWAL FLOW
# ───────────────────────────────────────────────
@router.callback_query(F.data == "withdraw_start")
async def cb_withdraw_start(call: CallbackQuery, state: FSMContext):
    if await state.get_state():
        await call.answer("Есть незавершённый процесс.", show_alert=True)
        return
    user_upsert(call.from_user.id, call.from_user.username or "")
    u = user_get(call.from_user.id)
    if not u:
        await call.answer("Профиль не найден. Нажми /start.", show_alert=True)
        return
    if u["balance"] < 1:
        await call.answer("❌ Минимальная сумма вывода: 1 ₴", show_alert=True)
        return
    await state.set_state(WithdrawFSM.choosing_bank)
    await call.message.answer(
        "💸 <b>Вывод средств</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 <b>Доступно</b>: <code>{fmt(u['balance'])}</code>\n\n"
        "Выбери банк получателя 👇",
        reply_markup=ikb_withdraw_bank(),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("wbank_"))
async def cb_withdraw_bank(call: CallbackQuery, state: FSMContext):
    if await state.get_state() != WithdrawFSM.choosing_bank:
        await call.answer("Сессия устарела.", show_alert=True)
        return
    bank_key = call.data[6:]
    if bank_key not in WITHDRAW_BANK_NAMES:
        await call.answer("Неизвестный банк.", show_alert=True)
        return
    bname = WITHDRAW_BANK_NAMES[bank_key]
    user_upsert(call.from_user.id, call.from_user.username or "")
    u = user_get(call.from_user.id)
    if not u:
        await state.clear()
        await call.answer("Профиль не найден. Нажми /start.", show_alert=True)
        return
    await state.update_data(withdraw_bank=bname)
    await state.set_state(WithdrawFSM.entering_amount)
    await call.message.edit_text(
        f"💸 <b>Вывод — {bname}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 <b>Доступно</b>: <code>{fmt(u['balance'])}</code>\n\n"
        "✏️ Введи сумму вывода:",
        parse_mode=ParseMode.HTML,
    )
    await call.message.answer("❌ Нажми кнопку ниже, если хочешь отменить вывод.", reply_markup=reply_cancel())
    await call.answer()

@router.message(WithdrawFSM.entering_amount)
async def fsm_withdraw_amount(message: Message, state: FSMContext):
    try:
        amount = float((message.text or "").replace(",", ".").strip())
    except ValueError:
        await message.answer("❌ Введи число. Пример: <code>200</code>", parse_mode=ParseMode.HTML)
        return
    user_upsert(message.from_user.id, message.from_user.username or "")
    u = user_get(message.from_user.id)
    if not u:
        await state.clear()
        await message.answer("❌ Профиль не найден. Нажми /start.", reply_markup=reply_main(message.from_user.id))
        return
    if amount < 1:
        await message.answer("❌ Минимум <b>1 ₴</b>.", parse_mode=ParseMode.HTML)
        return
    if amount > u["balance"]:
        await message.answer(
            f"❌ На балансе только <code>{fmt(u['balance'])}</code>\n"
            f"Введи сумму не больше <code>{fmt(u['balance'])}</code>.",
            parse_mode=ParseMode.HTML,
        )
        return
    await state.update_data(withdraw_amount=amount)
    await state.set_state(WithdrawFSM.entering_card)
    data  = await state.get_data()
    bname = data.get("withdraw_bank", "")
    await message.answer(
        f"💳 <b>Введи номер карты</b> ({bname})\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Введи номер карты куда перевести деньги:\n\n"
        "<i>Можно с пробелами или без них. Пример: 4149 1234 5678 9012</i>",
        reply_markup=reply_cancel(),
        parse_mode=ParseMode.HTML,
    )

@router.message(WithdrawFSM.entering_card)
async def fsm_withdraw_card(message: Message, state: FSMContext, bot: Bot):
    card = (message.text or "").strip()
    if re.search(r"[^\d\s-]", card):
        await message.answer(
            "❌ Некорректный номер карты.\n\n"
            "Используй только цифры, пробелы или дефисы.",
            parse_mode=ParseMode.HTML,
        )
        return
    digits = re.sub(r"\D", "", card)
    if not digits:
        await message.answer(
            "❌ Некорректный номер карты.\n\n"
            "Введи номер карты цифрами.\n<i>Пример: 4149 1234 5678 9012</i>",
            parse_mode=ParseMode.HTML,
        )
        return
    if len(digits) < 12:
        await message.answer(
            "❌ Номер карты слишком короткий.\n\n"
            "Проверь номер и введи его ещё раз.\n<i>Минимум 12 цифр.</i>",
            parse_mode=ParseMode.HTML,
        )
        return
    if len(digits) > 19:
        await message.answer(
            "❌ Номер карты слишком длинный.\n\n"
            "Проверь номер и введи его ещё раз.",
            parse_mode=ParseMode.HTML,
        )
        return

    card_fmt = format_card_number(digits)
    if re.fullmatch(r"\d+", card):
        await state.update_data(withdraw_card=card_fmt)
        await state.set_state(WithdrawFSM.confirming_card)
        await message.answer(
            "💳 <b>Проверь номер карты</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Ты ввёл карту без пробелов. Проверь, всё ли верно:\n\n"
            f"<code>{card_fmt}</code>\n\n"
            "<i>Если номер правильный, подтверди его кнопкой ниже.</i>",
            reply_markup=ikb_withdraw_card_confirm(),
            parse_mode=ParseMode.HTML,
        )
        return

    await finalize_withdrawal_request(
        message,
        state,
        bot,
        card_fmt,
        actor_id=message.from_user.id,
        actor_username=message.from_user.username,
    )

@router.callback_query(F.data == "wcard_confirm")
async def cb_withdraw_card_confirm(call: CallbackQuery, state: FSMContext, bot: Bot):
    if await state.get_state() != WithdrawFSM.confirming_card:
        await call.answer("Сессия устарела.", show_alert=True)
        return
    data = await state.get_data()
    card_number = data.get("withdraw_card")
    if not card_number:
        await state.clear()
        await call.answer("Сессия устарела.", show_alert=True)
        return
    await call.answer()
    await finalize_withdrawal_request(
        call.message,
        state,
        bot,
        card_number,
        actor_id=call.from_user.id,
        actor_username=call.from_user.username,
    )

@router.callback_query(F.data == "wcard_reenter")
async def cb_withdraw_card_reenter(call: CallbackQuery, state: FSMContext):
    if await state.get_state() != WithdrawFSM.confirming_card:
        await call.answer("Сессия устарела.", show_alert=True)
        return
    data = await state.get_data()
    bname = data.get("withdraw_bank", "")
    await state.update_data(withdraw_card=None)
    await state.set_state(WithdrawFSM.entering_card)
    await call.message.edit_text(
        f"💳 <b>Введи номер карты заново</b> ({bname})\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Проверь номер и отправь его ещё раз.\n\n"
        "<i>Пример: 4149 1234 5678 9012</i>",
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data == "wcancel")
async def cb_withdraw_cancel_start(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("❌ <b>Вывод отменён</b>", parse_mode=ParseMode.HTML)
    await call.message.answer("👇", reply_markup=reply_main(call.from_user.id))
    await call.answer()

# ═══════════════════════════════════════════════
#  ADMIN — ORDER callbacks
# ═══════════════════════════════════════════════
def _chk(call: CallbackQuery) -> bool:
    if not is_admin(call.from_user.id):
        asyncio.create_task(call.answer("⛔ Нет доступа.", show_alert=True))
        return False
    return True

@router.callback_query(F.data.startswith("aconfirm_"))
async def adm_order_confirm(call: CallbackQuery, bot: Bot):
    if not _chk(call): return
    oid   = call.data[9:]
    order = order_get(oid)
    if not order:
        await call.answer("Заказ не найден.", show_alert=True); return
    if order["status"] == "payment_confirmed":
        await call.answer("Уже подтверждено.", show_alert=True); return
    if order["status"] != "receipt_sent":
        await call.answer("Сейчас этот заказ нельзя подтверждать.", show_alert=True); return
    order_set(oid, status="payment_confirmed")
    try:
        await call.message.edit_reply_markup(reply_markup=ikb_admin_order_confirmed(oid))
    except Exception:
        pass
    try:
        await bot.send_message(order["user_id"],
            "✅ <b>Оплата подтверждена!</b>\n"
            f"🔖 Заказ <code>#{oid}</code>\n"
            f"📦 {h(order.get('product_emoji',''))} <b>{h(order.get('product_name','?'))}</b>\n"
            f"🧮 <code>{order.get('product_quantity', 1)}</code> шт.\n\n"
            f"Ожидай приглашение на <code>{h(order['email'])}</code> 📧",
            parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[NOTIFY] {e}")
    await call.answer("✅ Подтверждено.")

@router.callback_query(F.data.startswith("ainvite_"))
async def adm_order_invite(call: CallbackQuery, bot: Bot):
    if not _chk(call): return
    oid   = call.data[8:]
    order = order_get(oid)
    if not order:
        await call.answer("Заказ не найден.", show_alert=True); return
    if order["status"] == "invite_sent":
        await call.answer("Уже отправлено.", show_alert=True); return
    if order["status"] != "payment_confirmed":
        await call.answer("Сначала подтверди оплату.", show_alert=True); return
    order_set(oid, status="invite_sent")
    user_inc_purchases(order["user_id"], order.get("product_quantity", 1))
    try:
        await call.message.edit_reply_markup(reply_markup=ikb_admin_order_done(oid))
    except Exception:
        pass
    try:
        await bot.send_message(order["user_id"],
            "🎉 <b>Приглашение отправлено!</b>\n"
            f"📦 {h(order.get('product_emoji',''))} <b>{h(order.get('product_name','?'))}</b>\n"
            f"🧮 <code>{order.get('product_quantity', 1)}</code> шт.\n"
            f"🔖 <code>#{oid}</code>\n\n"
            f"📧 Отправлено на <code>{h(order['email'])}</code>\n\n"
            "📌 <i>Прими в течение 24 ч. Не пришло — проверь «Спам».</i>",
            reply_markup=IKB_ACCEPTED,
            parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[NOTIFY] {e}")
    await call.answer("📨 Уведомлён.")

@router.callback_query(F.data.startswith("areject_"))
async def adm_order_reject(call: CallbackQuery, bot: Bot):
    if not _chk(call): return
    oid   = call.data[8:]
    order = order_get(oid)
    if not order:
        await call.answer("Заказ не найден.", show_alert=True); return
    if order["status"] in {"invite_sent", "cancelled", "rejected"}:
        await call.answer("Этот заказ уже нельзя отклонить.", show_alert=True); return
    # если оплата с баланса — возвращаем деньги
    if order.get("paid_by_balance"):
        user_add_balance(order["user_id"], order.get("product_price", 0))
    product_restore_stock(order["product_id"], order.get("product_quantity", 1))
    order_set(oid, status="rejected")
    try:
        await call.message.edit_reply_markup(reply_markup=ikb([[("❌ Заказ отклонён", f"areject_{oid}")]]))
    except Exception:
        pass
    try:
        await bot.send_message(order["user_id"],
            "❌ <b>Оплата не подтверждена</b>\n"
            f"🔖 <code>#{oid}</code> · {h(order.get('product_emoji',''))} {h(order.get('product_name','?'))}\n"
            f"🧮 <code>{order.get('product_quantity', 1)}</code> шт.\n\n"
            f"Обратись в поддержку: {h(setting_get('support_user','@support_manager'))}",
            reply_markup=reply_main(order["user_id"]),
            parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[NOTIFY] {e}")
    await call.answer("❌ Отклонён.")

# ═══════════════════════════════════════════════
#  ADMIN — TOPUP callbacks
# ═══════════════════════════════════════════════
@router.callback_query(F.data.startswith("atconfirm_"))
async def adm_topup_confirm(call: CallbackQuery, bot: Bot):
    if not _chk(call): return
    tid   = call.data[10:]
    topup = topup_get(tid)
    if not topup:
        await call.answer("Не найден.", show_alert=True); return
    if topup["status"] == "confirmed":
        await call.answer("Уже подтверждено.", show_alert=True); return
    if topup["status"] != "receipt_sent":
        await call.answer("Сейчас этот запрос нельзя подтверждать.", show_alert=True); return
    topup_set(tid, status="confirmed")
    user_add_balance(topup["user_id"], topup["amount"])
    try:
        await call.message.edit_reply_markup(reply_markup=ikb_admin_topup_done(tid))
    except Exception:
        pass
    u = user_get(topup["user_id"])
    try:
        await bot.send_message(topup["user_id"],
            "💰 <b>Баланс пополнен!</b>\n\n"
            f"✅ Зачислено: <code>{fmt(topup['amount'])}</code>\n"
            f"💳 Текущий баланс: <code>{fmt(u['balance'] if u else 0)}</code>",
            reply_markup=reply_main(topup["user_id"]),
            parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[NOTIFY] {e}")
    await call.answer(f"✅ +{fmt(topup['amount'])}")

@router.callback_query(F.data.startswith("atreject_"))
async def adm_topup_reject(call: CallbackQuery, bot: Bot):
    if not _chk(call): return
    tid   = call.data[9:]
    topup = topup_get(tid)
    if not topup:
        await call.answer("Не найден.", show_alert=True); return
    if topup["status"] in {"confirmed", "cancelled", "rejected"}:
        await call.answer("Этот запрос уже нельзя отклонить.", show_alert=True); return
    topup_set(tid, status="rejected")
    try:
        await call.message.edit_reply_markup(reply_markup=ikb([[("❌ Отклонено", f"atreject_{tid}")]]))
    except Exception:
        pass
    try:
        await bot.send_message(topup["user_id"],
            "❌ <b>Пополнение не подтверждено</b>\n"
            f"🔖 <code>#{tid}</code> · <code>{fmt(topup['amount'])}</code>\n\n"
            f"Обратись: {h(setting_get('support_user','@support_manager'))}",
            reply_markup=reply_main(topup["user_id"]),
            parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[NOTIFY] {e}")
    await call.answer("❌ Отклонено.")

# ═══════════════════════════════════════════════
#  ADMIN — WITHDRAWAL callbacks
# ═══════════════════════════════════════════════
@router.callback_query(F.data.startswith("awconfirm_"))
async def adm_wd_confirm(call: CallbackQuery, bot: Bot):
    if not _chk(call): return
    wid = call.data[10:]
    w   = withdrawal_get(wid)
    if not w:
        await call.answer("Не найден.", show_alert=True); return
    if w["status"] == "confirmed":
        await call.answer("Уже подтверждено.", show_alert=True); return
    if w["status"] != "pending":
        await call.answer("Уже обработан.", show_alert=True); return
    withdrawal_set(wid, status="confirmed")
    try:
        await call.message.edit_reply_markup(reply_markup=ikb_admin_withdrawal_transfer(wid))
    except Exception:
        pass
    try:
        await bot.send_message(w["user_id"],
            "✅ <b>Вывод подтверждён!</b>\n\n"
            f"💵 Сумма: <code>{fmt(w['amount'])}</code>\n"
            f"💳 Карта: <code>{w['card_number']}</code>\n\n"
            "⏳ Заявка принята в обработку. Перевод скоро будет отправлен на указанную карту.",
            parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[NOTIFY] {e}")
    await call.answer("✅ Вывод подтверждён, клиент уведомлён.")

@router.callback_query(F.data.startswith("awsent_"))
async def adm_wd_sent(call: CallbackQuery, bot: Bot):
    if not _chk(call): return
    wid = call.data[7:]
    w   = withdrawal_get(wid)
    if not w:
        await call.answer("Не найден.", show_alert=True); return
    if w["status"] == "completed":
        await call.answer("Уже завершён.", show_alert=True); return
    if w["status"] not in ("confirmed", "transfer_sent"):
        await call.answer("Сначала подтверди вывод.", show_alert=True); return
    withdrawal_set(wid, status="completed")
    try:
        await call.message.edit_reply_markup(reply_markup=ikb_admin_withdrawal_done(wid))
    except Exception:
        pass
    try:
        await bot.send_message(w["user_id"],
            "💸 <b>Деньги отправлены!</b>\n\n"
            f"💵 Сумма: <code>{fmt(w['amount'])}</code>\n"
            f"💳 На карту: <code>{h(w['card_number'])}</code>\n\n"
            "✅ Если деньги не пришли в течение суток —\n"
            f"обратись в поддержку: {h(setting_get('support_user','@support_manager'))}",
            reply_markup=reply_main(w["user_id"]),
            parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[NOTIFY] {e}")
    await call.answer("💸 Перевод отмечен как отправленный.")

@router.callback_query(F.data.startswith("awreject_"))
async def adm_wd_reject(call: CallbackQuery, bot: Bot):
    if not _chk(call): return
    wid = call.data[9:]
    w   = withdrawal_get(wid)
    if not w:
        await call.answer("Не найден.", show_alert=True); return
    if w["status"] in {"completed", "rejected"}:
        await call.answer("Этот вывод уже нельзя отклонить.", show_alert=True); return
    withdrawal_set(wid, status="rejected")
    # Возвращаем деньги на баланс
    user_add_balance(w["user_id"], w["amount"])
    try:
        await call.message.edit_reply_markup(reply_markup=ikb([[("❌ Вывод отклонён", f"awreject_{wid}")]]))
    except Exception:
        pass
    u = user_get(w["user_id"])
    try:
        await bot.send_message(w["user_id"],
            "❌ <b>Вывод отклонён</b>\n\n"
            f"💵 Сумма <code>{fmt(w['amount'])}</code> возвращена на баланс.\n"
            f"💳 Текущий баланс: <code>{fmt(u['balance'] if u else 0)}</code>\n\n"
            f"Вопросы: {h(setting_get('support_user','@support_manager'))}",
            reply_markup=reply_main(w["user_id"]),
            parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[NOTIFY] {e}")
    await call.answer("❌ Отклонён, деньги возвращены.")

# ═══════════════════════════════════════════════
#  ADMIN — MESSAGING (написать клиенту)
# ═══════════════════════════════════════════════
@router.callback_query(F.data == "adm_broadcast")
async def adm_broadcast_prompt(call: CallbackQuery, state: FSMContext):
    if not _chk(call): return
    recipients = all_user_ids(exclude_admin=True)
    await state.clear()
    await state.update_data(
        broadcast_mode=True,
        msg_context="рассылка всем пользователям",
        msg_target_uid=None,
    )
    await state.set_state(AdminFSM.sending_message)
    await call.message.answer(
        "📢 <b>Рассылка всем пользователям</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Получателей в базе: <b>{len(recipients)}</b>\n\n"
        "Введи текст уведомления. Бот отправит его всем пользователям, которые уже запускали бота.\n\n"
        "<i>Для отмены напиши /cancel</i>",
        reply_markup=reply_cancel(),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("amsg_"))
async def adm_msg_prompt(call: CallbackQuery, state: FSMContext):
    if not _chk(call): return
    # amsg_order_{oid} / amsg_topup_{tid} / amsg_wd_{wid} / admmsg_{uid}
    parts = call.data[5:].split("_", 1)
    if len(parts) != 2:
        await call.answer("Ошибка данных.", show_alert=True); return
    kind  = parts[0]   # order / topup / wd / (uid for direct)

    if kind == "order":
        oid   = parts[1]
        order = order_get(oid)
        if not order:
            await call.answer("Не найден.", show_alert=True); return
        target_uid  = order["user_id"]
        context_str = f"заказ <code>#{oid}</code>"
    elif kind == "topup":
        tid   = parts[1]
        topup = topup_get(tid)
        if not topup:
            await call.answer("Не найден.", show_alert=True); return
        target_uid  = topup["user_id"]
        context_str = f"пополнение <code>#{tid}</code>"
    elif kind == "wd":
        wid = parts[1]
        w   = withdrawal_get(wid)
        if not w:
            await call.answer("Не найден.", show_alert=True); return
        target_uid  = w["user_id"]
        context_str = f"вывод <code>#{wid}</code>"
    else:
        # direct: admmsg_{uid} → amsg prefix not used, handled below
        await call.answer("Ошибка.", show_alert=True); return

    await state.clear()
    await state.update_data(
        broadcast_mode=False,
        msg_target_uid=target_uid,
        msg_context=context_str,
    )
    await state.set_state(AdminFSM.sending_message)
    await call.message.answer(
        f"📩 <b>Сообщение клиенту</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Контекст: {context_str}\n\n"
        "Введи текст сообщения. Оно будет отправлено клиенту от имени бота.\n\n"
        "<i>Для отмены напиши /cancel</i>",
        reply_markup=reply_cancel(),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("admmsg_"))
async def adm_direct_msg_prompt(call: CallbackQuery, state: FSMContext):
    if not _chk(call): return
    uid_raw = call.data[7:]
    if not uid_raw.isdigit():
        await call.answer("Ошибка данных.", show_alert=True); return
    uid = int(uid_raw)
    u   = user_get(uid)
    uname = display_username(u, fallback_id=uid)
    await state.clear()
    await state.update_data(
        broadcast_mode=False,
        msg_target_uid=uid,
        msg_context=f"пользователь {uname}",
    )
    await state.set_state(AdminFSM.sending_message)
    await call.message.answer(
        f"📩 <b>Сообщение клиенту</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Получатель: {uname}\n\n"
        "Введи текст сообщения:\n\n"
        "<i>Для отмены: /cancel</i>",
        reply_markup=reply_cancel(),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.message(AdminFSM.sending_message)
async def adm_send_message(message: Message, state: FSMContext, bot: Bot):
    if message.text and message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено.", reply_markup=reply_main(message.from_user.id))
        return
    data       = await state.get_data()
    broadcast_mode = bool(data.get("broadcast_mode"))
    target_uid = data.get("msg_target_uid")
    if not broadcast_mode and target_uid is None:
        await state.clear()
        await message.answer("❌ Сессия устарела. Начни заново.", reply_markup=reply_main(message.from_user.id))
        return
    context    = data.get("msg_context", "")
    text       = message.text or ""
    if not text.strip():
        await message.answer("❌ Сообщение не может быть пустым.")
        return
    if broadcast_mode:
        recipients = all_user_ids(exclude_admin=True)
        if not recipients:
            await state.clear()
            await message.answer(
                "❌ В базе пока нет пользователей для рассылки.",
                reply_markup=reply_main(message.from_user.id),
            )
            return
        sent = 0
        failed = 0
        for uid in recipients:
            try:
                await bot.send_message(
                    uid,
                    "📢 <b>Уведомление от магазина</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"{h(text)}",
                    reply_markup=reply_main(uid),
                    parse_mode=ParseMode.HTML,
                )
                sent += 1
            except Exception as e:
                failed += 1
                print(f"[BROADCAST:{uid}] {e}")
            await asyncio.sleep(0.05)
        await state.clear()
        await message.answer(
            "✅ <b>Рассылка завершена</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Доставлено: <b>{sent}</b>\n"
            f"Не доставлено: <b>{failed}</b>",
            reply_markup=reply_main(message.from_user.id),
            parse_mode=ParseMode.HTML,
        )
        return
    try:
        await bot.send_message(
            target_uid,
            "💬 <b>Сообщение от магазина</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{h(text)}",
            reply_markup=reply_main(target_uid),
            parse_mode=ParseMode.HTML,
        )
        await state.clear()
        await message.answer(
            f"✅ Сообщение отправлено!\n\nКонтекст: {h(context)}",
            reply_markup=reply_main(message.from_user.id),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка отправки: {e}")

# ═══════════════════════════════════════════════
#  ADMIN PANEL — PRODUCTS
# ═══════════════════════════════════════════════
@router.callback_query(F.data == "adm_main")
async def adm_main(call: CallbackQuery):
    if not _chk(call): return
    await call.message.edit_text(
        "⚙️ <b>Админ-панель</b>\n━━━━━━━━━━━━━━━━━━━━\n\nВыбери раздел 👇",
        reply_markup=ikb_admin_main(), parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data == "adm_products")
async def adm_products(call: CallbackQuery):
    if not _chk(call): return
    await call.message.edit_text(
        "📦 <b>Товары</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
        "✅ — показан · 🕒 — скоро · 🔒 — скрыт\n\n"
        "📂 Сверху категории, ниже отдельные товары без категории.",
        reply_markup=ikb_admin_products(), parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data == "adm_newcat")
async def adm_new_catalog_category(call: CallbackQuery, state: FSMContext):
    if not _chk(call): return
    await state.set_state(AdminFSM.new_catalog_category)
    await call.message.answer(
        "📂 <b>Новая категория</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
        "Введи название категории.\n\n"
        "Например: <code>YouTube Premium</code> или <code>Spotify</code>",
        reply_markup=reply_cancel(),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.message(AdminFSM.new_catalog_category)
async def adm_new_catalog_category_save(message: Message, state: FSMContext):
    if (message.text or "").strip() in {"/cancel", "❌ Отмена"}:
        await state.clear()
        await message.answer("❌ Создание категории отменено.", reply_markup=reply_main(message.from_user.id))
        return
    value = normalize_category_name(message.text or "")
    if not value:
        await message.answer("❌ Название категории не может быть пустым.", parse_mode=ParseMode.HTML)
        return
    if not CATEGORY_RE.fullmatch(value):
        await message.answer("❌ Категория может содержать латиницу, цифры, пробелы и _.", parse_mode=ParseMode.HTML)
        return
    created = category_create(value)
    await state.clear()
    if not created:
        await message.answer("❌ Не удалось создать категорию.", reply_markup=reply_main(message.from_user.id))
        return
    await message.answer(
        f"✅ Категория создана: <code>{h(value)}</code>",
        reply_markup=reply_main(message.from_user.id),
        parse_mode=ParseMode.HTML,
    )

@router.callback_query(F.data.startswith("admcat_"))
async def adm_category_view(call: CallbackQuery):
    if not _chk(call): return
    category = normalize_category_name(call.data[7:])
    products = [p for p in products_all() if normalize_category_name(p.get("category") or "") == category]
    info_text = "Выбери нужный товар 👇" if products else "Категория пока пустая. Можешь добавить в неё товары."
    await call.message.edit_text(
        "📂 <b>Категория товаров</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Название: <code>{h(category)}</code>\n"
        f"Товаров внутри: <code>{len(products)}</code>\n\n"
        f"{info_text}",
        reply_markup=ikb_admin_category(category),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("admprod_"))
async def adm_prod_view(call: CallbackQuery):
    if not _chk(call): return
    pid = call.data[8:]
    p   = product_get(pid)
    if not p:
        await call.answer("Не найден.", show_alert=True); return
    await call.message.edit_text(
        admin_product_text(p),
        reply_markup=ikb_admin_product(pid), parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("admstatus|"))
async def adm_set_status(call: CallbackQuery):
    if not _chk(call): return
    parts = call.data.split("|", 2)
    if len(parts) != 3:
        await call.answer("Ошибка данных.", show_alert=True); return
    pid, new_status = parts[1], parts[2]
    p = product_get(pid)
    if not p:
        await call.answer("Не найден.", show_alert=True); return
    if new_status not in PRODUCT_STATUS_LABELS:
        await call.answer("Неизвестный статус.", show_alert=True); return
    if product_status_value(p) == new_status:
        await call.answer(f"Уже: {PRODUCT_STATUS_LABELS[new_status]}", show_alert=True)
        return
    product_update(pid, product_status=new_status)
    updated = product_get(pid)
    await call.answer(PRODUCT_STATUS_LABELS[new_status], show_alert=True)
    if updated:
        await call.message.edit_text(
            admin_product_text(updated),
            reply_markup=ikb_admin_product(pid),
            parse_mode=ParseMode.HTML,
        )

@router.callback_query(F.data.startswith("admdelete_"))
async def adm_delete(call: CallbackQuery):
    if not _chk(call): return
    pid = call.data[10:]
    product_delete(pid)
    await call.answer("🗑 Удалён.", show_alert=True)
    await call.message.edit_text(
        "📦 <b>Товары</b>\n━━━━━━━━━━━━━━━━━━━━\n\nВыбери товар 👇",
        reply_markup=ikb_admin_products(), parse_mode=ParseMode.HTML,
    )

@router.callback_query(F.data.startswith("admrphoto_"))
async def adm_remove_photo(call: CallbackQuery):
    if not _chk(call): return
    pid = call.data[10:]
    product_update(pid, photo_id=None)
    await call.answer("🗑 Фото удалено.", show_alert=True)
    await call.message.edit_reply_markup(reply_markup=ikb_admin_product(pid))

# ── Edit field ─────────────────────────────────
@router.callback_query(F.data.startswith("admedit_"))
async def adm_edit_field(call: CallbackQuery, state: FSMContext):
    if not _chk(call): return
    # admedit_{pid}_{field}
    rest  = call.data[8:]
    parts = rest.rsplit("_", 1)
    if len(parts) != 2:
        await call.answer("Ошибка.", show_alert=True); return
    pid, field = parts
    if field not in {"photo", "price", "description", "name", "emoji", "badge", "category", "stock"}:
        await call.answer("Неизвестное поле.", show_alert=True); return
    await state.update_data(edit_pid=pid, edit_field=field)
    await state.set_state(AdminFSM.edit_field)
    hints = {
        "photo":       "🖼 Отправь новое фото товара:",
        "price":       "💲 Введи новую цену (число):",
        "description": "📋 Введи новое описание:",
        "name":        "✏️ Введи новое название:",
        "emoji":       "✏️ Введи новый эмодзи:",
        "badge":       "✏️ Введи бейдж или <code>-</code> чтобы убрать:",
        "category":    "✏️ Введи название категории или <code>-</code> для отдельного товара:",
        "stock":       "📦 Введи остаток товара. Для безлимита укажи 999999:",
    }
    await call.message.answer(
        hints.get(field, f"✏️ Введи новое значение для «{field}»:"),
        reply_markup=reply_cancel(),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.message(AdminFSM.edit_field, F.photo)
async def adm_edit_photo(message: Message, state: FSMContext):
    data  = await state.get_data()
    if data.get("edit_field") != "photo":
        await message.answer("Ожидается текст."); return
    pid = data.get("edit_pid")
    if not pid:
        await state.clear()
        await message.answer("❌ Сессия редактирования устарела.", reply_markup=reply_main(message.from_user.id))
        return
    file_id = message.photo[-1].file_id
    product_update(pid, photo_id=file_id)
    await state.clear()
    p = product_get(pid)
    if not p:
        await message.answer("❌ Товар не найден.", reply_markup=reply_main(message.from_user.id))
        return
    await message.answer(
        f"✅ Фото обновлено для <b>{h(p['name'])}</b>!",
        reply_markup=reply_main(message.from_user.id), parse_mode=ParseMode.HTML,
    )

@router.message(AdminFSM.edit_field)
async def adm_edit_text(message: Message, state: FSMContext):
    data  = await state.get_data()
    pid   = data.get("edit_pid")
    field = data.get("edit_field")
    if not pid or not field:
        await state.clear()
        await message.answer("❌ Сессия редактирования устарела.", reply_markup=reply_main(message.from_user.id))
        return
    value = (message.text or "").strip()
    if field == "price":
        try:
            value = float(value.replace(",", "."))
        except ValueError:
            await message.answer("❌ Введи число.", parse_mode=ParseMode.HTML); return
        if value <= 0:
            await message.answer("❌ Цена должна быть больше 0.", parse_mode=ParseMode.HTML); return
    elif field == "stock":
        try:
            value = int(value)
        except ValueError:
            await message.answer("❌ Введи целое число.", parse_mode=ParseMode.HTML); return
        if value < 0:
            await message.answer("❌ Остаток не может быть отрицательным.", parse_mode=ParseMode.HTML); return
    elif field in {"name", "description", "emoji"} and not value:
        await message.answer("❌ Значение не может быть пустым.", parse_mode=ParseMode.HTML); return
    elif field == "badge" and value == "-":
        value = ""
    elif field == "category":
        if value == "-":
            value = ""
        else:
            value = normalize_category_name(value)
        if value and not CATEGORY_RE.fullmatch(value):
            await message.answer("❌ Категория может содержать латиницу, цифры, пробелы и _.", parse_mode=ParseMode.HTML); return
    product_update(pid, **{field: value})
    await state.clear()
    p     = product_get(pid)
    if not p:
        await message.answer("❌ Товар не найден.", reply_markup=reply_main(message.from_user.id))
        return
    fname = FIELD_LABELS.get(field, field)
    if field == "price":
        disp = fmt(value)
    elif field == "stock":
        disp = product_stock_text({"stock": value})
    else:
        disp = h(str(value) or "—")
    await message.answer(
        f"✅ <b>{h(p['name'])}</b>\n{fname} → <code>{disp}</code>",
        reply_markup=reply_main(message.from_user.id), parse_mode=ParseMode.HTML,
    )

# ── Create new product ─────────────────────────
@router.callback_query(F.data == "adm_newprod")
async def adm_new_prod(call: CallbackQuery, state: FSMContext):
    if not _chk(call): return
    await state.set_state(AdminFSM.new_name)
    await call.message.answer(
        "➕ <b>Новый товар</b> (1/8)\n━━━━━━━━━━━━━━━━━━━━\n\n✏️ Введи <b>название</b>:",
        reply_markup=reply_cancel(), parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.message(AdminFSM.new_name)
async def adm_new_name(message: Message, state: FSMContext):
    value = (message.text or "").strip()
    if not value:
        await message.answer("❌ Название не может быть пустым.", parse_mode=ParseMode.HTML); return
    await state.update_data(n_name=value)
    await state.set_state(AdminFSM.new_emoji)
    await message.answer("(2/8) ✏️ Введи <b>эмодзи</b> (например: ❤️):", parse_mode=ParseMode.HTML)

@router.message(AdminFSM.new_emoji)
async def adm_new_emoji(message: Message, state: FSMContext):
    value = (message.text or "").strip()
    if not value:
        await message.answer("❌ Эмодзи не может быть пустым.", parse_mode=ParseMode.HTML); return
    await state.update_data(n_emoji=value)
    await state.set_state(AdminFSM.new_category)
    await message.answer("(3/8) ✏️ Введи <b>название категории</b> (например <code>YouTube Premium</code>) или <code>-</code> для отдельного товара:", parse_mode=ParseMode.HTML)

@router.message(AdminFSM.new_category)
async def adm_new_cat(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    value = "" if raw == "-" else normalize_category_name(raw)
    if value and not CATEGORY_RE.fullmatch(value):
        await message.answer("❌ Категория может содержать латиницу, цифры, пробелы и _. Для товара без категории напиши <code>-</code>.", parse_mode=ParseMode.HTML); return
    await state.update_data(n_cat=value)
    await state.set_state(AdminFSM.new_badge)
    await message.answer("(4/8) ✏️ Введи <b>бейдж</b> (напр. <code>DEFAULT</code>) или <code>-</code>:", parse_mode=ParseMode.HTML)

@router.message(AdminFSM.new_badge)
async def adm_new_badge(message: Message, state: FSMContext):
    v = (message.text or "").strip()
    await state.update_data(n_badge="" if v == "-" else v)
    await state.set_state(AdminFSM.new_description)
    await message.answer("(5/8) 📋 Введи <b>описание</b>:", parse_mode=ParseMode.HTML)

@router.message(AdminFSM.new_description)
async def adm_new_desc(message: Message, state: FSMContext):
    value = (message.text or "").strip()
    if not value:
        await message.answer("❌ Описание не может быть пустым.", parse_mode=ParseMode.HTML); return
    await state.update_data(n_desc=value)
    await state.set_state(AdminFSM.new_price)
    await message.answer("(6/8) 💲 Введи <b>цену</b> (число):", parse_mode=ParseMode.HTML)

@router.message(AdminFSM.new_price)
async def adm_new_price(message: Message, state: FSMContext):
    try:
        price = float((message.text or "").replace(",", ".").strip())
    except ValueError:
        await message.answer("❌ Введи число."); return
    if price <= 0:
        await message.answer("❌ Цена должна быть больше 0."); return
    await state.update_data(n_price=price)
    await state.set_state(AdminFSM.new_stock)
    await message.answer("(7/8) 📦 Введи <b>остаток</b>. Для безлимита укажи <code>999999</code>:", parse_mode=ParseMode.HTML)

@router.message(AdminFSM.new_stock)
async def adm_new_stock(message: Message, state: FSMContext):
    try:
        stock = int((message.text or "").strip())
    except ValueError:
        await message.answer("❌ Введи целое число."); return
    if stock < 0:
        await message.answer("❌ Остаток не может быть отрицательным."); return
    await state.update_data(n_stock=stock)
    await state.set_state(AdminFSM.new_photo)
    await message.answer("(8/8) 🖼 Отправь <b>фото</b> или напиши <code>-</code> чтобы пропустить:", parse_mode=ParseMode.HTML)

@router.message(AdminFSM.new_photo, F.photo)
async def adm_new_photo_img(message: Message, state: FSMContext):
    await _finish_product(message, state, message.photo[-1].file_id)

@router.message(AdminFSM.new_photo)
async def adm_new_photo_skip(message: Message, state: FSMContext):
    if (message.text or "").strip() != "-":
        await message.answer("❌ Отправь фото или напиши <code>-</code>, чтобы пропустить.", parse_mode=ParseMode.HTML)
        return
    await _finish_product(message, state, None)

async def _finish_product(message: Message, state: FSMContext, photo_id):
    data = await state.get_data()
    required_fields = ("n_name", "n_emoji", "n_cat", "n_desc", "n_price", "n_stock")
    if any(field not in data for field in required_fields):
        await state.clear()
        await message.answer("❌ Сессия создания товара устарела. Начни заново.", reply_markup=reply_main(message.from_user.id))
        return
    pid  = new_id().lower()
    p    = product_create(pid, data["n_name"], data["n_emoji"], data["n_cat"],
                          data.get("n_badge",""), data["n_desc"], data["n_price"], stock=data["n_stock"])
    if photo_id:
        product_update(pid, photo_id=photo_id)
        p = product_get(pid)
    await state.clear()
    badge = f" <code>[{h(p['badge'])}]</code>" if p.get("badge") else ""
    await message.answer(
        f"✅ <b>Товар создан!</b>\n\n{h(p['emoji'])} <b>{h(p['name'])}{badge}</b>\n"
        f"💲 {fmt(p['price'], p['currency'])}\n📦 {product_stock_text(p)} шт.\n🆔 <code>{p['id']}</code>",
        reply_markup=reply_main(message.from_user.id), parse_mode=ParseMode.HTML,
    )

# ═══════════════════════════════════════════════
#  ADMIN PANEL — USERS
# ═══════════════════════════════════════════════
@router.callback_query(F.data == "adm_users")
async def adm_users(call: CallbackQuery):
    if not _chk(call): return
    total = users_count()
    await call.message.edit_text(
        f"👥 <b>Пользователи</b>\n━━━━━━━━━━━━━━━━━━━━\n\nВсего: <b>{total}</b>\n\nВыбери 👇",
        reply_markup=ikb_admin_users(), parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("admup_"))
async def adm_users_page(call: CallbackQuery):
    if not _chk(call): return
    rest   = call.data[6:]
    parts  = rest.split("_", 1)
    if not parts[0].isdigit():
        await call.answer("Ошибка данных.", show_alert=True); return
    offset = int(parts[0])
    search = parts[1] if len(parts) > 1 else ""
    total  = users_count(search)
    await call.message.edit_text(
        f"👥 <b>Пользователи</b>" + (f" · <code>{h(search)}</code>" if search else "") +
        f"\n━━━━━━━━━━━━━━━━━━━━\nНайдено: <b>{total}</b>\n\nВыбери 👇",
        reply_markup=ikb_admin_users(offset=offset, search=search), parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data == "adm_usersearch")
async def adm_user_search(call: CallbackQuery, state: FSMContext):
    if not _chk(call): return
    await state.set_state(AdminFSM.user_search)
    await call.message.answer(
        "🔍 Введи <b>username</b> или <b>User ID</b>:",
        reply_markup=reply_cancel(), parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.message(AdminFSM.user_search)
async def adm_user_search_result(message: Message, state: FSMContext):
    search = (message.text or "").strip().lstrip("@")
    await state.clear()
    total  = users_count(search)
    await message.answer(
        f"👥 Результат: <code>{h(search)}</code>\nНайдено: <b>{total}</b>",
        reply_markup=ikb_admin_users(search=search), parse_mode=ParseMode.HTML,
    )

@router.callback_query(F.data.startswith("admusr_"))
async def adm_user_view(call: CallbackQuery):
    if not _chk(call): return
    uid_raw = call.data[7:]
    if not uid_raw.isdigit():
        await call.answer("Ошибка данных.", show_alert=True); return
    uid = int(uid_raw)
    u   = user_get(uid)
    if not u:
        await call.answer("Не найден.", show_alert=True); return
    uname  = h(display_username(u))
    ords   = orders_by_user(uid)
    tops   = topups_by_user(uid)
    wds    = withdrawals_by_user(uid)
    await call.message.edit_text(
        f"👤 <b>Пользователь</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"👤 Username: {uname}\n"
        f"📅 Регистрация: {u['created_at']}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Баланс: <code>{fmt(u['balance'])}</code>\n"
        f"🛍 Покупок: <code>{u['purchases']}</code>\n"
        f"📦 Заказов: <code>{len(ords)}</code>\n"
        f"💳 Пополнений: <code>{len(tops)}</code>\n"
        f"💸 Выводов: <code>{len(wds)}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━",
        reply_markup=ikb_admin_user(uid), parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("admhist_"))
async def adm_user_history(call: CallbackQuery):
    if not _chk(call): return
    uid_raw = call.data[8:]
    if not uid_raw.isdigit():
        await call.answer("Ошибка данных.", show_alert=True); return
    uid  = int(uid_raw)
    ords = orders_by_user(uid)
    tops = topups_by_user(uid)
    wds  = withdrawals_by_user(uid)
    await call.message.edit_text(
        history_overview_text(f"📋 <b>История</b> (<code>{uid}</code>)", ords, tops, wds),
        reply_markup=ikb_admin_history_list(uid, ords, tops, wds),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("ahisto_"))
async def adm_history_order_detail(call: CallbackQuery):
    if not _chk(call): return
    oid = call.data[7:]
    order = order_get(oid)
    if not order:
        await call.answer("Заказ не найден.", show_alert=True)
        return
    await call.message.edit_text(
        order_history_detail_text(order),
        reply_markup=ikb_tx_detail(
            f"admhist_{order['user_id']}",
            f"ahistor_{oid}" if order.get("receipt_file_id") else None,
        ),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("ahistt_"))
async def adm_history_topup_detail(call: CallbackQuery):
    if not _chk(call): return
    tid = call.data[7:]
    topup = topup_get(tid)
    if not topup:
        await call.answer("Пополнение не найдено.", show_alert=True)
        return
    await call.message.edit_text(
        topup_history_detail_text(topup),
        reply_markup=ikb_tx_detail(
            f"admhist_{topup['user_id']}",
            f"ahisttr_{tid}" if topup.get("receipt_file_id") else None,
        ),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("ahistw_"))
async def adm_history_withdrawal_detail(call: CallbackQuery):
    if not _chk(call): return
    wid = call.data[7:]
    withdrawal = withdrawal_get(wid)
    if not withdrawal:
        await call.answer("Вывод не найден.", show_alert=True)
        return
    await call.message.edit_text(
        withdrawal_history_detail_text(withdrawal),
        reply_markup=ikb_tx_detail(f"admhist_{withdrawal['user_id']}"),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.callback_query(F.data.startswith("ahistor_"))
async def adm_history_order_receipt(call: CallbackQuery):
    if not _chk(call): return
    oid = call.data[8:]
    order = order_get(oid)
    if not order:
        await call.answer("Заказ не найден.", show_alert=True)
        return
    receipt = order.get("receipt_file_id")
    if not receipt:
        await call.answer("Чек не прикреплён.", show_alert=True)
        return
    await send_receipt_preview(call, receipt, "Чек по заказу", oid)

@router.callback_query(F.data.startswith("ahisttr_"))
async def adm_history_topup_receipt(call: CallbackQuery):
    if not _chk(call): return
    tid = call.data[8:]
    topup = topup_get(tid)
    if not topup:
        await call.answer("Пополнение не найдено.", show_alert=True)
        return
    receipt = topup.get("receipt_file_id")
    if not receipt:
        await call.answer("Чек не прикреплён.", show_alert=True)
        return
    await send_receipt_preview(call, receipt, "Чек по пополнению", tid)

@router.callback_query(F.data.startswith("admbal_"))
async def adm_bal_prompt(call: CallbackQuery, state: FSMContext):
    if not _chk(call): return
    parts = call.data.split("_")   # admbal_add_uid / admbal_sub_uid
    if len(parts) != 3 or parts[1] not in {"add", "sub"} or not parts[2].isdigit():
        await call.answer("Ошибка данных.", show_alert=True)
        return
    op    = parts[1]
    uid   = int(parts[2])
    u     = user_get(uid)
    uname = display_username(u, fallback_id=uid)
    opname = "пополнить" if op == "add" else "списать"
    await state.update_data(bal_op=op, bal_uid=uid)
    await state.set_state(AdminFSM.manual_balance)
    await call.message.answer(
        f"💰 <b>{'Пополнение' if op=='add' else 'Списание'}</b>\n\n"
        f"Пользователь: {uname}\n"
        f"Баланс: <code>{fmt(u['balance'] if u else 0)}</code>\n\n"
        f"Введи сумму, которую хочешь {opname}:",
        reply_markup=reply_cancel(), parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.message(AdminFSM.manual_balance)
async def adm_bal_apply(message: Message, state: FSMContext, bot: Bot):
    try:
        amount = float((message.text or "").replace(",", ".").strip())
    except ValueError:
        await message.answer("❌ Введи число."); return
    if amount <= 0:
        await message.answer("❌ Должно быть больше 0."); return
    data  = await state.get_data()
    op    = data.get("bal_op")
    uid   = data.get("bal_uid")
    if op not in {"add", "sub"} or not isinstance(uid, int):
        await state.clear()
        await message.answer("❌ Сессия устарела. Начни заново.", reply_markup=reply_main(message.from_user.id))
        return
    if op == "add":
        user_add_balance(uid, amount)
    else:
        user_sub_balance(uid, amount)
    await state.clear()
    u     = user_get(uid)
    if not u:
        await message.answer("❌ Пользователь не найден.", reply_markup=reply_main(message.from_user.id))
        return
    uname = display_username(u, fallback_id=uid)
    label = f"+{fmt(amount)}" if op == "add" else f"-{fmt(amount)}"
    await message.answer(
        f"✅ Готово!\n\n{uname}\n{label}\nНовый баланс: <code>{fmt(u['balance'])}</code>",
        reply_markup=reply_main(message.from_user.id), parse_mode=ParseMode.HTML,
    )
    direction = "пополнен" if op == "add" else "скорректирован"
    try:
        await bot.send_message(uid,
            f"💰 <b>Баланс {direction}</b>\n\n{label}\n"
            f"Текущий баланс: <code>{fmt(u['balance'])}</code>",
            parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[NOTIFY] {e}")

# ═══════════════════════════════════════════════
#  ADMIN PANEL — STATS
# ═══════════════════════════════════════════════
@router.callback_query(F.data == "adm_stats")
async def adm_stats(call: CallbackQuery):
    if not _chk(call): return
    with dbc() as c:
        tu  = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        to  = c.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        toc = c.execute("SELECT COUNT(*) FROM orders WHERE status='invite_sent'").fetchone()[0]
        top = c.execute("SELECT COUNT(*) FROM orders WHERE status IN ('receipt_sent','payment_confirmed')").fetchone()[0]
        rev = c.execute("SELECT COALESCE(SUM(product_price),0) FROM orders WHERE status='invite_sent'").fetchone()[0]
        ttu = c.execute("SELECT COALESCE(SUM(amount),0) FROM topups WHERE status='confirmed'").fetchone()[0]
        twd = c.execute("SELECT COALESCE(SUM(amount),0) FROM withdrawals WHERE status='completed'").fetchone()[0]
        pwd = c.execute("SELECT COUNT(*) FROM withdrawals WHERE status='pending'").fetchone()[0]
    await call.message.edit_text(
        "📊 <b>Статистика</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Пользователей: <code>{tu}</code>\n"
        f"📦 Заказов всего: <code>{to}</code>\n"
        f"✅ Выполненных: <code>{toc}</code>\n"
        f"⏳ В обработке: <code>{top}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Выручка: <code>{fmt(rev)}</code>\n"
        f"💳 Пополнений: <code>{fmt(ttu)}</code>\n"
        f"💸 Выплачено: <code>{fmt(twd)}</code>\n"
        f"⏳ Выводов в ожидании: <code>{pwd}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━",
        reply_markup=ikb([[("🔙 Назад", "adm_main")]]),
        parse_mode=ParseMode.HTML,
    )
    await call.answer()

# ═══════════════════════════════════════════════
#  ADMIN PANEL — SETTINGS
# ═══════════════════════════════════════════════
@router.callback_query(F.data == "adm_settings")
async def adm_settings(call: CallbackQuery):
    if not _chk(call): return
    support = setting_get("support_user")
    pc      = setting_get("privat_card")
    mc      = setting_get("mono_card")
    tpc     = setting_get("topup_privat_card")
    tmc     = setting_get("topup_mono_card")
    await call.message.edit_text(
        "⚙️ <b>Настройки</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💬 Поддержка: <code>{h(support)}</code>\n\n"
        f"💳 Карта Privat (заказы): <code>{h(pc)}</code>\n"
        f"🖤 Карта Mono (заказы): <code>{h(mc)}</code>\n\n"
        f"💳 Карта Privat (пополнение): <code>{h(tpc)}</code>\n"
        f"🖤 Карта Mono (пополнение): <code>{h(tmc)}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━",
        reply_markup=ikb_settings(), parse_mode=ParseMode.HTML,
    )
    await call.answer()

SETTING_KEYS = {
    "adms_support":      ("support_user",      "💬 Введи новый контакт поддержки (напр. @manager):"),
    "adms_privat":       ("privat_card",        "💳 Введи номер карты PrivatBank (для заказов):"),
    "adms_mono":         ("mono_card",          "🖤 Введи номер карты Monobank (для заказов):"),
    "adms_topup_privat": ("topup_privat_card",  "💳 Введи номер карты PrivatBank (для пополнений):"),
    "adms_topup_mono":   ("topup_mono_card",    "🖤 Введи номер карты Monobank (для пополнений):"),
}

@router.callback_query(F.data.startswith("adms_"))
async def adm_setting_prompt(call: CallbackQuery, state: FSMContext):
    if not _chk(call): return
    entry = SETTING_KEYS.get(call.data)
    if not entry:
        await call.answer("Неизвестная настройка.", show_alert=True); return
    skey, prompt = entry
    await state.update_data(setting_key=skey)
    await state.set_state(AdminFSM.editing_setting)
    await call.message.answer(
        f"⚙️ <b>Изменение настройки</b>\n\n{prompt}",
        reply_markup=reply_cancel(), parse_mode=ParseMode.HTML,
    )
    await call.answer()

@router.message(AdminFSM.editing_setting)
async def adm_setting_save(message: Message, state: FSMContext):
    if (message.text or "").strip() in {"/cancel", "❌ Отмена"}:
        await state.clear()
        await message.answer(
            "❌ Изменение настройки отменено.",
            reply_markup=reply_main(message.from_user.id),
            parse_mode=ParseMode.HTML,
        )
        return
    data  = await state.get_data()
    skey  = data.get("setting_key")
    if not skey:
        await state.clear()
        await message.answer("❌ Сессия устарела. Открой настройки заново.", reply_markup=reply_main(message.from_user.id))
        return
    value = (message.text or "").strip()
    if not value:
        await message.answer("❌ Значение не может быть пустым."); return
    if skey == "support_user":
        value = "@" + value.lstrip("@")
    bank_name = settings_bank_key_name(skey)
    if bank_name:
        normalized_card = normalize_card_number(value)
        if not normalized_card:
            await message.answer(
                f"❌ Некорректный номер карты для {bank_name}.\n\nВведи от 12 до 19 цифр.",
                parse_mode=ParseMode.HTML,
            )
            return
        value = normalized_card
    setting_set(skey, value)
    await state.clear()
    await message.answer(
        f"✅ Настройка обновлена!\n\n<code>{h(skey)}</code> → <code>{h(value)}</code>",
        reply_markup=reply_main(message.from_user.id), parse_mode=ParseMode.HTML,
    )

# ───────────────────────────────────────────────
#  Accepted (user confirmed invite)
# ───────────────────────────────────────────────
@router.callback_query(F.data == "accepted")
async def cb_accepted(call: CallbackQuery):
    await call.message.edit_text(
        "🥳 <b>Добро пожаловать в Premium!</b>\n\nСпасибо за покупку! 🎉\n\n"
        "⭐️ Понравилось — порекомендуй нас другу!",
        parse_mode=ParseMode.HTML,
    )
    await call.message.answer("👇", reply_markup=reply_main(call.from_user.id))
    await call.answer("🎉 Спасибо!")

# ───────────────────────────────────────────────
#  /cancel command (anywhere)
# ───────────────────────────────────────────────
@router.message(F.text.in_({"/cancel", "❌ Отмена"}))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Отменено.",
                         reply_markup=reply_main(message.from_user.id),
                         parse_mode=ParseMode.HTML)

# ───────────────────────────────────────────────
#  FALLBACK
# ───────────────────────────────────────────────
FALLBACK_HINTS = {
    "OrderFSM:waiting_receipt":   "📸 Жду <b>фото</b> чека оплаты.",
    "TopUpFSM:waiting_receipt":   "📸 Жду <b>фото</b> чека пополнения.",
    "TopUpFSM:choosing_amount":   "✏️ Введи сумму цифрами.",
    "WithdrawFSM:entering_amount":"✏️ Введи сумму вывода цифрами.",
    "WithdrawFSM:entering_card":  "💳 Введи номер карты для вывода.",
    "WithdrawFSM:confirming_card":"💳 Подтверди номер карты кнопками ниже или введи заново.",
    "AdminFSM:edit_field":        "✏️ Введи новое значение или отправь фото.",
    "AdminFSM:new_catalog_category":"📂 Введи название новой категории.",
    "AdminFSM:new_stock":         "📦 Введи остаток товара целым числом.",
    "AdminFSM:user_search":       "🔍 Введи username или User ID.",
    "AdminFSM:manual_balance":    "💰 Введи сумму цифрами.",
    "AdminFSM:sending_message":   "📩 Введи текст сообщения. Для отмены: /cancel",
    "AdminFSM:editing_setting":   "⚙️ Введи новое значение.",
}

@router.message()
async def fallback(message: Message, state: FSMContext):
    cur = await state.get_state()
    if cur:
        hint = FALLBACK_HINTS.get(cur)
        if hint:
            await message.answer(hint, reply_markup=reply_cancel(), parse_mode=ParseMode.HTML)
        return
    await message.answer(
        "❌ <b>Не понял тебя</b>\n\nИспользуй кнопки ниже 👇",
        reply_markup=reply_main(message.from_user.id),
        parse_mode=ParseMode.HTML,
    )

@router.callback_query()
async def fallback_cb(call: CallbackQuery):
    await call.answer("Используй кнопки меню.", show_alert=True)


# ═══════════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════════
async def main():
    validate_runtime_config()
    db_init()
    bot = Bot(token=BOT_TOKEN)
    dp  = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    print("🤖 Bot started! DB:", DB_FILE)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
