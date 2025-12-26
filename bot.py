import asyncio
import logging
import os
import sys
import sqlite3
import re
import random
import time
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict

from aiogram import Bot, Dispatcher, F, Router, html
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    Message, CallbackQuery
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ========== TELEGRAM CLIENT API ==========
try:
    from telethon import TelegramClient
    from telethon.errors import (
        SessionPasswordNeededError, PhoneCodeInvalidError,
        FloodWaitError, ChannelInvalidError, ChatWriteForbiddenError,
        AuthKeyError, RpcCallFailError
    )
    from telethon.tl.functions.messages import ReportRequest
    from telethon.tl.types import (
        InputReportReasonSpam, InputReportReasonViolence,
        InputReportReasonPornography, InputReportReasonChildAbuse,
        InputReportReasonCopyright, InputReportReasonIllegalDrugs,
        InputReportReasonOther, InputReportReasonFake
    )
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False
    print("🎄 Telethon не установлен! Установите: pip install telethon")

# ========== КОНФИГУРАЦИЯ ==========
class Config:
    BOT_TOKEN = "8203239986:AAF7fFMo5t6Io3sgll8NFaAlYlldfrP2zTM"
    OWNER_ID = 8050595279
    OWNER_USERNAME = "Wawichh"
    
    API_ID = 22778226
    API_HASH = "9be02c55dfb4c834210599490dcd58a8"
    
    LOG_CHANNEL_ID = -1003688204597
    
    # Настройки безопасности
    COMPLAINT_COOLDOWN = 150
    MAX_COMPLAINTS_PER_HOUR = 20
    MAX_COMPLAINTS_PER_DAY = 100
    MAX_REQUESTS_PER_MINUTE = 30
    
    # Настройки подписки
    SUBSCRIPTION_DAYS = 30
    SUBSCRIPTION_PRICE_RUB = 100
    SUBSCRIPTION_PRICE_USD = 1
    
    # Пути
    DB_PATH = "wenty_snow_secure.db"
    SESSIONS_DIR = "sessions_secure"
    BLACKLIST_FILE = "blacklist.txt"

# Создаем директории
os.makedirs(Config.SESSIONS_DIR, exist_ok=True)

# ========== АНТИ-DDoS СИСТЕМА ==========
class AntiDDoS:
    def __init__(self):
        self.request_counts = defaultdict(int)
        self.last_reset = time.time()
        self.blacklist = self.load_blacklist()
    
    def load_blacklist(self):
        blacklist = set()
        if os.path.exists(Config.BLACKLIST_FILE):
            with open(Config.BLACKLIST_FILE, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        blacklist.add(int(line) if line.isdigit() else line)
        return blacklist
    
    def save_blacklist(self):
        with open(Config.BLACKLIST_FILE, 'w') as f:
            f.write("# Blacklisted users/IPs\n")
            for item in self.blacklist:
                f.write(f"{item}\n")
    
    def check_rate_limit(self, user_id: int) -> Tuple[bool, str]:
        current_time = time.time()
        
        if current_time - self.last_reset > 60:
            self.request_counts.clear()
            self.last_reset = current_time
        
        if user_id in self.blacklist:
            return False, "🚫 Вы в черном списке"
        
        user_key = f"user_{user_id}"
        self.request_counts[user_key] += 1
        
        if self.request_counts[user_key] > Config.MAX_REQUESTS_PER_MINUTE:
            self.blacklist.add(user_id)
            self.save_blacklist()
            return False, f"🚫 DDoS защита: превышен лимит запросов"
        
        return True, "✅ OK"
    
    def add_to_blacklist(self, user_id: int):
        self.blacklist.add(user_id)
        self.save_blacklist()
    
    def remove_from_blacklist(self, user_id: int):
        if user_id in self.blacklist:
            self.blacklist.remove(user_id)
            self.save_blacklist()

# Инициализация анти-DDoS
antiddos = AntiDDoS()

# ========== ЭМОДЗИ ==========
class Emojis:
    SNOWFLAKE = "❄️"
    SANTA = "🎅"
    TREE = "🎄"
    GIFT = "🎁"
    STAR = "⭐"
    BELL = "🔔"
    FIRE = "🔥"
    ICE = "🧊"
    SUCCESS = "✅"
    ERROR = "❌"
    WARNING = "⚠️"
    TIME = "⏰"
    LOADING = "🔄"
    COMPLAINT = "📨"
    PROFILE = "👤"
    STATS = "📊"
    ADMIN = "⚙️"
    TOP = "🏆"
    SUBSCRIPTION = "🔐"
    MONEY = "💰"
    CONTACT = "📱"
    SHIELD = "🛡️"
    ROCKET = "🚀"
    ZAP = "⚡"

# ========== ЛОГИРОВАНИЕ ==========
logging.basicConfig(
    level=logging.INFO,
    format=f'{Emojis.SNOWFLAKE} %(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('wenty_snow.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ========== БАЗА ДАННЫХ ==========
class Database:
    def __init__(self):
        self.db_path = Config.DB_PATH
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
    
    def _create_tables(self):
        cursor = self.conn.cursor()
        
        tables = [
            """CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE,
                username TEXT,
                full_name TEXT,
                snowflakes INTEGER DEFAULT 100,
                is_banned INTEGER DEFAULT 0,
                ban_reason TEXT,
                agreed_to_rules INTEGER DEFAULT 0,
                subscription_active INTEGER DEFAULT 0,
                subscription_until DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_complaint DATETIME,
                complaints_count INTEGER DEFAULT 0,
                successful_complaints INTEGER DEFAULT 0,
                failed_complaints INTEGER DEFAULT 0,
                user_level INTEGER DEFAULT 1,
                reputation INTEGER DEFAULT 100,
                last_activity DATETIME DEFAULT CURRENT_TIMESTAMP
            )""",
            
            """CREATE TABLE IF NOT EXISTS complaints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                message_link TEXT,
                chat_id TEXT,
                message_id INTEGER,
                status TEXT DEFAULT 'pending',
                sent_via TEXT,
                reason TEXT,
                snowflakes_earned INTEGER DEFAULT 0,
                error_log TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                sent_at DATETIME,
                retry_count INTEGER DEFAULT 0,
                hash TEXT UNIQUE
            )""",
            
            """CREATE TABLE IF NOT EXISTS telegram_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_name TEXT UNIQUE,
                phone_number TEXT,
                is_active INTEGER DEFAULT 1,
                success_rate REAL DEFAULT 0,
                total_used INTEGER DEFAULT 0,
                successful_used INTEGER DEFAULT 0,
                failed_used INTEGER DEFAULT 0,
                last_used DATETIME,
                last_error TEXT,
                priority INTEGER DEFAULT 1,
                daily_limit INTEGER DEFAULT 50,
                used_today INTEGER DEFAULT 0
            )""",
            
            """CREATE TABLE IF NOT EXISTS daily_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE UNIQUE,
                total_complaints INTEGER DEFAULT 0,
                successful_complaints INTEGER DEFAULT 0,
                failed_complaints INTEGER DEFAULT 0,
                active_users INTEGER DEFAULT 0,
                revenue_rub REAL DEFAULT 0,
                revenue_usd REAL DEFAULT 0
            )""",
            
            """CREATE TABLE IF NOT EXISTS action_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT,
                details TEXT,
                ip_address TEXT,
                user_agent TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )"""
        ]
        
        for table in tables:
            try:
                cursor.execute(table)
            except Exception as e:
                logger.error(f"Ошибка таблицы: {e}")
        
        self.conn.commit()
        logger.info(f"{Emojis.SUCCESS} База данных инициализирована")
    
    def get_user(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cursor.fetchone()
    
    def create_user(self, user_id: int, username: str, full_name: str):
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT OR IGNORE INTO users 
               (user_id, username, full_name) 
               VALUES (?, ?, ?)""",
            (user_id, username or f"user_{user_id}", full_name or "Пользователь")
        )
        self.conn.commit()
    
    def update_user_activity(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE users SET last_activity = CURRENT_TIMESTAMP WHERE user_id = ?",
            (user_id,)
        )
        self.conn.commit()
    
    def check_complaint_limits(self, user_id: int) -> Tuple[bool, str]:
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) as count FROM complaints 
            WHERE user_id = ? AND created_at > datetime('now', '-1 hour')
        """, (user_id,))
        hour_count = cursor.fetchone()["count"]
        
        if hour_count >= Config.MAX_COMPLAINTS_PER_HOUR:
            return False, f"🚫 Лимит: {Config.MAX_COMPLAINTS_PER_HOUR} жалоб в час"
        
        cursor.execute("""
            SELECT COUNT(*) as count FROM complaints 
            WHERE user_id = ? AND created_at > datetime('now', '-24 hours')
        """, (user_id,))
        day_count = cursor.fetchone()["count"]
        
        if day_count >= Config.MAX_COMPLAINTS_PER_DAY:
            return False, f"🚫 Лимит: {Config.MAX_COMPLAINTS_PER_DAY} жалоб в сутки"
        
        return True, "✅ OK"
    
    def add_complaint(self, user_id: int, message_link: str, reason: str = "spam", 
                     chat_id: str = None, message_id: int = None):
        complaint_hash = hashlib.md5(
            f"{user_id}_{message_link}_{reason}".encode()
        ).hexdigest()
        
        cursor = self.conn.cursor()
        
        cursor.execute(
            "SELECT 1 FROM complaints WHERE hash = ? AND created_at > datetime('now', '-24 hours')",
            (complaint_hash,)
        )
        if cursor.fetchone():
            return None
        
        cursor.execute(
            """INSERT INTO complaints 
               (user_id, message_link, chat_id, message_id, reason, hash) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, message_link, chat_id, message_id, reason, complaint_hash)
        )
        complaint_id = cursor.lastrowid
        
        cursor.execute(
            """UPDATE users SET 
               complaints_count = complaints_count + 1,
               last_complaint = CURRENT_TIMESTAMP,
               last_activity = CURRENT_TIMESTAMP
               WHERE user_id = ?""",
            (user_id,)
        )
        
        self.conn.commit()
        return complaint_id
    
    def update_complaint_status(self, complaint_id: int, status: str, 
                              session_name: str = None, error: str = None):
        cursor = self.conn.cursor()
        
        updates = ["status = ?", "sent_at = CURRENT_TIMESTAMP"]
        values = [status]
        
        if session_name:
            updates.append("sent_via = ?")
            values.append(session_name)
        
        if error:
            updates.append("error_log = ?")
            values.append(error[:500])
        
        values.append(complaint_id)
        set_clause = ", ".join(updates)
        
        cursor.execute(f"UPDATE complaints SET {set_clause} WHERE id = ?", values)
        
        if status == 'sent':
            cursor.execute("""
                UPDATE users u 
                SET successful_complaints = successful_complaints + 1,
                    snowflakes = snowflakes + 10,
                    reputation = CASE 
                        WHEN reputation < 95 THEN reputation + 5 
                        ELSE 100 
                    END
                WHERE user_id = (SELECT user_id FROM complaints WHERE id = ?)
            """, (complaint_id,))
        elif status == 'failed':
            cursor.execute("""
                UPDATE users u 
                SET failed_complaints = failed_complaints + 1,
                    reputation = CASE 
                        WHEN reputation > 10 THEN reputation - 5 
                        ELSE 0 
                    END
                WHERE user_id = (SELECT user_id FROM complaints WHERE id = ?)
            """, (complaint_id,))
        
        self.conn.commit()
    
    def get_best_session(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM telegram_sessions 
            WHERE is_active = 1 
            AND (daily_limit = 0 OR used_today < daily_limit)
            AND (last_error IS NULL OR last_error NOT LIKE '%flood%')
            ORDER BY 
                priority DESC,
                success_rate DESC,
                (CASE WHEN last_used IS NULL THEN 1 ELSE 0 END) DESC,
                last_used ASC
            LIMIT 1
        """)
        return cursor.fetchone()
    
    def update_session_stats(self, session_name: str, success: bool, error: str = None):
        cursor = self.conn.cursor()
        
        cursor.execute("""
            UPDATE telegram_sessions 
            SET used_today = 0 
            WHERE date(last_used) < date('now')
        """)
        
        cursor.execute(
            "UPDATE telegram_sessions SET total_used = total_used + 1 WHERE session_name = ?",
            (session_name,)
        )
        
        if success:
            cursor.execute("""
                UPDATE telegram_sessions SET 
                successful_used = successful_used + 1,
                used_today = used_today + 1,
                last_used = CURRENT_TIMESTAMP,
                last_error = NULL
                WHERE session_name = ?
            """, (session_name,))
        else:
            cursor.execute("""
                UPDATE telegram_sessions SET 
                failed_used = failed_used + 1,
                used_today = used_today + 1,
                last_used = CURRENT_TIMESTAMP,
                last_error = ?
                WHERE session_name = ?
            """, (error[:200] if error else "unknown", session_name))
        
        cursor.execute("""
            UPDATE telegram_sessions 
            SET success_rate = CASE 
                WHEN total_used > 0 THEN (successful_used * 100.0 / total_used)
                ELSE 0 
            END
            WHERE session_name = ?
        """, (session_name,))
        
        self.conn.commit()
    
    def add_session(self, session_name: str, phone_number: str = None, priority: int = 1):
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT OR REPLACE INTO telegram_sessions 
               (session_name, phone_number, priority) 
               VALUES (?, ?, ?)""",
            (session_name, phone_number, priority)
        )
        self.conn.commit()
    
    def get_user_stats(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                u.*,
                (SELECT COUNT(*) FROM complaints WHERE user_id = ? AND status = 'sent') as sent_today,
                (SELECT COUNT(*) FROM complaints WHERE user_id = ? AND status = 'failed') as failed_today
            FROM users u WHERE u.user_id = ?
        """, (user_id, user_id, user_id))
        return cursor.fetchone()
    
    def get_system_stats(self):
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_users,
                COUNT(CASE WHEN subscription_active = 1 THEN 1 END) as active_subs,
                COUNT(CASE WHEN is_banned = 1 THEN 1 END) as banned_users,
                SUM(complaints_count) as total_complaints,
                SUM(successful_complaints) as successful_complaints,
                SUM(snowflakes) as total_snowflakes
            FROM users
        """)
        users_stats = cursor.fetchone()
        
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("""
            SELECT 
                COUNT(*) as today_complaints,
                COUNT(CASE WHEN status = 'sent' THEN 1 END) as today_successful,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as today_failed
            FROM complaints WHERE date(created_at) = ?
        """, (today,))
        today_stats = cursor.fetchone()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_sessions,
                COUNT(CASE WHEN is_active = 1 THEN 1 END) as active_sessions,
                SUM(successful_used) as total_successful,
                AVG(success_rate) as avg_success_rate
            FROM telegram_sessions
        """)
        sessions_stats = cursor.fetchone()
        
        return {
            'users': users_stats,
            'today': today_stats,
            'sessions': sessions_stats
        }

# Инициализация БД
db = Database()

# ========== ПАРСЕР ССЫЛОК ==========
class LinkParser:
    @staticmethod
    def parse_message_link(link: str):
        link = link.strip().split('?')[0].split('#')[0]
        
        patterns = [
            r't\.me/c/(?P<chat_id>-?\d+)/(?P<message_id>\d+)',
            r't\.me/(?P<username>[a-zA-Z0-9_]+)/(?P<message_id>\d+)',
            r't\.me/(?P<username>[a-zA-Z0-9_]+)/(?P<message_id>\d+)/comment/(?P<reply_id>\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, link)
            if match:
                result = match.groupdict()
                try:
                    message_id = int(result['message_id'])
                    if message_id <= 0:
                        return None
                    result['message_id'] = message_id
                    return result
                except:
                    return None
        return None
    
    @staticmethod
    def normalize_chat_id(chat_id: str):
        try:
            chat_id_int = int(chat_id)
            if chat_id_int > 0:
                return int(f"-100{chat_id_int}")
            return chat_id_int
        except:
            return chat_id

# ========== ОПТИМИЗИРОВАННЫЙ МЕНЕДЖЕР СЕССИЙ ==========
class OptimizedSessionManager:
    def __init__(self):
        self.clients: Dict[str, TelegramClient] = {}
        self.parser = LinkParser()
        self.last_cleanup = time.time()
    
    def _cleanup_old_clients(self):
        if time.time() - self.last_cleanup > 300:
            for session_name, client in list(self.clients.items()):
                try:
                    if not client.is_connected():
                        del self.clients[session_name]
                except:
                    del self.clients[session_name]
            self.last_cleanup = time.time()
    
    async def get_client(self, session_name: str):
        self._cleanup_old_clients()
        
        if session_name in self.clients:
            client = self.clients[session_name]
            try:
                if await client.is_user_authorized():
                    return client
            except:
                pass
        
        session_path = os.path.join(Config.SESSIONS_DIR, f"{session_name}.session")
        if not os.path.exists(session_path):
            logger.error(f"Файл сессии не найден: {session_path}")
            return None
        
        client = TelegramClient(
            session_path,
            Config.API_ID,
            Config.API_HASH,
            connection_retries=3,
            request_retries=2,
            flood_sleep_threshold=120
        )
        
        try:
            await client.connect(timeout=10)
            
            if not await client.is_user_authorized():
                logger.error(f"Сессия не авторизована: {session_name}")
                await client.disconnect()
                return None
            
            self.clients[session_name] = client
            logger.info(f"{Emojis.SUCCESS} Сессия {session_name} подключена")
            return client
            
        except Exception as e:
            logger.error(f"{Emojis.ERROR} Ошибка подключения {session_name}: {str(e)[:100]}")
            return None
    
    async def send_complaint_with_retry(self, complaint_id: int, message_link: str, 
                                      reason: str = 'spam', max_retries: int = 3):
        link_data = self.parser.parse_message_link(message_link)
        if not link_data:
            return False, "❌ Неверный формат ссылки", None, 0
        
        for attempt in range(max_retries):
            session = db.get_best_session()
            if not session:
                return False, "❌ Нет доступных сессий", None, attempt
            
            session_name = session['session_name']
            client = await self.get_client(session_name)
            
            if not client:
                db.update_session_stats(session_name, False, "client_error")
                continue
            
            try:
                if 'username' in link_data:
                    entity = await client.get_entity(link_data['username'])
                else:
                    chat_id = self.parser.normalize_chat_id(link_data['chat_id'])
                    entity = await client.get_entity(chat_id)
                
                message_id = int(link_data['message_id'])
                
                reason_map = {
                    'spam': InputReportReasonSpam(),
                    'violence': InputReportReasonViolence(),
                    'pornography': InputReportReasonPornography(),
                    'child_abuse': InputReportReasonChildAbuse(),
                    'copyright': InputReportReasonCopyright(),
                    'drugs': InputReportReasonIllegalDrugs(),
                    'fake': InputReportReasonFake(),
                    'other': InputReportReasonOther()
                }
                
                report_reason = reason_map.get(reason, InputReportReasonSpam())
                
                await asyncio.sleep(random.uniform(1.0, 3.0))
                
                await client(ReportRequest(
                    peer=entity,
                    id=[message_id],
                    reason=report_reason,
                    message=""
                ))
                
                db.update_session_stats(session_name, True)
                db.update_complaint_status(complaint_id, 'sent', session_name)
                
                return True, f"✅ Жалоба отправлена через {session_name}", session_name, attempt
                
            except FloodWaitError as e:
                wait_time = e.seconds
                logger.warning(f"Flood wait {session_name}: {wait_time} сек")
                db.update_session_stats(session_name, False, f"flood_{wait_time}")
                await asyncio.sleep(wait_time + random.randint(5, 15))
                continue
                
            except (ChannelInvalidError, ChatWriteForbiddenError) as e:
                error_msg = str(e)
                db.update_session_stats(session_name, False, error_msg[:100])
                db.update_complaint_status(complaint_id, 'failed', session_name, error_msg)
                return False, "❌ Нет доступа к чату", session_name, attempt
                
            except Exception as e:
                error_msg = str(e)[:200]
                logger.error(f"Ошибка отправки {session_name}: {error_msg}")
                db.update_session_stats(session_name, False, error_msg)
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(random.uniform(2.0, 5.0))
                    continue
                else:
                    db.update_complaint_status(complaint_id, 'failed', session_name, error_msg)
                    return False, f"❌ Ошибка: {error_msg}", session_name, attempt
        
        return False, "❌ Все попытки исчерпаны", None, max_retries

# ========== СОСТОЯНИЯ ==========
class ComplaintStates(StatesGroup):
    waiting_for_link = State()

# ========== ИНИЦИАЛИЗАЦИЯ БОТА ==========
bot = Bot(token=Config.BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

session_manager = OptimizedSessionManager() if TELETHON_AVAILABLE else None

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def is_admin(user_id: int) -> bool:
    return user_id == Config.OWNER_ID

def get_main_menu(user_id: int):
    builder = InlineKeyboardBuilder()
    
    builder.button(text=f"{Emojis.COMPLAINT} Отправить жалобу", callback_data="send_complaint")
    builder.button(text=f"{Emojis.PROFILE} Профиль", callback_data="profile")
    builder.button(text=f"{Emojis.STATS} Статистика", callback_data="stats")
    
    if is_admin(user_id):
        builder.button(text=f"{Emojis.ADMIN} Админ", callback_data="admin_panel")
    
    builder.adjust(2, 1, 1)
    return builder.as_markup()

async def send_log_to_channel(text: str):
    try:
        await bot.send_message(Config.LOG_CHANNEL_ID, text, parse_mode=ParseMode.HTML)
    except:
        pass

# ========== ОСНОВНЫЕ ОБРАБОТЧИКИ ==========
@router.message(CommandStart())
async def cmd_start(message: Message):
    user_id = message.from_user.id
    
    allowed, ddos_msg = antiddos.check_rate_limit(user_id)
    if not allowed:
        await message.answer(ddos_msg)
        return
    
    db.create_user(user_id, message.from_user.username, message.from_user.full_name)
    db.update_user_activity(user_id)
    
    user = db.get_user(user_id)
    if user and user['is_banned']:
        await message.answer(f"{Emojis.ERROR} Вы заблокированы. Причина: {user['ban_reason']}")
        return
    
    stats = db.get_system_stats()
    avg_success = stats['sessions']['avg_success_rate'] or 0
    
    welcome_text = f"""
{Emojis.SANTA} <b>Wenty Snow v2.0</b>

{Emojis.SHIELD} <b>Улучшенная система:</b>
• Анти-DDoS защита
• Максимальная доходимость
• Умная ротация сессий
• Защита от дубликатов

{Emojis.ROCKET} <b>Статистика системы:</b>
• Активных сессий: {stats['sessions']['active_sessions'] or 0}
• Успешность: {avg_success:.1f}%
• Онлайн: {stats['today']['today_complaints'] or 0} жалоб сегодня

Выберите действие:
"""
    
    await message.answer(welcome_text, reply_markup=get_main_menu(user_id), parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "send_complaint")
async def start_complaint(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    allowed, ddos_msg = antiddos.check_rate_limit(user_id)
    if not allowed:
        await callback.answer(ddos_msg, show_alert=True)
        return
    
    user = db.get_user(user_id)
    if not user:
        await callback.answer("Пользователь не найден")
        return
    
    allowed, limit_msg = db.check_complaint_limits(user_id)
    if not allowed:
        await callback.answer(limit_msg, show_alert=True)
        return
    
    if user['last_complaint']:
        try:
            last_time = datetime.fromisoformat(user['last_complaint'].replace('Z', '+00:00'))
            seconds_passed = (datetime.utcnow() - last_time).total_seconds()
            if seconds_passed < Config.COMPLAINT_COOLDOWN:
                wait_time = int(Config.COMPLAINT_COOLDOWN - seconds_passed)
                await callback.answer(f"{Emojis.TIME} Подождите {wait_time} секунд", show_alert=True)
                return
        except:
            pass
    
    await state.set_state(ComplaintStates.waiting_for_link)
    
    stats = db.get_user_stats(user_id)
    success_rate = 0
    if stats and stats['complaints_count'] > 0:
        success_rate = (stats['successful_complaints'] / stats['complaints_count']) * 100
    
    await callback.message.edit_text(
        f"{Emojis.COMPLAINT} <b>Отправка жалобы</b>\n\n"
        f"{Emojis.ZAP} Ваша статистика:\n"
        f"• Успешность: {success_rate:.1f}%\n"
        f"• Репутация: {user['reputation']}/100\n"
        f"• Снежинок: {user['snowflakes']}\n\n"
        f"Отправьте ссылку на сообщение:\n"
        f"<i>Пример: https://t.me/durov/123</i>\n\n"
        f"{Emojis.SHIELD} <b>Защита включена:</b>\n"
        f"• Проверка дубликатов\n"
        f"• Умные повторы\n"
        f"• Анти-флуд система",
        parse_mode=ParseMode.HTML
    )

@router.message(ComplaintStates.waiting_for_link)
async def process_complaint(message: Message, state: FSMContext):
    user_id = message.from_user.id
    link = message.text.strip()
    
    allowed, ddos_msg = antiddos.check_rate_limit(user_id)
    if not allowed:
        await message.answer(ddos_msg)
        await state.clear()
        return
    
    parser = LinkParser()
    link_data = parser.parse_message_link(link)
    
    if not link_data:
        await message.answer(
            f"{Emojis.ERROR} <b>Неверный формат!</b>\n\n"
            f"Примеры корректных ссылок:\n"
            f"• https://t.me/username/123\n"
            f"• https://t.me/c/-123456789/123\n\n"
            f"Попробуйте снова:",
            parse_mode=ParseMode.HTML
        )
        return
    
    chat_id = link_data.get('chat_id') or link_data.get('username')
    complaint_id = db.add_complaint(
        user_id, link, 'spam', 
        chat_id, link_data['message_id']
    )
    
    if not complaint_id:
        await message.answer(
            f"{Emojis.ERROR} <b>Дубликат жалобы!</b>\n\n"
            f"Эта жалоба уже была отправлена за последние 24 часа.",
            parse_mode=ParseMode.HTML
        )
        await state.clear()
        return
    
    await message.answer(
        f"{Emojis.LOADING} <b>Обработка жалобы...</b>\n\n"
        f"ID: <code>{complaint_id}</code>\n"
        f"Ссылка: {link[:50]}...\n"
        f"Статус: отправка через сессии",
        parse_mode=ParseMode.HTML
    )
    
    if session_manager:
        success, result_msg, session_name, attempts = await session_manager.send_complaint_with_retry(
            complaint_id, link, 'spam'
        )
        
        if success:
            result_text = f"""
{Emojis.SUCCESS} <b>Жалоба отправлена!</b>

{Emojis.ROCKET} <b>Детали:</b>
• ID: <code>{complaint_id}</code>
• Сессия: {session_name}
• Попыток: {attempts + 1}
• Статус: Успешно

{Emojis.GIFT} <b>Награда:</b>
• +10 снежинок ❄️
• Репутация: +5 ⭐

{Emojis.TIME} Следующая жалоба через {Config.COMPLAINT_COOLDOWN} секунд
"""
        else:
            result_text = f"""
{Emojis.ERROR} <b>Ошибка отправки</b>

{Emojis.WARNING} <b>Детали:</b>
• ID: <code>{complaint_id}</code>
• Сессия: {session_name or 'Нет'}
• Попыток: {attempts + 1}
• Ошибка: {result_msg}

{Emojis.SHIELD} <b>Рекомендации:</b>
• Проверьте ссылку
• Подождите 5 минут
• Попробуйте снова
"""
    else:
        # Имитация отправки
        await asyncio.sleep(2)
        db.update_complaint_status(complaint_id, 'sent', 'test_session')
        result_text = f"""
{Emojis.SUCCESS} <b>Жалоба сохранена</b>

{Emojis.WARNING} <b>Режим тестирования</b>
Telethon не установлен. Установите:
<code>pip install telethon</code>

Жалоба будет отправлена при наличии сессий.
"""
    
    await message.answer(result_text, parse_mode=ParseMode.HTML)
    await state.clear()

@router.callback_query(F.data == "profile")
async def show_profile(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = db.get_user(user_id)
    
    if not user:
        await callback.answer("Пользователь не найден")
        return
    
    success_rate = 0
    if user['complaints_count'] > 0:
        success_rate = (user['successful_complaints'] / user['complaints_count']) * 100
    
    profile_text = f"""
{Emojis.PROFILE} <b>Ваш профиль</b>

{Emojis.STAR} <b>Основное:</b>
🆔 ID: <code>{user['user_id']}</code>
👤 Имя: {html.quote(user['full_name'])}
📛 Юзернейм: @{user['username'] or 'нет'}

{Emojis.STATS} <b>Статистика:</b>
📨 Всего жалоб: {user['complaints_count']}
✅ Успешных: {user['successful_complaints']}
❌ Неудачных: {user['failed_complaints']}
⭐ Успешность: {success_rate:.1f}%

{Emojis.SNOWFLAKE} <b>Прогресс:</b>