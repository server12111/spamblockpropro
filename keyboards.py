from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from database import get_price

def start_kb():
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton('📤 Отправить сообщение', callback_data='user_send'),
        InlineKeyboardButton('🛒 Купить бота',         callback_data='buy_bot'),
    )
    return kb

def back_to_start_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('🔙 Назад', callback_data='back_to_start'))
    return kb

def back_to_payment_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('🔙 Назад', callback_data='back_to_payment'))
    return kb

def cancel_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('❌ Отмена', callback_data='cancel'))
    return kb

def close_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('❌ Закрыть', callback_data='close'))
    return kb

def reply_kb(target_id):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('📤 Ответить', callback_data=f'admin_reply_{target_id}'))
    return kb

def payment_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('💳 Оплатить через CryptoBot',  callback_data='pay_cryptobot'))
    kb.add(InlineKeyboardButton('💎 Оплатить через TON Keeper', callback_data='pay_ton'))
    kb.add(InlineKeyboardButton('🔙 Назад',                     callback_data='back_to_start'))
    return kb

def cryptobot_kb(invoice_url, invoice_id):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('💳 Оплатить',         url=invoice_url))
    kb.add(InlineKeyboardButton('✅ Проверить оплату', callback_data=f'check_payment_{invoice_id}'))
    kb.add(InlineKeyboardButton('🔙 Назад',            callback_data='back_to_payment'))
    return kb

def broadcast_type_kb(scope):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('📝 Только текст', callback_data=f'bcast_text_{scope}'))
    kb.add(InlineKeyboardButton('🖼 С фото',       callback_data=f'bcast_photo_{scope}'))
    kb.add(InlineKeyboardButton('❌ Отмена',        callback_data='cancel'))
    return kb

def super_admin_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('📢 Рассылка (главный бот)',   callback_data='broadcast'))
    kb.add(InlineKeyboardButton('📡 Рассылка по всем ботам',  callback_data='broadcast_all'))
    kb.add(InlineKeyboardButton('🤖 Управление ботами',       callback_data='manage_bots'))
    kb.add(InlineKeyboardButton('📊 Статистика',              callback_data='stats'))
    kb.add(InlineKeyboardButton('💰 Установить цену',         callback_data='set_price'))
    return kb

# ── Тексты ────────────────────────────────────────────
def start_text(admin_username):
    return (f"<b>🤖 Привет, это бот обратной связи с {admin_username}\n\n"
            "💬 Отправь своё сообщение и он обязательно прочитает, когда будет онлайн</b>")

def buy_text():
    return (f"<b>🤖 SpamBot — бот обратной связи</b>\n\n"
            f"Что получишь:\n"
            f"• Собственный бот для приёма сообщений\n"
            f"• Ответы пользователям прямо из Telegram\n"
            f"• Панель админа с рассылкой (текст + фото)\n"
            f"• Настройка под ключ\n\n"
            f"💰 Цена: <b>{get_price()} USDT</b>\n\n"
            f"Выбери способ оплаты:")
