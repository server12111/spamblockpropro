from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from database import get_price

# ── Emoji для кнопок (plain Unicode) ──────────────────
PE_FIRE    = '🔥'
PE_DIAMOND = '💎'
PE_STAR    = '⭐'
PE_ROCKET  = '🚀'
PE_TROPHY  = '🏆'
PE_MONEY   = '💰'
PE_CROWN   = '👑'
PE_GIFT    = '🎁'
PE_CHECK   = '✅'
PE_BELL    = '🔔'

# ── Преміум анімовані emoji для HTML тексту повідомлень ─
def _te(emoji_id, fallback):
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'

TE_CHAT    = _te('5443038326535759644', '💬')
TE_DIAMOND = _te('5427168083074628963', '💎')
TE_LINK    = _te('5271604874419647061', '🔗')
TE_CARD    = _te('5445353829304387411', '💳')
TE_DOLLAR  = _te('5274244788953036050', '💲')
TE_CHECK   = _te('5206607081334906820', '✔️')
TE_GIFT    = _te('5449800250032143374', '🎁')
TE_FREE    = _te('5406756500108501710', '🆓')
TE_BACK    = _te('5255703720078879038', '🔙')
TE_CROSS   = _te('5210952531676504517', '❌')
TE_SEND    = _te('5433614747381538714', '📤')
TE_BLOCK   = _te('5240241223632954241', '🚫')
TE_STAR    = _te('5438496463044752972', '⭐️')
TE_MONEY   = _te('5224257782013769471', '💰')
TE_FIRE    = _te('5424972470023104089', '🔥')
TE_ROCKET  = _te('5445284980978621387', '🚀')


def _btn(text, **kwargs):
    """Создаёт InlineKeyboardButton с поддержкой color."""
    return InlineKeyboardButton(text, **kwargs)


def start_kb():
    kb = InlineKeyboardMarkup()
    kb.row(
        _btn('📤 Написать сообщение', callback_data='user_send'),
        _btn('🛒 Купить бота',        callback_data='buy_bot'),
    )
    kb.add(_btn('🔗 Реферальная ссылка', callback_data='get_ref_link'))
    return kb


def back_to_start_kb():
    kb = InlineKeyboardMarkup()
    kb.add(_btn('🔙 Назад', callback_data='back_to_start'))
    return kb


def back_to_payment_kb():
    kb = InlineKeyboardMarkup()
    kb.add(_btn('🔙 Назад', callback_data='back_to_payment'))
    return kb


def cancel_kb():
    kb = InlineKeyboardMarkup()
    kb.add(_btn('❌ Отмена', callback_data='cancel'))
    return kb


def close_kb():
    kb = InlineKeyboardMarkup()
    kb.add(_btn('❌ Закрыть', callback_data='close'))
    return kb


def reply_kb(target_id):
    kb = InlineKeyboardMarkup()
    kb.add(_btn('📤 Ответить', callback_data=f'admin_reply_{target_id}'))
    return kb


def payment_kb(discount_count: int = 0, show_trial: bool = False):
    kb = InlineKeyboardMarkup()
    if discount_count > 0:
        kb.add(_btn(
            f'🎁 Использовать скидку +10 дней (осталось: {discount_count})',
            callback_data='use_discount'))
    if show_trial:
        kb.add(_btn('🆓 Попробовать бесплатно (3 дня)', callback_data='try_free'))
    kb.add(_btn('💳 Оплатить через CryptoBot',  callback_data='pay_cryptobot'))
    kb.add(_btn('💎 Оплатить через TON Keeper', callback_data='pay_ton'))
    kb.add(_btn('🔙 Назад',                     callback_data='back_to_start'))
    return kb


def cryptobot_kb(invoice_url, invoice_id):
    kb = InlineKeyboardMarkup()
    kb.add(_btn('💳 Оплатить',           url=invoice_url))
    kb.add(_btn('✅ Проверить оплату',   callback_data=f'check_payment_{invoice_id}'))
    kb.add(_btn('🔙 Назад',              callback_data='back_to_payment'))
    return kb


def broadcast_type_kb(scope):
    kb = InlineKeyboardMarkup()
    kb.add(_btn('📝 Только текст', callback_data=f'bcast_text_{scope}'))
    kb.add(_btn('🖼 С фото',       callback_data=f'bcast_photo_{scope}'))
    kb.add(_btn('❌ Отмена',        callback_data='cancel'))
    return kb


def super_admin_kb():
    kb = InlineKeyboardMarkup()
    kb.row(
        _btn('📢 Рассылка (главный бот)',  callback_data='broadcast'),
        _btn('📡 Рассылка по всем ботам', callback_data='broadcast_all'),
    )
    kb.row(
        _btn('🤖 Управление ботами', callback_data='manage_bots'),
        _btn('📊 Статистика',        callback_data='stats'),
    )
    kb.row(
        _btn('💰 Установить цену',    callback_data='set_price'),
        _btn('🛒 Маркетплейс',       callback_data='marketplace'),
    )
    kb.row(
        _btn('🎁 Бесплатная выдача', callback_data='free_give'),
        _btn('💸 Запросы на вывод',  callback_data='withdrawal_list'),
    )
    return kb


# ── Тексты ────────────────────────────────────────────
def start_text(admin_username):
    return (f"{TE_STAR} <b>Привет! Это бот обратной связи с {admin_username}</b>\n\n"
            f"{TE_CHAT} Отправь своё сообщение и он обязательно прочитает, когда будет онлайн")


def buy_text():
    return (f"{TE_DIAMOND} <b>SpamBot — бот обратной связи</b>\n\n"
            f"Что получишь:\n"
            f"• Собственный бот для приёма сообщений\n"
            f"• Ответы пользователям прямо из Telegram\n"
            f"• Панель админа с рассылкой (текст + фото)\n"
            f"• Настройка под ключ\n\n"
            f"{TE_MONEY} Цена: <b>{get_price()} USDT</b>\n\n"
            f"Выбери способ оплаты:")
