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
TE_STATS   = _te('5431577498364158238', '📊')
TE_ROBOT   = _te('5372981976804366741', '🤖')
TE_HOURGLASS = _te('5451732530048802485', '⏳')
TE_CALENDAR = _te('5431897022456145283', '📆')
TE_CLOCK   = _te('5413704112220949842', '⏰')
TE_BELL    = _te('5242628160297641831', '🔔')
TE_CHART   = _te('5373001317042101552', '📈')
TE_NOTE    = _te('5334882760735598374', '📝')
TE_USER    = _te('5373012449597335010', '👤')
TE_INBOX   = _te('5433811242135331842', '📥')
TE_MAIL    = _te('5406631276042002796', '📨')
TE_KEYBOARD  = _te('5472111548572900003', '⌨️')
TE_PICTURE   = _te('5375074927252621134', '🖼')
TE_ANNOUNCE  = _te('5469903029144657419', '📣')
TE_REFRESH   = _te('5264727218734524899', '🔄')
TE_SEARCH    = _te('5188217332748527444', '🔍')
TE_FOLDER    = _te('5433653135799228968', '📁')
TE_NEW       = _te('5361979468887893611', '🆕')
TE_PARTY     = _te('5436040291507247633', '🎉')
TE_USERS     = _te('5372926953978341366', '👥')
TE_CART      = _te('5431499171045581032', '🛒')
TE_PLUS      = _te('5226945370684140473', '➕')
TE_MIC       = _te('5382360961313152917', '🎤')
TE_MASKS     = _te('5359441070201513074', '🎭')
TE_CROWN     = _te('5467406098367521267', '👑')
TE_ID        = _te('5422683699130933153', '🪪')
TE_RED       = _te('5449505950283078474', '❤️')
TE_GREEN     = _te('5449380056201697322', '💚')
TE_MOON      = _te('5188497854242495901', '🌑')
TE_PENCIL    = _te('5334673106202010226', '✏️')
TE_QUESTION  = _te('5467666648263564704', '❓')
TE_EXCLAM    = _te('5467928559664242360', '❗️')
TE_CLIP      = _te('5377844313575150051', '📎')
TE_VIDEO     = _te('5375309569905938163', '📹')
TE_BOLT      = _te('5431449001532594346', '⚡️')


_ICON = {
    '💬': '5443038326535759644',
    '💎': '5427168083074628963',
    '🔗': '5271604874419647061',
    '💳': '5445353829304387411',
    '💲': '5274244788953036050',
    '✔️': '5206607081334906820',
    '🎁': '5449800250032143374',
    '🆓': '5406756500108501710',
    '🔙': '5255703720078879038',
    '❌': '5210952531676504517',
    '📤': '5433614747381538714',
    '🚫': '5240241223632954241',
    '⭐': '5438496463044752972',
    '💰': '5224257782013769471',
    '🔥': '5424972470023104089',
    '🚀': '5445284980978621387',
    '📊': '5431577498364158238',
    '🤖': '5372981976804366741',
    '⏳': '5451732530048802485',
    '📆': '5431897022456145283',
    '⏰': '5413704112220949842',
    '🔔': '5242628160297641831',
    '📈': '5373001317042101552',
    '📝': '5334882760735598374',
    '👤': '5373012449597335010',
    '📥': '5433811242135331842',
    '📨': '5406631276042002796',
    '📁': '5433653135799228968',
    '📂': '5431721976769027887',
    '⌨️': '5472111548572900003',
    '🖼': '5375074927252621134',
    '📣': '5469903029144657419',
    '🔄': '5264727218734524899',
    '🔍': '5188217332748527444',
    '🆕': '5361979468887893611',
    '🎉': '5436040291507247633',
    '👥': '5372926953978341366',
    '🛒': '5431499171045581032',
    '➕': '5226945370684140473',
    '🎤': '5382360961313152917',
    '🎭': '5359441070201513074',
    '👑': '5467406098367521267',
    '🪪': '5422683699130933153',
    '❤️': '5449505950283078474',
    '💚': '5449380056201697322',
    '🌑': '5188497854242495901',
    '✏️': '5334673106202010226',
    '❓': '5467666648263564704',
    '❗️': '5467928559664242360',
    '📎': '5377844313575150051',
    '📹': '5375309569905938163',
    '⚡️': '5431449001532594346',
}

def _btn(text, icon=None, style=None, **kwargs):
    extra = {}
    if icon and icon in _ICON:
        extra['icon_custom_emoji_id'] = _ICON[icon]
    if style:
        extra['style'] = style
    return InlineKeyboardButton(text, **extra, **kwargs)


def start_kb():
    kb = InlineKeyboardMarkup()
    kb.row(
        _btn('Написать сообщение', icon='💬', style='primary',  callback_data='user_send'),
        _btn('Купить бота',        icon='💎', style='success',  callback_data='buy_bot'),
    )
    kb.add(_btn('Реферальная ссылка', icon='🔗', style='primary', callback_data='get_ref_link'))
    return kb


def back_to_start_kb():
    kb = InlineKeyboardMarkup()
    kb.add(_btn('Назад', icon='🔙', callback_data='back_to_start'))
    return kb


def back_to_payment_kb():
    kb = InlineKeyboardMarkup()
    kb.add(_btn('Назад', icon='🔙', callback_data='back_to_payment'))
    return kb


def cancel_kb():
    kb = InlineKeyboardMarkup()
    kb.add(_btn('Отмена', icon='❌', style='danger', callback_data='cancel'))
    return kb


def close_kb():
    kb = InlineKeyboardMarkup()
    kb.add(_btn('Закрыть', icon='❌', style='danger', callback_data='close'))
    return kb


def reply_kb(target_id):
    kb = InlineKeyboardMarkup()
    kb.add(_btn('Ответить', icon='📤', style='primary', callback_data=f'admin_reply_{target_id}'))
    return kb


def payment_kb(discount_count: int = 0, show_trial: bool = False):
    kb = InlineKeyboardMarkup()
    if discount_count > 0:
        kb.add(_btn(
            f'Скидка +10 дней (осталось: {discount_count})',
            icon='🎁', style='success', callback_data='use_discount'))
    if show_trial:
        kb.add(_btn('Попробовать бесплатно (3 дня)', icon='🆓', style='success', callback_data='try_free'))
    kb.add(_btn('Оплатить через CryptoBot',  icon='💳', style='primary', callback_data='pay_cryptobot'))
    kb.add(_btn('Оплатить через TON Keeper', icon='💎', style='primary', callback_data='pay_ton'))
    kb.add(_btn('Назад',                     icon='🔙',               callback_data='back_to_start'))
    return kb


def cryptobot_kb(invoice_url, invoice_id):
    kb = InlineKeyboardMarkup()
    kb.add(_btn('Оплатить',        icon='💳', style='primary', url=invoice_url))
    kb.add(_btn('Проверить оплату', icon='✔️', style='success', callback_data=f'check_payment_{invoice_id}'))
    kb.add(_btn('Назад',           icon='🔙',                  callback_data='back_to_payment'))
    return kb


def broadcast_type_kb(scope):
    kb = InlineKeyboardMarkup()
    kb.add(_btn('Только текст', style='primary', callback_data=f'bcast_text_{scope}'))
    kb.add(_btn('С фото',       style='primary', callback_data=f'bcast_photo_{scope}'))
    kb.add(_btn('Отмена',       icon='❌', style='danger', callback_data='cancel'))
    return kb


def super_admin_kb():
    kb = InlineKeyboardMarkup()
    kb.row(
        _btn('Рассылка (главный бот)', icon='📣',  style='primary', callback_data='broadcast'),
        _btn('Рассылка по всем ботам', icon='📣', style='primary', callback_data='broadcast_all'),
    )
    kb.row(
        _btn('Управление ботами', style='primary', callback_data='manage_bots'),
        _btn('Статистика', icon='📊',        style='primary', callback_data='stats'),
    )
    kb.row(
        _btn('Установить цену', style='success', callback_data='set_price'),
        _btn('Маркетплейс',     style='success', callback_data='marketplace'),
    )
    kb.row(
        _btn('Бесплатная выдача', icon='🎁', style='success', callback_data='free_give'),
        _btn('Запросы на вывод',  icon='💲', style='danger',  callback_data='withdrawal_list'),
    )
    kb.row(
        _btn('Скачать БД', icon='📁',      style='primary', callback_data='backup_db'),
        _btn('Восстановить БД', icon='📂', style='danger',  callback_data='restore_db_btn'),
    )
    return kb


# ── Тексты ────────────────────────────────────────────
def start_text():
    return (f"{TE_STAR} <b>Привет! Это бот для создания собственных ботов техподдержки</b>\n\n"
            f"{TE_CHAT} Купи подписку и получи своего бота для приёма обращений\n\n"
            f"{TE_MAIL} Также можешь просто написать сообщение — админ обязательно его увидит")


def buy_text():
    return (f"{TE_DIAMOND} <b>SpamBot — бот обратной связи</b>\n\n"
            f"Что получишь:\n"
            f"• Собственный бот для приёма сообщений\n"
            f"• Ответы пользователям прямо из Telegram\n"
            f"• Панель админа с рассылкой (текст + фото)\n"
            f"• Настройка под ключ\n\n"
            f"{TE_MONEY} Цена: <b>{get_price()} USDT</b>\n\n"
            f"Выбери способ оплаты:")
