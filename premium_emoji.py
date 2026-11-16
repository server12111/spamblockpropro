"""Единый повтор отправок и правок купленного бота без недоступных custom emoji"""
import copy
import re
import time
from functools import wraps
import database as db


def plain_emoji(value):
    if hasattr(value, 'read'):
        try: value.seek(0)
        except (AttributeError, OSError): pass
        return value
    if isinstance(value, str):
        return re.sub(r'</?tg-emoji\b[^>]*>', '', value, flags=re.I)
    if isinstance(value, (list, tuple)):
        # Символы custom_emoji уже находятся в тексте, UTF-16 offsets остальных entities сохраняются
        return [plain_emoji(v) for v in value if not (
            (v.get('type') if isinstance(v, dict) else getattr(v, 'type', None)) == 'custom_emoji')]
    if isinstance(value, dict):
        return {k: plain_emoji(v) for k, v in value.items() if k != 'icon_custom_emoji_id'}
    if hasattr(value, '__dict__'):
        result = copy.copy(value)
        result.__dict__ = plain_emoji(value.__dict__)
        if 'icon_custom_emoji_id' in value.__dict__:
            result.icon_custom_emoji_id = None
        return result
    return value


def custom_emoji_error(exc):
    low = str(exc).lower()
    return getattr(exc, 'error_code', None) == 400 and any(s in low for s in (
        'custom_emoji', 'custom emoji', 'emoji_status', 'premium', 'emoji-id', 'tg-emoji',
        'document_invalid', 'button_icon', 'emoji_id'))


def protect_premium_emoji(bot, bot_id):
    if getattr(bot, '_premium_guard', False):
        return
    bot._premium_guard = True
    def wrap(original):
        @wraps(original)
        def call(*args, **kwargs):
            until = float(db.db_get_bot_setting(bot_id, 'premium_unavailable_until', '0') or 0)
            if until > time.time():
                return original(*plain_emoji(args), **plain_emoji(kwargs))
            try:
                return original(*args, **kwargs)
            except Exception as exc:
                if not custom_emoji_error(exc):
                    raise
                result = original(*plain_emoji(args), **plain_emoji(kwargs))
                db.db_set_bot_setting(bot_id, 'premium_unavailable_until', str(time.time() + 86400))
                return result
        return call
    for name in dir(bot):
        if name.startswith(('send_', 'edit_message_')) and callable(getattr(bot, name)):
            setattr(bot, name, wrap(getattr(bot, name)))
