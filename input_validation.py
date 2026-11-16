"""Общий диапазон чисел на границе ввода и SQLite"""
import math
from functools import wraps

SQLITE_MIN = -(2**63)
SQLITE_MAX = 2**63 - 1


def bounded_number(value, *, integer=True, minimum=1, maximum=SQLITE_MAX):
    try:
        number = int(value) if integer else float(value)
        if not integer and not math.isfinite(number):
            raise ValueError
        if not minimum <= number <= maximum:
            raise ValueError
    except (ValueError, TypeError, OverflowError):
        raise ValueError('Неверное число') from None
    return number


def validate_bindings(parameters):
    values = parameters.values() if isinstance(parameters, dict) else parameters
    for value in values:
        if isinstance(value, (int, float)):
            bounded_number(value, integer=isinstance(value, int), minimum=SQLITE_MIN)
    return parameters


def guard_numeric_callbacks(bot):
    """Числовые ID из кнопок проходят тот же диапазон до обращения к БД"""
    for handler in bot.callback_query_handlers:
        original = handler['function']
        @wraps(original)
        def checked(callback, original=original):
            # Форумные действия сначала проверяют контекст группы и темы
            # и сами применяют bounded_number, сохраняя свои отказы
            if (callback.data or '').startswith(('fs_pin_', 'fs_mgmt_')):
                return original(callback)
            try:
                for part in (callback.data or '').split('_'):
                    if part.lstrip('+-').isascii() and part.lstrip('+-').isdecimal():
                        bounded_number(part, minimum=SQLITE_MIN)
            except ValueError:
                bot.answer_callback_query(callback.id, 'Неверный ID', show_alert=True)
                return
            return original(callback)
        handler['function'] = checked
