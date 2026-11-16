"""Общее маскирование секретов до передачи записи любому обработчику логов"""
import logging
import re
import threading
from urllib.parse import quote

_secrets = set()
_secret_lock = threading.RLock()


def register_secret(value):
    if value:
        value = str(value)
        with _secret_lock:
            _secrets.update((value, value.replace(':', '_'), quote(value, safe='')))


def mask_secrets(text):
    text = str(text)
    with _secret_lock:
        secrets = tuple(_secrets)
    for secret in sorted(secrets, key=len, reverse=True):
        text = text.replace(secret, '[СКРЫТО]')
    text = re.sub(r'\b\d{3,15}(?::|_|%3[aA])[A-Za-z0-9_-]{20,}', '[СКРЫТО]', text)
    return re.sub(r'(?i)((?:api[_-]?key|token|secret)(?:%3[dD]|[\s\"\x27]*[=:][\s\"\x27]*))[^\s&\"\x27<>]+',
                  r'\1[СКРЫТО]', text)


class SecretFilter(logging.Filter):
    def filter(self, record):
        record.msg = mask_secrets(record.getMessage())
        record.args = ()
        if record.exc_info:
            record.exc_text = mask_secrets(logging.Formatter().formatException(record.exc_info))
            record.exc_info = None
        elif record.exc_text:
            record.exc_text = mask_secrets(record.exc_text)
        if record.stack_info:
            record.stack_info = mask_secrets(record.stack_info)
        return record.name != 'werkzeug'


def install_secret_filter():
    import config
    for name in ('TOKEN', 'CRYPTOBOT_TKN', 'TONCENTER_KEY'):
        register_secret(getattr(config, name, ''))
    logging.getLogger('werkzeug').disabled = True
    previous = logging.getLogRecordFactory()
    if getattr(previous, '_secrets_filtered', False):
        return
    guard = SecretFilter()
    def factory(*args, **kwargs):
        record = previous(*args, **kwargs)
        guard.filter(record)
        return record
    factory._secrets_filtered = True
    logging.setLogRecordFactory(factory)


install_secret_filter()
