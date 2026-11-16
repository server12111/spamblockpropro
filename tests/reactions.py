"""Реальные обработчики и связи SQLite; Telegram API полностью подменён."""
import os
import json
import sys
import tempfile
import time
import traceback
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
(ROOT / '.test_tmp').mkdir(exist_ok=True)
os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='reactions_', dir=ROOT / '.test_tmp')
sys.modules['dotenv'] = SimpleNamespace(load_dotenv=lambda *a, **kw: None)

from telebot import types as t
from telebot.apihelper import ApiTelegramException
import database as db
import forum_support as fs

OK, FAIL = [], []


def check(name, condition):
    (OK if condition else FAIL).append(name)
    print(('[OK] ' if condition else '[FAIL] ') + name)


def api_error(code, description):
    return ApiTelegramException('test', None, dict(ok=False, error_code=code,
                                                 description=description))


class Bot:
    def __init__(self):
        self.calls = []
        self.messages = []
        self.custom_calls = []
        self.fault = None
        self.sticker_emoji = '👌'

    def set_message_reaction(self, chat_id, message_id, reaction):
        self.calls.append((chat_id, message_id, [r.to_dict() for r in reaction]))
        if self.fault:
            self.fault(reaction)
        return True

    def send_message(self, *args, **kwargs):
        self.messages.append((args, kwargs))

    def get_custom_emoji_stickers(self, ids):
        self.custom_calls.append(ids)
        return [SimpleNamespace(custom_emoji_id=ids[0], emoji=self.sticker_emoji)]


bot = Bot()
ADMIN, UID, GID = 777, 888, -100123
db.init_db()
BID = db.db_add_bot(ADMIN, '123456:TEST', ADMIN, days=30)
PRIVATE_BID = db.db_add_bot(ADMIN, '123457:TEST', ADMIN, days=30)
fs.fs_bind_group(BID, GID, 'Поддержка')
fs.fs_map_message(BID, GID, 500, UID, 10, 45)
fs.fs_map_message(PRIVATE_BID, ADMIN, 600, UID, 0, 55)


def event(reactions, chat=GID, mid=500, uid=ADMIN):
    return t.MessageReactionUpdated(
        chat=t.Chat(chat, 'supergroup' if chat < 0 else 'private'),
        message_id=mid, date=int(time.time()), user=t.User(uid, False, 'Админ'),
        old_reaction=[], new_reaction=reactions)


def emoji(value):
    return t.ReactionTypeEmoji(value)


def expect(name, reactions, expected, *, bid=BID, chat=GID, mid=500, uid=ADMIN):
    bot.calls.clear()
    bot.messages.clear()
    try:
        fs.fs_handle_reaction(bot, bid, event(reactions, chat, mid, uid), 1)
        check(name, bot.calls == expected and not bot.messages)
    except Exception:
        traceback.print_exc()
        check(name, False)


def target(value, src=45):
    return [(UID, src, [] if value is None else [dict(type='emoji', emoji=value)])]


expect('обычная реакция на исходнике, сообщений нет', [emoji('❤️')], target('❤️'))
expect('повтор той же реакции не вызывает API и не шлёт сообщения', [emoji('❤️')], [])
expect('смена реакции на том же исходнике', [emoji('👍')], target('👍'))
expect('снятие реакции пустым списком', [], target(None))
expect('повторная установка после снятия', [emoji('👍')], target('👍'))

custom = t.ReactionTypeCustomEmoji('555999')
expect('custom emoji заменяется известным обычным', [custom], target('👌'))
check('custom emoji определён по ID стикера', bot.custom_calls == [['555999']])
bot.sticker_emoji = '💙'
expect('цветное сердце custom emoji заменяется обычным сердцем', [custom], target('❤️'))
bot.sticker_emoji = None
expect('custom emoji без известного аналога пропускается', [custom], [])
bot.sticker_emoji = '🧬'
expect('custom emoji с неизвестным аналогом пропускается', [custom], [])
expect('недоступный обычный эмодзи заменяется близким', [emoji('😀')], target('😁'))
expect('вариант эмодзи с цветом кожи заменяется обычным', [emoji('👍🏽')], target('👍'))
expect('неизвестный обычный эмодзи пропускается', [emoji('🧬')], [])
expect('платная реакция пропускается', [t.ReactionTypePaid()], [])

def invalid_heart_fire(reaction):
    if reaction and reaction[0].emoji == '❤‍🔥':
        raise api_error(400, 'Bad Request: REACTION_INVALID')

bot.fault = invalid_heart_fire
with patch.object(fs.log, 'warning') as logged:
    expect('REACTION_INVALID: попытка близкой обычной реакции без сообщений',
           [emoji('❤‍🔥')], target('❤‍🔥') + target('❤️'))
    check('отказ Telegram записан в журнал', logged.called)
bot.fault = None

for description, code in [('Bad Request: REACTION_INVALID', 400),
                          ('Bad Request: message to react not found', 400),
                          ('Forbidden: bot was blocked by the user', 403)]:
    def fail(reaction, code=code, description=description):
        raise api_error(code, description)
    bot.fault = fail
    with patch.object(fs.log, 'warning') as logged:
        expect(description + ': исключения и сообщений нет', [emoji('🔥')], target('🔥'))
        check(description + ': ошибка в журнале', logged.called)
bot.fault = None
expect('неудачная установка не запоминается: повтор успешен', [emoji('🔥')], target('🔥'))
bot.fault = lambda reaction: (_ for _ in ()).throw(api_error(400, 'message to react not found'))
with patch.object(fs.log, 'warning'):
    expect('ошибка снятия подавляется', [], target(None))
bot.fault = None
expect('после ошибки снятие можно повторить', [], target(None))

with patch.object(bot, 'get_custom_emoji_stickers', side_effect=api_error(400, 'sticker not found')):
    with patch.object(fs.log, 'warning') as logged:
        expect('ошибка получения custom emoji подавляется', [custom], [])
        check('ошибка custom emoji в журнале', logged.called)

# Настоящая фиксация склейки: все исходники есть в обратном поиске,
# а основной mapping указывает на последнее сообщение.
rows = []
for src in (70, 71, 72):
    msg = t.Message.de_json(dict(message_id=src, date=int(time.time()), text='часть',
        chat=dict(id=UID, type='private'),
        **{'from': dict(id=UID, is_bot=False, first_name='Клиент')}))
    rid = db.db_delivery_enqueue(BID, msg, json.dumps(msg.json), 0)
    db.db_delivery_update([rid], status='pending')
rows = db.db_delivery_rows(BID)
db.db_delivery_finish(rows, [(GID, 501, 10, [70, 71, 72])])
expect('склейка: реакция только на последнее из трёх сообщений',
       [emoji('👍')], target('👍', 72), mid=501)
expect('склейка: снятие только на последнем исходнике', [], target(None, 72), mid=501)

expect('личный режим: реакция на связанный исходник', [emoji('👍')], target('👍', 55),
       bid=PRIVATE_BID, chat=ADMIN, mid=600)
expect('личный режим: смена реакции', [emoji('🔥')], target('🔥', 55),
       bid=PRIVATE_BID, chat=ADMIN, mid=600)
expect('личный режим: снятие реакции', [], target(None, 55),
       bid=PRIVATE_BID, chat=ADMIN, mid=600)
expect('личный режим без связи: ничего не происходит', [emoji('👍')], [],
       bid=PRIVATE_BID, chat=ADMIN, mid=999)
expect('чужой бот: mapping не используется', [emoji('👍')], [],
       bid=PRIVATE_BID, chat=GID)
expect('не админ в группе: ничего не происходит', [emoji('👍')], [], uid=UID)
expect('админ в чужой личке: ничего не происходит', [emoji('👍')], [],
       bid=PRIVATE_BID, chat=ADMIN, mid=600, uid=UID)
expect('реакция самого бота игнорируется', [emoji('👍')], [], uid=1)

expect('обратное направление: реакция пользователя на копии в теме', [emoji('🤔')],
       [(GID, 500, [dict(type='emoji', emoji='🤔')])], chat=UID, mid=45, uid=UID)
expect('обратное направление: снятие остаётся без изменений', [], [], chat=UID, mid=45, uid=UID)
expect('обратное направление: custom emoji передаётся без замены', [custom],
       [(GID, 500, [dict(type='custom_emoji', custom_emoji_id='555999')])], chat=UID, mid=45, uid=UID)

check('весь тест выполнен без сообщений пользователю', not bot.messages)
print(f'ИТОГО: {len(OK)} OK, {len(FAIL)} FAIL')
sys.exit(1 if FAIL else 0)
