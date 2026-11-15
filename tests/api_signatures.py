# -*- coding: utf-8 -*-
"""Сверка используемых Telegram API с реальными сигнатурами установленной telebot.
Ловит рассинхрон код↔библиотека, который не виден статическим анализом.
Запуск: py tests/api_signatures.py"""
import inspect
import sys
import telebot
from telebot.types import ReplyParameters, MessageReactionUpdated, ReactionTypeEmoji, ReactionTypeCustomEmoji

PASS, FAIL = [], []
def chk(name, cond, extra=''):
    (PASS if cond else FAIL).append(name)
    print(('  [OK]   ' if cond else '  [FAIL] ') + name + ('  ' + str(extra) if not cond else ''))

api = telebot.TeleBot.__dict__
used = ['create_forum_topic', 'edit_forum_topic', 'reopen_forum_topic', 'close_forum_topic',
        'copy_messages', 'forward_messages', 'copy_message', 'forward_message',
        'set_message_reaction', 'pin_chat_message', 'send_media_group',
        'edit_message_reply_markup', 'delete_webhook', 'delete_message',
        'edit_message_text', 'send_message', 'send_photo', 'send_video', 'send_document',
        'send_audio', 'send_voice', 'send_video_note', 'send_sticker', 'send_animation',
        'send_contact', 'send_location', 'send_venue', 'send_poll', 'answer_callback_query',
        'get_chat', 'get_chat_member', 'get_me', 'process_new_updates', 'stop_polling',
        'infinity_polling', 'set_webhook']
for m in used:
    chk(f'TeleBot.{m} существует', m in api)

sig_checks = [
    ('create_forum_topic', ['chat_id', 'name']),
    ('edit_forum_topic', ['chat_id', 'message_thread_id']),
    ('reopen_forum_topic', ['chat_id', 'message_thread_id']),
    ('set_message_reaction', ['chat_id', 'message_id', 'reaction']),
    ('copy_messages', ['chat_id', 'from_chat_id', 'message_ids']),
    ('forward_messages', ['chat_id', 'from_chat_id', 'message_ids']),
    ('pin_chat_message', ['chat_id', 'message_id']),
]
for m, want in sig_checks:
    params = list(inspect.signature(api[m]).parameters)
    missing = [p for p in want if p not in params]
    chk(f'{m}{tuple(want)}', not missing, f'нет {missing}')

rp = ReplyParameters(message_id=1, allow_sending_without_reply=True)
chk('ReplyParameters(message_id, allow_sending_without_reply)', rp.message_id == 1)
mr = MessageReactionUpdated(chat=None, message_id=1, date=0, old_reaction=[], new_reaction=[])
chk('MessageReactionUpdated(user, actor_chat, new_reaction)',
    hasattr(mr, 'user') and hasattr(mr, 'actor_chat') and hasattr(mr, 'new_reaction'))
chk('ReactionTypeEmoji.emoji', hasattr(ReactionTypeEmoji('👍'), 'emoji'))
chk('ReactionTypeCustomEmoji.custom_emoji_id',
    hasattr(ReactionTypeCustomEmoji('123'), 'custom_emoji_id'))
chk('message_reaction_handler существует', hasattr(telebot.TeleBot, 'message_reaction_handler'))

print(f'ИТОГО: {len(PASS)} OK, {len(FAIL)} FAIL')
sys.exit(1 if FAIL else 0)
