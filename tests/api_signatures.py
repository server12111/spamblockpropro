# -*- coding: utf-8 -*-
"""Сверка используемых Telegram API с реальными сигнатурами установленной telebot.
Ловит рассинхрон код↔библиотека, который не виден статическим анализом.
Запуск: py tests/api_signatures.py"""
import inspect
import json
import sys
import telebot
from telebot.types import ReplyParameters, MessageReactionUpdated, ReactionTypeEmoji, ReactionTypeCustomEmoji
from telebot.types import (KeyboardButtonRequestChat, ChatAdministratorRights,
                           KeyboardButton, ReplyKeyboardMarkup, ChatShared, Message)

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
chk('my_chat_member_handler существует', hasattr(telebot.TeleBot, 'my_chat_member_handler'))

rights = ChatAdministratorRights(is_anonymous=False, can_manage_chat=True,
    can_delete_messages=True, can_manage_video_chats=False, can_restrict_members=False,
    can_promote_members=False, can_change_info=False, can_invite_users=False,
    can_pin_messages=True, can_manage_topics=True)
request = KeyboardButtonRequestChat(request_id=1, chat_is_channel=False, chat_is_forum=True,
    user_administrator_rights=rights, bot_administrator_rights=rights, request_title=True)
kb = ReplyKeyboardMarkup().add(KeyboardButton('Выбрать группу', request_chat=request,
    icon_custom_emoji_id='5443038326535759644', style='primary'))
button = json.loads(kb.to_json())['keyboard'][0][0]
shared = button['request_chat']
for key in ('chat_is_forum', 'request_title', 'user_administrator_rights', 'bot_administrator_rights'):
    chk('KeyboardButtonRequestChat.' + key + ' сериализуется', bool(shared.get(key)))
for key in ('can_manage_topics', 'can_pin_messages', 'can_delete_messages'):
    chk('ChatAdministratorRights.' + key + ' сериализуется', shared['bot_administrator_rights'].get(key) is True)
chk('KeyboardButton поддерживает премиум-иконку и стиль',
    button.get('icon_custom_emoji_id') == '5443038326535759644' and button.get('style') == 'primary')
msg = Message.de_json({'message_id': 1, 'date': 0, 'chat': {'id': 1, 'type': 'private'},
    'chat_shared': {'request_id': 1, 'chat_id': -1001234567890, 'title': 'Поддержка'}})
chk('Message.chat_shared разбирается с title и request_id',
    msg.content_type == 'chat_shared' and isinstance(msg.chat_shared, ChatShared)
    and msg.chat_shared.request_id == 1 and msg.chat_shared.title == 'Поддержка')

print(f'ИТОГО: {len(PASS)} OK, {len(FAIL)} FAIL')
sys.exit(1 if FAIL else 0)
