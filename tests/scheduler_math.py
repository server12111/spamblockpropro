# -*- coding: utf-8 -*-
"""Property-тест математики недельного планировщика: 1000 случайных дат →
всегда понедельник 00:05 по таймзоне статистики, в будущем, не дальше 8 дней.
Плюс границы недели и roundtrip таймзоны. Запуск: py tests/scheduler_math.py"""
import os, sys, tempfile, random
os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='wkl_')
os.environ['FS_STATS_TZ'] = '3'
PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJ)
from datetime import datetime, timedelta
from forum_support import _next_fire, _utc_boundary, _tz, fs_weekly_report_text
from database import init_db
init_db()

PASS, FAIL = [], []
def chk(name, cond, extra=''):
    (PASS if cond else FAIL).append(name)
    print(('  [OK]   ' if cond else '  [FAIL] ') + name + ('  ' + str(extra) if not cond else ''))

random.seed(42)
base = datetime(2026, 1, 1)
bad = 0
for i in range(1000):
    dt = (base + timedelta(minutes=random.randint(0, 365*24*60))).replace(tzinfo=_tz())
    nf = _next_fire(dt)
    if nf.weekday() != 0 or (nf.hour, nf.minute) != (0, 5): bad += 1
    if nf <= dt: bad += 1
    if nf - dt > timedelta(days=8): bad += 1
chk('_next_fire: 1000 дат → Пн 00:05, в будущем, ≤8д', bad == 0, f'нарушений {bad}')

for label, dt, max_days in [
    ('Пн 00:00 → через 5 мин', datetime(2026, 6, 1, 0, 0), 0.01),
    ('Пн 00:04 → через 1 мин', datetime(2026, 6, 1, 0, 4), 0.01),
    ('Пн 00:06 → через неделю', datetime(2026, 6, 1, 0, 6), 7.01),
    ('Вс 23:00 → через 1ч5м', datetime(2026, 5, 31, 23, 0), 0.06),
]:
    dtw = dt.replace(tzinfo=_tz())
    nf = _next_fire(dtw)
    chk(f'{label}', nf.weekday() == 0 and (nf.hour, nf.minute) == (0, 5)
        and 0 < (nf - dtw).total_seconds() / 86400 <= max_days, nf)

b = _utc_boundary(datetime(2026, 6, 8, 0, 5).replace(tzinfo=_tz()))
chk('_utc_boundary(Пн 00:05 UTC+3) = Вс 21:05 UTC', b == '2026-06-07 21:05:00', b)

txt = fs_weekly_report_text(1, datetime(2026, 6, 1).replace(tzinfo=_tz()),
                               datetime(2026, 6, 8).replace(tzinfo=_tz()))
chk('fs_weekly_report_text: период корректен', '01.06.2026' in txt and '07.06.2026' in txt)

print(f'ИТОГО: {len(PASS)} OK, {len(FAIL)} FAIL')
sys.exit(1 if FAIL else 0)
