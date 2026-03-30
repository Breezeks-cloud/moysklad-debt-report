#!/usr/bin/env python3
"""
МойСклад → Задолженность перед клиентами
Шаг 3: Выгрузка ВСЕХ листов в Google Таблицу с числовыми данными.

Обновляет 8 листов: Сводка, Бризеры, Товары (все), Детализация,
Закрытые с долгом, _Справочник, _API_Клиенты, _API_Позиции.

Запуск: python3 03_upload_gsheets.py
"""

import os, pickle, json, time
from datetime import datetime
from collections import defaultdict

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    print('ERROR: pip install gspread google-auth')
    exit(1)

CACHE_PATH       = os.environ.get('CACHE_PATH', '/tmp/moysklad_report_v4.pkl')
SPREADSHEET_ID   = os.environ.get('SPREADSHEET_ID', '')
CREDENTIALS_FILE = os.environ.get('GOOGLE_CREDENTIALS_FILE',
                       os.path.join(os.path.dirname(__file__), 'credentials.json'))
CREDENTIALS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')

SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

ALL_SHEETS = ['Сводка', 'Бризеры', 'Товары (все)', 'Детализация',
              'Закрытые с долгом',
              '_Справочник', '_API_Клиенты', '_API_Позиции']

# ── Colors (RGB 0–1) ─────────────────────────────────────────────────────────
W  = {'red': 1, 'green': 1, 'blue': 1}
DK = {'red': 0.1, 'green': 0.1, 'blue': 0.1}
C  = {
    'hdr':   {'red': 0.169, 'green': 0.298, 'blue': 0.494},
    'blue':  {'red': 0.890, 'green': 0.945, 'blue': 0.992},
    'green': {'red': 0.910, 'green': 0.961, 'blue': 0.910},
    'orange':{'red': 1.000, 'green': 0.953, 'blue': 0.878},
    'grey':  {'red': 0.961, 'green': 0.969, 'blue': 0.980},
    'total': {'red': 0.839, 'green': 0.894, 'blue': 0.941},
    'warn':  {'red': 1.000, 'green': 0.922, 'blue': 0.922},
}
CAT_C = {'Бризер': C['green'], 'Сплит/Кондиционер': C['orange'],
         'Прочее': C['grey'], 'Услуга': W}

RUB = {'type': 'NUMBER', 'pattern': '#,##0.00 "₽"'}
QTY = {'type': 'NUMBER', 'pattern': '#,##0'}
PCT = {'type': 'NUMBER', 'pattern': '0.0%'}

# ── Фильтры позиций ───────────────────────────────────────────────────────────
# Запчасти/сервисные позиции — исключаются из вкладки «Бризеры»
_BREEZER_SERVICE_KW = [
    'бачок', 'подшипник', 'предохранител', 'адаптер питания',
    'плата управления', 'плата питания', 'плата wifi', 'управляющая плата',
    'материнская плата', 'плата ', 'платы ', 'кабель питания', 'сетевой кабель',
    'вилка', 'разъём', 'разъем', 'зарядн',
]

# Нетоварные позиции — полностью исключаются из всех расчётов отчёта
_EXCLUDE_MISC_KW = [
    'доплата', 'окраска бризера', 'окраска', 'платный ремонт atmeex',
    'ремонт atmeex', 'платный ремонт',
]


def _is_breezer_service(name: str) -> bool:
    """True, если позиция — запчасть/сервисный компонент Бризера (не сам аппарат)."""
    nl = name.lower()
    return any(kw in nl for kw in _BREEZER_SERVICE_KW)


def _is_excluded_misc(name: str) -> bool:
    """True, если позицию нужно полностью исключить из отчёта."""
    nl = name.lower()
    return any(kw in nl for kw in _EXCLUDE_MISC_KW)


def _filter_results(results):
    """Убрать нетоварные позиции (доплата, окраска, ремонт) из всех расчётов."""
    return [r for r in results if not _is_excluded_misc(r.get('item_name', ''))]


def get_anomaly_clients(clients):
    """Клиенты с положительным балансом и закрытыми заказами покупателя (аномалии).
    Только реальные клиенты с closed_orders — поставщики/подрядчики исключены на этапе сбора данных."""
    result = {}
    for k, v in clients.items():
        if v.get('closed_orders') and v.get('balance', 0) > 0:
            result[k] = v
    return result


# ── Auth ──────────────────────────────────────────────────────────────────────
def auth():
    if CREDENTIALS_JSON:
        creds = Credentials.from_service_account_info(
            json.loads(CREDENTIALS_JSON), scopes=SCOPES)
    elif os.path.exists(CREDENTIALS_FILE):
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    else:
        raise FileNotFoundError(f'credentials.json not found: {CREDENTIALS_FILE}')
    return gspread.authorize(creds)


# ── Formatting helpers ────────────────────────────────────────────────────────
def _rpt(sid, r, c, nr, nc, bg=W, bold=False, sz=10, fg=DK,
         ha='LEFT', wrap=False, nf=None):
    uf = {
        'backgroundColor': bg,
        'textFormat': {'bold': bold, 'fontSize': sz, 'foregroundColor': fg},
        'horizontalAlignment': ha,
        'wrapStrategy': 'WRAP' if wrap else 'CLIP',
    }
    flds = 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,wrapStrategy'
    if nf:
        uf['numberFormat'] = nf
        flds += ',numberFormat'
    flds += ')'
    return {'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': r, 'endRowIndex': r + nr,
                  'startColumnIndex': c, 'endColumnIndex': c + nc},
        'cell': {'userEnteredFormat': uf}, 'fields': flds}}


def _cw(sid, c, px):
    return {'updateDimensionProperties': {
        'range': {'sheetId': sid, 'dimension': 'COLUMNS',
                  'startIndex': c, 'endIndex': c + 1},
        'properties': {'pixelSize': px}, 'fields': 'pixelSize'}}


def _frz(sid, rows=1):
    return {'updateSheetProperties': {
        'properties': {'sheetId': sid,
                       'gridProperties': {'frozenRowCount': rows}},
        'fields': 'gridProperties.frozenRowCount'}}


def _merge(sid, r1, c1, r2, c2):
    return {'mergeCells': {
        'range': {'sheetId': sid, 'startRowIndex': r1, 'endRowIndex': r2,
                  'startColumnIndex': c1, 'endColumnIndex': c2},
        'mergeType': 'MERGE_ALL'}}


def _unmerge(sid):
    return {'unmergeCells': {
        'range': {'sheetId': sid, 'startRowIndex': 0, 'endRowIndex': 3000,
                  'startColumnIndex': 0, 'endColumnIndex': 30}}}


def _hdr(sid, ncols):
    return _rpt(sid, 0, 0, 1, ncols, bg=C['hdr'], bold=True, fg=W, ha='CENTER',
                wrap=True)


def _cp(name, pref):
    r = pref.get(name, {})
    return max(r.get('buy_price', 0), r.get('kit_price', 0))


def _full_reset(sid, R, nrows=2000, ncols=20):
    """Clear ALL old merges, frozen rows and formatting before writing new data."""
    R.append(_unmerge(sid))
    # Сброс замороженных строк/столбцов (исправляет «съехавшую» таблицу)
    R.append({'updateSheetProperties': {
        'properties': {'sheetId': sid,
                       'gridProperties': {'frozenRowCount': 0, 'frozenColumnCount': 0}},
        'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'}})
    R.append(_rpt(sid, 0, 0, nrows, ncols, bg=W, bold=False, sz=10, fg=DK, ha='LEFT'))


def _borders(sid, r, c, nr, nc, style='SOLID', alpha=0.7):
    """Добавить границы для диапазона ячеек."""
    border = {
        'style': style,
        'colorStyle': {'rgbColor': {'red': 0.6, 'green': 0.6, 'blue': 0.6, 'alpha': alpha}},
    }
    thin = {
        'style': 'SOLID',
        'colorStyle': {'rgbColor': {'red': 0.75, 'green': 0.75, 'blue': 0.75, 'alpha': 0.5}},
    }
    return {'updateBorders': {
        'range': {'sheetId': sid, 'startRowIndex': r, 'endRowIndex': r + nr,
                  'startColumnIndex': c, 'endColumnIndex': c + nc},
        'top': border, 'bottom': border, 'left': border, 'right': border,
        'innerHorizontal': thin, 'innerVertical': thin,
    }}


def _auto_resize(sid, ncols):
    """Автоподбор ширины столбцов по содержимому."""
    return {'autoResizeDimensions': {
        'dimensions': {'sheetId': sid, 'dimension': 'COLUMNS',
                       'startIndex': 0, 'endIndex': ncols}}}


def _write(ws, rows):
    ws.clear()
    ws.update(range_name='A1', values=rows, value_input_option='RAW')


# ── _API_Позиции ─────────────────────────────────────────────────────────────
def up_positions(ws, results, R, sid):
    H = ['Клиент', 'Код клиента', 'Тел.', 'Заказ', 'Наименование товара',
         'Кол-во', 'Долг аллоц., ₽', 'Категория', 'Статус']
    data = [H]
    for r in results:
        data.append([r.get('client', ''), r.get('client_code', ''),
                     r.get('client_phone', ''), r.get('order_name', ''),
                     r.get('item_name', ''), r.get('qty', 0),
                     round(r.get('debt_alloc', 0), 2),
                     r.get('category', ''), r.get('status', '')])
    _full_reset(sid, R)
    _write(ws, data)
    n = len(results)
    R.append(_hdr(sid, len(H)))
    if n:
        R.append(_rpt(sid, 1, 5, n, 1, ha='CENTER', nf=QTY))
        R.append(_rpt(sid, 1, 6, n, 1, nf=RUB))
        R.append(_borders(sid, 0, 0, n + 1, len(H)))
    R.append(_frz(sid))
    R.append(_auto_resize(sid, len(H)))
    for i, px in enumerate([220, 90, 130, 130, 320, 70, 130, 150, 90]):
        R.append(_cw(sid, i, px))


# ── _API_Клиенты ─────────────────────────────────────────────────────────────
def up_clients_raw(ws, clients, R, sid):
    H = ['Клиент', 'Код', 'Тел.', 'Тип', 'Баланс, ₽',
         'Долг, ₽', 'Заказы', 'Заказов', 'Статус']
    data = [H]
    for info in clients.values():
        data.append([
            info.get('name', ''), info.get('code', ''), info.get('phone', ''),
            'Юр. лицо' if info.get('companyType') == 'legal' else 'Физ. лицо',
            round(info.get('balance', 0), 2), round(info.get('debt', 0), 2),
            ', '.join(info.get('orders', [])), len(info.get('orders', [])),
            info.get('status', '')])
    _full_reset(sid, R)
    _write(ws, data)
    n = len(clients)
    R.append(_hdr(sid, len(H)))
    if n:
        R.append(_rpt(sid, 1, 4, n, 1, nf=RUB))
        R.append(_rpt(sid, 1, 5, n, 1, nf=RUB, bold=True))
        R.append(_rpt(sid, 1, 7, n, 1, ha='CENTER', nf=QTY))
        R.append(_borders(sid, 0, 0, n + 1, len(H)))
    R.append(_frz(sid))
    R.append(_auto_resize(sid, len(H)))
    for i, px in enumerate([240, 80, 140, 80, 110, 110, 300, 80, 80]):
        R.append(_cw(sid, i, px))


# ── _Справочник ──────────────────────────────────────────────────────────────
def up_spravochnik(ws, pref, R, sid):
    H = ['Наименование', 'Категория', 'Производитель', 'Модель',
         'buyPrice, ₽', 'kitPrice, ₽', 'Себест., ₽']
    data = [H]
    for name, info in sorted(pref.items()):
        bp, kp = info.get('buy_price', 0), info.get('kit_price', 0)
        data.append([name, info.get('category', ''), info.get('mfr', ''),
                     info.get('model', ''), bp, kp, max(bp, kp)])
    _full_reset(sid, R)
    _write(ws, data)
    n = len(pref)
    R.append(_hdr(sid, len(H)))
    if n:
        R.append(_rpt(sid, 1, 4, n, 1, nf=RUB))
        R.append(_rpt(sid, 1, 5, n, 1, nf=RUB))
        R.append(_rpt(sid, 1, 6, n, 1, nf=RUB, bold=True))
        R.append(_borders(sid, 0, 0, n + 1, len(H)))
    R.append(_frz(sid))
    R.append(_auto_resize(sid, len(H)))
    for i, px in enumerate([360, 140, 110, 140, 110, 110, 130]):
        R.append(_cw(sid, i, px))


# ── Сводка ────────────────────────────────────────────────────────────────────
def up_summary(ws, clients, results, pref, gen_at, R, sid):
    today = gen_at[:10]
    act = {k: v for k, v in clients.items() if v.get('status') == 'Активный'}
    # Аномалии — клиенты с closed_orders и balance > 0 (та же логика, что «Закрытые с долгом»)
    clo = get_anomaly_clients(clients)
    a_pos = [r for r in results if r.get('status') == 'Активный']
    # Позиции аномальных клиентов (по client_id)
    anom_ids = set(clo.keys())
    c_pos = [r for r in results if r.get('client_id', '') in anom_ids]

    total_debt = sum(v.get('debt', 0) for v in act.values())
    cats = ['Бризер', 'Сплит/Кондиционер', 'Прочее', 'Услуга']
    cq = {c: sum(r.get('qty', 0) for r in a_pos if r.get('category') == c) for c in cats}
    cd = {c: sum(r.get('debt_alloc', 0) for r in a_pos if r.get('category') == c) for c in cats}
    cc = {}
    for c in cats:
        if c == 'Услуга':
            cc[c] = 0
        else:
            cc[c] = sum(r.get('qty', 0) * _cp(r.get('item_name', ''), pref)
                        for r in a_pos if r.get('category') == c)
    total_cost = sum(cc.get(c, 0) for c in ['Бризер', 'Сплит/Кондиционер', 'Прочее'])
    total_qty = sum(r.get('qty', 0) for r in a_pos)

    # Аномалии: считаем баланс (фактически переплачено), а не долг по позициям
    c_debt = sum(v.get('balance', 0) for v in clo.values())
    c_qty = sum(r.get('qty', 0) for r in c_pos)
    c_cost = sum(r.get('qty', 0) * _cp(r.get('item_name', ''), pref)
                 for r in c_pos if r.get('category') != 'Услуга')
    c_bqty = sum(r.get('qty', 0) for r in c_pos if r.get('category') == 'Бризер')
    c_sqty = sum(r.get('qty', 0) for r in c_pos if r.get('category') == 'Сплит/Кондиционер')

    rows = []
    rows.append([f'Отчёт: Задолженность перед клиентами  |  {today}',
                 '', '', '', '', '', '', ''])                                       # 0
    rows.append([f'Период: 01.01.2023 — {today}  |  '
                 f'Долг = фактически оплачено − отгружено',
                 '', '', '', '', '', '', ''])                                       # 1
    rows.append(['Исключены: Микроклиматика, ИП Гончаров, Бризекс, тестовые',
                 '', '', '', '', '', '', ''])                                       # 2
    rows.append([''])                                                               # 3
    rows.append(['АКТИВНЫЕ КЛИЕНТЫ (без аномалий)', '', '', '', '', '', '', ''])    # 4
    rows.append(['Клиентов с задолженностью', '', len(act)])                        # 5
    rows.append(['Общая задолженность', '', round(total_debt, 2)])                  # 6
    rows.append(['Общая себестоимость резерва', '', round(total_cost, 2)])           # 7
    rows.append(['Устройств в резерве (всего)', '', total_qty])                     # 8
    rows.append(['  Бризеров', '', cq.get('Бризер', 0)])                            # 9
    rows.append(['  Сплит-систем / кондиционеров', '', cq.get('Сплит/Кондиционер', 0)])  # 10
    rows.append(['  Прочих товаров', '', cq.get('Прочее', 0)])                      # 11
    rows.append([''])                                                               # 12
    rows.append(['РАЗБИВКА ПО КАТЕГОРИЯМ (активные)', '', '', '', '', '', '', ''])   # 13
    rows.append(['', 'Категория', 'Кол-во', 'Долг, ₽',
                 'Себестоимость, ₽', 'Доля долга'])                                 # 14

    labels = [('Бризеры', 'Бризер'), ('Сплит / Кондиционеры', 'Сплит/Кондиционер'),
              ('Прочие товары', 'Прочее'), ('Услуги', 'Услуга')]
    for lab, cat in labels:
        d = cd.get(cat, 0)
        rows.append(['', lab, cq.get(cat, 0), round(d, 2),
                     round(cc.get(cat, 0), 2) if cat != 'Услуга' else '',
                     round(d / total_debt, 4) if total_debt > 0 else 0])            # 15-18
    rows.append(['', 'ИТОГО', total_qty, round(total_debt, 2),
                 round(total_cost, 2), 1.0])                                        # 19
    rows.append([''])                                                               # 20
    rows.append(['АНОМАЛИИ (закрытые заказы — НЕ входят в долг выше)',
                 '', '', '', '', '', '', ''])                                       # 21
    rows.append(['Клиентов', '', len(clo)])                                         # 22
    rows.append(['Задолженность', '', round(c_debt, 2)])                            # 23
    rows.append(['Себестоимость', '', round(c_cost, 2)])                            # 24
    rows.append(['Устройств', '', c_qty])                                           # 25
    rows.append(['  Бризеров', '', c_bqty])                                         # 26
    rows.append(['  Сплит / Кондиционеров', '', c_sqty])                            # 27
    rows.append([''])                                                               # 28
    rows.append(['ТОП-10 должников (активные)', '', '', '', '', '', '', ''])         # 29
    rows.append(['№', 'Клиент', 'Код', 'Тел.', 'Тип',
                 'Долг, ₽', 'Баланс, ₽', 'Заказов'])                               # 30

    sorted_a = sorted(act.items(), key=lambda x: -x[1].get('debt', 0))
    for i, (_, info) in enumerate(sorted_a[:10], 1):
        rows.append([
            i, info.get('name', ''), info.get('code', ''), info.get('phone', ''),
            'Юр. лицо' if info.get('companyType') == 'legal' else 'Физ. лицо',
            round(info.get('debt', 0), 2), round(info.get('balance', 0), 2),
            len(info.get('orders', []))])                                            # 31-40

    _full_reset(sid, R)
    _write(ws, rows)

    TB = {'red': 0.084, 'green': 0.396, 'blue': 0.753}
    TW = {'red': 0.902, 'green': 0.318, 'blue': 0.0}

    # Title block
    R.append(_merge(sid, 0, 0, 1, 8))
    R.append(_rpt(sid, 0, 0, 1, 8, bg=C['hdr'], bold=True, sz=14, fg=W, ha='CENTER'))
    R.append(_rpt(sid, 1, 0, 1, 8, bg=C['blue']))
    R.append(_rpt(sid, 2, 0, 1, 8, bg=W))

    # Active section
    R.append(_merge(sid, 4, 0, 5, 8))
    R.append(_rpt(sid, 4, 0, 1, 8, bg=C['blue'], bold=True, sz=12, fg=TB))
    for r in range(5, 12):
        bg = C['grey'] if r % 2 == 0 else W
        R.append(_rpt(sid, r, 0, 1, 3, bg=bg))
    R.append(_rpt(sid, 6, 2, 1, 1, bg=C['grey'], bold=True, nf=RUB))
    R.append(_rpt(sid, 7, 2, 1, 1, bold=True, nf=RUB))
    R.append(_rpt(sid, 8, 2, 1, 1, bg=C['grey'], bold=True, nf=QTY))

    # Category table
    R.append(_merge(sid, 13, 0, 14, 8))
    R.append(_rpt(sid, 13, 0, 1, 8, bg=C['blue'], bold=True, sz=12, fg=C['hdr']))
    R.append(_rpt(sid, 14, 0, 1, 6, bg=C['hdr'], bold=True, fg=W, ha='CENTER'))
    for i, (_, cat) in enumerate(labels):
        r = 15 + i
        bg = CAT_C.get(cat, W)
        R.append(_rpt(sid, r, 0, 1, 2, bg=bg, bold=True))
        R.append(_rpt(sid, r, 2, 1, 1, bg=bg, bold=True, ha='CENTER', nf=QTY))
        R.append(_rpt(sid, r, 3, 1, 1, bg=bg, bold=True, nf=RUB))
        R.append(_rpt(sid, r, 4, 1, 1, bg=bg, nf=RUB))
        R.append(_rpt(sid, r, 5, 1, 1, bg=bg, ha='CENTER', nf=PCT))
    R.append(_rpt(sid, 19, 0, 1, 2, bg=C['total'], bold=True))
    R.append(_rpt(sid, 19, 2, 1, 1, bg=C['total'], bold=True, ha='CENTER', nf=QTY))
    R.append(_rpt(sid, 19, 3, 1, 1, bg=C['total'], bold=True, nf=RUB))
    R.append(_rpt(sid, 19, 4, 1, 1, bg=C['total'], bold=True, nf=RUB))
    R.append(_rpt(sid, 19, 5, 1, 1, bg=C['total'], bold=True, ha='CENTER', nf=PCT))

    # Anomalies
    R.append(_merge(sid, 21, 0, 22, 8))
    R.append(_rpt(sid, 21, 0, 1, 8, bg=C['warn'], bold=True, sz=12, fg=TW))
    for r in range(22, 28):
        R.append(_rpt(sid, r, 0, 1, 3))
    R.append(_rpt(sid, 23, 2, 1, 1, bold=True, fg=TW, nf=RUB))
    R.append(_rpt(sid, 24, 2, 1, 1, fg=TW, nf=RUB))

    # Top-10
    R.append(_merge(sid, 29, 0, 30, 8))
    R.append(_rpt(sid, 29, 0, 1, 8, bg=C['blue'], bold=True, sz=12, fg=C['hdr']))
    R.append(_rpt(sid, 30, 0, 1, 8, bg=C['hdr'], bold=True, fg=W, ha='CENTER'))
    for i in range(min(10, len(sorted_a))):
        r = 31 + i
        bg = C['grey'] if i % 2 == 1 else W
        R.append(_rpt(sid, r, 0, 1, 8, bg=bg))
        R.append(_rpt(sid, r, 0, 1, 1, bg=bg, ha='CENTER'))
        R.append(_rpt(sid, r, 5, 1, 1, bg=bg, bold=True, nf=RUB))
        R.append(_rpt(sid, r, 6, 1, 1, bg=bg, nf=RUB))

    R.append(_frz(sid, 1))
    R.append(_auto_resize(sid, 8))
    for i, px in enumerate([300, 80, 160, 100, 150, 100, 80, 80]):
        R.append(_cw(sid, i, px))


# ── Бризеры ──────────────────────────────────────────────────────────────────
def up_breezers(ws, results, pref, R, sid):
    H = ['Производитель / Модель / Конфигурация', 'Кол-во, шт',
         'Долг, ₽', 'Себестоимость, ₽']

    active_b = [r for r in results
                if r.get('category') == 'Бризер' and r.get('status') == 'Активный'
                and not _is_breezer_service(r.get('item_name', ''))]
    by_mfr = defaultdict(lambda: defaultdict(list))
    for r in active_b:
        by_mfr[r.get('mfr') or 'Прочее'][r.get('model') or r.get('item_name', '')].append(r)

    rows = [H]
    fmt = []
    grand_qty, grand_debt, grand_cost = 0, 0.0, 0.0

    for mfr in sorted(by_mfr, key=lambda x: (x != 'AIRNANNY', x)):
        mq = sum(r.get('qty', 0) for ms in by_mfr[mfr].values() for r in ms)
        md = sum(r.get('debt_alloc', 0) for ms in by_mfr[mfr].values() for r in ms)
        mc = sum(r.get('qty', 0) * _cp(r.get('item_name', ''), pref)
                 for ms in by_mfr[mfr].values() for r in ms)
        grand_qty += mq; grand_debt += md; grand_cost += mc
        rows.append([mfr, mq, round(md, 2), round(mc, 2)])
        fmt.append((len(rows) - 1, 'mfr'))

        for model in sorted(by_mfr[mfr]):
            items = by_mfr[mfr][model]
            rows.append([f'   {model}',
                         sum(r.get('qty', 0) for r in items),
                         round(sum(r.get('debt_alloc', 0) for r in items), 2),
                         round(sum(r.get('qty', 0) * _cp(r.get('item_name', ''), pref)
                                   for r in items), 2)])
            fmt.append((len(rows) - 1, 'model'))

            cfgs = defaultdict(lambda: {'qty': 0, 'debt': 0.0, 'cost': 0.0})
            for r in items:
                n = r.get('item_name', '')
                cfgs[n]['qty'] += r.get('qty', 0)
                cfgs[n]['debt'] += r.get('debt_alloc', 0)
                cfgs[n]['cost'] += r.get('qty', 0) * _cp(n, pref)
            for cfg, t in sorted(cfgs.items()):
                rows.append([f'      {cfg}', t['qty'],
                             round(t['debt'], 2), round(t['cost'], 2)])
                fmt.append((len(rows) - 1, 'cfg'))

    total_ri = len(rows)
    rows.append(['ИТОГО', grand_qty, round(grand_debt, 2), round(grand_cost, 2)])

    _full_reset(sid, R)
    _write(ws, rows)
    R.append(_hdr(sid, 4))

    TB = {'red': 0.084, 'green': 0.396, 'blue': 0.753}
    for ri, kind in fmt:
        if kind == 'mfr':
            R.append(_rpt(sid, ri, 0, 1, 1, bg=C['hdr'], bold=True, sz=12, fg=W))
            R.append(_rpt(sid, ri, 1, 1, 1, bg=C['hdr'], bold=True, sz=12, fg=W,
                          ha='CENTER', nf=QTY))
            R.append(_rpt(sid, ri, 2, 1, 2, bg=C['hdr'], bold=True, sz=12, fg=W, nf=RUB))
        elif kind == 'model':
            R.append(_rpt(sid, ri, 0, 1, 1, bg=C['blue'], bold=True, sz=11, fg=TB))
            R.append(_rpt(sid, ri, 1, 1, 1, bg=C['blue'], bold=True, ha='CENTER', nf=QTY))
            R.append(_rpt(sid, ri, 2, 1, 2, bg=C['blue'], bold=True, nf=RUB))
        else:
            bg = C['grey'] if ri % 2 == 0 else W
            R.append(_rpt(sid, ri, 0, 1, 1, bg=bg))
            R.append(_rpt(sid, ri, 1, 1, 1, bg=bg, ha='CENTER', nf=QTY))
            R.append(_rpt(sid, ri, 2, 1, 2, bg=bg, nf=RUB))

    R.append(_rpt(sid, total_ri, 0, 1, 1, bg=C['total'], bold=True, sz=12))
    R.append(_rpt(sid, total_ri, 1, 1, 1, bg=C['total'], bold=True, sz=12,
                  ha='CENTER', nf=QTY))
    R.append(_rpt(sid, total_ri, 2, 1, 2, bg=C['total'], bold=True, sz=12, nf=RUB))

    R.append(_borders(sid, 0, 0, total_ri + 1, 4))
    R.append(_frz(sid, 1))
    R.append(_auto_resize(sid, 4))
    for i, px in enumerate([400, 100, 150, 170]):
        R.append(_cw(sid, i, px))


# ── Товары (все) ──────────────────────────────────────────────────────────────
def up_all_products(ws, results, pref, R, sid):
    H = ['Наименование товара', 'Категория', 'Кол-во', 'Долг, ₽', 'Себестоимость, ₽']
    active = [r for r in results if r.get('status') == 'Активный']
    agg = defaultdict(lambda: {'qty': 0, 'debt': 0.0, 'cat': ''})
    for r in active:
        n = r.get('item_name', '')
        agg[n]['qty'] += r.get('qty', 0)
        agg[n]['debt'] += r.get('debt_alloc', 0)
        agg[n]['cat'] = r.get('category', '')

    rows = [H]
    for name, info in sorted(agg.items()):
        rows.append([name, info['cat'], info['qty'],
                     round(info['debt'], 2),
                     round(info['qty'] * _cp(name, pref), 2)])

    _full_reset(sid, R)
    _write(ws, rows)
    n = len(agg)
    R.append(_hdr(sid, 5))
    if n:
        R.append(_rpt(sid, 1, 2, n, 1, ha='CENTER', nf=QTY))
        R.append(_rpt(sid, 1, 3, n, 1, nf=RUB))
        R.append(_rpt(sid, 1, 4, n, 1, nf=RUB))
        R.append(_borders(sid, 0, 0, n + 1, 5))
    R.append(_frz(sid, 1))
    R.append(_auto_resize(sid, 5))
    for i, px in enumerate([380, 160, 90, 140, 150]):
        R.append(_cw(sid, i, px))


# ── Детализация / Закрытые с долгом ───────────────────────────────────────────
def up_detail(ws, results, status, pref, R, sid):
    H = ['Клиент', 'Код', 'Тел.', 'Заказ', 'Наименование товара',
         'Категория', 'Кол-во', 'Долг аллоц., ₽', 'Себестоимость, ₽']
    filtered = [r for r in results if r.get('status') == status]
    rows = [H]
    for r in filtered:
        n, q = r.get('item_name', ''), r.get('qty', 0)
        rows.append([r.get('client', ''), r.get('client_code', ''),
                     r.get('client_phone', ''), r.get('order_name', ''),
                     n, r.get('category', ''), q,
                     round(r.get('debt_alloc', 0), 2),
                     round(q * _cp(n, pref), 2)])

    _full_reset(sid, R)
    _write(ws, rows)
    n = len(filtered)
    R.append(_hdr(sid, len(H)))
    if n:
        R.append(_rpt(sid, 1, 6, n, 1, ha='CENTER', nf=QTY))
        R.append(_rpt(sid, 1, 7, n, 1, nf=RUB))
        R.append(_rpt(sid, 1, 8, n, 1, nf=RUB))
        R.append(_borders(sid, 0, 0, n + 1, len(H)))
    R.append(_frz(sid, 1))
    R.append(_auto_resize(sid, len(H)))
    for i, px in enumerate([220, 80, 130, 130, 320, 150, 70, 130, 140]):
        R.append(_cw(sid, i, px))


# ── Закрытые с долгом (контрагенты с завершёнными заказами и нашим долгом) ──
def up_closed_clients(ws, clients, R, sid):
    # Только клиенты с реальными закрытыми заказами покупателя и положительным балансом.
    # Поставщики/подрядчики/наши юрлица исключены на этапе сбора данных (01_fetch_data.py).
    closed = {}
    for k, v in clients.items():
        co = v.get('closed_orders', [])
        if co and v.get('balance', 0) > 0:
            closed[k] = v

    H = ['Клиент', 'Код', 'Тел.', 'Тип', 'Баланс МС, ₽',
         'Закр. заказов', 'Заказы (закрытые)', 'Статусы']
    rows = [H]
    for info in sorted(closed.values(), key=lambda x: -x.get('balance', 0)):
        co = info.get('closed_orders', [])
        names = ', '.join(o.get('name', '') for o in co[:5])
        if len(co) > 5:
            names += f' (+{len(co) - 5})'
        states = ', '.join(sorted(set(o.get('state', '') for o in co))) if co else ''
        rows.append([
            info.get('name', ''), info.get('code', ''), info.get('phone', ''),
            'Юр. лицо' if info.get('companyType') == 'legal' else 'Физ. лицо',
            round(info.get('balance', 0), 2),
            len(co), names, states])
    _full_reset(sid, R)
    _write(ws, rows)
    n = len(closed)
    R.append(_hdr(sid, len(H)))
    if n:
        R.append(_rpt(sid, 1, 4, n, 1, nf=RUB, bold=True))
        R.append(_rpt(sid, 1, 5, n, 1, ha='CENTER', nf=QTY))
        R.append(_borders(sid, 0, 0, n + 1, len(H)))
    R.append(_frz(sid))
    R.append(_auto_resize(sid, len(H)))
    for i, px in enumerate([260, 80, 130, 80, 140, 90, 280, 200]):
        R.append(_cw(sid, i, px))


# ── Main ──────────────────────────────────────────────────────────────────────
def get_or_create(ss, title, idx):
    for ws in ss.worksheets():
        if ws.title == title:
            return ws
    return ss.add_worksheet(title=title, rows=2000, cols=20, index=idx)


if __name__ == '__main__':
    print(f'=== Google Sheets Upload [{datetime.now().strftime("%Y-%m-%d %H:%M")}] ===')

    if not os.path.exists(CACHE_PATH):
        print(f'ERROR: Cache not found: {CACHE_PATH}')
        exit(1)
    if not SPREADSHEET_ID:
        print('ERROR: SPREADSHEET_ID not set')
        exit(1)

    with open(CACHE_PATH, 'rb') as f:
        data = pickle.load(f)

    clients     = data['clients']
    results     = _filter_results(data['results'])  # убрать доплаты/окраски/ремонты
    product_ref = data.get('product_ref', {})
    gen_at      = data.get('generated_at', datetime.now().isoformat())

    print('Авторизация...')
    gc = auth()
    ss = gc.open_by_key(SPREADSHEET_ID)
    print(f'  Таблица: {ss.title}')

    sm = {}
    for i, t in enumerate(ALL_SHEETS):
        sm[t] = get_or_create(ss, t, i)
        print(f'  {t} (id={sm[t].id})')

    R = []

    steps = [
        ('_API_Позиции',      lambda: up_positions(sm['_API_Позиции'], results, R,
                                                    sm['_API_Позиции'].id)),
        ('_API_Клиенты',      lambda: up_clients_raw(sm['_API_Клиенты'], clients, R,
                                                      sm['_API_Клиенты'].id)),
        ('_Справочник',       lambda: up_spravochnik(sm['_Справочник'], product_ref, R,
                                                      sm['_Справочник'].id)),
        ('Сводка',            lambda: up_summary(sm['Сводка'], clients, results,
                                                  product_ref, gen_at, R,
                                                  sm['Сводка'].id)),
        ('Бризеры',           lambda: up_breezers(sm['Бризеры'], results, product_ref,
                                                   R, sm['Бризеры'].id)),
        ('Товары (все)',      lambda: up_all_products(sm['Товары (все)'], results,
                                                       product_ref, R,
                                                       sm['Товары (все)'].id)),
        ('Детализация',       lambda: up_detail(sm['Детализация'], results, 'Активный',
                                                 product_ref, R,
                                                 sm['Детализация'].id)),
        ('Закрытые с долгом', lambda: up_closed_clients(sm['Закрытые с долгом'],
                                                        clients, R,
                                                        sm['Закрытые с долгом'].id)),
    ]

    for name, fn in steps:
        print(f'  → {name}...')
        fn()
        time.sleep(0.5)

    if R:
        print(f'Форматирование ({len(R)} ops)...')
        for i in range(0, len(R), 100):
            ss.batch_update({'requests': R[i:i + 100]})
            time.sleep(0.5)

    existing = {ws.title for ws in ss.worksheets()}
    for extra in existing - set(ALL_SHEETS):
        try:
            ss.del_worksheet(ss.worksheet(extra))
            print(f'  Удалён лишний лист: {extra}')
        except Exception:
            pass

    ac = sum(1 for v in clients.values() if v.get('status') == 'Активный')
    ar = sum(1 for r in results if r.get('status') == 'Активный')
    print(f'\n✅ ГОТОВО')
    print(f'   https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}')
    print(f'   Клиентов: {ac} активных | Позиций: {ar} активных')
