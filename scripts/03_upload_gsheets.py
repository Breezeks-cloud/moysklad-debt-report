#!/usr/bin/env python3
"""
МойСклад → Задолженность перед клиентами
Шаг 3: Выгрузка данных в Google Таблицу.

Требования:
    pip install gspread google-auth

Настройка:
    1. Создайте Service Account в Google Cloud Console
    2. Скачайте JSON-ключ → сохраните как credentials.json (рядом со скриптом)
    3. Создайте Google Таблицу и поделитесь ею с email из credentials.json (Editor)
    4. Скопируйте ID таблицы из URL → SPREADSHEET_ID ниже

Запуск: python3 03_upload_gsheets.py
"""

import os, pickle, json
from datetime import datetime
from collections import defaultdict

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    print('ERROR: Установите зависимости:')
    print('  pip install gspread google-auth')
    exit(1)

# ── Настройки ──────────────────────────────────────────────────────────────────
CACHE_PATH       = os.environ.get('CACHE_PATH', '/tmp/moysklad_report_v4.pkl')
SPREADSHEET_ID   = os.environ.get('SPREADSHEET_ID', '')          # ID вашей Google Таблицы
CREDENTIALS_FILE = os.environ.get('GOOGLE_CREDENTIALS_FILE',
                       os.path.join(os.path.dirname(__file__), 'credentials.json'))
# Для GitHub Actions — JSON-строка из секрета
CREDENTIALS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

# Цвета (RGB 0.0–1.0)
COLOR = {
    'dark_blue':   {'red': 0.169, 'green': 0.298, 'blue': 0.494},
    'light_blue':  {'red': 0.890, 'green': 0.945, 'blue': 0.992},
    'green':       {'red': 0.910, 'green': 0.961, 'blue': 0.910},
    'orange':      {'red': 1.000, 'green': 0.953, 'blue': 0.878},
    'grey':        {'red': 0.961, 'green': 0.969, 'blue': 0.980},
    'total':       {'red': 0.839, 'green': 0.894, 'blue': 0.941},
    'warn':        {'red': 1.000, 'green': 0.922, 'blue': 0.922},
    'white':       {'red': 1.000, 'green': 1.000, 'blue': 1.000},
}

CAT_COLOR = {
    'Бризер':            COLOR['green'],
    'Сплит/Кондиционер': COLOR['orange'],
    'Прочее':            COLOR['grey'],
    'Услуга':            COLOR['white'],
}

# ── Авторизация ────────────────────────────────────────────────────────────────
def get_client():
    if CREDENTIALS_JSON:
        cred_info = json.loads(CREDENTIALS_JSON)
        creds = Credentials.from_service_account_info(cred_info, scopes=SCOPES)
    elif os.path.exists(CREDENTIALS_FILE):
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    else:
        raise FileNotFoundError(
            f'Файл credentials.json не найден: {CREDENTIALS_FILE}\n'
            'Создайте Service Account и скачайте JSON-ключ.\n'
            'Инструкция: GOOGLE_SHEETS_SETUP.md'
        )
    return gspread.authorize(creds)

# ── Форматирование ────────────────────────────────────────────────────────────
def cell_fmt(bg=None, bold=False, size=10, color=None, halign='LEFT', wrap=False):
    fmt = {
        'textFormat': {
            'bold': bold,
            'fontSize': size,
            'foregroundColor': color or {'red': 0.1, 'green': 0.1, 'blue': 0.1},
        },
        'horizontalAlignment': halign,
        'wrapStrategy': 'WRAP' if wrap else 'CLIP',
    }
    if bg:
        fmt['backgroundColor'] = bg
    return fmt

def hdr_fmt():
    return cell_fmt(bg=COLOR['dark_blue'], bold=True, size=10,
                    color={'red': 1, 'green': 1, 'blue': 1}, halign='CENTER')

def batch_format(requests, sheet_id, row, col, rows, cols, fmt):
    requests.append({
        'repeatCell': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': row, 'endRowIndex': row + rows,
                'startColumnIndex': col, 'endColumnIndex': col + cols,
            },
            'cell': {'userEnteredFormat': fmt},
            'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,wrapStrategy)',
        }
    })

def set_col_width(requests, sheet_id, col, px):
    requests.append({
        'updateDimensionProperties': {
            'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS',
                      'startIndex': col, 'endIndex': col + 1},
            'properties': {'pixelSize': px},
            'fields': 'pixelSize',
        }
    })

def freeze_row(requests, sheet_id, rows=1, cols=0):
    requests.append({
        'updateSheetProperties': {
            'properties': {
                'sheetId': sheet_id,
                'gridProperties': {'frozenRowCount': rows, 'frozenColumnCount': cols},
            },
            'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount',
        }
    })

def merge_cells(requests, sheet_id, r1, c1, r2, c2):
    requests.append({
        'mergeCells': {
            'range': {'sheetId': sheet_id,
                      'startRowIndex': r1, 'endRowIndex': r2,
                      'startColumnIndex': c1, 'endColumnIndex': c2},
            'mergeType': 'MERGE_ALL',
        }
    })

def rub(v):
    try: return f'{float(v):,.0f} ₽'.replace(',', ' ')
    except: return '0 ₽'

def pct(v):
    try: return f'{float(v)*100:.1f}%'
    except: return '0%'

# ── Лист Сводка ────────────────────────────────────────────────────────────────
def write_summary(ws, clients, results, generated_at, requests, sheet_id):
    today = generated_at[:10]
    active_clients = {k: v for k, v in clients.items() if v.get('status') == 'Активный'}
    closed_clients = {k: v for k, v in clients.items() if v.get('status') == 'Закрытый'}
    active_pos = [r for r in results if r.get('status') == 'Активный']
    closed_pos = [r for r in results if r.get('status') == 'Закрытый']

    total_debt = sum(v.get('debt', 0) for v in active_clients.values())
    total_qty  = sum(r.get('qty', 0) for r in active_pos)

    cats = ['Бризер', 'Сплит/Кондиционер', 'Прочее', 'Услуга']
    cat_qty  = {c: sum(r.get('qty', 0) for r in active_pos if r.get('category') == c) for c in cats}
    cat_debt = {c: sum(r.get('debt_alloc', 0) for r in active_pos if r.get('category') == c) for c in cats}

    rows = []

    # Title
    rows.append([f'Отчёт: Задолженность перед клиентами  |  {today}'])
    rows.append([f'Период: 01.01.2023 — {today}  |  Долг = фактически оплачено − отгружено'])
    rows.append([''])

    # Active section header
    rows.append(['АКТИВНЫЕ КЛИЕНТЫ (без аномалий)'])
    rows.append(['Клиентов с задолженностью', '', str(len(active_clients))])
    rows.append(['Общая задолженность', '', rub(total_debt)])
    rows.append(['Устройств в резерве (всего)', '', str(total_qty)])
    rows.append(['  Бризеров', '', str(cat_qty.get('Бризер', 0))])
    rows.append(['  Сплит-систем / кондиционеров', '', str(cat_qty.get('Сплит/Кондиционер', 0))])
    rows.append(['  Прочих товаров', '', str(cat_qty.get('Прочее', 0))])
    rows.append([''])

    # Category table header
    rows.append(['РАЗБИВКА ПО КАТЕГОРИЯМ'])
    rows.append(['Категория', 'Кол-во, шт', 'Долг, ₽', 'Себестоимость, ₽', 'Доля долга'])

    cat_labels = [
        ('Бризеры',            'Бризер'),
        ('Сплит / Кондиционеры', 'Сплит/Кондиционер'),
        ('Прочие товары',       'Прочее'),
        ('Услуги',              'Услуга'),
    ]
    for label, cat in cat_labels:
        debt = cat_debt.get(cat, 0)
        share = pct(debt / total_debt) if total_debt > 0 else '0%'
        rows.append([label, str(cat_qty.get(cat, 0)), rub(debt), '—', share])

    rows.append(['ИТОГО', str(total_qty), rub(total_debt), '', '100%'])
    rows.append([''])

    # Anomalies
    rows.append(['АНОМАЛИИ (закрытые заказы — НЕ входят в задолженность выше)'])
    rows.append(['Клиентов', '', str(len(closed_clients))])
    rows.append(['Задолженность', '', rub(sum(v.get('debt', 0) for v in closed_clients.values()))])
    rows.append(['Устройств', '', str(sum(r.get('qty', 0) for r in closed_pos))])
    rows.append(['  Бризеров', '', str(sum(r.get('qty', 0) for r in closed_pos if r.get('category') == 'Бризер'))])
    rows.append([''])

    # Top-10
    rows.append(['ТОП-10 должников (активные)'])
    rows.append(['№', 'Клиент', 'Код', 'Тел.', 'Долг, ₽', 'Баланс, ₽', 'Заказов'])
    sorted_active = sorted(active_clients.items(), key=lambda x: -x[1].get('debt', 0))
    for idx, (aid, info) in enumerate(sorted_active[:10], 1):
        rows.append([
            str(idx), info.get('name', ''), info.get('code', ''), info.get('phone', ''),
            rub(info.get('debt', 0)), rub(info.get('balance', 0)), str(len(info.get('orders', []))),
        ])

    ws.clear()
    ws.update('A1', rows, value_input_option='RAW')

    # Formatting
    r = 0
    merge_cells(requests, sheet_id, r, 0, r+1, 8); batch_format(requests, sheet_id, r, 0, 1, 8, cell_fmt(bg=COLOR['dark_blue'], bold=True, size=13, color=COLOR['white'])); r+=1
    batch_format(requests, sheet_id, r, 0, 1, 8, cell_fmt(bg=COLOR['light_blue'])); r+=1
    r+=1
    merge_cells(requests, sheet_id, r, 0, r+1, 8); batch_format(requests, sheet_id, r, 0, 1, 8, cell_fmt(bg=COLOR['light_blue'], bold=True, size=12)); r+=1
    for _ in range(6): batch_format(requests, sheet_id, r, 0, 1, 3, cell_fmt(bg=COLOR['grey'] if r%2==0 else COLOR['white'])); r+=1
    r+=1
    merge_cells(requests, sheet_id, r, 0, r+1, 8); batch_format(requests, sheet_id, r, 0, 1, 8, cell_fmt(bg=COLOR['light_blue'], bold=True, size=12)); r+=1
    batch_format(requests, sheet_id, r, 0, 1, 5, hdr_fmt()); r+=1
    for label, cat in cat_labels:
        batch_format(requests, sheet_id, r, 0, 1, 5, cell_fmt(bg=CAT_COLOR.get(cat, COLOR['white']))); r+=1
    batch_format(requests, sheet_id, r, 0, 1, 5, cell_fmt(bg=COLOR['total'], bold=True)); r+=2
    merge_cells(requests, sheet_id, r, 0, r+1, 8); batch_format(requests, sheet_id, r, 0, 1, 8, cell_fmt(bg=COLOR['warn'], bold=True, size=12)); r+=1
    for _ in range(4): r+=1
    r+=1
    merge_cells(requests, sheet_id, r, 0, r+1, 8); batch_format(requests, sheet_id, r, 0, 1, 8, cell_fmt(bg=COLOR['light_blue'], bold=True, size=12)); r+=1
    batch_format(requests, sheet_id, r, 0, 1, 7, hdr_fmt()); r+=1
    for i in range(10):
        batch_format(requests, sheet_id, r, 0, 1, 7, cell_fmt(bg=COLOR['grey'] if i%2==1 else COLOR['white'])); r+=1

    freeze_row(requests, sheet_id, 1)
    for col, px in enumerate([300, 80, 160, 100, 150, 100, 80], 0):
        set_col_width(requests, sheet_id, col, px)

# ── Лист Детализация ───────────────────────────────────────────────────────────
def write_detail(ws, results, filter_status, requests, sheet_id):
    hdrs = ['Клиент', 'Код', 'Тел.', 'Заказ', 'Наименование товара',
            'Категория', 'Кол-во', 'Долг аллоц., ₽']
    rows = [hdrs]
    filtered = [r for r in results if r.get('status') == filter_status]
    for r in filtered:
        rows.append([
            r.get('client', ''), r.get('client_code', ''), r.get('client_phone', ''),
            r.get('order_name', ''), r.get('item_name', ''),
            r.get('category', ''), str(r.get('qty', 0)), rub(r.get('debt_alloc', 0)),
        ])
    ws.clear()
    ws.update('A1', rows, value_input_option='RAW')

    batch_format(requests, sheet_id, 0, 0, 1, len(hdrs), hdr_fmt())
    for i, r in enumerate(filtered, 1):
        cat = r.get('category', '')
        bg = CAT_COLOR.get(cat, COLOR['white'])
        if filter_status == 'Закрытый': bg = COLOR['warn']
        batch_format(requests, sheet_id, i, 0, 1, len(hdrs), cell_fmt(bg=bg))

    freeze_row(requests, sheet_id, 1)
    for col, px in enumerate([200, 80, 120, 120, 300, 140, 60, 120], 0):
        set_col_width(requests, sheet_id, col, px)

    ws.freeze(rows=1)

# ── Лист Бризеры ───────────────────────────────────────────────────────────────
def write_breezers(ws, results, requests, sheet_id):
    hdrs = ['Производитель / Модель / Конфигурация', 'Кол-во, шт', 'Долг, ₽']
    rows = [hdrs]
    fmt_map = []

    active_breezers = [r for r in results if r.get('category') == 'Бризер' and r.get('status') == 'Активный']
    by_mfr = defaultdict(lambda: defaultdict(list))
    for r in active_breezers:
        by_mfr[r.get('mfr') or 'Прочее'][r.get('model') or r.get('item_name', '')].append(r)

    for mfr in sorted(by_mfr.keys(), key=lambda x: (x != 'AIRNANNY', x)):
        mfr_qty  = sum(r.get('qty', 0) for models in by_mfr[mfr].values() for r in models)
        mfr_debt = sum(r.get('debt_alloc', 0) for models in by_mfr[mfr].values() for r in models)
        rows.append([f'  {mfr}', str(mfr_qty), rub(mfr_debt)])
        fmt_map.append(('mfr', len(rows)-1))

        for model in sorted(by_mfr[mfr].keys()):
            model_rows = by_mfr[mfr][model]
            m_qty  = sum(r.get('qty', 0) for r in model_rows)
            m_debt = sum(r.get('debt_alloc', 0) for r in model_rows)
            rows.append([f'    {model}', str(m_qty), rub(m_debt)])
            fmt_map.append(('model', len(rows)-1))

            cfg_totals = defaultdict(lambda: {'qty': 0, 'debt': 0})
            for r in model_rows:
                cfg_totals[r.get('item_name', '')]['qty']  += r.get('qty', 0)
                cfg_totals[r.get('item_name', '')]['debt'] += r.get('debt_alloc', 0)
            for cfg, totals in sorted(cfg_totals.items()):
                rows.append([f'      {cfg}', str(totals['qty']), rub(totals['debt'])])
                fmt_map.append(('cfg', len(rows)-1))

    ws.clear()
    ws.update('A1', rows, value_input_option='RAW')
    batch_format(requests, sheet_id, 0, 0, 1, 3, hdr_fmt())

    for kind, row_idx in fmt_map:
        if kind == 'mfr':
            batch_format(requests, sheet_id, row_idx, 0, 1, 3,
                         cell_fmt(bg=COLOR['dark_blue'], bold=True, size=12, color=COLOR['white']))
        elif kind == 'model':
            batch_format(requests, sheet_id, row_idx, 0, 1, 3,
                         cell_fmt(bg=COLOR['light_blue'], bold=True, size=11))
        else:
            bg = COLOR['grey'] if row_idx % 2 == 0 else COLOR['white']
            batch_format(requests, sheet_id, row_idx, 0, 1, 3, cell_fmt(bg=bg))

    freeze_row(requests, sheet_id, 1)
    for col, px in enumerate([380, 100, 140], 0):
        set_col_width(requests, sheet_id, col, px)

# ── Лист Клиенты ───────────────────────────────────────────────────────────────
def write_clients(ws, clients, requests, sheet_id):
    hdrs = ['Клиент', 'Код', 'Тел.', 'Тип', 'Баланс, ₽', 'Долг, ₽', 'Заказы', 'Кол-во заказов', 'Статус']
    rows = [hdrs]
    for aid, info in clients.items():
        rows.append([
            info.get('name', ''), info.get('code', ''), info.get('phone', ''),
            'Юр. лицо' if info.get('companyType') == 'legal' else 'Физ. лицо',
            rub(info.get('balance', 0)), rub(info.get('debt', 0)),
            ', '.join(info.get('orders', [])), str(len(info.get('orders', []))),
            info.get('status', ''),
        ])

    ws.clear()
    ws.update('A1', rows, value_input_option='RAW')
    batch_format(requests, sheet_id, 0, 0, 1, len(hdrs), hdr_fmt())
    for i, (aid, info) in enumerate(clients.items(), 1):
        bg = COLOR['warn'] if info.get('status') == 'Закрытый' else (COLOR['grey'] if i%2==0 else COLOR['white'])
        batch_format(requests, sheet_id, i, 0, 1, len(hdrs), cell_fmt(bg=bg))

    freeze_row(requests, sheet_id, 1)
    for col, px in enumerate([240, 80, 140, 80, 110, 110, 300, 80, 80], 0):
        set_col_width(requests, sheet_id, col, px)

# ── Создание / получение листа ─────────────────────────────────────────────────
def get_or_create_sheet(spreadsheet, title, index):
    titles = [ws.title for ws in spreadsheet.worksheets()]
    if title in titles:
        return spreadsheet.worksheet(title)
    return spreadsheet.add_worksheet(title=title, rows=2000, cols=20, index=index)

# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print(f'=== Google Sheets Upload [{datetime.now().strftime("%Y-%m-%d %H:%M")}] ===')

    if not os.path.exists(CACHE_PATH):
        print(f'ERROR: Cache not found: {CACHE_PATH}')
        print('Run 01_fetch_data.py first')
        exit(1)

    if not SPREADSHEET_ID:
        print('ERROR: SPREADSHEET_ID не задан.')
        print('Укажите переменную окружения SPREADSHEET_ID или задайте её в скрипте.')
        print('Инструкция: GOOGLE_SHEETS_SETUP.md')
        exit(1)

    with open(CACHE_PATH, 'rb') as f:
        data = pickle.load(f)

    clients      = data['clients']
    results      = data['results']
    generated_at = data.get('generated_at', datetime.now().isoformat())

    print('Авторизация в Google...')
    gc = get_client()
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    print(f'  Таблица: {spreadsheet.title}')

    sheet_configs = [
        ('Сводка',           0),
        ('Бризеры',          1),
        ('Детализация',      2),
        ('Закрытые с долгом',3),
        ('Клиенты',          4),
    ]

    sheet_map = {}
    for title, idx in sheet_configs:
        sheet_map[title] = get_or_create_sheet(spreadsheet, title, idx)
        print(f'  Лист готов: {title}')

    # Записываем данные и собираем запросы форматирования
    all_requests = []

    print('Записываю Сводка...')
    write_summary(sheet_map['Сводка'], clients, results, generated_at,
                  all_requests, sheet_map['Сводка'].id)

    print('Записываю Бризеры...')
    write_breezers(sheet_map['Бризеры'], results, all_requests, sheet_map['Бризеры'].id)

    print('Записываю Детализация...')
    write_detail(sheet_map['Детализация'], results, 'Активный',
                 all_requests, sheet_map['Детализация'].id)

    print('Записываю Закрытые с долгом...')
    write_detail(sheet_map['Закрытые с долгом'], results, 'Закрытый',
                 all_requests, sheet_map['Закрытые с долгом'].id)

    print('Записываю Клиенты...')
    write_clients(sheet_map['Клиенты'], clients, all_requests, sheet_map['Клиенты'].id)

    # Применяем форматирование батчем
    if all_requests:
        print(f'Применяю форматирование ({len(all_requests)} операций)...')
        # Разбиваем на чанки по 100 запросов (лимит API)
        for i in range(0, len(all_requests), 100):
            chunk = all_requests[i:i+100]
            spreadsheet.batch_update({'requests': chunk})

    url = f'https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}'
    print(f'\n✅ ГОТОВО')
    print(f'   Ссылка: {url}')
    print(f'   Клиентов: {sum(1 for v in clients.values() if v.get("status")=="Активный")} активных')
    print(f'   Позиций: {len([r for r in results if r.get("status")=="Активный"])} активных')
