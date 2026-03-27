#!/usr/bin/env python3
"""
МойСклад → Задолженность перед клиентами
Шаг 1: Сбор данных из API и сохранение в pkl-кэш.

Запуск: python3 01_fetch_data.py
"""

import os, json, gzip, time, re, pickle
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── Настройки ──────────────────────────────────────────────────────────────────
TOKEN = os.environ.get('MOYSKLAD_TOKEN', '')
if not TOKEN:
    raise RuntimeError("MOYSKLAD_TOKEN не задан. Установите переменную окружения MOYSKLAD_TOKEN.")
CACHE_PATH = os.environ.get('CACHE_PATH', '/tmp/moysklad_report_v4.pkl')
BASE = 'https://api.moysklad.ru/api/remap/1.2'
LIMIT = 1000
WORKERS = 10

EXCLUDE_NAMES = ['микроклиматика', 'ип гончаров', 'гончаров м', 'бризекс']
TEST_KW = ['тест', 'test', 'ананас', 'четвёртый', 'четвертый', 'ромашка']

BREEZER_KW = ['airnanny', 'tion ', 'tion4s', 'бризер', 'fanmaster', 'ballu',
              'royal clima', 'турков', 'turkov', 'живой воздух', 'приточная',
              'realtherm', 'minibox', 'vakio']
NOT_BREEZER_KW = ['фильтр', 'filter', 'картридж', 'cartridge', 'запасн',
                  'запчасть', 'part', 'сменн', 'к бризеру', 'для бризера',
                  'аксессуар', 'пульт', 'трубк', 'крепеж', 'монтаж']
SPLIT_KW = ['сплит', 'кондиционер', 'split', 'conditioner', 'inverter', 'инвертор']

MFR_MAP = {
    'airnanny': 'AIRNANNY', 'tion': 'Tion', 'ballu': 'Ballu',
    'royal clima': 'Royal Clima', 'fanmaster': 'FanMaster',
    'turkov': 'Turkov', 'турков': 'Turkov', 'minibox': 'MiniBox',
    'vakio': 'Vakio', 'daikin': 'Daikin', 'mitsubishi': 'Mitsubishi',
    'lg ': 'LG', 'samsung': 'Samsung',
}

# ── HTTP helper ────────────────────────────────────────────────────────────────
def api_get(url, retries=3):
    for attempt in range(retries):
        try:
            req = Request(url, headers={
                'Authorization': f'Bearer {TOKEN}',
                'Accept-Encoding': 'gzip',
            })
            with urlopen(req, timeout=60) as r:
                raw = r.read()
                if r.info().get('Content-Encoding') == 'gzip':
                    raw = gzip.decompress(raw)
                return json.loads(raw.decode('utf-8'))
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)

def get_all(path, params=None):
    items = []
    offset = 0
    while True:
        p = {'limit': LIMIT, 'offset': offset}
        if params:
            p.update(params)
        data = api_get(f'{BASE}{path}?{urlencode(p)}')
        rows = data.get('rows', data if isinstance(data, list) else [])
        items.extend(rows)
        meta = data.get('meta', {})
        if offset + LIMIT >= meta.get('size', len(rows)):
            break
        offset += LIMIT
        time.sleep(0.2)
    return items

# ── Классификация ───────────────────────────────────────────────────────────────
def get_category(name, path, meta_type):
    if meta_type == 'service':
        return 'Услуга'
    n = (name + ' ' + (path or '')).lower()
    if any(k in n for k in SPLIT_KW):
        return 'Сплит/Кондиционер'
    if any(k in n for k in BREEZER_KW) and not any(k in n for k in NOT_BREEZER_KW):
        return 'Бризер'
    return 'Прочее'

def extract_mfr(name):
    n = name.lower()
    for key, val in MFR_MAP.items():
        if key in n:
            return val
    return ''

def extract_model(name, mfr):
    n = name.upper()
    if mfr == 'AIRNANNY':
        m = re.search(r'A\d+', n)
        return f'AIRNANNY {m.group()}' if m else 'AIRNANNY'
    if mfr == 'Tion':
        m = re.search(r'(O\d+|LITE|S\d+|4S)', n, re.I)
        return f'Tion {m.group().upper()}' if m else 'Tion'
    return mfr or 'Прочее'

# ── Фаза 1: контрагенты ────────────────────────────────────────────────────────
def fetch_counterparties():
    print('Phase 1: fetching counterparties...')
    rows = get_all('/report/counterparty')
    result = {}
    for r in rows:
        balance_raw = r.get('balance', 0) or 0
        balance = balance_raw / 100.0
        if balance <= 0:
            continue
        cp = r.get('counterparty', {})
        name = cp.get('name', '')
        nl = name.lower()
        if any(e in nl for e in EXCLUDE_NAMES):
            continue
        if any(t in nl for t in TEST_KW):
            continue
        aid = cp.get('id', '')
        if not aid:
            continue
        result[aid] = {
            'name': name,
            'balance': balance,
            'companyType': cp.get('companyType', 'individual'),
            'href': cp.get('meta', {}).get('href', ''),
            'code': '', 'phone': cp.get('phone', '') or '',
        }
    print(f'  positive balance: {len(result)} counterparties')
    return result

# ── Фаза 2: детали контрагентов ────────────────────────────────────────────────
def fetch_cp_details(clients):
    print('Phase 2: fetching counterparty details...')
    def fetch_one(aid, info):
        try:
            data = api_get(f'{BASE}/entity/counterparty/{aid}')
            info['code'] = data.get('code', '')
            info['phone'] = data.get('phone', '') or ''
            info['externalCode'] = data.get('externalCode', '')
            return aid, info
        except Exception as e:
            return aid, info

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch_one, aid, info): aid for aid, info in clients.items()}
        for i, f in enumerate(as_completed(futures), 1):
            aid, info = f.result()
            clients[aid] = info
            if i % 100 == 0:
                print(f'  cp_details: {i}/{len(clients)}')
    print(f'  done: {len(clients)} counterparties')
    return clients

# ── Фаза 3: сканирование заказов ───────────────────────────────────────────────
def scan_orders(clients):
    print('Phase 3: scanning orders...')
    client_ids = set(clients.keys())
    candidates = {}
    closed_orders_map = {}
    offset = 0
    total = 0
    while True:
        params = {
            'limit': LIMIT, 'offset': offset,
            'filter': 'moment>=2023-01-01 00:00:00;applicable=true',
            'order': 'moment,desc',
        }
        data = api_get(f'{BASE}/entity/customerorder?{urlencode(params)}')
        rows = data.get('rows', [])
        if not rows:
            break
        total += len(rows)
        for o in rows:
            agent_href = (o.get('agent', {}).get('meta', {}).get('href', ''))
            aid = agent_href.split('/')[-1] if agent_href else ''
            if aid not in client_ids:
                continue
            payed = (o.get('payedSum', 0) or 0) / 100.0
            shipped = (o.get('shippedSum', 0) or 0) / 100.0
            state_name = (o.get('state') or {}).get('name', '')
            is_closed = any(w in state_name.lower()
                           for w in ['закрыт', 'отмен', 'выполн', 'реализован'])
            if is_closed:
                closed_orders_map.setdefault(aid, []).append({
                    'name': o.get('name', ''), 'state': state_name,
                    'payedSum': payed, 'shippedSum': shipped,
                })
            if payed <= shipped:
                continue
            oid = o['id']
            if oid not in candidates:
                candidates[oid] = {
                    'id': oid, 'name': o.get('name', ''),
                    'agent_id': aid, 'payedSum': payed, 'shippedSum': shipped,
                    'is_closed': is_closed,
                }
        if total % 5000 < LIMIT:
            print(f'  scanned {total}, candidates: {len(candidates)}')
        meta = data.get('meta', {})
        if offset + LIMIT >= meta.get('size', len(rows)):
            break
        offset += LIMIT
        time.sleep(0.1)
    print(f'  total scanned: {total}, candidates: {len(candidates)}')
    print(f'  clients with closed orders: {len(closed_orders_map)}')
    return candidates, closed_orders_map

# ── Фаза 4: детали заказов ─────────────────────────────────────────────────────
def fetch_order_details(candidates, clients):
    print(f'Phase 4: fetching {len(candidates)} order details...')
    results = []
    product_ref = {}

    def fetch_one(order_meta):
        oid = order_meta['id']
        try:
            data = api_get(f'{BASE}/entity/customerorder/{oid}?expand=agent,positions.assortment')
            return oid, data
        except Exception as e:
            return oid, None

    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch_one, om): om for om in candidates.values()}
        for f in as_completed(futures):
            oid, data = f.result()
            done += 1
            if done % 100 == 0:
                print(f'  proc {done}/{len(candidates)}')
            if not data:
                continue

            om = candidates[oid]
            aid = om['agent_id']
            client_info = clients.get(aid, {})
            order_debt = max(0.0, om['payedSum'] - om['shippedSum'])
            is_closed = om['is_closed']

            positions = data.get('positions', {}).get('rows', [])
            # Build discounted line totals for debt allocation
            lines = []
            for pos in positions:
                assortment = pos.get('assortment', {})
                meta_type = assortment.get('meta', {}).get('type', '')
                if meta_type == 'service':
                    continue
                qty = pos.get('quantity', 0) or 0
                shipped_qty = pos.get('shipped', 0) or 0
                unshipped = max(0.0, qty - shipped_qty)
                if unshipped <= 0:
                    continue
                price = pos.get('price', 0) or 0
                discount = pos.get('discount', 0) or 0
                disc_price = price * (1 - discount / 100.0) / 100.0  # копейки → рубли
                lines.append((pos, assortment, meta_type, int(round(unshipped)), disc_price))

            total_line = sum(dp * qty for (_, _, _, qty, dp) in lines)

            for pos, assortment, meta_type, qty, disc_price in lines:
                name = assortment.get('name', 'Без названия')
                path_name = assortment.get('pathName', '')
                category = get_category(name, path_name, meta_type)
                mfr = extract_mfr(name)
                model = extract_model(name, mfr)
                share = (disc_price * qty / total_line) if total_line > 0 else 0
                debt_alloc = round(order_debt * share, 2)

                # Cost price
                buy_price = 0.0
                kit_price = 0.0
                if 'buyPrice' in assortment:
                    buy_price = (assortment['buyPrice'].get('value', 0) or 0) / 100.0
                for sp in assortment.get('salePrices', []):
                    pt_name = (sp.get('priceType') or {}).get('name', '').lower()
                    if any(k in pt_name for k in ['комплект', 'закупочн', 'минимальн']):
                        v = (sp.get('value', 0) or 0) / 100.0
                        if v > 0:
                            kit_price = v
                            break

                if name not in product_ref:
                    product_ref[name] = {
                        'buy_price': buy_price, 'kit_price': kit_price,
                        'category': category, 'mfr': mfr, 'model': model,
                    }

                # Track orders per client
                order_name = om['name']
                if 'orders' not in client_info:
                    client_info['orders'] = []
                if order_name not in client_info['orders']:
                    client_info['orders'].append(order_name)
                clients[aid] = client_info

                status = 'Закрытый' if is_closed else 'Активный'
                results.append({
                    'client': client_info.get('name', ''),
                    'client_id': aid,
                    'client_code': client_info.get('code', ''),
                    'client_phone': client_info.get('phone', ''),
                    'order_name': order_name,
                    'item_name': name,
                    'qty': qty,
                    'debt_alloc': debt_alloc,
                    'category': category,
                    'mfr': mfr,
                    'model': model,
                    'status': status,
                })

    print(f'  DONE: {len(results)} positions, {len(product_ref)} unique products')
    return results, product_ref, clients

# ── Агрегация клиентов ─────────────────────────────────────────────────────────
def aggregate_clients(results, clients, closed_orders_map=None):
    debt_map = defaultdict(float)
    status_map = {}
    for r in results:
        debt_map[r['client_id']] += r['debt_alloc']
        if r['status'] == 'Активный':
            status_map[r['client_id']] = 'Активный'
        elif r['client_id'] not in status_map:
            status_map[r['client_id']] = 'Закрытый'

    for aid in clients:
        clients[aid]['debt'] = round(debt_map.get(aid, 0.0), 2)
        clients[aid]['status'] = status_map.get(aid, 'Закрытый')
        if 'orders' not in clients[aid]:
            clients[aid]['orders'] = []
        if closed_orders_map and aid in closed_orders_map:
            clients[aid]['closed_orders'] = closed_orders_map[aid]

    active = sorted(
        [(k, v) for k, v in clients.items() if v.get('status') == 'Активный'],
        key=lambda x: -x[1]['debt']
    )
    closed = [(k, v) for k, v in clients.items() if v.get('status') == 'Закрытый']

    sorted_clients = dict(active + closed)
    return sorted_clients

# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print(f'=== МойСклад API → Data Fetch  [{datetime.now().strftime("%Y-%m-%d %H:%M")}] ===')
    clients = fetch_counterparties()
    clients = fetch_cp_details(clients)
    candidates, closed_orders_map = scan_orders(clients)
    results, product_ref, clients = fetch_order_details(candidates, clients)
    clients = aggregate_clients(results, clients, closed_orders_map)

    data = {
        'clients': clients,
        'results': results,
        'product_ref': product_ref,
        'generated_at': datetime.now().isoformat(),
    }
    with open(CACHE_PATH, 'wb') as f:
        pickle.dump(data, f)

    active = sum(1 for v in clients.values() if v.get('status') == 'Активный')
    closed_c = sum(1 for v in clients.values() if v.get('status') == 'Закрытый')
    print(f'\n✅ Saved to {CACHE_PATH}')
    print(f'   Clients: {active} active + {closed_c} closed')
    print(f'   Positions: {len(results)}')
    print(f'   Products: {len(product_ref)}')
