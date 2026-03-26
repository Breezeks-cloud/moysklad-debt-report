# Техническая документация

## Архитектура

```
МойСклад API
     │
     ▼
01_fetch_data.py          ← Сбор данных (Phase 1: контрагенты, Phase 2: заказы)
     │
     ▼
moysklad_report_v4.pkl    ← Промежуточный кэш (сериализованные данные)
     │
     ▼
02_build_report.py        ← Генерация xlsx
     │
     ▼
Задолженность_перед_клиентами_v2.xlsx
```

---

## API МойСклад

**Базовый URL:** `https://online.moysklad.ru/api/remap/1.2`  
**Аутентификация:** Bearer Token  
**Обязательные заголовки:**
```http
Authorization: Bearer <TOKEN>
Accept-Encoding: gzip
```

> ⚠️ Без `Accept-Encoding: gzip` API возвращает 415 Unsupported Media Type

### Используемые эндпоинты

| Эндпоинт | Назначение |
|---|---|
| `GET /report/counterparty` | Контрагенты с балансами |
| `GET /entity/customerorder` | Список заказов покупателей |
| `GET /entity/customerorder/{id}` | Детали заказа (expand: agent, positions.assortment) |
| `GET /entity/customerorder/metadata` | Статусы заказов |
| `GET /entity/counterparty` | Детали контрагента (код, телефон) |

### Фильтрация контрагентов

```python
EXCLUDE_NAMES = ['микроклиматика', 'ип гончаров', 'гончаров м', 'бризекс']
TEST_KW = ['тест', 'test', 'ананас', 'четвёртый', 'четвертый']
```

Фильтр: `balance > 0` + исключение по именам выше.

### Фильтрация заказов

- `moment >= 2023-01-01`
- `applicable = true`
- `payedSum > shippedSum` (фильтруется на стороне клиента, т.к. API не поддерживает)

---

## Структура данных (pkl)

```python
{
  'clients': {
    agent_id: {
      'name': str,
      'code': str,         # код контрагента в МойСклад
      'phone': str,
      'companyType': str,  # 'legal' | 'individual'
      'balance': float,
      'debt': float,       # рассчитанный долг
      'orders': [str],     # список номеров заказов
      'status': str,       # 'Активный' | 'Закрытый'
    }
  },
  'results': [
    {
      'client': str,
      'client_id': str,
      'client_code': str,
      'client_phone': str,
      'order_name': str,
      'item_name': str,
      'qty': int,          # всегда целое число (int(round()))
      'debt_alloc': float, # аллоцированный долг по позиции
      'category': str,     # 'Бризер' | 'Сплит/Кондиционер' | 'Прочее' | 'Услуга'
      'mfr': str,          # производитель
      'model': str,        # семейство модели
      'status': str,       # 'Активный' | 'Закрытый'
    }
  ],
  'product_ref': {
    item_name: {
      'buy_price': float,  # buyPrice из карточки товара
      'kit_price': float,  # закупочная/комплект цена
      'category': str,
      'mfr': str,
      'model': str,
    }
  }
}
```

---

## Расчёт долга (детально)

```python
order_debt = max(0.0, order['payedSum'] - order['shippedSum'])

# Сумма позиций с учётом скидок
total_line = sum(
    pos['price'] * (1 - pos['discount']/100.0) * pos['quantity']
    for pos in positions
    if pos['meta_type'] != 'service'
)

# Аллокация долга по каждой позиции
for pos in positions:
    discounted_price = pos['price'] * (1 - pos['discount']/100.0)
    line_total = discounted_price * pos['quantity']
    share = line_total / total_line if total_line > 0 else 0
    pos['debt_alloc'] = order_debt * share
```

---

## Категоризация товаров

```python
BREEZER_KW = ['airnanny','tion ','tion4s','бризер','fanmaster','ballu','royal clima','турков','turkov','живой воздух','бризер','приточная']
NOT_BREEZER_KW = ['фильтр','filter','картридж','cartridge','запасн','запчасть','part','сменн','к бризеру','для бризера','аксессуар','пульт','трубк','крепеж','монтаж']
SPLIT_KW = ['сплит','кондиционер','split','conditioner','inverter','инвертор','мульти','мульти-сплит']

def get_category(name, path, meta_type):
    if meta_type == 'service': return 'Услуга'
    n = (name + ' ' + path).lower()
    if any(k in n for k in SPLIT_KW): return 'Сплит/Кондиционер'
    if any(k in n for k in BREEZER_KW) and not any(k in n for k in NOT_BREEZER_KW):
        return 'Бризер'
    return 'Прочее'
```

---

## Структура Excel файла

### Слой 1 — данные (из API)

| Лист | Источник | Обновляется |
|---|---|---|
| `_API_Клиенты` | Python → xlsx | При каждом обновлении |
| `_API_Позиции` | Python → xlsx | При каждом обновлении |
| `_Справочник` | Python → xlsx | При каждом обновлении (цены можно переопределить вручную) |

### Слой 2 — отображение (формулы)

Все листы `Сводка`, `Бризеры`, `Товары (все)`, `Детализация`, `Закрытые с долгом` — **только формулы**, данные из слоя 1. Перезапись при обновлении не затрагивает ручные правки в `_Справочник`.

### Ключевые формулы

```excel
# Себестоимость позиции
=E{row} * IFERROR(VLOOKUP(D{row}, '_Справочник'!A:G, 7, FALSE), 0)

# Долг по категории (Сводка)
=SUMIFS('_API_Позиции'!F:F, '_API_Позиции'!H:H, "Активный", '_API_Позиции'!G:G, "Бризер")

# Себестоимость категории (Сводка)
=SUMPRODUCT(('_API_Позиции'!H5:H592="Активный") * ('_API_Позиции'!G5:G592="Бризер") * ('_API_Позиции'!E5:E592) * IFERROR(VLOOKUP('_API_Позиции'!D5:D592,'_Справочник'!A:G,7,FALSE),0))
```

---

## Зависимости Python

```
python >= 3.8
openpyxl >= 3.1
```

Стандартные библиотеки: `urllib`, `json`, `gzip`, `pickle`, `re`, `time`, `datetime`, `collections`, `concurrent.futures`

---

## Переменные окружения

| Переменная | Описание | Пример |
|---|---|---|
| `MOYSKLAD_TOKEN` | API-токен МойСклад | `5cb4bc34...` |
| `OUTPUT_PATH` | Путь для сохранения xlsx | `/Users/m_goncharov/Nextcloud/0_Inbox/Задолженность_перед_клиентами_v2.xlsx` |
| `CACHE_PATH` | Путь для pkl-кэша | `/tmp/moysklad_report_v4.pkl` |

> Токен по умолчанию прописан в скрипте. Для GitHub Actions — использовать GitHub Secrets.

---

## Производительность

- Фаза 1 (контрагенты): ~30 сек
- Фаза 2 (сканирование заказов): ~3–5 мин (25 000+ заказов)
- Фаза 3 (детали кандидатов): ~2–3 мин с параллелизмом (ThreadPoolExecutor, 10 потоков)
- Генерация xlsx: ~10 сек

**Итого: ~6–8 минут на полное обновление**
