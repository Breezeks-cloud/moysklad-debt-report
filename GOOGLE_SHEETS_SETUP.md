# Настройка Google Sheets

Одноразовая настройка занимает ~15 минут. После этого отчёт будет автоматически обновляться в таблице, к которой у всех есть доступ по ссылке.

---

## Шаг 1 — Создать Google Service Account

Service Account — это «технический пользователь» Google, от имени которого скрипт будет писать в таблицу. Вам не нужно авторизовываться вручную каждый раз.

1. Перейдите в [Google Cloud Console](https://console.cloud.google.com/)
2. Создайте новый проект или выберите существующий (например, `breezeks-reports`)
3. В меню слева: **APIs & Services → Library**
   - Найдите **Google Sheets API** → нажмите **Enable**
   - Найдите **Google Drive API** → нажмите **Enable**
4. В меню слева: **APIs & Services → Credentials**
5. Нажмите **+ Create Credentials → Service Account**
   - Имя: `moysklad-report`
   - Описание: `Автообновление отчёта задолженности`
   - Нажмите **Done**
6. Нажмите на созданный Service Account → вкладка **Keys**
7. **Add Key → Create new key → JSON**
8. Файл автоматически скачается. Это ваш `credentials.json`

> ⚠️ Храните этот файл в безопасном месте. Не загружайте в публичный репозиторий.

---

## Шаг 2 — Создать Google Таблицу

1. Откройте [Google Sheets](https://sheets.google.com/) → создайте новую таблицу
2. Назовите её: `Задолженность перед клиентами — Бризекс`
3. Скопируйте **ID таблицы** из адресной строки браузера:
   ```
   https://docs.google.com/spreadsheets/d/  ← ВОТ ЭТО  /edit
                                              1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
   ```
4. Поделитесь таблицей с email Service Account:
   - Кнопка **Поделиться** → вставьте email из `credentials.json` (поле `client_email`)
   - Выберите **Редактор** (Editor)
   - Уберите галку «Уведомить людей»

---

## Шаг 3 — Установить зависимости

```bash
pip install gspread google-auth
```

---

## Шаг 4 — Запустить скрипт

```bash
cd scripts/

# Положите credentials.json рядом со скриптом:
cp ~/Downloads/your-service-account-key.json credentials.json

# Укажите ID таблицы:
export SPREADSHEET_ID="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"

# Убедитесь, что кэш данных свежий:
python3 01_fetch_data.py    # если кэш старше недели

# Загрузите в Google Sheets:
python3 03_upload_gsheets.py
```

---

## Шаг 5 — Открыть доступ для Павла

1. В Google Таблице нажмите **Поделиться**
2. Введите email Павла → **Читатель** (Viewer) — он сможет просматривать, но не редактировать
3. Скопируйте ссылку и отправьте ему

> Или создайте **«Доступ по ссылке»** (Link sharing) → **Читатель** — тогда ссылку можно отправить любому без Gmail-аккаунта.

---

## Настройка для GitHub Actions (автообновление)

Чтобы GitHub Actions мог обновлять таблицу, нужно добавить два секрета в репозиторий:

**Settings → Secrets and variables → Actions → New repository secret:**

| Имя секрета | Значение |
|---|---|
| `MOYSKLAD_TOKEN` | `5cb4bc34b5eb0ba857c61a016fca8c223922ae40` |
| `SPREADSHEET_ID` | ID вашей Google Таблицы |
| `GOOGLE_CREDENTIALS_JSON` | Содержимое `credentials.json` (весь JSON одной строкой) |

Чтобы получить JSON одной строкой:
```bash
cat credentials.json | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin)))"
```

После этого workflow будет каждый понедельник:
1. Загружать данные из МойСклад
2. Генерировать xlsx (сохраняется как Artifact)
3. Обновлять Google Таблицу

---

## Структура Google Таблицы

После первого запуска в таблице появятся 5 листов:

| Лист | Содержание |
|---|---|
| **Сводка** | Ключевые цифры, разбивка по категориям, ТОП-10 |
| **Бризеры** | Производитель → Модель → Конфигурации с кол-вом и долгом |
| **Детализация** | Клиент → Заказ → Товар → Долг (только активные) |
| **Закрытые с долгом** | Аномалии: закрытые заказы с ненулевым долгом |
| **Клиенты** | Все контрагенты с балансами, долгом, телефонами |

---

## Файл credentials.json — где хранить

| Среда | Где хранить |
|---|---|
| Локально | `scripts/credentials.json` (в `.gitignore`!) |
| GitHub Actions | Секрет `GOOGLE_CREDENTIALS_JSON` (JSON как строка) |
| Nextcloud | `0_BREEZEKS/3. РЕСУРСЫ/391-БИБЛИОТЕКА/.../scripts/credentials.json` — только для вас |

> Обязательно добавьте `credentials.json` в `.gitignore` перед первым коммитом!
