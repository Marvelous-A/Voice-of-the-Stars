# AdminBot

Общая административная панель для «Voice of the Stars» и «ЭХО».

## Возможности

- выбор управляемого проекта;
- статистика обоих ботов;
- пользователи, консультации и история диалогов Voice;
- модерация отзывов и запрос обратной связи;
- управление промокодами и просмотр оплат;
- ручной предпросмотр и публикация постов в Telegram, VK и OK;
- отправка сообщений пользователям через основной Voice-бот.

AdminBot не копирует пользовательские данные. Он читает рабочие файлы Voice из
`VOICE_DATA_DIR`, код публикации из `VOICE_APP_DIR`, а SQLite-базу «ЭХО» — по
`ECHO_DATABASE_PATH`. Локально по умолчанию используются соседние папки в `TG Боты`;
если `AdminBot` лежит внутри репозитория Voice, основной `main.py` находится
автоматически в родительской папке.

## Запуск на Windows

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
# Заполнить ADMIN_BOT_TOKEN и ADMIN_ID
.\.venv\Scripts\python.exe main.py
```

Можно также запустить `start_admin.cmd`. Если собственного `.env` нет, настройки
Voice подхватываются из `Voice of the Stars/.env`.

## Сервер

```bash
sudo bash setup_server.sh
```

Скрипт создаёт `/home/admin-bot`, отдельное виртуальное окружение и службу
`tarot-admin.service`. По умолчанию рабочий Voice находится в `/home/bot`, а база
«ЭХО» — в `/var/lib/echo-dialog-bot/echo.db`.

AdminBot теперь живёт отдельным каталогом внутри репозитория Voice. Для запуска
на сервере используй этот `setup_server.sh`; рабочие пути можно переопределить
через `VOICE_APP_DIR`, `VOICE_DATA_DIR`, `ECHO_DATABASE_PATH` и `NEBO_DATABASE_PATH`.
