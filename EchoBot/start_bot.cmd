@echo off
chcp 65001 >nul
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo Не найдено виртуальное окружение .venv
    echo Создайте его и установите зависимости из requirements.txt
    pause
    exit /b 1
)

echo Запуск «ЭХО ^| Анонимный чат»...
".venv\Scripts\python.exe" main.py

if errorlevel 1 (
    echo.
    echo Бот завершился с ошибкой. Сообщение находится выше.
    pause
)
