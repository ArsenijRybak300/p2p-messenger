@echo off
chcp 65001 > nul
cls
echo.

python --version > nul 2>&1
if errorlevel 1 (
    echo Ошибка: Python не найден!
    pause
    exit /b 1
)

echo Создание конфигурационных файлов...

echo { "host": "127.0.0.1", "port": 8888, "discovery_port": 8889 } > config.json
echo { "host": "127.0.0.1", "port": 8890, "discovery_port": 8889 } > config2.json

echo.
echo Запуск первого экземпляра (порт 8888)...
start "Узел 1" cmd /k "python network_messenger.py & pause"

timeout /t 3 /nobreak > nul

echo Запуск второго экземпляра (порт 8890)...
start "Узел 2" cmd /k "python network_messenger.py config2.json & pause"

echo.
echo Система запущена!
pause