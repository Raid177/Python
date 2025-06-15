@echo off
setlocal

REM Назва тунелю
set TUNNEL_NAME=MySQLTunnel

REM SSH команда тунелю
set SSH_COMMAND=ssh -N -L 3307:127.0.0.1:3306 root@95.216.219.145

:loop
echo [%DATE% %TIME%] Перевірка %TUNNEL_NAME%
%SSH_COMMAND%
echo [%DATE% %TIME%] Тунель %TUNNEL_NAME% завершився. Перезапуск через 5 секунд...
timeout /t 5 >nul
goto loop
