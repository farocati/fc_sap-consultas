@echo off
title Iniciando Servidor SAP - Reportes
color 0a

echo [+] Esperando 30 segundos para que la red esté lista...
timeout /t 30 >nul

echo [+] Navegando a la carpeta del proyecto...
cd /d "C:\sap-inventario"

echo [+] Limpiando procesos anteriores...
pm2 delete sap-inventario >nul 2>&1
pm2 flush

echo [+] Iniciando servidor con PM2...
pm2 start server.js --name "sap-inventario"

echo [+] Iniciando túnel con ngrok...
start "ngrok" C:\tools\ngrok\ngrok.exe http 3000

echo.
echo [✅] Servidor iniciado.
echo     Acceso externo: https://abc123.ngrok.io (ver en consola de ngrok)
echo     Acceso local: http://localhost:3000
echo.
pause