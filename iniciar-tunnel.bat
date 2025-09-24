@echo off
title Túnel - Inventario HVGA
color 0a

echo.
echo [INFO] Asegúrate de que tu servidor ya está corriendo:
echo          npm start
echo.
echo [INFO] Este script mantendrá el túnel activo.
echo          Si se cae, se reiniciará automáticamente.
echo.
echo [+] Iniciando túnel con subdominio fijo: hvga-inventario.loca.lt
echo.

:loop
npx localtunnel --port 3000 --subdomain hvga-inventario
echo [!] El túnel se cerró. Reintentando en 5 segundos...
timeout /t 5 >nul
goto loop