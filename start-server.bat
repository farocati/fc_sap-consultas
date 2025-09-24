@echo off
title SERVIDOR SAP - Reportes Comerciales (HVGA)
color 0a
cls

:: -------------------------------------------------------------------------
:: SCRIPT: Inicia servidor Node.js + PM2 + ngrok para acceso global
:: OBJETIVO: Mantener reportes en tiempo real para México, China e Italia
:: -------------------------------------------------------------------------

echo.
echo [🚀 INICIANDO SISTEMA DE REPORTES]
echo =====================================================
echo   Servidor: Node.js
echo   Monitor : PM2 (reinicio automático)
echo   Acceso  : ngrok.io (global, incluye China/Italia)
echo =====================================================
echo.

:: Paso 1: Navegar al directorio del proyecto
cd /d "C:\sap-inventario"
if %errorlevel% neq 0 (
    echo [❌ ERROR] No se encontró la carpeta C:\sap-inventario
    echo Asegúrate de que el proyecto esté en esa ruta.
    pause
    exit /b 1
)

echo [✔] Carpeta encontrada: C:\sap-inventario

:: Paso 2: Verificar que server.js existe
if not exist "server.js" (
    echo [❌ ERROR] No se encontró el archivo 'server.js'
    echo Colócalo en C:\sap-inventario\server.js
    pause
    exit /b 1
)
echo [✔] Archivo 'server.js' encontrado

:: Paso 3: Iniciar el servidor con PM2
echo.
echo [🔧] Iniciando servidor con PM2...
pm2 delete sap-inventario >nul 2>&1
pm2 start server.js --name "sap-inventario" --no-daemon --log-date-format "YYYY-MM-DD HH:mm:ss"

if %errorlevel% neq 0 (
    echo [❌ ERROR] Falló al iniciar PM2. Revisa server.js
    pause
    exit /b 1
)
echo [✔] Servidor iniciado correctamente con PM2

:: Paso 4: Esperar a que el servidor levante
echo [⏳] Esperando 10 segundos para que el servidor inicie...
timeout /t 10 >nul

:: Paso 5: Ejecutar ngrok
echo.
echo [🌐] Iniciando túnel seguro con ngrok...
echo     Acceso desde cualquier parte del mundo:
echo     (Ej: https://abc123.ngrok.io)
echo.
echo [❗] Si no aparece un enlace HTTPS, revisa:
echo      1. Que ngrok esté autenticado
echo      2. Conexión a internet
echo      3. Firewall o antivirus
echo.

:: Ruta absoluta a ngrok (ajusta si lo tienes en otra carpeta)
"C:\tools\ngrok\ngrok.exe" http 3000

:: Si ngrok se cierra, permite reiniciar manualmente
echo [🔁] ngrok se cerró. Presiona una tecla para reintentar o Cierra la ventana.
pause
goto loop