@echo off
setlocal
cd /d "%~dp0"
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File "%~dp0collect-event-info.ps1"
set "peer_exit=%errorlevel%"
pause
exit /b %peer_exit%
