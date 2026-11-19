@echo off
setlocal
cd /d "%~dp0"
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File "%~dp0install.ps1" -Mode EnableUpdates
set "peer_exit=%errorlevel%"
pause
exit /b %peer_exit%
