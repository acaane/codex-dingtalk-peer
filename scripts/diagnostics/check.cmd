@echo off
setlocal
cd /d "%~dp0..\.." || exit /b 1
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File "%CD%\install.ps1" -Mode Check
set "peer_exit=%errorlevel%"
pause
exit /b %peer_exit%
