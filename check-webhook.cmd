@echo off
setlocal
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference = 'Stop'; [Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false); $OutputEncoding = [Console]::OutputEncoding; try { $root = [Environment]::CurrentDirectory; $toolsFile = Join-Path $root '.tools\toolchain.json'; if (-not (Test-Path -LiteralPath $toolsFile)) { throw 'Put this file beside start.cmd in the extracted deployment folder.' }; $tools = Get-Content -LiteralPath $toolsFile -Raw -Encoding UTF8 | ConvertFrom-Json; & $tools.python_path (Join-Path $root 'setup_helper.py') webhook-diagnostics; exit $LASTEXITCODE } catch { Write-Host 'Collection failed. Keep this window for diagnosis.' -ForegroundColor Red; exit 1 }"
if errorlevel 1 echo Collection failed. Keep the error shown above.
pause
