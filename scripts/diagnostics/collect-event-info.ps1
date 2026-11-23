[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object Text.UTF8Encoding($false)
$env:PYTHONUTF8 = '1'
try {
    $peerRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
    Set-Location -LiteralPath $peerRoot
    $toolchainFile = Join-Path $peerRoot '.tools/toolchain.json'
    $pythonExe = $null
    if (Test-Path -LiteralPath $toolchainFile -PathType Leaf) {
        $toolchain = Get-Content -LiteralPath $toolchainFile -Raw -Encoding UTF8 | ConvertFrom-Json
        if (Test-Path -LiteralPath $toolchain.python_path -PathType Leaf) { $pythonExe = $toolchain.python_path }
    }
    if (-not $pythonExe) {
        $portablePython = Join-Path $peerRoot '.tools/python-3.13.16/python.exe'
        if (Test-Path -LiteralPath $portablePython -PathType Leaf) { $pythonExe = $portablePython }
    }
    if (-not $pythonExe) { throw 'Existing connector Python was not found. Keep your installed tools and configuration.' }
    & $pythonExe (Join-Path $PSScriptRoot 'collect-event-info.py')
    exit $LASTEXITCODE
} catch {
    Write-Host ('Diagnostics failed: ' + $_.Exception.Message) -ForegroundColor Red
    exit 1
}
