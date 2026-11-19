[CmdletBinding()]
param([switch]$LibraryOnly)
$ErrorActionPreference = 'Stop'

function Clear-StaleBusPid {
    param([string]$DwsRoot, [string]$IdentityHash, [int]$ExpectedPid, [string]$PeerRoot)
    if ($IdentityHash -notmatch '^[0-9a-f]{16}$' -or $ExpectedPid -le 0) { throw 'Invalid recovery identity or PID.' }
    $configRoot = [IO.Path]::GetFullPath($DwsRoot).TrimEnd('\')
    $targetDirectory = [IO.Path]::GetFullPath((Join-Path $configRoot ('events\open\personal_stream\' + $IdentityHash)))
    if (-not $targetDirectory.StartsWith($configRoot + '\', [StringComparison]::OrdinalIgnoreCase)) { throw 'Bus directory escapes configured root.' }
    $lockFile = Join-Path $targetDirectory 'bus.lock'
    $checkPath = $lockFile
    while ($checkPath) {
        $item = Get-Item -LiteralPath $checkPath -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw 'Recovery refuses links and reparse points.' }
        if ($checkPath -eq $configRoot) { break }
        $checkPath = [IO.Path]::GetDirectoryName($checkPath)
    }
    $backupRoot = [IO.Path]::GetFullPath((Join-Path $PeerRoot '.local'))
    $backupItem = Get-Item -LiteralPath $backupRoot -Force
    if (($backupItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw 'Recovery refuses a linked backup directory.' }
    $backupFile = Join-Path $backupRoot ('bus-pid-recovery-' + [guid]::NewGuid().ToString('N') + '.json')
    $stream = $null
    $locked = $false
    try {
        $stream = New-Object IO.FileStream($lockFile, [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::ReadWrite)
        # The installed DWS v1.0.65 locks one byte at 1 GiB. Holding that
        # same range proves that the recorded PID does not own the bus lock.
        $stream.Lock(1073741824, 1)
        $locked = $true
        if ($stream.Length -gt 64) { throw 'Unexpected bus.lock content; nothing changed.' }
        $oldBytes = New-Object byte[] ([int]$stream.Length)
        [void]$stream.Read($oldBytes, 0, $oldBytes.Length)
        $oldText = [Text.Encoding]::ASCII.GetString($oldBytes).Trim()
        if ($oldText -ne [string]$ExpectedPid) { throw 'Recorded PID changed during checks; nothing changed.' }
        $record = [ordered]@{ status='prepared'; lock_file=$lockFile; old_pid=$ExpectedPid; old_bytes_base64=[Convert]::ToBase64String($oldBytes); time=[DateTimeOffset]::Now.ToString('o') }
        $record | ConvertTo-Json | Set-Content -LiteralPath $backupFile -Encoding UTF8
        try {
            $stream.SetLength(0)
            $stream.Flush($true)
        } catch {
            $stream.SetLength(0)
            $stream.Position = 0
            $stream.Write($oldBytes, 0, $oldBytes.Length)
            $stream.Flush($true)
            throw
        }
        $record.status = 'stale_pid_cleared'
        $record | ConvertTo-Json | Set-Content -LiteralPath $backupFile -Encoding UTF8
        return $backupFile
    } finally {
        if ($stream) {
            if ($locked) { $stream.Unlock(1073741824, 1) }
            $stream.Dispose()
        }
    }
}

if ($LibraryOnly) { return }
[Console]::OutputEncoding = New-Object Text.UTF8Encoding($false)
$env:PYTHONUTF8 = '1'
try {
    Set-Location -LiteralPath $PSScriptRoot
    $tools = Get-Content -LiteralPath (Join-Path $PSScriptRoot '.tools/toolchain.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    $pythonExe = $tools.python_path
    if (-not (Test-Path -LiteralPath $pythonExe -PathType Leaf)) { throw 'Configured Python was not found. Keep the existing installation.' }
    $planText = & $pythonExe (Join-Path $PSScriptRoot 'collect-event-info.py') --bus-plan
    if ($LASTEXITCODE -ne 0) { throw 'Cannot obtain a read-only recovery plan.' }
    $plan = ($planText -join "`n") | ConvertFrom-Json
    if (-not $plan.ok) { throw ('Recovery withheld: ' + $plan.reason) }
    $processes = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -ieq 'dws.exe' -or $_.Name -ieq $plan.dws_executable_name })
    if ($processes.Count) { throw 'DWS processes are still running. Close the failed connector normally first; no process was stopped.' }
    $owner = Get-CimInstance Win32_Process | Where-Object { $_.ProcessId -eq $plan.holder_pid } | Select-Object ProcessId,Name,CreationDate
    Write-Host ('Recorded PID: ' + $plan.holder_pid)
    if ($owner) { Write-Host ('Current PID belongs to: ' + $owner.Name) }
    $backup = Clear-StaleBusPid -DwsRoot $plan.dws_root -IdentityHash $plan.identity_hash -ExpectedPid $plan.holder_pid -PeerRoot $PSScriptRoot
    Write-Host 'Recovery completed: cleared only the stale PID in bus.lock.' -ForegroundColor Green
    Write-Host ('Recovery record: ' + $backup)
    Write-Host 'No process was stopped. Login, subscriptions, protection records, and bus.meta were preserved.'
    Write-Host 'Now run start.cmd and wait for ready/connected.'
    exit 0
} catch {
    Write-Host ('Recovery withheld or failed: ' + $_.Exception.Message) -ForegroundColor Red
    Write-Host 'Keep the error and run collect-event-info.cmd for diagnostics.'
    exit 1
}
