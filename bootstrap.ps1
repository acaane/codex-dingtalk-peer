# Application-local dependency installation. Windows PowerShell 5.1 compatible.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-PeerJson($Path, $Value) {
    $parent = Split-Path -Parent $Path
    if (-not (Test-Path -LiteralPath $parent)) { New-Item -ItemType Directory -Path $parent -Force | Out-Null }
    $json = ConvertTo-Json -InputObject $Value -Depth 12
    [IO.File]::WriteAllText($Path, $json, (New-Object Text.UTF8Encoding($false)))
}

function ConvertTo-PeerArgument([string]$Value) {
    if ($Value.Length -gt 0 -and $Value -notmatch '[\s"]') { return $Value }
    $escaped = [regex]::Replace($Value, '(\\*)"', '$1$1\"')
    $escaped = [regex]::Replace($escaped, '(\\+)$', '$1$1')
    return '"' + $escaped + '"'
}

function Invoke-PeerProbe($Path, $Arguments) {
    if (-not $Path -or -not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $null }
    $process = New-Object Diagnostics.Process
    try {
        $process.StartInfo.FileName = $Path
        $process.StartInfo.Arguments = (@($Arguments | ForEach-Object { ConvertTo-PeerArgument $_ }) -join ' ')
        $process.StartInfo.UseShellExecute = $false
        $process.StartInfo.CreateNoWindow = $true
        $process.StartInfo.RedirectStandardOutput = $true
        $process.StartInfo.RedirectStandardError = $true
        [void]$process.Start()
        $output = $process.StandardOutput.ReadToEndAsync()
        $errorOutput = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit(30000)) { $process.Kill(); return $null }
        return [pscustomobject]@{ Code=$process.ExitCode; Text=($output.Result + $errorOutput.Result) }
    } catch { return $null } finally { $process.Dispose() }
}

function Test-PeerExecutable($Path, $Arguments, $Pattern) {
    $probe = Invoke-PeerProbe $Path $Arguments
    return ($null -ne $probe -and $probe.Code -eq 0 -and $probe.Text -match $Pattern)
}

function Get-PeerArchive($Spec, $ToolsRoot) {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    $cache = Join-Path $ToolsRoot 'downloads'
    New-Item -ItemType Directory -Path $cache -Force | Out-Null
    $archive = Join-Path $cache ([IO.Path]::GetFileName(([uri]$Spec.url).AbsolutePath))
    if ((Test-Path -LiteralPath $archive) -and ((Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash -ieq $Spec.sha256)) { return $archive }
    Write-Host ('Downloading ' + [IO.Path]::GetFileName($archive) + ' ...')
    $partial = $archive + '.partial'
    $curlPath = Join-Path $env:SystemRoot 'System32\curl.exe'
    if (Test-Path -LiteralPath $curlPath) {
        & $curlPath --silent --show-error --fail --location --connect-timeout 20 --max-time 300 --continue-at - --output $partial $Spec.url
        if ($LASTEXITCODE -ne 0) { throw 'Download failed or timed out. Check internet access and rerun start.cmd; the system environment is unchanged.' }
    } else {
        $request = [Net.HttpWebRequest]::Create($Spec.url)
        $request.Timeout = 300000
        $request.ReadWriteTimeout = 300000
        $response = $request.GetResponse()
        $stream = $response.GetResponseStream()
        $file = [IO.File]::Create($partial)
        try { $stream.CopyTo($file) } finally { $file.Dispose(); $stream.Dispose(); $response.Dispose() }
    }
    if ((Get-FileHash -LiteralPath $partial -Algorithm SHA256).Hash -ine $Spec.sha256) { throw 'Download checksum mismatch. Installation stopped.' }
    Move-Item -LiteralPath $partial -Destination $archive -Force
    return $archive
}

function Initialize-PeerTools([string]$PeerRoot, [switch]$PortableOnly) {
    if (-not [Environment]::Is64BitOperatingSystem) { throw 'This package requires 64-bit Windows 10/11.' }
    if (($env:PROCESSOR_ARCHITECTURE -eq 'ARM64') -or ($env:PROCESSOR_ARCHITEW6432 -eq 'ARM64')) { throw 'This release targets Windows x64. ARM64 requires a separate package.' }
    $versions = Get-Content -LiteralPath (Join-Path $PeerRoot 'toolchain.versions.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    $toolsRoot = Join-Path $PeerRoot '.tools'
    $vendorRoot = Join-Path $PeerRoot 'vendor\windows-x64'
    function Test-PeerPrivatePath([string]$Candidate) {
        return ($Candidate.StartsWith($toolsRoot, [StringComparison]::OrdinalIgnoreCase) -or $Candidate.StartsWith($vendorRoot, [StringComparison]::OrdinalIgnoreCase))
    }
    New-Item -ItemType Directory -Path $toolsRoot -Force | Out-Null
    $pythonPath = $null
    $dwsPath = $null
    $codexPath = $null
    $savedPaths = @()
    foreach ($file in @((Join-Path $toolsRoot 'toolchain.json'), (Join-Path $PeerRoot 'local.json'))) {
        if (Test-Path -LiteralPath $file) { $savedPaths += (Get-Content -LiteralPath $file -Raw -Encoding UTF8 | ConvertFrom-Json) }
    }
    if (-not $PortableOnly) {
        $pythonCandidates = @()
        foreach ($saved in $savedPaths) { if ($saved.PSObject.Properties['python_path']) { $pythonCandidates += $saved.python_path } }
        foreach ($command in @(Get-Command python.exe,python3.exe -CommandType Application -ErrorAction SilentlyContinue)) {
            if ($command.Source -notmatch '\\WindowsApps\\') { $pythonCandidates += $command.Source }
        }
        $pythonCandidates = @($pythonCandidates | Where-Object { -not (Test-PeerPrivatePath ([string]$_)) }) + @($pythonCandidates | Where-Object { Test-PeerPrivatePath ([string]$_) })
        $pythonProbe = @('-c', 'import sys,sqlite3,zipfile,xml.etree.ElementTree; assert sys.version_info >= (3,7); print(123456789)')
        foreach ($candidate in $pythonCandidates) {
            if (Test-PeerExecutable $candidate $pythonProbe '123456789') { $pythonPath = $candidate; break }
        }
    }
    if (-not $pythonPath) {
        $pythonDir = Join-Path $toolsRoot $versions.python.directory
        $vendoredPythonDir = Join-Path $vendorRoot $versions.python.directory
        if (Test-Path -LiteralPath (Join-Path $vendoredPythonDir 'python.exe')) { $pythonDir = $vendoredPythonDir }
        $pythonPath = Join-Path $pythonDir 'python.exe'
        if (-not (Test-Path -LiteralPath $pythonPath)) {
            $archive = Get-PeerArchive $versions.python $toolsRoot
            Expand-Archive -LiteralPath $archive -DestinationPath $pythonDir -Force
        }
        # Embedded Python excludes the script directory from sys.path by default.
        foreach ($pth in @(Get-ChildItem -LiteralPath $pythonDir -Filter '*._pth' -File)) {
            $lines = @(($pth.BaseName + '.zip'), '.', $PeerRoot)
            [IO.File]::WriteAllLines($pth.FullName, [string[]]$lines, (New-Object Text.UTF8Encoding($false)))
        }
        if (-not (Test-PeerExecutable $pythonPath @('--version') '^Python 3\.')) { throw 'Portable Python cannot run. Check Windows version/runtime policy.' }
    }
    Write-Host ('Python: ' + $pythonPath)
    Write-Host (Invoke-PeerProbe $pythonPath @('--version')).Text.Trim()
    $npmRoot = Join-Path $toolsRoot 'npm'
    $dwsCandidates = @((Join-Path $toolsRoot 'native-dws\dws.exe'), (Join-Path $vendorRoot 'native-dws\dws.exe'), (Join-Path $npmRoot 'node_modules\dingtalk-workspace-cli\vendor\dws.exe'))
    $codexCandidates = @((Join-Path $toolsRoot 'native-codex\bin\codex.exe'), (Join-Path $vendorRoot 'native-codex\bin\codex.exe'))
    function Get-CodexCandidates($ModulesRoot) {
        foreach ($sub in @('@openai\codex-win32-x64','@openai\codex\node_modules\@openai\codex-win32-x64','@openai\codex')) {
            Join-Path (Join-Path $ModulesRoot $sub) 'vendor\x86_64-pc-windows-msvc\bin\codex.exe'
        }
    }
    $codexCandidates += @(Get-CodexCandidates (Join-Path $npmRoot 'node_modules'))
    if (-not $PortableOnly) {
        foreach ($saved in $savedPaths) {
            if ($saved.PSObject.Properties['dws_path']) { $dwsCandidates += $saved.dws_path }
            if ($saved.PSObject.Properties['codex_path']) { $codexCandidates += $saved.codex_path }
        }
        foreach ($command in @(Get-Command dws.exe,dws.cmd -CommandType Application -ErrorAction SilentlyContinue)) {
            if ($command.Source -like '*.exe') { $dwsCandidates += $command.Source }
            else { $dwsCandidates += Join-Path (Split-Path -Parent $command.Source) 'node_modules\dingtalk-workspace-cli\vendor\dws.exe' }
        }
        foreach ($command in @(Get-Command codex.exe,codex.cmd -CommandType Application -ErrorAction SilentlyContinue)) {
            if ($command.Source -like '*.exe') { $codexCandidates += $command.Source }
            else { $codexCandidates += @(Get-CodexCandidates (Join-Path (Split-Path -Parent $command.Source) 'node_modules')) }
        }
        $appReleases = Join-Path $env:USERPROFILE '.codex\packages\app-server-daemon\releases'
        if (Test-Path -LiteralPath $appReleases) {
            foreach ($release in @(Get-ChildItem -LiteralPath $appReleases -Directory | Sort-Object Name -Descending)) {
                $codexCandidates += Join-Path $release.FullName 'bin\codex.exe'
            }
        }
    }
    if (-not $PortableOnly) {
        $dwsCandidates = @($dwsCandidates | Where-Object { -not (Test-PeerPrivatePath ([string]$_)) }) + @($dwsCandidates | Where-Object { Test-PeerPrivatePath ([string]$_) })
        $codexCandidates = @($codexCandidates | Where-Object { -not (Test-PeerPrivatePath ([string]$_)) }) + @($codexCandidates | Where-Object { Test-PeerPrivatePath ([string]$_) })
    }
    foreach ($candidate in $dwsCandidates) { if (Test-PeerExecutable $candidate @('--version') 'dws version') { $dwsPath = $candidate; break } }
    foreach ($candidate in $codexCandidates) {
        if (Test-PeerExecutable $candidate @('--version') 'codex') { $codexPath = $candidate; break }
    }
    if (-not $dwsPath -or -not $codexPath) {
        $nodePath = $null
        if (-not $PortableOnly) {
            foreach ($command in @(Get-Command node.exe -CommandType Application -ErrorAction SilentlyContinue)) {
                if ((Test-PeerExecutable $command.Source @('--version') '^v') -and (Test-Path -LiteralPath (Join-Path (Split-Path -Parent $command.Source) 'node_modules\npm\bin\npm-cli.js'))) { $nodePath = $command.Source; break }
            }
        }
        $nodeDir = Join-Path $toolsRoot $versions.node.directory
        if (-not $nodePath) { $nodePath = Join-Path $nodeDir 'node.exe' }
        if (-not (Test-Path -LiteralPath $nodePath)) {
            $archive = Get-PeerArchive $versions.node $toolsRoot
            Expand-Archive -LiteralPath $archive -DestinationPath $toolsRoot -Force
        }
        if (-not (Test-PeerExecutable $nodePath @('--version') '^v')) { throw 'Node.js cannot run.' }
        $packages = @()
        if (-not $dwsPath) { $packages += ('dingtalk-workspace-cli@' + $versions.dws) }
        if (-not $codexPath) { $packages += ('@openai/codex@' + $versions.codex) }
        & $pythonPath (Join-Path $PeerRoot 'setup_helper.py') install-npm --node $nodePath --prefix $npmRoot --packages @packages | Out-Host
        if ($LASTEXITCODE -ne 0) { throw 'CLI download/install failed. See the error above; internet access to npm and GitHub is required.' }
        if (-not $dwsPath) { $dwsPath = Join-Path $npmRoot 'node_modules\dingtalk-workspace-cli\vendor\dws.exe' }
        if (-not $codexPath) {
            foreach ($candidate in @(Get-CodexCandidates (Join-Path $npmRoot 'node_modules'))) {
                if (Test-PeerExecutable $candidate @('--version') 'codex') { $codexPath = $candidate; break }
            }
        }
    }
    if (-not (Test-PeerExecutable $dwsPath @('--version') 'dws version')) { throw 'DWS installation verification failed.' }
    if (-not (Test-PeerExecutable $codexPath @('--version') 'codex')) { throw 'Codex installation verification failed.' }
    Write-Host (Invoke-PeerProbe $dwsPath @('--version')).Text.Trim()
    Write-Host (Invoke-PeerProbe $codexPath @('--version')).Text.Trim()
    $result = [ordered]@{ python_path=$pythonPath; dws_path=$dwsPath; codex_path=$codexPath }
    Write-PeerJson (Join-Path $toolsRoot 'toolchain.json') $result
    Write-Host 'Toolchain ready.' -ForegroundColor Green
    return $result
}
