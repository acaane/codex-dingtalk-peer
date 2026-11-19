[CmdletBinding()]
param(
    [ValidateSet('Start','Setup','Task','StopTask','Check','Resume','EnableUpdates','PublishUpdate')][string]$Mode = 'Start',
    [switch]$ToolsOnly,
    [switch]$PortableOnly,
    [switch]$NoStart,
    [string]$Project
)
$ErrorActionPreference = 'Stop'
$peerRoot = $PSScriptRoot
Set-Location -LiteralPath $peerRoot
[Console]::OutputEncoding = New-Object Text.UTF8Encoding($false)
$env:PYTHONUTF8 = '1'
$ProgressPreference = 'SilentlyContinue'
. (Join-Path $peerRoot 'bootstrap.ps1')

function Invoke-PeerPython($PythonPath, $PeerArguments) {
    & $PythonPath (Join-Path $peerRoot 'setup_helper.py') @PeerArguments
    if ($LASTEXITCODE -ne 0) { throw '当前步骤未完成。请查看上方提示，处理后重新双击 start.cmd。已有环境和配置会保留。' }
}

function Select-PeerProject($Suggested) {
    Write-Host '请选择本机参与联调的项目目录。'
    try {
        Add-Type -AssemblyName System.Windows.Forms
        $dialog = New-Object System.Windows.Forms.FolderBrowserDialog
        $dialog.Description = '选择本机项目目录（不会修改项目代码）'
        $dialog.ShowNewFolderButton = $false
        if ($Suggested -and (Test-Path -LiteralPath $Suggested -PathType Container)) { $dialog.SelectedPath = $Suggested }
        $choice = $dialog.ShowDialog()
        $selected = $dialog.SelectedPath
        $dialog.Dispose()
        if ($choice -eq [System.Windows.Forms.DialogResult]::OK) { return $selected }
        throw '未选择项目目录。'
    } catch {
        $path = (Read-Host '输入或拖入项目目录后回车').Trim().Trim('"')
        if (-not (Test-Path -LiteralPath $path -PathType Container)) { throw '项目目录不存在。' }
        return (Resolve-Path -LiteralPath $path).Path
    }
}

try {
    foreach ($required in @('peer.py','reliability.py','connector_update.py','supervisor.py','setup_helper.py','config.example.json','toolchain.versions.json')) {
        if (-not (Test-Path -LiteralPath (Join-Path $peerRoot $required))) { throw '文件不完整。请先把整个压缩包解压到普通文件夹，再双击 start.cmd。' }
    }
    Write-Host '=== AI 联调连接器：复用已有环境，缺少时仅安装到本工具目录 ===' -ForegroundColor Cyan
    $tools = Initialize-PeerTools -PeerRoot $peerRoot -PortableOnly:$PortableOnly
    if ($ToolsOnly) { Write-Host '环境准备完成；未登录、未生成业务配置、未启动机器人。'; exit 0 }
    $localRoot = Join-Path $peerRoot '.local'
    $machineFile = Join-Path $localRoot 'machine.json'
    $configFile = Join-Path $localRoot 'config.json'
    if ($Mode -eq 'Setup' -or -not (Test-Path -LiteralPath $machineFile) -or -not (Test-Path -LiteralPath $configFile)) {
        Write-Host ''
        Write-Host '首次部署需要本人完成登录，并选择自己的机器人和项目。'
        $discoveryArguments = @('discover')
        if ($Mode -eq 'Setup') { $discoveryArguments += '--ask-project' }
        Invoke-PeerPython $tools.python_path $discoveryArguments
        $discovery = Get-Content -LiteralPath (Join-Path $localRoot 'discovery.json') -Raw -Encoding UTF8 | ConvertFrom-Json
        $rosterText = & $tools.python_path (Join-Path $peerRoot 'setup_helper.py') participants
        if ($LASTEXITCODE -ne 0) { throw '参与者配置读取失败，请保留错误信息。' }
        $roster = ($rosterText -join [Environment]::NewLine) | ConvertFrom-Json
        $participants = @($roster.peers)
        for ($index = 0; $index -lt $participants.Count; $index++) {
            Write-Host ('  ' + ($index + 1) + '. ' + $participants[$index].owner_name + '（' + $participants[$index].id + '）')
        }
        $peerId = $discovery.peer
        Write-Host ('已检测本人端ID：' + $peerId + '；项目标识：' + $discovery.project_id)
        $suggested = $null
        $oldDeployment = $null
        $oldConfigFile = Join-Path $peerRoot 'local.json'
        $workbook = $null
        if (Test-Path -LiteralPath $oldConfigFile) {
            $oldConfig = Get-Content -LiteralPath $oldConfigFile -Raw -Encoding UTF8 | ConvertFrom-Json
            if ($oldConfig.PSObject.Properties['workbook'] -and (Test-Path -LiteralPath $oldConfig.workbook)) { $workbook = $oldConfig.workbook }
        }
        if (Test-Path -LiteralPath $configFile) {
            $oldDeployment = Get-Content -LiteralPath $configFile -Raw -Encoding UTF8 | ConvertFrom-Json
            $ownPeer = @($oldDeployment.peers | Where-Object { $_.id -eq $peerId })
            if ($ownPeer.Count -eq 1) { $suggested = $ownPeer[0].workdir }
        }
        $detectedProject = $null
        if ($Mode -ne 'Setup' -and -not $Project -and -not $suggested) {
            $projectDetection = & $tools.python_path (Join-Path $peerRoot 'setup_helper.py') detect-project
            if ($LASTEXITCODE -eq 0) {
                $projectCandidate = ($projectDetection -join [Environment]::NewLine) | ConvertFrom-Json
                if ($projectCandidate.path -and (Test-Path -LiteralPath $projectCandidate.path -PathType Container)) { $detectedProject = $projectCandidate.path }
            }
        }
        if ($Project -and (Test-Path -LiteralPath $Project -PathType Container)) {
            $project = (Resolve-Path -LiteralPath $Project).Path
        } elseif ($Mode -ne 'Setup' -and $suggested -and (Test-Path -LiteralPath $suggested -PathType Container)) {
            $project = (Resolve-Path -LiteralPath $suggested).Path
            Write-Host ('已复用本机项目目录：' + $project)
        } elseif ($detectedProject) {
            $project = $detectedProject
            Write-Host ('已自动识别邻近的唯一业务工程：' + $project)
        } else {
            $project = Select-PeerProject $suggested
        }
        $arguments = @('configure','--peer',$peerId,'--project',$project,'--project-id',$discovery.project_id)
        Write-Host '项目配置、任务记录和agent绑定将分别保存。'
        $credentialFile = Join-Path $localRoot ('webhook-' + $peerId + '.protected')
        $nearbyWorkbook = Join-Path $peerRoot 'AI调试信息.xlsx'
        if (-not $workbook -and (Test-Path -LiteralPath $nearbyWorkbook)) { $workbook = $nearbyWorkbook }
        $hasCredential = Test-Path -LiteralPath $credentialFile
        $replaceWebhook = $false
        if ($Mode -eq 'Setup' -and $hasCredential) {
            Write-Host '已保存本人的 Webhook：1=复用；2=重新粘贴本人当前的完整 Webhook（输入隐藏）。'
            do { $credentialChoice = (Read-Host '直接回车复用，或输入 1 / 2').Trim() } while ($credentialChoice -notin @('','1','2'))
            $replaceWebhook = $credentialChoice -eq '2'
        }
        if ($replaceWebhook) {
            $arguments += '--paste-webhook'
        } elseif ($hasCredential) {
            Write-Host '已复用本机加密保存的本人 Webhook。'
        } elseif ($workbook) {
            Write-Host '已找到本机机器人表格，只导入所选本人的 Webhook。'
            $arguments += @('--workbook',$workbook)
        } else {
            Write-Host 'Webhook 来源：1=选择 Excel 表格；2=粘贴本人 Webhook（输入隐藏）。'
            do { $source = (Read-Host '输入 1 或 2').Trim() } while ($source -notin @('1','2'))
            if ($source -eq '1') {
                Add-Type -AssemblyName System.Windows.Forms
                $dialog = New-Object System.Windows.Forms.OpenFileDialog
                $dialog.Title = '选择 AI调试信息.xlsx'
                $dialog.Filter = 'Excel 工作簿 (*.xlsx)|*.xlsx'
                if ($dialog.ShowDialog() -ne [System.Windows.Forms.DialogResult]::OK) { throw '未选择机器人表格。' }
                $arguments += @('--workbook',$dialog.FileName)
                $dialog.Dispose()
            } else { $arguments += '--paste-webhook' }
        }
        Invoke-PeerPython $tools.python_path $arguments
        Write-Host '正在检查本机 Codex 是否能实际执行任务（不会向群里发送检查消息）...'
        Invoke-PeerPython $tools.python_path @('check')
        if ($NoStart) { exit 0 }
        $Mode = 'Start'
    }
    if ($NoStart) { exit 0 }
    $machine = Get-Content -LiteralPath $machineFile -Raw -Encoding UTF8 | ConvertFrom-Json
    if (-not $machine.PSObject.Properties['checked'] -or -not $machine.checked) {
        Invoke-PeerPython $tools.python_path @('check')
    }
    switch ($Mode) {
        'Start' { Invoke-PeerPython $tools.python_path @('start') }
        'Task' { Invoke-PeerPython $tools.python_path @('task') }
        'Resume' { Invoke-PeerPython $tools.python_path @('resume') }
        'EnableUpdates' {
            Invoke-PeerPython $tools.python_path @('enable-updates')
            Invoke-PeerPython $tools.python_path @('start')
        }
        'PublishUpdate' { Invoke-PeerPython $tools.python_path @('publish-update') }
        'StopTask' { Invoke-PeerPython $tools.python_path @('stop-task') }
        'Check' { Invoke-PeerPython $tools.python_path @('check') }
    }
    exit 0
} catch {
    Write-Host ''
    Write-Host ('未完成：' + $_.Exception.Message) -ForegroundColor Red
    Write-Host '无需卸载或重装原有环境。请保留窗口中的错误提示。'
    exit 1
}
