# AI 联调连接器

两位同事各自在 Windows 上使用自己的工程助手，在同一个联调窗口查看聊天、任务和回报。项目独立于业务程序运行。Python v1.0 位于 `main`，本项目的开发主线是 `master`。

## 当前可用

打开窗口后在「本机聊天」输入消息并点击「发送」。有效消息保存在当前 Windows 用户的 `%LOCALAPPDATA%\CodexDingTalkPeer\chat.sqlite3`。关闭后重新打开，窗口会恢复本机记录。纯空白输入会收到提示。保存失败时，输入框保留草稿以便重试。

本机聊天是第一个可独立验收的功能。局域网通信、邀请和任务执行将在后续阶段接入。

## 首版范围

- 主协调电脑在内网托管房间。另一位同事导入邀请，确认本机工程目录并绑定已打开的项目助手。公司服务器托管作为后续选项。
- 双方绑定后先做一次只读接单和回报。就绪后由发起人对自己的助手下达开始指令，按登记的目标、权限和截止时间推进。窗口显示进度、阻塞和处理人。
- 房间保留聊天、任务和基础文件收发记录。到点写检查点，下次启动可以继续上次联调或打开最近联调。
- 首个试用包覆盖两端内网使用。多人、可选钉钉通道和其他模型适配分别验收。

## 构建与检查

在 Windows x64 上打开 Visual Studio Developer PowerShell，使用 Rust MSVC、Windows SDK 和 CMake。

```powershell
cargo fmt --check
cargo test --release --locked
cargo build --release --locked
```

发布构建得到 `target/release/codex-dingtalk-peer.exe`。Windows 交付使用默认动态链接的单 EXE，目标机明确缺运行库时再补。

手动验证时，打开窗口，发一条含中文和换行的消息，确认它出现在历史中。尝试发空白消息，再关闭并重开，核对消息内容和顺序。
