# AI 联调连接器工程约定

- 开工先读 `README.md`，核对本地变更和已有检查。重构从独立 `master` 建开发分支，推送远程同名分支，经 GitHub PR 合并后拉取 `master`。Python v1.0 使用 `main`。
- 每次实现一个可验收的功能。核对正常操作路径、失败恢复和实际差异，记录已验证范围。对外发布和业务写入按用户授权执行。
- 桌面版采用 Rust 与 GPUI Kit。Windows 首次交付为默认动态链接的单 EXE。工具、账号和运行时优先沿用本机现有环境。
- 业务工程默认只读。使用合成数据和独立目录验证。密钥、登录信息、个人配置、运行数据库、测试证据和本机交接均留在本机。
- 构建和测试入口见 `README.md`。脚本随功能归入 `scripts/setup`、`scripts/run`、`scripts/diagnostics` 或 `scripts/release`。Windows PowerShell 5.1 脚本使用 UTF-8 BOM，文件操作支持中文及空格。
