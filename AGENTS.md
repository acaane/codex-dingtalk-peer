# Codex DingTalk Peer 工程约定

开始工作先读 `README.md`、`HANDOFF.md`。正式根目录是 `D:\project\codex-dingtalk-peer`。

## 用户已确定的要求

- 多位同事在各自电脑运行 Codex，通过钉钉群进行联调交流、展示过程并结束任务。先两端，后续考虑多端。
- 连接器独立运行，不给业务程序加入通信代码，不依赖公司专门协调服务器。
- 当前采用个人 DWS 消息监听 + 群自定义 Webhook 机器人；企业应用发布/领导审批路线已放弃，勿自行恢复。
- 一键部署优先复用本人当前可用的 Python、DWS、Codex 等工具，读取实际版本及能力。不强制升级/降级，不卸载，不更改全局 PATH 或原有工具配置。缺少环境时使用工程/包内独立组件。
- 当前默认只读项目；真实业务控制或代码修改须按后续明确任务实施，不能将模拟成功报告为业务联调成功。

## 文件与安全边界

- `.local` 保存本机部署配置和 Windows DPAPI 加密 Webhook；同事各自登录、各自配置。严禁输出实际 Webhook/token、AppSecret 或登录凭据。
- `.local`、`.tools`、`runtime`、`archive`、`vendor/windows-x64`、`dist` 不进入 Git。发行包通过 `build_package.py` 白名单生成，不直接压缩整个目录。
- 业务工程与用户提供的 Excel 均在本工程外，仅按授权读取，不挪动或改写。原有 Codex/DWS 安装与账号数据属于共享环境，清理时不得删除。
- Windows PowerShell 5.1 脚本保留 UTF-8 BOM；CMD 启动器保持 ASCII 内容。路径需支持中文、空格；PowerShell 文件操作用 `-LiteralPath`。递归清理前验证绝对目标与工程范围。
- 机器人身份以实际发送者 ID 核对，不按名称或消息中的 A/B 标识授权。不要改用个人身份替代机器人发送。

## 验证与发布

- 本地单元测试：`python -m unittest -v`；也可使用 `vendor/windows-x64/python-3.13.16/python.exe`。
- 环境检查：`powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\install.ps1 -ToolsOnly`。
- 包内运行时检查加 `-PortableOnly`；不会登录或向群发送消息。
- `check.cmd` 会校验账号、群身份并进行一次小规模 Codex 实际调用。`demo.py` 会在已授权测试群中发送模拟任务，两个监听有界运行 120 秒。
- 发行构建：Python 3.11+ 执行 `python build_package.py`。原生运行时来源目录为 `vendor/windows-x64`，最终包位于 `dist`。
- 不为迁移或文档更新重复开展真实业务测试；有代码变动时执行有针对性的检查。不要无限重试失败的服务写入或重发投递未知的消息。

## 交接

更新 `HANDOFF.md` 的完成状态、已验证范围、未完成事项和关键入口。真实双机双账号通信已于 2026-10-23 通过，任务 `c7ce78088bb4`。下一位 agent 优先确认双方真实项目与接口，再进行只读业务对比；不要重新把部署与基础通信当成未验证事项。
