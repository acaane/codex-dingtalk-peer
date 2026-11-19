# 独立运行时组件

`windows-x64` 保存 Windows 完整部署包的回退运行时：Python 嵌入版、DWS 原生组件、Codex 原生组件。目录不进入 Git，当前工程已保留本机验证过的一份副本。

- 优先使用用户现有工具；这些组件仅用于未安装或指定独立运行时验证的场景。
- `build_package.py` 从此目录读取组件，打包到发行包的 `.tools`。
- 不从安装测试目录构建；不要复制账号目录、`.local`、`.tools/toolchain.json` 或运行日志进来。
- 上游许可证在工程根目录 `licenses`，Python 自带许可证保留在组件目录。
- 完整组件丢失时，可在独立目录运行 `install.ps1 -ToolsOnly -PortableOnly` 补齐，再按白名单整理；不要求覆盖或升级任何全局安装。
