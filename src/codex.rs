use std::{
    env, fs,
    io::{self, BufRead, BufReader, Write},
    os::windows::process::CommandExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver},
    time::{Duration, Instant},
};

use serde::Deserialize;
use serde_json::{Value, json};

const CREATE_NO_WINDOW: u32 = 0x0800_0000;
const LIST_TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Clone, Deserialize)]
pub struct Candidate {
    pub id: String,
    cwd: PathBuf,
    name: Option<String>,
    preview: String,
}

impl Candidate {
    pub fn label(&self) -> String {
        let name = self.name.as_deref().filter(|name| !name.trim().is_empty());
        let label = name.unwrap_or(&self.preview).trim();
        let short: String = label.split_whitespace().collect::<Vec<_>>().join(" ");
        if short.is_empty() {
            "未命名会话".into()
        } else {
            short.chars().take(46).collect()
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page {
    data: Vec<Candidate>,
    next_cursor: Option<String>,
}

pub fn project_sessions(project: &Path) -> io::Result<Vec<Candidate>> {
    let directory = project.canonicalize()?;
    let executable = executable()?;
    let mut server = AppServer::start(&executable, project)?;
    server.call(
        "initialize",
        json!({"clientInfo": {"name": "ai_integration_desk", "version": env!("CARGO_PKG_VERSION")}}),
    )?;
    server.send(json!({"method": "initialized"}))?;

    let mut candidates = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let response = server.call(
            "thread/list",
            json!({
                "cwd": project,
                "sourceKinds": ["cli", "vscode", "appServer"],
                "modelProviders": [],
                "useStateDbOnly": true,
                "limit": 50,
                "sortKey": "updated_at",
                "cursor": cursor
            }),
        )?;
        let page: Page = serde_json::from_value(response)
            .map_err(|_| io::Error::other("当前 Codex 返回了无法识别的会话列表"))?;
        candidates.extend(
            page.data
                .into_iter()
                .filter(|entry| entry.cwd.canonicalize().is_ok_and(|path| path == directory)),
        );
        let Some(next) = page.next_cursor else {
            return Ok(candidates);
        };
        if cursor.as_deref() == Some(&next) {
            return Err(io::Error::other("Codex 会话列表未能继续读取"));
        }
        cursor = Some(next);
    }
}

fn executable() -> io::Result<PathBuf> {
    if let Some(path) = env::var_os("CODEX_APP_RUNTIME_CLI_PATH").map(PathBuf::from)
        && path.is_file()
    {
        return Ok(path);
    }
    if let Some(paths) = env::var_os("PATH")
        && let Some(path) = env::split_paths(&paths)
            .filter(|path| path.is_absolute())
            .map(|path| path.join("codex.exe"))
            .find(|path| path.is_file())
    {
        return Ok(path);
    }
    if let Some(root) = env::var_os("SystemRoot") {
        let powershell = PathBuf::from(root).join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let output = Command::new(powershell)
            .creation_flags(CREATE_NO_WINDOW)
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[Console]::OutputEncoding = [Text.Encoding]::UTF8; (Get-AppxPackage -Name OpenAI.Codex).InstallLocation",
            ])
            .stderr(Stdio::null())
            .output()?;
        let path = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim())
            .join("app/resources/codex.exe");
        if output.status.success() && path.is_absolute() && fs::metadata(&path).is_ok() {
            return Ok(path);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "未找到本机 Codex，请确认已安装 Codex 桌面版或 Codex CLI",
    ))
}

struct AppServer {
    process: Child,
    replies: Receiver<Value>,
    seq: u64,
    deadline: Instant,
}

impl AppServer {
    fn start(executable: &Path, project: &Path) -> io::Result<Self> {
        let mut process = Command::new(executable)
            .args(["app-server", "--listen", "stdio://"])
            .current_dir(project)
            .creation_flags(CREATE_NO_WINDOW)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let stdout = process.stdout.take().expect("已设置子进程输出管道");
        let (sender, replies) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Ok(message) = serde_json::from_str::<Value>(&line)
                    && message.get("id").is_some()
                    && sender.send(message).is_err()
                {
                    break;
                }
            }
        });
        Ok(Self {
            process,
            replies,
            seq: 0,
            deadline: Instant::now() + LIST_TIMEOUT,
        })
    }

    fn send(&mut self, message: Value) -> io::Result<()> {
        let stdin = self.process.stdin.as_mut().expect("已设置子进程输入管道");
        serde_json::to_writer(&mut *stdin, &message).map_err(io::Error::other)?;
        stdin.write_all(b"\n")?;
        stdin.flush()
    }

    fn call(&mut self, method: &str, params: Value) -> io::Result<Value> {
        self.seq += 1;
        let id = self.seq;
        self.send(json!({"method": method, "id": id, "params": params}))?;
        loop {
            let left = self.deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(io::Error::other("查询 Codex 会话超时"));
            }
            let mut message = self
                .replies
                .recv_timeout(left)
                .map_err(|error| match error {
                    mpsc::RecvTimeoutError::Timeout => io::Error::other("查询 Codex 会话超时"),
                    mpsc::RecvTimeoutError::Disconnected => {
                        io::Error::other("Codex 后台进程已退出")
                    }
                })?;
            if message.get("method").is_some() {
                return Err(io::Error::other("Codex 要求本机确认，请检查当前登录或配置"));
            }
            if message.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            return message
                .get_mut("result")
                .map(Value::take)
                .ok_or_else(|| io::Error::other(format!("本机 Codex 接口 {method} 查询失败")));
        }
    }
}

impl Drop for AppServer {
    fn drop(&mut self) {
        drop(self.process.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if matches!(self.process.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}
