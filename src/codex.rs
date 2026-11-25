use std::{
    collections::VecDeque,
    env, fs,
    io::{self, BufRead, BufReader, Write},
    os::windows::process::CommandExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver},
    time::{Duration, Instant},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::readiness::{Failure, Id};

const CREATE_NO_WINDOW: u32 = 0x0800_0000;
const LIST_TIMEOUT: Duration = Duration::from_secs(12);
const PROBE_TIMEOUT: Duration = Duration::from_secs(90);
pub const COORDINATOR_NAME: &str = "AI 联调台协调会话";

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
    let mut server = AppServer::connect(project, LIST_TIMEOUT)?;

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

pub fn verify_session(project: &Path, thread: &str, challenge: Id) -> Result<Id, Failure> {
    let code = format!("PROBE-{}", URL_SAFE_NO_PAD.encode(challenge));
    let prompt = format!(
        "这是 AI 联调台的只读接单验证。请不要读取文件、运行命令或调用工具。只原样回复以下一行（不要加引号）：{code}"
    );
    let answer = run_readonly(project, thread, &prompt, PROBE_TIMEOUT)?;
    if answer.trim() != code {
        return Err(Failure::Reply);
    }
    Ok(challenge)
}

pub fn run_task(
    project: &Path,
    thread: &str,
    goal: &str,
    id: Id,
    timeout: Duration,
) -> Result<String, Failure> {
    let id = URL_SAFE_NO_PAD.encode(id);
    let prompt = format!(
        "请执行 AI 联调台派给本会话的只读联调任务 {id}。范围限当前绑定工程。\n+         仅做只读检查。不要修改文件、启停业务服务、操作设备或联系外部系统。\n+         目标需要额外权限时请描述阻塞。最终用中文回报发现、证据和未完成项，勿回传凭据或秘密。\n\n+         本次任务目标：\n{goal}"
    );
    run_readonly(project, thread, &prompt, timeout).map(crate::tasks::bounded_report)
}

fn run_readonly(
    project: &Path,
    thread: &str,
    prompt: &str,
    timeout: Duration,
) -> Result<String, Failure> {
    let mut server =
        AppServer::connect(project, timeout).map_err(|error| classify(error, Failure::Session))?;
    let resumed = server
        .call(
            "thread/resume",
            json!({
                "threadId": thread, "cwd": project, "excludeTurns": true,
                "sandbox": "read-only", "approvalPolicy": "never"
            }),
        )
        .map_err(|error| classify(error, Failure::Session))?;
    if resumed["thread"]["id"].as_str() != Some(thread) {
        return Err(Failure::Session);
    }
    if resumed["thread"]["status"]["type"] == "active" {
        return Err(Failure::Busy);
    }
    let started = server
        .call(
            "turn/start",
            json!({
                "threadId": thread,
                "input": [{"type": "text", "text": prompt, "text_elements": []}],
                "approvalPolicy": "never",
                "sandboxPolicy": {"type": "readOnly", "networkAccess": false}
            }),
        )
        .map_err(|error| classify(error, Failure::Model))?;
    let turn = started["turn"]["id"].as_str().ok_or(Failure::Model)?;
    server.completed_answer(thread, turn)
}

fn classify(error: io::Error, stage: Failure) -> Failure {
    if error.kind() == io::ErrorKind::TimedOut {
        Failure::Timeout
    } else {
        stage
    }
}

pub struct AppServer {
    process: Child,
    replies: Receiver<Value>,
    seq: u64,
    deadline: Instant,
    events: VecDeque<Value>,
}

impl AppServer {
    pub fn connect(project: &Path, timeout: Duration) -> io::Result<Self> {
        let deadline = Instant::now() + timeout;
        let mut process = Command::new(executable()?)
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
                    && (message.get("id").is_some()
                        || matches!(
                            message["method"].as_str(),
                            Some("item/completed" | "turn/completed")
                        ))
                    && sender.send(message).is_err()
                {
                    break;
                }
            }
        });
        let mut server = Self {
            process,
            replies,
            seq: 0,
            deadline,
            events: VecDeque::new(),
        };
        server.call(
            "initialize",
            json!({"clientInfo": {"name": "ai_integration_desk", "version": env!("CARGO_PKG_VERSION")}}),
        )?;
        server.send(json!({"method": "initialized"}))?;
        Ok(server)
    }

    pub fn create_coordinator(&mut self, project: &Path) -> io::Result<String> {
        let started = self.call(
            "thread/start",
            json!({"cwd": project, "ephemeral": false, "sandbox": "read-only"}),
        )?;
        let id = started["thread"]["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| io::Error::other("Codex 未返回新会话编号"))?;
        // Naming persists the empty session so it can be resumed before its first turn.
        self.call(
            "thread/name/set",
            json!({"threadId": id, "name": COORDINATOR_NAME}),
        )
        .map_err(io::Error::other)?;
        Ok(id.to_owned())
    }

    fn send(&mut self, message: Value) -> io::Result<()> {
        let stdin = self.process.stdin.as_mut().expect("已设置子进程输入管道");
        serde_json::to_writer(&mut *stdin, &message).map_err(io::Error::other)?;
        stdin.write_all(b"\n")?;
        stdin.flush()
    }

    fn call(&mut self, method: &str, params: Value) -> io::Result<Value> {
        if Instant::now() >= self.deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "等待 Codex 响应超时",
            ));
        }
        self.seq += 1;
        let id = self.seq;
        self.send(json!({"method": method, "id": id, "params": params}))?;
        loop {
            let mut message = self.receive()?;
            if message.get("id").and_then(Value::as_u64) != Some(id) {
                if method == "turn/start" && message.get("method").is_some() {
                    // A fast turn can report its items before the RPC acknowledgement arrives.
                    self.events.push_back(message);
                }
                continue;
            }
            if message.get("error").is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("本机 Codex 接口 {method} 拒绝了请求"),
                ));
            }
            return message
                .get_mut("result")
                .map(Value::take)
                .ok_or_else(|| io::Error::other(format!("本机 Codex 接口 {method} 回执缺少结果")));
        }
    }

    fn receive(&self) -> io::Result<Value> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "等待 Codex 响应超时",
            ));
        }
        let message = self
            .replies
            .recv_timeout(left)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => {
                    io::Error::new(io::ErrorKind::TimedOut, "等待 Codex 响应超时")
                }
                mpsc::RecvTimeoutError::Disconnected => io::Error::other("Codex 后台进程已退出"),
            })?;
        if message.get("id").is_some() && message.get("method").is_some() {
            return Err(io::Error::other("Codex 要求本机确认，请检查当前登录或配置"));
        }
        Ok(message)
    }

    fn completed_answer(&mut self, thread: &str, turn: &str) -> Result<String, Failure> {
        let mut answer = None;
        loop {
            let message = match self
                .events
                .pop_front()
                .map(Ok)
                .unwrap_or_else(|| self.receive())
            {
                Ok(message) => message,
                Err(error) => {
                    let _ = self.send(json!({"id": self.seq + 1, "method": "turn/interrupt", "params": {"threadId": thread, "turnId": turn}}));
                    return Err(classify(error, Failure::Model));
                }
            };
            let params = &message["params"];
            if params["threadId"].as_str() != Some(thread) {
                continue;
            }
            match message["method"].as_str() {
                Some("item/completed") if params["turnId"].as_str() == Some(turn) => {
                    let item = &params["item"];
                    if item["type"] == "agentMessage" && item["phase"] != "commentary" {
                        answer = item["text"]
                            .as_str()
                            .filter(|text| !text.trim().is_empty())
                            .map(str::to_owned);
                    }
                }
                Some("turn/completed") if params["turn"]["id"].as_str() == Some(turn) => {
                    if params["turn"]["status"] != "completed" {
                        return Err(Failure::Model);
                    }
                    return answer.ok_or(Failure::Reply);
                }
                _ => {}
            }
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
