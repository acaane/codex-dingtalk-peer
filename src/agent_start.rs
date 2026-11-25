use std::{env, ffi::OsString, io, path::Path};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_lite::future::block_on;
use serde::{Deserialize, Serialize};

use crate::{
    agent_binding::Binding,
    room::{RoomClient, Secret},
    storage,
    tasks::{self, Action, Submission},
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Plan {
    pub submission: Submission,
    pub binding: Secret,
}

impl Plan {
    pub async fn load(directory: &Path, room: &str) -> io::Result<Option<Self>> {
        let path = directory.join("agent-start.sqlite3");
        if !path.try_exists()? {
            return Ok(None);
        }
        let mut db = storage::read_only(&path).await.map_err(io::Error::other)?;
        sqlx::query_scalar::<_, Vec<u8>>("SELECT data FROM start_plans WHERE room = ?1")
            .bind(room)
            .fetch_optional(&mut db)
            .await
            .map_err(io::Error::other)?
            .map(|data| serde_json::from_slice(&data).map_err(io::Error::other))
            .transpose()
    }

    pub async fn save(&self, directory: &Path, room: &str) -> io::Result<()> {
        let mut db = storage::connect(&directory.join("agent-start.sqlite3"))
            .await
            .map_err(io::Error::other)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS start_plans (room TEXT PRIMARY KEY, data BLOB NOT NULL)",
        )
        .execute(&mut db)
        .await
        .map_err(io::Error::other)?;
        sqlx::query(
            "INSERT INTO start_plans (room, data) VALUES (?1, ?2)
                     ON CONFLICT(room) DO UPDATE SET data = excluded.data",
        )
        .bind(room)
        .bind(serde_json::to_vec(self).map_err(io::Error::other)?)
        .execute(&mut db)
        .await
        .map_err(io::Error::other)?;
        Ok(())
    }
}

pub fn instructions(room: &str) -> io::Result<String> {
    let exe = env::current_exe()?;
    let exe = exe.to_string_lossy().replace("'", "''");
    Ok(format!(
        "请记住这条本项目会话的联调启动说明。只有我明确说「开始联调」时，才在本会话运行下列 PowerShell 命令一次。收到 accepted 后告诉我 task_id。若有错误，告诉我原因并等待我处理。\n\n& '{exe}' start-integration '{room}' | Out-String"
    ))
}

pub fn command(args: &[OsString]) -> io::Result<String> {
    let [command, room] = args else {
        return Err(io::Error::other("用法：start-integration <房间编号>"));
    };
    if command != "start-integration" {
        return Err(io::Error::other("用法：start-integration <房间编号>"));
    }
    let room = room
        .to_str()
        .ok_or_else(|| io::Error::other("房间编号无效"))?;
    let thread = env::var("CODEX_THREAD_ID")
        .map_err(|_| io::Error::other("请在主协调端绑定的 Codex 会话中启动联调"))?;
    let directory = storage::data_directory()?;
    start(&directory, room, &thread)
}

fn start(directory: &Path, room: &str, thread: &str) -> io::Result<String> {
    let plan = block_on(Plan::load(directory, room))?
        .ok_or_else(|| io::Error::other("联调台尚未登记开始联调的目标和时限"))?;
    let binding = block_on(Binding::read_existing(directory, room))
        .map_err(io::Error::other)?
        .filter(|binding| binding.thread_id == thread)
        .ok_or_else(|| io::Error::other("当前 Codex 会话与本房间的主协调会话不同"))?;
    if binding.fingerprint() != plan.binding {
        return Err(io::Error::other("主协调会话已更换，请在联调台重新登记"));
    }
    let client = RoomClient::for_saved_host(directory, room)?;
    let id = plan.submission.id;
    // Keep the saved ID even if the first confirmation was lost.
    let update = client
        .exchange_room(
            i64::MAX,
            None,
            None,
            None,
            Some(tasks::Request {
                action: Some(Action::Submit(plan.submission)),
                binding: Some(plan.binding),
            }),
        )
        .map_err(|error| {
            io::Error::other(format!(
                "未确认派单结果。请检查主协调端房间是否打开。{error}"
            ))
        })?;
    let update = update
        .task
        .ok_or_else(|| io::Error::other("请在主协调端打开支持 agent 启动的联调台"))?;
    if let Some(error) = update.error {
        return Err(io::Error::other(error));
    }
    if update.acknowledged != Some(id) {
        return Err(io::Error::other("主机尚未确认已登记的任务"));
    }
    Ok(serde_json::json!({"status":"accepted","task_id":URL_SAFE_NO_PAD.encode(id)}).to_string())
}
