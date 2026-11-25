use std::{io, path::Path, time::Duration};

use sqlx::{Connection, SqliteConnection};

use crate::{codex, storage};

pub struct Binding {
    pub project: String,
    pub thread_id: String,
    pub label: String,
}

impl Binding {
    pub async fn load(directory: &Path, room_id: &str) -> sqlx::Result<Option<Self>> {
        let mut db = open_bindings(directory).await?;
        Self::load_from(&mut db, room_id).await
    }

    pub async fn save(&self, directory: &Path, room_id: &str) -> sqlx::Result<()> {
        let mut db = open_bindings(directory).await?;
        let mut transaction = db.begin().await?;
        self.write(&mut transaction, room_id).await?;
        sqlx::query("DELETE FROM agent_creation WHERE room_id = ?1")
            .bind(room_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await
    }

    pub async fn ensure(directory: &Path, room_id: &str, project: &Path) -> io::Result<Self> {
        let canonical = project.canonicalize()?;
        let to_store = project.to_string_lossy().into_owned();
        let mut db = open_bindings(directory).await.map_err(io::Error::other)?;
        if let Some(saved) = Self::load_from(&mut db, room_id)
            .await
            .map_err(io::Error::other)?
            && saved.matches(&canonical)
        {
            return Ok(saved);
        }

        let mut codex = codex::AppServer::connect(project, Duration::from_secs(45))?;
        let mut transaction = db
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(io::Error::other)?;
        if let Some(saved) = Self::load_from(&mut transaction, room_id)
            .await
            .map_err(io::Error::other)?
            && saved.matches(&canonical)
        {
            return Ok(saved);
        }
        // Reserve before calling Codex so an interrupted request can't create a second session.
        let request = crate::room::random::<16>()?;
        let reserved = sqlx::query(
            "INSERT INTO agent_creation (room_id, request_id) VALUES (?1, ?2)
             ON CONFLICT(room_id) DO NOTHING",
        )
        .bind(room_id)
        .bind(request.as_slice())
        .execute(&mut *transaction)
        .await
        .map_err(io::Error::other)?;
        transaction.commit().await.map_err(io::Error::other)?;
        if reserved.rows_affected() == 0 {
            return Err(io::Error::other(
                "本轮已有创建请求。请稍后再试，或选择已有会话恢复绑定。",
            ));
        }

        let thread_id = match codex.create_coordinator(project) {
            Ok(id) => id,
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
                clear_creation(&mut db, room_id, &request)
                    .await
                    .map_err(io::Error::other)?;
                return Err(error);
            }
            Err(error) => {
                return Err(io::Error::other(format!(
                    "创建回执待核实。可通过「选择已有会话」恢复绑定。{error}"
                )));
            }
        };
        let saved = Self {
            project: to_store,
            thread_id,
            label: codex::COORDINATOR_NAME.into(),
        };
        let mut transaction = db.begin().await.map_err(io::Error::other)?;
        // A manual binding saved in another window can supersede this request.
        if !clear_creation(&mut transaction, room_id, &request)
            .await
            .map_err(io::Error::other)?
        {
            return Err(io::Error::other(
                "本轮绑定已在另一窗口更新，请重新打开会话面板",
            ));
        }
        saved
            .write(&mut transaction, room_id)
            .await
            .map_err(io::Error::other)?;
        transaction.commit().await.map_err(io::Error::other)?;
        Ok(saved)
    }

    fn matches(&self, canonical: &Path) -> bool {
        Path::new(&self.project)
            .canonicalize()
            .is_ok_and(|path| path == canonical)
    }

    async fn load_from(db: &mut SqliteConnection, room_id: &str) -> sqlx::Result<Option<Self>> {
        let saved: Option<(String, String, String)> = sqlx::query_as(
            "SELECT project, thread_id, label FROM agent_bindings WHERE room_id = ?1",
        )
        .bind(room_id)
        .fetch_optional(db)
        .await?;
        Ok(saved.map(|(project, thread_id, label)| Self {
            project,
            thread_id,
            label,
        }))
    }

    async fn write(&self, db: &mut SqliteConnection, room_id: &str) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO agent_bindings (room_id, project, thread_id, label) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(room_id) DO UPDATE SET
                project = excluded.project, thread_id = excluded.thread_id, label = excluded.label",
        )
        .bind(room_id)
        .bind(&self.project)
        .bind(&self.thread_id)
        .bind(&self.label)
        .execute(db)
        .await?;
        Ok(())
    }
}

async fn clear_creation(
    db: &mut SqliteConnection,
    room_id: &str,
    request: &[u8; 16],
) -> sqlx::Result<bool> {
    let result = sqlx::query("DELETE FROM agent_creation WHERE room_id = ?1 AND request_id = ?2")
        .bind(room_id)
        .bind(request.as_slice())
        .execute(db)
        .await?;
    Ok(result.rows_affected() != 0)
}

async fn open_bindings(directory: &Path) -> sqlx::Result<SqliteConnection> {
    let mut db = storage::connect(&directory.join("agent-bindings.sqlite3")).await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS agent_bindings (
            room_id TEXT PRIMARY KEY,
            project TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            label TEXT NOT NULL
        )",
    )
    .execute(&mut db)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS agent_creation (
            room_id TEXT PRIMARY KEY,
            request_id BLOB NOT NULL
        )",
    )
    .execute(&mut db)
    .await?;
    Ok(db)
}
