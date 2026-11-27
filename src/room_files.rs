use std::{
    fs::File,
    io::{self, Read, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};
use sqlx::{Connection, SqliteConnection};

use crate::{room, storage};

const MAX_FILE_BYTES: u32 = 5 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub name: String,
    pub size: u32,
}

impl FileInfo {
    pub fn valid(&self) -> bool {
        self.size <= MAX_FILE_BYTES
            && !self.name.is_empty()
            && self.name.len() <= 1024
            && !self.name.ends_with([' ', '.'])
            && !self
                .name
                .chars()
                .any(|ch| ch.is_control() || "\\/:<>|\"?*".contains(ch))
    }
}

pub struct Upload {
    pub id: [u8; 16],
    pub info: FileInfo,
    pub bytes: Vec<u8>,
}

impl Upload {
    pub fn read(path: &Path) -> io::Result<Self> {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("无法读取文件名"))?;
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::other("请选择普通文件"));
        }
        if metadata.len() > u64::from(MAX_FILE_BYTES) {
            return Err(io::Error::other("单个文件最多为 5 MiB"));
        }
        let mut bytes = Vec::new();
        file.take(u64::from(MAX_FILE_BYTES) + 1)
            .read_to_end(&mut bytes)?;
        let info = FileInfo {
            name: name.into(),
            size: bytes.len() as u32,
        };
        if !info.valid() {
            return Err(io::Error::other("文件名无效，或文件超过 5 MiB"));
        }
        Ok(Self {
            id: room::random()?,
            info,
            bytes,
        })
    }
}

pub fn save_to(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let directory = path
        .parent()
        .ok_or_else(|| io::Error::other("缺少保存目录"))?;
    let mut staged = tempfile::NamedTempFile::new_in(directory)?;
    staged.write_all(bytes)?;
    staged.as_file().sync_all()?;
    staged
        .persist_noclobber(path)
        .map_err(|error| error.error)?;
    Ok(())
}

pub async fn initialize(db: &mut SqliteConnection) -> sqlx::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS room_files (
            message_id INTEGER PRIMARY KEY REFERENCES room_messages(id),
            name TEXT NOT NULL,
            data BLOB NOT NULL
        )",
    )
    .execute(db)
    .await?;
    Ok(())
}

pub async fn store(
    path: &Path,
    author: i64,
    id: [u8; 16],
    info: FileInfo,
    data: Vec<u8>,
) -> sqlx::Result<bool> {
    let mut db = storage::connect(path).await?;
    let mut tx = db.begin().await?;
    let message_id = sqlx::query_scalar::<_, i64>(
        "INSERT INTO room_messages (author, request_id, body) VALUES (?1, ?2, ?3)
         ON CONFLICT(author, request_id) DO NOTHING RETURNING id",
    )
    .bind(author)
    .bind(id.as_slice())
    .bind(format!("附件：{}", info.name))
    .fetch_optional(&mut *tx)
    .await?;
    // A retry must match the committed attachment. The chat row and bytes commit together.
    let confirmed = if let Some(message_id) = message_id {
        sqlx::query("INSERT INTO room_files (message_id, name, data) VALUES (?1, ?2, ?3)")
            .bind(message_id)
            .bind(info.name)
            .bind(data)
            .execute(&mut *tx)
            .await?;
        true
    } else {
        sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                 SELECT 1 FROM room_messages JOIN room_files ON room_files.message_id = room_messages.id
                 WHERE author = ?1 AND request_id = ?2 AND name = ?3 AND data = ?4
             )",
        )
        .bind(author)
        .bind(id.as_slice())
        .bind(info.name)
        .bind(data)
        .fetch_one(&mut *tx)
        .await? != 0
    };
    tx.commit().await?;
    Ok(confirmed)
}

pub async fn read(path: &Path, id: i64) -> sqlx::Result<Option<Vec<u8>>> {
    let mut db = storage::read_only(path).await?;
    sqlx::query_scalar("SELECT data FROM room_files WHERE message_id = ?1")
        .bind(id)
        .fetch_optional(&mut db)
        .await
}
