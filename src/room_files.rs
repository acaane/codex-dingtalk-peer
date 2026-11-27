use std::{
    collections::HashSet,
    fs::{self, File},
    io::{self, Cursor, Read, Write},
    path::{Path, PathBuf},
    time::SystemTime,
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, SqliteConnection};
use tempfile::NamedTempFile;

use crate::{room, storage};

const MAX_FILE_BYTES: u32 = 1024 * 1024 * 1024;

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

#[derive(Clone)]
pub struct Upload {
    pub id: [u8; 16],
    pub info: FileInfo,
    path: PathBuf,
    modified: Option<SystemTime>,
}

impl Upload {
    pub fn select(path: PathBuf) -> io::Result<Self> {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("无法读取文件名"))?;
        let metadata = fs::metadata(&path)?;
        if !metadata.is_file() {
            return Err(io::Error::other("请选择普通文件"));
        }
        if metadata.len() > u64::from(MAX_FILE_BYTES) {
            return Err(io::Error::other("单个文件最多为 1 GB"));
        }
        let info = FileInfo {
            name: name.into(),
            size: metadata.len() as u32,
        };
        if !info.valid() {
            return Err(io::Error::other("文件名无效"));
        }
        Ok(Self {
            id: room::random()?,
            info,
            path,
            modified: metadata.modified().ok(),
        })
    }

    pub fn open(&self) -> io::Result<File> {
        let input = File::open(&self.path)?;
        let metadata = input.metadata()?;
        if !metadata.is_file()
            || metadata.len() != u64::from(self.info.size)
            || metadata.modified().ok() != self.modified
        {
            return Err(io::Error::other("文件已更改，请移除后重新选择"));
        }
        Ok(input)
    }
}

pub fn copy_exact(input: &mut impl Read, output: &mut impl Write, size: u32) -> io::Result<()> {
    if io::copy(&mut input.take(u64::from(size)), output)? != u64::from(size) {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "附件传输中断"));
    }
    Ok(())
}

pub fn save_to(path: &Path, input: &mut impl Read, size: u32) -> io::Result<()> {
    let directory = path
        .parent()
        .ok_or_else(|| io::Error::other("缺少保存目录"))?;
    let mut staged = NamedTempFile::new_in(directory)?;
    copy_exact(input, &mut staged, size)?;
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
    .execute(&mut *db)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS room_file_streams (
            message_id INTEGER PRIMARY KEY REFERENCES room_messages(id),
            name TEXT NOT NULL,
            size INTEGER NOT NULL,
            digest BLOB NOT NULL
        )",
    )
    .execute(db)
    .await?;
    Ok(())
}

pub struct StagedFile {
    file: NamedTempFile,
    digest: [u8; 32],
}

pub fn receive(database: &Path, input: &mut impl Read, size: u32) -> io::Result<StagedFile> {
    let directory = database.with_extension("files");
    fs::create_dir_all(&directory)?;
    let mut file = NamedTempFile::new_in(directory)?;
    let mut digest = Context::new(&SHA256);
    let mut left = size as usize;
    let mut buffer = vec![0; 64 * 1024];
    while left != 0 {
        let chunk = left.min(buffer.len());
        let bytes = &mut buffer[..chunk];
        input.read_exact(bytes)?;
        file.write_all(bytes)?;
        digest.update(bytes);
        left -= chunk;
    }
    file.as_file().sync_all()?;
    let mut key = [0; 32];
    key.copy_from_slice(digest.finish().as_ref());
    Ok(StagedFile { file, digest: key })
}

fn blob_path(database: &Path, digest: [u8; 32]) -> PathBuf {
    database
        .with_extension("files")
        .join(URL_SAFE_NO_PAD.encode(digest))
}

pub async fn store(
    path: &Path,
    author: i64,
    id: [u8; 16],
    info: FileInfo,
    staged: StagedFile,
) -> io::Result<bool> {
    let mut db = storage::connect(path).await.map_err(io::Error::other)?;
    let mut tx = db.begin().await.map_err(io::Error::other)?;
    let message_id = sqlx::query_scalar::<_, i64>(
        "INSERT INTO room_messages (author, request_id, body) VALUES (?1, ?2, ?3)
         ON CONFLICT(author, request_id) DO NOTHING RETURNING id",
    )
    .bind(author)
    .bind(id)
    .bind(format!("附件：{}", info.name))
    .fetch_optional(&mut *tx)
    .await
    .map_err(io::Error::other)?;
    let confirmed = if let Some(message_id) = message_id {
        sqlx::query("INSERT INTO room_file_streams (message_id, name, size, digest) VALUES (?1, ?2, ?3, ?4)")
            .bind(message_id).bind(info.name).bind(info.size).bind(staged.digest)
            .execute(&mut *tx).await.map_err(io::Error::other)?;
        // Publish complete bytes before the chat row can become visible.
        let destination = blob_path(path, staged.digest);
        if let Err(error) = staged.file.persist_noclobber(&destination)
            && error.error.kind() != io::ErrorKind::AlreadyExists
        {
            return Err(error.error);
        }
        true
    } else {
        sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                 SELECT 1 FROM room_messages JOIN room_file_streams ON message_id = room_messages.id
                 WHERE author = ?1 AND request_id = ?2 AND name = ?3 AND size = ?4 AND digest = ?5
             )",
        )
        .bind(author)
        .bind(id)
        .bind(info.name)
        .bind(info.size)
        .bind(staged.digest)
        .fetch_one(&mut *tx)
        .await
        .map_err(io::Error::other)?
            != 0
    };
    tx.commit().await.map_err(io::Error::other)?;
    Ok(confirmed)
}

pub async fn read(path: &Path, id: i64) -> io::Result<Option<(u32, Box<dyn Read>)>> {
    let mut db = storage::read_only(path).await.map_err(io::Error::other)?;
    let recorded = sqlx::query_as::<_, (u32, [u8; 32])>(
        "SELECT size, digest FROM room_file_streams WHERE message_id = ?1",
    )
    .bind(id)
    .fetch_optional(&mut db)
    .await
    .map_err(io::Error::other)?;
    if let Some((size, digest)) = recorded {
        let file = File::open(blob_path(path, digest))?;
        if file.metadata()?.len() != u64::from(size) {
            return Err(io::Error::other("主机附件大小与记录不符"));
        }
        return Ok(Some((size, Box::new(file))));
    }
    // Earlier versions stored small attachments directly in SQLite.
    let legacy =
        sqlx::query_scalar::<_, Vec<u8>>("SELECT data FROM room_files WHERE message_id = ?1")
            .bind(id)
            .fetch_optional(&mut db)
            .await
            .map_err(io::Error::other)?;
    Ok(legacy.map(|bytes| {
        (
            bytes.len() as u32,
            Box::new(Cursor::new(bytes)) as Box<dyn Read>,
        )
    }))
}

pub struct LocalSaves {
    pub saved: HashSet<i64>,
    pub directory: Option<PathBuf>,
}

impl LocalSaves {
    pub async fn load(room: &str) -> io::Result<Self> {
        let mut db = open_local_saves().await?;
        let saved =
            sqlx::query_scalar::<_, i64>("SELECT message_id FROM local_saves WHERE room = ?1")
                .bind(room)
                .fetch_all(&mut db)
                .await
                .map_err(io::Error::other)?;
        let last = sqlx::query_scalar::<_, String>(
            "SELECT directory FROM local_saves ORDER BY saved_order DESC LIMIT 1",
        )
        .fetch_optional(&mut db)
        .await
        .map_err(io::Error::other)?;
        Ok(Self {
            saved: saved.into_iter().collect(),
            directory: last.map(PathBuf::from),
        })
    }

    pub async fn record(room: &str, message_id: i64, directory: &Path) -> io::Result<()> {
        let mut db = open_local_saves().await?;
        sqlx::query(
            "INSERT INTO local_saves (room, message_id, directory, saved_order)
             VALUES (?1, ?2, ?3, (SELECT COALESCE(MAX(saved_order), 0) + 1 FROM local_saves))
             ON CONFLICT(room, message_id) DO UPDATE SET
                 directory = excluded.directory, saved_order = excluded.saved_order",
        )
        .bind(room)
        .bind(message_id)
        .bind(directory.to_string_lossy().as_ref())
        .execute(&mut db)
        .await
        .map_err(io::Error::other)?;
        Ok(())
    }
}

async fn open_local_saves() -> io::Result<SqliteConnection> {
    let mut db = storage::connect(&storage::data_directory()?.join("attachment-saves.sqlite3"))
        .await
        .map_err(io::Error::other)?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS local_saves (
            room TEXT NOT NULL, message_id INTEGER NOT NULL, directory TEXT NOT NULL,
            saved_order INTEGER NOT NULL, PRIMARY KEY (room, message_id)
        )",
    )
    .execute(&mut db)
    .await
    .map_err(io::Error::other)?;
    Ok(db)
}
