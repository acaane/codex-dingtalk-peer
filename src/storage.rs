use std::{
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};

pub(crate) fn data_directory() -> io::Result<PathBuf> {
    let app_data = std::env::var_os("LOCALAPPDATA")
        .filter(|directory| !directory.is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "Windows 未提供 LOCALAPPDATA 数据目录",
            )
        })?;
    let directory = PathBuf::from(app_data).join("CodexDingTalkPeer");
    std::fs::create_dir_all(&directory)?;
    Ok(directory)
}

pub(crate) async fn connect(path: &Path) -> sqlx::Result<SqliteConnection> {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .busy_timeout(Duration::from_millis(250)),
    )
    .await
}
