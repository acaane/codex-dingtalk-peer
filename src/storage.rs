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

pub(crate) async fn read_only(path: &Path) -> sqlx::Result<SqliteConnection> {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .read_only(true)
            .busy_timeout(Duration::from_millis(250)),
    )
    .await
}

pub(crate) fn protect(bytes: &[u8], encrypt: bool) -> io::Result<Vec<u8>> {
    use std::{ptr::null, ptr::null_mut, slice};
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::Cryptography::{
            CRYPT_INTEGER_BLOB, CRYPTPROTECT_UI_FORBIDDEN, CryptProtectData, CryptUnprotectData,
        },
    };

    let input = CRYPT_INTEGER_BLOB {
        cbData: bytes.len().try_into().map_err(io::Error::other)?,
        pbData: bytes.as_ptr().cast_mut(),
    };
    let mut output = CRYPT_INTEGER_BLOB::default();
    // DPAPI allocates the output buffer with LocalAlloc. The input stays borrowed.
    unsafe {
        let success = if encrypt {
            CryptProtectData(
                &input,
                null(),
                null(),
                null(),
                null(),
                CRYPTPROTECT_UI_FORBIDDEN,
                &mut output,
            )
        } else {
            CryptUnprotectData(
                &input,
                null_mut(),
                null(),
                null(),
                null(),
                CRYPTPROTECT_UI_FORBIDDEN,
                &mut output,
            )
        };
        if success == 0 {
            return Err(io::Error::last_os_error());
        }
        let result = slice::from_raw_parts(output.pbData, output.cbData as usize).to_vec();
        LocalFree(output.pbData.cast());
        Ok(result)
    }
}
