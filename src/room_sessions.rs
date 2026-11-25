use std::{io, path::Path};

use futures_lite::future::block_on;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::SqliteConnection;

use crate::storage;

pub struct Sessions(SqliteConnection);

impl Sessions {
    pub fn open_existing(directory: &Path) -> io::Result<Self> {
        block_on(storage::read_only(&directory.join("room-sessions.sqlite3")))
            .map(Self)
            .map_err(io::Error::other)
    }

    pub fn open(directory: &Path) -> io::Result<Self> {
        block_on(async {
            let mut db = storage::connect(&directory.join("room-sessions.sqlite3"))
                .await
                .map_err(io::Error::other)?;
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS room_sessions (
                    id TEXT PRIMARY KEY,
                    protected_state BLOB NOT NULL,
                    last_opened INTEGER
                )",
            )
            .execute(&mut db)
            .await
            .map_err(io::Error::other)?;
            Ok(Self(db))
        })
    }

    pub fn has_last(directory: &Path) -> io::Result<bool> {
        if !directory.join("room-sessions.sqlite3").try_exists()? {
            return Ok(false);
        }
        let mut sessions = Self::open(directory)?;
        block_on(
            sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(SELECT 1 FROM room_sessions WHERE last_opened IS NOT NULL)",
            )
            .fetch_one(&mut sessions.0),
        )
        .map(|exists| exists != 0)
        .map_err(io::Error::other)
    }

    pub fn remember(&mut self, id: &str, session: &impl Serialize) -> io::Result<()> {
        let json = serde_json::to_vec(session).map_err(io::Error::other)?;
        let encrypted = storage::protect(&json, true)?;
        block_on(
            sqlx::query(
                "INSERT INTO room_sessions (id, protected_state) VALUES (?1, ?2)
                 ON CONFLICT(id) DO NOTHING",
            )
            .bind(id)
            .bind(encrypted)
            .execute(&mut self.0),
        )
        .map_err(io::Error::other)?;
        Ok(())
    }

    pub fn get<T: DeserializeOwned>(&mut self, id: &str) -> io::Result<Option<T>> {
        let state = block_on(
            sqlx::query_scalar::<_, Vec<u8>>(
                "SELECT protected_state FROM room_sessions WHERE id = ?1",
            )
            .bind(id)
            .fetch_optional(&mut self.0),
        )
        .map_err(io::Error::other)?;
        state.map(Self::decode).transpose()
    }

    pub fn last<T: DeserializeOwned>(&mut self) -> io::Result<Option<T>> {
        let state = block_on(
            sqlx::query_scalar::<_, Vec<u8>>(
                "SELECT protected_state FROM room_sessions
                 WHERE last_opened IS NOT NULL ORDER BY last_opened DESC LIMIT 1",
            )
            .fetch_optional(&mut self.0),
        )
        .map_err(io::Error::other)?;
        state.map(Self::decode).transpose()
    }

    pub fn mark_opened(&mut self, id: &str) -> io::Result<()> {
        block_on(
            sqlx::query(
                "UPDATE room_sessions SET last_opened =
                     (SELECT COALESCE(MAX(last_opened), 0) + 1 FROM room_sessions)
                 WHERE id = ?1",
            )
            .bind(id)
            .execute(&mut self.0),
        )
        .map_err(io::Error::other)?;
        Ok(())
    }

    fn decode<T: DeserializeOwned>(encrypted: Vec<u8>) -> io::Result<T> {
        let json = storage::protect(&encrypted, false)?;
        serde_json::from_slice(&json).map_err(io::Error::other)
    }
}
