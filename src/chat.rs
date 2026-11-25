use std::path::Path;

use sqlx::{Connection, SqliteConnection};

#[derive(Clone)]
pub struct Message {
    pub id: i64,
    pub body: String,
    pub local_time: String,
}

pub struct ChatStore {
    connection: SqliteConnection,
}

#[derive(Debug)]
pub enum SendError {
    Empty,
    Database(sqlx::Error),
}

impl From<sqlx::Error> for SendError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

impl ChatStore {
    pub async fn open(path: &Path) -> sqlx::Result<Self> {
        let mut connection = crate::storage::connect(path).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY,
                body TEXT NOT NULL,
                sent_at INTEGER NOT NULL DEFAULT (unixepoch())
            )",
        )
        .execute(&mut connection)
        .await?;
        Ok(Self { connection })
    }

    pub async fn history(&mut self) -> sqlx::Result<Vec<Message>> {
        read_after(&mut self.connection, 0).await
    }

    pub async fn send(&mut self, body: &str, last_seen: i64) -> Result<Vec<Message>, SendError> {
        if body.trim().is_empty() {
            return Err(SendError::Empty);
        }

        let mut transaction = self.connection.begin().await?;
        sqlx::query("INSERT INTO chat_messages (body) VALUES (?1)")
            .bind(body)
            .execute(&mut *transaction)
            .await?;
        let messages = read_after(&mut transaction, last_seen).await?;
        transaction.commit().await?;
        Ok(messages)
    }
}

async fn read_after(
    connection: &mut SqliteConnection,
    last_seen: i64,
) -> sqlx::Result<Vec<Message>> {
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT id, body, strftime('%Y-%m-%d %H:%M', sent_at, 'unixepoch', 'localtime')
         FROM chat_messages WHERE id > ?1 ORDER BY id",
    )
    .bind(last_seen)
    .fetch_all(connection)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, body, local_time)| Message {
            id,
            body,
            local_time,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use futures_lite::future::block_on;

    use super::*;

    #[test]
    fn reads_the_existing_schema_and_preserves_messages_on_reopen() {
        block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("聊天 记录.sqlite3");
            let first = "接口检查完成。\n已保存中文日志。\n";
            let mut legacy = crate::storage::connect(&path).await.unwrap();
            sqlx::query("CREATE TABLE chat_messages (id INTEGER PRIMARY KEY, body TEXT NOT NULL, sent_at INTEGER NOT NULL DEFAULT (unixepoch()))")
                .execute(&mut legacy).await.unwrap();
            sqlx::query("INSERT INTO chat_messages (id, body) VALUES (27, ?1)")
                .bind(first)
                .execute(&mut legacy)
                .await
                .unwrap();
            legacy.close().await.unwrap();

            let mut store = ChatStore::open(&path).await.unwrap();
            assert_eq!(store.history().await.unwrap()[0].body, first);
            store.send("继续下一步", 27).await.unwrap();
            store.connection.close().await.unwrap();

            let mut store = ChatStore::open(&path).await.unwrap();
            let messages = store.history().await.unwrap();
            assert_eq!(messages.len(), 2);
            assert_eq!(messages[0].id, 27);
            assert_eq!(messages[0].body, first);
            assert_eq!(messages[1].body, "继续下一步");
            assert_eq!(messages[1].local_time.len(), 16);
        });
    }

    #[test]
    fn whitespace_does_not_write_a_message() {
        block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let mut store = ChatStore::open(&directory.path().join("chat.sqlite3"))
                .await
                .unwrap();
            assert!(matches!(
                store.send(" \t\n　", 0).await,
                Err(SendError::Empty)
            ));
            assert!(store.history().await.unwrap().is_empty());
        });
    }

    #[test]
    fn sending_once_catches_up_with_another_window() {
        block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("chat.sqlite3");
            let mut first = ChatStore::open(&path).await.unwrap();
            let mut second = ChatStore::open(&path).await.unwrap();
            let last_id = first.send("第一条", 0).await.unwrap()[0].id;
            second.send("另一窗口", last_id).await.unwrap();

            let new_messages = first.send("第三条", last_id).await.unwrap();
            assert_eq!(new_messages.len(), 2);
            assert_eq!(new_messages[0].body, "另一窗口");
            assert_eq!(new_messages[1].body, "第三条");
            assert_eq!(second.history().await.unwrap().len(), 3);
        });
    }

    #[test]
    fn a_failed_commit_does_not_leave_a_message_to_duplicate_on_retry() {
        block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("chat.sqlite3");
            let mut writer = ChatStore::open(&path).await.unwrap();
            let mut reader = ChatStore::open(&path).await.unwrap();
            let mut lock = reader.connection.begin().await.unwrap();
            sqlx::query("SELECT * FROM chat_messages")
                .fetch_all(&mut *lock)
                .await
                .unwrap();

            assert!(matches!(
                writer.send("待保存", 0).await,
                Err(SendError::Database(_))
            ));
            lock.rollback().await.unwrap();
            assert!(writer.history().await.unwrap().is_empty());
            assert_eq!(writer.send("待保存", 0).await.unwrap().len(), 1);
        });
    }
}
