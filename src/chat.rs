use std::{path::Path, time::Duration};

use rusqlite::{Connection, params};

#[derive(Clone)]
pub struct Message {
    pub id: i64,
    pub body: String,
    pub local_time: String,
}

pub struct ChatStore {
    connection: Connection,
}

#[derive(Debug)]
pub enum SendError {
    Empty,
    Database(rusqlite::Error),
}

impl From<rusqlite::Error> for SendError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

impl ChatStore {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_millis(250))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY,
                body TEXT NOT NULL,
                sent_at INTEGER NOT NULL DEFAULT (unixepoch())
            );",
        )?;
        Ok(Self { connection })
    }

    pub fn history(&self) -> rusqlite::Result<Vec<Message>> {
        read_after(&self.connection, 0)
    }

    pub fn send(&mut self, body: &str, last_seen: i64) -> Result<Vec<Message>, SendError> {
        if body.trim().is_empty() {
            return Err(SendError::Empty);
        }

        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT INTO chat_messages (body) VALUES (?1)",
            params![body],
        )?;
        let messages = read_after(&transaction, last_seen)?;
        transaction.commit()?;
        Ok(messages)
    }
}

fn read_after(connection: &Connection, last_seen: i64) -> rusqlite::Result<Vec<Message>> {
    let mut statement = connection.prepare(
        "SELECT id, body, strftime('%Y-%m-%d %H:%M', sent_at, 'unixepoch', 'localtime')
         FROM chat_messages WHERE id > ?1 ORDER BY id",
    )?;
    statement
        .query_map(params![last_seen], |row| {
            Ok(Message {
                id: row.get(0)?,
                body: row.get(1)?,
                local_time: row.get(2)?,
            })
        })?
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_and_multiline_messages_survive_reopening_in_order() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("聊天 记录.sqlite3");
        let first = "接口检查完成。\n已保存中文日志。\n";
        let mut store = ChatStore::open(&path).unwrap();
        let last_id = store.send(first, 0).unwrap()[0].id;
        store.send("继续下一步", last_id).unwrap();
        drop(store);

        let store = ChatStore::open(&path).unwrap();
        let messages = store.history().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].id, last_id);
        assert_eq!(messages[0].body, first);
        assert_eq!(messages[1].body, "继续下一步");
        assert_eq!(messages[1].local_time.len(), 16);
    }

    #[test]
    fn whitespace_does_not_write_a_message() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = ChatStore::open(&directory.path().join("chat.sqlite3")).unwrap();
        assert!(matches!(store.send(" \t\n　", 0), Err(SendError::Empty)));
        assert!(store.history().unwrap().is_empty());
    }

    #[test]
    fn sending_once_catches_up_with_another_window() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("chat.sqlite3");
        let mut first = ChatStore::open(&path).unwrap();
        let mut second = ChatStore::open(&path).unwrap();
        let last_id = first.send("第一条", 0).unwrap()[0].id;
        second.send("另一窗口", last_id).unwrap();

        let new_messages = first.send("第三条", last_id).unwrap();
        assert_eq!(new_messages.len(), 2);
        assert_eq!(new_messages[0].body, "另一窗口");
        assert_eq!(new_messages[1].body, "第三条");
        assert_eq!(second.history().unwrap().len(), 3);
    }

    #[test]
    fn a_failed_commit_does_not_leave_a_message_to_duplicate_on_retry() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("chat.sqlite3");
        let mut writer = ChatStore::open(&path).unwrap();
        let reader = ChatStore::open(&path).unwrap();
        reader
            .connection
            .execute_batch("BEGIN; SELECT * FROM chat_messages;")
            .unwrap();

        assert!(matches!(
            writer.send("待保存", 0),
            Err(SendError::Database(_))
        ));
        reader.connection.execute_batch("ROLLBACK;").unwrap();
        assert!(writer.history().unwrap().is_empty());
        assert_eq!(writer.send("待保存", 0).unwrap().len(), 1);
    }
}
