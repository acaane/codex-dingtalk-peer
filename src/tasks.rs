use std::{io, path::Path};

use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

use crate::{
    readiness::{Failure, Id},
    room::{Member, Secret},
    storage,
};

pub const MINUTES: [u64; 3] = [1, 5, 15];
const MAX_GOAL: usize = 4096;
const MAX_REPORT: usize = 16 * 1024;

#[derive(Clone, Serialize, Deserialize)]
pub enum Action {
    Submit(Submission),
    Claim { id: Id, attempt: Id },
    Report(Report),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Submission {
    pub id: Id,
    pub goal: String,
    pub minutes: u64,
}

impl Submission {
    pub fn new(goal: &str, minutes: u64) -> io::Result<Self> {
        let goal = goal.trim();
        if !valid_goal(goal, minutes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "请填写联调目标（最多 4096 字节）并选择有效时限",
            ));
        }
        Ok(Self {
            id: crate::room::random()?,
            goal: goal.into(),
            minutes,
        })
    }
}

fn valid_goal(goal: &str, minutes: u64) -> bool {
    MINUTES.contains(&minutes) && !goal.is_empty() && goal.len() <= MAX_GOAL
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    pub action: Option<Action>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<Secret>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Report {
    pub id: Id,
    pub attempt: Id,
    pub outcome: Result<String, Failure>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Phase {
    Waiting,
    Running,
    Completed(String),
    Failed(Failure),
    Expired,
    Uncertain,
    BindingChanged,
}

impl Phase {
    pub fn active(&self) -> bool {
        matches!(self, Self::Waiting | Self::Running)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: Id,
    pub goal: String,
    pub deadline: u64,
    pub deadline_label: String,
    pub phase: Phase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Grant {
    pub id: Id,
    pub attempt: Id,
    pub seconds_left: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Update {
    pub latest: Option<Task>,
    pub acknowledged: Option<Id>,
    pub grant: Option<Grant>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Record {
    task: Task,
    bindings: [Secret; 2],
    attempt: Option<Id>,
}

pub async fn initialize(db: &mut SqliteConnection) -> sqlx::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS room_tasks (id BLOB PRIMARY KEY, record BLOB NOT NULL)",
    )
    .execute(db)
    .await?;
    Ok(())
}

async fn load(db: &mut SqliteConnection, id: Option<Id>) -> io::Result<Option<Record>> {
    let sql = if id.is_some() {
        "SELECT record FROM room_tasks WHERE id = ?1"
    } else {
        "SELECT record FROM room_tasks ORDER BY rowid DESC LIMIT 1"
    };
    let mut query = sqlx::query_scalar::<_, Vec<u8>>(sql);
    if let Some(id) = &id {
        query = query.bind(id.as_slice());
    }
    query
        .fetch_optional(db)
        .await
        .map_err(io::Error::other)?
        .map(|data| serde_json::from_slice(&data).map_err(io::Error::other))
        .transpose()
}

async fn save(db: &mut SqliteConnection, record: &Record) -> io::Result<()> {
    sqlx::query(
        "INSERT INTO room_tasks (id, record) VALUES (?1, ?2)
                 ON CONFLICT(id) DO UPDATE SET record = excluded.record",
    )
    .bind(record.task.id.as_slice())
    .bind(serde_json::to_vec(record).map_err(io::Error::other)?)
    .execute(db)
    .await
    .map_err(io::Error::other)?;
    Ok(())
}

pub async fn apply(
    db: &mut SqliteConnection,
    role: Member,
    action: Option<Action>,
    ready: Option<[Secret; 2]>,
    advertised: Option<Secret>,
    now: u64,
) -> io::Result<Update> {
    let mut update = Update::default();
    let mut latest = load(db, None).await?;
    if let Some(record) = latest
        .as_mut()
        .filter(|record| record.task.phase.active() && now >= record.task.deadline)
    {
        record.task.phase = if record.attempt.is_some() {
            Phase::Uncertain
        } else {
            Phase::Expired
        };
        save(db, record).await?;
    }
    if let Some(record) = latest.as_mut().filter(|record| {
        record.task.phase == Phase::Waiting
            && ready.is_some_and(|current| current != record.bindings)
    }) {
        record.task.phase = Phase::BindingChanged;
        save(db, record).await?;
    }
    match action {
        Some(Action::Submit(Submission { id, goal, minutes })) if role == Member::Host => {
            let goal = goal.trim();
            if load(db, Some(id)).await?.is_some() {
                update.acknowledged = Some(id);
            } else if !valid_goal(goal, minutes) {
                update.error = Some("请填写有效的目标和截止时间".into());
            } else if latest
                .as_ref()
                .is_some_and(|record| record.task.phase.active())
            {
                update.error = Some("当前任务仍在处理".into());
            } else if let Some(bindings) = ready.filter(|bindings| Some(bindings[0]) == advertised)
            {
                let deadline = now + minutes * 60;
                let deadline_label = sqlx::query_scalar(
                    "SELECT strftime('%m-%d %H:%M:%S', ?1, 'unixepoch', 'localtime')",
                )
                .bind(deadline as i64)
                .fetch_one(&mut *db)
                .await
                .map_err(io::Error::other)?;
                let record = Record {
                    task: Task {
                        id,
                        goal: goal.into(),
                        deadline,
                        deadline_label,
                        phase: Phase::Waiting,
                    },
                    bindings,
                    attempt: None,
                };
                save(db, &record).await?;
                latest = Some(record);
                update.acknowledged = Some(id);
            } else {
                update.error = Some("双方的当前会话需要通过接单验证".into());
            }
        }
        Some(Action::Claim { id, attempt }) if role == Member::Guest => {
            if let Some(record) = latest.as_mut().filter(|record| {
                record.task.id == id
                    && record.task.phase.active()
                    && ready == Some(record.bindings)
                    && advertised == Some(record.bindings[1])
            }) {
                if record.task.phase == Phase::Waiting {
                    record.task.phase = Phase::Running;
                    record.attempt = Some(attempt);
                    save(db, record).await?;
                }
                // A repeated claim can recover its acknowledgement. A new process has a different attempt.
                if record.attempt == Some(attempt) {
                    update.grant = Some(Grant {
                        id,
                        attempt,
                        seconds_left: record.task.deadline - now,
                    });
                }
            }
        }
        Some(Action::Report(report)) if role == Member::Guest => {
            if let Some(mut record) = load(db, Some(report.id))
                .await?
                .filter(|record| record.attempt == Some(report.attempt))
            {
                if matches!(record.task.phase, Phase::Running | Phase::Uncertain) {
                    record.task.phase = match report.outcome {
                        Ok(text) if !text.trim().is_empty() && text.len() <= MAX_REPORT => {
                            Phase::Completed(text)
                        }
                        Ok(_) => Phase::Failed(Failure::Reply),
                        Err(reason) => Phase::Failed(reason),
                    };
                    save(db, &record).await?;
                    if latest
                        .as_ref()
                        .is_some_and(|latest| latest.task.id == report.id)
                    {
                        latest = Some(record);
                    }
                }
                update.acknowledged = Some(report.id);
            }
        }
        Some(_) => update.error = Some("该操作不适用于当前房间角色".into()),
        None => {}
    }
    update.latest = latest.map(|record| record.task);
    Ok(update)
}

pub fn bounded_report(mut text: String) -> String {
    if text.len() > MAX_REPORT {
        let suffix = "\n…（回报超过 16 KiB，完整内容请查看原会话）";
        let cutoff = text.floor_char_boundary(MAX_REPORT - suffix.len());
        text.truncate(cutoff);
        text.push_str(suffix);
    }
    text
}

async fn outbox(directory: &Path) -> io::Result<SqliteConnection> {
    let mut db = storage::connect(&directory.join("task-reports.sqlite3"))
        .await
        .map_err(io::Error::other)?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS reports (
                    room TEXT NOT NULL, id BLOB NOT NULL, data BLOB NOT NULL,
                    PRIMARY KEY (room, id)
                 )",
    )
    .execute(&mut db)
    .await
    .map_err(io::Error::other)?;
    Ok(db)
}

pub async fn load_reports(directory: &Path, room: &str) -> io::Result<Vec<Report>> {
    let mut db = outbox(directory).await?;
    sqlx::query_scalar::<_, Vec<u8>>("SELECT data FROM reports WHERE room = ?1")
        .bind(room)
        .fetch_all(&mut db)
        .await
        .map_err(io::Error::other)?
        .into_iter()
        .map(|data| serde_json::from_slice(&data).map_err(io::Error::other))
        .collect()
}

pub async fn save_report(directory: &Path, room: &str, report: &Report) -> io::Result<()> {
    let mut db = outbox(directory).await?;
    sqlx::query(
        "INSERT INTO reports (room, id, data) VALUES (?1, ?2, ?3)
                 ON CONFLICT(room, id) DO UPDATE SET data = excluded.data",
    )
    .bind(room)
    .bind(report.id.as_slice())
    .bind(serde_json::to_vec(report).map_err(io::Error::other)?)
    .execute(&mut db)
    .await
    .map_err(io::Error::other)?;
    Ok(())
}

pub async fn acknowledge_report(directory: &Path, room: &str, id: Id) -> io::Result<()> {
    let mut db = outbox(directory).await?;
    sqlx::query("DELETE FROM reports WHERE room = ?1 AND id = ?2")
        .bind(room)
        .bind(id.as_slice())
        .execute(&mut db)
        .await
        .map_err(io::Error::other)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use futures_lite::future::block_on;
    use sqlx::Connection;

    use super::*;

    const BINDINGS: [Secret; 2] = [[1; 32], [2; 32]];
    const ID: Id = [4; 16];
    const ATTEMPT: Id = [5; 16];

    #[test]
    fn submission_retains_the_existing_wire_shape_and_normalizes_the_goal() {
        let task = Submission::new("  核对首行\n", 5).unwrap();
        let json = serde_json::to_value(Action::Submit(task.clone())).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"Submit": {"id": task.id, "goal": "核对首行", "minutes": 5}})
        );
        assert!(Submission::new("   ", 5).is_err());
        assert!(Submission::new("核对首行", 2).is_err());
    }

    fn submission() -> Action {
        Action::Submit(Submission {
            id: ID,
            goal: "核对测试接口返回字段".into(),
            minutes: 1,
        })
    }

    async fn exchange(
        db: &mut SqliteConnection,
        role: Member,
        command: Option<Action>,
        now: u64,
    ) -> Update {
        let binding = if role == Member::Host {
            BINDINGS[0]
        } else {
            BINDINGS[1]
        };
        apply(db, role, command, Some(BINDINGS), Some(binding), now)
            .await
            .unwrap()
    }

    #[test]
    fn only_the_verified_guest_claims_once_across_restart_and_reports_back() {
        block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let database = directory.path().join("room.sqlite3");
            let mut db = storage::connect(&database).await.unwrap();
            initialize(&mut db).await.unwrap();

            let guest_submit = exchange(&mut db, Member::Guest, Some(submission()), 20).await;
            assert!(guest_submit.error.is_some());
            let not_ready = apply(
                &mut db,
                Member::Host,
                Some(submission()),
                None,
                Some(BINDINGS[0]),
                20,
            )
            .await
            .unwrap();
            assert!(not_ready.latest.is_none());
            let created = exchange(&mut db, Member::Host, Some(submission()), 20).await;
            assert_eq!(created.acknowledged, Some(ID));
            assert_eq!(created.latest.unwrap().deadline, 80);
            let duplicate = exchange(&mut db, Member::Host, Some(submission()), 35).await;
            assert_eq!(duplicate.latest.unwrap().deadline, 80);

            let claim = Action::Claim {
                id: ID,
                attempt: ATTEMPT,
            };
            let wrong_binding = apply(
                &mut db,
                Member::Guest,
                Some(claim.clone()),
                Some(BINDINGS),
                Some([9; 32]),
                35,
            )
            .await
            .unwrap();
            assert!(wrong_binding.grant.is_none());
            let granted = exchange(&mut db, Member::Guest, Some(claim.clone()), 35).await;
            assert_eq!(granted.grant.unwrap().seconds_left, 45);
            db.close().await.unwrap();

            let mut db = storage::connect(&database).await.unwrap();
            let reconnected = exchange(
                &mut db,
                Member::Guest,
                Some(Action::Claim {
                    id: ID,
                    attempt: [6; 16],
                }),
                36,
            )
            .await;
            assert!(reconnected.grant.is_none());
            assert_eq!(reconnected.latest.unwrap().phase, Phase::Running);
            assert!(
                exchange(&mut db, Member::Guest, Some(claim), 36)
                    .await
                    .grant
                    .is_some()
            );

            let reported = Action::Report(Report {
                id: ID,
                attempt: ATTEMPT,
                outcome: Ok("证据支持字段一致".into()),
            });
            let wrong_author = exchange(&mut db, Member::Host, Some(reported.clone()), 38).await;
            assert_eq!(wrong_author.latest.unwrap().phase, Phase::Running);
            assert_eq!(
                exchange(&mut db, Member::Guest, Some(reported.clone()), 39)
                    .await
                    .acknowledged,
                Some(ID)
            );
            assert_eq!(
                exchange(&mut db, Member::Guest, Some(reported), 40)
                    .await
                    .acknowledged,
                Some(ID)
            );
            let saved = exchange(&mut db, Member::Host, None, 41)
                .await
                .latest
                .unwrap();
            assert_eq!(saved.phase, Phase::Completed("证据支持字段一致".into()));
        });
    }

    #[test]
    fn an_expired_task_cannot_be_claimed_and_a_late_saved_report_reaches_the_host() {
        block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let mut db = storage::connect(&directory.path().join("room.sqlite3"))
                .await
                .unwrap();
            initialize(&mut db).await.unwrap();
            exchange(&mut db, Member::Host, Some(submission()), 20).await;
            let too_late = exchange(
                &mut db,
                Member::Guest,
                Some(Action::Claim {
                    id: ID,
                    attempt: ATTEMPT,
                }),
                80,
            )
            .await;
            assert!(too_late.grant.is_none());
            assert_eq!(too_late.latest.unwrap().phase, Phase::Expired);
            let next = [7; 16];
            exchange(
                &mut db,
                Member::Host,
                Some(Action::Submit(Submission {
                    id: next,
                    goal: "另一项检查".into(),
                    minutes: 1,
                })),
                81,
            )
            .await;
            exchange(
                &mut db,
                Member::Guest,
                Some(Action::Claim {
                    id: next,
                    attempt: ATTEMPT,
                }),
                82,
            )
            .await;
            assert_eq!(
                exchange(&mut db, Member::Host, None, 142)
                    .await
                    .latest
                    .unwrap()
                    .phase,
                Phase::Uncertain
            );

            let report = Report {
                id: next,
                attempt: ATTEMPT,
                outcome: Ok("断线期间完成".into()),
            };
            save_report(directory.path(), "room", &report)
                .await
                .unwrap();
            let restored = load_reports(directory.path(), "room")
                .await
                .unwrap()
                .pop()
                .unwrap();
            let received =
                exchange(&mut db, Member::Guest, Some(Action::Report(restored)), 180).await;
            assert_eq!(
                received.latest.unwrap().phase,
                Phase::Completed("断线期间完成".into())
            );
            acknowledge_report(directory.path(), "room", next)
                .await
                .unwrap();
            assert!(
                load_reports(directory.path(), "room")
                    .await
                    .unwrap()
                    .is_empty()
            );
        });
    }
}
