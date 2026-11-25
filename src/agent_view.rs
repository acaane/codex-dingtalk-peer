use std::{
    io,
    path::{Path, PathBuf},
};

use gpui_kit::component::{
    ActiveTheme, Disableable,
    button::{Button, ButtonVariants},
    label::Label,
};
use gpui_kit::{prelude::FluentBuilder, *};
use sqlx::SqliteConnection;

use crate::{codex, storage};

struct Binding {
    project: String,
    thread_id: String,
    label: String,
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
    Ok(db)
}

impl Binding {
    async fn load(directory: &Path, room_id: &str) -> sqlx::Result<Option<Self>> {
        let mut db = open_bindings(directory).await?;
        let saved: Option<(String, String, String)> = sqlx::query_as(
            "SELECT project, thread_id, label FROM agent_bindings WHERE room_id = ?1",
        )
        .bind(room_id)
        .fetch_optional(&mut db)
        .await?;
        Ok(saved.map(|(project, thread_id, label)| Self {
            project,
            thread_id,
            label,
        }))
    }

    async fn save(&self, directory: &Path, room_id: &str) -> sqlx::Result<()> {
        let mut db = open_bindings(directory).await?;
        sqlx::query(
            "INSERT INTO agent_bindings (room_id, project, thread_id, label) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(room_id) DO UPDATE SET
                project = excluded.project, thread_id = excluded.thread_id, label = excluded.label",
        )
        .bind(room_id)
        .bind(&self.project)
        .bind(&self.thread_id)
        .bind(&self.label)
        .execute(&mut db)
        .await?;
        Ok(())
    }
}

pub struct AgentView {
    room_id: String,
    saved: Option<Binding>,
    project: Option<PathBuf>,
    candidates: Vec<codex::Candidate>,
    status: Option<SharedString>,
    busy: bool,
}

impl AgentView {
    pub fn new(room_id: String, cx: &mut Context<Self>) -> Self {
        let load_id = room_id.clone();
        let load = cx.background_executor().spawn(async move {
            let directory = storage::data_directory()?;
            Binding::load(&directory, &load_id)
                .await
                .map_err(io::Error::other)
        });
        cx.spawn(async move |this, cx| {
            let result = load.await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                match result {
                    Ok(binding) => {
                        view.project = binding.as_ref().map(|saved| PathBuf::from(&saved.project));
                        view.saved = binding;
                    }
                    Err(error) => view.status = Some(format!("读取本机绑定失败。{error}").into()),
                }
                cx.notify();
            });
        })
        .detach();
        Self {
            room_id,
            saved: None,
            project: None,
            candidates: Vec::new(),
            status: None,
            busy: true,
        }
    }

    fn choose_project(&mut self, cx: &mut Context<Self>) {
        let selection = cx.prompt_for_paths(PathPromptOptions {
            directories: true,
            files: false,
            multiple: false,
            prompt: Some("选择 Codex 会话使用的工程目录".into()),
        });
        self.busy = true;
        cx.spawn(async move |this, cx| {
            let selection = selection.await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                match selection {
                    Ok(Ok(Some(paths))) => {
                        if let Some(path) = paths.into_iter().next() {
                            view.search(path, cx);
                        }
                    }
                    Ok(Err(_)) => view.status = Some("无法打开工程目录选择器".into()),
                    _ => {}
                }
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }

    fn search(&mut self, project: PathBuf, cx: &mut Context<Self>) {
        self.project = Some(project.clone());
        self.candidates.clear();
        self.busy = true;
        self.status = Some("正在查找该目录的 Codex 会话".into());
        let search = cx
            .background_executor()
            .spawn(async move { codex::project_sessions(&project) });
        cx.spawn(async move |this, cx| {
            let result = search.await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                view.status = match result {
                    Ok(candidates) if candidates.is_empty() => {
                        Some("该目录没有可绑定的会话。请在 Codex 打开对应工程后刷新。".into())
                    }
                    Ok(candidates) => {
                        view.candidates = candidates;
                        None
                    }
                    Err(error) => Some(format!("查找会话失败。{error}").into()),
                };
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }

    fn bind(&mut self, candidate: &codex::Candidate, cx: &mut Context<Self>) {
        let Some(project) = &self.project else {
            return;
        };
        let binding = Binding {
            project: project.to_string_lossy().into_owned(),
            thread_id: candidate.id.clone(),
            label: candidate.label(),
        };
        self.busy = true;
        self.status = Some("正在保存本机绑定".into());
        let room_id = self.room_id.clone();
        let save = cx.background_executor().spawn(async move {
            let directory = storage::data_directory()?;
            binding
                .save(&directory, &room_id)
                .await
                .map_err(io::Error::other)?;
            Ok::<_, io::Error>(binding)
        });
        cx.spawn(async move |this, cx| {
            let result = save.await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                match result {
                    Ok(binding) => {
                        view.saved = Some(binding);
                        view.candidates.clear();
                        view.status = None;
                    }
                    Err(error) => view.status = Some(format!("保存绑定失败。{error}").into()),
                }
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }
}

impl Render for AgentView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let project = self.project.clone();
        div()
            .id("codex-binding")
            .size_full()
            .overflow_y_scroll()
            .p_5()
            .flex()
            .flex_col()
            .gap_3()
            .child(Label::new("请在 Codex 中打开当前联调工程的会话，再选择该工程目录。").text_sm())
            .when_some(self.saved.as_ref(), |panel, binding| {
                panel
                    .child(Label::new(format!("已绑定 {}", binding.label)).text_sm())
                    .child(Label::new(binding.project.clone()).text_sm())
                    .child(Label::new(format!("会话 ID {}", binding.thread_id)).text_sm())
            })
            .child(
                div()
                    .flex()
                    .gap_2()
                    .child(
                        Button::new("choose-project")
                            .label("选择工程目录")
                            .primary()
                            .disabled(self.busy)
                            .on_click(cx.listener(|view, _, _, cx| view.choose_project(cx))),
                    )
                    .when_some(project, |actions, project| {
                        actions.child(
                            Button::new("refresh-project-sessions")
                                .label("刷新会话")
                                .disabled(self.busy)
                                .on_click(cx.listener(move |view, _, _, cx| {
                                    view.search(project.clone(), cx)
                                })),
                        )
                    }),
            )
            .when_some(
                self.project.as_ref().filter(|path| {
                    self.saved
                        .as_ref()
                        .is_none_or(|saved| Path::new(&saved.project) != *path)
                }),
                |panel, project| {
                    panel.child(Label::new(format!("查找目录 {}", project.display())).text_sm())
                },
            )
            .when_some(self.status.clone(), |panel, state| {
                panel.child(Label::new(state).text_sm())
            })
            .children(
                self.candidates
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(index, candidate)| {
                        let id = candidate.id.rsplit('-').next().unwrap_or(&candidate.id);
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .p_2()
                            .border_1()
                            .border_color(cx.theme().border)
                            .rounded_md()
                            .child(div().flex_1().child(
                                Label::new(format!("{} · {id}", candidate.label())).text_sm(),
                            ))
                            .child(
                                Button::new(("bind-codex-session", index))
                                    .label("绑定会话")
                                    .disabled(self.busy)
                                    .on_click(
                                        cx.listener(move |view, _, _, cx| {
                                            view.bind(&candidate, cx)
                                        }),
                                    ),
                            )
                    }),
            )
    }
}
