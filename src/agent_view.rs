use std::{
    io,
    path::{Path, PathBuf},
};

use crate::{agent_binding::Binding, codex, storage};
use gpui_kit::component::{
    ActiveTheme, Disableable,
    button::{Button, ButtonVariants},
    label::Label,
};
use gpui_kit::{prelude::FluentBuilder, *};

enum ProjectAction {
    Bind,
    List,
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
                    Ok(binding) => view.saved = binding,
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

    fn choose_project(&mut self, action: ProjectAction, cx: &mut Context<Self>) {
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
                            match action {
                                ProjectAction::Bind => view.bind_project(path, cx),
                                ProjectAction::List => view.search(path, cx),
                            }
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

    fn bind_project(&mut self, project: PathBuf, cx: &mut Context<Self>) {
        self.project = None;
        self.candidates.clear();
        self.status = Some("正在准备本机协调会话".into());
        let room_id = self.room_id.clone();
        let pending = cx.background_executor().spawn(async move {
            let directory = storage::data_directory()?;
            Binding::ensure(&directory, &room_id, &project).await
        });
        self.update_binding(pending, cx);
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
                    Ok(candidates) if candidates.is_empty() => Some(
                        "该目录没有可绑定的会话。可通过「选择工程并绑定」创建协调会话。".into(),
                    ),
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
        self.update_binding(save, cx);
    }

    fn update_binding(&mut self, save: Task<io::Result<Binding>>, cx: &mut Context<Self>) {
        self.busy = true;
        cx.spawn(async move |this, cx| {
            let result = save.await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                match result {
                    Ok(binding) => {
                        view.saved = Some(binding);
                        view.project = None;
                        view.candidates.clear();
                        view.status = None;
                    }
                    Err(error) => view.status = Some(format!("绑定失败。{error}").into()),
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
            .when_some(self.saved.as_ref(), |panel, binding| {
                panel
                    .child(Label::new(format!("已绑定 {}", binding.label)).text_sm())
                    .child(Label::new(binding.project.clone()).text_sm())
                    .child(Label::new(format!("会话 ID {}", binding.thread_id)).text_sm())
            })
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_2()
                    .child(
                        Button::new("choose-project")
                            .label("选择工程并绑定")
                            .primary()
                            .disabled(self.busy)
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.choose_project(ProjectAction::Bind, cx)
                            })),
                    )
                    .child(
                        Button::new("choose-existing-session")
                            .label("选择已有会话")
                            .disabled(self.busy)
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.choose_project(ProjectAction::List, cx)
                            })),
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
