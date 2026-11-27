use std::{
    collections::VecDeque,
    io,
    path::Path,
    time::{Duration, Instant},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use gpui_kit::component::{
    Disableable,
    button::{Button, ButtonVariants},
    input::{Textarea, TextareaState},
    label::Label,
};
use gpui_kit::{prelude::FluentBuilder, *};

use crate::{
    agent_binding::Binding,
    agent_start::{self, Plan},
    codex,
    readiness::{Failure, Id},
    room::{self, Member, RoomClient, Secret},
    storage,
    tasks::{
        self, Action, Grant, HistoryEntry, Phase, Report, Submission, Task as RoomTask, Update,
    },
};

struct Claim {
    id: Id,
    attempt: Id,
    binding: Secret,
    started: bool,
}

#[derive(Default)]
struct HistoryPanel {
    entry: Option<HistoryEntry>,
    loading: bool,
    notice: Option<SharedString>,
}

struct CheckpointPanel {
    id: Id,
    draft: Option<Entity<TextareaState>>,
    saving: bool,
    notice: Option<SharedString>,
}

pub struct TaskView {
    room_id: String,
    member: Member,
    client: RoomClient,
    goal: Entity<TextareaState>,
    minutes: u64,
    latest: Option<RoomTask>,
    history: Option<HistoryPanel>,
    checkpoint: Option<CheckpointPanel>,
    pending: Option<Submission>,
    binding: Option<Secret>,
    plan: Option<Plan>,
    plan_busy: bool,
    claim: Option<Claim>,
    reports: VecDeque<Report>,
    next_outbox_read: Instant,
    connected: bool,
    ready: bool,
    notice: Option<SharedString>,
}

impl TaskView {
    pub fn new(
        room_id: String,
        member: Member,
        client: RoomClient,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut view = Self {
            room_id,
            member,
            client,
            goal: cx.new(|cx| {
                TextareaState::new(window, cx)
                    .rows(4)
                    .placeholder("填写接入端需要核对的目标")
            }),
            minutes: 5,
            latest: None,
            history: None,
            checkpoint: None,
            pending: None,
            binding: None,
            plan: None,
            plan_busy: member == Member::Host,
            claim: None,
            reports: VecDeque::new(),
            next_outbox_read: Instant::now(),
            connected: false,
            ready: false,
            notice: None,
        };
        if member == Member::Guest {
            view.read_outbox(cx);
        } else {
            view.read_plan(cx);
        }
        view
    }

    fn read_history(&mut self, id: Option<Id>, cx: &mut Context<Self>) {
        let history = self.history.get_or_insert_with(HistoryPanel::default);
        if history.loading {
            return;
        }
        history.loading = true;
        history.notice = None;
        let client = self.client.clone();
        // A separate exchange leaves the normal claim and report polling active.
        let load = cx.background_executor().spawn(async move {
            task_exchange(&client, Action::History { id }).map(|update| update.history)
        });
        cx.spawn(async move |this, cx| {
            let result = load.await;
            let _ = this.update(cx, |view, cx| {
                if let Some(history) = view.history.as_mut() {
                    history.loading = false;
                    match result {
                        Ok(entry) => history.entry = entry,
                        Err(error) => {
                            history.notice = Some(format!("读取任务记录失败。{error}").into())
                        }
                    }
                    cx.notify();
                }
            });
        })
        .detach();
        cx.notify();
    }

    fn open_checkpoint(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(task) = self.latest.as_ref() else {
            return;
        };
        let draft = (self.member == Member::Host).then(|| {
            cx.new(|cx| {
                let mut input = TextareaState::new(window, cx)
                    .rows(2)
                    .placeholder("填写下次联调要处理的事项");
                if let Some(note) = &task.next_step {
                    input.set_value(note.clone(), window, cx);
                }
                input
            })
        });
        self.checkpoint = Some(CheckpointPanel {
            id: task.id,
            draft,
            saving: false,
            notice: None,
        });
        cx.notify();
    }

    fn save_next_step(&mut self, cx: &mut Context<Self>) {
        let Some(checkpoint) = self.checkpoint.as_mut().filter(|checkpoint| {
            self.connected
                && !checkpoint.saving
                && self
                    .latest
                    .as_ref()
                    .is_some_and(|task| task.id == checkpoint.id)
        }) else {
            return;
        };
        let Some(draft) = &checkpoint.draft else {
            return;
        };
        let id = checkpoint.id;
        let note = draft.read(cx).value().to_string();
        checkpoint.saving = true;
        checkpoint.notice = None;
        let client = self.client.clone();
        let save = cx.background_executor().spawn(async move {
            let update = task_exchange(&client, Action::SaveNextStep { id, note })?;
            if let Some(error) = update.error {
                return Err(io::Error::other(error));
            }
            if update.acknowledged != Some(id) {
                return Err(io::Error::other("主机未确认已保存"));
            }
            Ok(())
        });
        cx.spawn(async move |this, cx| {
            let result = save.await;
            let _ = this.update(cx, |view, cx| {
                if let Some(checkpoint) = view.checkpoint.as_mut().filter(|point| point.id == id) {
                    checkpoint.saving = false;
                    checkpoint.notice = Some(match result {
                        Ok(()) => "下一步已保存".into(),
                        Err(error) => format!("保存下一步失败。{error}").into(),
                    });
                    cx.notify();
                }
            });
        })
        .detach();
        cx.notify();
    }

    fn read_plan(&self, cx: &mut Context<Self>) {
        let room = self.room_id.clone();
        let load = cx
            .background_executor()
            .spawn(async move { Plan::load(&storage::data_directory()?, &room).await });
        cx.spawn(async move |this, cx| {
            let result = load.await;
            let _ = this.update(cx, |view, cx| {
                view.plan_busy = false;
                match result {
                    Ok(plan) => view.plan = plan,
                    Err(error) => view.notice = Some(format!("读取联调登记失败。{error}").into()),
                }
                cx.notify();
            });
        })
        .detach();
    }

    fn read_outbox(&mut self, cx: &mut Context<Self>) {
        self.next_outbox_read = Instant::now() + Duration::from_secs(2);
        let room = self.room_id.clone();
        let load = cx
            .background_executor()
            .spawn(async move { tasks::load_reports(&storage::data_directory()?, &room).await });
        cx.spawn(async move |this, cx| {
            let restored = load.await;
            let _ = this.update(cx, |view, cx| {
                match restored {
                    Ok(reports) => {
                        for report in reports {
                            if !view.reports.iter().any(|pending| pending.id == report.id) {
                                view.reports.push_back(report);
                            }
                        }
                    }
                    Err(error) => view.notice = Some(format!("读取待发送回报失败。{error}").into()),
                }
                cx.notify();
            });
        })
        .detach();
    }

    pub fn request(&mut self, binding: Option<Secret>) -> tasks::Request {
        let action = if self.member == Member::Host {
            self.pending.clone().map(Action::Submit)
        } else if let Some(report) = self.reports.front() {
            Some(Action::Report(report.clone()))
        } else {
            self.claim_action(binding)
        };
        tasks::Request {
            action,
            binding: None,
        }
    }

    fn claim_action(&mut self, binding: Option<Secret>) -> Option<Action> {
        let task = self.latest.as_ref()?;
        let binding = binding?;
        if task.phase == Phase::Waiting
            && self
                .claim
                .as_ref()
                .is_none_or(|claim| claim.id != task.id || claim.binding != binding)
        {
            self.claim = Some(Claim {
                id: task.id,
                attempt: room::random().ok()?,
                binding,
                started: false,
            });
        }
        let claim = self
            .claim
            .as_ref()
            .filter(|claim| !claim.started && claim.id == task.id && claim.binding == binding)?;
        task.phase.active().then_some(Action::Claim {
            id: claim.id,
            attempt: claim.attempt,
        })
    }

    pub fn disconnected(&mut self, cx: &mut Context<Self>) {
        if self.connected {
            self.connected = false;
            cx.notify();
        }
    }

    pub fn receive(
        &mut self,
        update: Option<Update>,
        ready: bool,
        binding: Option<Binding>,
        sent_at: Instant,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let fingerprint = binding.as_ref().map(Binding::fingerprint);
        let changed = !self.connected || self.ready != ready || self.binding != fingerprint;
        self.connected = true;
        self.ready = ready;
        self.binding = fingerprint;
        let Some(update) = update else {
            if changed {
                cx.notify();
            }
            return;
        };
        if changed || self.latest != update.latest {
            self.latest = update.latest;
            cx.notify();
        }
        if let Some(error) = update.error {
            self.pending = None;
            self.notice = Some(error.into());
            cx.notify();
        }
        if let Some(id) = update.acknowledged {
            if self
                .pending
                .as_ref()
                .is_some_and(|pending| pending.id == id)
            {
                self.pending = None;
                self.notice = None;
                self.goal
                    .update(cx, |input, cx| input.set_value("", window, cx));
                cx.notify();
            } else if self.reports.front().is_some_and(|report| report.id == id) {
                self.reports.pop_front();
                let room = self.room_id.clone();
                cx.background_executor()
                    .spawn(async move {
                        if let Ok(directory) = storage::data_directory() {
                            let _ = tasks::acknowledge_report(&directory, &room, id).await;
                        }
                    })
                    .detach();
            }
        }
        if let (Some(grant), Some(binding)) = (update.grant, binding) {
            self.start(grant, binding, sent_at, cx);
        }
        if self.member == Member::Guest
            && self.reports.is_empty()
            && self
                .latest
                .as_ref()
                .is_some_and(|task| matches!(task.phase, Phase::Running | Phase::Uncertain))
            && self.next_outbox_read <= Instant::now()
        {
            // A worker from a previously closed room view may have finished after this view loaded.
            self.read_outbox(cx);
        }
    }

    fn start(&mut self, grant: Grant, binding: Binding, sent_at: Instant, cx: &mut Context<Self>) {
        let Some(claim) = self.claim.as_mut().filter(|claim| {
            !claim.started
                && claim.id == grant.id
                && claim.attempt == grant.attempt
                && claim.binding == binding.fingerprint()
        }) else {
            return;
        };
        let Some(task) = self.latest.as_ref().filter(|task| task.id == grant.id) else {
            return;
        };
        claim.started = true;
        let goal = task.goal.clone();
        let room = self.room_id.clone();
        let work = cx.background_executor().spawn(async move {
            // Subtract time spent exchanging the grant so the worker cannot extend the host's deadline.
            let remaining =
                Duration::from_secs(grant.seconds_left).saturating_sub(sent_at.elapsed());
            let outcome = if remaining.is_zero() {
                Err(Failure::Timeout)
            } else {
                codex::run_task(
                    Path::new(&binding.project),
                    &binding.thread_id,
                    &goal,
                    grant.id,
                    remaining,
                )
            };
            let report = Report {
                id: grant.id,
                attempt: grant.attempt,
                outcome,
            };
            let queued = match storage::data_directory() {
                Ok(directory) => tasks::save_report(&directory, &room, &report).await,
                Err(error) => Err(error),
            };
            (report, queued)
        });
        cx.spawn(async move |this, cx| {
            let (report, queued) = work.await;
            let _ = this.update(cx, |view, cx| {
                if !view.reports.iter().any(|pending| pending.id == report.id) {
                    view.reports.push_back(report);
                }
                if let Err(error) = queued {
                    view.notice = Some(
                        format!("本机无法保存回报。请保持联调台在线以发送结果。{error}").into(),
                    );
                }
                cx.notify();
            });
        })
        .detach();
    }

    fn can_submit(&self) -> bool {
        self.connected
            && self.ready
            && !self.plan_busy
            && self.pending.is_none()
            && self.latest.as_ref().is_none_or(|task| !task.phase.active())
    }

    fn submit(&mut self, cx: &mut Context<Self>) {
        if !self.can_submit() {
            return;
        }
        let value = self.goal.read(cx).value();
        let goal = value.trim();
        let submission = self
            .active_plan()
            .filter(|plan| plan.submission.goal == goal && plan.submission.minutes == self.minutes)
            .map(|plan| Ok(plan.submission.clone()))
            .unwrap_or_else(|| Submission::new(goal, self.minutes));
        match submission {
            Ok(submission) => {
                self.pending = Some(submission);
                self.notice = Some("等待主机确认任务".into());
            }
            Err(error) => self.notice = Some(error.to_string().into()),
        }
        cx.notify();
    }

    fn active_plan(&self) -> Option<&Plan> {
        self.plan
            .as_ref()
            .filter(|plan| Some(plan.binding) == self.binding)
    }

    fn can_register(&self) -> bool {
        self.member == Member::Host
            && self.binding.is_some()
            && !self.plan_busy
            && self.pending.is_none()
    }

    fn register(&mut self, cx: &mut Context<Self>) {
        let Some(binding) = self.binding.filter(|_| self.can_register()) else {
            return;
        };
        let submission = match Submission::new(self.goal.read(cx).value().as_ref(), self.minutes) {
            Ok(submission) => submission,
            Err(error) => {
                self.notice = Some(error.to_string().into());
                cx.notify();
                return;
            }
        };
        let plan = Plan {
            submission,
            binding,
        };
        self.plan_busy = true;
        let room = self.room_id.clone();
        let save = cx.background_executor().spawn(async move {
            plan.save(&storage::data_directory()?, &room).await?;
            Ok::<_, std::io::Error>(plan)
        });
        cx.spawn(async move |this, cx| {
            let result = save.await;
            let _ = this.update(cx, |view, cx| {
                view.plan_busy = false;
                match result {
                    Ok(plan) => {
                        view.plan = Some(plan);
                        view.notice =
                            Some("已登记开始联调。可复制启动说明给绑定的 Codex 会话。".into());
                    }
                    Err(error) => view.notice = Some(format!("登记失败。{error}").into()),
                }
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }

    fn copy_start_instructions(&mut self, cx: &mut Context<Self>) {
        if self.active_plan().is_none() {
            return;
        }
        self.notice = Some(match agent_start::instructions(&self.room_id) {
            Ok(instructions) => {
                cx.write_to_clipboard(ClipboardItem::new_string(instructions));
                "启动说明已复制。请粘贴到主协调端绑定的 Codex 会话。".into()
            }
            Err(error) => error.to_string().into(),
        });
        cx.notify();
    }
}

fn task_exchange(client: &RoomClient, action: Action) -> io::Result<Update> {
    client
        .exchange_room(
            i64::MAX,
            None,
            None,
            None,
            Some(tasks::Request {
                action: Some(action),
                binding: None,
            }),
        )?
        .task
        .ok_or_else(|| io::Error::other("主机没有返回任务信息"))
}

fn task_details(view: Stateful<Div>, task: &RoomTask) -> Stateful<Div> {
    view.child(Label::new(task.phase.label()))
        .child(Label::new(format!("任务 ID {}", URL_SAFE_NO_PAD.encode(task.id))).text_sm())
        .child(Label::new(format!("主机截止时间 {}", task.deadline_label)).text_sm())
        .child(Label::new(task.goal.clone()).text_sm())
        .when_some(
            if let Phase::Completed(report) = &task.phase {
                Some(report)
            } else {
                None
            },
            |view, report| view.child(Label::new(report.clone()).text_sm()),
        )
        .when_some(task.next_step.as_ref(), |view, note| {
            view.child(Label::new(format!("下一步 · {note}")).text_sm())
        })
}

impl Render for TaskView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let panel = div()
            .id("room-tasks")
            .size_full()
            .overflow_y_scroll()
            .p_5()
            .flex()
            .flex_col()
            .gap_3();
        if let Some(history) = &self.history {
            let older = history.entry.as_ref().and_then(|entry| entry.older);
            let newer = history.entry.as_ref().and_then(|entry| entry.newer);
            let current = history.entry.as_ref().map(|entry| entry.task.id);
            return panel
                .child(
                    div().flex().flex_wrap().gap_2().children([
                        Button::new("back-current-task")
                            .label("返回当前任务")
                            .disabled(history.loading)
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.history = None;
                                cx.notify();
                            })),
                        Button::new("older-task")
                            .label("较早一条")
                            .disabled(history.loading || older.is_none())
                            .on_click(cx.listener(move |view, _, _, cx| {
                                view.read_history(older, cx);
                            })),
                        Button::new("newer-task")
                            .label("较新一条")
                            .disabled(history.loading || newer.is_none())
                            .on_click(cx.listener(move |view, _, _, cx| {
                                view.read_history(newer, cx);
                            })),
                        Button::new("refresh-task-record")
                            .label("刷新记录")
                            .disabled(history.loading)
                            .on_click(cx.listener(move |view, _, _, cx| {
                                view.read_history(current, cx);
                            })),
                    ]),
                )
                .when(history.loading, |view| {
                    view.child(Label::new("正在读取任务记录").text_sm())
                })
                .when_some(history.notice.clone(), |view, notice| {
                    view.child(Label::new(notice).text_sm())
                })
                .when_some(history.entry.as_ref(), |view, entry| {
                    task_details(view, &entry.task)
                })
                .when(
                    history.entry.is_none() && !history.loading && history.notice.is_none(),
                    |view| view.child(Label::new("本房间暂无任务记录").text_sm()),
                );
        }
        if let Some(checkpoint) = &self.checkpoint {
            let task = self.latest.as_ref().filter(|task| task.id == checkpoint.id);
            return panel
                .child(
                    div().flex().child(
                        Button::new("back-from-checkpoint")
                            .label("返回当前任务")
                            .disabled(checkpoint.saving)
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.checkpoint = None;
                                cx.notify();
                            })),
                    ),
                )
                .when_some(task, |view, task| {
                    task_details(view, task).child(Label::new(task.phase.resume_hint()).text_sm())
                })
                .when(task.is_none(), |view| {
                    view.child(Label::new("当前任务已更新，返回后可以打开新的检查点").text_sm())
                })
                .when_some(checkpoint.draft.as_ref(), |view, draft| {
                    view.child(
                        Textarea::new(draft)
                            .aria_label("下一步")
                            .disabled(checkpoint.saving),
                    )
                    .child(
                        div().flex().child(
                            Button::new("save-next-step")
                                .label("保存下一步")
                                .disabled(!self.connected || checkpoint.saving || task.is_none())
                                .on_click(cx.listener(|view, _, _, cx| view.save_next_step(cx))),
                        ),
                    )
                })
                .when_some(checkpoint.notice.clone(), |view, notice| {
                    view.child(Label::new(notice).text_sm())
                });
        }
        panel
            .when(!self.connected, |view| {
                view.child(Label::new("正在等待房间连接").text_sm())
            })
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_2()
                    .child(
                        Button::new("task-history")
                            .label("任务记录")
                            .disabled(!self.connected)
                            .on_click(cx.listener(|view, _, _, cx| view.read_history(None, cx))),
                    )
                    .child(
                        Button::new("room-checkpoint")
                            .label("联调检查点")
                            .disabled(self.latest.is_none())
                            .on_click(
                                cx.listener(|view, _, window, cx| view.open_checkpoint(window, cx)),
                            ),
                    ),
            )
            .when_some(self.latest.as_ref(), task_details)
            .when(
                self.member == Member::Guest && self.latest.is_none(),
                |view| view.child(Label::new("等待主协调端发起联调").text_sm()),
            )
            .when(self.member == Member::Host, |view| {
                view.when(!self.ready, |view| {
                    view.child(Label::new("双方接单验证通过后可发起联调").text_sm())
                })
                .child(
                    Textarea::new(&self.goal)
                        .aria_label("联调目标")
                        .disabled(self.pending.is_some()),
                )
                .child(
                    div()
                        .flex()
                        .items_center()
                        .flex_wrap()
                        .gap_2()
                        .child(Label::new("任务时限").text_sm())
                        .children(tasks::MINUTES.into_iter().map(|minutes| {
                            let button = Button::new(("task-minutes", minutes as usize))
                                .label(format!("{minutes} 分钟"));
                            let button = if minutes == self.minutes {
                                button.primary()
                            } else {
                                button
                            };
                            button
                                .disabled(self.pending.is_some())
                                .on_click(cx.listener(move |view, _, _, cx| {
                                    view.minutes = minutes;
                                    cx.notify();
                                }))
                        })),
                )
                .child(
                    Button::new("submit-room-task")
                        .label("派给接入端（只读）")
                        .primary()
                        .disabled(!self.can_submit())
                        .on_click(cx.listener(|view, _, _, cx| view.submit(cx))),
                )
                .child(
                    div()
                        .flex()
                        .flex_wrap()
                        .gap_2()
                        .child(
                            Button::new("register-agent-start")
                                .label("登记「开始联调」")
                                .disabled(!self.can_register())
                                .on_click(cx.listener(|view, _, _, cx| view.register(cx))),
                        )
                        .when(self.active_plan().is_some(), |view| {
                            view.child(
                                Button::new("copy-agent-start")
                                    .label("复制 agent 启动说明")
                                    .disabled(self.plan_busy)
                                    .on_click(cx.listener(|view, _, _, cx| {
                                        view.copy_start_instructions(cx)
                                    })),
                            )
                        }),
                )
                .when_some(self.plan.as_ref(), |view, plan| {
                    view.child(
                        Label::new(if self.active_plan().is_some() {
                            format!(
                                "已登记目标 · {}（{} 分钟）",
                                plan.submission.goal, plan.submission.minutes
                            )
                        } else {
                            "本轮主协调会话尚未匹配已登记目标".into()
                        })
                        .text_sm(),
                    )
                })
            })
            .when_some(self.notice.clone(), |view, notice| {
                view.child(Label::new(notice).text_sm())
            })
    }
}
