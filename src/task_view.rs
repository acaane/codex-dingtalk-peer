use std::{
    collections::VecDeque,
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
    codex,
    readiness::{Failure, Id},
    room::{self, Member, Secret},
    storage,
    tasks::{self, Action, Grant, Phase, Report, Task as RoomTask, Update},
};

struct Claim {
    id: Id,
    attempt: Id,
    binding: Secret,
    started: bool,
}

pub struct TaskView {
    room_id: String,
    member: Member,
    goal: Entity<TextareaState>,
    minutes: u64,
    latest: Option<RoomTask>,
    pending: Option<Action>,
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
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut view = Self {
            room_id,
            member,
            goal: cx.new(|cx| {
                TextareaState::new(window, cx)
                    .rows(4)
                    .placeholder("填写接入端需要核对的目标")
            }),
            minutes: 5,
            latest: None,
            pending: None,
            claim: None,
            reports: VecDeque::new(),
            next_outbox_read: Instant::now(),
            connected: false,
            ready: false,
            notice: None,
        };
        if member == Member::Guest {
            view.read_outbox(cx);
        }
        view
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
            self.pending.clone()
        } else if let Some(report) = self.reports.front() {
            Some(Action::Report(report.clone()))
        } else {
            self.claim_action(binding)
        };
        tasks::Request { action }
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
        let changed = !self.connected || self.ready != ready;
        self.connected = true;
        self.ready = ready;
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
            if matches!(self.pending, Some(Action::Submit { id: pending, .. }) if pending == id) {
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
            && self.pending.is_none()
            && self.latest.as_ref().is_none_or(|task| !task.phase.active())
    }

    fn submit(&mut self, cx: &mut Context<Self>) {
        if !self.can_submit() {
            return;
        }
        let goal = self.goal.read(cx).value().trim().to_owned();
        if goal.is_empty() || goal.len() > tasks::MAX_GOAL {
            self.notice = Some("请填写联调目标，最多 4096 字节".into());
        } else {
            match room::random() {
                Ok(id) => {
                    self.pending = Some(Action::Submit {
                        id,
                        goal,
                        minutes: self.minutes,
                    });
                    self.notice = Some("等待主机确认任务".into());
                }
                Err(error) => self.notice = Some(error.to_string().into()),
            }
        }
        cx.notify();
    }
}

fn task_status(phase: &Phase) -> String {
    match phase {
        Phase::Waiting => "等待接入端接单".into(),
        Phase::Running => "接入端正在执行".into(),
        Phase::Completed(_) => "已收到接入端回报".into(),
        Phase::Failed(Failure::Timeout) => "接入端执行已到时限".into(),
        Phase::Failed(Failure::Reply) => "接入端会话没有返回正文".into(),
        Phase::Failed(reason) => format!("接入端执行失败。{}", reason.label()),
        Phase::Expired => "已过截止时间，任务未开始".into(),
        Phase::Uncertain => "已到截止时间，结果待核对。请查看接入端原会话".into(),
        Phase::BindingChanged => "会话已更换。请由主协调端重新派单".into(),
    }
}

impl Render for TaskView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("room-tasks")
            .size_full()
            .overflow_y_scroll()
            .p_5()
            .flex()
            .flex_col()
            .gap_3()
            .when(!self.connected, |view| {
                view.child(Label::new("正在等待房间连接").text_sm())
            })
            .when_some(self.latest.as_ref(), |view, task| {
                view.child(Label::new(task_status(&task.phase)))
                    .child(
                        Label::new(format!("任务 ID {}", URL_SAFE_NO_PAD.encode(task.id)))
                            .text_sm(),
                    )
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
            })
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
            })
            .when_some(self.notice.clone(), |view, notice| {
                view.child(Label::new(notice).text_sm())
            })
    }
}
