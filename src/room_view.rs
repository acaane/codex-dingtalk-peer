use std::{net::Ipv4Addr, path::Path, rc::Rc, time::Duration};

use gpui_kit::component::{
    ActiveTheme, Disableable,
    button::{Button, ButtonVariants},
    input::{Textarea, TextareaState},
    label::Label,
};
use gpui_kit::{prelude::FluentBuilder, *};
use ring::digest::{SHA256, digest};

use crate::{
    agent_view::AgentView,
    codex,
    readiness::{self, Id, Status},
    room::{self, Member, Outgoing, RoomClient, RoomConnection, RoomHost, RoomMessage, RoomUpdate},
    room_sessions::Sessions,
};

pub struct RoomView {
    addresses: Vec<Ipv4Addr>,
    selected: Option<Ipv4Addr>,
    invite: Entity<TextareaState>,
    can_resume: bool,
    composer: Entity<TextareaState>,
    connecting: bool,
    notice: Option<SharedString>,
    active: Option<ActiveRoom>,
    sync: Option<Task<()>>,
}

struct ActiveRoom {
    host: Option<(RoomHost, String)>,
    client: RoomClient,
    agent: Entity<AgentView>,
    readiness: Option<readiness::Update>,
    probe: Option<LocalProbe>,
    show_agent: bool,
    member: Member,
    connected: bool,
    guest_joined: bool,
    messages: Rc<Vec<RoomMessage>>,
    list: ListState,
    pending: Option<Outgoing>,
    queued: bool,
    sending: bool,
}

struct LocalProbe {
    binding: Option<room::Secret>,
    id: Id,
    started: bool,
    retry: bool,
    result: Option<readiness::Report>,
}

impl ActiveRoom {
    fn readiness_request(&mut self, cx: &App) -> Option<readiness::Request> {
        let binding = self
            .agent
            .read(cx)
            .binding()?
            .map(|binding| binding_key(&binding.thread_id));
        if self
            .probe
            .as_ref()
            .is_none_or(|probe| probe.binding != binding)
        {
            self.probe = Some(LocalProbe {
                binding,
                id: room::random().ok()?,
                started: false,
                retry: false,
                result: None,
            });
        }
        let probe = self.probe.as_ref()?;
        Some(readiness::Request {
            binding,
            attempt: probe.id,
            retry: probe.retry,
            result: probe.result,
        })
    }

    fn start_probe(&mut self, challenge: Id, cx: &mut Context<RoomView>) {
        let Some(probe) = self.probe.as_mut().filter(|probe| !probe.started) else {
            return;
        };
        let Some(binding) = self.agent.read(cx).binding().flatten().cloned() else {
            return;
        };
        if probe.binding != Some(binding_key(&binding.thread_id)) {
            return;
        }
        probe.started = true;
        probe.retry = false;
        let attempt = probe.id;
        let job = cx.background_executor().spawn(async move {
            codex::verify_session(Path::new(&binding.project), &binding.thread_id, challenge)
        });
        cx.spawn(async move |view, cx| {
            let answer = job.await;
            let _ = view.update(cx, |view, _| {
                if let Some(probe) = view
                    .active
                    .as_mut()
                    .and_then(|room| room.probe.as_mut())
                    .filter(|probe| probe.id == attempt)
                {
                    probe.result = Some(readiness::Report { challenge, answer });
                }
            });
        })
        .detach();
    }
}

fn binding_key(thread: &str) -> room::Secret {
    let mut key = [0; 32];
    key.copy_from_slice(digest(&SHA256, thread.as_bytes()).as_ref());
    key
}

fn readiness_label(member: Member, status: Status) -> String {
    let label = match status {
        Status::Unbound => "等待绑定 Codex 会话",
        Status::Waiting => "等待双方绑定",
        Status::Running => "正在验证接单",
        Status::Passed => "接单验证通过",
        Status::Failed(reason) => {
            return format!("{} · 验证失败。{}", member.label(), reason.label());
        }
    };
    format!("{} · {label}", member.label())
}

impl RoomView {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let addresses = room::private_addresses().unwrap_or_default();
        let selected = (addresses.len() == 1).then(|| addresses[0]);
        Self {
            addresses,
            selected,
            can_resume: crate::storage::data_directory()
                .and_then(|directory| Sessions::has_last(&directory))
                .unwrap_or(false),
            invite: cx.new(|cx| {
                TextareaState::new(window, cx)
                    .rows(3)
                    .placeholder("粘贴主协调端发来的邀请")
            }),
            composer: cx.new(|cx| {
                TextareaState::new(window, cx)
                    .rows(3)
                    .placeholder("输入房间消息")
            }),
            connecting: false,
            notice: None,
            active: None,
            sync: None,
        }
    }

    fn connect(&mut self, request: ConnectionRequest, window: &Window, cx: &mut Context<Self>) {
        if self.connecting {
            return;
        }
        let action = match &request {
            ConnectionRequest::Host(_) => "创建",
            ConnectionRequest::Join(_) => "加入",
            ConnectionRequest::Resume => "继续",
        };
        self.connecting = true;
        self.notice = None;
        let connection = cx
            .background_executor()
            .spawn(async move { request.open() });
        cx.spawn_in(window, async move |this, cx| {
            let result = connection.await;
            let _ = this.update_in(cx, |view, window, cx| {
                view.connecting = false;
                match result {
                    Ok(connection) => {
                        view.can_resume = true;
                        view.invite
                            .update(cx, |input, cx| input.set_value("", window, cx));
                        view.enter(connection, window, cx);
                    }
                    Err(error) => {
                        view.notice = Some(format!("{action}房间失败。{error}").into());
                    }
                }
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }

    fn enter(&mut self, connection: RoomConnection, window: &Window, cx: &mut Context<Self>) {
        let member = if connection.host.is_some() {
            Member::Host
        } else {
            Member::Guest
        };
        let list = ListState::new(0, ListAlignment::Bottom, px(240.));
        list.set_follow_mode(FollowMode::Tail);
        let agent = cx.new(|cx| AgentView::new(connection.room_id, cx));
        self.active = Some(ActiveRoom {
            host: connection.host,
            client: connection.client,
            agent,
            readiness: None,
            probe: None,
            show_agent: false,
            member,
            connected: connection.ready,
            guest_joined: member == Member::Guest,
            messages: Rc::new(Vec::new()),
            list,
            pending: None,
            queued: false,
            sending: false,
        });
        self.sync = Some(cx.spawn_in(window, async move |this, cx| {
            loop {
                let Ok(Some((client, cursor, outgoing, pending, readiness))) =
                    this.update_in(cx, |view, _, cx| {
                        let room = view.active.as_mut()?;
                        let outgoing = if std::mem::take(&mut room.queued) {
                            room.pending.clone()
                        } else {
                            None
                        };
                        Some((
                            room.client.clone(),
                            room.messages.last().map_or(0, |msg| msg.id),
                            outgoing,
                            room.pending.clone(),
                            room.readiness_request(cx),
                        ))
                    })
                else {
                    break;
                };
                let sent = outgoing.is_some();
                let response = cx
                    .background_executor()
                    .spawn(async move {
                        client.exchange_with_readiness(
                            cursor,
                            outgoing,
                            pending.as_ref(),
                            readiness,
                        )
                    })
                    .await;
                if this
                    .update_in(cx, |view, window, cx| {
                        view.receive(response, sent, window, cx)
                    })
                    .is_err()
                {
                    break;
                }
                cx.background_executor()
                    .timer(Duration::from_millis(500))
                    .await;
            }
        }));
    }

    fn receive(
        &mut self,
        result: std::io::Result<RoomUpdate>,
        sent: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(room) = self.active.as_mut() else {
            return;
        };
        let previously_connected = room.connected;
        room.connected = result.is_ok();
        match result {
            Ok(update) => {
                let changed = previously_connected != room.connected
                    || room.guest_joined != update.guest_joined
                    || room.readiness != update.readiness;
                room.guest_joined = update.guest_joined;
                room.readiness = update.readiness;
                if let Some(challenge) = update.readiness.and_then(|ready| ready.challenge) {
                    room.start_probe(challenge, cx);
                }
                let count = update.messages.len();
                if count != 0 {
                    let end = room.messages.len();
                    room.list.splice(end..end, count);
                    Rc::make_mut(&mut room.messages).extend(update.messages);
                }
                if update.confirmed && room.pending.is_some() {
                    room.pending = None;
                    room.queued = false;
                    room.sending = false;
                    room.list.set_follow_mode(FollowMode::Tail);
                    self.notice = None;
                    self.composer
                        .update(cx, |input, cx| input.set_value("", window, cx));
                } else if sent {
                    room.sending = false;
                    self.notice = Some("主机尚未确认，可重试发送原消息".into());
                } else if !changed && count == 0 {
                    return;
                }
            }
            Err(error) if sent => {
                room.sending = false;
                self.notice = Some(format!("发送未确认，草稿已保留。{error}").into());
            }
            Err(_) if !previously_connected => return,
            Err(_) => (),
        }
        cx.notify();
    }

    fn send(&mut self, cx: &mut Context<Self>) {
        let Some(room) = self.active.as_mut() else {
            return;
        };
        if room.sending || !room.connected {
            return;
        }
        if room.pending.is_none() {
            match Outgoing::new(self.composer.read(cx).value().to_string()) {
                Ok(message) => room.pending = Some(message),
                Err(error) => {
                    self.notice = Some(error.to_string().into());
                    cx.notify();
                    return;
                }
            }
        }
        room.queued = true;
        room.sending = true;
        self.notice = Some("正在等待主机确认".into());
        cx.notify();
    }

    fn lobby(&self, cx: &mut Context<Self>) -> AnyElement {
        let border = cx.theme().border;
        let background = cx.theme().background;
        let card = || {
            div()
                .flex()
                .flex_col()
                .gap_3()
                .p_4()
                .bg(background)
                .border_1()
                .border_color(border)
                .rounded_lg()
        };
        div()
            .id("room-lobby")
            .size_full()
            .overflow_y_scroll()
            .p_5()
            .flex()
            .flex_col()
            .gap_4()
            .when(self.can_resume, |view| {
                view.child(
                    card().child(
                        Button::new("resume-room")
                            .label("继续上次房间")
                            .primary()
                            .disabled(self.connecting)
                            .on_click(cx.listener(|view, _, window, cx| {
                                view.connect(ConnectionRequest::Resume, window, cx)
                            })),
                    ),
                )
            })
            .child(
                card()
                    .child(Label::new("选择主协调电脑的内网地址").text_sm())
                    .when(self.addresses.is_empty(), |view| {
                        view.child(Label::new("未检测到内网 IPv4 地址").text_sm())
                    })
                    .child(
                        div().flex().flex_wrap().gap_2().children(
                            self.addresses
                                .iter()
                                .copied()
                                .enumerate()
                                .map(|(index, ip)| {
                                    let button = Button::new(("address", index))
                                        .label(ip.to_string())
                                        .disabled(self.connecting);
                                    let button = if self.selected == Some(ip) {
                                        button.primary()
                                    } else {
                                        button
                                    };
                                    button.on_click(cx.listener(move |view, _, _, cx| {
                                        view.selected = Some(ip);
                                        cx.notify();
                                    }))
                                }),
                        ),
                    )
                    .child(
                        Button::new("create-room")
                            .label("创建房间")
                            .primary()
                            .disabled(self.selected.is_none() || self.connecting)
                            .on_click(cx.listener(|view, _, window, cx| {
                                if let Some(ip) = view.selected {
                                    view.connect(ConnectionRequest::Host(ip), window, cx);
                                }
                            })),
                    ),
            )
            .child(
                card()
                    .child(
                        Textarea::new(&self.invite)
                            .aria_label("房间邀请")
                            .disabled(self.connecting),
                    )
                    .child(
                        Button::new("join-room")
                            .label("加入房间")
                            .disabled(self.connecting)
                            .on_click(cx.listener(|view, _, window, cx| {
                                let code = view.invite.read(cx).value().trim().to_owned();
                                view.connect(ConnectionRequest::Join(code), window, cx);
                            })),
                    ),
            )
            .when(self.connecting, |view| {
                view.child(Label::new("正在连接房间").text_sm())
            })
            .children(
                self.notice
                    .iter()
                    .cloned()
                    .map(|notice| Label::new(notice).text_sm()),
            )
            .into_any_element()
    }

    fn in_room(&self, room: &ActiveRoom, cx: &mut Context<Self>) -> AnyElement {
        let background = cx.theme().background;
        let border = cx.theme().border;
        let member = room.member;
        let messages = room.messages.clone();
        let history = list(room.list.clone(), move |index, _, cx| {
            let message = &messages[index];
            crate::ui::message_row(
                message.id,
                message.author == member,
                &message.body,
                &format!("{} · {}", message.author.label(), message.time),
                cx,
            )
        })
        .flex_1()
        .min_h_0();
        let state = if !room.connected {
            "连接中断，正在重新连接"
        } else if member == Member::Host && !room.guest_joined {
            "等待接入端加入"
        } else {
            "房间已连接"
        };
        let header =
            div()
                .flex()
                .items_center()
                .gap_3()
                .px_4()
                .py_3()
                .border_b_1()
                .border_color(border)
                .bg(background)
                .child(Label::new(format!("{} · {state}", member.label())).text_sm())
                .child(div().flex_1())
                .when_some(
                    room.host
                        .as_ref()
                        .filter(|_| !room.guest_joined)
                        .map(|(_, invitation)| invitation.clone()),
                    |view, invitation| {
                        view.child(Button::new("copy-invite").label("复制邀请").on_click(
                            cx.listener(move |view, _, _, cx| {
                                cx.write_to_clipboard(ClipboardItem::new_string(
                                    invitation.clone(),
                                ));
                                view.notice = Some("邀请已复制".into());
                                cx.notify();
                            }),
                        ))
                    },
                )
                .child(
                    Button::new("codex-agent")
                        .label(if room.show_agent {
                            "返回房间聊天"
                        } else {
                            "Codex 会话"
                        })
                        .on_click(cx.listener(|view, _, _, cx| {
                            let Some(room) = view.active.as_mut() else {
                                return;
                            };
                            room.show_agent = !room.show_agent;
                            cx.notify();
                        })),
                )
                .child(
                    Button::new("leave-room")
                        .label("离开房间")
                        .on_click(cx.listener(|view, _, window, cx| {
                            view.sync = None;
                            view.active = None;
                            view.notice = None;
                            view.composer
                                .update(cx, |input, cx| input.set_value("", window, cx));
                            cx.notify();
                        })),
                );
        let status = div()
            .flex()
            .items_center()
            .flex_wrap()
            .gap_3()
            .px_4()
            .py_2()
            .bg(background)
            .border_b_1()
            .border_color(border)
            .when_some(room.readiness.filter(|_| room.connected), |view, update| {
                view.child(Label::new(readiness_label(Member::Host, update.host)).text_sm())
                    .child(Label::new(readiness_label(Member::Guest, update.guest)).text_sm())
                    .when(matches!(update.status(member), Status::Failed(_)), |view| {
                        view.child(
                            Button::new("retry-readiness")
                                .label("重试本端验证")
                                .disabled(room.probe.as_ref().is_some_and(|probe| probe.retry))
                                .on_click(cx.listener(|view, _, _, cx| {
                                    if let Some(probe) =
                                        view.active.as_mut().and_then(|room| room.probe.as_mut())
                                    {
                                        match room::random() {
                                            Ok(id) => {
                                                probe.id = id;
                                                probe.result = None;
                                                probe.started = false;
                                                probe.retry = true;
                                            }
                                            Err(error) => {
                                                view.notice = Some(error.to_string().into())
                                            }
                                        }
                                        cx.notify();
                                    }
                                })),
                        )
                    })
            });
        if room.show_agent {
            return div()
                .size_full()
                .flex()
                .flex_col()
                .child(header)
                .child(status)
                .child(div().flex_1().min_h_0().child(room.agent.clone()))
                .into_any_element();
        }
        let input = div()
            .flex_none()
            .flex()
            .flex_col()
            .gap_2()
            .p_4()
            .border_t_1()
            .border_color(border)
            .bg(background)
            .child(
                Textarea::new(&self.composer)
                    .aria_label("房间消息")
                    .disabled(room.pending.is_some()),
            )
            .children(
                self.notice
                    .iter()
                    .cloned()
                    .map(|notice| Label::new(notice).text_sm()),
            )
            .child(
                div().flex().justify_end().child(
                    Button::new("send-room-message")
                        .label(if room.pending.is_some() && !room.sending {
                            "重试发送"
                        } else {
                            "发送"
                        })
                        .primary()
                        .disabled(room.sending || !room.connected)
                        .on_click(cx.listener(|view, _, _, cx| view.send(cx))),
                ),
            );
        div()
            .size_full()
            .flex()
            .flex_col()
            .child(header)
            .child(status)
            .child(history)
            .child(input)
            .into_any_element()
    }
}

enum ConnectionRequest {
    Host(Ipv4Addr),
    Join(String),
    Resume,
}

impl ConnectionRequest {
    fn open(self) -> std::io::Result<RoomConnection> {
        let directory = crate::storage::data_directory()?;
        match self {
            Self::Host(ip) => RoomConnection::create(ip, &directory),
            Self::Join(code) => RoomConnection::join(&code, &directory),
            Self::Resume => RoomConnection::resume(&directory),
        }
    }
}

impl Render for RoomView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        match &self.active {
            Some(room) => self.in_room(room, cx),
            None => self.lobby(cx),
        }
    }
}
