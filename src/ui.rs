use std::{error::Error, rc::Rc};

use gpui_kit::base::StyledExt;
use gpui_kit::component::{
    ActiveTheme, Disableable,
    button::{Button, ButtonVariants},
    input::{Textarea, TextareaState},
    label::Label,
};
use gpui_kit::*;

use crate::chat::{ChatStore, Message, SendError};
use crate::room_view::RoomView;

pub(crate) struct ChatWindow {
    opened: Result<Chat, SharedString>,
    show_room: bool,
    room: Entity<RoomView>,
}

struct Chat {
    store: Option<ChatStore>,
    messages: Rc<Vec<Message>>,
    list: ListState,
    composer: Entity<TextareaState>,
    notice: Option<SharedString>,
}

impl ChatWindow {
    pub(crate) fn new(
        opened: Result<(ChatStore, Vec<Message>), Box<dyn Error>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let opened = opened
            .map(|(store, messages)| {
                let list = ListState::new(messages.len(), ListAlignment::Bottom, px(240.));
                list.set_follow_mode(FollowMode::Tail);
                Chat {
                    store: Some(store),
                    messages: Rc::new(messages),
                    list,
                    composer: cx.new(|cx| {
                        TextareaState::new(window, cx)
                            .rows(3)
                            .placeholder("输入要保存的消息")
                    }),
                    notice: None,
                }
            })
            .map_err(|error| format!("无法打开本机聊天记录。{error}").into());
        let room = cx.new(|cx| RoomView::new(window, cx));
        Self {
            opened,
            room,
            show_room: false,
        }
    }

    fn send(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Ok(chat) = &mut self.opened else { return };
        let Some(mut store) = chat.store.take() else {
            return;
        };
        let draft = chat.composer.read(cx).value();
        let last_seen = chat.messages.last().map_or(0, |message| message.id);
        cx.spawn_in(window, async move |this, cx| {
            let result = store.send(&draft, last_seen).await;
            let _ = this.update_in(cx, |this, window, cx| {
                let Ok(chat) = &mut this.opened else { return };
                chat.store = Some(store);
                chat.saved(result, window, cx);
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }
}

impl Chat {
    fn saved(
        &mut self,
        result: Result<Vec<Message>, SendError>,
        window: &mut Window,
        cx: &mut Context<ChatWindow>,
    ) {
        self.notice = match result {
            Ok(messages) => {
                let old_len = self.messages.len();
                self.list.splice(old_len..old_len, messages.len());
                Rc::make_mut(&mut self.messages).extend(messages);
                self.list.set_follow_mode(FollowMode::Tail);
                self.composer
                    .update(cx, |input, cx| input.set_value("", window, cx));
                None
            }
            Err(SendError::Empty) => Some("请输入消息后再发送".into()),
            Err(SendError::Database(error)) => {
                Some(format!("消息保存失败，草稿已保留。{error}").into())
            }
        };
    }
}

impl Render for ChatWindow {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let background = cx.theme().background;
        let border = cx.theme().border;
        let muted = cx.theme().muted;
        let muted_text = cx.theme().muted_foreground;
        let header = div()
            .flex_none()
            .flex()
            .items_center()
            .bg(background)
            .border_b_1()
            .border_color(border)
            .px_6()
            .py_4()
            .child(
                Label::new(if self.show_room {
                    "内网房间"
                } else {
                    "本机聊天"
                })
                .text_lg()
                .font_semibold(),
            )
            .child(div().flex_1())
            .child(
                Button::new("switch-chat")
                    .label(if self.show_room {
                        "本机聊天"
                    } else {
                        "内网房间"
                    })
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.show_room = !this.show_room;
                        cx.notify();
                    })),
            );

        if self.show_room {
            return div()
                .size_full()
                .flex()
                .flex_col()
                .bg(muted)
                .child(header)
                .child(div().flex_1().min_h_0().child(self.room.clone()));
        }
        let content = match &self.opened {
            Err(error) => div()
                .p_6()
                .child(Label::new(error.clone()))
                .into_any_element(),
            Ok(chat) => {
                let history = if chat.messages.is_empty() {
                    div()
                        .flex_1()
                        .flex()
                        .items_center()
                        .justify_center()
                        .child(Label::new("暂无消息").text_sm().text_color(muted_text))
                        .into_any_element()
                } else {
                    let messages = chat.messages.clone();
                    list(chat.list.clone(), move |index, _, cx| {
                        let message = &messages[index];
                        message_row(
                            message.id,
                            true,
                            &message.body,
                            &message.local_time,
                            None,
                            cx,
                        )
                    })
                    .flex_1()
                    .min_h_0()
                    .into_any_element()
                };
                let composer = div()
                    .flex_none()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .border_t_1()
                    .border_color(border)
                    .bg(background)
                    .p_4()
                    .child(
                        Textarea::new(&chat.composer)
                            .aria_label("消息输入")
                            .disabled(chat.store.is_none()),
                    )
                    .children(
                        chat.notice
                            .iter()
                            .cloned()
                            .map(|notice| Label::new(notice).text_sm()),
                    )
                    .child(
                        div().flex().justify_end().child(
                            Button::new("send")
                                .label("发送")
                                .primary()
                                .disabled(chat.store.is_none())
                                .on_click(cx.listener(|this, _, window, cx| this.send(window, cx))),
                        ),
                    );
                div()
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .flex_col()
                    .child(history)
                    .child(composer)
                    .into_any_element()
            }
        };

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(muted)
            .child(header)
            .child(content)
    }
}

pub(crate) fn message_row(
    id: i64,
    own: bool,
    body: &str,
    meta: &str,
    action: Option<Button>,
    cx: &App,
) -> AnyElement {
    let row = div()
        .id(("chat-message", id as usize))
        .w_full()
        .flex()
        .px_6()
        .py_2();
    let row = if own {
        row.justify_end()
    } else {
        row.justify_start()
    };
    row.child(
        div()
            .flex()
            .flex_col()
            .gap_1()
            .max_w(px(620.))
            .rounded_lg()
            .bg(cx.theme().background)
            .border_1()
            .border_color(cx.theme().border)
            .px_4()
            .py_3()
            .child(
                Label::new(meta.to_owned())
                    .text_xs()
                    .text_color(cx.theme().muted_foreground),
            )
            .child(Label::new(body.to_owned()).text_sm())
            .children(action),
    )
    .into_any_element()
}
