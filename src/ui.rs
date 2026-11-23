use std::{error::Error, rc::Rc};

use gpui_kit::base::StyledExt;
use gpui_kit::component::{
    ActiveTheme,
    button::{Button, ButtonVariants},
    input::{Textarea, TextareaState},
    label::Label,
};
use gpui_kit::*;

use crate::chat::{ChatStore, Message, SendError};

pub(crate) struct ChatWindow {
    opened: Result<Chat, SharedString>,
}

struct Chat {
    store: ChatStore,
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
                    store,
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
        Self { opened }
    }

    fn send(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Ok(chat) = &mut self.opened else { return };
        let draft = chat.composer.read(cx).value();
        let last_seen = chat.messages.last().map_or(0, |message| message.id);
        chat.notice = match chat.store.send(&draft, last_seen) {
            Ok(messages) => {
                let old_len = chat.messages.len();
                chat.list.splice(old_len..old_len, messages.len());
                Rc::make_mut(&mut chat.messages).extend(messages);
                chat.list.set_follow_mode(FollowMode::Tail);
                chat.composer
                    .update(cx, |input, cx| input.set_value("", window, cx));
                None
            }
            Err(SendError::Empty) => Some("请输入消息后再发送".into()),
            Err(SendError::Database(error)) => {
                Some(format!("消息保存失败，草稿已保留。{error}").into())
            }
        };
        cx.notify();
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
            .bg(background)
            .border_b_1()
            .border_color(border)
            .px_6()
            .py_4()
            .child(Label::new("本机聊天").text_lg().font_semibold());

        let content =
            match &self.opened {
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
                        list(chat.list.clone(), move |index, _, _| {
                            let message = &messages[index];
                            div()
                                .id(("chat-message", message.id as usize))
                                .w_full()
                                .flex()
                                .justify_end()
                                .px_6()
                                .py_2()
                                .child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .gap_1()
                                        .max_w(px(620.))
                                        .rounded_lg()
                                        .bg(background)
                                        .border_1()
                                        .border_color(border)
                                        .px_4()
                                        .py_3()
                                        .child(
                                            Label::new(message.local_time.clone())
                                                .text_xs()
                                                .text_color(muted_text),
                                        )
                                        .child(Label::new(message.body.clone()).text_sm()),
                                )
                                .into_any_element()
                        })
                        .flex_1()
                        .min_h_0()
                        .into_any_element()
                    };
                    let composer =
                        div()
                            .flex_none()
                            .flex()
                            .flex_col()
                            .gap_2()
                            .border_t_1()
                            .border_color(border)
                            .bg(background)
                            .p_4()
                            .child(Textarea::new(&chat.composer).aria_label("消息输入"))
                            .children(
                                chat.notice
                                    .iter()
                                    .cloned()
                                    .map(|notice| Label::new(notice).text_sm()),
                            )
                            .child(div().flex().justify_end().child(
                                Button::new("send").label("发送").primary().on_click(
                                    cx.listener(|this, _, window, cx| this.send(window, cx)),
                                ),
                            ));
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
