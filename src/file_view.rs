use std::{io, path::PathBuf, sync::Arc};

use gpui_kit::component::{Disableable, button::Button, label::Label};
use gpui_kit::{prelude::FluentBuilder, *};

use crate::{
    room::RoomClient,
    room_files::{self, FileInfo, Upload},
};

enum UploadSource {
    Selected(PathBuf),
    Retry(Arc<Upload>),
}

pub struct FileView {
    client: RoomClient,
    busy: bool,
    retry: Option<Arc<Upload>>,
    notice: Option<SharedString>,
}

impl FileView {
    pub fn new(client: RoomClient) -> Self {
        Self {
            client,
            busy: false,
            retry: None,
            notice: None,
        }
    }

    fn choose(&mut self, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        self.busy = true;
        let selection = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: Some("选择发送到房间的文件".into()),
        });
        cx.spawn(async move |this, cx| {
            let selection = selection.await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                match selection {
                    Ok(Ok(Some(paths))) => {
                        if let Some(path) = paths.into_iter().next() {
                            view.send(UploadSource::Selected(path), cx);
                        }
                    }
                    Ok(Ok(None)) => {}
                    _ => view.notice = Some("无法打开文件选择器".into()),
                }
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }

    fn send(&mut self, source: UploadSource, cx: &mut Context<Self>) {
        self.busy = true;
        self.notice = Some("正在发送附件".into());
        let client = self.client.clone();
        let upload = cx.background_executor().spawn(async move {
            let file = match source {
                UploadSource::Selected(path) => Arc::new(Upload::read(&path)?),
                UploadSource::Retry(file) => file,
            };
            let result = client.upload(&file);
            Ok::<_, io::Error>((file, result))
        });
        cx.spawn(async move |this, cx| {
            let result = upload.await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                view.notice = Some(match result {
                    Ok((_, Ok(()))) => {
                        view.retry = None;
                        "附件已发送".into()
                    }
                    Ok((file, Err(error))) => {
                        view.retry = Some(file);
                        format!("附件发送未确认。{error}").into()
                    }
                    Err(error) => format!("无法读取要发送的文件。{error}").into(),
                });
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }

    pub fn choose_destination(&mut self, id: i64, file: FileInfo, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        let home = std::env::var_os("USERPROFILE")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        let selection = cx.prompt_for_new_path(&home, Some(&file.name));
        self.busy = true;
        cx.spawn(async move |this, cx| {
            let selection = selection.await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                match selection {
                    Ok(Ok(Some(path))) => view.save(id, file, path, cx),
                    Ok(Ok(None)) => {}
                    _ => view.notice = Some("无法打开文件保存对话框".into()),
                }
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }

    fn save(&mut self, id: i64, file: FileInfo, path: PathBuf, cx: &mut Context<Self>) {
        self.busy = true;
        self.notice = Some("正在保存附件".into());
        let client = self.client.clone();
        let save = cx.background_executor().spawn(async move {
            let bytes = client.download(id, &file)?;
            room_files::save_to(&path, &bytes)
        });
        cx.spawn(async move |this, cx| {
            let result = save.await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                view.notice = Some(match result {
                    Ok(()) => "附件已保存".into(),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        "已有同名文件，请选择其他文件名".into()
                    }
                    Err(error) => format!("保存附件失败。{error}").into(),
                });
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }
}

impl Render for FileView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_2()
            .when_some(self.notice.clone(), |view, notice| {
                view.child(Label::new(notice).text_sm())
            })
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_2()
                    .child(
                        Button::new("send-room-file")
                            .label("发送文件")
                            .disabled(self.busy)
                            .on_click(cx.listener(|view, _, _, cx| view.choose(cx))),
                    )
                    .when_some(self.retry.clone(), |view, file| {
                        view.child(
                            Button::new("retry-room-file")
                                .label("重试附件")
                                .disabled(self.busy)
                                .on_click(cx.listener(move |view, _, _, cx| {
                                    if !view.busy {
                                        view.send(UploadSource::Retry(file.clone()), cx);
                                    }
                                })),
                        )
                    }),
            )
    }
}
