use std::{collections::HashSet, io, path::PathBuf};

use gpui_kit::component::{ActiveTheme, Disableable, button::Button, label::Label};
use gpui_kit::{prelude::FluentBuilder, *};

use crate::{
    room::RoomClient,
    room_files::{FileInfo, LocalSaves, Upload},
};

pub struct FileView {
    room_id: String,
    client: RoomClient,
    busy: bool,
    draft: Option<Upload>,
    saved: HashSet<i64>,
    directory: Option<PathBuf>,
    notice: Option<SharedString>,
}

impl FileView {
    pub fn new(client: RoomClient, room_id: String, cx: &mut Context<Self>) -> Self {
        let loading = room_id.clone();
        let load = cx
            .background_executor()
            .spawn(async move { LocalSaves::load(&loading).await });
        cx.spawn(async move |this, cx| {
            let result = load.await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                match result {
                    Ok(local) => {
                        view.saved = local.saved;
                        view.directory = local.directory;
                    }
                    Err(error) => {
                        view.notice = Some(format!("无法读取附件保存记录。{error}").into())
                    }
                }
                cx.notify();
            });
        })
        .detach();
        Self {
            room_id,
            client,
            busy: true,
            draft: None,
            saved: HashSet::new(),
            directory: None,
            notice: None,
        }
    }

    pub fn has_draft(&self) -> bool {
        self.draft.is_some()
    }

    pub fn busy(&self) -> bool {
        self.busy
    }

    pub fn was_saved(&self, id: i64) -> bool {
        self.saved.contains(&id)
    }

    pub fn choose(&mut self, cx: &mut Context<Self>) {
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
            let result = match selection.await {
                Ok(Ok(Some(paths))) => {
                    if let Some(path) = paths.into_iter().next() {
                        cx.background_executor()
                            .spawn(async move { Upload::select(path) })
                            .await
                            .map(Some)
                    } else {
                        Ok(None)
                    }
                }
                Ok(Ok(None)) => Ok(None),
                _ => Err(io::Error::other("无法打开文件选择器")),
            };
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                match result {
                    Ok(Some(file)) => {
                        view.draft = Some(file);
                        view.notice = None;
                    }
                    Ok(None) => {}
                    Err(error) => view.notice = Some(error.to_string().into()),
                }
                cx.notify();
            });
        })
        .detach();
        cx.notify();
    }

    pub fn send_draft(&mut self, cx: &mut Context<Self>) {
        let Some(file) = self.draft.clone().filter(|_| !self.busy) else {
            return;
        };
        self.busy = true;
        self.notice = Some("正在发送附件".into());
        let client = self.client.clone();
        let upload = cx
            .background_executor()
            .spawn(async move { client.upload(&file) });
        cx.spawn(async move |this, cx| {
            let result = upload.await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                view.notice = Some(match result {
                    Ok(()) => {
                        view.draft = None;
                        "附件已发送".into()
                    }
                    Err(error) => format!("附件发送未确认，可再次点击发送。{error}").into(),
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
        let directory = self
            .directory
            .as_deref()
            .filter(|path| path.is_dir())
            .unwrap_or(&home);
        let selection = cx.prompt_for_new_path(directory, Some(&file.name));
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
        let room = self.room_id.clone();
        let save = cx.background_executor().spawn(async move {
            client.download(id, &file, &path)?;
            let directory = path.parent().expect("download has a parent").to_path_buf();
            let recorded = LocalSaves::record(&room, id, &directory).await;
            Ok::<_, io::Error>((directory, recorded))
        });
        cx.spawn(async move |this, cx| {
            let result = save.await;
            let _ = this.update(cx, |view, cx| {
                view.busy = false;
                view.notice = match result {
                    Ok((directory, recorded)) => {
                        view.saved.insert(id);
                        view.directory = Some(directory);
                        recorded
                            .err()
                            .map(|error| format!("附件已保存。无法记录保存目录。{error}").into())
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        Some("已有同名文件，请选择其他文件名".into())
                    }
                    Err(error) => Some(format!("保存附件失败。{error}").into()),
                };
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
            .when_some(self.draft.as_ref(), |view, file| {
                let size = f64::from(file.info.size);
                let size = if size < 1024.0 {
                    format!("{size:.0} B")
                } else if size < 1024.0 * 1024.0 {
                    format!("{:.1} KiB", size / 1024.0)
                } else {
                    format!("{:.1} MiB", size / (1024.0 * 1024.0))
                };
                view.child(
                    div()
                        .flex()
                        .items_center()
                        .flex_wrap()
                        .gap_3()
                        .p_2()
                        .border_1()
                        .border_color(cx.theme().border)
                        .rounded_lg()
                        .child(
                            Label::new(format!("待发送 · {} · {size}", file.info.name)).text_sm(),
                        )
                        .child(
                            Button::new("remove-room-file")
                                .label("移除附件")
                                .disabled(self.busy)
                                .on_click(cx.listener(|view, _, _, cx| {
                                    view.draft = None;
                                    view.notice = None;
                                    cx.notify();
                                })),
                        ),
                )
            })
            .when_some(self.notice.clone(), |view, notice| {
                view.child(Label::new(notice).text_sm())
            })
    }
}
