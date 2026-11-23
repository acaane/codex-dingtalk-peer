#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

mod chat;
mod ui;

use std::{error::Error, path::PathBuf};

use gpui_kit::*;

use crate::chat::{ChatStore, Message};

fn open_chat() -> Result<(ChatStore, Vec<Message>), Box<dyn Error>> {
    let app_data = std::env::var_os("LOCALAPPDATA")
        .filter(|directory| !directory.is_empty())
        .ok_or("Windows 未提供 LOCALAPPDATA 数据目录")?;
    let directory = PathBuf::from(app_data).join("CodexDingTalkPeer");
    std::fs::create_dir_all(&directory)?;
    let store = ChatStore::open(&directory.join("chat.sqlite3"))?;
    let history = store.history()?;
    Ok((store, history))
}

fn main() {
    application().with_assets(assets::Assets).run(|cx| {
        init(cx);
        let options = WindowOptions {
            titlebar: Some(TitlebarOptions {
                title: Some("AI 联调连接器".into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(Bounds::centered(
                None,
                size(px(900.), px(680.)),
                cx,
            ))),
            window_min_size: Some(size(px(440.), px(340.))),
            ..Default::default()
        };
        let chat = open_chat();
        open_window(options, cx, |window, cx| {
            cx.new(|cx| ui::ChatWindow::new(chat, window, cx))
        })
        .expect("打开联调连接器窗口");
    });
}
